from __future__ import annotations
import collections
import logging
import time
from datetime import datetime, timezone
import numpy as np
import psutil
from sqlalchemy import insert as sa_insert

from ..db import LatencyRecord, ParkingSpot, ScenarioRun, make_session
from ..models import BatchUpdate, ParkingEvent, SpotState
from ..config import ScenarioConfig
from ..des.engine import SimClock

logger = logging.getLogger(__name__)

_BROKER_SERVICE_RATE: dict[str, float] = {
    "mqtt_qos0": 50_000.0,
    "mqtt_qos1": 20_000.0,
    "mqtt_qos2": 8_000.0,
    "amqp_direct/auto": 25_000.0,
    "amqp_direct/manual": 10_000.0,
    "amqp_topic/manual": 8_000.0,
    "coap_NON": 45_000.0,
    "coap_CON": 18_000.0
}

_WARMUP_S: float = 300.0


class CloudBackend:

    def __init__(self, config: ScenarioConfig, clock: SimClock, epoch: float) -> None:
        self.config = config
        self.clock = clock
        self.epoch = epoch
        self.num_spots = config.num_spots

        self._spots: dict[int, dict] = {
            i: {"state": SpotState.FREE.value, "last_updated": 0.0, "received_at": 0.0}
            for i in range(self.num_spots)
        }

        self.received_batches = 0
        self.received_events = 0
        self.transitions_received = 0

        self._latency_ms: list[float] = []
        self._post_warmup_ms: list[float] = []
        self.warmup_excluded: int = 0
        self._event_rows: list[tuple] = []

        self._total_bytes_received = 0
        self._process = psutil.Process()
        self._process.cpu_percent(interval=None)
        self._event_buffer: collections.deque = collections.deque(maxlen=50_000)

        self._run_id: int | None = None
        self._started_at: datetime = datetime.now(timezone.utc)

        self._snapshot_cache: dict | None = None
        self._snapshot_cache_len: int = -1
        self._last_cpu_time: float = time.monotonic()
        self._cpu_samples: list[float] = []

    def receive_batch(self, batch: BatchUpdate, raw_bytes: bytes) -> None:
        from simulator.protocols.broker_config import use_real_brokers
        if use_real_brokers():
            arrival = time.time()
        else:
            arrival = self.epoch + self.clock.now

        self.received_batches += 1
        self._total_bytes_received += len(raw_bytes)
        for event in batch.events:
            self._process_event(event, arrival)

    def receive_batch_real(self, batch: BatchUpdate, raw_bytes: bytes, wall_arrival: float | None = None) -> None:
        import time as _t
        arrival = wall_arrival if wall_arrival is not None else _t.time()
        self.received_batches += 1
        self._total_bytes_received += len(raw_bytes)
        for event in batch.events:
            self._process_event(event, arrival)

    def _process_event(self, event: ParkingEvent, arrival: float) -> None:
        self.received_events += 1
        state_val = (event.state.value if isinstance(event.state, SpotState) else str(event.state))
        spot = self._spots.get(event.spot_id)
        is_transition = False
        if spot is not None:
            if spot["state"] != state_val and spot["received_at"] > 0.0:
                is_transition = True
            elif spot["received_at"] == 0.0 and state_val != SpotState.FREE.value:
                is_transition = False
            spot["state"] = state_val
            spot["last_updated"] = event.timestamp
            spot["received_at"] = arrival
        if is_transition:
            self.transitions_received += 1

        latency_ms = max(0.0, (arrival - event.timestamp) * 1000)
        self._latency_ms.append(latency_ms)

        virtual_time = event.timestamp - self.epoch
        if virtual_time >= _WARMUP_S:
            self._post_warmup_ms.append(latency_ms)
        else:
            self.warmup_excluded += 1

        self._event_rows.append((event.spot_id, event.sequence, event.timestamp, arrival, latency_ms))
        self._event_buffer.append({"spot_id": event.spot_id, "state": state_val, "latency_ms": round(latency_ms, 2), "timestamp": arrival})
        self._snapshot_cache = None

    def open_run(self, engine, config_json: str = "") -> None:
        if engine is None:
            return
        session = make_session(engine)
        try:
            run = ScenarioRun(
                scenario_name=self.config.name,
                protocol=self.config.protocol,
                architecture=self.config.architecture,
                traffic_level=self.config.traffic_level,
                num_spots=self.config.num_spots,
                sim_duration_s=self.config.sim_duration_s,
                started_at=self._started_at,
                config_json=config_json
            )
            session.add(run)
            session.commit()
            self._run_id = run.id
            logger.info(f"[DB] Opened scenario_run id={self._run_id} for '{self.config.name}'")
        finally:
            session.close()

    def flush_to_db(self, engine, metrics) -> None:
        if engine is None or self._run_id is None:
            return
        session = make_session(engine)
        try:
            logger.info(
                f"[DB] Flushing run {self._run_id}: "
                f"{len(self._event_rows)} latency records, {len(self._spots)} spots ..."
            )
            CHUNK = 1000
            proto = self.config.protocol
            arch = self.config.architecture
            for i in range(0, len(self._event_rows), CHUNK):
                chunk = self._event_rows[i: i + CHUNK]
                session.execute(
                    sa_insert(LatencyRecord),
                    [{"run_id": self._run_id, "spot_id": row[0], "sequence": row[1],
                      "protocol": proto, "architecture": arch, "sent_at": row[2],
                      "received_at": row[3], "latency_ms": round(row[4], 4)}
                     for row in chunk]
                )
            session.flush()

            session.execute(
                sa_insert(ParkingSpot),
                [{"run_id": self._run_id, "spot_id": sid, "state": s["state"],
                  "last_updated": s["last_updated"], "received_at": s["received_at"]}
                 for sid, s in self._spots.items()]
            )
            session.flush()

            run = session.get(ScenarioRun, self._run_id)
            if run is not None:
                run.completed_at = datetime.now(timezone.utc)
                run.latency_mean_ms = metrics.latency_mean_ms
                run.latency_p50_ms = metrics.latency_p50_ms
                run.latency_p95_ms = metrics.latency_p95_ms
                run.latency_p99_ms = metrics.latency_p99_ms
                run.latency_min_ms = metrics.latency_min_ms
                run.latency_max_ms = metrics.latency_max_ms
                run.sensor_to_edge_msgs = metrics.sensor_to_edge_msgs
                run.edge_to_cloud_msgs = metrics.edge_to_cloud_msgs
                run.sensor_to_edge_delivery_ratio = metrics.sensor_to_edge_delivery_ratio
                run.edge_to_cloud_delivery_ratio = metrics.edge_to_cloud_delivery_ratio
                run.end_to_end_delivery_ratio = metrics.end_to_end_delivery_ratio
                run.aggregation_ratio = metrics.aggregation_ratio
                run.filtered_events = metrics.filtered_events
                run.anomalies_detected = metrics.anomalies_detected
                run.adaptive_mode_switches = metrics.adaptive_mode_switches
                run.edge_cpu_pct = metrics.edge_cpu_pct
                run.edge_mem_mb = metrics.edge_mem_mb
                run.cloud_cpu_pct = metrics.cloud_cpu_pct
                run.cloud_mem_mb = metrics.cloud_mem_mb
                run.broker_overhead_score = metrics.broker_overhead_score

            session.commit()
            logger.info(f"[DB] Run {self._run_id} committed.")
        except Exception:
            session.rollback()
            logger.exception(f"[DB] Flush failed for run {self._run_id}")
            raise
        finally:
            session.close()

    def sample_cpu(self) -> None:
        try:
            v = self._process.cpu_percent(interval=None)
            if v > 0.0 or self._cpu_samples:
                self._cpu_samples.append(v)
        except Exception:
            pass

    def get_occupancy(self) -> dict:
        total = len(self._spots)
        occupied = sum(1 for s in self._spots.values() if s["state"] == "occupied")
        return {"total": total, "occupied": occupied, "free": total - occupied, "occupancy_pct": round(occupied / total * 100, 1) if total else 0}

    def get_latest_events(self, limit: int = 50) -> list[dict]:
        return list(self._event_buffer)[-limit:]

    def get_metrics_snapshot(self, include_cpu: bool = True) -> dict:
        samples = self._latency_ms
        current_len = len(samples)

        if self._snapshot_cache is not None and current_len == self._snapshot_cache_len:
            return self._snapshot_cache

        if samples:
            arr = np.array(samples)
            mean = float(np.mean(arr))
            p50 = float(np.percentile(arr, 50))
            p95 = float(np.percentile(arr, 95))
            p99 = float(np.percentile(arr, 99))
            mn = float(np.min(arr))
            mx = float(np.max(arr))
        else:
            mean = p50 = p95 = p99 = mn = mx = 0.0

        if include_cpu:
            if self._cpu_samples:
                cpu = sum(self._cpu_samples) / len(self._cpu_samples)
            else:
                now = time.monotonic()
                elapsed = now - self._last_cpu_time
                cpu = self._process.cpu_percent(interval=0.1 if elapsed < 0.5 else None)
                self._last_cpu_time = time.monotonic()
        else:
            cpu = (sum(self._cpu_samples) / len(self._cpu_samples)) if self._cpu_samples else -1.0
        mem = self._process.memory_info().rss / (1024 * 1024)

        snapshot = {
            "received_batches": self.received_batches,
            "received_events": self.received_events,
            "transitions_received": self.transitions_received,
            "total_bytes_received": self._total_bytes_received,
            "latency_mean_ms": round(mean, 2),
            "latency_p50_ms": round(p50, 2),
            "latency_p95_ms": round(p95, 2),
            "latency_p99_ms": round(p99, 2),
            "latency_min_ms": round(mn, 2),
            "latency_max_ms": round(mx, 2),
            "cpu_pct": round(cpu, 1),
            "mem_mb": round(mem, 2),
            "latency_samples": [round(v, 2) for v in samples[-200:]]
        }

        self._snapshot_cache = snapshot
        self._snapshot_cache_len = current_len
        return snapshot

    def get_all_latency_samples(self) -> list[float]:
        return self._post_warmup_ms if self._post_warmup_ms else self._latency_ms

    def compute_broker_overhead_score(self) -> float:
        cfg = self.config
        proto = cfg.protocol
        if proto == "mqtt":
            key = f"mqtt_qos{cfg.mqtt.qos}"
        elif proto == "amqp":
            key = f"amqp_{cfg.amqp.exchange_type}/{cfg.amqp.ack_mode}"
        elif proto == "coap":
            key = f"coap_{cfg.coap.mode}"
        else:
            key = ""
        mu = _BROKER_SERVICE_RATE.get(key, 20_000.0)
        lam = self.received_events / max(cfg.sim_duration_s, 1.0)
        rho = min(lam / mu, 0.999)
        e_w_ms = 1000.0 / (mu * (1.0 - rho))
        return round(e_w_ms, 6)