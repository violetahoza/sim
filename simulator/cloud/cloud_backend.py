from __future__ import annotations
import logging
from datetime import datetime, timezone
from sqlalchemy import insert as sa_insert

from .db import LatencyRecord, ParkingSpot, ScenarioRun, make_session
from ..models.models import BatchUpdate, ParkingEvent, SpotState
from ..config.config import ScenarioConfig
from ..des.engine import SimClock

logger = logging.getLogger(__name__)


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

        self._latency_state_change_ms: list[float] = []
        self._applied_ids: set[tuple[int, int]] = set()
        self._max_ts: dict[int, float] = {} 
        self.duplicate_events_at_cloud: int = 0
        self._event_rows: list[tuple] = []

        self._total_bytes_received = 0

        self._run_id: int | None = None
        self._started_at: datetime = datetime.now(timezone.utc)


    def receive_batch(self, batch: BatchUpdate, raw_bytes: bytes) -> None:
        arrival = self.epoch + self.clock.now
        self.received_batches += 1
        self._total_bytes_received += len(raw_bytes)
        for event in batch.events:
            self._process_event(event, arrival)

    def _process_event(self, event: ParkingEvent, arrival: float) -> None:
        self.received_events += 1

        key = (event.spot_id, event.sequence)
        latency_ms = (arrival - event.timestamp) * 1000

        if key in self._applied_ids:
            self.duplicate_events_at_cloud += 1
            self._event_rows.append((event.spot_id, event.sequence, event.timestamp, arrival, latency_ms))
            return

        self._applied_ids.add(key)

        prev_ts = self._max_ts.get(event.spot_id)
        is_fresh = prev_ts is None or event.timestamp >= prev_ts
        if is_fresh:
            self._max_ts[event.spot_id] = event.timestamp

        state_val = (event.state.value if isinstance(event.state, SpotState) else str(event.state))
        is_real = (not event.is_initial) and (not event.is_heartbeat_event)

        applied_state_change = False
        spot = self._spots.get(event.spot_id)
        if spot is not None:
            prev_state = spot["state"]
            spot["state"] = state_val
            spot["last_updated"] = event.timestamp
            spot["received_at"] = arrival
            if is_real and prev_state != state_val:
                self.transitions_received += 1
                applied_state_change = True

        if applied_state_change and is_fresh:
            self._latency_state_change_ms.append(latency_ms)

        self._event_rows.append((event.spot_id, event.sequence, event.timestamp, arrival, latency_ms))

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
                    [{"run_id": self._run_id, "spot_id": row[0], "sequence": row[1], "protocol": proto, "architecture": arch, "sent_at": row[2],
                      "received_at": row[3], "latency_ms": round(row[4], 4)}
                     for row in chunk]
                )
            session.flush()

            session.execute(
                sa_insert(ParkingSpot),
                [{"run_id": self._run_id, "spot_id": sid, "state": s["state"], "last_updated": s["last_updated"], "received_at": s["received_at"]}
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
                run.edge_to_cloud_delivery_ratio = metrics.backhaul_delivery_ratio
                run.end_to_end_delivery_ratio = metrics.e2e_unique_delivery_ratio
                run.aggregation_ratio = metrics.aggregation_ratio
                run.filtered_events = metrics.filtered_events
                run.anomalies_detected = metrics.anomalies_detected
                run.adaptive_mode_switches = metrics.adaptive_mode_switches

            session.commit()
            logger.info(f"[DB] Run {self._run_id} committed.")
        except Exception:
            session.rollback()
            logger.exception(f"[DB] Flush failed for run {self._run_id}")
            raise
        finally:
            session.close()

    def get_occupancy(self) -> dict:
        total = len(self._spots)
        occupied = sum(1 for s in self._spots.values() if s["state"] == "occupied")
        return {"total": total, "occupied": occupied, "free": total - occupied, "occupancy_pct": round(occupied / total * 100, 1) if total else 0}

    def compute_state_agreement(self, ground_truth: dict[int, str]) -> float:
        if not ground_truth:
            return 1.0
        match = sum(
            1 for sid, true_state in ground_truth.items()
            if (self._spots.get(sid) or {}).get("state") == true_state
        )
        return match / len(ground_truth)

    def get_all_latency_samples(self) -> list[float]:
        return self._latency_state_change_ms