from __future__ import annotations

import collections
import logging
import time
from datetime import datetime, timezone

import numpy as np
import psutil

from ..models import BatchUpdate, ParkingEvent, SpotState
from ..config import ScenarioConfig
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
        self._latency_ms: list[float] = []
        self._event_rows: list[tuple] = []
        self._total_bytes_received = 0
        self._process = psutil.Process()
        self._process.cpu_percent()
        self._event_buffer: collections.deque = collections.deque(maxlen=50_000)
        self._agg_interval = config.edge.aggregation_interval_s
        self._lat_buckets: dict[int, list[float]] = collections.defaultdict(list)

        self._run_id: int | None = None
        self._started_at: datetime = datetime.now(timezone.utc)


    def receive_batch(self, batch: BatchUpdate, raw_bytes: bytes) -> None:
        arrival_virtual = self.clock.now
        arrival_epoch = self.epoch + arrival_virtual
        self.received_batches += 1
        self._total_bytes_received += len(raw_bytes)
        for event in batch.events:
            self._process_event(event, arrival_virtual, arrival_epoch)

    def _process_event(
        self, event: ParkingEvent, arrival_virtual: float, arrival_epoch: float
    ) -> None:
        self.received_events += 1
        state_val = (
            event.state.value if isinstance(event.state, SpotState) else str(event.state)
        )
        spot = self._spots.get(event.spot_id)
        if spot is not None:
            spot["state"] = state_val
            spot["last_updated"] = event.timestamp
            spot["received_at"] = arrival_epoch

        latency_ms = (arrival_epoch - event.timestamp) * 1000
        self._latency_ms.append(latency_ms)

        self._event_rows.append(
            (event.spot_id, event.sequence, event.timestamp, arrival_epoch, latency_ms)
        )

        self._event_buffer.append({
            "spot_id": event.spot_id,
            "state": state_val,
            "latency_ms": round(latency_ms, 2),
            "timestamp": arrival_epoch,
        })

        bucket = int(arrival_virtual / self._agg_interval)
        self._lat_buckets[bucket].append(latency_ms)

    # ── DB persistence (called once at end of run) ────────────────────────────

    def open_run(self, engine) -> None:
        """Insert the scenario_runs row and store the run_id for later."""
        if engine is None:
            return
        from ..db import ScenarioRun, make_session
        session = make_session(engine)
        try:
            run = ScenarioRun(
                scenario_name  = self.config.name,
                protocol       = self.config.protocol,
                architecture   = self.config.architecture,
                traffic_level  = self.config.traffic_level,
                num_spots      = self.config.num_spots,
                sim_duration_s = self.config.sim_duration_s,
                started_at     = self._started_at,
            )
            session.add(run)
            session.commit()
            self._run_id = run.id
            logger.info(f"[DB] Opened scenario_run id={self._run_id} for '{self.config.name}'")
        finally:
            session.close()

    def flush_to_db(self, engine, metrics) -> None:
        """
        Bulk-insert latency_records and parking_spots, then update the
        scenario_runs row with final metrics.  Called after the DES finishes.
        """
        if engine is None or self._run_id is None:
            return

        from sqlalchemy import insert as sa_insert
        from ..db import LatencyRecord, ParkingSpot, ScenarioRun, make_session

        session = make_session(engine)
        try:
            logger.info(
                f"[DB] Flushing run {self._run_id}: "
                f"{len(self._event_rows)} latency records, "
                f"{len(self._spots)} spots …"
            )

            CHUNK = 1000
            proto = self.config.protocol
            arch  = self.config.architecture
            for i in range(0, len(self._event_rows), CHUNK):
                chunk = self._event_rows[i : i + CHUNK]
                session.execute(
                    sa_insert(LatencyRecord),
                    [
                        {
                            "run_id":       self._run_id,
                            "spot_id":      row[0],
                            "sequence":     row[1],
                            "protocol":     proto,
                            "architecture": arch,
                            "sent_at":      row[2],
                            "received_at":  row[3],
                            "latency_ms":   round(row[4], 4),
                        }
                        for row in chunk
                    ],
                )
            session.flush()

            # ── parking_spots (final state per spot) ──────────────────────────
            session.execute(
                sa_insert(ParkingSpot),
                [
                    {
                        "run_id":       self._run_id,
                        "spot_id":      sid,
                        "state":        s["state"],
                        "last_updated": s["last_updated"],
                        "received_at":  s["received_at"],
                    }
                    for sid, s in self._spots.items()
                ],
            )
            session.flush()

            run = session.get(ScenarioRun, self._run_id)
            if run is not None:
                run.completed_at                  = datetime.now(timezone.utc)
                run.latency_mean_ms               = metrics.latency_mean_ms
                run.latency_p50_ms                = metrics.latency_p50_ms
                run.latency_p95_ms                = metrics.latency_p95_ms
                run.latency_p99_ms                = metrics.latency_p99_ms
                run.latency_min_ms                = metrics.latency_min_ms
                run.latency_max_ms                = metrics.latency_max_ms
                run.sensor_to_edge_msgs           = metrics.sensor_to_edge_msgs
                run.edge_to_cloud_msgs            = metrics.edge_to_cloud_msgs
                run.sensor_to_edge_delivery_ratio = metrics.sensor_to_edge_delivery_ratio
                run.edge_to_cloud_delivery_ratio  = metrics.edge_to_cloud_delivery_ratio
                run.aggregation_ratio             = metrics.aggregation_ratio
                run.filtered_events               = metrics.filtered_events
                run.anomalies_detected            = metrics.anomalies_detected
                run.edge_cpu_pct                  = metrics.edge_cpu_pct
                run.edge_mem_mb                   = metrics.edge_mem_mb
                run.cloud_cpu_pct                 = metrics.cloud_cpu_pct
                run.cloud_mem_mb                  = metrics.cloud_mem_mb

            session.commit()
            logger.info(f"[DB] Run {self._run_id} committed.")
        except Exception:
            session.rollback()
            logger.exception(f"[DB] Flush failed for run {self._run_id}")
            raise
        finally:
            session.close()


    def get_latency_timeseries(self) -> list[dict]:
        return [
            {
                "t_s": bucket * self._agg_interval,
                "mean_ms": round(sum(lats) / len(lats), 2),
            }
            for bucket, lats in sorted(self._lat_buckets.items())
            if lats
        ]

    def get_occupancy(self) -> dict:
        total = len(self._spots)
        occupied = sum(1 for s in self._spots.values() if s["state"] == "occupied")
        return {
            "total": total,
            "occupied": occupied,
            "free": total - occupied,
            "occupancy_pct": round(occupied / total * 100, 1) if total else 0,
        }

    def get_latest_events(self, limit: int = 50) -> list[dict]:
        return list(self._event_buffer)[-limit:]

    def get_metrics_snapshot(self) -> dict:
        samples = self._latency_ms
        if samples:
            arr = np.array(samples)
            mean = float(np.mean(arr))
            p50  = float(np.percentile(arr, 50))
            p95  = float(np.percentile(arr, 95))
            p99  = float(np.percentile(arr, 99))
            mn   = float(np.min(arr))
            mx   = float(np.max(arr))
        else:
            mean = p50 = p95 = p99 = mn = mx = 0.0

        cpu = self._process.cpu_percent()
        mem = self._process.memory_info().rss / (1024 * 1024)

        return {
            "received_batches":  self.received_batches,
            "received_events":   self.received_events,
            "total_bytes_received": self._total_bytes_received,
            "latency_mean_ms":   round(mean, 2),
            "latency_p50_ms":    round(p50, 2),
            "latency_p95_ms":    round(p95, 2),
            "latency_p99_ms":    round(p99, 2),
            "latency_min_ms":    round(mn, 2),
            "latency_max_ms":    round(mx, 2),
            "cpu_pct":           cpu,
            "mem_mb":            round(mem, 2),
            "latency_samples":   [round(v, 2) for v in samples[-200:]],
        }

    def get_all_latency_samples(self) -> list[float]:
        return self._latency_ms