from __future__ import annotations
import collections
import time
import psutil
import numpy as np

from ..models import BatchUpdate, ParkingEvent, SpotState, LatencyRecord
from ..config import ScenarioConfig
from ..des.engine import SimClock


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
        self._latency_records: list[LatencyRecord] = []
        self._total_bytes_received = 0
        self._process = psutil.Process()
        self._process.cpu_percent()
        self._event_buffer: collections.deque = collections.deque(maxlen=50_000)
        self._agg_interval = config.edge.aggregation_interval_s
        self._lat_buckets: dict[int, list[float]] = collections.defaultdict(list)

    def receive_batch(self, batch: BatchUpdate, raw_bytes: bytes) -> None:
        arrival_virtual = self.clock.now
        arrival_epoch = self.epoch + arrival_virtual
        self.received_batches += 1
        self._total_bytes_received += len(raw_bytes)
        for event in batch.events:
            self._process_event(event, arrival_virtual, arrival_epoch)

    def _process_event(self, event: ParkingEvent, arrival_virtual: float, arrival_epoch: float) -> None:
        self.received_events += 1
        state_val = event.state.value if isinstance(event.state, SpotState) else str(event.state)
        spot = self._spots.get(event.spot_id)
        if spot is not None:
            spot["state"] = state_val
            spot["last_updated"] = event.timestamp
            spot["received_at"] = arrival_epoch

        latency_ms = (arrival_epoch - event.timestamp) * 1000

        self._latency_records.append(LatencyRecord(
            event_id=f"{event.sensor_id}_{event.sequence}",
            protocol=self.config.protocol,
            sent_at=event.timestamp,
            received_at=arrival_epoch,
            latency_ms=latency_ms,
            architecture=self.config.architecture,
        ))

        self._event_buffer.append({
            "spot_id": event.spot_id,
            "state": state_val,
            "latency_ms": round(latency_ms, 2),
            "timestamp": arrival_epoch,
        })

        bucket = int(arrival_virtual / self._agg_interval)
        self._lat_buckets[bucket].append(latency_ms)

    def get_latency_timeseries(self) -> list[dict]:
        return [
            {"t_s": bucket * self._agg_interval, "mean_ms": round(sum(lats) / len(lats), 2)}
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

    def get_all_spots(self) -> list[dict]:
        return [{"spot_id": sid, **state} for sid, state in sorted(self._spots.items())]

    def get_latest_events(self, limit: int = 50) -> list[dict]:
        buf = list(self._event_buffer)
        return buf[-limit:]

    def get_metrics_snapshot(self) -> dict:
        records = self._latency_records
        if records:
            arr = np.array([r.latency_ms for r in records], dtype=float)
            mean = float(np.mean(arr))
            p50 = float(np.percentile(arr, 50))
            p95 = float(np.percentile(arr, 95))
            p99 = float(np.percentile(arr, 99))
            mn = float(np.min(arr))
            mx = float(np.max(arr))
        else:
            mean = p50 = p95 = p99 = mn = mx = 0.0

        cpu = self._process.cpu_percent()  
        mem = self._process.memory_info().rss / (1024 * 1024)

        return {
            "received_batches": self.received_batches,
            "received_events": self.received_events,
            "total_bytes_received": self._total_bytes_received,
            "latency_mean_ms": round(mean, 2),
            "latency_p50_ms": round(p50, 2),
            "latency_p95_ms": round(p95, 2),
            "latency_p99_ms": round(p99, 2),
            "latency_min_ms": round(mn, 2),
            "latency_max_ms": round(mx, 2),
            "cpu_pct": cpu,
            "mem_mb": round(mem, 2),
            "latency_samples": [round(r.latency_ms, 2) for r in records[-200:]],
        }

    def get_all_latency_samples(self) -> list[float]:
        return [r.latency_ms for r in self._latency_records]
