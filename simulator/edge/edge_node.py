from __future__ import annotations
import json
import logging
from typing import Callable, Optional

import psutil

from ..models import ParkingEvent, BatchUpdate, SensorState, SpotState, LinkStats
from ..config import EdgeConfig, ScenarioConfig
from ..des.engine import SimClock

logger = logging.getLogger(__name__)

CloudForwardCallback = Callable[[BatchUpdate, bytes], None]


class EdgeNode:
    STUCK_THRESHOLD = 10
    ANOMALY_INTERVAL_S = 30.0

    def __init__(
        self,
        config: ScenarioConfig,
        clock: SimClock,
        cloud_cb: CloudForwardCallback,
        epoch: float,
    ) -> None:
        self.config = config
        self.edge_cfg = config.edge
        self.clock = clock
        self._cloud_cb = cloud_cb
        self._epoch = epoch
        self.edge_id = "edge_01"
        self._cache: dict[int, SensorState] = {}
        self._pending: list[ParkingEvent] = []
        self.stats = LinkStats(name="edge_to_cloud")
        self.received_count = 0
        self.filtered_count = 0
        self.anomaly_count = 0
        self.forwarded_events = 0
        self._process = psutil.Process()
        self._process.cpu_percent()
        self._ui_update_cb: Optional[Callable] = None

        if self.edge_cfg.architecture == "edge_aggregated":
            self.clock.schedule(self.edge_cfg.aggregation_interval_s, self._aggregation_tick)
        if self.edge_cfg.anomaly_detection:
            self.clock.schedule(self.ANOMALY_INTERVAL_S, self._anomaly_tick)

    def set_ui_callback(self, cb: Callable) -> None:
        self._ui_update_cb = cb

    def receive(self, event: ParkingEvent, raw_bytes: bytes) -> None:
        self.received_count += 1
        cached = self._cache.get(event.spot_id)
        if cached is None:
            cached = SensorState(spot_id=event.spot_id)
            self._cache[event.spot_id] = cached

        if self.edge_cfg.filter_no_change and cached.state == event.state:
            self.filtered_count += 1
            cached.consecutive_same += 1
            return

        cached.state = event.state
        cached.last_updated = event.timestamp
        cached.last_event_seq = event.sequence
        cached.consecutive_same = 0
        cached.total_events += 1

        if self.edge_cfg.architecture == "edge_filtered":
            self._forward_single(event)
        elif self.edge_cfg.architecture == "edge_aggregated":
            self._pending.append(event)

        if self._ui_update_cb:
            self._ui_update_cb(event)

    def flush_final(self) -> None:
        if self.edge_cfg.architecture == "edge_aggregated" and self._pending:
            self._flush_batch()

    def _forward_single(self, event: ParkingEvent) -> None:
        batch = BatchUpdate(edge_id=self.edge_id, events=[event])
        payload = self._serialize_batch(batch)
        batch.bytes_size = len(payload)
        self.stats.sent += 1
        self.stats.total_bytes_sent += len(payload)
        self.forwarded_events += 1
        self._cloud_cb(batch, payload)
        self.stats.received += 1
        self.stats.total_bytes_received += len(payload)

    def _flush_batch(self) -> None:
        if not self._pending:
            return
        events = list(self._pending)
        self._pending.clear()
        batch = BatchUpdate(edge_id=self.edge_id, events=events)
        payload = self._serialize_batch(batch)
        batch.bytes_size = len(payload)
        self.stats.sent += 1
        self.stats.total_bytes_sent += len(payload)
        self.forwarded_events += len(events)
        self._cloud_cb(batch, payload)
        self.stats.received += 1
        self.stats.total_bytes_received += len(payload)

    def _aggregation_tick(self) -> None:
        self._flush_batch()
        self.clock.schedule(self.edge_cfg.aggregation_interval_s, self._aggregation_tick)

    def _anomaly_tick(self) -> None:
        self._check_anomalies()
        self.clock.schedule(self.ANOMALY_INTERVAL_S, self._anomaly_tick)

    def _check_anomalies(self) -> None:
        now_virtual = self.clock.now
        threshold = max(self.edge_cfg.cache_ttl_s * 6, 5400.0)
        for spot_id, state in self._cache.items():
            last_virtual = state.last_updated - self._epoch
            silent_s = now_virtual - last_virtual
            if silent_s > threshold:
                if state.consecutive_same == 0:
                    logger.warning(
                        f"[ANOMALY] Sensor {spot_id} silent for {silent_s:.0f}s "
                        f"(threshold={threshold:.0f}s)"
                    )
                self.anomaly_count += 1
            if state.consecutive_same > self.STUCK_THRESHOLD:
                logger.warning(
                    f"[ANOMALY] Sensor {spot_id} consecutive_same={state.consecutive_same}"
                )
                self.anomaly_count += 1

    @staticmethod
    def _serialize_batch(batch: BatchUpdate) -> bytes:
        return json.dumps(batch.to_dict()).encode()

    def resource_usage(self) -> dict:
        cpu = self._process.cpu_percent()  
        mem = self._process.memory_info().rss / (1024 * 1024)
        return {"cpu_pct": cpu, "mem_mb": round(mem, 2)}

    def summary(self) -> dict:
        res = self.resource_usage()
        return {
            "received": self.received_count,
            "filtered": self.filtered_count,
            "forwarded_events": self.forwarded_events,
            "anomalies": self.anomaly_count,
            "link_stats": self.stats.to_dict(),
            **res,
        }
