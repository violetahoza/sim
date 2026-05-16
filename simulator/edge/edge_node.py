from __future__ import annotations
import json
import logging
from typing import Callable, Optional
import psutil

from ..models import ParkingEvent, BatchUpdate, SensorState, LinkStats
from ..config import ScenarioConfig
from ..des.engine import SimClock

logger = logging.getLogger(__name__)

CloudForwardCallback = Callable[[BatchUpdate, bytes], None]

_RAPID_ARRIVAL_S: float = 0.5
_MIN_DWELL_S: float = 30.0
_QUARANTINE_THRESHOLD: int = 3
_RELEASE_CLEAN_TICKS: int = 5


class EdgeNode:

    ANOMALY_INTERVAL_S = 30.0
    ADAPTIVE_DEGRADE_THRESHOLD = 0.85
    ADAPTIVE_RECOVER_THRESHOLD = 0.95

    def __init__(self, config: ScenarioConfig, clock: SimClock, cloud_cb: CloudForwardCallback, epoch: float) -> None:
        self.config = config
        self.edge_cfg = config.edge
        self.clock = clock
        self._cloud_cb = cloud_cb
        self._epoch = epoch
        self.edge_id = "edge_01"

        self._active_arch = config.architecture
        self._cache: dict[int, SensorState] = {}
        self._pending: list[ParkingEvent] = []
        self.stats = LinkStats(name="edge_to_cloud")
        self.received_count = 0
        self.filtered_count = 0
        self.anomaly_count = 0
        self.forwarded_events = 0
        self.mode_switches = 0
        self._process = psutil.Process()
        self._process.cpu_percent()
        self._sensor_link_stats: Optional[LinkStats] = None

        self._anomaly_flags: dict[int, int] = {}
        self._cumulative_flags: dict[int, int] = {}
        self._quarantine: set[int] = set()
        self._quarantine_clean_ticks: dict[int, int] = {}
        self._anomaly_log: list[dict] = []
        self._last_arrival_virtual: dict[int, float] = {}
        self._reported_silent: set[int] = set()
        self._reported_stuck: set[int] = set()

        if self._active_arch == "edge_aggregated":
            self.clock.schedule(self.edge_cfg.aggregation_interval_s, self._aggregation_tick)
        if self.edge_cfg.anomaly_detection:
            self.clock.schedule(self.ANOMALY_INTERVAL_S, self._anomaly_tick)

    def set_sensor_link_stats(self, stats: LinkStats) -> None:
        self._sensor_link_stats = stats

    def receive(self, event: ParkingEvent, raw_bytes: bytes) -> None:
        self.received_count += 1
        cached = self._cache.get(event.spot_id)
        if cached is None:
            cached = SensorState(spot_id=event.spot_id)
            self._cache[event.spot_id] = cached

        now_virtual = event.timestamp - self._epoch

        if self.edge_cfg.anomaly_detection:
            self._check_r3_r4(event, cached, now_virtual)

        if self.edge_cfg.filter_no_change and cached.state == event.state:
            self.filtered_count += 1
            cached.consecutive_same += 1
            return

        if self.edge_cfg.anomaly_detection:
            self._check_r5(event, cached, now_virtual)

        cached.state = event.state
        cached.last_updated = event.timestamp
        cached.last_event_seq = event.sequence
        cached.consecutive_same = 0
        cached.total_events += 1

        if event.spot_id in self._quarantine:
            return  

        if self._active_arch == "edge_filtered":
            self._forward_single(event)
        elif self._active_arch == "edge_aggregated":
            self._pending.append(event)

    def record_cloud_drop(self) -> None:
        self.stats.dropped += 1

    def flush_final(self) -> None:
        if self._active_arch == "edge_aggregated" and self._pending:
            self._flush_batch()

    def _check_r3_r4(self, event: ParkingEvent, cached: SensorState, now_virtual: float) -> None:
        sid = event.spot_id

        last_arr = self._last_arrival_virtual.get(sid)
        if last_arr is not None and (now_virtual - last_arr) < _RAPID_ARRIVAL_S:
            self._flag_anomaly(sid, "R3_rapid_arrival", now_virtual)
        self._last_arrival_virtual[sid] = now_virtual

        if cached.last_event_seq > 0 and event.sequence <= cached.last_event_seq:
            self._flag_anomaly(sid, "R4_stale_seq", now_virtual)

    def _check_r5(self, event: ParkingEvent, cached: SensorState, now_virtual: float) -> None:
        if cached.last_updated > 0.0:
            last_virtual = cached.last_updated - self._epoch
            if (now_virtual - last_virtual) < _MIN_DWELL_S:
                self._flag_anomaly(event.spot_id, "R5_rapid_state_flip", now_virtual)

    def _flag_anomaly(self, spot_id: int, rule: str, virtual_time: float) -> None:
        self._anomaly_flags[spot_id] = self._anomaly_flags.get(spot_id, 0) + 1
        self._cumulative_flags[spot_id] = self._cumulative_flags.get(spot_id, 0) + 1
        self.anomaly_count += 1
        if len(self._anomaly_log) < 10_000: 
            self._anomaly_log.append({"spot_id": spot_id, "rule": rule, "virtual_time": round(virtual_time, 2)})

        if (self._cumulative_flags[spot_id] >= _QUARANTINE_THRESHOLD and spot_id not in self._quarantine):
            self._quarantine.add(spot_id)
            logger.warning(
                f"[QUARANTINE] Spot {spot_id} quarantined "
                f"(cumulative_flags={self._cumulative_flags[spot_id]})"
            )

    def _check_anomalies(self) -> None:
        now_virtual = self.clock.now
        stuck_thr = self.edge_cfg.stuck_threshold
        silent_thr_s = self.edge_cfg.silent_threshold_s

        for spot_id, state in self._cache.items():
            if state.consecutive_same > stuck_thr:
                if spot_id not in self._reported_stuck:
                    logger.warning(
                        f"[ANOMALY] R1 Spot {spot_id} stuck: "
                        f"consecutive_same={state.consecutive_same}"
                    )
                    self._reported_stuck.add(spot_id)
                self._flag_anomaly(spot_id, "R1_stuck_at", now_virtual)
            else:
                self._reported_stuck.discard(spot_id)

            if state.last_updated == 0.0:
                continue
            last_virtual = state.last_updated - self._epoch
            if (now_virtual - last_virtual) > silent_thr_s:
                if spot_id not in self._reported_silent:
                    logger.warning(
                        f"[ANOMALY] R2 Spot {spot_id} silent for "
                        f"{now_virtual - last_virtual:.0f}s"
                    )
                    self._reported_silent.add(spot_id)
                self._flag_anomaly(spot_id, "R2_silent", now_virtual)


    def _forward_single(self, event: ParkingEvent) -> None:
        batch = BatchUpdate(edge_id=self.edge_id, events=[event])
        payload = self._serialize_batch(batch)
        self.stats.sent += 1
        self.stats.total_bytes_sent += len(payload)
        self.forwarded_events += 1
        self._cloud_cb(batch, payload)

    def _flush_batch(self) -> None:
        if not self._pending:
            return
        events = list(self._pending)
        self._pending.clear()
        batch = BatchUpdate(edge_id=self.edge_id, events=events)
        payload = self._serialize_batch(batch)
        self.stats.sent += 1
        self.stats.total_bytes_sent += len(payload)
        self.forwarded_events += len(events)
        self._cloud_cb(batch, payload)

    def _aggregation_tick(self) -> None:
        self._flush_batch()
        self.clock.schedule(self.edge_cfg.aggregation_interval_s, self._aggregation_tick)

    def _anomaly_tick(self) -> None:
        self._check_anomalies()  # R1, R2
        if self.edge_cfg.adaptive_edge:
            self._check_adaptive_mode()

        for spot_id in list(self._quarantine):
            if self._anomaly_flags.get(spot_id, 0) == 0:
                ticks = self._quarantine_clean_ticks.get(spot_id, 0) + 1
                self._quarantine_clean_ticks[spot_id] = ticks
                if ticks >= _RELEASE_CLEAN_TICKS:
                    self._quarantine.discard(spot_id)
                    self._quarantine_clean_ticks.pop(spot_id, None)
                    logger.info(f"[QUARANTINE] Spot {spot_id} released after {ticks} clean ticks")
            else:
                self._quarantine_clean_ticks[spot_id] = 0

        self._anomaly_flags.clear()
        self.clock.schedule(self.ANOMALY_INTERVAL_S, self._anomaly_tick)

    def _check_adaptive_mode(self) -> None:
        if self.config.architecture != "edge_aggregated":
            return
        if self._sensor_link_stats is None or self._sensor_link_stats.sent == 0:
            return

        dr = self._sensor_link_stats.delivery_ratio

        if self._active_arch == "edge_aggregated" and dr < self.ADAPTIVE_DEGRADE_THRESHOLD:
            logger.info(
                f"[ADAPTIVE] DR={dr:.2%} < {self.ADAPTIVE_DEGRADE_THRESHOLD:.0%} "
                f"— edge_aggregated → edge_filtered"
            )
            self._flush_batch()
            self._active_arch = "edge_filtered"
            self.mode_switches += 1

        elif self._active_arch == "edge_filtered" and dr >= self.ADAPTIVE_RECOVER_THRESHOLD:
            logger.info(
                f"[ADAPTIVE] DR={dr:.2%} >= {self.ADAPTIVE_RECOVER_THRESHOLD:.0%} "
                f"— edge_filtered → edge_aggregated"
            )
            self._active_arch = "edge_aggregated"
            self.mode_switches += 1

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
            "mode_switches": self.mode_switches,
            "active_arch": self._active_arch,
            "link_stats": self.stats.to_dict(),
            "quarantined": sorted(self._quarantine),
            "quarantined_count": len(self._quarantine),
            "detected_spots": len(self._cumulative_flags),
            "anomaly_log": self._anomaly_log[-200:],
            **res
        }
