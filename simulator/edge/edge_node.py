from __future__ import annotations

import json
import logging
from typing import Callable, Optional

import psutil

from ..config import ScenarioConfig
from ..des.engine import SimClock
from ..models import BatchUpdate, LinkStats, ParkingEvent, SensorState

logger = logging.getLogger(__name__)

CloudForwardCallback = Callable[[BatchUpdate, bytes], None]

_RAPID_ARRIVAL_S: float = 0.5
_MIN_DWELL_S: float = 10.0
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
        self._process.cpu_percent(interval=None)
        self._sensor_link_stats: Optional[LinkStats] = None

        self._active_anomalies: set[tuple[int, str]] = set()
        self._resolved_anomalies: int = 0
        self._anomaly_flags: dict[int, int] = {}
        self._cumulative_flags: dict[int, int] = {}
        self._quarantine: set[int] = set()
        self._quarantine_clean_ticks: dict[int, int] = {}
        self._quarantine_valid_events: dict[int, int] = {}
        self._anomaly_log: list[dict] = []
        self._last_arrival_virtual: dict[int, float] = {}

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

        if event.is_initial and event.sequence > self.config.num_spots:
            cached.last_updated = event.timestamp
            self.filtered_count += 1
            return

        if self.edge_cfg.anomaly_detection:
            self._check_r3_r4(event, cached, now_virtual)

        if self._should_filter(event, cached):
            self.filtered_count += 1
            return

        if self.edge_cfg.anomaly_detection:
            self._check_r5(event, cached, now_virtual)

        previous_state = cached.state
        state_changed = previous_state != event.state

        cached.state = event.state
        cached.last_updated = event.timestamp

        if state_changed:
            cached.last_state_change_timestamp = event.timestamp
            cached.consecutive_same = 0
        else:
            cached.consecutive_same += 1

        cached.last_event_seq = max(cached.last_event_seq, event.sequence)
        cached.total_events += 1

        if self.edge_cfg.anomaly_detection:
            self._mark_valid_event_for_recovery(event.spot_id)

        if event.spot_id in self._quarantine and not state_changed:
            self.filtered_count += 1
            return

        if self._active_arch == "edge_filtered":
            self._forward_single(event)
            cached.last_forwarded_timestamp = event.timestamp
        elif self._active_arch == "edge_aggregated":
            self._pending.append(event)
            cached.last_forwarded_timestamp = event.timestamp
            self._flush_if_needed()

    def _should_filter(self, event: ParkingEvent, cached: SensorState) -> bool:
        if not self.edge_cfg.filter_no_change:
            return False

        same_state = cached.state == event.state
        stale_or_duplicate_sequence = cached.last_event_seq > 0 and event.sequence <= cached.last_event_seq
        if stale_or_duplicate_sequence:
            return True

        if not same_state:
            return False

        event_virtual = event.timestamp - self._epoch
        last_virtual = cached.last_updated - self._epoch if cached.last_updated else None
        recent_repeat = (last_virtual is not None and (event_virtual - last_virtual) <= self.edge_cfg.duplicate_window_s)
        if recent_repeat:
            return True

        heartbeat_due = (
            cached.last_forwarded_timestamp == 0.0
            or (event.timestamp - cached.last_forwarded_timestamp)
            >= self.edge_cfg.heartbeat_forward_interval_s
        )
        return not heartbeat_due

    def _flush_if_needed(self) -> None:
        if not self._pending:
            return
        if len(self._pending) >= self.edge_cfg.max_batch_size:
            self._flush_batch()
            return
        oldest_event_virtual = self._pending[0].timestamp - self._epoch
        oldest_age = self.clock.now - oldest_event_virtual
        if oldest_age >= self.edge_cfg.max_event_age_s:
            self._flush_batch()

    def record_cloud_drop(self) -> None:
        self.stats.dropped += 1

    def flush_final(self) -> None:
        if self._active_arch == "edge_aggregated" and self._pending:
            self._flush_batch()

    def _check_r3_r4(self, event: ParkingEvent, cached: SensorState, now_virtual: float) -> None:
        sid = event.spot_id

        if event.state.value == "occupied" and not event.is_initial:
            last_arr = self._last_arrival_virtual.get(sid)
            if last_arr is not None and (now_virtual - last_arr) < _RAPID_ARRIVAL_S:
                self._flag_anomaly(sid, "R3_rapid_arrival", now_virtual)
            else:
                self._resolve_anomaly(sid, "R3_rapid_arrival", now_virtual)
            self._last_arrival_virtual[sid] = now_virtual
        else:
            self._resolve_anomaly(sid, "R3_rapid_arrival", now_virtual)

        if cached.last_event_seq > 0 and event.sequence < cached.last_event_seq:
            self._flag_anomaly(sid, "R4_stale_seq", now_virtual)
        else:
            self._resolve_anomaly(sid, "R4_stale_seq", now_virtual)

    def _check_r5(self, event: ParkingEvent, cached: SensorState, now_virtual: float) -> None:
        """R5: impossibly short dwell between two state transitions."""
        if cached.last_state_change_timestamp > 0.0 and cached.state != event.state:
            last_change_virtual = cached.last_state_change_timestamp - self._epoch
            if (now_virtual - last_change_virtual) < _MIN_DWELL_S:
                self._flag_anomaly(event.spot_id, "R5_rapid_state_flip", now_virtual)
                return
        self._resolve_anomaly(event.spot_id, "R5_rapid_state_flip", now_virtual)

    def _check_anomalies(self) -> None:
        now_virtual = self.clock.now
        silent_thr_s = self.edge_cfg.silent_threshold_s

        stuck_thr_s = 12.0 * 3600.0

        for spot_id, state in self._cache.items():
            if state.last_state_change_timestamp > 0.0:
                last_change_virtual = state.last_state_change_timestamp - self._epoch
                same_state_age = now_virtual - last_change_virtual

                if state.state.value == "occupied" and same_state_age > stuck_thr_s:
                    self._flag_anomaly(spot_id, "R1_stuck_at", now_virtual)
                else:
                    self._resolve_anomaly(spot_id, "R1_stuck_at", now_virtual)
            else:
                self._resolve_anomaly(spot_id, "R1_stuck_at", now_virtual)

            if state.last_updated == 0.0:
                continue
            last_virtual = state.last_updated - self._epoch
            if (now_virtual - last_virtual) > silent_thr_s:
                self._flag_anomaly(spot_id, "R2_silent", now_virtual)
            else:
                self._resolve_anomaly(spot_id, "R2_silent", now_virtual)

    def _flag_anomaly(self, spot_id: int, rule: str, virtual_time: float) -> None:
        key = (spot_id, rule)
        if key in self._active_anomalies:
            return

        self._active_anomalies.add(key)
        self._anomaly_flags[spot_id] = self._anomaly_flags.get(spot_id, 0) + 1
        self._cumulative_flags[spot_id] = self._cumulative_flags.get(spot_id, 0) + 1
        self.anomaly_count += 1
        self._quarantine_valid_events[spot_id] = 0

        if len(self._anomaly_log) < 10_000:
            self._anomaly_log.append({"spot_id": spot_id, "rule": rule, "virtual_time": round(virtual_time, 2), "event": "started"})

        threshold = getattr(self.edge_cfg, "quarantine_threshold", 3)
        if self._cumulative_flags[spot_id] >= threshold and spot_id not in self._quarantine:
            self._quarantine.add(spot_id)
            self._quarantine_clean_ticks[spot_id] = 0
            logger.warning(
                f"[QUARANTINE] Spot {spot_id} quarantined "
                f"(cumulative_flags={self._cumulative_flags[spot_id]})"
            )

    def _resolve_anomaly(self, spot_id: int, rule: str, virtual_time: float) -> None:
        key = (spot_id, rule)
        if key not in self._active_anomalies:
            return

        self._active_anomalies.remove(key)
        self._resolved_anomalies += 1
        if len(self._anomaly_log) < 10_000:
            self._anomaly_log.append({"spot_id": spot_id, "rule": rule, "virtual_time": round(virtual_time, 2), "event": "resolved"})

    def _mark_valid_event_for_recovery(self, spot_id: int) -> None:
        if spot_id not in self._quarantine:
            return
        if any(sid == spot_id for sid, _rule in self._active_anomalies):
            self._quarantine_valid_events[spot_id] = 0
            return

        valid = self._quarantine_valid_events.get(spot_id, 0) + 1
        self._quarantine_valid_events[spot_id] = valid
        required = getattr(self.edge_cfg, "quarantine_recovery_events", 3)
        if valid >= required:
            self._release_quarantine(spot_id, reason=f"{valid} valid events")

    def _release_quarantine(self, spot_id: int, reason: str) -> None:
        self._quarantine.discard(spot_id)
        self._quarantine_clean_ticks.pop(spot_id, None)
        self._quarantine_valid_events.pop(spot_id, None)
        self._cumulative_flags[spot_id] = 0
        for key in list(self._active_anomalies):
            if key[0] == spot_id:
                self._active_anomalies.remove(key)
                self._resolved_anomalies += 1
        logger.info(f"[QUARANTINE] Spot {spot_id} released after {reason}")

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
        self._check_anomalies()
        if self.edge_cfg.adaptive_edge:
            self._check_adaptive_mode()

        for spot_id in list(self._quarantine):
            if not any(sid == spot_id for sid, _rule in self._active_anomalies):
                ticks = self._quarantine_clean_ticks.get(spot_id, 0) + 1
                self._quarantine_clean_ticks[spot_id] = ticks
                if ticks >= _RELEASE_CLEAN_TICKS:
                    self._release_quarantine(spot_id, reason=f"{ticks} clean anomaly ticks")
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

    def resource_usage(self, include_cpu: bool = False) -> dict:
        cpu = self._process.cpu_percent(interval=0.1) if include_cpu else -1.0
        mem = self._process.memory_info().rss / (1024 * 1024)
        return {"cpu_pct": round(cpu, 1), "mem_mb": round(mem, 2)}

    def summary(self, include_cpu: bool = False) -> dict:
        res = self.resource_usage(include_cpu=include_cpu)
        return {
            "received": self.received_count,
            "filtered": self.filtered_count,
            "forwarded_events": self.forwarded_events,
            "anomalies": self.anomaly_count,
            "active_anomalies": len(self._active_anomalies),
            "resolved_anomalies": self._resolved_anomalies,
            "mode_switches": self.mode_switches,
            "active_arch": self._active_arch,
            "link_stats": self.stats.to_dict(),
            "quarantined": sorted(self._quarantine),
            "quarantined_count": len(self._quarantine),
            "detected_spots": len(self._cumulative_flags),
            "anomaly_log": self._anomaly_log[-200:],
            **res
        }