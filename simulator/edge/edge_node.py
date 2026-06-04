from __future__ import annotations
import json
import logging
from typing import Callable, Optional

from ..models.models import BatchUpdate, LinkStats, ParkingEvent, SensorState, SpotState
from ..config.config import EdgeConfig, ScenarioConfig
from ..des.engine import SimClock

logger = logging.getLogger(__name__)

CloudForwardCallback = Callable[[BatchUpdate, bytes], None]

_RAPID_ARRIVAL_S: float = 0.5
_MIN_DWELL_S: float = 10.0
_RELEASE_CLEAN_TICKS: int = 5
_STUCK_THRESHOLD_S: float = 12.0 * 3600.0


class EdgeNode:

    ANOMALY_INTERVAL_S: float = 30.0
    ADAPTIVE_INTERVAL_S: float = 30.0
    ADAPTIVE_DEGRADE_THRESHOLD: float = 0.85
    ADAPTIVE_RECOVER_THRESHOLD: float = 0.95

    def __init__(self, config: ScenarioConfig, clock: SimClock, cloud_cb: CloudForwardCallback, epoch: float) -> None:
        self.config = config
        self.edge_cfg: EdgeConfig = config.edge
        self.clock = clock
        self._cloud_cb = cloud_cb
        self._epoch = epoch
        self.edge_id = "edge_01"

        self._backhaul_encoding_ratio: float = getattr(config.backhaul_link, "payload_encoding_ratio", 1.0)
        self._active_arch: str = config.architecture
        self._cache: dict[int, SensorState] = {}
        self._pending: list[ParkingEvent] = []

        self.stats = LinkStats(name="edge_to_cloud")
        self._sensor_link_stats: Optional[LinkStats] = None
        self._backhaul_link_stats: Optional[LinkStats] = None

        self.received_count: int = 0
        self.filtered_count: int = 0
        self.forwarded_events: int = 0
        self.heartbeats_forwarded: int = 0

        self.heartbeats_suppressed: int = 0
        self.quarantine_suppressed: int = 0

        self.anomaly_count: int = 0
        self.mode_switches: int = 0

        self._adaptive_prev_backhaul_sent: int = 0
        self._adaptive_prev_backhaul_recv: int = 0

        self._active_anomalies: set[tuple[int, str]] = set()
        self._resolved_anomalies: int = 0
        self._cumulative_flags: dict[int, int] = {}
        self._quarantine: set[int] = set()
        self._quarantine_clean_ticks: dict[int, int] = {}
        self._quarantine_valid_events: dict[int, int] = {}
        self._last_arrival_virtual: dict[int, float] = {}

        self._event_log: list[dict] = []

        if self._active_arch == "edge_aggregated":
            self.clock.schedule(self.edge_cfg.aggregation_interval_s, self._aggregation_tick)
        if self.edge_cfg.anomaly_detection:
            self.clock.schedule(self.ANOMALY_INTERVAL_S, self._anomaly_tick)
        elif self.edge_cfg.adaptive_edge:
            self.clock.schedule(self.ADAPTIVE_INTERVAL_S, self._standalone_adaptive_tick)

    def set_sensor_link_stats(self, stats: LinkStats) -> None:
        self._sensor_link_stats = stats

    def set_backhaul_link_stats(self, stats: LinkStats) -> None:
        self._backhaul_link_stats = stats

    def receive(self, event: ParkingEvent, raw_bytes: bytes) -> None:
        self.received_count += 1
        cached = self._cache.get(event.spot_id)
        if cached is None:
            cached = SensorState(spot_id=event.spot_id)
            self._cache[event.spot_id] = cached

        now_virtual = event.timestamp - self._epoch

        if event.is_initial and event.sequence > self.config.num_spots:
            hb_forward_interval = self.edge_cfg.heartbeat_forward_interval_s
            hb_due = (
                hb_forward_interval <= 0
                or cached.last_heartbeat_forwarded_timestamp == 0.0
                or (event.timestamp - cached.last_heartbeat_forwarded_timestamp) >= hb_forward_interval
            )
            if not hb_due:
                cached.last_updated = event.timestamp
                self.filtered_count += 1
                self.heartbeats_suppressed += 1
                return

            previous_state = cached.state
            state_changed = previous_state != event.state
            cached.state = event.state
            cached.last_updated = event.timestamp
            if state_changed:
                cached.last_state_change_timestamp = event.timestamp
                cached.consecutive_same = 0
            cached.last_event_seq = max(cached.last_event_seq, event.sequence)
            cached.total_events += 1

            if self._active_arch == "edge_filtered":
                self._forward_single(event)
                self.heartbeats_forwarded += 1
            elif self._active_arch == "edge_aggregated":
                self._pending.append(event)
                self.heartbeats_forwarded += 1
                self._flush_if_needed()
            cached.last_heartbeat_forwarded_timestamp = event.timestamp
            return

        if self.edge_cfg.anomaly_detection:
            self._check_r3_r4(event, cached, now_virtual)

        if self._should_filter(event, cached):
            self.filtered_count += 1
            return

        previous_state_pre = cached.state
        state_would_change = previous_state_pre != event.state
        if event.spot_id in self._quarantine and not state_would_change:
            self.filtered_count += 1
            self.quarantine_suppressed += 1
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

        stale_seq = (cached.last_event_seq > 0 and event.sequence <= cached.last_event_seq)
        if stale_seq:
            return True

        same_state = cached.state == event.state
        if not same_state:
            return False

        event_virtual = event.timestamp - self._epoch
        last_virtual = cached.last_updated - self._epoch if cached.last_updated else None
        if last_virtual is not None and (event_virtual - last_virtual) <= self.edge_cfg.duplicate_window_s:
            return True

        heartbeat_due = (cached.last_forwarded_timestamp == 0.0 or (event.timestamp - cached.last_forwarded_timestamp) >= self.edge_cfg.heartbeat_forward_interval_s)
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

    def _aggregation_tick(self) -> None:
        self._flush_batch()
        self.clock.schedule(self.edge_cfg.aggregation_interval_s, self._aggregation_tick)

    def record_cloud_drop(self) -> None:
        self.stats.dropped += 1

    def flush_final(self) -> None:
        if self._active_arch == "edge_aggregated" and self._pending:
            self._flush_batch()

    def _forward_single(self, event: ParkingEvent) -> None:
        batch = BatchUpdate(edge_id=self.edge_id, events=[event])
        payload = self._serialize_batch(batch)
        wire_bytes = max(1, int(len(payload) * self._backhaul_encoding_ratio))
        self.stats.sent += 1
        self.stats.total_bytes_sent += wire_bytes
        self.forwarded_events += 1
        self._cloud_cb(batch, payload)

    def _flush_batch(self) -> None:
        if not self._pending:
            return
        events = list(self._pending)
        self._pending.clear()
        batch = BatchUpdate(edge_id=self.edge_id, events=events)
        payload = self._serialize_batch(batch)
        wire_bytes = max(1, int(len(payload) * self._backhaul_encoding_ratio))
        self.stats.sent += 1
        self.stats.total_bytes_sent += wire_bytes
        self.forwarded_events += len(events)
        self._cloud_cb(batch, payload)

    @staticmethod
    def _serialize_batch(batch: BatchUpdate) -> bytes:
        return json.dumps(batch.to_dict()).encode()

    def _check_r3_r4(self, event: ParkingEvent, cached: SensorState, now_virtual: float) -> None:
        sid = event.spot_id
        if event.state == SpotState.OCCUPIED and not event.is_initial:
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
        if cached.last_state_change_timestamp > 0.0 and cached.state != event.state:
            last_change_virtual = cached.last_state_change_timestamp - self._epoch
            if (now_virtual - last_change_virtual) < _MIN_DWELL_S:
                self._flag_anomaly(event.spot_id, "R5_rapid_state_flip", now_virtual)
                return
        self._resolve_anomaly(event.spot_id, "R5_rapid_state_flip", now_virtual)

    def _check_anomalies(self) -> None:
        now_virtual = self.clock.now
        silent_thr_s = self.edge_cfg.silent_threshold_s
        for spot_id, state in self._cache.items():
            if state.last_state_change_timestamp > 0.0:
                last_c_v = state.last_state_change_timestamp - self._epoch
                if (now_virtual - last_c_v) > _STUCK_THRESHOLD_S:
                    self._flag_anomaly(spot_id, "R1_stuck_sensor", now_virtual)
                else:
                    self._resolve_anomaly(spot_id, "R1_stuck_sensor", now_virtual)
            if state.last_updated > 0.0:
                last_u_v = state.last_updated - self._epoch
                if (now_virtual - last_u_v) > silent_thr_s:
                    self._flag_anomaly(spot_id, "R2_silent_sensor", now_virtual)
                else:
                    self._resolve_anomaly(spot_id, "R2_silent_sensor", now_virtual)

    def _flag_anomaly(self, spot_id: int, rule: str, t_virtual: float) -> None:
        key = (spot_id, rule)
        if key in self._active_anomalies:
            return
        self._active_anomalies.add(key)
        self.anomaly_count += 1
        flags = self._cumulative_flags.get(spot_id, 0) + 1
        self._cumulative_flags[spot_id] = flags
        threshold = self.edge_cfg.quarantine_threshold
        self._event_log.append({"t_virtual": round(t_virtual, 1), "event": "ANOMALY_FLAG", "detail": f"spot={spot_id} rule={rule} cumulative_flags={flags}"})
        if flags >= threshold and spot_id not in self._quarantine:
            self._quarantine.add(spot_id)
            self._quarantine_clean_ticks[spot_id] = 0
            self._quarantine_valid_events[spot_id] = 0
            self._event_log.append({"t_virtual": round(t_virtual, 1), "event": "QUARANTINE_ADD", "detail": f"spot={spot_id} cumulative_flags={flags} threshold={threshold}"})
            logger.info(f"[QUARANTINE] t={t_virtual:.0f}s Spot {spot_id} quarantined (flags={flags})")

    def _resolve_anomaly(self, spot_id: int, rule: str, t_virtual: float) -> None:
        key = (spot_id, rule)
        if key not in self._active_anomalies:
            return
        self._active_anomalies.discard(key)
        self._resolved_anomalies += 1
        self._event_log.append({"t_virtual": round(t_virtual, 1), "event": "ANOMALY_RESOLVE", "detail": f"spot={spot_id} rule={rule}"})

    def _mark_valid_event_for_recovery(self, spot_id: int) -> None:
        if spot_id not in self._quarantine:
            return
        if any(sid == spot_id for sid, _rule in self._active_anomalies):
            self._quarantine_valid_events[spot_id] = 0
            return
        valid = self._quarantine_valid_events.get(spot_id, 0) + 1
        self._quarantine_valid_events[spot_id] = valid
        if valid >= self.edge_cfg.quarantine_recovery_events:
            self._release_quarantine(spot_id, reason=f"{valid} valid events", t_virtual=self.clock.now)

    def _release_quarantine(self, spot_id: int, reason: str, t_virtual: float = 0.0) -> None:
        self._quarantine.discard(spot_id)
        self._quarantine_clean_ticks.pop(spot_id, None)
        self._quarantine_valid_events.pop(spot_id, None)
        self._cumulative_flags[spot_id] = 0
        for key in list(self._active_anomalies):
            if key[0] == spot_id:
                self._active_anomalies.discard(key)
                self._resolved_anomalies += 1
        self._event_log.append({"t_virtual": round(t_virtual, 1), "event": "QUARANTINE_RELEASE", "detail": f"spot={spot_id} reason={reason}"})
        logger.info(f"[QUARANTINE] t={t_virtual:.0f}s Spot {spot_id} released: {reason}")

    def _anomaly_tick(self) -> None:
        self._check_anomalies()
        if self.edge_cfg.adaptive_edge:
            self._check_adaptive_mode()
        for spot_id in list(self._quarantine):
            if not any(sid == spot_id for sid, _rule in self._active_anomalies):
                ticks = self._quarantine_clean_ticks.get(spot_id, 0) + 1
                self._quarantine_clean_ticks[spot_id] = ticks
                if ticks >= _RELEASE_CLEAN_TICKS:
                    self._release_quarantine(spot_id, reason=f"{ticks} clean anomaly ticks", t_virtual=self.clock.now)
            else:
                self._quarantine_clean_ticks[spot_id] = 0
        self.clock.schedule(self.ANOMALY_INTERVAL_S, self._anomaly_tick)

    def _standalone_adaptive_tick(self) -> None:
        self._check_adaptive_mode()
        self.clock.schedule(self.ADAPTIVE_INTERVAL_S, self._standalone_adaptive_tick)

    def _check_adaptive_mode(self) -> None:
        if self.config.architecture != "edge_aggregated":
            return
        if not self.edge_cfg.adaptive_edge:
            return

        ls = self._backhaul_link_stats if self._backhaul_link_stats is not None else self.stats
        delta_sent = ls.sent - self._adaptive_prev_backhaul_sent
        delta_recv = ls.received - self._adaptive_prev_backhaul_recv
        self._adaptive_prev_backhaul_sent = ls.sent
        self._adaptive_prev_backhaul_recv = ls.received

        if delta_sent < 3:
            return

        dr = delta_recv / delta_sent

        if self._active_arch == "edge_aggregated" and dr < self.ADAPTIVE_DEGRADE_THRESHOLD:
            detail = (
                f"backhaul_window_DR={dr:.3f} < {self.ADAPTIVE_DEGRADE_THRESHOLD:.0%} "
                f"delta_sent={delta_sent} delta_recv={delta_recv}"
            )
            logger.info(f"[ADAPTIVE] t={self.clock.now:.0f}s → edge_filtered  ({detail})")
            self._event_log.append({"t_virtual": round(self.clock.now, 1), "event": "MODE_SWITCH", "detail": f"aggregated→filtered {detail}"})
            self._flush_batch()
            self._active_arch = "edge_filtered"
            self.mode_switches += 1

        elif self._active_arch == "edge_filtered" and dr >= self.ADAPTIVE_RECOVER_THRESHOLD:
            detail = (
                f"backhaul_window_DR={dr:.3f} >= {self.ADAPTIVE_RECOVER_THRESHOLD:.0%} "
                f"delta_sent={delta_sent} delta_recv={delta_recv}"
            )
            logger.info(f"[ADAPTIVE] t={self.clock.now:.0f}s → edge_aggregated ({detail})")
            self._event_log.append({"t_virtual": round(self.clock.now, 1), "event": "MODE_SWITCH", "detail": f"filtered→aggregated {detail}"})
            self._active_arch = "edge_aggregated"
            self.mode_switches += 1

    def summary(self) -> dict:
        return {
            "received": self.received_count,
            "filtered": self.filtered_count,
            "forwarded_events": self.forwarded_events,
            "heartbeats_forwarded": self.heartbeats_forwarded,
            "heartbeats_suppressed": self.heartbeats_suppressed,
            "quarantine_suppressed": self.quarantine_suppressed,
            "anomalies": self.anomaly_count,
            "active_anomalies": len(self._active_anomalies),
            "resolved_anomalies": self._resolved_anomalies,
            "mode_switches": self.mode_switches,
            "active_arch": self._active_arch,
            "link_stats": self.stats.to_dict(),
            "quarantined": sorted(self._quarantine),
            "quarantined_count": len(self._quarantine),
            "detected_spots": len(self._cumulative_flags),
            "event_log": list(self._event_log)
        }