from __future__ import annotations
import logging
import statistics
from collections import deque
from typing import Callable, Optional

from ..models.models import BatchUpdate, LinkStats, ParkingEvent, SensorState, SpotState
from ..config.config import EdgeConfig, ScenarioConfig
from ..des.engine import SimClock
from ..utils import encode_batch

logger = logging.getLogger(__name__)

CloudForwardCallback = Callable[[BatchUpdate, bytes], None]

ADAPTIVE_WARMUP_SAMPLES = 3


class EdgeNode:

    def __init__(self, config: ScenarioConfig, clock: SimClock, cloud_cb: CloudForwardCallback, epoch: float) -> None:
        self.config = config
        self.edge_cfg: EdgeConfig = config.edge
        self.clock = clock
        self._cloud_cb = cloud_cb
        self._epoch = epoch
        self.edge_id = "edge_01"

        self._backhaul_overhead: int = getattr(config.backhaul_link, "transport_overhead_bytes", 0)
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
        self._backhaul_probe: Optional[Callable[[], tuple[int, int]]] = None
        self._adaptive_prev_offered: int = 0
        self._adaptive_prev_first_pass: int = 0
        self._adaptive_dr_ewma: Optional[float] = None
        self._adaptive_last_switch_time: float = -1e9
        self._adaptive_min_window_dr: Optional[float] = None
        self._adaptive_window_adjustments: int = 0
        self._adaptive_samples: int = 0

        self._base_aggregation_interval: float = self.edge_cfg.aggregation_interval_s
        self._active_aggregation_interval: float = self._base_aggregation_interval

        self._active_anomalies: set[tuple[int, str]] = set()
        self._resolved_anomalies: int = 0
        self._detected_spots: set[int] = set()
        self._quarantine: set[int] = set()
        self._ever_quarantined: set[int] = set()
        self._quarantine_clean_ticks: dict[int, int] = {}
        self._event_times: dict[int, deque] = {} 
        self._flip_times: dict[int, deque] = {} 
        self._incident_times: dict[int, deque] = {} 
        self._last_periodic_incident: dict[int, float] = {} 
        self._pending_rules: dict[int, set] = {} 

        self._event_log: list[dict] = []

        if self.edge_cfg.anomaly_detection:
            for sid in range(config.num_spots):
                self._cache[sid] = SensorState(spot_id=sid, last_updated=epoch, last_state_change_timestamp=epoch)

        if self._active_arch == "edge_aggregated":
            self.clock.schedule(self._active_aggregation_interval, self._aggregation_tick)
        if self.edge_cfg.anomaly_detection:
            self.clock.schedule(self.edge_cfg.anomaly_check_interval_s, self._anomaly_tick)
        elif self.edge_cfg.adaptive_edge:
            self.clock.schedule(self.edge_cfg.adaptive_check_interval_s, self._standalone_adaptive_tick)

    def set_sensor_link_stats(self, stats: LinkStats) -> None:
        self._sensor_link_stats = stats

    def set_backhaul_link_stats(self, stats: LinkStats) -> None:
        self._backhaul_link_stats = stats

    def set_backhaul_delivery_probe(self, cb: Callable[[], tuple[int, int]]) -> None:
        self._backhaul_probe = cb

    def receive(self, event: ParkingEvent, raw_bytes: bytes) -> None:
        self.received_count += 1
        cached = self._cache.get(event.spot_id)
        if cached is None:
            cached = SensorState(spot_id=event.spot_id)
            self._cache[event.spot_id] = cached

        now_virtual = event.timestamp - self._epoch

        if self.edge_cfg.anomaly_detection and not event.is_initial:
            self._observe_for_anomaly(event, cached, now_virtual)

        if event.is_heartbeat_event:
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

        if self._should_filter(event, cached):
            self.filtered_count += 1
            return

        previous_state_pre = cached.state
        state_would_change = previous_state_pre != event.state
        if event.spot_id in self._quarantine and not state_would_change:
            self.filtered_count += 1
            self.quarantine_suppressed += 1
            return

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
        last_virtual = (cached.last_updated - self._epoch) if cached.total_events > 0 else None
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
        self.clock.schedule(self._active_aggregation_interval, self._aggregation_tick)

    def record_cloud_drop(self) -> None:
        self.stats.dropped += 1

    def flush_final(self) -> None:
        if self._active_arch == "edge_aggregated" and self._pending:
            self._flush_batch()

    def _forward_single(self, event: ParkingEvent) -> None:
        batch = BatchUpdate(edge_id=self.edge_id, events=[event])
        payload = encode_batch(batch)
        wire_bytes = len(payload) + self._backhaul_overhead
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
        payload = encode_batch(batch)
        wire_bytes = len(payload) + self._backhaul_overhead
        self.stats.sent += 1
        self.stats.total_bytes_sent += wire_bytes
        self.forwarded_events += len(events)
        self._cloud_cb(batch, payload)


    def _observe_for_anomaly(self, event: ParkingEvent, cached: SensorState, now_virtual: float) -> None:
        sid = event.spot_id
        cutoff = now_virtual - self.edge_cfg.anomaly_rate_window_s

        ev = self._event_times.setdefault(sid, deque())
        ev.append(now_virtual)
        while ev and ev[0] < cutoff:
            ev.popleft()

        if cached.last_event_seq > 0 and event.sequence <= cached.last_event_seq:
            self._incident_times.setdefault(sid, deque()).append(now_virtual)
            self._pending_rules.setdefault(sid, set()).add("seq_integrity")

        if event.state != cached.state:
            fl = self._flip_times.setdefault(sid, deque())
            fl.append(now_virtual)
            while fl and fl[0] < cutoff:
                fl.popleft()

    def _rate_outlier_spots(self, counts: dict[int, int], all_spots) -> set[int]:
        all_spots = list(all_spots)
        if len(all_spots) < 5:
            return set()
        values = sorted(counts.get(sid, 0) for sid in all_spots)
        median = statistics.median(values)
        mad = statistics.median([abs(v - median) for v in values])
        scale = 1.4826 * mad if mad > 0 else (statistics.pstdev(values) or 1.0)
        z = self.edge_cfg.anomaly_robust_z
        floor = self.edge_cfg.anomaly_min_window_events
        return {sid for sid in all_spots
                if counts.get(sid, 0) >= floor and (counts.get(sid, 0) - median) / scale > z}

    def _score(self, spot_id: int, now_virtual: float) -> int:
        dq = self._incident_times.get(spot_id)
        if not dq:
            return 0
        cutoff = now_virtual - self.edge_cfg.anomaly_persistence_window_s
        while dq and dq[0] < cutoff:
            dq.popleft()
        return len(dq)

    def _check_anomalies(self) -> None:
        now_virtual = self.clock.now
        silent_thr_s = self.edge_cfg.silent_threshold_s or 2700.0
        stuck_thr_s = self.edge_cfg.stuck_threshold_s or 44100.0
        spacing = self.edge_cfg.anomaly_incident_spacing_s

        rate_outliers = self._rate_outlier_spots({s: len(t) for s, t in self._event_times.items()}, self._cache.keys())
        flip_outliers = self._rate_outlier_spots({s: len(t) for s, t in self._flip_times.items()}, self._cache.keys())

        new_active: set[tuple[int, str]] = set()

        for spot_id, state in self._cache.items():
            rules: set[str] = self._pending_rules.pop(spot_id, set()) 

            if state.last_updated >= 0.0 and (now_virtual - (state.last_updated - self._epoch)) > silent_thr_s:
                rules.add("silent")
            if state.state == SpotState.OCCUPIED and (now_virtual - (state.last_state_change_timestamp - self._epoch)) > stuck_thr_s:
                rules.add("stuck")
            if spot_id in rate_outliers:
                rules.add("rate_outlier")
            if spot_id in flip_outliers:
                rules.add("flip_outlier")

            if rules & {"silent", "stuck", "rate_outlier", "flip_outlier"}:
                if now_virtual - self._last_periodic_incident.get(spot_id, -1e9) >= spacing:
                    self._incident_times.setdefault(spot_id, deque()).append(now_virtual)
                    self._last_periodic_incident[spot_id] = now_virtual

            for r in rules:
                new_active.add((spot_id, r))

            score = self._score(spot_id, now_virtual)
            if score >= self.edge_cfg.anomaly_detect_score:
                self._detected_spots.add(spot_id)
            if score >= self.edge_cfg.quarantine_threshold and spot_id not in self._quarantine:
                self._quarantine.add(spot_id)
                self._ever_quarantined.add(spot_id)
                self._quarantine_clean_ticks[spot_id] = 0
                self._event_log.append({"t_virtual": round(now_virtual, 1), "event": "QUARANTINE_ADD", "detail": f"spot={spot_id} incidents={score} rules={sorted(rules)}"})
                logger.info(f"[QUARANTINE] t={now_virtual:.0f}s Spot {spot_id} quarantined (incidents={score}, rules={sorted(rules)})")

        for key in new_active - self._active_anomalies:
            self.anomaly_count += 1
            self._event_log.append({"t_virtual": round(now_virtual, 1), "event": "ANOMALY_FLAG", "detail": f"spot={key[0]} rule={key[1]}"})
        self._resolved_anomalies += len(self._active_anomalies - new_active)
        self._active_anomalies = new_active
        self._pending_rules.clear()

    def _release_quarantine(self, spot_id: int, reason: str, t_virtual: float = 0.0) -> None:
        self._quarantine.discard(spot_id)
        self._quarantine_clean_ticks.pop(spot_id, None)
        self._event_log.append({"t_virtual": round(t_virtual, 1), "event": "QUARANTINE_RELEASE", "detail": f"spot={spot_id} reason={reason}"})
        logger.info(f"[QUARANTINE] t={t_virtual:.0f}s Spot {spot_id} released: {reason}")

    def _anomaly_tick(self) -> None:
        self._check_anomalies()
        if self.edge_cfg.adaptive_edge:
            self._check_adaptive_mode()
        for spot_id in list(self._quarantine):
            if self._score(spot_id, self.clock.now) == 0:
                ticks = self._quarantine_clean_ticks.get(spot_id, 0) + 1
                self._quarantine_clean_ticks[spot_id] = ticks
                if ticks >= self.edge_cfg.quarantine_release_clean_ticks:
                    self._release_quarantine(spot_id, reason=f"score recovered ({ticks} clean ticks)", t_virtual=self.clock.now)
            else:
                self._quarantine_clean_ticks[spot_id] = 0
        self.clock.schedule(self.edge_cfg.anomaly_check_interval_s, self._anomaly_tick)

    def _standalone_adaptive_tick(self) -> None:
        self._check_adaptive_mode()
        self.clock.schedule(self.edge_cfg.adaptive_check_interval_s, self._standalone_adaptive_tick)

    def _check_adaptive_mode(self) -> None:
        if self.config.architecture != "edge_aggregated":
            return
        if not self.edge_cfg.adaptive_edge:
            return
        
        cooldown = 2.0 * self.edge_cfg.adaptive_check_interval_s
        if (self.clock.now - self._adaptive_last_switch_time) < cooldown:
            return

        if self._backhaul_probe is not None:
            offered, first_pass = self._backhaul_probe()
            delta_sent = offered - self._adaptive_prev_offered
            delta_recv = first_pass - self._adaptive_prev_first_pass
            if delta_sent < self.edge_cfg.adaptive_min_window_samples:
                return
            self._adaptive_prev_offered = offered
            self._adaptive_prev_first_pass = first_pass
        else:
            ls = self._backhaul_link_stats if self._backhaul_link_stats is not None else self.stats
            delta_sent = ls.sent - self._adaptive_prev_backhaul_sent
            delta_recv = ls.received - self._adaptive_prev_backhaul_recv
            if delta_sent < self.edge_cfg.adaptive_min_window_samples:
                return
            self._adaptive_prev_backhaul_sent = ls.sent
            self._adaptive_prev_backhaul_recv = ls.received

        if delta_sent <= 0:
            return
        raw_dr = delta_recv / delta_sent
        alpha = self.edge_cfg.adaptive_dr_smoothing
        if self._adaptive_dr_ewma is None:
            self._adaptive_dr_ewma = raw_dr
        else:
            self._adaptive_dr_ewma = alpha * raw_dr + (1.0 - alpha) * self._adaptive_dr_ewma
        dr = self._adaptive_dr_ewma

        self._adaptive_samples += 1
        if self._adaptive_samples < ADAPTIVE_WARMUP_SAMPLES:
            return
        if self._adaptive_min_window_dr is None or dr < self._adaptive_min_window_dr:
            self._adaptive_min_window_dr = dr

        if self._active_arch == "edge_aggregated" and dr < self.edge_cfg.adaptive_degrade_threshold:
            new_interval = min(self._active_aggregation_interval * 2.0, 60.0)
            detail = (
                f"backhaul_window_DR={dr:.3f} < {self.edge_cfg.adaptive_degrade_threshold:.0%} "
                f"delta_sent={delta_sent} delta_recv={delta_recv} "
                f"agg_interval={self._active_aggregation_interval:.1f}→{new_interval:.1f}s"
            )
            self._active_aggregation_interval = new_interval

            if dr < self.edge_cfg.adaptive_filtered_threshold:
                logger.info(f"[ADAPTIVE] t={self.clock.now:.0f}s → edge_filtered  ({detail})")
                self._event_log.append({"t_virtual": round(self.clock.now, 1), "event": "MODE_SWITCH", "detail": f"aggregated→filtered {detail}"})
                self._flush_batch()
                self._active_arch = "edge_filtered"
                self.mode_switches += 1
                self._adaptive_last_switch_time = self.clock.now
            else:
                self._adaptive_window_adjustments += 1
                self._event_log.append({"t_virtual": round(self.clock.now, 1), "event": "AGG_WINDOW_INCREASE", "detail": detail})

        elif self._active_arch == "edge_filtered" and dr >= self.edge_cfg.adaptive_recover_threshold:
            detail = (
                f"backhaul_window_DR={dr:.3f} >= {self.edge_cfg.adaptive_recover_threshold:.0%} "
                f"delta_sent={delta_sent} delta_recv={delta_recv}"
            )
            logger.info(f"[ADAPTIVE] t={self.clock.now:.0f}s → edge_aggregated ({detail})")
            self._event_log.append({"t_virtual": round(self.clock.now, 1), "event": "MODE_SWITCH", "detail": f"filtered→aggregated {detail}"})
            self._active_arch = "edge_aggregated"
            self._active_aggregation_interval = self._base_aggregation_interval
            self.mode_switches += 1
            self._adaptive_last_switch_time = self.clock.now

        elif self._active_arch == "edge_aggregated" and dr >= self.edge_cfg.adaptive_recover_threshold:
            if self._active_aggregation_interval > self._base_aggregation_interval:
                new_interval = max(self._base_aggregation_interval, self._active_aggregation_interval / 2.0)
                detail = f"DR={dr:.3f} healthy, agg_interval={self._active_aggregation_interval:.1f}→{new_interval:.1f}s"
                self._active_aggregation_interval = new_interval
                self._adaptive_window_adjustments += 1
                self._event_log.append({"t_virtual": round(self.clock.now, 1), "event": "AGG_WINDOW_DECREASE", "detail": detail})

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
            "adaptive_window_adjustments": self._adaptive_window_adjustments,
            "adaptive_min_window_dr": self._adaptive_min_window_dr,
            "adaptive_samples": self._adaptive_samples,
            "active_arch": self._active_arch,
            "link_stats": self.stats.to_dict(),
            "quarantined": sorted(self._quarantine),
            "ever_quarantined": sorted(self._ever_quarantined),
            "quarantined_count": len(self._quarantine),
            "detected_spots": len(self._detected_spots),
            "detected_spot_ids": sorted(self._detected_spots),
            "event_log": list(self._event_log)
        }