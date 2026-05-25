from __future__ import annotations
from typing import Callable

from ..models import ParkingEvent, SensorState, SpotState
from ..traffic.traffic_model import TrafficModel
from ..config import TrafficConfig
from ..des.engine import SimClock

SensorCallback = Callable[[ParkingEvent], None]


class SensorEmulator:
    def __init__(self, config: TrafficConfig, arrival_rate: float, wall_clock: bool = False) -> None:
        self.config = config
        self.arrival_rate = arrival_rate
        self._wall_clock = wall_clock
        self.num_spots = config.num_spots
        self._sensor_states: dict[int, SensorState] = {i: SensorState(spot_id=i) for i in range(self.num_spots)}
        self._callbacks: list[SensorCallback] = []
        self._total_generated = 0
        self._state_changes_generated = 0
        self._heartbeats_generated = 0
        self._initial_snapshots_generated = 0
        self._duplicate_sends_generated = 0
        self._fault_injector = None

    def set_fault_injector(self, fi) -> None:
        self._fault_injector = fi

    def add_callback(self, cb: SensorCallback) -> None:
        self._callbacks.append(cb)

    def _on_event(self, event: ParkingEvent) -> None:
        state = self._sensor_states[event.spot_id]

        is_initial = event.is_initial
        is_initial_snapshot = is_initial and event.sequence <= self.num_spots
        is_heartbeat = is_initial and event.sequence > self.num_spots
        is_transition = (not is_initial) and (state.state != event.state)
        is_duplicate_send = (not is_initial) and (state.state == event.state)

        if state.state == event.state:
            state.consecutive_same += 1
        else:
            state.consecutive_same = 0

        state.state = event.state
        state.last_event_seq = event.sequence
        state.last_updated = event.timestamp
        state.total_events += 1
        self._total_generated += 1

        if is_transition:
            self._state_changes_generated += 1
        elif is_heartbeat:
            self._heartbeats_generated += 1
        elif is_initial_snapshot:
            self._initial_snapshots_generated += 1
        elif is_duplicate_send:
            self._duplicate_sends_generated += 1

        tx_events = (self._fault_injector.apply(event) if self._fault_injector is not None else [event])
        for e in tx_events:
            for cb in self._callbacks:
                cb(e)

    def schedule_run(self, clock: SimClock, duration_s: float, epoch: float) -> None:
        traffic = TrafficModel(self.config, self.arrival_rate, clock, self._on_event, epoch, wall_clock=self._wall_clock)
        traffic.schedule_run(duration_s)

    @property
    def total_generated(self) -> int:
        return self._total_generated

    @property
    def state_changes_generated(self) -> int:
        return self._state_changes_generated

    @property
    def heartbeats_generated(self) -> int:
        return self._heartbeats_generated

    @property
    def initial_snapshots_generated(self) -> int:
        return self._initial_snapshots_generated

    @property
    def duplicate_sends_generated(self) -> int:
        return self._duplicate_sends_generated

    def occupancy_snapshot(self) -> dict:
        total = self.num_spots
        occupied = sum(1 for s in self._sensor_states.values() if s.state == SpotState.OCCUPIED)
        return {"total": total, "occupied": occupied, "free": total - occupied,
                "occupancy_pct": round(occupied / total * 100, 1) if total else 0}