from __future__ import annotations
from typing import Callable

from ..models import ParkingEvent, SensorState, SpotState
from ..traffic.traffic_model import TrafficModel
from ..config import TrafficConfig
from ..des.engine import SimClock

SensorCallback = Callable[[ParkingEvent], None]

class SensorEmulator:
    def __init__(self, config: TrafficConfig, arrival_rate: float) -> None:
        self.config = config
        self.arrival_rate = arrival_rate
        self.num_spots = config.num_spots
        self._sensor_states: dict[int, SensorState] = {
            i: SensorState(spot_id=i) for i in range(self.num_spots)
        }
        self._callbacks: list[SensorCallback] = []
        self._total_generated = 0

    def add_callback(self, cb: SensorCallback) -> None:
        self._callbacks.append(cb)

    def _on_event(self, event: ParkingEvent) -> None:
        state = self._sensor_states[event.spot_id]
        if state.state == event.state:
            state.consecutive_same += 1
        else:
            state.consecutive_same = 0
        state.state = event.state
        state.last_event_seq = event.sequence
        state.last_updated = event.timestamp
        state.total_events += 1
        self._total_generated += 1
        for cb in self._callbacks:
            cb(event)

    def schedule_run(self, clock: SimClock, duration_s: float, epoch: float) -> None:
        traffic = TrafficModel(
            self.config, self.arrival_rate, clock, self._on_event, epoch
        )
        traffic.schedule_run(duration_s)

    @property
    def total_generated(self) -> int:
        return self._total_generated

    def occupancy_snapshot(self) -> dict:
        total = self.num_spots
        occupied = sum(
            1 for s in self._sensor_states.values() if s.state == SpotState.OCCUPIED
        )
        return {
            "total": total,
            "occupied": occupied,
            "free": total - occupied,
            "occupancy_pct": round(occupied / total * 100, 1) if total else 0
        }