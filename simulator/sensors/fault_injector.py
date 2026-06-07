from __future__ import annotations
import random
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from ..models.models import ParkingEvent, SpotState


class FaultType(str, Enum):
    NONE = "none"
    STUCK_AT = "stuck_at"
    SILENT = "silent"
    FLAPPING = "flapping"
    REPLAY = "replay"
    FLOODING = "flooding"


@dataclass
class FaultSpec:
    fault_type: FaultType = FaultType.NONE
    stuck_state: str = "occupied"  
    replay_count: int = 3  
    flood_count: int = 10  


class FaultInjector:

    def __init__(self, rng: Optional[random.Random] = None) -> None:
        self._faults: dict[int, FaultSpec] = {}
        self._last_events: dict[int, ParkingEvent] = {}
        self.rng = rng or random.Random(0)
        self.injected_count: int = 0 


    def set_fault(self, spot_id: int, spec: FaultSpec) -> None:
        self._faults[spot_id] = spec

    def set_faults(self, spot_ids: list[int], spec: FaultSpec) -> None:
        for sid in spot_ids:
            self._faults[sid] = spec

    def clear_fault(self, spot_id: int) -> None:
        self._faults.pop(spot_id, None)

    def clear_all(self) -> None:
        self._faults.clear()

    def active_faults(self) -> dict[int, str]:
        return {sid: spec.fault_type.value for sid, spec in self._faults.items()}


    def apply(self, event: ParkingEvent) -> list[ParkingEvent]:
        spec = self._faults.get(event.spot_id)
        if spec is None or spec.fault_type == FaultType.NONE:
            self._last_events[event.spot_id] = event
            return [event]

        ft = spec.fault_type

        if ft == FaultType.SILENT:
            self.injected_count += 1
            return []

        if ft == FaultType.STUCK_AT:
            stuck = (SpotState.OCCUPIED if spec.stuck_state == "occupied" else SpotState.FREE)
            modified = ParkingEvent(sensor_id=event.sensor_id, spot_id=event.spot_id, state=stuck, timestamp=event.timestamp, sequence=event.sequence, is_initial=event.is_initial, is_heartbeat_event=event.is_heartbeat_event)
            self._last_events[event.spot_id] = modified
            if modified.state != event.state:
                self.injected_count += 1
            return [modified]

        if ft == FaultType.FLAPPING:
            wrong = (SpotState.FREE if event.state == SpotState.OCCUPIED else SpotState.OCCUPIED)
            flipped = ParkingEvent(sensor_id=event.sensor_id, spot_id=event.spot_id, state=wrong, timestamp=event.timestamp, sequence=event.sequence, is_initial=event.is_initial, is_heartbeat_event=event.is_heartbeat_event)
            self._last_events[event.spot_id] = event
            self.injected_count += 1
            return [event, flipped]

        if ft == FaultType.REPLAY:
            prev = self._last_events.get(event.spot_id)
            self._last_events[event.spot_id] = event
            result = [event]
            if prev is not None:
                for _ in range(spec.replay_count):
                    result.append(ParkingEvent(sensor_id=prev.sensor_id, spot_id=prev.spot_id, state=prev.state, timestamp=prev.timestamp, sequence=prev.sequence, is_initial=event.is_initial, is_heartbeat_event=event.is_heartbeat_event))
                    self.injected_count += 1
            return result

        if ft == FaultType.FLOODING:
            self._last_events[event.spot_id] = event
            copies = []
            for _ in range(max(0, spec.flood_count - 1)):
                copies.append(ParkingEvent(sensor_id=event.sensor_id, spot_id=event.spot_id, state=event.state, timestamp=event.timestamp, sequence=event.sequence, is_initial=event.is_initial, is_heartbeat_event=event.is_heartbeat_event))
                self.injected_count += 1
            return [event] + copies

        return [event]
