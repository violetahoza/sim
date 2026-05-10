from __future__ import annotations
import math
import random
from typing import Callable, Optional

from ..models import ParkingEvent, SpotState
from ..config import TrafficConfig
from ..des.engine import SimClock


class TrafficModel:

    def __init__(
        self,
        config: TrafficConfig,
        arrival_rate: float,
        clock: SimClock,
        event_cb: Callable[[ParkingEvent], None],
        epoch: float,
        rng: Optional[random.Random] = None
    ) -> None:
        self.config = config
        self.arrival_rate = arrival_rate
        self.num_spots = config.num_spots
        self.mean_duration = config.mean_parking_duration_s
        self.clock = clock
        self.event_cb = event_cb
        self.epoch = epoch
        self.rng = rng or random.Random(config.random_seed)
        self._tod_factors = config.tod_factors

        self.occupied: dict[int, bool] = {
            i: self.rng.random() < config.initial_occupancy
            for i in range(self.num_spots)
        }
        self._end_time: float = 0.0
        self._seq = 0

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _free_spots(self) -> list[int]:
        return [i for i, occ in self.occupied.items() if not occ]

    def _sample_dwell(self) -> float:
        cv = self.config.parking_duration_cv
        if cv == 1.0:
            return self.rng.expovariate(1.0 / self.mean_duration)
        sigma_log = math.sqrt(math.log(1.0 + cv * cv))
        mu_log = math.log(self.mean_duration) - 0.5 * sigma_log * sigma_log
        return math.exp(mu_log + sigma_log * self.rng.gauss(0.0, 1.0))

    def _tod_factor(self, virtual_s: float) -> float:
        if not self.config.use_time_of_day:
            return 1.0
        hour = (self.config.start_hour + virtual_s / 3600.0) % 24.0
        h = int(hour)
        frac = hour - h
        f0 = self._tod_factors[h]
        f1 = self._tod_factors[(h + 1) % 24]
        return max(f0 + frac * (f1 - f0), 0.01)

    def schedule_run(self, duration_s: float) -> None:
        self._end_time = duration_s
        for spot_id, is_occ in self.occupied.items():
            if is_occ:
                event = ParkingEvent(sensor_id=f"sensor_{spot_id:04d}", spot_id=spot_id, state=SpotState.OCCUPIED, timestamp=self.epoch, sequence=self._next_seq(), is_initial=True)
                self.event_cb(event)
                dwell = self._sample_dwell()
                dep_t = min(dwell, duration_s)
                self.clock.schedule_at(dep_t, lambda s=spot_id, t=dep_t: self._on_departure(s, t))
        self._schedule_next_arrival(0.0)

    def _schedule_next_arrival(self, from_time: float) -> None:
        tod = self._tod_factor(from_time)
        rate = max(self.arrival_rate * tod, 1e-9)
        inter = self.rng.expovariate(rate)
        next_t = from_time + inter
        if next_t <= self._end_time:
            self.clock.schedule_at(next_t, lambda t=next_t: self._on_arrival(t))

    def _on_arrival(self, virtual_time: float) -> None:
        free = self._free_spots()
        if free:
            spot_id = self.rng.choice(free)
            self.occupied[spot_id] = True
            event = ParkingEvent(sensor_id=f"sensor_{spot_id:04d}", spot_id=spot_id, state=SpotState.OCCUPIED, timestamp=self.epoch + virtual_time, sequence=self._next_seq())
            self.event_cb(event)
            dwell = self._sample_dwell()
            dep_t = virtual_time + dwell
            self.clock.schedule_at(dep_t, lambda s=spot_id, t=dep_t: self._on_departure(s, t))
        self._schedule_next_arrival(virtual_time)

    def _on_departure(self, spot_id: int, virtual_time: float) -> None:
        if self.occupied.get(spot_id, False):
            self.occupied[spot_id] = False
            event = ParkingEvent(sensor_id=f"sensor_{spot_id:04d}", spot_id=spot_id, state=SpotState.FREE, timestamp=self.epoch + virtual_time, sequence=self._next_seq())
            self.event_cb(event)