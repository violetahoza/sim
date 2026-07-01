from __future__ import annotations
import math
import random
import time as _time_module
from typing import Callable, Optional

from ..models.models import ParkingEvent, SpotState
from ..config.config import TrafficConfig
from ..des.engine import SimClock
from ..config.constants import DWELL_SHORT_MU_S, DWELL_SHORT_CV, DWELL_LONG_MU_S, DWELL_LONG_CV, DWELL_SHORT_PROB

class TrafficModel:

    MIN_DWELL_S: float = 30.0
    MAX_DWELL_S: float = 43_200.0

    def __init__(self, config: TrafficConfig, arrival_rate: float, clock: SimClock, event_cb: Callable[[ParkingEvent], None], epoch: float, rng: Optional[random.Random] = None, wall_clock: bool = False) -> None:
        self.config = config
        self.arrival_rate = arrival_rate
        self.num_spots = config.num_spots
        self.mean_duration = config.mean_parking_duration_s
        self.clock = clock
        self.event_cb = event_cb
        self.epoch = epoch
        self.rng = rng or random.Random(config.random_seed)
        self._tod_factors: list[float] = config.tod_factors
        self._wall_clock = wall_clock
        self._time_scale = config.time_scale
        self.occupied: dict[int, bool] = {i: self.rng.random() < config.initial_occupancy for i in range(self.num_spots)}
        self._end_time: float = 0.0
        self._seq: int = 0
        self._arrivals_suspended: bool = False
        self._lambda_max: float = self._compute_lambda_max()

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _free_spots(self) -> list[int]:
        return [i for i, occ in self.occupied.items() if not occ]

    def _occupied_spots(self) -> list[int]:
        return [i for i, occ in self.occupied.items() if occ]

    def _sample_dwell(self) -> float:
        if self.config.use_dwell_mixture:
            return self._sample_dwell_mixture()

        cv = self.config.parking_duration_cv
        mu = self.mean_duration

        if cv <= 0.0:
            return max(self.MIN_DWELL_S, min(self.MAX_DWELL_S, mu))

        if abs(cv - 1.0) < 1e-6:
            raw = self.rng.expovariate(1.0 / mu)
        else:
            sigma_sq = math.log(1.0 + cv * cv)
            sigma = math.sqrt(sigma_sq)
            mu_log = math.log(mu) - 0.5 * sigma_sq
            raw = math.exp(mu_log + sigma * self.rng.gauss(0.0, 1.0))

        return max(self.MIN_DWELL_S, min(self.MAX_DWELL_S, raw))

    def _sample_dwell_mixture(self) -> float:
        if self.rng.random() < DWELL_SHORT_PROB:
            mu, cv = DWELL_SHORT_MU_S, DWELL_SHORT_CV
        else:
            mu, cv = DWELL_LONG_MU_S, DWELL_LONG_CV

        sigma_sq = math.log(1.0 + cv * cv)
        sigma = math.sqrt(sigma_sq)
        mu_log = math.log(mu) - 0.5 * sigma_sq
        raw = math.exp(mu_log + sigma * self.rng.gauss(0.0, 1.0))
        return max(self.MIN_DWELL_S, min(self.MAX_DWELL_S, raw))

    def _tod_factor(self, virtual_s: float) -> float:
        if not self.config.use_time_of_day:
            return 1.0
        hour = (self.config.start_hour + virtual_s / 3600.0) % 24.0
        h = int(hour)
        frac = hour - h
        f0 = self._tod_factors[h % 24]
        f1 = self._tod_factors[(h + 1) % 24]
        return max(f0 + frac * (f1 - f0), 0.001)

    def _rate_at(self, virtual_s: float) -> float:
        return max(self.arrival_rate * self._tod_factor(virtual_s), 1e-9)

    def _compute_lambda_max(self) -> float:
        # Linear interpolation between hourly buckets never exceeds the max
        # bucket value, so this is a valid majorant for the thinning algorithm.
        if not self.config.use_time_of_day:
            return max(self.arrival_rate, 1e-9)
        return max(self.arrival_rate * max(self._tod_factors), 1e-9)

    def _make_timestamp(self, virtual_time: float) -> float:
        if self._wall_clock:
            return _time_module.time()
        return self.epoch + virtual_time

    def schedule_run(self, duration_s: float) -> None:
        self._end_time = duration_s

        n_initial = 0
        for spot_id, is_occ in self.occupied.items():
            if is_occ:
                n_initial += 1
                ts = self._make_timestamp(0.0)
                event = ParkingEvent(sensor_id=f"sensor_{spot_id:04d}", spot_id=spot_id, state=SpotState.OCCUPIED, timestamp=ts, sequence=spot_id + 1, is_initial=True)
                self.event_cb(event)
                residual = self.rng.uniform(0.0, self.mean_duration)
                dep_t = min(residual, duration_s - 1.0)
                if dep_t > 0:
                    self.clock.schedule_at(dep_t, lambda s=spot_id, t=dep_t: self._on_departure(s, t))

        self._seq = self.num_spots

        self._schedule_next_arrival(0.0)

        hb = self.config.heartbeat_interval_s
        if hb > 0.0:
            for spot_id in range(self.num_spots):
                offset = self.rng.uniform(0.0, hb)
                self.clock.schedule_at(offset, lambda s=spot_id, t=offset: self._heartbeat_spot(s, t))

    def _schedule_next_arrival(self, from_time: float) -> None:
        # Ogata's thinning algorithm for a non-homogeneous Poisson process:
        # draw candidates from a homogeneous process at the peak rate
        # (lambda_max) and accept each one with probability lambda(t)/lambda_max,
        # which yields exactly the target arrival rate at every instant t
        # rather than only approximating it between events.
        t = from_time
        while t < self._end_time:
            t += self.rng.expovariate(self._lambda_max)
            if t >= self._end_time:
                return
            if self.rng.random() < self._rate_at(t) / self._lambda_max:
                self.clock.schedule_at(t, lambda tt=t: self._on_arrival(tt))
                return

    def _occupy_spot(self, spot_id: int, virtual_time: float) -> None:
        self.occupied[spot_id] = True

        ts = self._make_timestamp(virtual_time)
        event = ParkingEvent(sensor_id=f"sensor_{spot_id:04d}", spot_id=spot_id, state=SpotState.OCCUPIED, timestamp=ts, sequence=self._next_seq(), is_initial=False)
        self.event_cb(event)

        self._maybe_schedule_duplicate(spot_id, SpotState.OCCUPIED, virtual_time)

        dwell = self._sample_dwell()
        dep_t = virtual_time + dwell
        self.clock.schedule_at(dep_t, lambda s=spot_id, t=dep_t: self._on_departure(s, t))

    def _on_arrival(self, virtual_time: float) -> None:
        free = self._free_spots()
        if free:
            spot_id = self.rng.choice(free)
            self._occupy_spot(spot_id, virtual_time)
            self._schedule_next_arrival(virtual_time)
        else:
            self._arrivals_suspended = True

    def _on_departure(self, spot_id: int, virtual_time: float) -> None:
        if not self.occupied.get(spot_id, False):
            return
        self.occupied[spot_id] = False

        ts = self._make_timestamp(virtual_time)
        event = ParkingEvent(sensor_id=f"sensor_{spot_id:04d}", spot_id=spot_id, state=SpotState.FREE, timestamp=ts, sequence=self._next_seq(), is_initial=False)
        self.event_cb(event)
        self._maybe_schedule_duplicate(spot_id, SpotState.FREE, virtual_time)

        if self._arrivals_suspended:
            self._arrivals_suspended = False
            self._schedule_next_arrival(virtual_time)


    def _heartbeat_spot(self, spot_id: int, virtual_time: float) -> None:
        if virtual_time >= self._end_time:
            return
        state = SpotState.OCCUPIED if self.occupied.get(spot_id, False) else SpotState.FREE
        ts = self._make_timestamp(virtual_time)
        event = ParkingEvent(sensor_id=f"sensor_{spot_id:04d}", spot_id=spot_id, state=state, timestamp=ts, sequence=self._next_seq(), is_initial=False, is_heartbeat_event=True)
        self.event_cb(event)
        next_t = virtual_time + self.config.heartbeat_interval_s
        if next_t < self._end_time:
            self.clock.schedule_at(next_t, lambda s=spot_id, t=next_t: self._heartbeat_spot(s, t))

    def _maybe_schedule_duplicate(self, spot_id: int, state: SpotState, virtual_time: float) -> None:
        prob = self.config.duplicate_send_prob
        if prob <= 0.0 or self.rng.random() >= prob:
            return
        delay = self.rng.uniform(0.5, 4.5)
        dup_t = virtual_time + delay
        if dup_t >= self._end_time:
            return
        seq = self._next_seq()
        ts_dup = self._make_timestamp(dup_t)
        self.clock.schedule_at(
            dup_t,
            lambda s=spot_id, st=state, t=ts_dup, sq=seq: self.event_cb(
                ParkingEvent(sensor_id=f"sensor_{s:04d}", spot_id=s, state=st, timestamp=t, sequence=sq, is_initial=False)
            )
        )