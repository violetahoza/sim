from __future__ import annotations
import random
from typing import Optional

from simulator.des.engine import SimClock


class ConnectionOutageModel:

    def __init__(self, clock: SimClock, rng: random.Random, mean_up_s: float, mean_down_s: float) -> None:
        if mean_up_s <= 0.0 or mean_down_s <= 0.0:
            raise ValueError("ConnectionOutageModel requires positive mean_up_s and mean_down_s")
        self.clock = clock
        self.rng = rng
        self.mean_up_s = mean_up_s
        self.mean_down_s = mean_down_s
        self._is_down = False
        self._next_flip_at = clock.now + self._draw(mean_up_s)

    def _draw(self, mean: float) -> float:
        return self.rng.expovariate(1.0 / mean)

    def is_down(self, now: Optional[float] = None) -> bool:
        now = self.clock.now if now is None else now
        while now >= self._next_flip_at:
            self._is_down = not self._is_down
            self._next_flip_at += self._draw(self.mean_down_s if self._is_down else self.mean_up_s)
        return self._is_down

    def seconds_until_up(self, now: Optional[float] = None) -> float:
        now = self.clock.now if now is None else now
        if not self.is_down(now):
            return 0.0
        return max(0.0, self._next_flip_at - now)