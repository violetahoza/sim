from __future__ import annotations
import asyncio
from typing import Callable
import simpy

class SimClock:

    def __init__(self) -> None:
        self.env: simpy.Environment = simpy.Environment()

    @property
    def now(self) -> float:
        return self.env.now

    def schedule(self, delay: float, cb: Callable[[], None]) -> None:
        def _proc() -> simpy.events.Event:
            yield self.env.timeout(max(0.0, delay))
            cb()
        self.env.process(_proc())

    def schedule_at(self, t: float, cb: Callable[[], None]) -> None:
        self.schedule(max(0.0, t - self.env.now), cb)

    @property
    def pending(self) -> bool:
        return self.env.peek() < simpy.core.Infinity

    def run_until(self, end_time: float) -> None:
        self.env.run(until=end_time)

    async def run_until_async(self, end_time: float, progress_cb=None, cancelled_cb=None, steps: int = 50, real_mode: bool = False, time_scale: float = 1.0) -> None:
        slice_size = end_time / max(steps, 1)
        next_stop = slice_size

        while self.env.now < end_time:
            if cancelled_cb and cancelled_cb():
                break

            stop = min(next_stop, end_time)
            self.env.run(until=stop)

            if progress_cb:
                progress_cb(self.env.now, end_time)

            next_stop += slice_size
            sleep_s = slice_size / time_scale if real_mode else 0.05
            await asyncio.sleep(sleep_s)
