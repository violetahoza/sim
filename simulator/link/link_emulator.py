from __future__ import annotations
import json
import random
from typing import Callable

from ..models import ParkingEvent, LinkStats
from ..config import LinkConfig
from ..des.engine import SimClock

ForwardCallback = Callable[[ParkingEvent, bytes], None]


class TokenBucket:

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.tokens = 1.0
        self._last_virtual = 0.0

    def wait_time(self, clock: SimClock) -> float:
        elapsed = clock.now - self._last_virtual
        self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
        self._last_virtual = clock.now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return 0.0
        deficit = 1.0 - self.tokens
        wait = deficit / self.rate
        self.tokens = 0.0
        return wait


class LinkEmulator:

    def __init__(
        self,
        config: LinkConfig,
        clock: SimClock,
        forward_cb: ForwardCallback,
        rng: random.Random | None = None,
    ) -> None:
        self.config = config
        self.clock = clock
        self._callback = forward_cb
        self.rng = rng or random.Random(config.packet_loss_rate.__hash__())
        self.stats = LinkStats(name="sensor_to_edge")
        self._bucket = TokenBucket(config.rate_limit_msgs_per_sec)

    def _serialize(self, event: ParkingEvent) -> bytes:
        return json.dumps(event.to_dict()).encode()

    def _should_drop(self) -> bool:
        return self.rng.random() < self.config.packet_loss_rate

    def _compute_delay(self) -> float:
        jitter = self.rng.uniform(-self.config.jitter_ms, self.config.jitter_ms)
        return max(0.0, self.config.base_delay_ms + jitter) / 1000.0

    def transmit(self, event: ParkingEvent) -> None:
        payload = self._serialize(event)
        wire_bytes = max(1, int(len(payload) * self.config.payload_encoding_ratio))

        self.stats.sent += 1
        self.stats.total_bytes_sent += wire_bytes

        token_delay = self._bucket.wait_time(self.clock)

        if self._should_drop():
            self.stats.dropped += 1
            return

        if wire_bytes > self.config.max_payload_bytes:
            self.stats.dropped += 1
            return

        total_delay = token_delay + self._compute_delay()

        def deliver() -> None:
            self.stats.received += 1
            self.stats.total_bytes_received += wire_bytes
            self._callback(event, payload)

        self.clock.schedule(total_delay, deliver)
