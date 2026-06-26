from __future__ import annotations
import math
import random
import threading
from typing import Callable, Optional

from ..models.models import ParkingEvent, BatchUpdate, LinkStats
from ..config.config import LinkConfig
from ..des.engine import SimClock
from ..utils import encode_event
from ..constants import compute_lora_airtime_s, LORAWAN_OVERHEAD_BYTES

ForwardCallback = Callable[[ParkingEvent, bytes], None]
ForwardBatchCallback = Callable[[BatchUpdate, bytes], None]


class TokenBucket:
    def __init__(self, rate: float) -> None:
        self.rate = rate
        self._enabled = rate > 0.0
        self.tokens: float = 1.0
        self._last_virtual: float = 0.0
        self._next_free: float = 0.0

    def consume(self, clock: SimClock) -> float:
        if not self._enabled:
            return 0.0
        
        now = clock.now
        elapsed = now - self._last_virtual
        self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
        self._last_virtual = now

        if self._next_free <= now and self.tokens >= 1.0:
            self.tokens -= 1.0
            self._next_free = now + 1.0 / self.rate
            return 0.0
        else:
            self._next_free = max(self._next_free, now) + 1.0 / self.rate
            self.tokens = 0.0
            return self._next_free - 1.0 / self.rate - now


class GilbertElliotModel:
    def __init__(self, base_loss_rate: float, burst_enabled: bool = True, p_loss_bad: float = 0.50, burst_mean_length: float = 4.0, rng: random.Random | None = None) -> None:
        self.rng = rng or random.Random()
        self.burst_enabled = burst_enabled

        if not burst_enabled or base_loss_rate <= 0.0:
            self._bernoulli_rate = base_loss_rate
            self._in_bad = False
            self.burst_enabled = False
            return

        p_loss_good = max(0.001, base_loss_rate / 5.0)
        p_bg = 1.0 / max(burst_mean_length, 1.5)

        if abs(p_loss_bad - p_loss_good) < 1e-9:
            pi_bad = 0.0
        else:
            pi_bad = (base_loss_rate - p_loss_good) / (p_loss_bad - p_loss_good)
        pi_bad = max(0.0, min(0.95, pi_bad))

        self._p_gb = pi_bad * p_bg / max(1.0 - pi_bad, 1e-9)
        self._p_bg = p_bg
        self._p_loss_good = p_loss_good
        self._p_loss_bad = p_loss_bad
        self._in_bad = self.rng.random() < pi_bad
        self._bernoulli_rate = 0.0

    @property
    def in_burst(self) -> bool:
        return self._in_bad

    def should_drop(self) -> bool:
        if not self.burst_enabled:
            return self.rng.random() < self._bernoulli_rate

        if self._in_bad:
            if self.rng.random() < self._p_bg:
                self._in_bad = False
        else:
            if self.rng.random() < self._p_gb:
                self._in_bad = True

        threshold = self._p_loss_bad if self._in_bad else self._p_loss_good
        return self.rng.random() < threshold


class QueueOverflowModel:
    def __init__(self, capacity: int = 500) -> None:
        self._capacity = capacity
        self._depth: int = 0
        self.overflow_drops: int = 0

    @property
    def depth(self) -> int:
        return self._depth

    def try_enqueue(self) -> bool:
        if self._capacity <= 0:
            return True
        if self._depth >= self._capacity:
            self.overflow_drops += 1
            return False
        self._depth += 1
        return True

    def dequeue(self) -> None:
        if self._depth > 0:
            self._depth -= 1


class LinkEmulator:
    DEFAULT_QUEUE_CAPACITY: int = 500

    def __init__(self, config: LinkConfig, clock: SimClock, forward_cb: Optional[ForwardCallback] = None, rng: random.Random | None = None,
                 queue_capacity: int | None = None, wall_clock: bool = False) -> None:
        self.config = config
        self.clock = clock
        self._callback = forward_cb
        self._batch_cb: Optional[ForwardBatchCallback] = None
        self.on_drop: Optional[Callable[[], None]] = None
        self.rng = rng or random.Random(hash(config.packet_loss_rate))
        self._wall_clock = wall_clock

        _chan_rng = random.Random(self.rng.randint(0, 2**32))
        self._channel = GilbertElliotModel(base_loss_rate=config.packet_loss_rate, burst_enabled=True, rng=_chan_rng)

        cap = queue_capacity if queue_capacity is not None else self.DEFAULT_QUEUE_CAPACITY
        self._queue = QueueOverflowModel(capacity=cap)

        self.stats = LinkStats(name="sensor_to_edge")
        self._bucket = TokenBucket(config.rate_limit_msgs_per_sec)

    def set_batch_callback(self, cb: ForwardBatchCallback) -> None:
        self._batch_cb = cb

    def _wire_bytes(self, payload: bytes) -> int:
        return len(payload) + self.config.transport_overhead_bytes

    def _compute_delay(self, payload_bytes: int = 0) -> float:
        if self.config.use_lora_airtime and payload_bytes > 0:
            return compute_lora_airtime_s(payload_bytes)
        if self.config.jitter_ms <= 0:
            return self.config.base_delay_ms / 1000.0
        jittered_ms = self.config.base_delay_ms + self.rng.gauss(0, self.config.jitter_ms)
        return max(0.0, jittered_ms / 1000.0)

    def transmit(self, event: ParkingEvent) -> None:
        payload = encode_event(event)
        wire_bytes = self._wire_bytes(payload)

        self.stats.sent += 1
        self.stats.total_bytes_sent += wire_bytes

        if wire_bytes > self.config.max_payload_bytes + self.config.transport_overhead_bytes:
            self.stats.dropped += 1
            if self.on_drop:
                self.on_drop()
            return

        if self._channel.should_drop():
            self.stats.dropped += 1
            if self.on_drop:
                self.on_drop()
            return

        if not self._queue.try_enqueue():
            self.stats.dropped += 1
            if self.on_drop:
                self.on_drop()
            return

        token_delay = self._bucket.consume(self.clock)
        prop_delay = self._compute_delay(len(payload))
        total_delay = token_delay + prop_delay

        def deliver() -> None:
            self._queue.dequeue()
            self.stats.received += 1
            self.stats.total_bytes_received += wire_bytes
            if self._callback:
                self._callback(event, payload)

        if self._wall_clock:
            threading.Timer(total_delay, deliver).start()
        else:
            self.clock.schedule(total_delay, deliver)

    def transmit_batch(self, batch: BatchUpdate, payload: bytes) -> None:
        wire_bytes = self._wire_bytes(payload)
        self.stats.sent += 1
        self.stats.total_bytes_sent += wire_bytes

        if wire_bytes > self.config.max_payload_bytes + self.config.transport_overhead_bytes:
            self.stats.dropped += 1
            if self.on_drop:
                self.on_drop()
            return

        if self._channel.should_drop():
            self.stats.dropped += 1
            if self.on_drop:
                self.on_drop()
            return

        if not self._queue.try_enqueue():
            self.stats.dropped += 1
            if self.on_drop:
                self.on_drop()
            return

        token_delay = self._bucket.consume(self.clock)
        prop_delay = self._compute_delay(len(payload))
        total_delay = token_delay + prop_delay

        def deliver() -> None:
            self._queue.dequeue()
            self.stats.received += 1
            self.stats.total_bytes_received += wire_bytes
            if self._batch_cb:
                self._batch_cb(batch, payload)

        if self._wall_clock:
            threading.Timer(total_delay, deliver).start()
        else:
            self.clock.schedule(total_delay, deliver)

    @property
    def queue_depth(self) -> int:
        return self._queue.depth

    @property
    def overflow_drops(self) -> int:
        return self._queue.overflow_drops