from __future__ import annotations
import random
from typing import Callable

from ..models import BatchUpdate
from ..config import MQTTConfig
from ..des.engine import SimClock

SubscriberCb = Callable[[BatchUpdate, bytes], None]


class SimulatedMQTTBackend:
    QOS_OVERHEAD_S = {0: 0.0, 1: 0.005, 2: 0.015}
    RETRY_DELAY_S = 1.0
    MAX_RETRIES = 3

    def __init__(
        self,
        config: MQTTConfig,
        clock: SimClock,
        subscriber_cb: SubscriberCb,
        loss_rate: float = 0.0,
        seed: int = 0,
    ) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.sent = 0
        self.bytes_sent = 0
        self.retransmitted = 0

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        mqtt_overhead = 2 + len(f"parking/{batch.edge_id}/update".encode())
        self.sent += 1
        self.bytes_sent += len(payload) + mqtt_overhead
        self._attempt(batch, payload, attempt=0)

    def _attempt(self, batch: BatchUpdate, payload: bytes, attempt: int) -> None:
        overhead = self.QOS_OVERHEAD_S[self.config.qos]

        def on_overhead_elapsed() -> None:
            if self._rng.random() < self.loss_rate:
                if self.config.qos == 0:
                    return  # fire-and-forget: silently lost
                self.retransmitted += 1
                if attempt < self.MAX_RETRIES - 1:
                    backoff = self.RETRY_DELAY_S * (2 ** attempt)
                    self.clock.schedule(backoff, lambda a=attempt + 1: self._attempt(batch, payload, a))
                return
            self._subscriber(batch, payload)

        self.clock.schedule(overhead, on_overhead_elapsed)
