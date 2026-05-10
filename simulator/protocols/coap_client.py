# simulator/protocols/coap_client.py
from __future__ import annotations
import random
from typing import Callable

from ..models import BatchUpdate
from ..config import CoAPConfig
from ..des.engine import SimClock

SubscriberCb = Callable[[BatchUpdate, bytes], None]

COAP_HEADER_BYTES = 4
COAP_TOKEN_BYTES = 4


class SimulatedCoAPBackend:

    CON_OVERHEAD_S = 0.008
    NON_OVERHEAD_S = 0.001
    CON_RETRY_TIMEOUT_S = 2.0
    CON_MAX_RETRIES = 4
    CBOR_COMPRESSION = 0.65

    def __init__(
        self,
        config: CoAPConfig,
        clock: SimClock,
        subscriber_cb: SubscriberCb,
        loss_rate: float = 0.02,
        seed: int = 0,
    ) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.bytes_sent = 0      
        self.retransmissions = 0

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        coap_bytes = int(len(payload) * self.CBOR_COMPRESSION)
        self.bytes_sent += coap_bytes + COAP_HEADER_BYTES + COAP_TOKEN_BYTES
        if self.config.mode == "NON":
            self._attempt_non(batch, payload)
        else:
            self._attempt_con(batch, payload, attempt=0)

    def _attempt_non(self, batch: BatchUpdate, payload: bytes) -> None:
        def deliver() -> None:
            if self._rng.random() >= self.loss_rate:
                self._subscriber(batch, payload)

        self.clock.schedule(self.NON_OVERHEAD_S, deliver)

    def _attempt_con(self, batch: BatchUpdate, payload: bytes, attempt: int) -> None:
        def on_overhead() -> None:
            if self._rng.random() < self.loss_rate:
                self.retransmissions += 1
                if attempt < self.CON_MAX_RETRIES - 1:
                    timeout = self.CON_RETRY_TIMEOUT_S * (2 ** attempt)
                    self.clock.schedule(timeout, lambda a=attempt + 1: self._attempt_con(batch, payload, a))
                return
            self._subscriber(batch, payload)

        self.clock.schedule(self.CON_OVERHEAD_S, on_overhead)