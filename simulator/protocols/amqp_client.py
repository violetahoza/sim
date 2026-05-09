from __future__ import annotations
import random
from typing import Callable

from ..models import BatchUpdate
from ..config import AMQPConfig
from ..des.engine import SimClock

SubscriberCb = Callable[[BatchUpdate, bytes], None]

AMQP_FRAME_OVERHEAD = 37
AMQP_EXCHANGE_OVERHEAD = 16


class SimulatedAMQPBackend:
    EXCHANGE_OVERHEAD_S = {"direct": 0.002, "fanout": 0.003, "topic": 0.004}
    ACK_OVERHEAD_S = {"auto": 0.0, "manual": 0.006}
    DURABLE_OVERHEAD_S = 0.004
    RETRY_DELAY_S = 1.0
    MAX_RETRIES = 3

    def __init__(
        self,
        config: AMQPConfig,
        clock: SimClock,
        subscriber_cb: SubscriberCb,
        loss_rate: float = 0.01,
        seed: int = 0,
    ) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.sent = 0
        self.bytes_sent = 0
        self.nacked = 0
        self.retransmitted = 0

    def _overhead_s(self, include_durable: bool = True) -> float:
        return (
            self.EXCHANGE_OVERHEAD_S.get(self.config.exchange_type, 0.002)
            + self.ACK_OVERHEAD_S.get(self.config.ack_mode, 0.0)
            + (self.DURABLE_OVERHEAD_S if (self.config.durable and include_durable) else 0.0)
        )

    def _routing_key(self, batch: BatchUpdate) -> str:
        if self.config.exchange_type == "fanout":
            return ""
        if self.config.exchange_type == "topic":
            return f"parking.{batch.edge_id}.update"
        return f"parking.{batch.edge_id}"

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        rk = self._routing_key(batch)
        total_bytes = len(payload) + AMQP_FRAME_OVERHEAD + AMQP_EXCHANGE_OVERHEAD + len(rk.encode())
        self.sent += 1
        self.bytes_sent += total_bytes
        self._attempt(batch, payload, attempt=0)

    def _attempt(self, batch: BatchUpdate, payload: bytes, attempt: int) -> None:
        overhead = self._overhead_s(include_durable=(attempt == 0))

        def on_overhead() -> None:
            if self._rng.random() < self.loss_rate:
                self.nacked += 1
                if self.config.ack_mode == "manual" and attempt < self.MAX_RETRIES - 1:
                    backoff = self.RETRY_DELAY_S * (2 ** attempt)
                    self.retransmitted += 1
                    self.clock.schedule(backoff, lambda a=attempt + 1: self._attempt(batch, payload, a))
                return
            self._subscriber(batch, payload)

        self.clock.schedule(overhead, on_overhead)
