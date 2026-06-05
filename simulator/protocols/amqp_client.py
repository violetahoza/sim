from __future__ import annotations
import logging
import random
from typing import Callable, Optional

from simulator.models.models import BatchUpdate
from simulator.config.config import AMQPConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback

logger = logging.getLogger(__name__)

AMQP_FRAME_OVERHEAD = 37
AMQP_EXCHANGE_OVERHEAD = 16

_EXCHANGE_OVERHEAD_S: dict[str, float] = {"direct": 0.0005, "fanout": 0.0008, "topic": 0.0015}
_DURABLE_OVERHEAD_S = 0.0010
_CONFIRM_OVERHEAD_S = 0.0005

_MAX_RETRIES = 3
_RETRY_BASE_S = 1.0


class SimulatedAMQPBackend(ProtocolBackend):

    def __init__(self, config: AMQPConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.01, seed: int = 0, ack_one_way_delay_s: float = 0.030, ack_jitter_s: float = 0.010) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.bytes_sent = 0
        self.nacked = 0
        self.retransmitted = 0
        self._msg_seq = 0
        self._delivered_ids: set[int] = set()
        self.on_drop: Optional[Callable[[], None]] = None
        self._ack_one_way_s = ack_one_way_delay_s
        self._ack_jitter_s = ack_jitter_s

    def _next_id(self) -> int:
        self._msg_seq += 1
        return self._msg_seq

    def _ack_delay(self) -> float:
        if self._ack_jitter_s <= 0:
            return self._ack_one_way_s
        return max(0.0, self._ack_one_way_s + self._rng.gauss(0, self._ack_jitter_s))

    def _broker_overhead_s(self) -> float:
        return (_EXCHANGE_OVERHEAD_S.get(self.config.exchange_type, 0.0005) + (_DURABLE_OVERHEAD_S if self.config.durable else 0.0) + _CONFIRM_OVERHEAD_S)

    def _routing_key(self, batch: BatchUpdate) -> str:
        if self.config.exchange_type == "fanout":
            return ""
        if self.config.exchange_type == "topic":
            return f"parking.{batch.edge_id}.update"
        return f"parking.{batch.edge_id}"

    def _frame_bytes(self, batch: BatchUpdate, payload: bytes) -> int:
        rk = self._routing_key(batch)
        return len(payload) + AMQP_FRAME_OVERHEAD + AMQP_EXCHANGE_OVERHEAD + len(rk.encode())

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        self.bytes_sent += self._frame_bytes(batch, payload)
        msg_id = self._next_id()
        self._do_publish(batch, payload, msg_id, attempt=0)

    def _do_publish(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        broker_oh = self._broker_overhead_s()

        def at_broker() -> None:
            if msg_id not in self._delivered_ids:
                self._delivered_ids.add(msg_id)
                self._subscriber(batch, payload)

            if self.config.ack_mode == "auto":
                return

            def consumer_ack_arrives() -> None:
                if self._rng.random() < self.loss_rate:
                    self.nacked += 1
                    if attempt < _MAX_RETRIES - 1:
                        backoff = _RETRY_BASE_S * (2 ** attempt)
                        self.retransmitted += 1
                        self.bytes_sent += self._frame_bytes(batch, payload)
                        self.clock.schedule(backoff, lambda a=attempt + 1: self._do_publish(batch, payload, msg_id, a))
                    else:
                        if self.on_drop:
                            self.on_drop()

            self.clock.schedule(self._ack_delay(), consumer_ack_arrives)

        self.clock.schedule(broker_oh, at_broker)