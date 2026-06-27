from __future__ import annotations
import logging
import random
from typing import Callable, Optional

from simulator.models.models import BatchUpdate
from simulator.config.config import AMQPConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.config.constants import (AMQP_FRAME_ENVELOPE, AMQP_PUBLISH_METHOD_FIXED, AMQP_CONTENT_HEADER_FIXED, AMQP_PROPERTY_TABLE_EST,
    AMQP_DURABLE_PROPERTY, AMQP_ACK_FRAME, TCP_TRANSPORT_OVERHEAD)

logger = logging.getLogger(__name__)

class SimulatedAMQPBackend(ProtocolBackend):

    def __init__(self, config: AMQPConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.01, seed: int = 0, ack_one_way_delay_s: float = 0.030, 
                 ack_jitter_s: float = 0.010, downlink_loss_rate: Optional[float] = None, loss_provider: Optional[Callable[[float], float]] = None) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.uplink_loss = loss_rate
        self.downlink_loss = loss_rate if downlink_loss_rate is None else downlink_loss_rate
        self._loss_provider = loss_provider
        self._rng = random.Random(seed)
        self.bytes_sent = 0
        self.retransmitted = 0
        self.duplicates_delivered = 0
        self.frames_offered = 0
        self.frames_delivered = 0
        self.frames_dropped = 0
        self.first_pass_delivered = 0
        self._delivered: set[int] = set()
        self._dirty: set[int] = set()
        self._msg_seq = 0
        self.on_drop: Optional[Callable[[], None]] = None
        self._ack_one_way_s = ack_one_way_delay_s
        self._ack_jitter_s = ack_jitter_s

    def _uplink_drop(self) -> bool:
        rate = self._loss_provider(self.clock.now) if self._loss_provider is not None else self.uplink_loss
        return self._rng.random() < rate

    def _downlink_drop(self) -> bool:
        rate = self._loss_provider(self.clock.now) if self._loss_provider is not None else self.downlink_loss
        return self._rng.random() < rate

    def _next_id(self) -> int:
        self._msg_seq += 1
        return self._msg_seq

    def _ack_delay(self) -> float:
        if self._ack_jitter_s <= 0:
            return self._ack_one_way_s
        return max(0.0, self._ack_one_way_s + self._rng.gauss(0, self._ack_jitter_s))

    def _routing_key(self, batch: BatchUpdate) -> str:
        if self.config.exchange_type == "fanout":
            return ""
        if self.config.exchange_type == "topic":
            return f"parking.{batch.edge_id}.update"
        return f"parking.{batch.edge_id}"

    def _publish_bytes(self, batch: BatchUpdate, payload: bytes) -> int:
        rk = self._routing_key(batch)
        method = AMQP_PUBLISH_METHOD_FIXED + len(self.config.exchange.encode()) + len(rk.encode())
        header = AMQP_CONTENT_HEADER_FIXED + AMQP_PROPERTY_TABLE_EST + (AMQP_DURABLE_PROPERTY if self.config.durable else 0)
        return TCP_TRANSPORT_OVERHEAD + 3 * AMQP_FRAME_ENVELOPE + method + header + len(payload)

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        self.frames_offered += 1
        self._do_publish(batch, payload, self._next_id(), attempt=0)

    def _release(self, batch: BatchUpdate, payload: bytes, msg_id: int) -> None:
        if msg_id in self._delivered:
            self.duplicates_delivered += 1
            self._subscriber(batch, payload)
            return
        self._delivered.add(msg_id)
        self.frames_delivered += 1
        if msg_id not in self._dirty:
            self.first_pass_delivered += 1
        self._subscriber(batch, payload)

    def _drop(self, msg_id: int) -> None:
        if msg_id in self._delivered:
            return
        self.frames_dropped += 1
        if self.on_drop:
            self.on_drop()

    def _requeue(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        if attempt >= self.config.max_redeliveries:
            self._drop(msg_id)
            return
        self.retransmitted += 1
        self._dirty.add(msg_id)
        delay = self.config.requeue_delay_s * (2 ** attempt)
        self.clock.schedule(delay, lambda: self._do_publish(batch, payload, msg_id, attempt + 1))

    def _do_publish(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        self.bytes_sent += self._publish_bytes(batch, payload)

        if self._uplink_drop():
            if self.config.ack_mode == "auto":
                self._drop(msg_id)
            else:
                self._requeue(batch, payload, msg_id, attempt)
            return
        
        self.bytes_sent += AMQP_ACK_FRAME
        if self.config.ack_mode == "auto":
            self._release(batch, payload, msg_id)
            return

        def consumer_ack() -> None:
            if self._downlink_drop():
                self._requeue(batch, payload, msg_id, attempt)
            else:
                self.bytes_sent += AMQP_ACK_FRAME
                self._release(batch, payload, msg_id)

        self.clock.schedule(self._ack_delay(), consumer_ack)