from __future__ import annotations
import logging
import random
from typing import Callable, Optional

from simulator.models.models import BatchUpdate
from simulator.config.config import MQTTConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback

logger = logging.getLogger(__name__)

_TOPIC_TMPL = "{prefix}/{edge_id}/update"

MQTT_CONTROL_BYTE = 1
MQTT_PACKET_ID_BYTES = 2
MQTT_TOPIC_LEN_BYTES = 2
MQTT_ACK_BYTES = 4

_MAX_RETRIES = {0: 0, 1: 3, 2: 3}


def _remaining_length_bytes(n: int) -> int:
    if n < 128:
        return 1
    if n < 16384:
        return 2
    if n < 2097152:
        return 3
    return 4


class SimulatedMQTTBackend(ProtocolBackend):

    def __init__(self, config: MQTTConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.0, seed: int = 0, ack_one_way_delay_s: float = 0.030, ack_jitter_s: float = 0.010, downlink_loss_rate: Optional[float] = None, loss_provider: Optional[Callable[[float], float]] = None) -> None:
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
        self._rto_s = max(0.2, 3.0 * (2.0 * ack_one_way_delay_s))

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

    def _publish_bytes(self, batch: BatchUpdate, payload: bytes, qos: int) -> int:
        topic = _TOPIC_TMPL.format(prefix=self.config.topic_prefix, edge_id=batch.edge_id)
        remaining = MQTT_TOPIC_LEN_BYTES + len(topic.encode()) + (MQTT_PACKET_ID_BYTES if qos > 0 else 0) + len(payload)
        return MQTT_CONTROL_BYTE + _remaining_length_bytes(remaining) + remaining

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        qos = self.config.qos
        self.frames_offered += 1
        self._send_publish(batch, payload, self._next_id(), qos, attempt=0)

    def _deliver(self, batch: BatchUpdate, payload: bytes, msg_id: int) -> None:
        if msg_id in self._delivered:
            self.duplicates_delivered += 1
            self._subscriber(batch, payload)
            return
        self._release(batch, payload, msg_id)

    def _release(self, batch: BatchUpdate, payload: bytes, msg_id: int) -> None:
        if msg_id in self._delivered:
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

    def _retry_publish(self, batch: BatchUpdate, payload: bytes, msg_id: int, qos: int, attempt: int) -> None:
        if attempt >= _MAX_RETRIES[qos]:
            self._drop(msg_id)
            return
        self.retransmitted += 1
        self._dirty.add(msg_id)
        self.clock.schedule(self._rto_s, lambda: self._send_publish(batch, payload, msg_id, qos, attempt + 1))

    def _retry_pubrel(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        if attempt >= _MAX_RETRIES[2]:
            self._drop(msg_id)
            return
        self.retransmitted += 1
        self._dirty.add(msg_id)
        self.clock.schedule(self._rto_s, lambda: self._send_pubrel(batch, payload, msg_id, attempt + 1))

    def _send_publish(self, batch: BatchUpdate, payload: bytes, msg_id: int, qos: int, attempt: int) -> None:
        self.bytes_sent += self._publish_bytes(batch, payload, qos)
        if self._uplink_drop():
            self._retry_publish(batch, payload, msg_id, qos, attempt)
            return
        if qos == 0:
            self._deliver(batch, payload, msg_id)
            return
        if qos == 1:
            self._deliver(batch, payload, msg_id)
            self.bytes_sent += MQTT_ACK_BYTES

            def puback() -> None:
                if self._downlink_drop():
                    self._retry_publish(batch, payload, msg_id, qos, attempt)

            self.clock.schedule(self._ack_delay(), puback)
            return
        self.bytes_sent += MQTT_ACK_BYTES

        def pubrec() -> None:
            if self._downlink_drop():
                self._retry_publish(batch, payload, msg_id, qos, attempt)
                return
            self._send_pubrel(batch, payload, msg_id, attempt=0)

        self.clock.schedule(self._ack_delay(), pubrec)

    def _send_pubrel(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        self.bytes_sent += MQTT_ACK_BYTES
        if self._uplink_drop():
            self._retry_pubrel(batch, payload, msg_id, attempt)
            return
        self._release(batch, payload, msg_id)
        self.bytes_sent += MQTT_ACK_BYTES

        def pubcomp() -> None:
            if self._downlink_drop():
                self._retry_pubrel(batch, payload, msg_id, attempt)

        self.clock.schedule(self._ack_delay(), pubcomp)