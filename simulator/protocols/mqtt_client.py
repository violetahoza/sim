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

MQTT_FIXED_HEADER = 2
MQTT_PUBACK_BYTES = 4
MQTT_PUBREL_BYTES = 4

_QOS_BROKER_OVERHEAD_S = {0: 0.0005, 1: 0.001, 2: 0.0015}
_MAX_RETRIES = {0: 0, 1: 3, 2: 5}
_RETRY_BASE_S = 1.0


class SimulatedMQTTBackend(ProtocolBackend):

    def __init__(self, config: MQTTConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.0, seed: int = 0, ack_one_way_delay_s: float = 0.030, ack_jitter_s: float = 0.010) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.bytes_sent = 0
        self.retransmitted = 0
        self.duplicates_delivered = 0
        self._qos2_delivered: dict[int, bool] = {}
        self._qos1_delivered: set[int] = set()
        self._msg_seq = 0
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

    def _topic_overhead(self, batch: BatchUpdate) -> int:
        topic = _TOPIC_TMPL.format(prefix=self.config.topic_prefix, edge_id=batch.edge_id)
        return MQTT_FIXED_HEADER + len(topic.encode())

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        topic_oh = self._topic_overhead(batch)
        qos = self.config.qos
        qos_extra = (MQTT_PUBACK_BYTES if qos == 1 else (MQTT_PUBACK_BYTES + MQTT_PUBREL_BYTES * 2) if qos == 2 else 0)
        self.bytes_sent += len(payload) + topic_oh + qos_extra
        msg_id = self._next_id()
        self._do_publish(batch, payload, msg_id, attempt=0)

    def _deliver_to_subscriber(self, batch: BatchUpdate, payload: bytes, msg_id: int, qos: int) -> None:
        if qos == 2:
            if self._qos2_delivered.get(msg_id):
                self.duplicates_delivered += 1
                return
            self._qos2_delivered[msg_id] = True
        elif qos == 1:
            if msg_id in self._qos1_delivered:
                self.duplicates_delivered += 1
                return
            self._qos1_delivered.add(msg_id)
        self._subscriber(batch, payload)

    def _retry_or_drop(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int, qos: int) -> None:
        max_r = _MAX_RETRIES[qos]
        if attempt < max_r:
            backoff = _RETRY_BASE_S * (2 ** attempt)
            self.retransmitted += 1
            self.clock.schedule(backoff, lambda a=attempt + 1: self._do_publish(batch, payload, msg_id, a))
        else:
            if self.on_drop:
                self.on_drop()

    def _do_publish(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        qos = self.config.qos
        broker_oh = _QOS_BROKER_OVERHEAD_S[qos]

        def at_broker() -> None:
            if qos == 0:
                self._deliver_to_subscriber(batch, payload, msg_id, qos)
                return

            self._deliver_to_subscriber(batch, payload, msg_id, qos)

            if qos == 1:
                def puback_arrives() -> None:
                    if self._rng.random() < self.loss_rate:
                        self._retry_or_drop(batch, payload, msg_id, attempt, qos)
                self.clock.schedule(self._ack_delay(), puback_arrives)
                return

            if qos == 2:
                def pubrec_arrives() -> None:
                    if self._rng.random() < self.loss_rate:
                        self._retry_or_drop(batch, payload, msg_id, attempt, qos)
                        return
                    def pubrel_arrives() -> None:
                        if self._rng.random() < self.loss_rate:
                            self._retry_or_drop(batch, payload, msg_id, attempt, qos)
                            return
                        def pubcomp_arrives() -> None:
                            if self._rng.random() < self.loss_rate:
                                self._retry_or_drop(batch, payload, msg_id, attempt, qos)
                        self.clock.schedule(self._ack_delay(), pubcomp_arrives)
                    self.clock.schedule(self._ack_delay(), pubrel_arrives)
                self.clock.schedule(self._ack_delay(), pubrec_arrives)
                return

        self.clock.schedule(broker_oh, at_broker)

