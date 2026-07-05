from __future__ import annotations
import logging
import random
from typing import Callable, Optional

from simulator.models.models import BatchUpdate
from simulator.config.config import MQTTConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.config.constants import MQTT_CONTROL_BYTE, MQTT_PACKET_ID_BYTES, MQTT_TOPIC_LEN_BYTES, MQTT_ACK_WIRE_BYTES, TCP_TRANSPORT_OVERHEAD, TCP_LOCAL_RETRY_ATTEMPTS
from simulator.link.tcp_transport import tcp_transport_outcome, tcp_extra_delay, ConnectionOutageModel

logger = logging.getLogger(__name__)

_TOPIC_TMPL = "{prefix}/{edge_id}/update"

def _remaining_length_bytes(n: int) -> int:
    if n < 128:
        return 1
    if n < 16384:
        return 2
    if n < 2097152:
        return 3
    return 4


class SimulatedMQTTBackend(ProtocolBackend):

    def __init__(self, config: MQTTConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.0, seed: int = 0, ack_one_way_delay_s: float = 0.030,
                 ack_jitter_s: float = 0.010, downlink_loss_rate: Optional[float] = None, loss_provider: Optional[Callable[[float], float]] = None,
                 outage: Optional[ConnectionOutageModel] = None) -> None:
        self.config = config
        self._outage = outage
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
        self._tcp_local_retries = TCP_LOCAL_RETRY_ATTEMPTS
        self._tcp_rtt_s = max(0.02, 2.0 * ack_one_way_delay_s)
        self.transport_recovered = 0

    def _uplink_outcome(self) -> tuple[bool, float]:
        rate = self._loss_provider(self.clock.now) if self._loss_provider is not None else self.uplink_loss
        dropped, attempts = tcp_transport_outcome(self._rng, rate, self._tcp_local_retries)
        if not dropped and attempts > 0:
            self.transport_recovered += 1
        return dropped, tcp_extra_delay(attempts, self._tcp_rtt_s)

    def _downlink_outcome(self) -> tuple[bool, float]:
        rate = self._loss_provider(self.clock.now) if self._loss_provider is not None else self.downlink_loss
        dropped, attempts = tcp_transport_outcome(self._rng, rate, self._tcp_local_retries)
        if not dropped and attempts > 0:
            self.transport_recovered += 1
        return dropped, tcp_extra_delay(attempts, self._tcp_rtt_s)

    def _await_reconnect(self, resend: Callable[[], None]) -> bool:
        if self._outage is None or not self._outage.is_down():
            return False
        self.retransmitted += 1
        wait = self._outage.seconds_until_up() + self._ack_delay()
        self.clock.schedule(wait, resend)
        return True

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
        return TCP_TRANSPORT_OVERHEAD + MQTT_CONTROL_BYTE + _remaining_length_bytes(remaining) + remaining

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
        if qos == 0:
            self._drop(msg_id)
            return
        
        max_retries = self.config.max_retries_qos1 if qos == 1 else self.config.max_retries_qos2

        if attempt >= max_retries:
            self._drop(msg_id)
            return
        
        self.retransmitted += 1
        self._dirty.add(msg_id)
        delay = self._rto_s * (2 ** attempt)
        self.clock.schedule(delay, lambda: self._send_publish(batch, payload, msg_id, qos, attempt + 1))

    def _retry_pubrel(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        if attempt >= self.config.max_retries_qos2:
            self._drop(msg_id)
            return
        
        self.retransmitted += 1
        self._dirty.add(msg_id)
        delay = self._rto_s * (2 ** attempt)
        self.clock.schedule(delay, lambda: self._send_pubrel(batch, payload, msg_id, attempt + 1))

    def _send_publish(self, batch: BatchUpdate, payload: bytes, msg_id: int, qos: int, attempt: int) -> None:
        if qos == 0 and self._outage is not None and self._outage.is_down():
            self._drop(msg_id)
            return
        if qos > 0 and self._await_reconnect(lambda: self._send_publish(batch, payload, msg_id, qos, attempt)):
            return

        self.bytes_sent += self._publish_bytes(batch, payload, qos)

        uplink_dropped, uplink_extra = self._uplink_outcome()
        if uplink_dropped:
            self._retry_publish(batch, payload, msg_id, qos, attempt)
            return

        def arrival() -> None:
            if qos == 0:
                self._deliver(batch, payload, msg_id)
                return
            if qos == 1:
                self._deliver(batch, payload, msg_id)

                downlink_dropped, downlink_extra = self._downlink_outcome()

                def puback() -> None:
                    if downlink_dropped:
                        self._retry_publish(batch, payload, msg_id, qos, attempt)
                    else:
                        self.bytes_sent += MQTT_ACK_WIRE_BYTES

                self.clock.schedule(self._ack_delay() + downlink_extra, puback)
                return

            downlink_dropped, downlink_extra = self._downlink_outcome()

            def pubrec() -> None:
                if downlink_dropped:
                    self._retry_publish(batch, payload, msg_id, qos, attempt)
                    return
                self.bytes_sent += MQTT_ACK_WIRE_BYTES
                self._send_pubrel(batch, payload, msg_id, attempt=0)

            self.clock.schedule(self._ack_delay() + downlink_extra, pubrec)

        if attempt > 0:
            self.clock.schedule(self._ack_delay() + uplink_extra, arrival)
        elif uplink_extra > 0.0:
            self.clock.schedule(uplink_extra, arrival)
        else:
            arrival()

    def _send_pubrel(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        if self._await_reconnect(lambda: self._send_pubrel(batch, payload, msg_id, attempt)):
            return

        self.bytes_sent += MQTT_ACK_WIRE_BYTES

        uplink_dropped, uplink_extra = self._uplink_outcome()
        if uplink_dropped:
            self._retry_pubrel(batch, payload, msg_id, attempt)
            return

        def arrival() -> None:
            self._release(batch, payload, msg_id)

            downlink_dropped, downlink_extra = self._downlink_outcome()

            def pubcomp() -> None:
                if downlink_dropped:
                    self._retry_pubrel(batch, payload, msg_id, attempt)
                    return
                self.bytes_sent += MQTT_ACK_WIRE_BYTES

            self.clock.schedule(self._ack_delay() + downlink_extra, pubcomp)

        self.clock.schedule(self._ack_delay() + uplink_extra, arrival)