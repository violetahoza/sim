from __future__ import annotations
import logging
import random
from typing import Callable, Optional

from simulator.models.models import BatchUpdate
from simulator.config.config import CoAPConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.config.constants import COAP_HEADER_BYTES, COAP_TOKEN_BYTES, COAP_PAYLOAD_MARKER, COAP_URI_PATH_OPTION_EST, COAP_ACK_BYTES, UDP_TRANSPORT_OVERHEAD

logger = logging.getLogger(__name__)

class SimulatedCoAPBackend(ProtocolBackend):

    def __init__(self, config: CoAPConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.02, seed: int = 0, ack_one_way_delay_s: float = 0.030, 
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
        self.duplicates_suppressed = 0
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

    def _initial_timeout(self) -> float:
        ack_timeout = self.config.ack_timeout_s
        ack_random = self.config.ack_random_factor
        return self._rng.uniform(ack_timeout, ack_timeout * ack_random)

    def _frame_bytes(self, payload: bytes) -> int:
        return (UDP_TRANSPORT_OVERHEAD + COAP_HEADER_BYTES + COAP_TOKEN_BYTES +
                COAP_URI_PATH_OPTION_EST + COAP_PAYLOAD_MARKER + len(payload))

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        msg_id = self._next_id()
        self.frames_offered += 1
        if self.config.mode == "NON":
            self._send_non(batch, payload, msg_id)
        else:
            self._send_con(batch, payload, msg_id, attempt=0, timeout=self._initial_timeout())

    def _release(self, batch: BatchUpdate, payload: bytes, msg_id: int) -> None:
        if msg_id in self._delivered:
            self.duplicates_suppressed += 1
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

    def _send_non(self, batch: BatchUpdate, payload: bytes, msg_id: int) -> None:
        self.bytes_sent += self._frame_bytes(payload)
        if self._uplink_drop():
            self._drop(msg_id)
            return
        self._release(batch, payload, msg_id)

    def _retransmit(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int, timeout: float) -> None:
        if attempt >= self.config.max_retransmit:
            self._drop(msg_id)
            return
        self.retransmitted += 1
        self._dirty.add(msg_id)
        self.clock.schedule(timeout, lambda: self._send_con(batch, payload, msg_id, attempt + 1, timeout * 2.0))

    def _send_con(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int, timeout: float) -> None:
        self.bytes_sent += self._frame_bytes(payload)

        if self._uplink_drop():
            self._retransmit(batch, payload, msg_id, attempt, timeout)
            return

        def ack() -> None:
            if self._downlink_drop():
                self._retransmit(batch, payload, msg_id, attempt, timeout)
            else:
                self.bytes_sent += COAP_ACK_BYTES
                self._release(batch, payload, msg_id)

        self.clock.schedule(self._ack_delay(), ack)