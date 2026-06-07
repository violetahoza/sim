from __future__ import annotations
import logging
import random
from typing import Callable, Optional

from simulator.models.models import BatchUpdate
from simulator.config.config import CoAPConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback


logger = logging.getLogger(__name__)

COAP_ACK_TIMEOUT_S = 2.0
COAP_ACK_RANDOM_FACTOR = 1.5
COAP_MAX_RETRANSMIT = 4
COAP_HEADER_BYTES = 4
COAP_TOKEN_BYTES = 4
CBOR_RATIO = 0.65
_OVERHEAD_S = {"CON": 0.0005, "NON": 0.0002}


class SimulatedCoAPBackend(ProtocolBackend):

    def __init__(self, config: CoAPConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.02, seed: int = 0, ack_one_way_delay_s: float = 0.030, ack_jitter_s: float = 0.010) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.bytes_sent = 0
        self.retransmitted = 0
        self.duplicates_suppressed = 0
        self.frames_offered = 0
        self.frames_delivered = 0
        self.frames_dropped = 0
        self.first_pass_delivered = 0
        self._delivered_ids: dict[int, bool] = {}
        self._non_delivered_ids: set[int] = set()
        self._msg_seq = 0
        self.on_drop: Optional[Callable[[], None]] = None
        self._ack_one_way_s = ack_one_way_delay_s
        self._ack_jitter_s = ack_jitter_s

    def _next_msg_id(self) -> int:
        self._msg_seq += 1
        return self._msg_seq

    def _ack_delay(self) -> float:
        if self._ack_jitter_s <= 0:
            return self._ack_one_way_s
        return max(0.0, self._ack_one_way_s + self._rng.gauss(0, self._ack_jitter_s))

    def _initial_timeout(self) -> float:
        return self._rng.uniform(COAP_ACK_TIMEOUT_S, COAP_ACK_TIMEOUT_S * COAP_ACK_RANDOM_FACTOR)

    def _con_frame_bytes(self, payload: bytes) -> int:
        return int(len(payload) * CBOR_RATIO) + COAP_HEADER_BYTES + COAP_TOKEN_BYTES

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        self.bytes_sent += self._con_frame_bytes(payload)
        msg_id = self._next_msg_id()
        self.frames_offered += 1
        if self.config.mode == "NON":
            self._send_non(batch, payload, msg_id)
        else:
            self._send_con(batch, payload, msg_id, attempt=0, timeout=self._initial_timeout())

    def _retransmit_or_drop(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int, timeout: float) -> None:
        if attempt < COAP_MAX_RETRANSMIT:
            self.retransmitted += 1
            self.bytes_sent += self._con_frame_bytes(payload)
            next_timeout = timeout * 2.0
            self.clock.schedule(timeout, lambda a=attempt + 1, t=next_timeout: self._send_con(batch, payload, msg_id, a, t))
        else:
            self.frames_dropped += 1
            if self.on_drop:
                self.on_drop()

    def _send_non(self, batch: BatchUpdate, payload: bytes, msg_id: int) -> None:
        overhead = _OVERHEAD_S["NON"]

        def deliver() -> None:
            if self._rng.random() < self.loss_rate:
                self.frames_dropped += 1
                if self.on_drop:
                    self.on_drop()
                return
            if msg_id in self._non_delivered_ids:
                self.duplicates_suppressed += 1
                return
            self._non_delivered_ids.add(msg_id)
            self.frames_delivered += 1
            self.first_pass_delivered += 1
            self._subscriber(batch, payload)

        self.clock.schedule(overhead, deliver)

    def _send_con(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int, timeout: float) -> None:
        overhead = _OVERHEAD_S["CON"]

        def at_server() -> None:
            if self._rng.random() < self.loss_rate:
                self._retransmit_or_drop(batch, payload, msg_id, attempt, timeout)
                return
            if not self._delivered_ids.get(msg_id):
                self._delivered_ids[msg_id] = True
                self.frames_delivered += 1
                if attempt == 0:
                    self.first_pass_delivered += 1
                self._subscriber(batch, payload)
            else:
                self.duplicates_suppressed += 1

            def ack_arrives() -> None:
                if self._rng.random() < self.loss_rate:
                    self._retransmit_or_drop(batch, payload, msg_id, attempt, timeout)

            self.clock.schedule(self._ack_delay(), ack_arrives)

        self.clock.schedule(overhead, at_server)