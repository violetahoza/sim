from __future__ import annotations
import asyncio
import json
import logging
import random
from typing import Callable, Optional
import aiocoap
import aiocoap.resource as resource

from simulator.models import BatchUpdate
from simulator.config import CoAPConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.protocols.broker_config import CoAPBrokerConfig
from simulator.protocols.mqtt_client import _batch_from_dict

logger = logging.getLogger(__name__)

COAP_ACK_TIMEOUT_S = 2.0
COAP_ACK_RANDOM_FACTOR = 1.5
COAP_MAX_RETRANSMIT = 4
COAP_NSTART = 1
COAP_HEADER_BYTES = 4
COAP_TOKEN_BYTES = 4
CBOR_RATIO = 0.65


class SimulatedCoAPBackend(ProtocolBackend):

    _OVERHEAD_S = {"CON": 0.008, "NON": 0.001}

    def __init__(self, config: CoAPConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.02, seed: int = 0) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.bytes_sent = 0
        self.retransmissions = 0
        self.duplicates_suppressed = 0
        self._delivered_ids: dict[int, bool] = {}
        self._non_delivered_ids: set[int] = set()
        self._msg_seq = 0
        self.on_drop: Optional[Callable[[], None]] = None

    def _next_msg_id(self) -> int:
        self._msg_seq += 1
        return self._msg_seq

    def _initial_timeout(self) -> float:
        return self._rng.uniform(COAP_ACK_TIMEOUT_S, COAP_ACK_TIMEOUT_S * COAP_ACK_RANDOM_FACTOR)

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        coap_bytes = int(len(payload) * CBOR_RATIO)
        self.bytes_sent += coap_bytes + COAP_HEADER_BYTES + COAP_TOKEN_BYTES
        msg_id = self._next_msg_id()
        if self.config.mode == "NON":
            self._send_non(batch, payload, msg_id)
        else:
            self._send_con(batch, payload, msg_id, attempt=0, timeout=self._initial_timeout())

    def _send_non(self, batch: BatchUpdate, payload: bytes, msg_id: int) -> None:
        overhead = self._OVERHEAD_S["NON"]

        def deliver() -> None:
            if self._rng.random() < self.loss_rate:
                if self.on_drop:
                    self.on_drop()
                return
            if msg_id in self._non_delivered_ids:
                self.duplicates_suppressed += 1
                return
            self._non_delivered_ids.add(msg_id)
            self._subscriber(batch, payload)

        self.clock.schedule(overhead, deliver)

    def _send_con(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int, timeout: float) -> None:
        overhead = self._OVERHEAD_S["CON"]

        def on_transmit() -> None:
            if self._rng.random() < self.loss_rate:
                if attempt < COAP_MAX_RETRANSMIT:
                    self.retransmissions += 1
                    next_timeout = timeout * 2.0
                    self.clock.schedule(timeout, lambda a=attempt + 1, t=next_timeout: self._send_con(batch, payload, msg_id, a, t))
                else:
                    if self.on_drop:
                        self.on_drop()
                return

            if self._delivered_ids.get(msg_id):
                self.duplicates_suppressed += 1
                return
            self._delivered_ids[msg_id] = True
            self._subscriber(batch, payload)

        self.clock.schedule(overhead, on_transmit)


class RealCoAPBackend(ProtocolBackend):

    CBOR_COMPRESSION = CBOR_RATIO

    def __init__(self, config: CoAPConfig, broker: CoAPBrokerConfig, cloud_recv_cb: CloudRecvCallback, scenario_name: str = "run") -> None:
        self.config = config
        self.broker = broker
        self._cloud_recv_cb = cloud_recv_cb
        self._scenario_name = scenario_name
        self.bytes_sent: int = 0
        self.retransmissions: int = 0
        self._server_context = None
        self._client_context = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        root = resource.Site()
        root.add_resource(["parking", "update"], _ParkingUpdateResource(self._cloud_recv_cb))
        self._server_context = await aiocoap.Context.create_server_context(root, bind=(self.broker.host, self.broker.port))
        logger.info(f"[CoAP-real] Server on coap://{self.broker.host}:{self.broker.port}/parking/update")
        self._client_context = await aiocoap.Context.create_client_context()
        logger.info("[CoAP-real] Client context ready.")

    async def stop(self) -> None:
        if self._client_context:
            await self._client_context.shutdown()
        if self._server_context:
            await self._server_context.shutdown()
        logger.info("[CoAP-real] Shut down.")

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        if self._loop is None:
            raise RuntimeError("RealCoAPBackend.start() was not called")
        coap_bytes = int(len(payload) * self.CBOR_COMPRESSION)
        self.bytes_sent += coap_bytes + COAP_HEADER_BYTES + COAP_TOKEN_BYTES
        asyncio.ensure_future(self._async_post(payload), loop=self._loop)

    async def _async_post(self, payload: bytes) -> None:
        if self._client_context is None:
            return
        uri = f"coap://{self.broker.host}:{self.broker.port}/parking/update"
        mtype = aiocoap.CON if self.config.mode == "CON" else aiocoap.NON
        request = aiocoap.Message(mtype=mtype, code=aiocoap.Code.POST, uri=uri, payload=payload)
        try:
            response = await self._client_context.request(request).response
            if not response.code.is_successful():
                logger.warning(f"[CoAP-real] POST response: {response.code}")
        except Exception as exc:
            logger.warning(f"[CoAP-real] POST failed: {exc}")
            self.retransmissions += 1


class _ParkingUpdateResource:
    def __init__(self, cloud_recv_cb: CloudRecvCallback) -> None:
        self._cb = cloud_recv_cb

    async def render_post(self, request):
        raw: bytes = request.payload
        try:
            data = json.loads(raw)
            batch = _batch_from_dict(data)
            self._cb(batch, raw)
        except Exception:
            logger.exception("[CoAP-real] Error processing POST")
            return aiocoap.Message(code=aiocoap.Code.INTERNAL_SERVER_ERROR)
        return aiocoap.Message(code=aiocoap.Code.CHANGED)
