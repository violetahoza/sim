from __future__ import annotations
import asyncio
import json
import logging
import random
import aiocoap 
import aiocoap.resource as resource

from simulator.models import BatchUpdate
from simulator.config import CoAPConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.protocols.broker_config import CoAPBrokerConfig
from simulator.protocols.mqtt_client import _batch_from_dict

logger = logging.getLogger(__name__)

COAP_HEADER_BYTES = 4
COAP_TOKEN_BYTES = 4


class SimulatedCoAPBackend(ProtocolBackend):
    CON_OVERHEAD_S = 0.008
    NON_OVERHEAD_S = 0.001
    CON_RETRY_TIMEOUT_S = 2.0
    CON_MAX_RETRIES = 4
    CBOR_COMPRESSION = 0.65

    def __init__(self, config: CoAPConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.02, seed: int = 0) -> None:
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
                    timeout = self.CON_RETRY_TIMEOUT_S * (2**attempt)
                    self.clock.schedule(
                        timeout,
                        lambda a=attempt + 1: self._attempt_con(batch, payload, a),
                    )
                return
            self._subscriber(batch, payload)

        self.clock.schedule(self.CON_OVERHEAD_S, on_overhead)


class RealCoAPBackend(ProtocolBackend):

    CBOR_COMPRESSION = 0.65 

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
        bind_addr = f"{self.broker.host}:{self.broker.port}"
        self._server_context = await aiocoap.Context.create_server_context(root, bind=(self.broker.host, self.broker.port))
        logger.info(f"[CoAP-real] Server listening on coap://{bind_addr}/parking/update")
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

        uri = (
            f"coap://{self.broker.host}:{self.broker.port}"
            f"/parking/update"
        )
        msg_type = (aiocoap.CON if self.config.mode == "CON" else aiocoap.NON)
        request = aiocoap.Message(mtype=msg_type, code=aiocoap.Code.POST, uri=uri, payload=payload)
        try:
            response = await self._client_context.request(request).response
            if response.code.is_successful():
                pass  # cloud_recv_cb already called server-side
            else:
                logger.warning(f"[CoAP-real] POST response: {response.code}")
        except Exception as exc:
            logger.warning(f"[CoAP-real] POST failed: {exc}")
            self.retransmissions += 1

class _ParkingUpdateResource:

    def __init__(self, cloud_recv_cb: CloudRecvCallback) -> None:
        self._cb = cloud_recv_cb

    async def render_post(self, request):
        import aiocoap

        raw: bytes = request.payload
        try:
            data = json.loads(raw)
            batch = _batch_from_dict(data)
            self._cb(batch, raw)
        except Exception:
            logger.exception("[CoAP-real] Error processing POST")
            return aiocoap.Message(code=aiocoap.Code.INTERNAL_SERVER_ERROR)
        return aiocoap.Message(code=aiocoap.Code.CHANGED)