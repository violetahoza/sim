from __future__ import annotations
import asyncio
import json
import logging
import random
from typing import Callable, Optional
import aio_pika

from simulator.models import BatchUpdate
from simulator.config import AMQPConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.protocols.broker_config import AMQPBrokerConfig
from simulator.protocols.mqtt_client import _batch_from_dict

logger = logging.getLogger(__name__)

AMQP_FRAME_OVERHEAD = 37
AMQP_EXCHANGE_OVERHEAD = 16

_EXCHANGE_OVERHEAD_S: dict[str, float] = {"direct": 0.002, "fanout": 0.003, "topic": 0.005}
_ACK_OVERHEAD_S: dict[str, float] = {"auto": 0.0, "manual": 0.006}

_DURABLE_OVERHEAD_S = 0.004
_CONFIRM_OVERHEAD_S = 0.003
_MAX_RETRIES = 3
_RETRY_BASE_S = 1.0


class SimulatedAMQPBackend(ProtocolBackend):

    def __init__(self, config: AMQPConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.01, seed: int = 0) -> None:
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

    def _next_id(self) -> int:
        self._msg_seq += 1
        return self._msg_seq

    def _overhead_s(self) -> float:
        return (
            _EXCHANGE_OVERHEAD_S.get(self.config.exchange_type, 0.002)
            + _ACK_OVERHEAD_S.get(self.config.ack_mode, 0.0)
            + (_DURABLE_OVERHEAD_S if self.config.durable else 0.0)
            + _CONFIRM_OVERHEAD_S
        )

    def _routing_key(self, batch: BatchUpdate) -> str:
        if self.config.exchange_type == "fanout":
            return ""
        if self.config.exchange_type == "topic":
            return f"parking.{batch.edge_id}.update"
        return f"parking.{batch.edge_id}"

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        rk = self._routing_key(batch)
        total_bytes = (len(payload) + AMQP_FRAME_OVERHEAD + AMQP_EXCHANGE_OVERHEAD + len(rk.encode()))
        self.bytes_sent += total_bytes
        msg_id = self._next_id()
        self._attempt(batch, payload, msg_id, attempt=0)

    def _attempt(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        overhead = self._overhead_s()

        def on_overhead() -> None:
            if self._rng.random() < self.loss_rate:
                self.nacked += 1
                if self.config.ack_mode == "manual" and attempt < _MAX_RETRIES - 1:
                    backoff = _RETRY_BASE_S * (2 ** attempt)
                    self.retransmitted += 1
                    self.clock.schedule(backoff, lambda a=attempt + 1: self._attempt(batch, payload, msg_id, a))
                else:
                    if self.on_drop:
                        self.on_drop()
                return

            if msg_id in self._delivered_ids:
                return
            self._delivered_ids.add(msg_id)
            self._subscriber(batch, payload)

        self.clock.schedule(overhead, on_overhead)


class RealAMQPBackend(ProtocolBackend):

    QUEUE_NAME = "parking.cloud.inbound"

    def __init__(self, config: AMQPConfig, broker: AMQPBrokerConfig, cloud_recv_cb: CloudRecvCallback, scenario_name: str = "run") -> None:
        self.config = config
        self.broker = broker
        self._cloud_recv_cb = cloud_recv_cb
        self._scenario_name = scenario_name
        self.bytes_sent: int = 0
        self.nacked: int = 0
        self.retransmitted: int = 0
        self._pub_connection = None
        self._pub_channel = None
        self._pub_exchange = None
        self._consume_task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._pub_connection = await aio_pika.connect_robust(self.broker.url)
        self._pub_channel = await self._pub_connection.channel()
        etype_map = {"direct": aio_pika.ExchangeType.DIRECT, "fanout": aio_pika.ExchangeType.FANOUT, "topic": aio_pika.ExchangeType.TOPIC}
        etype = etype_map.get(self.config.exchange_type, aio_pika.ExchangeType.DIRECT)
        self._pub_exchange = await self._pub_channel.declare_exchange(self.config.exchange, etype, durable=self.config.durable)
        logger.info(
            f"[AMQP-real] Publisher ready — exchange={self.config.exchange} "
            f"type={self.config.exchange_type}"
        )
        self._consume_task = asyncio.create_task(self._consume_loop())
        await asyncio.sleep(0.3)

    async def stop(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        if self._pub_connection:
            await self._pub_connection.close()
        logger.info("[AMQP-real] Disconnected.")

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        if self._loop is None:
            raise RuntimeError("RealAMQPBackend.start() was not called")
        rk = self._routing_key(batch)
        wire = len(payload) + AMQP_FRAME_OVERHEAD + AMQP_EXCHANGE_OVERHEAD + len(rk.encode())
        self.bytes_sent += wire
        asyncio.ensure_future(self._async_publish(payload, rk), loop=self._loop)

    async def _async_publish(self, payload: bytes, routing_key: str) -> None:
        if self._pub_exchange is None:
            return
        try:
            delivery_mode = (
                aio_pika.DeliveryMode.PERSISTENT
                if self.config.durable
                else aio_pika.DeliveryMode.NOT_PERSISTENT
            )
            msg = aio_pika.Message(body=payload, delivery_mode=delivery_mode, content_type="application/json")
            await self._pub_exchange.publish(msg, routing_key=routing_key)
        except Exception:
            logger.exception("[AMQP-real] Publish error")

    async def _consume_loop(self) -> None:
        connection = await aio_pika.connect_robust(self.broker.url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=self.config.prefetch_count)
        etype_map = {"direct": aio_pika.ExchangeType.DIRECT, "fanout": aio_pika.ExchangeType.FANOUT, "topic": aio_pika.ExchangeType.TOPIC}
        etype = etype_map.get(self.config.exchange_type, aio_pika.ExchangeType.DIRECT)
        exchange = await channel.declare_exchange(self.config.exchange, etype, durable=self.config.durable)
        queue = await channel.declare_queue(self.QUEUE_NAME, durable=self.config.durable)

        if self.config.exchange_type == "fanout":
            await queue.bind(exchange)
        else:
            await queue.bind(exchange, routing_key="parking.#")

        auto_ack = self.config.ack_mode == "auto"
        logger.info(f"[AMQP-real] Consumer ready on queue={self.QUEUE_NAME}")

        async with queue.iterator() as q_iter:
            async for message in q_iter:
                async with message.process(ignore_processed=True):
                    try:
                        raw: bytes = message.body
                        data = json.loads(raw)
                        batch = _batch_from_dict(data)
                        self._cloud_recv_cb(batch, raw)
                        if not auto_ack:
                            await message.ack()
                    except Exception:
                        logger.exception("[AMQP-real] Error processing message")
                        if not auto_ack:
                            await message.nack(requeue=False)
                        self.nacked += 1

    def _routing_key(self, batch: BatchUpdate) -> str:
        if self.config.exchange_type == "fanout":
            return ""
        if self.config.exchange_type == "topic":
            return f"parking.{batch.edge_id}.update"
        return f"parking.{batch.edge_id}"
