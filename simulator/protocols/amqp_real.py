from __future__ import annotations
import asyncio
import logging
import threading
from typing import Callable, Optional

import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aio_pika.abc import AbstractIncomingMessage

from simulator.models.models import BatchUpdate
from simulator.config.config import AMQPConfig
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.utils import deserialize_batch

logger = logging.getLogger(__name__)

_AMQP_FRAME_HEADER = 8 


class RealAMQPBackend(ProtocolBackend):

    def __init__(self, config: AMQPConfig, subscriber_cb: CloudRecvCallback, connect_timeout_s: float = 8.0) -> None:
        self.config = config
        self._subscriber = subscriber_cb
        self._connect_timeout_s = connect_timeout_s

        self.bytes_sent = 0   
        self.frames_offered = 0 
        self.frames_delivered = 0 
        self.frames_dropped = 0

        self.retransmitted: Optional[int] = None
        self.first_pass_delivered: Optional[int] = None
        self.duplicates_delivered: Optional[int] = None

        self.on_drop: Optional[Callable[[], None]] = None

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._conn = None
        self._channel = None
        self._exchange = None
        self._queue = None
        self._exchange_type = ExchangeType(config.exchange_type)
        self._auto_ack = (config.ack_mode == "auto")
        self._direct_key = f"{config.queue_prefix}.update"
        self._lock = threading.Lock()

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._conn = await asyncio.wait_for(
            aio_pika.connect_robust(
                host=self.config.host, port=self.config.port,
                login="guest", password="guest", 
                virtualhost=self.config.virtual_host
            ),
            timeout=self._connect_timeout_s
        )
        self._channel = await self._conn.channel()
        await self._channel.set_qos(prefetch_count=self.config.prefetch_count)
        self._exchange = await self._channel.declare_exchange(self.config.exchange, self._exchange_type, durable=self.config.durable)
        self._queue = await self._channel.declare_queue("", exclusive=True, auto_delete=True)
        if self._exchange_type == ExchangeType.FANOUT:
            await self._queue.bind(self._exchange)
        elif self._exchange_type == ExchangeType.TOPIC:
            await self._queue.bind(self._exchange, routing_key=f"{self.config.queue_prefix}.#")
        else:  # direct
            await self._queue.bind(self._exchange, routing_key=self._direct_key)
        await self._queue.consume(self._on_message, no_ack=self._auto_ack)

    async def stop(self) -> None:
        if self._conn is not None:
            await self._conn.close()

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        try:
            batch = deserialize_batch(message.body)
        except Exception:
            logger.exception("RealAMQPBackend: failed to decode message")
            if not self._auto_ack:
                await message.nack(requeue=False)
            return
        self.frames_delivered += 1
        self._subscriber(batch, message.body)
        if not self._auto_ack:
            await message.ack()

    def _routing_key(self, edge_id: str) -> str:
        if self._exchange_type == ExchangeType.FANOUT:
            return ""
        if self._exchange_type == ExchangeType.TOPIC:
            return f"{self.config.queue_prefix}.{edge_id}.update"
        return self._direct_key 

    def _amqp_publish_wire_bytes(self, routing_key: str, payload: bytes) -> int:
        ex = self.config.exchange.encode()
        rk = routing_key.encode()
        method = 2 + 2 + 2 + (1 + len(ex)) + (1 + len(rk)) + 1
        props = 1 if self.config.durable else 0
        content_header = 2 + 2 + 8 + 2 + props
        body = len(payload)
        return ((_AMQP_FRAME_HEADER + method)
                + (_AMQP_FRAME_HEADER + content_header)
                + (_AMQP_FRAME_HEADER + body))

    async def _publish_async(self, routing_key: str, payload: bytes) -> None:
        msg = Message(payload, delivery_mode=(DeliveryMode.PERSISTENT if self.config.durable else DeliveryMode.NOT_PERSISTENT))
        await self._exchange.publish(msg, routing_key=routing_key)
        self.bytes_sent += self._amqp_publish_wire_bytes(routing_key, payload)

    def _on_publish_done(self, ft) -> None:
        try:
            exc = ft.exception()
        except Exception:
            exc = None
        if exc is not None:
            with self._lock:
                self.frames_dropped += 1
            logger.warning("RealAMQPBackend publish failed: %s", exc)
            if self.on_drop:
                self.on_drop()

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        with self._lock:
            self.frames_offered += 1
        if self._loop is None or self._exchange is None:
            with self._lock:
                self.frames_dropped += 1
            if self.on_drop:
                self.on_drop()
            return
        rk = self._routing_key(batch.edge_id)
        ft = asyncio.run_coroutine_threadsafe(self._publish_async(rk, payload), self._loop)
        ft.add_done_callback(self._on_publish_done)