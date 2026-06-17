from __future__ import annotations
import logging
import threading
from typing import Callable, Optional

import paho.mqtt.client as mqtt

from simulator.models.models import BatchUpdate
from simulator.config.config import MQTTConfig
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.utils import deserialize_batch

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


class RealMQTTBackend(ProtocolBackend):

    def __init__(self, config: MQTTConfig, subscriber_cb: CloudRecvCallback, connect_timeout_s: float = 5.0) -> None:
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
        self._lock = threading.Lock() 

        self._sub_topic = _TOPIC_TMPL.format(prefix=config.topic_prefix, edge_id="+")
        self._connected = threading.Event()

        self._client = mqtt.Client(clean_session=config.clean_session)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    async def start(self) -> None:
        self._client.connect(self.config.host, self.config.port, keepalive=self.config.keepalive)
        self._client.loop_start()  
        if not self._connected.wait(timeout=self._connect_timeout_s):
            self._client.loop_stop()
            raise ConnectionError(
                f"RealMQTTBackend: broker {self.config.host}:{self.config.port} "
                f"did not confirm connect within {self._connect_timeout_s}s "
                f"(is `docker compose up -d` running?)"
            )

    async def stop(self) -> None:
        try:
            self._client.disconnect()
        finally:
            self._client.loop_stop()

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(self._sub_topic, qos=self.config.qos)
            self._connected.set()
        else:
            logger.error("RealMQTTBackend connect failed rc=%s", rc)

    def _on_message(self, client, userdata, msg):
        try:
            batch = deserialize_batch(msg.payload)
        except Exception:
            logger.exception("RealMQTTBackend: failed to decode message")
            return
        self.frames_delivered += 1
        self._subscriber(batch, msg.payload)

    def _publish_wire_bytes(self, topic: str, payload: bytes) -> int:
        topic_b = topic.encode()
        var_header = 2 + len(topic_b) + (2 if self.config.qos > 0 else 0)
        remaining = var_header + len(payload)
        return 1 + _remaining_length_bytes(remaining) + remaining

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        topic = _TOPIC_TMPL.format(prefix=self.config.topic_prefix, edge_id=batch.edge_id)
        with self._lock:
            self.frames_offered += 1
        info = self._client.publish(topic, payload, qos=self.config.qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            with self._lock:
                self.frames_dropped += 1
            if self.on_drop:
                self.on_drop()
            return
        with self._lock:
            self.bytes_sent += self._publish_wire_bytes(topic, payload)