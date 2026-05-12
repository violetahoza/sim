from __future__ import annotations
import json
import logging
import threading
import time
import paho.mqtt.client as mqtt
import random

from simulator.models import BatchUpdate, ParkingEvent, SpotState
from simulator.config import MQTTConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.protocols.broker_config import MQTTBrokerConfig


logger = logging.getLogger(__name__)

_TOPIC_TMPL = "{prefix}/{edge_id}/update"

class SimulatedMQTTBackend(ProtocolBackend):
    QOS_OVERHEAD_S = {0: 0.0, 1: 0.005, 2: 0.015}
    RETRY_DELAY_S = 1.0
    MAX_RETRIES = 3

    def __init__(self, config: MQTTConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.0, seed: int = 0) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.bytes_sent = 0
        self.retransmitted = 0

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        topic = _TOPIC_TMPL.format(prefix=self.config.topic_prefix, edge_id=batch.edge_id)
        mqtt_overhead = 2 + len(topic.encode())
        self.bytes_sent += len(payload) + mqtt_overhead
        self._attempt(batch, payload, attempt=0)

    def _attempt(self, batch: BatchUpdate, payload: bytes, attempt: int) -> None:
        overhead = self.QOS_OVERHEAD_S[self.config.qos]

        def on_overhead_elapsed() -> None:
            if self._rng.random() < self.loss_rate:
                if self.config.qos == 0:
                    return
                self.retransmitted += 1
                if attempt < self.MAX_RETRIES - 1:
                    backoff = self.RETRY_DELAY_S * (2**attempt)
                    self.clock.schedule(backoff, lambda a=attempt + 1: self._attempt(batch, payload, a))
                return
            self._subscriber(batch, payload)

        self.clock.schedule(overhead, on_overhead_elapsed)

class RealMQTTBackend(ProtocolBackend):

    def __init__(self, config: MQTTConfig, broker: MQTTBrokerConfig, cloud_recv_cb: CloudRecvCallback, scenario_name: str = "run") -> None:
        self.config = config
        self.broker = broker
        self._cloud_recv_cb = cloud_recv_cb
        self._scenario_name = scenario_name

        self.bytes_sent: int = 0
        self.retransmitted: int = 0

        self._pub_client: mqtt.Client | None = None
        self._sub_client: mqtt.Client | None = None
        self._connected = threading.Event()
        self._sub_ready = threading.Event()

    async def start(self) -> None:
        """Connect publisher + subscriber and block until both are ready."""
        self._start_publisher()
        self._start_subscriber()
        # Give both clients up to 10 s to connect
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if self._connected.is_set() and self._sub_ready.is_set():
                logger.info("[MQTT-real] Publisher and subscriber connected.")
                return
            time.sleep(0.05)
        raise RuntimeError(
            f"[MQTT-real] Could not connect to broker at "
            f"{self.broker.host}:{self.broker.port} within 10 s"
        )

    async def stop(self) -> None:
        if self._pub_client:
            self._pub_client.loop_stop()
            self._pub_client.disconnect()
        if self._sub_client:
            self._sub_client.loop_stop()
            self._sub_client.disconnect()
        logger.info("[MQTT-real] Disconnected.")

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        if self._pub_client is None:
            raise RuntimeError("RealMQTTBackend.start() was not called")
        topic = _TOPIC_TMPL.format(prefix=self.config.topic_prefix, edge_id=batch.edge_id)
        wire_bytes = len(payload) + 2 + len(topic.encode())
        self.bytes_sent += wire_bytes
        result = self._pub_client.publish(topic, payload, qos=self.config.qos)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning(f"[MQTT-real] publish rc={result.rc} topic={topic}")

    def _start_publisher(self) -> None:
        client_id = f"parking-pub-{self._scenario_name}"
        c = mqtt.Client(client_id=client_id, clean_session=self.config.clean_session)
        if self.broker.username:
            c.username_pw_set(self.broker.username, self.broker.password)

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info(f"[MQTT-real] Publisher connected (rc={rc})")
                self._connected.set()
            else:
                logger.error(f"[MQTT-real] Publisher connect failed rc={rc}")

        def on_disconnect(client, userdata, rc):
            if rc != 0:
                logger.warning(f"[MQTT-real] Publisher unexpectedly disconnected rc={rc}")
            self._connected.clear()

        c.on_connect = on_connect
        c.on_disconnect = on_disconnect
        c.connect_async(self.broker.host, self.broker.port, keepalive=self.config.keepalive)
        c.loop_start()
        self._pub_client = c

    def _start_subscriber(self) -> None:
        client_id = f"parking-sub-{self._scenario_name}"
        c = mqtt.Client(client_id=client_id, clean_session=True)
        if self.broker.username:
            c.username_pw_set(self.broker.username, self.broker.password)

        subscribe_topic = f"{self.config.topic_prefix}/+/update"

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.subscribe(subscribe_topic, qos=self.config.qos)
                logger.info(f"[MQTT-real] Subscriber connected, subscribed to {subscribe_topic}")
                self._sub_ready.set()
            else:
                logger.error(f"[MQTT-real] Subscriber connect failed rc={rc}")

        def on_message(client, userdata, msg):
            try:
                raw: bytes = msg.payload
                data = json.loads(raw)
                batch = _batch_from_dict(data)
                self._cloud_recv_cb(batch, raw)
            except Exception:
                logger.exception("[MQTT-real] Error processing message")

        c.on_connect = on_connect
        c.on_message = on_message
        c.connect_async(self.broker.host, self.broker.port, keepalive=self.config.keepalive)
        c.loop_start()
        self._sub_client = c

def _batch_from_dict(data: dict) -> BatchUpdate:
    events = []
    for e in data.get("events", []):
        state_raw = e.get("state", "free")
        try:
            state = SpotState(state_raw)
        except ValueError:
            state = SpotState.FREE
        events.append(
            ParkingEvent(
                sensor_id=e.get("sensor_id", ""),
                spot_id=int(e.get("spot_id", 0)),
                state=state,
                timestamp=float(e.get("timestamp", 0.0)),
                sequence=int(e.get("sequence", 0))
            )
        )
    return BatchUpdate(edge_id=data.get("edge_id", "edge_01"), events=events)