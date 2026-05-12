from __future__ import annotations
import json
import logging
import threading
import time
import random
import paho.mqtt.client as mqtt

from simulator.models import BatchUpdate, ParkingEvent, SpotState
from simulator.config import MQTTConfig
from simulator.des.engine import SimClock
from simulator.protocols.base import ProtocolBackend, CloudRecvCallback
from simulator.protocols.broker_config import MQTTBrokerConfig


logger = logging.getLogger(__name__)

_TOPIC_TMPL = "{prefix}/{edge_id}/update"

MQTT_FIXED_HEADER = 2  
MQTT_PUBACK_BYTES = 4   
MQTT_PUBREL_BYTES = 4    
MQTT_CONNACK_BYTES = 4

_QOS_OVERHEAD_S = {0: 0.0, 1: 0.005, 2: 0.015}
_MAX_RETRIES = {0: 0, 1: 3, 2: 5}
_RETRY_BASE_S = 1.0  


class SimulatedMQTTBackend(ProtocolBackend):
    
    def __init__(self, config: MQTTConfig, clock: SimClock, subscriber_cb: CloudRecvCallback, loss_rate: float = 0.0, seed: int = 0) -> None:
        self.config = config
        self.clock = clock
        self._subscriber = subscriber_cb
        self.loss_rate = loss_rate
        self._rng = random.Random(seed)
        self.bytes_sent = 0
        self.retransmitted = 0
        self.duplicates_delivered = 0
        self._qos2_delivered: dict[int, bool] = {}
        self._msg_seq = 0

    def _next_id(self) -> int:
        self._msg_seq += 1
        return self._msg_seq

    def _topic_overhead(self, batch: BatchUpdate) -> int:
        topic = _TOPIC_TMPL.format(prefix=self.config.topic_prefix, edge_id=batch.edge_id)
        return MQTT_FIXED_HEADER + len(topic.encode())

    def publish(self, batch: BatchUpdate, payload: bytes) -> None:
        topic_oh = self._topic_overhead(batch)
        qos = self.config.qos
        qos_extra = MQTT_PUBACK_BYTES if qos == 1 else (MQTT_PUBACK_BYTES + MQTT_PUBREL_BYTES * 2) if qos == 2 else 0
        self.bytes_sent += len(payload) + topic_oh + qos_extra
        msg_id = self._next_id()
        self._attempt(batch, payload, msg_id, attempt=0)

    def _attempt(self, batch: BatchUpdate, payload: bytes, msg_id: int, attempt: int) -> None:
        qos = self.config.qos
        overhead = _QOS_OVERHEAD_S[qos]

        def on_overhead_elapsed() -> None:
            if self._rng.random() < self.loss_rate:
                if qos == 0:
                    return
                max_r = _MAX_RETRIES[qos]
                if attempt < max_r:
                    backoff = _RETRY_BASE_S * (2 ** attempt)
                    self.retransmitted += 1
                    self.clock.schedule(backoff, lambda a=attempt + 1: self._attempt(batch, payload, msg_id, a))
                return

            if qos == 2:
                if self._qos2_delivered.get(msg_id):
                    self.duplicates_delivered += 1
                    return
                self._qos2_delivered[msg_id] = True
            elif qos == 1 and attempt > 0:
                self.duplicates_delivered += 1

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
        self._start_publisher()
        self._start_subscriber()
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
        wire_bytes = len(payload) + MQTT_FIXED_HEADER + len(topic.encode())
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
                logger.info(f"[MQTT-real] Subscriber subscribed to {subscribe_topic}")
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