from __future__ import annotations
import os
from dataclasses import dataclass
from simulator.utils import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class MQTTBrokerConfig:
    host: str
    port: int
    username: str
    password: str

    @classmethod
    def from_env(cls) -> "MQTTBrokerConfig":
        return cls(
            host=os.environ.get("MQTT_HOST", "localhost"),
            port=int(os.environ.get("MQTT_PORT", "1883")),
            username=os.environ.get("MQTT_USER", ""),
            password=os.environ.get("MQTT_PASSWORD", ""),
        )


@dataclass(frozen=True)
class AMQPBrokerConfig:
    url: str

    @classmethod
    def from_env(cls) -> "AMQPBrokerConfig":
        return cls(
            url=os.environ.get(
                "AMQP_URL", "amqp://guest:guest@localhost:5672/"
            )
        )


@dataclass(frozen=True)
class CoAPBrokerConfig:
    host: str
    port: int

    @classmethod
    def from_env(cls) -> "CoAPBrokerConfig":
        return cls(
            host=os.environ.get("COAP_HOST", "localhost"),
            port=int(os.environ.get("COAP_PORT", "5683")),
        )


def broker_mode() -> str:
    return os.environ.get("BROKER_MODE", "simulated").strip().lower()


def use_real_brokers() -> bool:
    return broker_mode() == "real"