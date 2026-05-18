from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Literal, Optional
import yaml

Protocol = Literal["mqtt", "amqp", "coap"]
Architecture = Literal["cloud_only", "edge_filtered", "edge_aggregated"]
TrafficLevel = Literal["low", "medium", "peak"]
MQTTQoS = Literal[0, 1, 2]
CoAPMode = Literal["CON", "NON"]
AMQPExchange = Literal["direct", "fanout", "topic"]
AMQPAckMode = Literal["auto", "manual"]

DEFAULT_TOD_FACTORS: list[float] = [
    0.05, 0.03, 0.03, 0.03, 0.05, 0.15,  # 00–05
    0.50, 1.40, 2.00, 1.80, 1.50, 1.60,  # 06–11
    1.70, 1.50, 1.30, 1.50, 1.80, 2.20,  # 12–17
    2.00, 1.60, 1.20, 0.90, 0.60, 0.30,  # 18–23
]

ARRIVAL_RATES: dict[str, float] = {
    "low": 0.0069,
    "medium": 0.0153,
    "peak": 0.0236,
}

SIM_DURATION_S = 10_800.0
DEFAULT_AGG_INTERVAL_S = 1.0
DEFAULT_TIME_SCALE = 60.0

_SCENARIOS_YAML = Path(__file__).parent / "scenarios.yaml"
_CUSTOM_SCENARIOS_FILE = Path(__file__).parent.parent / "data" / "custom_scenarios.json"

logger = logging.getLogger(__name__)


@dataclass
class LinkConfig:
    base_delay_ms: float = 80.0
    jitter_ms: float = 30.0
    packet_loss_rate: float = 0.05
    max_payload_bytes: int = 51
    rate_limit_msgs_per_sec: float = 5.0
    payload_encoding_ratio: float = 0.15
    lorawan_duty_cycle: bool = False
    sf_airtime_ms: float = 41.0

@dataclass
class BackhaulLinkConfig:

    base_delay_ms: float = 30.0
    jitter_ms: float = 10.0
    packet_loss_rate: float = 0.001
    max_payload_bytes: int = 65535
    rate_limit_msgs_per_sec: float = 1000.0
    payload_encoding_ratio: float = 1.0

    def to_link_config(self) -> "LinkConfig":
        return LinkConfig(
            base_delay_ms=self.base_delay_ms,
            jitter_ms=self.jitter_ms,
            packet_loss_rate=self.packet_loss_rate,
            max_payload_bytes=self.max_payload_bytes,
            rate_limit_msgs_per_sec=self.rate_limit_msgs_per_sec,
            payload_encoding_ratio=self.payload_encoding_ratio,
            lorawan_duty_cycle=False
        )

@dataclass
class EdgeConfig:
    architecture: Architecture = "edge_aggregated"
    aggregation_interval_s: float = 1.0
    max_event_age_s: float = 2.0
    max_batch_size: int = 50

    filter_no_change: bool = True
    duplicate_window_s: float = 5.0
    heartbeat_forward_interval_s: float = 300.0

    anomaly_detection: bool = True
    adaptive_edge: bool = False
    stuck_threshold: int = 10
    silent_threshold_s: float = 3600.0
    quarantine_threshold: int = 5
    quarantine_recovery_events: int = 3


@dataclass
class MQTTConfig:
    host: str = "localhost"
    port: int = 1883
    topic_prefix: str = "parking"
    qos: MQTTQoS = 1
    keepalive: int = 60
    clean_session: bool = True


@dataclass
class AMQPConfig:
    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"
    exchange: str = "parking"
    exchange_type: AMQPExchange = "direct"
    ack_mode: AMQPAckMode = "manual"
    durable: bool = True
    prefetch_count: int = 10
    heartbeat_s: int = 60
    queue_prefix: str = "parking.edge"


@dataclass
class CoAPConfig:
    host: str = "localhost"
    port: int = 5683
    mode: CoAPMode = "CON"
    resource: str = "parking/update"

@dataclass
class TrafficConfig:
    num_spots: int = 50

    arrival_rate_low: float = 0.0069
    arrival_rate_medium: float = 0.0153
    arrival_rate_peak: float = 0.0236

    mean_parking_duration_s: float = 1800.0
    parking_duration_cv: float = 1.5
    sim_duration_s: float = 10800.0
    random_seed: Optional[int] = 42
    initial_occupancy: float = 0.55
    time_scale: float = 60.0
    use_time_of_day: bool = False
    start_hour: float = 8.0
    tod_factors: list[float] = field(default_factory=lambda: list(DEFAULT_TOD_FACTORS))
    use_dwell_mixture: bool = True
    heartbeat_interval_s: float = 60.0
    duplicate_send_prob: float = 0.05


@dataclass
class ScenarioConfig:
    name: str
    description: str
    protocol: Protocol
    architecture: Architecture
    traffic_level: TrafficLevel
    num_spots: int
    arrival_rate: float
    sim_duration_s: float = 10800.0
    link: LinkConfig = field(default_factory=LinkConfig)
    backhaul_link: BackhaulLinkConfig = field(default_factory=BackhaulLinkConfig)
    edge: EdgeConfig = field(default_factory=EdgeConfig)
    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    amqp: AMQPConfig = field(default_factory=AMQPConfig)
    coap: CoAPConfig = field(default_factory=CoAPConfig)
    traffic: TrafficConfig = field(default_factory=TrafficConfig)
    random_seed: int = 42
    group: str = ""
    group_order: int = 0
    is_builtin: bool = False

    def to_save_dict(self) -> dict:
        d = asdict(self)
        link = d["link"]
        bh = d["backhaul_link"]
        edge = d["edge"]
        mqtt = d["mqtt"]
        amqp = d["amqp"]
        coap = d["coap"]
        traffic = d["traffic"]
        return {
            "name": d["name"],
            "description": d["description"],
            "protocol": d["protocol"],
            "architecture": d["architecture"],
            "traffic_level": d["traffic_level"],
            "num_spots": d["num_spots"],
            "sim_duration_s": d["sim_duration_s"],
            "group": d["group"],
            "group_order": d["group_order"],
            "seed": d["random_seed"],
            "is_builtin": False,
            "base_delay_ms": link["base_delay_ms"],
            "jitter_ms": link["jitter_ms"],
            "max_payload_bytes": link["max_payload_bytes"],
            "payload_encoding_ratio": link["payload_encoding_ratio"],
            "loss_rate": link["packet_loss_rate"],
            "rate_limit": link["rate_limit_msgs_per_sec"],
            "lorawan_duty_cycle": link["lorawan_duty_cycle"],
            "sf_airtime_ms": link["sf_airtime_ms"],
            "backhaul_base_delay_ms": bh["base_delay_ms"],
            "backhaul_jitter_ms": bh["jitter_ms"],
            "backhaul_loss_rate": bh["packet_loss_rate"],
            "aggregation_interval": edge["aggregation_interval_s"],
            "max_event_age_s": edge["max_event_age_s"],
            "max_batch_size": edge["max_batch_size"],
            "duplicate_window_s": edge["duplicate_window_s"],
            "heartbeat_forward_interval_s": edge["heartbeat_forward_interval_s"],
            "anomaly_detection": edge["anomaly_detection"],
            "adaptive_edge": edge["adaptive_edge"],
            "stuck_threshold": edge["stuck_threshold"],
            "silent_threshold_s": edge["silent_threshold_s"],
            "quarantine_threshold": edge["quarantine_threshold"],
            "quarantine_recovery_events": edge["quarantine_recovery_events"],
            "mqtt_qos": mqtt["qos"],
            "coap_mode": coap["mode"],
            "amqp_exchange": amqp["exchange_type"],
            "amqp_ack": amqp["ack_mode"],
            "amqp_durable": amqp["durable"],
            "time_scale": traffic["time_scale"],
            "parking_duration_cv": traffic["parking_duration_cv"],
            "use_time_of_day": traffic["use_time_of_day"],
            "start_hour": traffic["start_hour"],
            "initial_occupancy": traffic["initial_occupancy"],
            "tod_factors": traffic["tod_factors"],
            "use_dwell_mixture": traffic["use_dwell_mixture"],
            "use_dwell_mixture": traffic["use_dwell_mixture"],
            "heartbeat_interval_s": traffic["heartbeat_interval_s"],
            "duplicate_send_prob": traffic["duplicate_send_prob"]
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ScenarioConfig":
        return make_scenario(
            name=d["name"],
            description=d.get("description", ""),
            protocol=d.get("protocol", "mqtt"),
            architecture=d.get("architecture", "edge_aggregated"),
            traffic_level=d.get("traffic_level", "medium"),
            num_spots=d.get("num_spots", 50),
            loss_rate=d.get("loss_rate", 0.05),
            aggregation_interval=d.get("aggregation_interval", DEFAULT_AGG_INTERVAL_S),
            max_event_age_s=d.get("max_event_age_s", 2.0),
            max_batch_size=d.get("max_batch_size", 50),
            duplicate_window_s=d.get("duplicate_window_s", 5.0),
            heartbeat_forward_interval_s=d.get("heartbeat_forward_interval_s", 300.0),
            anomaly_detection=d.get("anomaly_detection", True),
            adaptive_edge=d.get("adaptive_edge", False),
            stuck_threshold=d.get("stuck_threshold", 10),
            silent_threshold_s=d.get("silent_threshold_s", 3600.0),
            quarantine_threshold=d.get("quarantine_threshold", 3),
            quarantine_recovery_events=d.get("quarantine_recovery_events", 3),
            mqtt_qos=d.get("mqtt_qos", 1),
            coap_mode=d.get("coap_mode", "CON"),
            amqp_exchange=d.get("amqp_exchange", "direct"),
            amqp_ack=d.get("amqp_ack", "manual"),
            amqp_durable=d.get("amqp_durable", True),
            sim_duration_s=d.get("sim_duration_s", SIM_DURATION_S),
            seed=d.get("seed", 42),
            group=d.get("group", "User Scenarios"),
            group_order=d.get("group_order", 99),
            rate_limit=d.get("rate_limit"),
            time_scale=d.get("time_scale", DEFAULT_TIME_SCALE),
            is_builtin=False,
            base_delay_ms=d.get("base_delay_ms", 80.0),
            jitter_ms=d.get("jitter_ms", 30.0),
            max_payload_bytes=d.get("max_payload_bytes", 51),
            payload_encoding_ratio=d.get("payload_encoding_ratio", 0.15),
            lorawan_duty_cycle=d.get("lorawan_duty_cycle", True),
            sf_airtime_ms=d.get("sf_airtime_ms", 41.0),
            backhaul_base_delay_ms=d.get("backhaul_base_delay_ms", 30.0),
            backhaul_jitter_ms=d.get("backhaul_jitter_ms", 10.0),
            backhaul_loss_rate=d.get("backhaul_loss_rate", 0.001),
            parking_duration_cv=d.get("parking_duration_cv", 1.5),
            use_time_of_day=d.get("use_time_of_day", False),
            start_hour=d.get("start_hour", 8.0),
            initial_occupancy=d.get("initial_occupancy"),
            tod_factors=d.get("tod_factors"),
            use_dwell_mixture=d.get("use_dwell_mixture", True),
            heartbeat_interval_s=d.get("heartbeat_interval_s", 60.0)
        )
    

def make_scenario(
    name: str,
    description: str,
    protocol: Protocol,
    architecture: Architecture,
    traffic_level: TrafficLevel,
    num_spots: int = 50,
    loss_rate: float = 0.05,
    aggregation_interval: float = DEFAULT_AGG_INTERVAL_S,
    max_event_age_s: float = 2.0,
    max_batch_size: int = 50,
    duplicate_window_s: float = 5.0,
    heartbeat_forward_interval_s: float = 300.0,
    anomaly_detection: bool = True,
    adaptive_edge: bool = False,
    stuck_threshold: int = 10,
    silent_threshold_s: float = 3600.0,
    quarantine_threshold: int = 3,
    quarantine_recovery_events: int = 3,
    mqtt_qos: MQTTQoS = 1,
    coap_mode: CoAPMode = "CON",
    amqp_exchange: AMQPExchange = "direct",
    amqp_ack: AMQPAckMode = "manual",
    amqp_durable: bool = True,
    sim_duration_s: float = SIM_DURATION_S,
    seed: int = 42,
    group: str = "",
    group_order: int = 0,
    rate_limit: Optional[float] = None,
    time_scale: float = DEFAULT_TIME_SCALE,
    is_builtin: bool = False,
    base_delay_ms: float = 80.0,
    jitter_ms: float = 30.0,
    max_payload_bytes: int = 51,
    payload_encoding_ratio: float = 0.15,
    lorawan_duty_cycle: bool = True,
    sf_airtime_ms: float = 41.0,
    backhaul_base_delay_ms: float = 30.0,
    backhaul_jitter_ms: float = 10.0,
    backhaul_loss_rate: float = 0.001,
    mean_parking_duration_s: float = 1800.0,
    parking_duration_cv: float = 1.5,
    use_time_of_day: bool = False,
    start_hour: float = 8.0,
    initial_occupancy: Optional[float] = None,
    tod_factors: Optional[list[float]] = None,
    use_dwell_mixture: bool = True,
    heartbeat_interval_s: float = 60.0,
    duplicate_send_prob: float = 0.05
) -> ScenarioConfig:
    arrival = ARRIVAL_RATES[traffic_level] * (num_spots / 50)

    if rate_limit is None:
        rate_limit = max(2.0, num_spots / 10.0)

    occ_map = {"low": 0.25, "medium": 0.55, "peak": 0.85}
    occ = initial_occupancy if initial_occupancy is not None else occ_map[traffic_level]

    link = LinkConfig(
        packet_loss_rate=loss_rate,
        rate_limit_msgs_per_sec=rate_limit,
        base_delay_ms=base_delay_ms,
        jitter_ms=jitter_ms,
        max_payload_bytes=max_payload_bytes,
        payload_encoding_ratio=payload_encoding_ratio
    )
    edge = EdgeConfig(
        architecture=architecture,
        aggregation_interval_s=aggregation_interval,
        max_event_age_s=max_event_age_s,
        max_batch_size=max_batch_size,
        duplicate_window_s=duplicate_window_s,
        heartbeat_forward_interval_s=heartbeat_forward_interval_s,
        anomaly_detection=anomaly_detection,
        adaptive_edge=adaptive_edge,
        stuck_threshold=stuck_threshold,
        silent_threshold_s=silent_threshold_s,
        quarantine_threshold=quarantine_threshold,
        quarantine_recovery_events=quarantine_recovery_events
    )
    traffic = TrafficConfig(
        num_spots=num_spots,
        arrival_rate_low=ARRIVAL_RATES["low"] * (num_spots / 50),
        arrival_rate_medium=ARRIVAL_RATES["medium"] * (num_spots / 50),
        arrival_rate_peak=ARRIVAL_RATES["peak"] * (num_spots / 50),
        mean_parking_duration_s=mean_parking_duration_s,
        parking_duration_cv=parking_duration_cv,
        sim_duration_s=sim_duration_s,
        random_seed=seed,
        initial_occupancy=occ,
        time_scale=time_scale,
        use_time_of_day=use_time_of_day,
        start_hour=start_hour,
        heartbeat_interval_s=heartbeat_interval_s,
        duplicate_send_prob=duplicate_send_prob,
        **({"tod_factors": tod_factors} if tod_factors is not None else {})
    )
    return ScenarioConfig(
        name=name, description=description, protocol=protocol,
        architecture=architecture, traffic_level=traffic_level,
        num_spots=num_spots, arrival_rate=arrival, sim_duration_s=sim_duration_s,
        link=link, edge=edge,
        mqtt=MQTTConfig(qos=mqtt_qos),
        amqp=AMQPConfig(exchange_type=amqp_exchange, ack_mode=amqp_ack, durable=amqp_durable),
        coap=CoAPConfig(mode=coap_mode),
        traffic=traffic, random_seed=seed,
        group=group, group_order=group_order, is_builtin=is_builtin
    )


def _load_builtin_scenarios() -> list[ScenarioConfig]:
    with open(_SCENARIOS_YAML, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return [
        make_scenario(**{k: v for k, v in s.items() if k != "is_builtin"}, is_builtin=s.get("is_builtin", True))
        for s in data["scenarios"]
    ]


PREDEFINED_SCENARIOS: list[ScenarioConfig] = _load_builtin_scenarios()
SCENARIO_REGISTRY: dict[str, ScenarioConfig] = {s.name: s for s in PREDEFINED_SCENARIOS}


def load_custom_scenarios() -> list[ScenarioConfig]:
    if not _CUSTOM_SCENARIOS_FILE.exists():
        return []
    data = json.loads(_CUSTOM_SCENARIOS_FILE.read_text())
    return [ScenarioConfig.from_dict(d) for d in data]


def save_custom_scenarios(scenarios: list[ScenarioConfig]) -> None:
    _CUSTOM_SCENARIOS_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = [s.to_save_dict() for s in scenarios if not s.is_builtin]
    _CUSTOM_SCENARIOS_FILE.write_text(json.dumps(data, indent=2))