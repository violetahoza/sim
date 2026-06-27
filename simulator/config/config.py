from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Literal, Optional
import yaml

from simulator.config.constants import ARRIVAL_RATES, LORAWAN_OVERHEAD_BYTES

Protocol = Literal["mqtt", "amqp", "coap"]
Architecture = Literal["cloud_only", "edge_filtered", "edge_aggregated"]
TrafficLevel = Literal["low", "medium", "peak"]
MQTTQoS = Literal[0, 1, 2]
CoAPMode = Literal["CON", "NON"]
AMQPExchange = Literal["direct", "fanout", "topic"]
AMQPAckMode = Literal["auto", "manual"]

DEFAULT_TOD_FACTORS: list[float] = [
    0.05, 0.03, 0.03, 0.03, 0.05, 0.15,  
    0.50, 1.40, 2.00, 1.80, 1.50, 1.60,  
    1.70, 1.50, 1.30, 1.50, 1.80, 2.20, 
    2.00, 1.60, 1.20, 0.90, 0.60, 0.30,  
]

SIM_DURATION_S = 10_800.0
DEFAULT_AGG_INTERVAL_S = 1.0
DEFAULT_TIME_SCALE = 60.0

DEFAULT_GATEWAY_RATE_MSGS_PER_SEC = 8.0
DEFAULT_CONTENTION_CHANNELS = 3

_SCENARIOS_YAML = Path(__file__).parent / "scenarios.yaml"
_CUSTOM_SCENARIOS_FILE = Path(__file__).parent.parent / "data" / "custom_scenarios.json"

logger = logging.getLogger(__name__)


@dataclass
class LinkConfig:
    base_delay_ms: float = 80.0
    jitter_ms: float = 30.0
    packet_loss_rate: float = 0.05
    max_payload_bytes: int = 222
    rate_limit_msgs_per_sec: float = 5.0
    transport_overhead_bytes: int = LORAWAN_OVERHEAD_BYTES
    use_lora_airtime: bool = True
    contention_channels: int = 0


@dataclass
class BackhaulLinkConfig:
    base_delay_ms: float = 30.0
    jitter_ms: float = 10.0
    packet_loss_rate: float = 0.02
    downlink_loss_rate: Optional[float] = None
    loss_peak_rate: Optional[float] = None
    max_payload_bytes: int = 65535
    rate_limit_msgs_per_sec: float = 1000.0
    transport_overhead_bytes: int = 0

    def to_link_config(self) -> "LinkConfig":
        return LinkConfig(
            base_delay_ms=self.base_delay_ms,
            jitter_ms=self.jitter_ms,
            packet_loss_rate=self.packet_loss_rate,
            max_payload_bytes=self.max_payload_bytes,
            rate_limit_msgs_per_sec=self.rate_limit_msgs_per_sec,
            transport_overhead_bytes=self.transport_overhead_bytes,
            use_lora_airtime=False
        )


@dataclass
class EdgeConfig:
    architecture: Architecture = "edge_aggregated"
    aggregation_interval_s: float = 1.0
    max_event_age_s: float = 2.0
    max_batch_size: int = 50

    filter_no_change: bool = True
    duplicate_window_s: float = 5.0
    heartbeat_forward_interval_s: float = 3600.0

    anomaly_detection: bool = True
    adaptive_edge: bool = False
    silent_threshold_s: Optional[float] = None
    quarantine_threshold: int = 5

    stuck_threshold_s: Optional[float] = None
    quarantine_release_clean_ticks: int = 5
    anomaly_check_interval_s: float = 30.0

    anomaly_rate_window_s: float = 600.0
    anomaly_robust_z: float = 4.5 
    anomaly_min_window_events: int = 15 
    anomaly_persistence_window_s: float = 7200.0 
    anomaly_incident_spacing_s: float = 180.0 
    anomaly_detect_score: float = 3.0 

    adaptive_check_interval_s: float = 30.0
    adaptive_degrade_threshold: float = 0.90
    adaptive_filtered_threshold: float = 0.80
    adaptive_recover_threshold: float = 0.95
    adaptive_min_window_samples: int = 10
    adaptive_dr_smoothing: float = 0.15


@dataclass
class MQTTConfig:
    host: str = "localhost"
    port: int = 1883
    topic_prefix: str = "parking"
    qos: MQTTQoS = 1
    keepalive: int = 60
    clean_session: bool = True
    max_retries_qos1: int = 3
    max_retries_qos2: int = 3

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
    heartbeat_s: int = 900
    queue_prefix: str = "parking.edge"
    max_redeliveries: int = 3
    requeue_delay_s: float = 0.2


@dataclass
class CoAPConfig:
    host: str = "localhost"
    port: int = 5683
    mode: CoAPMode = "CON"
    resource: str = "parking/update"
    max_retransmit: int = 4  
    ack_timeout_s: float = 2.0  
    ack_random_factor: float = 1.5 


@dataclass
class TrafficConfig:
    num_spots: int = 50
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
    faults: list = field(default_factory=list)

    def to_save_dict(self) -> dict:
        d = asdict(self)
        link = d["link"]; edge = d["edge"]; mqtt = d["mqtt"]
        amqp = d["amqp"]; coap = d["coap"]; traffic = d["traffic"]
        return {
            "name": self.name,
            "description": self.description,
            "protocol": self.protocol,
            "architecture": self.architecture,
            "traffic_level": self.traffic_level,
            "num_spots": self.num_spots,
            "sim_duration_s": self.sim_duration_s,
            "seed": self.random_seed,
            "group": self.group,
            "group_order": self.group_order,
            "loss_rate": link["packet_loss_rate"],
            "rate_limit": link["rate_limit_msgs_per_sec"],
            "contention_channels": link["contention_channels"],
            "base_delay_ms": link["base_delay_ms"],
            "jitter_ms": link["jitter_ms"],
            "max_payload_bytes": link["max_payload_bytes"],
            "backhaul_base_delay_ms": self.backhaul_link.base_delay_ms,
            "backhaul_jitter_ms": self.backhaul_link.jitter_ms,
            "backhaul_loss_rate": self.backhaul_link.packet_loss_rate,
            "backhaul_downlink_loss_rate": self.backhaul_link.downlink_loss_rate,
            "backhaul_loss_peak_rate": self.backhaul_link.loss_peak_rate,
            "aggregation_interval": edge["aggregation_interval_s"],
            "max_event_age_s": edge["max_event_age_s"],
            "max_batch_size": edge["max_batch_size"],
            "duplicate_window_s": edge["duplicate_window_s"],
            "heartbeat_forward_interval_s": edge["heartbeat_forward_interval_s"],
            "anomaly_detection": edge["anomaly_detection"],
            "adaptive_edge": edge["adaptive_edge"],
            "silent_threshold_s": edge["silent_threshold_s"],
            "quarantine_threshold": edge["quarantine_threshold"],
            "stuck_threshold_s": edge["stuck_threshold_s"],
            "quarantine_release_clean_ticks": edge["quarantine_release_clean_ticks"],
            "anomaly_check_interval_s": edge["anomaly_check_interval_s"],
            "adaptive_check_interval_s": edge["adaptive_check_interval_s"],
            "adaptive_degrade_threshold": edge["adaptive_degrade_threshold"],
            "adaptive_filtered_threshold": edge["adaptive_filtered_threshold"],
            "adaptive_recover_threshold": edge["adaptive_recover_threshold"],
            "adaptive_min_window_samples": edge["adaptive_min_window_samples"],
            "adaptive_dr_smoothing": edge["adaptive_dr_smoothing"],
            "mqtt_qos": mqtt["qos"],
            "mqtt_max_retries_qos1": mqtt["max_retries_qos1"],
            "mqtt_max_retries_qos2": mqtt["max_retries_qos2"],
            "coap_mode": coap["mode"],
            "coap_max_retransmit": coap["max_retransmit"],
            "coap_ack_timeout_s": coap["ack_timeout_s"],
            "coap_ack_random_factor": coap["ack_random_factor"],
            "amqp_exchange": amqp["exchange_type"],
            "amqp_ack": amqp["ack_mode"],
            "amqp_durable": amqp["durable"],
            "amqp_max_redeliveries": amqp["max_redeliveries"],
            "amqp_requeue_delay_s": amqp["requeue_delay_s"],
            "time_scale": traffic["time_scale"],
            "parking_duration_cv": traffic["parking_duration_cv"],
            "mean_parking_duration_s": traffic["mean_parking_duration_s"],
            "use_time_of_day": traffic["use_time_of_day"],
            "start_hour": traffic["start_hour"],
            "initial_occupancy": traffic["initial_occupancy"],
            "tod_factors": traffic["tod_factors"],
            "faults": self.faults,
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
            heartbeat_forward_interval_s=d.get("heartbeat_forward_interval_s", 3600.0),
            anomaly_detection=d.get("anomaly_detection", True),
            adaptive_edge=d.get("adaptive_edge", False),
            silent_threshold_s=d.get("silent_threshold_s"),
            quarantine_threshold=d.get("quarantine_threshold", 5),
            stuck_threshold_s=d.get("stuck_threshold_s"),
            quarantine_release_clean_ticks=d.get("quarantine_release_clean_ticks", 5),
            anomaly_check_interval_s=d.get("anomaly_check_interval_s", 30.0),
            adaptive_check_interval_s=d.get("adaptive_check_interval_s", 30.0),
            adaptive_degrade_threshold=d.get("adaptive_degrade_threshold", 0.90),
            adaptive_filtered_threshold=d.get("adaptive_filtered_threshold", 0.80),
            adaptive_recover_threshold=d.get("adaptive_recover_threshold", 0.95),
            adaptive_min_window_samples=d.get("adaptive_min_window_samples", 10),
            adaptive_dr_smoothing=d.get("adaptive_dr_smoothing", 0.15),
            mqtt_qos=d.get("mqtt_qos", 1),
            mqtt_max_retries_qos1=d.get("mqtt_max_retries_qos1", 3),
            mqtt_max_retries_qos2=d.get("mqtt_max_retries_qos2", 3),
            coap_mode=d.get("coap_mode", "CON"),
            coap_max_retransmit=d.get("coap_max_retransmit", 4),
            coap_ack_timeout_s=d.get("coap_ack_timeout_s", 2.0),
            coap_ack_random_factor=d.get("coap_ack_random_factor", 1.5),
            amqp_exchange=d.get("amqp_exchange", "direct"),
            amqp_ack=d.get("amqp_ack", "manual"),
            amqp_durable=d.get("amqp_durable", True),
            amqp_max_redeliveries=d.get("amqp_max_redeliveries", 3),
            amqp_requeue_delay_s=d.get("amqp_requeue_delay_s", 0.2),
            sim_duration_s=d.get("sim_duration_s", SIM_DURATION_S),
            seed=d.get("seed", 42),
            group=d.get("group", ""),
            group_order=d.get("group_order", 0),
            rate_limit=d.get("rate_limit"),
            contention_channels=d.get("contention_channels"),
            time_scale=d.get("time_scale", DEFAULT_TIME_SCALE),
            base_delay_ms=d.get("base_delay_ms", 80.0),
            jitter_ms=d.get("jitter_ms", 30.0),
            max_payload_bytes=d.get("max_payload_bytes", 222),
            backhaul_base_delay_ms=d.get("backhaul_base_delay_ms", 30.0),
            backhaul_jitter_ms=d.get("backhaul_jitter_ms", 10.0),
            backhaul_loss_rate=d.get("backhaul_loss_rate", 0.02),
            backhaul_downlink_loss_rate=d.get("backhaul_downlink_loss_rate"),
            backhaul_loss_peak_rate=d.get("backhaul_loss_peak_rate"),
            mean_parking_duration_s=d.get("mean_parking_duration_s", 1800.0),
            parking_duration_cv=d.get("parking_duration_cv", 1.5),
            use_time_of_day=d.get("use_time_of_day", False),
            start_hour=d.get("start_hour", 8.0),
            initial_occupancy=d.get("initial_occupancy"),
            tod_factors=d.get("tod_factors"),
            use_dwell_mixture=d.get("use_dwell_mixture", True),
            heartbeat_interval_s=d.get("heartbeat_interval_s", 900.0),
            duplicate_send_prob=d.get("duplicate_send_prob", 0.05),
            is_builtin=d.get("is_builtin", False),
            faults=d.get("faults", [])
        )


def _derive_threshold(value: Optional[float], fallback: float) -> float:
    return value if value is not None else fallback


def make_scenario(
    name: str,
    description: str = "",
    protocol: Protocol = "mqtt",
    architecture: Architecture = "edge_aggregated",
    traffic_level: TrafficLevel = "medium",
    num_spots: int = 50,
    loss_rate: float = 0.05,
    aggregation_interval: float = DEFAULT_AGG_INTERVAL_S,
    max_event_age_s: float = 2.0,
    max_batch_size: int = 50,
    duplicate_window_s: float = 5.0,
    heartbeat_forward_interval_s: float = 3600.0,
    anomaly_detection: bool = True,
    adaptive_edge: bool = False,
    silent_threshold_s: Optional[float] = None,
    quarantine_threshold: int = 5,
    stuck_threshold_s: Optional[float] = None,
    quarantine_release_clean_ticks: int = 5,
    anomaly_check_interval_s: float = 30.0,
    adaptive_check_interval_s: float = 30.0,
    adaptive_degrade_threshold: float = 0.90,
    adaptive_filtered_threshold: float = 0.80,
    adaptive_recover_threshold: float = 0.95,
    adaptive_min_window_samples: int = 10,
    adaptive_dr_smoothing: float = 0.15,
    mqtt_qos: MQTTQoS = 1,
    mqtt_max_retries_qos1: int = 3,
    mqtt_max_retries_qos2: int = 3,
    coap_mode: CoAPMode = "CON",
    coap_max_retransmit: int = 4,
    coap_ack_timeout_s: float = 2.0,
    coap_ack_random_factor: float = 1.5,
    amqp_exchange: AMQPExchange = "direct",
    amqp_ack: AMQPAckMode = "manual",
    amqp_durable: bool = True,
    amqp_max_redeliveries: int = 3,
    amqp_requeue_delay_s: float = 0.2,
    sim_duration_s: float = SIM_DURATION_S,
    seed: int = 42,
    group: str = "",
    group_order: int = 0,
    rate_limit: Optional[float] = None,
    contention_channels: Optional[int] = None,
    time_scale: float = DEFAULT_TIME_SCALE,
    is_builtin: bool = False,
    faults: Optional[list] = None,
    base_delay_ms: float = 80.0,
    jitter_ms: float = 30.0,
    max_payload_bytes: int = 222,
    backhaul_base_delay_ms: float = 30.0,
    backhaul_jitter_ms: float = 10.0,
    backhaul_loss_rate: float = 0.02,
    backhaul_downlink_loss_rate: Optional[float] = None,
    backhaul_loss_peak_rate: Optional[float] = None,
    mean_parking_duration_s: float = 1800.0,
    parking_duration_cv: float = 1.5,
    use_time_of_day: bool = False,
    start_hour: float = 8.0,
    initial_occupancy: Optional[float] = None,
    tod_factors: Optional[list[float]] = None,
    use_dwell_mixture: bool = True,
    heartbeat_interval_s: float = 900.0,
    duplicate_send_prob: float = 0.05
) -> ScenarioConfig:
    arrival = ARRIVAL_RATES[traffic_level] * (num_spots / 50)

    if rate_limit is None:
        rate_limit = DEFAULT_GATEWAY_RATE_MSGS_PER_SEC

    if contention_channels is None:
        contention_channels = DEFAULT_CONTENTION_CHANNELS

    occ = initial_occupancy if initial_occupancy is not None else 0.0

    _silent = _derive_threshold(silent_threshold_s, 3.0 * heartbeat_interval_s)
    _stuck = _derive_threshold(stuck_threshold_s, 43_200.0 + heartbeat_interval_s)

    link = LinkConfig(
        packet_loss_rate=loss_rate,
        rate_limit_msgs_per_sec=rate_limit,
        base_delay_ms=base_delay_ms,
        jitter_ms=jitter_ms,
        max_payload_bytes=max_payload_bytes,
        contention_channels=contention_channels
    )

    backhaul = BackhaulLinkConfig(
        base_delay_ms=backhaul_base_delay_ms,
        jitter_ms=backhaul_jitter_ms,
        packet_loss_rate=backhaul_loss_rate,
        downlink_loss_rate=backhaul_downlink_loss_rate,
        loss_peak_rate=backhaul_loss_peak_rate
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
        silent_threshold_s=_silent,
        quarantine_threshold=quarantine_threshold,
        stuck_threshold_s=_stuck,
        quarantine_release_clean_ticks=quarantine_release_clean_ticks,
        anomaly_check_interval_s=anomaly_check_interval_s,
        adaptive_check_interval_s=adaptive_check_interval_s,
        adaptive_degrade_threshold=adaptive_degrade_threshold,
        adaptive_filtered_threshold=adaptive_filtered_threshold,
        adaptive_recover_threshold=adaptive_recover_threshold,
        adaptive_min_window_samples=adaptive_min_window_samples,
        adaptive_dr_smoothing=adaptive_dr_smoothing
    )
    traffic = TrafficConfig(
        num_spots=num_spots,
        mean_parking_duration_s=mean_parking_duration_s,
        parking_duration_cv=parking_duration_cv,
        sim_duration_s=sim_duration_s,
        random_seed=seed,
        initial_occupancy=occ,
        time_scale=time_scale,
        use_time_of_day=use_time_of_day,
        start_hour=start_hour,
        use_dwell_mixture=use_dwell_mixture,
        heartbeat_interval_s=heartbeat_interval_s,
        duplicate_send_prob=duplicate_send_prob,
        **({"tod_factors": tod_factors} if tod_factors is not None else {})
    )
    return ScenarioConfig(
        name=name, description=description, protocol=protocol,
        architecture=architecture, traffic_level=traffic_level,
        num_spots=num_spots, arrival_rate=arrival, sim_duration_s=sim_duration_s,
        link=link, backhaul_link=backhaul, edge=edge,
        mqtt=MQTTConfig(qos=mqtt_qos, max_retries_qos1=mqtt_max_retries_qos1, max_retries_qos2=mqtt_max_retries_qos2),
        amqp=AMQPConfig(
            exchange_type=amqp_exchange, ack_mode=amqp_ack,
            durable=amqp_durable,
            max_redeliveries=amqp_max_redeliveries,
            requeue_delay_s=amqp_requeue_delay_s,
        ),
        coap=CoAPConfig(
            mode=coap_mode,
            max_retransmit=coap_max_retransmit,
            ack_timeout_s=coap_ack_timeout_s,
            ack_random_factor=coap_ack_random_factor,
        ),
        traffic=traffic, random_seed=seed,
        group=group, group_order=group_order, is_builtin=is_builtin,
        faults=faults or []
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