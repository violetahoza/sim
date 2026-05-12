from __future__ import annotations
from dataclasses import dataclass, field, asdict
from enum import Enum


class SpotState(str, Enum):
    FREE = "free"
    OCCUPIED = "occupied"


@dataclass
class ParkingEvent:
    sensor_id: str
    spot_id: int
    state: SpotState
    timestamp: float = 0.0
    sequence: int = 0
    is_initial: bool = False

    def to_dict(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "spot_id": self.spot_id,
            "state": self.state.value,
            "timestamp": self.timestamp,
            "sequence": self.sequence
        }


@dataclass
class BatchUpdate:
    edge_id: str
    events: list[ParkingEvent]

    def to_dict(self) -> dict:
        return {
            "edge_id": self.edge_id,
            "events": [e.to_dict() for e in self.events],
        }


@dataclass
class SensorState:
    spot_id: int
    state: SpotState = SpotState.FREE
    last_event_seq: int = 0
    last_updated: float = 0.0
    total_events: int = 0
    consecutive_same: int = 0


@dataclass
class LinkStats:
    name: str
    sent: int = 0
    received: int = 0
    dropped: int = 0
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    retransmissions: int = 0
    duplicate_deliveries: int = 0

    @property
    def delivery_ratio(self) -> float:
        return self.received / self.sent if self.sent > 0 else 1.0

    @property
    def drop_rate(self) -> float:
        return self.dropped / self.sent if self.sent > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "sent": self.sent,
            "received": self.received,
            "dropped": self.dropped,
            "total_bytes_sent": self.total_bytes_sent,
            "total_bytes_received": self.total_bytes_received,
            "delivery_ratio": self.delivery_ratio,
            "drop_rate": self.drop_rate,
            "retransmissions": self.retransmissions,
            "duplicate_deliveries": self.duplicate_deliveries
        }


@dataclass
class ExperimentMetrics:
    scenario_name: str
    protocol: str
    architecture: str
    traffic_level: str
    num_spots: int
    sim_duration_s: float
    group: str = ""

    latency_mean_ms: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    latency_max_ms: float = 0.0
    latency_min_ms: float = 0.0
    latency_mean_ms_with_warmup: float = 0.0

    warmup_s: float = 0.0
    warmup_events_excluded: int = 0

    sensor_to_edge_msgs: int = 0
    edge_to_cloud_msgs: int = 0
    cloud_only_msgs: int = 0

    transport_msgs_total: int = 0  
    retransmissions_total: int = 0   
    duplicate_deliveries: int = 0 

    sensor_to_edge_bytes: int = 0
    edge_to_cloud_bytes: int = 0
    protocol_bytes: int = 0

    sensor_to_edge_delivery_ratio: float = 0.0
    edge_to_cloud_delivery_ratio: float = 0.0
    end_to_end_delivery_ratio: float = 0.0

    aggregation_ratio: float = 0.0
    filtered_events: int = 0
    anomalies_detected: int = 0
    adaptive_mode_switches: int = 0

    edge_cpu_pct: float = 0.0
    edge_mem_mb: float = 0.0
    cloud_cpu_pct: float = 0.0
    cloud_mem_mb: float = 0.0

    broker_overhead_score: float = 0.0

    latency_timeseries: list[dict] = field(default_factory=list)
    latency_samples: list[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("latency_samples", None)
        d.pop("latency_timeseries", None)
        return d