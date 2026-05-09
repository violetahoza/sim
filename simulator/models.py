from __future__ import annotations
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


class SpotState(str, Enum):
    FREE = "free"
    OCCUPIED = "occupied"


@dataclass
class ParkingEvent:
    sensor_id: str
    spot_id: int
    state: SpotState
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0

    def to_dict(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "spot_id": self.spot_id,
            "state": self.state.value,
            "timestamp": self.timestamp,
            "sequence": self.sequence,
        }


@dataclass
class BatchUpdate:
    edge_id: str
    events: list[ParkingEvent]
    created_at: float = field(default_factory=time.time)
    bytes_size: int = 0

    def to_dict(self) -> dict:
        return {
            "edge_id": self.edge_id,
            "created_at": self.created_at,
            "events": [e.to_dict() for e in self.events],
        }


@dataclass
class SensorState:
    spot_id: int
    state: SpotState = SpotState.FREE
    last_event_seq: int = 0
    last_updated: float = field(default_factory=time.time)
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
        }


@dataclass
class LatencyRecord:
    event_id: str
    protocol: str
    sent_at: float
    received_at: float
    latency_ms: float
    architecture: str


@dataclass
class ExperimentMetrics:
    scenario_name: str
    protocol: str
    architecture: str
    traffic_level: str
    num_spots: int
    sim_duration_s: float

    latency_mean_ms: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    latency_max_ms: float = 0.0
    latency_min_ms: float = 0.0

    sensor_to_edge_msgs: int = 0
    edge_to_cloud_msgs: int = 0
    cloud_only_msgs: int = 0

    sensor_to_edge_bytes: int = 0
    edge_to_cloud_bytes: int = 0

    sensor_to_edge_delivery_ratio: float = 0.0
    edge_to_cloud_delivery_ratio: float = 0.0

    aggregation_ratio: float = 0.0
    filtered_events: int = 0

    anomalies_detected: int = 0

    edge_cpu_pct: float = 0.0
    edge_mem_mb: float = 0.0
    cloud_cpu_pct: float = 0.0
    cloud_mem_mb: float = 0.0

    latency_timeseries: list[dict] = field(default_factory=list)
    latency_samples: list[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("latency_samples", None)
        d.pop("latency_timeseries", None)
        return d

    def to_prompt_context(self) -> str:
        reduction = (1.0 - self.aggregation_ratio) * 100
        lines = [
            f"Results for scenario '{self.scenario_name}':",
            f"  Protocol: {self.protocol.upper()}  Architecture: {self.architecture}",
            f"  Spots: {self.num_spots}  Traffic: {self.traffic_level}  Duration: {self.sim_duration_s / 3600:.1f} h",
            f"  Events: sensor→edge={self.sensor_to_edge_msgs}  edge→cloud={self.edge_to_cloud_msgs}  (cloud_only={self.cloud_only_msgs})",
            f"  Bytes:  sensor→edge={self.sensor_to_edge_bytes:,}  edge→cloud={self.edge_to_cloud_bytes:,}",
            f"  Latency (ms): mean={self.latency_mean_ms:.1f}  P50={self.latency_p50_ms:.1f}  P95={self.latency_p95_ms:.1f}  P99={self.latency_p99_ms:.1f}  min={self.latency_min_ms:.1f}  max={self.latency_max_ms:.1f}",
            f"  Delivery: S→E={self.sensor_to_edge_delivery_ratio:.1%}  E→C={self.edge_to_cloud_delivery_ratio:.1%}",
            f"  Aggregation ratio: {self.aggregation_ratio:.3f}  ({reduction:.1f}% cloud message reduction)",
            f"  Filtered events: {self.filtered_events}  Anomalies detected: {self.anomalies_detected}",
            f"  Resources: edge CPU={self.edge_cpu_pct:.1f}%  edge mem={self.edge_mem_mb:.1f} MB  cloud CPU={self.cloud_cpu_pct:.1f}%  cloud mem={self.cloud_mem_mb:.1f} MB",
        ]
        if self.latency_timeseries:
            last = self.latency_timeseries[-1]
            first = self.latency_timeseries[0]
            lines.append(f"  Latency drift: {first['mean_ms']:.1f} ms at t={first['t_s']:.0f}s → {last['mean_ms']:.1f} ms at t={last['t_s']:.0f}s")
        return "\n".join(lines)
