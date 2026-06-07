from __future__ import annotations
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
    timestamp: float = 0.0
    sequence: int = 0
    is_initial: bool = False
    is_heartbeat_event: bool = False

    def to_dict(self) -> dict:
        d = {
            "sensor_id": self.sensor_id,
            "spot_id": self.spot_id,
            "state": self.state.value,
            "timestamp": self.timestamp,
            "sequence": self.sequence
        }
        if self.is_initial:
            d["is_initial"] = True
        if self.is_heartbeat_event:
            d["is_heartbeat_event"] = True
        return d


@dataclass
class BatchUpdate:
    edge_id: str
    events: list[ParkingEvent]

    def to_dict(self) -> dict:
        return {"edge_id": self.edge_id, "events": [e.to_dict() for e in self.events]}


@dataclass
class SensorState:
    spot_id: int
    state: SpotState = SpotState.FREE
    last_event_seq: int = 0
    last_updated: float = 0.0
    last_forwarded_timestamp: float = 0.0
    last_heartbeat_forwarded_timestamp: float = 0.0
    last_state_change_timestamp: float = 0.0
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
            "drop_rate": self.drop_rate
        }


@dataclass
class ExperimentMetrics:
    scenario_name: str
    protocol: str
    architecture: str
    traffic_level: str
    num_spots: int
    sim_duration_s: float

    latency_mean_ms: Optional[float] = 0.0
    latency_p50_ms: Optional[float] = 0.0
    latency_p95_ms: Optional[float] = 0.0
    latency_p99_ms: Optional[float] = 0.0
    latency_max_ms: Optional[float] = 0.0
    latency_min_ms: Optional[float] = 0.0

    events_generated: int = 0
    valid_state_changes: int = 0
    initial_snapshots_generated: int = 0
    heartbeats_generated: int = 0
    duplicate_sends_generated: int = 0
    heartbeat_interval_s: float = 0.0

    sensor_to_edge_msgs: int = 0
    sensor_link_dropped: int = 0
    sensor_to_edge_delivery_ratio: Optional[float] = 0.0
    sensor_to_edge_bytes: int = 0
    bytes_s2e_received: int = 0

    filtered_events: int = 0
    heartbeats_suppressed: int = 0
    quarantine_suppressed: int = 0
    heartbeats_forwarded: int = 0
    events_forwarded_total: int = 0

    edge_to_cloud_msgs: int = 0
    edge_to_cloud_bytes: int = 0
    edge_to_cloud_dropped: int = 0
    edge_to_cloud_delivered: int = 0
    bytes_e2c_received: int = 0
    backhaul_delivery_ratio: Optional[float] = 1.0

    aggregation_ratio: Optional[float] = 0.0
    message_reduction_ratio: Optional[float] = 0.0
    events_per_cloud_message: Optional[float] = 0.0

    protocol_bytes: int = 0
    retransmissions_total: int = 0
    duplicate_deliveries: int = 0

    cloud_msgs_received_total: int = 0
    cloud_state_changes_reflected: int = 0
    duplicate_events_at_cloud: int = 0
    e2e_unique_delivery_ratio: Optional[float] = None
    cloud_reflection_ratio: Optional[float] = 0.0  
    physical_delivery_ratio: Optional[float] = 0.0  
    cloud_state_agreement_ratio: Optional[float] = 0.0   

    anomalies_detected: int = 0
    anomalies_resolved: int = 0
    active_anomalies: int = 0
    adaptive_mode_switches: int = 0
    quarantined_spots_final: int = 0
    anomaly_detected_spots: int = 0

    fault_injected_count: int = 0

    cloud_only_msgs: int = 0
    transport_msgs_total: int = 0
    edge_to_cloud_delivery_ratio: float = 1.0
    end_to_end_delivery_ratio: float = 0.0

    seed: int = 0
    run_id: str = ""

    latency_samples: list[float] = field(default_factory=list)
    scenario_log: list[dict] = field(default_factory=list)

    @property
    def events_generated_total(self) -> int:
        return self.events_generated

    @property
    def state_changes_generated_total(self) -> int:
        return self.valid_state_changes

    @property
    def heartbeats_generated_total(self) -> int:
        return self.heartbeats_generated

    @property
    def initial_snapshots_generated_total(self) -> int:
        return self.initial_snapshots_generated

    @property
    def duplicate_sends_generated_total(self) -> int:
        return self.duplicate_sends_generated

    @property
    def frames_s2e_sent(self) -> int:
        return self.sensor_to_edge_msgs

    @property
    def frames_s2e_dropped(self) -> int:
        return self.sensor_link_dropped

    @property
    def frames_s2e_delivered(self) -> int:
        return self.sensor_to_edge_msgs - self.sensor_link_dropped

    @property
    def bytes_s2e_sent(self) -> int:
        return self.sensor_to_edge_bytes

    @property
    def s2e_delivery_ratio(self) -> Optional[float]:
        return self.sensor_to_edge_delivery_ratio

    @property
    def events_filtered_total(self) -> int:
        return self.filtered_events

    @property
    def frames_e2c_sent(self) -> int:
        return self.edge_to_cloud_msgs

    @property
    def frames_e2c_delivered(self) -> int:
        return self.edge_to_cloud_delivered

    @property
    def frames_e2c_dropped(self) -> int:
        return self.edge_to_cloud_dropped

    @property
    def bytes_e2c_sent(self) -> int:
        return self.edge_to_cloud_bytes

    @property
    def aggregation_batches_e2c(self) -> int:
        return self.edge_to_cloud_msgs

    @property
    def proto_bytes_sent(self) -> int:
        return self.protocol_bytes

    @property
    def proto_retransmissions(self) -> int:
        return self.retransmissions_total

    @property
    def proto_duplicate_deliveries(self) -> int:
        return self.duplicate_deliveries

    @property
    def unique_state_changes_applied_at_cloud(self) -> int:
        return self.cloud_state_changes_reflected

    def to_dict(self) -> dict:
        arch = self.architecture
        is_cloud_only = arch == "cloud_only"
        is_edge_agg = arch == "edge_aggregated"

        wire_frames_delivered = self.sensor_to_edge_msgs - self.sensor_link_dropped

        d: dict = {
            "scenario_name": self.scenario_name,
            "seed": self.seed,
            "run_id": self.run_id,
            "protocol": self.protocol,
            "architecture": self.architecture,
            "traffic_level": self.traffic_level,
            "num_spots": self.num_spots,
            "sim_duration_s": self.sim_duration_s,

            "events_generated": self.events_generated,
            "valid_state_changes": self.valid_state_changes,
            "initial_snapshots_generated": self.initial_snapshots_generated,
            "heartbeats_generated": self.heartbeats_generated,
            "duplicate_sends_generated": self.duplicate_sends_generated,
            "heartbeat_interval_s": self.heartbeat_interval_s,

            "sensor_to_edge_msgs": self.sensor_to_edge_msgs,
            "sensor_link_dropped": self.sensor_link_dropped,
            "wire_frames_delivered": wire_frames_delivered,
            "sensor_to_edge_delivery_ratio": self.sensor_to_edge_delivery_ratio,
            "sensor_to_edge_bytes": self.sensor_to_edge_bytes,

            "protocol_bytes": self.protocol_bytes,
            "retransmissions_total": self.retransmissions_total,
            "duplicate_deliveries": self.duplicate_deliveries,

            "cloud_msgs_received_total": self.cloud_msgs_received_total,
            "cloud_state_changes_reflected": self.cloud_state_changes_reflected,
            "cloud_reflection_ratio": self.cloud_reflection_ratio,

            "physical_delivery_ratio": self.physical_delivery_ratio,
            "cloud_state_agreement_ratio": self.cloud_state_agreement_ratio,

            "fault_injected_count": self.fault_injected_count,

            "latency_mean_ms": self.latency_mean_ms,
            "latency_p50_ms": self.latency_p50_ms,
            "latency_p95_ms": self.latency_p95_ms,
            "latency_p99_ms": self.latency_p99_ms,
            "latency_min_ms": self.latency_min_ms,
            "latency_max_ms": self.latency_max_ms,
        }

        d.update({
            "events_generated_total": self.events_generated,
            "state_changes_generated_total": self.valid_state_changes,
            "heartbeats_generated_total": self.heartbeats_generated,
            "initial_snapshots_generated_total": self.initial_snapshots_generated,
            "duplicate_sends_generated_total": self.duplicate_sends_generated,

            "frames_s2e_sent": self.sensor_to_edge_msgs,
            "frames_s2e_delivered": wire_frames_delivered,
            "frames_s2e_dropped": self.sensor_link_dropped,
            "bytes_s2e_sent": self.sensor_to_edge_bytes,
            "bytes_s2e_received": self.bytes_s2e_received,
            "s2e_delivery_ratio": self.sensor_to_edge_delivery_ratio,

            "proto_bytes_sent": self.protocol_bytes,
            "proto_retransmissions": self.retransmissions_total,
            "proto_duplicate_deliveries": self.duplicate_deliveries,

            "unique_state_changes_applied_at_cloud": self.cloud_state_changes_reflected,
            "duplicate_events_at_cloud": self.duplicate_events_at_cloud,
            "e2e_unique_delivery_ratio": self.e2e_unique_delivery_ratio,
        })

        if is_cloud_only:
            d["cloud_msgs_received"] = self.cloud_msgs_received_total
            d["e2e_delivery_ratio"] = (
                round(self.sensor_to_edge_delivery_ratio, 4)
                if self.sensor_to_edge_delivery_ratio is not None else None
            )
            d["edge_to_cloud_dropped"] = 0
            d["backhaul_delivery_ratio"] = 1.0
            d.update({
                "frames_e2c_sent": 0,
                "frames_e2c_delivered": 0,
                "frames_e2c_dropped": 0,
                "bytes_e2c_sent": 0,
                "bytes_e2c_received": 0,
                "events_forwarded_total": 0,
                "aggregation_ratio": None,
                "message_reduction_ratio": None,
            })
        else:
            d["edge_to_cloud_msgs"] = self.edge_to_cloud_msgs
            d["edge_to_cloud_bytes"] = self.edge_to_cloud_bytes
            d["edge_to_cloud_dropped"] = self.edge_to_cloud_dropped
            d["backhaul_delivery_ratio"] = self.backhaul_delivery_ratio

            d["filtered_events"] = self.filtered_events
            d["heartbeats_suppressed"] = self.heartbeats_suppressed
            d["quarantine_suppressed"] = self.quarantine_suppressed
            d["heartbeats_forwarded"] = self.heartbeats_forwarded

            d["message_reduction_ratio"] = self.message_reduction_ratio
            d["aggregation_ratio"] = self.aggregation_ratio

            d["anomalies_detected"] = self.anomalies_detected
            d["anomalies_resolved"] = self.anomalies_resolved
            d["active_anomalies"] = self.active_anomalies
            d["adaptive_mode_switches"] = self.adaptive_mode_switches
            d["quarantined_spots_final"] = self.quarantined_spots_final
            d["anomaly_detected_spots"] = self.anomaly_detected_spots

            if is_edge_agg:
                d["events_per_cloud_message"] = self.events_per_cloud_message
                d["aggregation_batches_created"] = self.edge_to_cloud_msgs

            d.update({
                "frames_e2c_sent": self.edge_to_cloud_msgs,
                "frames_e2c_delivered": self.edge_to_cloud_delivered,
                "frames_e2c_dropped": self.edge_to_cloud_dropped,
                "bytes_e2c_sent": self.edge_to_cloud_bytes,
                "bytes_e2c_received": self.bytes_e2c_received,
                "events_filtered_total": self.filtered_events,
                "events_forwarded_total": self.events_forwarded_total,
                "aggregation_batches_e2c": self.edge_to_cloud_msgs,
            })

        return d