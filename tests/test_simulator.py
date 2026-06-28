from __future__ import annotations
import json
import math
import random

import msgpack
import pytest

import simulator.config.config as config_mod
from simulator.config.config import make_scenario, ScenarioConfig, TrafficConfig, LinkConfig, MQTTConfig, AMQPConfig, CoAPConfig, PREDEFINED_SCENARIOS, save_custom_scenarios, load_custom_scenarios
from simulator.config.constants import compute_lora_airtime_s
from simulator.des.engine import SimClock
from simulator.cloud.cloud_backend import CloudBackend
from simulator.edge.edge_node import EdgeNode
from simulator.sensors.sensor_emulator import SensorEmulator
from simulator.sensors.fault_injector import FaultInjector, FaultSpec, FaultType
from simulator.traffic.traffic_model import TrafficModel
from simulator.link.link_emulator import LinkEmulator, TokenBucket, GilbertElliotModel, QueueOverflowModel, SharedMediumModel
from simulator.models.models import ParkingEvent, BatchUpdate, SpotState, ExperimentMetrics
from simulator.protocols.mqtt_client import SimulatedMQTTBackend
from simulator.protocols.amqp_client import SimulatedAMQPBackend
from simulator.protocols.coap_client import SimulatedCoAPBackend
from simulator.utils import encode_event, encode_batch
from experiments.runner import run_scenario_sync, save_results, _stats, _make_loss_provider
from experiments.aggregate import _summarise, _is_number, aggregate

SEED = 20250607
ARCH = ["cloud_only", "edge_filtered", "edge_aggregated"]
PROTOCOLS = ["mqtt", "amqp", "coap"]


def _edge_scn(name: str, **kw):
    """edge_filtered scenario with no sensor-link loss and heavy backhaul loss, so protocol-level recovery is the only thing that separates the variants."""
    base = dict(name=name, protocol="mqtt", architecture="edge_filtered", traffic_level="peak", num_spots=200, loss_rate=0.0, backhaul_loss_rate=0.30, sim_duration_s=3600.0, seed=SEED, heartbeat_interval_s=1_000_000.0,  anomaly_detection=False)
    base.update(kw)
    return make_scenario(**base)


def test_workload_identity_partition():
    cfg = make_scenario(name="t_identity", protocol="mqtt", architecture="edge_filtered", traffic_level="medium", num_spots=100, loss_rate=0.0, backhaul_loss_rate=0.05, sim_duration_s=3600.0, seed=SEED, anomaly_detection=False)
    m = run_scenario_sync(cfg)
    assert m.events_generated_total > 0
    assert m.events_generated_total == (m.state_changes_generated_total + m.heartbeats_generated_total + m.initial_snapshots_generated_total + m.duplicate_sends_generated_total)
    assert m.events_generated_total == m.events_generated
    assert m.state_changes_generated_total == m.valid_state_changes


def test_reliability_ordering_mqtt_qos():
    q0 = run_scenario_sync(_edge_scn("t_q0", protocol="mqtt", mqtt_qos=0))
    q1 = run_scenario_sync(_edge_scn("t_q1", protocol="mqtt", mqtt_qos=1))
    q2 = run_scenario_sync(_edge_scn("t_q2", protocol="mqtt", mqtt_qos=2))
    for m in (q0, q1, q2):
        assert m.e2e_unique_delivery_ratio is not None
    assert q0.e2e_unique_delivery_ratio < q1.e2e_unique_delivery_ratio
    assert q0.e2e_unique_delivery_ratio < q2.e2e_unique_delivery_ratio
    # QoS0 (fire & forget) and QoS2 (exactly-once) never deliver duplicates
    assert q2.duplicate_deliveries == 0
    assert q0.duplicate_deliveries == 0
    # QoS1 (at-least-once) delivers duplicates: a lost PUBACK retransmits the PUBLISH, so the broker re-delivers. The cloud dedupes them by (spot, seq).
    assert q1.duplicate_deliveries > 0
    assert q1.duplicate_events_at_cloud > 0


def test_reliability_ordering_coap_non_vs_con():
    non = run_scenario_sync(_edge_scn("t_non", protocol="coap", coap_mode="NON"))
    con = run_scenario_sync(_edge_scn("t_con", protocol="coap", coap_mode="CON"))
    assert non.e2e_unique_delivery_ratio is not None
    assert con.e2e_unique_delivery_ratio is not None
    assert non.e2e_unique_delivery_ratio < con.e2e_unique_delivery_ratio


def test_reliability_ordering_amqp_auto_vs_manual():
    auto = run_scenario_sync(_edge_scn("t_auto", protocol="amqp", amqp_ack="auto", amqp_durable=False))
    manual = run_scenario_sync(_edge_scn("t_manual", protocol="amqp", amqp_ack="manual", amqp_durable=True))
    assert auto.e2e_unique_delivery_ratio is not None
    assert manual.e2e_unique_delivery_ratio is not None
    assert auto.e2e_unique_delivery_ratio < manual.e2e_unique_delivery_ratio

def test_cloud_dedup_same_identity_not_double_counted():
    cfg = make_scenario(name="t_dedup", num_spots=4, loss_rate=0.0, sim_duration_s=1.0, seed=SEED)
    clock = SimClock()
    cloud = CloudBackend(cfg, clock, epoch=0.0)

    ev = ParkingEvent(sensor_id="s0", spot_id=0, state=SpotState.OCCUPIED, timestamp=0.0, sequence=1)
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[ev]), b"x")
    assert cloud.transitions_received == 1  
    assert cloud.duplicate_events_at_cloud == 0

    dup = ParkingEvent(sensor_id="s0", spot_id=0, state=SpotState.OCCUPIED, timestamp=0.0, sequence=1)  
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[dup]), b"x")
    assert cloud.transitions_received == 1   
    assert cloud.duplicate_events_at_cloud == 1


def test_stats_empty_returns_none():
    assert _stats([]) == (None, None, None, None, None, None)
    mean, p50, p95, p99, mn, mx = _stats([10.0, 20.0])
    assert mean == 15.0


def test_to_dict_emits_null_not_zero():
    m = ExperimentMetrics(
        scenario_name="t", protocol="mqtt", architecture="cloud_only", traffic_level="low", num_spots=1, sim_duration_s=1.0,
        latency_mean_ms=None, latency_p99_ms=None, e2e_unique_delivery_ratio=None, aggregation_ratio=None, message_reduction_ratio=None
    )
    d = m.to_dict()
    assert d["latency_mean_ms"] is None
    assert d["latency_p99_ms"] is None
    assert d["e2e_unique_delivery_ratio"] is None
    assert d["aggregation_ratio"] is None
    js = json.dumps(d)
    assert '"latency_mean_ms": null' in js  


def test_null_policy_end_to_end_no_state_changes():
    cfg = make_scenario(name="t_empty", protocol="mqtt", architecture="cloud_only", traffic_level="low", num_spots=1, loss_rate=0.0, sim_duration_s=0.01, seed=SEED, heartbeat_interval_s=1_000_000.0, anomaly_detection=False)
    m = run_scenario_sync(cfg)
    assert m.valid_state_changes == 0
    assert m.latency_mean_ms is None
    assert m.latency_p99_ms is None
    assert m.e2e_unique_delivery_ratio is None
    assert m.aggregation_ratio is None
    assert m.message_reduction_ratio is None


def test_summarise_math_and_edge_cases():
    s = _summarise([10.0, 12.0, 14.0])
    assert s["n"] == 3
    assert abs(s["mean"] - 12.0) < 1e-9
    assert abs(s["std"] - 2.0) < 1e-9
    assert abs(s["ci95_halfwidth"] - (4.303 * 2.0 / math.sqrt(3))) < 1e-3  
    assert _summarise([])["n"] == 0
    assert _summarise([5.0])["ci95_low"] is None 
    assert _is_number(None) is False
    assert _is_number(True) is False
    assert _is_number(3.0) is True


def test_aggregator_skips_nulls(tmp_path):
    base = {"scenario_name": "X", "protocol": "mqtt", "architecture": "edge_filtered", "traffic_level": "peak", "e2e_unique_delivery_ratio": 0.9}
    r1 = dict(base, seed=1, run_id="a", latency_mean_ms=100.0)
    r2 = dict(base, seed=2, run_id="b", latency_mean_ms=None) 
    (tmp_path / "X__seed1__a.json").write_text(json.dumps(r1))
    (tmp_path / "X__seed2__b.json").write_text(json.dumps(r2))

    summary = aggregate(tmp_path, tmp_path / "agg")
    block = summary["X"]
    assert block["num_runs"] == 2
    assert block["seeds"] == [1, 2]

    lat = block["metrics"]["latency_mean_ms"]
    assert lat["n"] == 1                
    assert lat["ci95_low"] is None    

    e2e = block["metrics"]["e2e_unique_delivery_ratio"]
    assert e2e["n"] == 2               
    assert e2e["ci95_low"] is not None


def _advance(clock: SimClock, t: float) -> None:
    """Advance virtual time to t, running every event scheduled up to t."""
    clock.env.run(until=t)


def test_simclock_schedule_fires_in_time_order():
    clock = SimClock()
    fired: list[str] = []
    clock.schedule(0.3, lambda: fired.append("c"))
    clock.schedule(0.1, lambda: fired.append("a"))
    clock.schedule(0.2, lambda: fired.append("b"))
    _advance(clock, 1.0)
    assert fired == ["a", "b", "c"]
    assert clock.now == 1.0

def test_simclock_schedule_at_absolute_time():
    clock = SimClock()
    seen: list[float] = []
    clock.schedule_at(0.5, lambda: seen.append(clock.now))
    _advance(clock, 1.0)
    assert len(seen) == 1
    assert abs(seen[0] - 0.5) < 1e-9



def _lossless_link_cfg(**kw) -> LinkConfig:
    base = dict(base_delay_ms=10.0, jitter_ms=0.0, packet_loss_rate=0.0, max_payload_bytes=100_000, rate_limit_msgs_per_sec=0.0)
    base.update(kw)
    return LinkConfig(**base)

def test_link_zero_loss_delivers_all_and_byte_accounting_balances():
    import random
    clock = SimClock()
    received: list[ParkingEvent] = []
    link = LinkEmulator(_lossless_link_cfg(), clock, forward_cb=lambda ev, raw: received.append(ev), rng=random.Random(0))
    for i in range(20):
        link.transmit(ParkingEvent("s%d" % i, i, SpotState.OCCUPIED, timestamp=0.0, sequence=i + 1))
    _advance(clock, 1.0)
    assert link.stats.sent == 20
    assert link.stats.received == 20
    assert link.stats.dropped == 0
    assert abs(link.stats.delivery_ratio - 1.0) < 1e-9
    # with no loss, bytes that left == bytes that arrived
    assert link.stats.total_bytes_received == link.stats.total_bytes_sent
    assert len(received) == 20

def test_link_oversized_payload_is_dropped():
    import random
    clock = SimClock()
    link = LinkEmulator(_lossless_link_cfg(max_payload_bytes=1), clock, forward_cb=lambda ev, raw: None, rng=random.Random(0))
    for i in range(10):
        link.transmit(ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=i + 1))
    assert link.stats.sent == 10
    assert link.stats.dropped == 10
    assert link.stats.received == 0
    assert link.stats.delivery_ratio == 0.0

def test_gilbert_elliot_zero_loss_never_drops():
    import random
    model = GilbertElliotModel(base_loss_rate=0.0, rng=random.Random(1))
    assert all(model.should_drop() is False for _ in range(1000))

def test_token_bucket_spaces_out_when_rate_limited():
    clock = SimClock() # stays at now=0 for this unit test
    bucket = TokenBucket(rate=2.0) # 1 token every 0.5 s
    assert bucket.consume(clock) == 0.0 # first token available immediately
    assert abs(bucket.consume(clock) - 0.5) < 1e-9 # next must wait one interval

def test_token_bucket_disabled_when_rate_zero():
    clock = SimClock()
    bucket = TokenBucket(rate=0.0)
    assert all(bucket.consume(clock) == 0.0 for _ in range(5))

def test_queue_overflow_model_caps_depth():
    q = QueueOverflowModel(capacity=2)
    assert q.try_enqueue() is True
    assert q.try_enqueue() is True
    assert q.try_enqueue() is False # full
    assert q.overflow_drops == 1
    q.dequeue()
    assert q.try_enqueue() is True # space freed


def test_shared_medium_collides_on_overlap_not_when_spaced():
    import random
    m = SharedMediumModel(channels=1, rng=random.Random(0))
    # two transmissions overlapping in time on the single shared channel -> both corrupted
    t1 = m.begin(now=0.0, airtime=1.0)
    t2 = m.begin(now=0.5, airtime=1.0) # starts while t1 is still on air
    assert t1[0] is False and t2[0] is False
    assert m.collisions == 1
    # a transmission starting after the medium is free -> survives
    t3 = m.begin(now=10.0, airtime=1.0)
    assert t3[0] is True


def test_anomaly_detector_precision_and_recall():
    cfg = make_scenario(
        name="t_anom", protocol="mqtt", architecture="edge_filtered", traffic_level="peak",
        num_spots=150, mqtt_qos=1, sim_duration_s=14400.0, use_time_of_day=False,
        loss_rate=0.02, backhaul_loss_rate=0.02, seed=SEED, anomaly_detection=True,
        heartbeat_interval_s=900.0, silent_threshold_s=3600.0,
        quarantine_threshold=5,
        faults=[{"type": "flooding", "count": 5, "flood_count": 10},
                {"type": "replay", "count": 5, "replay_count": 3},
                {"type": "flapping", "count": 5},
                {"type": "silent", "count": 5}]
    )
    m = run_scenario_sync(cfg)
    assert m.fault_true_count == 20
    assert m.anomaly_recall is not None and m.anomaly_recall >= 0.8
    assert m.anomaly_precision is not None and m.anomaly_precision >= 0.7
    assert m.anomaly_detected_spots < cfg.num_spots * 0.4


def test_scalability_contention_degrades_with_scale():
    def s2e(n: int) -> float:
        cfg = make_scenario(name=f"sc{n}", protocol="mqtt", architecture="edge_filtered", traffic_level="peak", num_spots=n, mqtt_qos=1, sim_duration_s=3600.0, loss_rate=0.05, backhaul_loss_rate=0.02, seed=3001)
        return run_scenario_sync(cfg).sensor_to_edge_delivery_ratio
    dr_small, dr_large = s2e(50), s2e(2000)
    assert dr_small > dr_large + 0.1  # clearly degraded by contention at scale



def test_sensor_event_classification_partition():
    cfg = TrafficConfig(num_spots=2)
    sensors = SensorEmulator(cfg, arrival_rate=0.01)
    seen: list[ParkingEvent] = []
    sensors.add_callback(seen.append)

    # spot 0 starts FREE in SensorState
    sensors._on_event(ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1, is_initial=True)) # initial snapshot
    sensors._on_event(ParkingEvent("s0", 0, SpotState.FREE, timestamp=1.0, sequence=2)) # real transition
    sensors._on_event(ParkingEvent("s0", 0, SpotState.FREE, timestamp=2.0, sequence=3)) # duplicate send (same state)
    sensors._on_event(ParkingEvent("s0", 0, SpotState.FREE, timestamp=3.0, sequence=4, is_heartbeat_event=True)) # heartbeat

    assert sensors.initial_snapshots_generated == 1
    assert sensors.state_changes_generated == 1
    assert sensors.duplicate_sends_generated == 1
    assert sensors.heartbeats_generated == 1
    assert sensors.total_generated == 4
    assert sensors.total_generated == (sensors.state_changes_generated + sensors.heartbeats_generated + sensors.initial_snapshots_generated + sensors.duplicate_sends_generated)
    # no fault injector -> every event passed through to callbacks
    assert len(seen) == 4

def test_sensor_occupancy_snapshot_tracks_state():
    cfg = TrafficConfig(num_spots=4)
    sensors = SensorEmulator(cfg, arrival_rate=0.01)
    sensors._on_event(ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1))
    sensors._on_event(ParkingEvent("s1", 1, SpotState.OCCUPIED, timestamp=0.0, sequence=1))
    snap = sensors.occupancy_snapshot()
    assert snap["total"] == 4 and snap["occupied"] == 2 and snap["free"] == 2


def _edge(arch: str, **kw) -> tuple[EdgeNode, SimClock, list[BatchUpdate]]:
    base = dict(name="t_edge", architecture=arch, num_spots=8, loss_rate=0.0, anomaly_detection=False, adaptive_edge=False, seed=SEED)
    base.update(kw)
    cfg = make_scenario(**base)
    clock = SimClock()
    batches: list[BatchUpdate] = []
    edge = EdgeNode(cfg, clock, cloud_cb=lambda b, raw: batches.append(b), epoch=0.0)
    return edge, clock, batches

def test_edge_filtered_forwards_change_filters_same_state_in_window():
    edge, clock, batches = _edge("edge_filtered", duplicate_window_s=5.0)
    edge.receive(ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1), b"x") # forward
    edge.receive(ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=1.0, sequence=2), b"x") # filtered (same state, in window)
    edge.receive(ParkingEvent("s0", 0, SpotState.FREE, timestamp=2.0, sequence=3), b"x") # forward (state change)
    assert edge.forwarded_events == 2
    assert edge.filtered_count == 1
    assert len(batches) == 2

def test_edge_heartbeat_suppressed_when_not_due():
    edge, clock, batches = _edge("edge_filtered", heartbeat_forward_interval_s=3600.0)
    # first heartbeat (ts>0 so it isn't treated as never forwarded) is forwarded
    edge.receive(ParkingEvent("s0", 0, SpotState.FREE, timestamp=100.0, sequence=1, is_heartbeat_event=True), b"x")
    # second heartbeat well within the forward interval -> suppressed
    edge.receive(ParkingEvent("s0", 0, SpotState.FREE, timestamp=110.0, sequence=2, is_heartbeat_event=True), b"x")
    assert edge.heartbeats_forwarded == 1
    assert edge.heartbeats_suppressed == 1
    assert len(batches) == 1


def test_edge_aggregated_collapses_events_into_one_batch():
    edge, clock, batches = _edge("edge_aggregated", aggregation_interval=1.0, max_event_age_s=10.0, max_batch_size=50)
    for sid in range(3):
        edge.receive(ParkingEvent("s%d" % sid, sid, SpotState.OCCUPIED, timestamp=0.0, sequence=1), b"x")
    assert len(batches) == 0 # buffered, not yet flushed
    _advance(clock, 1.5) # aggregation tick at t=1.0 flushes
    assert len(batches) == 1
    assert len(batches[0].events) == 3
    assert edge.forwarded_events == 3
    assert edge.stats.sent == 1 # three events -> a single cloud frame

def test_edge_record_cloud_drop_increments_dropped():
    edge, clock, batches = _edge("edge_filtered")
    edge.record_cloud_drop()
    edge.record_cloud_drop()
    assert edge.stats.dropped == 2

def test_edge_summary_exposes_expected_keys():
    edge, clock, batches = _edge("edge_aggregated")
    s = edge.summary()
    for key in ("received", "filtered", "forwarded_events", "heartbeats_suppressed", "quarantine_suppressed", "anomalies", "active_anomalies", "mode_switches", "link_stats", "quarantined_count", "event_log"):
        assert key in s


def _cloud(num_spots: int = 4) -> tuple[CloudBackend, SimClock]:
    cfg = make_scenario(name="t_cloud", num_spots=num_spots, loss_rate=0.0, sim_duration_s=1.0, seed=SEED)
    clock = SimClock()
    return CloudBackend(cfg, clock, epoch=0.0), clock

def test_cloud_latency_only_state_changes_in_headline_bucket():
    cloud, clock = _cloud()
    _advance(clock, 1.5) 
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1)]), b"x")
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[ParkingEvent("s1", 1, SpotState.FREE, timestamp=0.0, sequence=1, is_heartbeat_event=True)]), b"x")

    samples = cloud.get_all_latency_samples()
    assert len(samples) == 1
    assert abs(samples[0] - 1500.0) < 1e-6 
    assert cloud.transitions_received == 1
    assert cloud.received_events == 2


def test_cloud_state_agreement_against_ground_truth():
    cloud, clock = _cloud(num_spots=4)
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[
        ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1),
        ParkingEvent("s1", 1, SpotState.OCCUPIED, timestamp=0.0, sequence=1)]), b"x")
    truth_match = {0: "occupied", 1: "occupied", 2: "free", 3: "free"}
    truth_mismatch = {0: "free", 1: "occupied", 2: "free", 3: "free"}
    assert cloud.compute_state_agreement(truth_match) == 1.0
    assert abs(cloud.compute_state_agreement(truth_mismatch) - 0.75) < 1e-9
    assert cloud.compute_state_agreement({}) == 1.0 # no ground truth -> trivially agree
    occ = cloud.get_occupancy()
    assert occ["occupied"] == 2 and occ["total"] == 4


def _publish_once(backend, clock: SimClock, until: float = 30.0):
    delivered: list[bytes] = []
    backend._subscriber = lambda b, p: delivered.append(p)
    batch = BatchUpdate(edge_id="edge_01", events=[ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1)])
    backend.publish(batch, b"x" * 40)
    _advance(clock, until)
    return delivered

def test_protocol_lossless_delivers_on_first_pass():
    for make in (
        lambda c: SimulatedMQTTBackend(MQTTConfig(qos=0), c, None, 0.0, 0, 0.03, 0.0),
        lambda c: SimulatedMQTTBackend(MQTTConfig(qos=1), c, None, 0.0, 0, 0.03, 0.0),
        lambda c: SimulatedMQTTBackend(MQTTConfig(qos=2), c, None, 0.0, 0, 0.03, 0.0),
        lambda c: SimulatedAMQPBackend(AMQPConfig(ack_mode="manual", durable=True), c, None, 0.0, 0, 0.03, 0.0),
        lambda c: SimulatedCoAPBackend(CoAPConfig(mode="CON"), c, None, 0.0, 0, 0.03, 0.0),
        lambda c: SimulatedCoAPBackend(CoAPConfig(mode="NON"), c, None, 0.0, 0, 0.03, 0.0)
    ):
        clock = SimClock()
        backend = make(clock)
        delivered = _publish_once(backend, clock, until=5.0)
        assert backend.frames_delivered == 1
        assert backend.first_pass_delivered == 1
        assert backend.frames_dropped == 0
        assert backend.retransmitted == 0
        assert len(delivered) == 1

def test_mqtt_qos0_total_loss_drops_without_retransmit():
    clock = SimClock()
    backend = SimulatedMQTTBackend(MQTTConfig(qos=0), clock, None, loss_rate=1.0, seed=0, ack_one_way_delay_s=0.03, ack_jitter_s=0.0)
    _publish_once(backend, clock, until=5.0)
    assert backend.frames_delivered == 0
    assert backend.frames_dropped == 1
    assert backend.retransmitted == 0 # QoS0 = fire and forget

def test_mqtt_qos1_total_loss_retransmits_then_drops():
    clock = SimClock()
    backend = SimulatedMQTTBackend(MQTTConfig(qos=1), clock, None, loss_rate=1.0, seed=0, ack_one_way_delay_s=0.03, ack_jitter_s=0.0)
    _publish_once(backend, clock, until=60.0)
    assert backend.frames_delivered == 0
    assert backend.frames_dropped == 1
    assert backend.retransmitted > 0 # confirmable QoS retries before giving up

def test_amqp_auto_vs_manual_recovery_under_total_loss():
    clock_a = SimClock()
    auto = SimulatedAMQPBackend(AMQPConfig(ack_mode="auto", durable=False), clock_a, None, loss_rate=1.0, seed=0, ack_one_way_delay_s=0.03, ack_jitter_s=0.0)
    _publish_once(auto, clock_a, until=30.0)
    assert auto.frames_dropped == 1
    assert auto.retransmitted == 0 # auto-ack cannot recover a broker-side loss

    clock_m = SimClock()
    manual = SimulatedAMQPBackend(AMQPConfig(ack_mode="manual", durable=True), clock_m, None, loss_rate=1.0, seed=0, ack_one_way_delay_s=0.03, ack_jitter_s=0.0)
    _publish_once(manual, clock_m, until=30.0)
    assert manual.frames_dropped == 1
    assert manual.retransmitted > 0 # manual ack retries

def test_coap_con_retransmits_non_does_not_under_total_loss():
    clock_n = SimClock()
    non = SimulatedCoAPBackend(CoAPConfig(mode="NON"), clock_n, None, loss_rate=1.0, seed=0, ack_one_way_delay_s=0.03, ack_jitter_s=0.0)
    _publish_once(non, clock_n, until=120.0)
    assert non.frames_delivered == 0
    assert non.retransmitted == 0 # NON = fire and forget

    clock_c = SimClock()
    con = SimulatedCoAPBackend(CoAPConfig(mode="CON"), clock_c, None, loss_rate=1.0, seed=0, ack_one_way_delay_s=0.03, ack_jitter_s=0.0)
    _publish_once(con, clock_c, until=120.0)
    assert con.frames_delivered == 0
    assert con.retransmitted > 0 # CON retransmits with backoff

def test_mqtt_byte_overhead_increases_with_qos():
    def bytes_for(qos: int) -> int:
        clock = SimClock()
        b = SimulatedMQTTBackend(MQTTConfig(qos=qos), clock, lambda *_: None, 0.0, 0, 0.03, 0.0)
        b.publish(BatchUpdate(edge_id="edge_01", events=[ParkingEvent("s0", 0, SpotState.OCCUPIED, sequence=1)]), b"x" * 40)
        _advance(clock, 5.0)
        return b.bytes_sent
    b0, b1, b2 = bytes_for(0), bytes_for(1), bytes_for(2)
    assert b0 < b1 < b2


def _short_cfg(name: str, seed: int, **kw) -> ScenarioConfig:
    base = dict(name=name, protocol="mqtt", architecture="edge_filtered", traffic_level="medium", num_spots=30, loss_rate=0.05, backhaul_loss_rate=0.05, sim_duration_s=600.0, seed=seed, anomaly_detection=False)
    base.update(kw)
    return make_scenario(**base)


def _comparable(metrics) -> dict:
    d = metrics.to_dict()
    d.pop("run_id", None)
    return d

def test_same_seed_is_deterministic():
    a = run_scenario_sync(_short_cfg("det_a", seed=SEED))
    b = run_scenario_sync(_short_cfg("det_b", seed=SEED))
    da, db = _comparable(a), _comparable(b)
    da.pop("scenario_name"); db.pop("scenario_name")
    assert da == db # identical seed -> bit-identical metrics

def test_different_seed_changes_results():
    a = _comparable(run_scenario_sync(_short_cfg("seed_x", seed=SEED)))
    b = _comparable(run_scenario_sync(_short_cfg("seed_x", seed=SEED + 1)))
    a.pop("seed"); b.pop("seed")
    assert a != b # different seed -> different realisation


def test_edge_aggregation_reduces_cloud_messages_vs_cloud_only():
    common = dict(traffic_level="medium", num_spots=50, loss_rate=0.0, sim_duration_s=900.0, seed=SEED, heartbeat_interval_s=60.0, anomaly_detection=False)
    cloud_only = run_scenario_sync(make_scenario(name="rq_cloud", architecture="cloud_only", **common))
    aggregated = run_scenario_sync(make_scenario(name="rq_agg", architecture="edge_aggregated", aggregation_interval=5.0, **common))
    # edge filtering + aggregation cuts traffic reaching the cloud
    assert aggregated.cloud_msgs_received_total < cloud_only.cloud_msgs_received_total
    assert aggregated.message_reduction_ratio is not None
    assert aggregated.message_reduction_ratio > 0.0
    assert cloud_only.message_reduction_ratio is None
    # aggregation collapses many forwarded events into fewer cloud frames
    assert aggregated.aggregation_ratio is not None
    assert 0.0 < aggregated.aggregation_ratio <= 1.0
    assert aggregated.events_per_cloud_message is not None
    assert aggregated.events_per_cloud_message >= 1.0


def _ev(spot=0, state=SpotState.OCCUPIED, ts=0.0, seq=1, **kw) -> ParkingEvent:
    return ParkingEvent(sensor_id=f"s{spot}", spot_id=spot, state=state, timestamp=ts, sequence=seq, **kw)


def test_fault_none_passes_event_through_unchanged():
    fi = FaultInjector(rng=random.Random(0))
    out = fi.apply(_ev())
    assert out == [out[0]] and len(out) == 1
    assert out[0].state == SpotState.OCCUPIED
    assert fi.injected_count == 0


def test_fault_silent_drops_event():
    fi = FaultInjector(rng=random.Random(0))
    fi.set_fault(0, FaultSpec(fault_type=FaultType.SILENT))
    assert fi.apply(_ev()) == []
    assert fi.injected_count == 1


def test_fault_stuck_at_forces_state_and_counts_only_on_change():
    fi = FaultInjector(rng=random.Random(0))
    fi.set_fault(0, FaultSpec(fault_type=FaultType.STUCK_AT, stuck_state="occupied"))
    # incoming FREE is forced to OCCUPIED -> counted
    out = fi.apply(_ev(state=SpotState.FREE))
    assert len(out) == 1 and out[0].state == SpotState.OCCUPIED
    assert fi.injected_count == 1
    # incoming already-OCCUPIED matches the stuck state -> no new injection counted
    out2 = fi.apply(_ev(state=SpotState.OCCUPIED, seq=2))
    assert out2[0].state == SpotState.OCCUPIED
    assert fi.injected_count == 1


def test_fault_flapping_emits_original_plus_flipped():
    fi = FaultInjector(rng=random.Random(0))
    fi.set_fault(0, FaultSpec(fault_type=FaultType.FLAPPING))
    out = fi.apply(_ev(state=SpotState.OCCUPIED))
    assert len(out) == 2
    assert out[0].state == SpotState.OCCUPIED
    assert out[1].state == SpotState.FREE  # the spurious flip
    assert fi.injected_count == 1


def test_fault_replay_repeats_previous_event():
    fi = FaultInjector(rng=random.Random(0))
    fi.set_fault(0, FaultSpec(fault_type=FaultType.REPLAY, replay_count=3))
    first = fi.apply(_ev(state=SpotState.OCCUPIED, seq=1))
    assert len(first) == 1  # nothing to replay yet
    second = fi.apply(_ev(state=SpotState.FREE, seq=2))
    assert len(second) == 4  # the real event + 3 replays of the previous one
    assert all(e.state == SpotState.OCCUPIED for e in second[1:])
    assert fi.injected_count == 3


def test_fault_flooding_emits_flood_count_total():
    fi = FaultInjector(rng=random.Random(0))
    fi.set_fault(0, FaultSpec(fault_type=FaultType.FLOODING, flood_count=10))
    out = fi.apply(_ev())
    assert len(out) == 10
    assert all(e.state == SpotState.OCCUPIED for e in out)
    assert fi.injected_count == 9  # the original is not an injection


def test_fault_clear_restores_passthrough():
    fi = FaultInjector(rng=random.Random(0))
    fi.set_fault(0, FaultSpec(fault_type=FaultType.SILENT))
    assert fi.active_faults() == {0: "silent"}
    fi.clear_fault(0)
    assert fi.active_faults() == {}
    assert len(fi.apply(_ev())) == 1


def test_encode_event_msgpack_roundtrip():
    ev = _ev(spot=3, state=SpotState.FREE, ts=12.5, seq=7, is_heartbeat_event=True)
    d = msgpack.unpackb(encode_event(ev), raw=False)
    assert d["spot_id"] == 3 and d["sequence"] == 7
    assert d["state"] == "free" and d["timestamp"] == 12.5
    assert d["is_heartbeat_event"] is True


def test_encode_batch_grows_with_event_count():
    one = encode_batch(BatchUpdate(edge_id="e", events=[_ev(seq=1)]))
    many = encode_batch(BatchUpdate(edge_id="e", events=[_ev(spot=i, seq=i) for i in range(20)]))
    assert len(many) > len(one)
    assert len(msgpack.unpackb(many, raw=False)["events"]) == 20


def test_lora_airtime_positive_and_monotonic_in_payload():
    sizes = [1, 10, 20, 50, 100, 200]
    times = [compute_lora_airtime_s(n) for n in sizes]
    assert all(t > 0 for t in times)
    assert times == sorted(times)
    assert times[0] < times[-1]


def test_lora_airtime_increases_with_spreading_factor():
    # higher SF = slower data rate = longer airtime for the same payload
    assert compute_lora_airtime_s(40, sf=7) < compute_lora_airtime_s(40, sf=10) < compute_lora_airtime_s(40, sf=12)


def test_lora_airtime_decreases_with_bandwidth():
    assert compute_lora_airtime_s(40, bw=125_000) > compute_lora_airtime_s(40, bw=250_000)


def test_scenario_to_save_dict_from_dict_roundtrip():
    cfg = make_scenario(
        name="rt", protocol="coap", architecture="edge_aggregated", traffic_level="peak",
        num_spots=120, loss_rate=0.07, backhaul_loss_rate=0.04, coap_mode="NON",
        aggregation_interval=2.5, mqtt_qos=2, seed=99
    )
    back = ScenarioConfig.from_dict(cfg.to_save_dict())
    assert back.name == "rt"
    assert back.protocol == "coap"
    assert back.architecture == "edge_aggregated"
    assert back.traffic_level == "peak"
    assert back.num_spots == 120
    assert back.random_seed == 99
    assert back.coap.mode == "NON"
    assert abs(back.link.packet_loss_rate - 0.07) < 1e-9
    assert abs(back.edge.aggregation_interval_s - 2.5) < 1e-9


def test_make_scenario_arrival_rate_scales_with_spots():
    small = make_scenario(name="s", traffic_level="medium", num_spots=50)
    large = make_scenario(name="l", traffic_level="medium", num_spots=200)
    # arrival rate scales linearly with the number of spots (n/50)
    assert abs(large.arrival_rate - 4 * small.arrival_rate) < 1e-9


def test_save_and_load_custom_scenarios_roundtrip(tmp_path, monkeypatch):
    target = tmp_path / "custom_scenarios.json"
    monkeypatch.setattr(config_mod, "_CUSTOM_SCENARIOS_FILE", target)
    scns = [
        make_scenario(name="c1", protocol="mqtt", num_spots=10, seed=1),
        make_scenario(name="c2", protocol="amqp", architecture="cloud_only", num_spots=20, seed=2),
    ]
    save_custom_scenarios(scns)
    assert target.exists()
    loaded = load_custom_scenarios()
    assert {s.name for s in loaded} == {"c1", "c2"}
    assert {s.protocol for s in loaded} == {"mqtt", "amqp"}


def test_load_custom_scenarios_missing_file_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(config_mod, "_CUSTOM_SCENARIOS_FILE", tmp_path / "does_not_exist.json")
    assert load_custom_scenarios() == []


def _traffic(seed=SEED, **cfg_kw):
    cfg = TrafficConfig(num_spots=cfg_kw.pop("num_spots", 20), random_seed=seed, **cfg_kw)
    clock = SimClock()
    events: list[ParkingEvent] = []
    tm = TrafficModel(cfg, arrival_rate=0.05, clock=clock, event_cb=events.append, epoch=0.0)
    return tm, clock, events


def test_dwell_samples_within_bounds():
    tm, _, _ = _traffic(use_dwell_mixture=True)
    samples = [tm._sample_dwell() for _ in range(2000)]
    assert all(TrafficModel.MIN_DWELL_S <= s <= TrafficModel.MAX_DWELL_S for s in samples)
    # mixture should yield a spread, not a constant
    assert len(set(round(s) for s in samples)) > 50


def test_tod_factor_flat_when_disabled_varies_when_enabled():
    flat, _, _ = _traffic(use_time_of_day=False)
    assert all(flat._tod_factor(h * 3600.0) == 1.0 for h in range(24))
    tod, _, _ = _traffic(use_time_of_day=True, start_hour=0.0)
    factors = {round(tod._tod_factor(h * 3600.0), 4) for h in range(24)}
    assert len(factors) > 1 # genuinely time-varying
    assert all(f >= 0.001 for f in factors)


def test_schedule_run_emits_initial_snapshots_for_occupied_spots():
    tm, clock, events = _traffic(num_spots=30, initial_occupancy=1.0, heartbeat_interval_s=0.0)
    tm.schedule_run(duration_s=1.0)
    initial = [e for e in events if e.is_initial]
    assert len(initial) == 30 # every spot starts occupied -> one snapshot each
    assert all(e.state == SpotState.OCCUPIED for e in initial)


def test_traffic_model_is_deterministic_for_same_seed():
    def run(seed):
        cfg = TrafficConfig(num_spots=25, random_seed=seed, initial_occupancy=0.5)
        clock = SimClock()
        out: list[tuple] = []
        tm = TrafficModel(cfg, 0.05, clock, lambda e: out.append((e.spot_id, e.state.value, round(e.timestamp, 3))), epoch=0.0)
        tm.schedule_run(600.0)
        clock.env.run(until=600.0)
        return out
    assert run(SEED) == run(SEED)
    assert run(SEED) != run(SEED + 1)


def test_traffic_arrivals_and_departures_are_generated():
    cfg = TrafficConfig(num_spots=40, random_seed=SEED, initial_occupancy=0.3, heartbeat_interval_s=0.0)
    clock = SimClock()
    events: list[ParkingEvent] = []
    tm = TrafficModel(cfg, arrival_rate=0.5, clock=clock, event_cb=events.append, epoch=0.0)
    tm.schedule_run(1800.0)
    clock.env.run(until=1800.0)
    arrivals = [e for e in events if not e.is_initial and e.state == SpotState.OCCUPIED]
    departures = [e for e in events if not e.is_initial and e.state == SpotState.FREE]
    assert len(arrivals) > 0 and len(departures) > 0
    assert all(0.0 <= e.timestamp <= 1800.0 for e in events)


def test_loss_provider_none_for_cloud_only_or_no_peak():
    assert _make_loss_provider(make_scenario(name="co", architecture="cloud_only")) is None
    edge_no_peak = make_scenario(name="np", architecture="edge_filtered", backhaul_loss_rate=0.05)
    assert _make_loss_provider(edge_no_peak) is None


def test_loss_provider_peaks_at_congestion_hours():
    floor, peak = 0.05, 0.5
    cfg = make_scenario(name="lp", architecture="edge_filtered", backhaul_loss_rate=floor,
                        backhaul_loss_peak_rate=peak, start_hour=8.0)
    p = _make_loss_provider(cfg)
    assert p is not None
    # start_hour=8 is a congestion peak -> loss near the peak at t=0
    assert p(0.0) == pytest.approx(peak, abs=1e-6)
    # mid-afternoon lull (hour 13) -> loss near the floor
    off = p(5 * 3600.0)
    assert floor <= off < 0.1
    # always bounded between floor and peak
    assert all(floor - 1e-9 <= p(t) <= peak + 1e-9 for t in range(0, 24 * 3600, 1800))


def _assert_metrics_invariants(m, arch: str) -> None:
    assert m.events_generated > 0
    assert m.events_generated == (m.valid_state_changes + m.heartbeats_generated + m.initial_snapshots_generated + m.duplicate_sends_generated)

    assert m.sensor_to_edge_msgs >= 0
    assert 0 <= m.sensor_link_dropped <= m.sensor_to_edge_msgs
    assert m.bytes_s2e_received <= m.sensor_to_edge_bytes
    if m.sensor_to_edge_delivery_ratio is not None:
        assert 0.0 <= m.sensor_to_edge_delivery_ratio <= 1.0

    lat = [m.latency_min_ms, m.latency_p50_ms, m.latency_p95_ms, m.latency_p99_ms, m.latency_max_ms]
    if any(v is not None for v in lat):
        assert all(v is not None for v in lat), "latency stats should be all-set or all-None"
        assert m.latency_min_ms <= m.latency_p50_ms <= m.latency_p95_ms <= m.latency_p99_ms <= m.latency_max_ms
        assert m.latency_min_ms <= m.latency_mean_ms <= m.latency_max_ms

    for r in (m.e2e_unique_delivery_ratio, m.cloud_reflection_ratio, m.physical_delivery_ratio, m.backhaul_delivery_ratio):
        assert r is None or 0.0 <= r <= 1.0

    assert m.cloud_events_post_dedup == m.cloud_msgs_received_total - m.duplicate_events_at_cloud
    assert m.cloud_state_changes_reflected >= 0

    if arch == "cloud_only":
        assert m.aggregation_ratio is None
        assert m.message_reduction_ratio is None
    else:
        assert m.edge_to_cloud_msgs >= 0
        if m.aggregation_ratio is not None:
            assert 0.0 < m.aggregation_ratio <= 1.0
        if m.message_reduction_ratio is not None:
            assert 0.0 <= m.message_reduction_ratio <= 1.0
        if m.events_per_cloud_message is not None:
            assert m.events_per_cloud_message >= 1.0

    json.dumps(m.to_dict())


@pytest.mark.parametrize("arch", ARCH)
@pytest.mark.parametrize("proto", PROTOCOLS)
def test_full_run_completes_with_valid_metrics(arch, proto):
    cfg = make_scenario(
        name=f"smoke_{arch}_{proto}", protocol=proto, architecture=arch,
        traffic_level="medium", num_spots=25, loss_rate=0.05, backhaul_loss_rate=0.05,
        sim_duration_s=300.0, seed=SEED, heartbeat_interval_s=120.0
    )
    m = run_scenario_sync(cfg)
    assert m.scenario_name == f"smoke_{arch}_{proto}"
    assert m.protocol == proto and m.architecture == arch
    _assert_metrics_invariants(m, arch)


def test_edge_architectures_reduce_cloud_load_vs_cloud_only():
    common = dict(protocol="mqtt", traffic_level="medium", num_spots=40, loss_rate=0.0,
                  backhaul_loss_rate=0.0, sim_duration_s=600.0, seed=SEED, heartbeat_interval_s=60.0,
                  anomaly_detection=False)
    cloud = run_scenario_sync(make_scenario(name="cl", architecture="cloud_only", **common))
    filt = run_scenario_sync(make_scenario(name="fl", architecture="edge_filtered", **common))
    agg = run_scenario_sync(make_scenario(name="ag", architecture="edge_aggregated", aggregation_interval=5.0, **common))
    # both edge variants must put no more load on the cloud than cloud-only
    assert filt.cloud_msgs_received_total <= cloud.cloud_msgs_received_total
    assert agg.cloud_msgs_received_total <= cloud.cloud_msgs_received_total


def test_lossless_run_delivers_everything_end_to_end():
    cfg = make_scenario(
        name="perfect", protocol="mqtt", architecture="edge_filtered", traffic_level="medium",
        num_spots=30, loss_rate=0.0, backhaul_loss_rate=0.0, sim_duration_s=600.0, seed=SEED,
        heartbeat_interval_s=1_000_000.0, anomaly_detection=False
    )
    m = run_scenario_sync(cfg)
    # with zero loss every offered frame is delivered on both hops
    assert m.sensor_to_edge_delivery_ratio == pytest.approx(1.0, abs=1e-9)
    assert m.backhaul_delivery_ratio == pytest.approx(1.0, abs=1e-9)
    # every generated state change is reflected at the cloud
    assert m.e2e_unique_delivery_ratio == pytest.approx(1.0, abs=1e-9)
    assert m.duplicate_deliveries == 0


def test_latency_tail_is_not_capped():
    """A state change delivered with a very large latency (>> the old 93s cap) must still be recorded, so p99/max reflect the true tail."""
    cfg = make_scenario(name="taillat", num_spots=2, loss_rate=0.0, sim_duration_s=1.0, seed=SEED)
    clock = SimClock()
    cloud = CloudBackend(cfg, clock, epoch=0.0)
    clock.env.run(until=200.0)  # 200 s of virtual time elapse before arrival
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1)]), b"x")
    samples = cloud.get_all_latency_samples()
    assert len(samples) == 1
    assert samples[0] == pytest.approx(200_000.0, abs=1.0)


def test_save_results_writes_loadable_json(tmp_path):
    cfg = make_scenario(name="persist", protocol="mqtt", architecture="edge_filtered", traffic_level="low", num_spots=10, loss_rate=0.0, sim_duration_s=120.0, seed=SEED)
    m = run_scenario_sync(cfg)
    path = save_results(m, str(tmp_path))
    data = json.loads(open(path).read())
    assert data["scenario_name"] == "persist"
    assert data["seed"] == SEED
    assert "latency_mean_ms" in data


def test_predefined_scenarios_are_wellformed():
    assert len(PREDEFINED_SCENARIOS) > 0
    names = [s.name for s in PREDEFINED_SCENARIOS]
    assert len(names) == len(set(names)) 
    for s in PREDEFINED_SCENARIOS:
        assert s.protocol in PROTOCOLS
        assert s.architecture in ARCH
        assert s.num_spots > 0
        assert s.arrival_rate > 0
        assert s.sim_duration_s > 0