from __future__ import annotations
import json
import math

from simulator.config.config import make_scenario, ScenarioConfig, TrafficConfig, LinkConfig, MQTTConfig, AMQPConfig, CoAPConfig, ARRIVAL_RATES
from simulator.des.engine import SimClock
from simulator.cloud.cloud_backend import CloudBackend
from simulator.edge.edge_node import EdgeNode
from simulator.sensors.sensor_emulator import SensorEmulator
from simulator.link.link_emulator import LinkEmulator, TokenBucket, GilbertElliotModel, QueueOverflowModel
from simulator.models.models import ParkingEvent, BatchUpdate, SpotState, ExperimentMetrics, LinkStats
from simulator.protocols.mqtt_client import SimulatedMQTTBackend
from simulator.protocols.amqp_client import SimulatedAMQPBackend
from simulator.protocols.coap_client import SimulatedCoAPBackend
from experiments.runner import run_scenario_sync, _stats
from experiments.aggregate import _summarise, _is_number, aggregate, _t_critical, load_runs, HEADLINE_METRICS

SEED = 20250607


def _edge_scn(name: str, **kw):
    """edge_filtered scenario with NO sensor-link loss and heavy backhaul loss, so protocol-level recovery is the only thing that separates the variants."""
    base = dict(
        name=name,
        protocol="mqtt",
        architecture="edge_filtered",
        traffic_level="peak",
        num_spots=200,
        loss_rate=0.0,  
        backhaul_loss_rate=0.30,  
        sim_duration_s=3600.0,
        seed=SEED,
        heartbeat_interval_s=1_000_000.0, 
        anomaly_detection=False
    )
    base.update(kw)
    return make_scenario(**base)


def test_workload_identity_partition():
    cfg = make_scenario(
        name="t_identity", protocol="mqtt", architecture="edge_filtered", traffic_level="medium", num_spots=100, loss_rate=0.0,
        backhaul_loss_rate=0.05, sim_duration_s=3600.0, seed=SEED, anomaly_detection=False
    )
    m = run_scenario_sync(cfg)
    assert m.events_generated_total > 0
    assert m.events_generated_total == (
        m.state_changes_generated_total
        + m.heartbeats_generated_total
        + m.initial_snapshots_generated_total
        + m.duplicate_sends_generated_total
    )
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
    assert q2.duplicate_deliveries == 0
    assert q0.duplicate_deliveries == 0


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
    cfg = make_scenario(
        name="t_empty", protocol="mqtt", architecture="cloud_only", traffic_level="low", num_spots=1, loss_rate=0.0,
        sim_duration_s=0.01, seed=SEED, heartbeat_interval_s=1_000_000.0, anomaly_detection=False
    )
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
    clock = SimClock()  # stays at now=0 for this unit test
    bucket = TokenBucket(rate=2.0)  # 1 token every 0.5 s
    assert bucket.consume(clock) == 0.0 # first token available immediately
    assert abs(bucket.consume(clock) - 0.5) < 1e-9  # next must wait one interval

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
    assert sensors.total_generated == (
        sensors.state_changes_generated + sensors.heartbeats_generated
        + sensors.initial_snapshots_generated + sensors.duplicate_sends_generated
    )
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
    # first heartbeat (ts>0 so it isn't treated as "never forwarded") is forwarded
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
    for key in ("received", "filtered", "forwarded_events", "heartbeats_suppressed", "quarantine_suppressed", "anomalies", "active_anomalies",
                "mode_switches", "link_stats", "quarantined_count", "event_log"):
        assert key in s


def _cloud(num_spots: int = 4) -> tuple[CloudBackend, SimClock]:
    cfg = make_scenario(name="t_cloud", num_spots=num_spots, loss_rate=0.0, sim_duration_s=1.0, seed=SEED)
    clock = SimClock()
    return CloudBackend(cfg, clock, epoch=0.0), clock

def test_cloud_latency_only_state_changes_in_headline_bucket():
    cloud, clock = _cloud()
    _advance(clock, 1.5)  # so arrival = epoch + now = 1.5
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1)]), b"x")
    # heartbeat must NOT pollute the headline latency series
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[ParkingEvent("s1", 1, SpotState.FREE, timestamp=0.0, sequence=1, is_heartbeat_event=True)]), b"x")

    samples = cloud.get_all_latency_samples()
    assert len(samples) == 1
    assert abs(samples[0] - 1500.0) < 1e-6 # (1.5 - 0.0) s -> ms
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
    # Same payload + topic; only the QoS control-packet overhead differs.
    def bytes_for(qos: int) -> int:
        clock = SimClock()
        b = SimulatedMQTTBackend(MQTTConfig(qos=qos), clock, lambda *_: None, 0.0, 0, 0.03, 0.0)
        b.publish(BatchUpdate(edge_id="edge_01", events=[ParkingEvent("s0", 0, SpotState.OCCUPIED, sequence=1)]), b"x" * 40)
        _advance(clock, 5.0)
        return b.bytes_sent
    b0, b1, b2 = bytes_for(0), bytes_for(1), bytes_for(2)
    assert b0 < b1 < b2


def _short_cfg(name: str, seed: int, **kw) -> ScenarioConfig:
    base = dict(name=name, protocol="mqtt", architecture="edge_filtered", traffic_level="medium", num_spots=30, loss_rate=0.05,
                backhaul_loss_rate=0.05, sim_duration_s=600.0, seed=seed, anomaly_detection=False)
    base.update(kw)
    return make_scenario(**base)


def _comparable(metrics) -> dict:
    d = metrics.to_dict()
    d.pop("run_id", None) # run_id embeds a timestamp/uuid and is expected to differ
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