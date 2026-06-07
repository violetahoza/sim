from __future__ import annotations
import json
import math

from simulator.config.config import make_scenario
from simulator.des.engine import SimClock
from simulator.cloud.cloud_backend import CloudBackend
from simulator.models.models import ParkingEvent, BatchUpdate, SpotState, ExperimentMetrics
from experiments.runner import run_scenario_sync, _stats
from experiments.aggregate import _summarise, _is_number, aggregate

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
        name="t_identity", protocol="mqtt", architecture="edge_filtered",
        traffic_level="medium", num_spots=100, loss_rate=0.0,
        backhaul_loss_rate=0.05, sim_duration_s=3600.0, seed=SEED,
        anomaly_detection=False
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
    assert (q0.e2e_unique_delivery_ratio
            <= q1.e2e_unique_delivery_ratio
            <= q2.e2e_unique_delivery_ratio)
    assert q0.e2e_unique_delivery_ratio < q1.e2e_unique_delivery_ratio


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
    cfg = make_scenario(name="t_dedup", num_spots=4, loss_rate=0.0,
                        sim_duration_s=1.0, seed=SEED)
    clock = SimClock()
    cloud = CloudBackend(cfg, clock, epoch=0.0)

    ev = ParkingEvent(sensor_id="s0", spot_id=0, state=SpotState.OCCUPIED,
                      timestamp=0.0, sequence=1)
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[ev]), b"x")
    assert cloud.transitions_received == 1  
    assert cloud.duplicate_events_at_cloud == 0

    dup = ParkingEvent(sensor_id="s0", spot_id=0, state=SpotState.OCCUPIED,
                       timestamp=0.0, sequence=1)  
    cloud.receive_batch(BatchUpdate(edge_id="t", events=[dup]), b"x")
    assert cloud.transitions_received == 1   
    assert cloud.duplicate_events_at_cloud == 1


def test_stats_empty_returns_none():
    assert _stats([]) == (None, None, None, None, None, None)
    mean, p50, p95, p99, mn, mx = _stats([10.0, 20.0])
    assert mean == 15.0


def test_to_dict_emits_null_not_zero():
    m = ExperimentMetrics(
        scenario_name="t", protocol="mqtt", architecture="cloud_only",
        traffic_level="low", num_spots=1, sim_duration_s=1.0,
        latency_mean_ms=None, latency_p99_ms=None,
        e2e_unique_delivery_ratio=None,
        aggregation_ratio=None, message_reduction_ratio=None,
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
        name="t_empty", protocol="mqtt", architecture="cloud_only",
        traffic_level="low", num_spots=1, loss_rate=0.0,
        sim_duration_s=0.01, seed=SEED, heartbeat_interval_s=1_000_000.0,
        anomaly_detection=False
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
    base = {
        "scenario_name": "X", "protocol": "mqtt", "architecture": "edge_filtered",
        "traffic_level": "peak", "e2e_unique_delivery_ratio": 0.9
    }
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