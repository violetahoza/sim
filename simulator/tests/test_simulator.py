from __future__ import annotations

from simulator.config import TrafficConfig, make_scenario
from simulator.des.engine import SimClock
from simulator.models import ParkingEvent, SpotState
from simulator.traffic.traffic_model import TrafficModel
from simulator.link.link_emulator import LinkEmulator
from simulator.edge.edge_node import EdgeNode


def test_traffic_model_event_count():
    clock = SimClock()
    cfg = TrafficConfig(
        num_spots=100,
        initial_occupancy=0.0,
        time_scale=1.0,
        random_seed=0,
        parking_duration_cv=1.0,
        sim_duration_s=3600.0,
        mean_parking_duration_s=1800.0
    )
    epoch = 0.0
    events: list[ParkingEvent] = []

    # arrival_rate = 0.1 events/s, duration = 3600 s -> expected ~360 arrivals
    traffic = TrafficModel(cfg, arrival_rate=0.1, clock=clock, event_cb=events.append, epoch=epoch)
    traffic.schedule_run(3600.0)
    clock.run_until(3600.0)

    arrivals = [e for e in events if e.state == SpotState.OCCUPIED]
    assert 200 <= len(arrivals) <= 520, (
        f"Expected ~360 arrivals at 0.1 ev/s × 3600 s, got {len(arrivals)}"
    )


def test_edge_filter_drops_same_state():
    clock = SimClock()
    cfg = make_scenario("t", "t", "mqtt", "edge_filtered", "medium", num_spots=10)
    import time as _time
    epoch = _time.time()

    forwarded: list[tuple] = []

    edge = EdgeNode(cfg, clock, lambda b, r: forwarded.append((b, r)), epoch)

    t = epoch
    # First OCCUPIED - new state, should forward
    edge.receive(ParkingEvent("sensor_0001", 1, SpotState.OCCUPIED, timestamp=t), b"")
    t += 1.0
    # Second OCCUPIED - same state, should be filtered
    edge.receive(ParkingEvent("sensor_0001", 1, SpotState.OCCUPIED, timestamp=t), b"")
    t += 1.0
    # FREE - state change, should forward
    edge.receive(ParkingEvent("sensor_0001", 1, SpotState.FREE, timestamp=t), b"")

    assert edge.filtered_count == 1, f"Expected 1 filtered event, got {edge.filtered_count}"
    assert len(forwarded) == 2, f"Expected 2 forwarded events, got {len(forwarded)}"


def test_link_drop_rate():
    clock = SimClock()
    target_loss = 0.30
    cfg = make_scenario("t", "t", "mqtt", "cloud_only", "medium", loss_rate=target_loss, num_spots=10)
    import random as _random
    delivered: list[ParkingEvent] = []

    link = LinkEmulator(cfg.link, clock, forward_cb=lambda e, b: delivered.append(e), rng=_random.Random(99))

    n = 300
    for i in range(n):
        ev = ParkingEvent(f"sensor_{i:04d}", i % 10, SpotState.OCCUPIED, timestamp=float(i))
        link.transmit(ev)

    clock.run_until(1e9)

    actual_drop = link.stats.dropped / link.stats.sent
    assert abs(actual_drop - target_loss) < 0.10, (f"Expected drop rate ~{target_loss:.0%}, measured {actual_drop:.2%}")
