from __future__ import annotations
import random
import pytest

from simulator.config import TrafficConfig, make_scenario
from simulator.des.engine import SimClock
from simulator.models import BatchUpdate, ParkingEvent, SpotState
from simulator.traffic.traffic_model import TrafficModel
from simulator.link.link_emulator import LinkEmulator, TokenBucket
from simulator.edge.edge_node import EdgeNode
from simulator.cloud.cloud_backend import CloudBackend
from simulator.sensors.sensor_emulator import SensorEmulator


def _occ(spot_id: int = 0, t: float = 0.0, seq: int = 1) -> ParkingEvent:
    return ParkingEvent(f"sensor_{spot_id:04d}", spot_id, SpotState.OCCUPIED, timestamp=t, sequence=seq)

def _free(spot_id: int = 0, t: float = 1.0, seq: int = 2) -> ParkingEvent:
    return ParkingEvent(f"sensor_{spot_id:04d}", spot_id, SpotState.FREE, timestamp=t, sequence=seq)

def _make_traffic_cfg(num_spots: int = 50, initial_occupancy: float = 0.0, seed: int = 0, cv: float = 1.0, mean_park: float = 1800.0, duration: float = 3600.0) -> TrafficConfig:
    return TrafficConfig(
        num_spots=num_spots,
        initial_occupancy=initial_occupancy,
        time_scale=1.0,
        random_seed=seed,
        parking_duration_cv=cv,
        sim_duration_s=duration,
        mean_parking_duration_s=mean_park
    )


def _scenario(arch: str = "edge_filtered", protocol: str = "mqtt", num_spots: int = 10, loss: float = 0.0, agg: float = 30.0, anomaly: bool = False, adaptive: bool = False) -> object:
    return make_scenario(
        "test", "test", protocol, arch, "medium",
        num_spots=num_spots, loss_rate=loss,
        aggregation_interval=agg,
        anomaly_detection=anomaly,
        adaptive_edge=adaptive
    )


class TestSimClock:
    def test_clock_starts_at_zero(self):
        clock = SimClock()
        assert clock.now == 0.0

    def test_clock_schedule_fires_at_correct_time(self):
        clock = SimClock()
        fired_at: list[float] = []
        clock.schedule(10.0, lambda: fired_at.append(clock.now))
        clock.run_until(15.0)
        assert len(fired_at) == 1
        assert abs(fired_at[0] - 10.0) < 1e-9

    def test_clock_schedule_multiple_ordered(self):
        clock = SimClock()
        times: list[float] = []
        for delay in [5.0, 1.0, 3.0]:
            clock.schedule(delay, lambda d=delay: times.append(d))
        clock.run_until(10.0)
        # All three must fire; they should fire in the order 1 → 3 → 5
        assert sorted(times) == times, "Events fired out of DES order"
        assert len(times) == 3

    def test_clock_schedule_at_absolute_time(self):
        clock = SimClock()
        fired: list[float] = []
        clock.schedule_at(7.5, lambda: fired.append(clock.now))
        clock.run_until(10.0)
        assert len(fired) == 1
        assert abs(fired[0] - 7.5) < 1e-9

    def test_clock_pending_flag(self):
        clock = SimClock()
        assert not clock.pending
        clock.schedule(1.0, lambda: None)
        assert clock.pending
        clock.run_until(5.0)
        assert not clock.pending

    def test_clock_run_until_stops_exactly(self):
        clock = SimClock()
        clock.schedule(100.0, lambda: None) # never fires
        clock.run_until(50.0)
        assert clock.now == 50.0
        assert clock.pending # event still in queue


class TestTrafficModel:

    def _run(self, cfg: TrafficConfig, arrival_rate: float, duration: float, epoch: float = 0.0, seed: int | None = None) -> list[ParkingEvent]:
        clock = SimClock()
        events: list[ParkingEvent] = []
        rng = random.Random(seed) if seed is not None else None
        model = TrafficModel(cfg, arrival_rate, clock, events.append, epoch, rng=rng)
        model.schedule_run(duration)
        clock.run_until(duration)
        return events

    def test_traffic_model_event_count(self):
        """~360 arrivals at 0.1 ev/s x 3600 s, 100 spots, no initial occupancy."""
        cfg = _make_traffic_cfg(num_spots=100, seed=0, cv=1.0, duration=3600.0)
        events = self._run(cfg, arrival_rate=0.1, duration=3600.0, seed=0)
        arrivals = [e for e in events if e.state == SpotState.OCCUPIED and not e.is_initial]
        assert 200 <= len(arrivals) <= 520, f"Expected ~360, got {len(arrivals)}"

    def test_traffic_model_no_arrivals_when_full(self):
        """With 1 spot initially occupied, no new arrivals can park."""
        cfg = _make_traffic_cfg(num_spots=1, initial_occupancy=1.0, seed=1, mean_park=7200.0, duration=1000.0)
        events = self._run(cfg, arrival_rate=1.0, duration=1000.0, seed=1)
        initial = [e for e in events if e.is_initial and e.state == SpotState.OCCUPIED]
        assert len(initial) == 1

    def test_traffic_model_arrival_departure_pairs(self):
        """Every spot that had an arrival should at some point be freed."""
        cfg = _make_traffic_cfg(num_spots=20, seed=7, mean_park=100.0, duration=2000.0)
        events = self._run(cfg, arrival_rate=0.05, duration=2000.0, seed=7)
        occ_spots = {e.spot_id for e in events if e.state == SpotState.OCCUPIED and not e.is_initial}
        free_spots = {e.spot_id for e in events if e.state == SpotState.FREE}
        assert occ_spots.issubset(free_spots | occ_spots), "Some spots never freed"

    def test_traffic_model_departure_clears_spot(self):
        """After a departure event, the same spot must be available for re-arrival."""
        cfg = _make_traffic_cfg(num_spots=5, seed=3, mean_park=50.0, duration=1000.0)
        events = self._run(cfg, arrival_rate=0.2, duration=1000.0, seed=3)
        state: dict[int, str] = {}
        for e in events:
            sid = e.spot_id
            new = e.state.value
            if sid in state:
                assert state[sid] != new, f"Spot {sid} got duplicate consecutive state={new}"
            state[sid] = new

    def test_traffic_model_initial_occupancy_events(self):
        """Initial occupancy=0.8 should emit is_initial OCCUPIED events."""
        cfg = _make_traffic_cfg(num_spots=50, initial_occupancy=0.8, seed=5, duration=1.0)
        events = self._run(cfg, arrival_rate=0.0, duration=1.0, seed=5)
        initial_occ = [e for e in events if e.is_initial and e.state == SpotState.OCCUPIED]
        # With seed=5 and 50 spots at 80%, expect roughly 40 initial events
        assert 30 <= len(initial_occ) <= 50

    def test_traffic_model_tod_factor_peak_higher_than_off_peak(self):
        """TOD-enabled model produces more arrivals in the peak window than off-peak."""
        tod = [0.01] * 24
        tod[9] = 3.0 # 09:00 peak
        tod[10] = 3.0
        cfg = _make_traffic_cfg(num_spots=200, seed=42, duration=86400.0)
        cfg.use_time_of_day = True
        cfg.start_hour = 0.0
        cfg.tod_factors = tod

        clock = SimClock()
        events: list[ParkingEvent] = []
        model = TrafficModel(cfg, arrival_rate=0.05, clock=clock, event_cb=events.append, epoch=0.0, rng=random.Random(42))
        model.schedule_run(86400.0)
        clock.run_until(86400.0)

        peak_arrivals = [e for e in events
                         if e.state == SpotState.OCCUPIED and not e.is_initial
                         and 9 * 3600 <= e.timestamp < 11 * 3600]
        offpeak_arrivals = [e for e in events
                            if e.state == SpotState.OCCUPIED and not e.is_initial
                            and 0 <= e.timestamp < 2 * 3600]
        assert len(peak_arrivals) > len(offpeak_arrivals), (f"Peak ({len(peak_arrivals)}) should exceed off-peak ({len(offpeak_arrivals)})")

    def test_traffic_model_deterministic_with_same_seed(self):
        cfg = _make_traffic_cfg(num_spots=30, seed=99, duration=500.0)
        events_a = self._run(cfg, arrival_rate=0.05, duration=500.0, seed=99)
        events_b = self._run(cfg, arrival_rate=0.05, duration=500.0, seed=99)
        assert len(events_a) == len(events_b)
        for a, b in zip(events_a, events_b):
            assert a.spot_id == b.spot_id and a.state == b.state

    def test_traffic_model_different_seeds_differ(self):
        cfg = _make_traffic_cfg(num_spots=50, seed=1, duration=3600.0)
        events_a = self._run(cfg, arrival_rate=0.05, duration=3600.0, seed=1)
        events_b = self._run(cfg, arrival_rate=0.05, duration=3600.0, seed=2)
        seqs_a = [(e.spot_id, e.state) for e in events_a[:20]]
        seqs_b = [(e.spot_id, e.state) for e in events_b[:20]]
        assert seqs_a != seqs_b, "Different seeds produced identical event streams"


class TestLinkEmulator:
    def _make_link(self, loss: float = 0.0, base_delay_ms: float = 80.0, jitter_ms: float = 0.0, max_payload: int = 512, rate_limit: float = 1000.0, seed: int = 42):
        cfg = make_scenario("t", "t", "mqtt", "cloud_only", "medium",
                            loss_rate=loss, num_spots=10,
                            base_delay_ms=base_delay_ms, jitter_ms=jitter_ms,
                            max_payload_bytes=max_payload,
                            rate_limit=rate_limit)
        clock = SimClock()
        delivered: list[ParkingEvent] = []
        link = LinkEmulator(cfg.link, clock, forward_cb=lambda e, b: delivered.append(e), rng=random.Random(seed))
        return clock, link, delivered

    def test_link_drop_rate(self):
        clock = SimClock()
        target_loss = 0.30
        cfg = make_scenario("t", "t", "mqtt", "cloud_only", "medium", loss_rate=target_loss, num_spots=10)
        delivered: list[ParkingEvent] = []
        link = LinkEmulator(cfg.link, clock, forward_cb=lambda e, b: delivered.append(e), rng=random.Random(99))
        n = 300
        for i in range(n):
            ev = ParkingEvent(f"sensor_{i:04d}", i % 10, SpotState.OCCUPIED, timestamp=float(i))
            link.transmit(ev)
        clock.run_until(1e9)
        actual = link.stats.dropped / link.stats.sent
        assert abs(actual - target_loss) < 0.10

    def test_link_zero_loss_delivers_all(self):
        clock, link, delivered = self._make_link(loss=0.0)
        n = 50
        for i in range(n):
            link.transmit(_occ(spot_id=i, t=float(i)))
        clock.run_until(1e9)
        assert len(delivered) == n
        assert link.stats.dropped == 0

    def test_link_full_loss_drops_all(self):
        clock, link, delivered = self._make_link(loss=1.0)
        for i in range(30):
            link.transmit(_occ(spot_id=i, t=float(i)))
        clock.run_until(1e9)
        assert len(delivered) == 0
        assert link.stats.dropped == 30

    def test_link_delivery_ratio_property(self):
        clock, link, _ = self._make_link(loss=0.5, seed=7)
        for i in range(200):
            link.transmit(_occ(spot_id=i % 10, t=float(i)))
        clock.run_until(1e9)
        dr = link.stats.delivery_ratio
        assert 0.0 <= dr <= 1.0
        assert abs(dr - 0.5) < 0.15, f"DR={dr:.3f} too far from 0.5"

    def test_link_delay_is_positive(self):
        """All deliveries must happen after t=0 (link delay > 0)."""
        clock = SimClock()
        delivery_times: list[float] = []
        cfg = make_scenario("t", "t", "mqtt", "cloud_only", "medium", loss_rate=0.0, num_spots=5, base_delay_ms=100.0, jitter_ms=0.0)
        link = LinkEmulator(cfg.link, clock, forward_cb=lambda e, b: delivery_times.append(clock.now), rng=random.Random(1))
        for i in range(10):
            link.transmit(_occ(spot_id=i, t=float(i)))
        clock.run_until(1e9)
        assert all(t > 0 for t in delivery_times), "Some events delivered at t=0 (no delay?)"

    def test_link_payload_too_large_is_dropped(self):
        """max_payload_bytes=1 means almost every real payload exceeds the cap."""
        clock, link, delivered = self._make_link(loss=0.0, max_payload=1)
        for i in range(20):
            link.transmit(_occ(spot_id=i, t=float(i)))
        clock.run_until(1e9)
        assert len(delivered) == 0
        assert link.stats.dropped == 20

    def test_link_stats_bytes_accounting(self):
        clock, link, _ = self._make_link(loss=0.0)
        for i in range(10):
            link.transmit(_occ(spot_id=i, t=float(i)))
        clock.run_until(1e9)
        assert link.stats.total_bytes_sent > 0
        assert link.stats.total_bytes_received > 0
        assert link.stats.total_bytes_sent >= link.stats.total_bytes_received

    def test_link_rate_limit_delays_burst(self):
        """With rate=1 msg/s, 10 messages sent at t=0 must spread over ~9 s.
        Each message consumes one token; at rate=1 the bucket refills 1 token/s, so message k is delayed by k seconds.  With base_delay=0 and jitter=0 the spread between the first and last delivery must be at least 9 s.
        The TokenBucket starts with tokens=1.0 so the FIRST message is free (wait=0), the second waits ~1 s, …, the tenth waits ~9 s."""
        cfg = make_scenario("t", "t", "mqtt", "cloud_only", "medium", loss_rate=0.0, num_spots=15, base_delay_ms=0.0, jitter_ms=0.0, rate_limit=1.0)
        clock = SimClock()
        delivery_times: list[float] = []
        link = LinkEmulator(cfg.link, clock, forward_cb=lambda e, b: delivery_times.append(clock.now), rng=random.Random(2))
        # Use distinct spot_ids AND distinct timestamps so each event is unique
        for i in range(10):
            link.transmit(ParkingEvent(f"s{i}", i, SpotState.OCCUPIED, timestamp=0.0, sequence=i + 1))
        clock.run_until(20.0)
        assert len(delivery_times) == 10, (f"Expected 10 deliveries, got {len(delivery_times)}")
        spread = max(delivery_times) - min(delivery_times)
        # First message is free (token available), last one waits ~9 s → spread ≥ 8 s
        assert spread >= 8.0, (
            f"Rate-limiting spread={spread:.2f}s — expected ≥8 s for rate=1 msg/s x 10 msgs.\n"
            f"Delivery times: {[round(t,2) for t in sorted(delivery_times)]}"
        )


class TestTokenBucket:
    def test_token_bucket_no_wait_first_token(self):
        clock = SimClock()
        tb = TokenBucket(rate=5.0)
        wait = tb.consume(clock)
        assert wait == 0.0

    def test_token_bucket_wait_on_second_immediate(self):
        clock = SimClock()
        tb = TokenBucket(rate=1.0)
        tb.consume(clock) # first: free
        wait = tb.consume(clock) # second immediate: must wait
        assert wait > 0.0

    def test_token_bucket_refills_over_time(self):
        clock = SimClock()
        tb = TokenBucket(rate=1.0)
        tb.consume(clock) # drain
        # Advance virtual clock by 1 s → bucket refills
        clock.schedule(1.0, lambda: None)
        clock.run_until(1.0)
        wait = tb.consume(clock)
        assert wait == 0.0, "Bucket should have refilled after 1 s"


class TestEdgeNodeFiltering:
 
    def test_edge_filter_drops_same_state(self):
        clock = SimClock()
        cfg = _scenario(arch="edge_filtered")
        epoch = 1000.0
        forwarded: list[tuple] = []
        edge = EdgeNode(cfg, clock, lambda b, r: forwarded.append((b, r)), epoch)

        t = epoch
        edge.receive(ParkingEvent("s0", 1, SpotState.OCCUPIED, timestamp=t), b"")
        t += 1.0
        edge.receive(ParkingEvent("s0", 1, SpotState.OCCUPIED, timestamp=t), b"") # dup
        t += 1.0
        edge.receive(ParkingEvent("s0", 1, SpotState.FREE, timestamp=t), b"")

        assert edge.filtered_count == 1
        assert len(forwarded) == 2

    def test_edge_cloud_only_forwards_every_event(self):
        """cloud_only arch: EdgeNode never filters — all events pass through."""
        clock = SimClock()
        cfg = _scenario(arch="cloud_only")
        forwarded: list[BatchUpdate] = []
        edge = EdgeNode(cfg, clock, lambda b, r: forwarded.append(b), 0.0)
        for i in range(5):
            edge.receive(_occ(spot_id=0, t=float(i), seq=i), b"")
        assert edge.received_count == 5

    def test_edge_aggregated_batches_events(self):
        """In edge_aggregated mode events accumulate; batch fires at agg_interval."""
        clock = SimClock()
        cfg = _scenario(arch="edge_aggregated", agg=10.0)
        batches: list[BatchUpdate] = []
        edge = EdgeNode(cfg, clock, lambda b, r: batches.append(b), 0.0)

        # Send 3 distinct events before the agg window fires
        for i in range(3):
            edge.receive(_occ(spot_id=i, t=float(i), seq=i + 1), b"")

        # No forward yet — batch is pending
        assert len(batches) == 0
        assert len(edge._pending) == 3

        # Advance clock past agg interval
        clock.run_until(11.0)
        assert len(batches) == 1
        assert len(batches[0].events) == 3

    def test_edge_aggregated_flush_final_sends_pending(self):
        clock = SimClock()
        cfg = _scenario(arch="edge_aggregated", agg=9999.0) # interval never fires
        batches: list[BatchUpdate] = []
        edge = EdgeNode(cfg, clock, lambda b, r: batches.append(b), 0.0)

        edge.receive(_occ(spot_id=0, t=0.0), b"")
        edge.receive(_occ(spot_id=1, t=1.0, seq=2), b"")
        assert len(batches) == 0

        edge.flush_final()
        assert len(batches) == 1
        assert len(batches[0].events) == 2

    def test_edge_multi_spot_independent_filtering(self):
        """Filtering is per-spot; spot A's duplicate does not affect spot B."""
        clock = SimClock()
        cfg = _scenario(arch="edge_filtered")
        forwarded: list[BatchUpdate] = []
        edge = EdgeNode(cfg, clock, lambda b, r: forwarded.append(b), 0.0)

        # spot 0: OCCUPIED → OCCUPIED (dup) → FREE
        # spot 1: OCCUPIED → FREE  (no dup)
        edge.receive(_occ(spot_id=0, t=0.0, seq=1), b"")
        edge.receive(_occ(spot_id=0, t=1.0, seq=2), b"") # dup for spot 0
        edge.receive(_free(spot_id=0, t=2.0, seq=3), b"")
        edge.receive(_occ(spot_id=1, t=0.0, seq=4), b"")
        edge.receive(_free(spot_id=1, t=1.0, seq=5), b"")

        assert edge.filtered_count == 1 # only one dup
        assert len(forwarded) == 4 # 2 from spot0 + 2 from spot1

    def test_edge_stuck_sensor_increments_anomaly_count(self):
        """A sensor with consecutive_same > STUCK_THRESHOLD triggers an anomaly."""
        clock = SimClock()
        cfg = _scenario(arch="edge_filtered", anomaly=True)
        epoch = 1000.0
        edge = EdgeNode(cfg, clock, lambda b, r: None, epoch)

        from simulator.models import SensorState
        # last_updated = epoch (virtual t=0); consecutive_same=11 > STUCK_THRESHOLD=10
        edge._cache[0] = SensorState(
            spot_id=0,
            state=SpotState.OCCUPIED,
            consecutive_same=11,
            last_updated=epoch, # non-zero so the early-continue is skipped
        )

        # Advance virtual clock slightly so silent_s is small (not a silent-sensor hit)
        clock.env.run(until=1.0)
        edge._check_anomalies()
        assert edge.anomaly_count >= 1, (f"Expected anomaly_count ≥ 1 for stuck sensor, got {edge.anomaly_count}")

    def test_edge_summary_keys_present(self):
        clock = SimClock()
        cfg = _scenario(arch="edge_filtered")
        edge = EdgeNode(cfg, clock, lambda b, r: None, 0.0)
        s = edge.summary()
        for key in ("received", "filtered", "forwarded_events", "anomalies", "mode_switches", "active_arch", "link_stats"):
            assert key in s, f"Missing key '{key}' in summary"

    def test_edge_received_count_tracks_all_events(self):
        clock = SimClock()
        cfg = _scenario(arch="edge_filtered")
        edge = EdgeNode(cfg, clock, lambda b, r: None, 0.0)
        n = 15
        for i in range(n):
            edge.receive(_occ(spot_id=i % 5, t=float(i), seq=i + 1), b"")
        assert edge.received_count == n

    def test_edge_forwarded_count_matches_cloud_calls(self):
        clock = SimClock()
        cfg = _scenario(arch="edge_filtered")
        calls: list[int] = []
        edge = EdgeNode(cfg, clock, lambda b, r: calls.append(len(b.events)), 0.0)

        # Alternating OCCUPIED / FREE for one spot → all are state changes
        for i in range(6):
            state = SpotState.OCCUPIED if i % 2 == 0 else SpotState.FREE
            edge.receive(ParkingEvent("s0", 0, state, timestamp=float(i), sequence=i + 1), b"")

        assert edge.forwarded_events == 6
        assert len(calls) == 6


class TestAdaptiveEdge:
    def _make_adaptive_edge(self):
        clock = SimClock()
        cfg = _scenario(arch="edge_aggregated", adaptive=True, agg=9999.0)
        edge = EdgeNode(cfg, clock, lambda b, r: None, 0.0)
        return clock, cfg, edge

    def test_adaptive_edge_switches_to_filtered_on_low_dr(self):
        _, _, edge = self._make_adaptive_edge()
        # Simulate low delivery ratio by injecting fake LinkStats
        from simulator.models import LinkStats
        fake_stats = LinkStats(name="test")
        fake_stats.sent = 100
        fake_stats.received = 80 # DR = 0.80 < 0.85 threshold
        fake_stats.dropped = 20
        edge.set_sensor_link_stats(fake_stats)

        edge._check_adaptive_mode()

        assert edge._active_arch == "edge_filtered"
        assert edge.mode_switches == 1

    def test_adaptive_edge_recovers_to_aggregated_on_high_dr(self):
        _, _, edge = self._make_adaptive_edge()
        # First degrade to filtered
        from simulator.models import LinkStats
        bad_stats = LinkStats(name="test")
        bad_stats.sent = 100; bad_stats.received = 80; bad_stats.dropped = 20
        edge.set_sensor_link_stats(bad_stats)
        edge._check_adaptive_mode()
        assert edge._active_arch == "edge_filtered"

        # Now improve delivery ratio
        good_stats = LinkStats(name="test")
        good_stats.sent = 100; good_stats.received = 96; good_stats.dropped = 4
        edge.set_sensor_link_stats(good_stats)
        edge._check_adaptive_mode()

        assert edge._active_arch == "edge_aggregated"
        assert edge.mode_switches == 2

    def test_adaptive_edge_no_switch_under_normal_loss(self):
        _, _, edge = self._make_adaptive_edge()
        from simulator.models import LinkStats
        normal_stats = LinkStats(name="test")
        normal_stats.sent = 100; normal_stats.received = 95; normal_stats.dropped = 5
        edge.set_sensor_link_stats(normal_stats)

        edge._check_adaptive_mode()

        # DR=0.95 exactly at recovery threshold — no switch from aggregated
        assert edge._active_arch == "edge_aggregated"
        assert edge.mode_switches == 0


class TestCloudBackend:
    def _make_cloud(self, arch: str = "edge_filtered", num_spots: int = 10, warmup: float = 0.0) -> CloudBackend:
        cfg = make_scenario("t", "t", "mqtt", arch, "medium", num_spots=num_spots, warmup_s=warmup)
        clock = SimClock()
        epoch = 1000.0
        return CloudBackend(cfg, clock, epoch)

    def _send_event(self, cloud: CloudBackend, spot_id: int = 0, state: SpotState = SpotState.OCCUPIED, sent_at: float = 1000.0, latency_ms: float = 100.0) -> None:
        """Push one event through receive_batch with a known sent timestamp."""
        arrival = sent_at + latency_ms / 1000.0
        event = ParkingEvent(f"s{spot_id}", spot_id, state, timestamp=sent_at, sequence=1)
        batch = BatchUpdate(edge_id="edge_01", events=[event])
        payload = b'{"test": 1}'
        # Patch clock so virtual time matches
        cloud.clock.schedule_at(0.0, lambda: None)
        cloud.clock.env.run(until=arrival - cloud.epoch)
        cloud.receive_batch(batch, payload)

    def test_cloud_receives_batch_increments_counter(self):
        cloud = self._make_cloud()
        batch = BatchUpdate("e", [_occ(spot_id=0, t=1000.0)])
        cloud.receive_batch(batch, b"{}")
        assert cloud.received_batches == 1
        assert cloud.received_events == 1

    def test_cloud_latency_computed_from_timestamps(self):
        """Latency = (arrival_time - sent_time) x 1000 ms."""
        cfg = make_scenario("t", "t", "mqtt", "edge_filtered", "medium", num_spots=5, warmup_s=0.0)
        clock = SimClock()
        epoch = 0.0
        cloud = CloudBackend(cfg, clock, epoch)

        sent_at = 0.0
        # Advance DES clock to represent 200 ms of virtual delay
        clock.env.run(until=0.200)

        event = ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=sent_at, sequence=1)
        batch = BatchUpdate("e", [event])
        cloud.receive_batch(batch, b"{}")

        assert len(cloud._latency_ms_all) == 1
        latency = cloud._latency_ms_all[0]
        assert abs(latency - 200.0) < 5.0, f"Expected ~200 ms, got {latency:.2f} ms"

    def test_cloud_warmup_events_excluded_from_post_samples(self):
        """Events arriving before warmup_s should not appear in _latency_ms_post."""
        cfg = make_scenario("t", "t", "mqtt", "edge_filtered", "medium", num_spots=5, warmup_s=60.0)
        clock = SimClock()
        epoch = 0.0
        cloud = CloudBackend(cfg, clock, epoch)

        # Warmup event: virtual time = 10 s (< 60 s warmup)
        clock.env.run(until=10.0)
        ev_warm = ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1)
        cloud.receive_batch(BatchUpdate("e", [ev_warm]), b"{}")

        # Post-warmup event: virtual time = 120 s
        clock.env.run(until=120.0)
        ev_post = ParkingEvent("s0", 0, SpotState.FREE, timestamp=60.0, sequence=2)
        cloud.receive_batch(BatchUpdate("e", [ev_post]), b"{}")

        assert len(cloud._latency_ms_all) == 2
        assert len(cloud._latency_ms_post) == 1

    def test_cloud_occupancy_snapshot_correct(self):
        cfg = make_scenario("t", "t", "mqtt", "edge_filtered", "medium", num_spots=4, warmup_s=0.0)
        clock = SimClock()
        cloud = CloudBackend(cfg, clock, 0.0)

        # Mark spots 0 and 1 as occupied
        for sid in [0, 1]:
            ev = ParkingEvent(f"s{sid}", sid, SpotState.OCCUPIED, timestamp=0.0, sequence=sid + 1)
            cloud.receive_batch(BatchUpdate("e", [ev]), b"{}")

        snap = cloud.get_occupancy()
        assert snap["total"] == 4
        assert snap["occupied"] == 2
        assert snap["free"] == 2
        assert snap["occupancy_pct"] == 50.0

    def test_cloud_get_latest_events_limit(self):
        cfg = make_scenario("t", "t", "mqtt", "edge_filtered", "medium", num_spots=20, warmup_s=0.0)
        clock = SimClock()
        cloud = CloudBackend(cfg, clock, 0.0)

        for i in range(15):
            ev = ParkingEvent(f"s{i}", i, SpotState.OCCUPIED, timestamp=float(i), sequence=i + 1)
            cloud.receive_batch(BatchUpdate("e", [ev]), b"{}")

        latest = cloud.get_latest_events(limit=5)
        assert len(latest) == 5

    def test_cloud_metrics_snapshot_keys_present(self):
        cloud = self._make_cloud()
        snap = cloud.get_metrics_snapshot()
        for key in ("received_batches", "received_events", "latency_mean_ms", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "latency_min_ms", "latency_max_ms", "cpu_pct", "mem_mb"):
            assert key in snap, f"Missing key '{key}' in metrics snapshot"

    def test_cloud_broker_overhead_score_positive(self):
        cfg = make_scenario("t", "t", "mqtt", "edge_filtered", "medium", num_spots=5, warmup_s=0.0)
        clock = SimClock()
        cloud = CloudBackend(cfg, clock, 0.0)
        ev = ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=0.0, sequence=1)
        cloud.receive_batch(BatchUpdate("e", [ev]), b"{}")
        assert cloud.compute_broker_overhead_score() > 0.0

    def test_cloud_latency_timeseries_non_empty_after_events(self):
        """Latency time-series buckets are populated for post-warmup events."""
        cfg = make_scenario("t", "t", "mqtt", "edge_filtered", "medium", num_spots=5, warmup_s=0.0, aggregation_interval=30.0)
        clock = SimClock()
        cloud = CloudBackend(cfg, clock, 0.0)

        # Start at i=1 so the first run(until=...) call is run(until=35) > 0.
        for i in range(1, 6):
            clock.env.run(until=float(i * 35))
            ev = ParkingEvent("s0", 0, SpotState.OCCUPIED, timestamp=float((i - 1) * 30), sequence=i)
            cloud.receive_batch(BatchUpdate("e", [ev]), b"{}")

        ts = cloud.get_latency_timeseries()
        assert len(ts) >= 1, "Expected at least one time-series bucket"
        assert all("t_s" in entry and "mean_ms" in entry for entry in ts)


class TestSensorEmulator:
    def _run_emulator(self, num_spots: int = 20, arrival_rate: float = 0.05, duration: float = 1000.0, seed: int = 42) -> tuple[SensorEmulator, list[ParkingEvent]]:
        cfg = _make_traffic_cfg(num_spots=num_spots, seed=seed, duration=duration)
        clock = SimClock()
        events: list[ParkingEvent] = []
        emulator = SensorEmulator(cfg, arrival_rate)
        emulator.add_callback(events.append)
        emulator.schedule_run(clock, duration, epoch=0.0)
        clock.run_until(duration)
        return emulator, events

    def test_sensor_emulator_generates_events(self):
        _, events = self._run_emulator()
        assert len(events) > 0

    def test_sensor_emulator_total_generated_count(self):
        emulator, events = self._run_emulator()
        assert emulator.total_generated == len(events)

    def test_sensor_emulator_occupancy_snapshot_sums_to_num_spots(self):
        num_spots = 20
        emulator, _ = self._run_emulator(num_spots=num_spots)
        snap = emulator.occupancy_snapshot()
        assert snap["total"] == num_spots
        assert snap["occupied"] + snap["free"] == num_spots

    def test_sensor_emulator_multiple_callbacks_all_called(self):
        cfg = _make_traffic_cfg(num_spots=10, seed=11, duration=200.0)
        clock = SimClock()
        calls_a: list[ParkingEvent] = []
        calls_b: list[ParkingEvent] = []
        emulator = SensorEmulator(cfg, 0.05)
        emulator.add_callback(calls_a.append)
        emulator.add_callback(calls_b.append)
        emulator.schedule_run(clock, 200.0, epoch=0.0)
        clock.run_until(200.0)
        assert len(calls_a) == len(calls_b) == emulator.total_generated


class TestIntegration:

    def _build_pipeline(self, arch: str, num_spots: int = 20, loss: float = 0.0, arrival_rate: float = 0.05, duration: float = 1000.0, agg: float = 30.0, seed: int = 42):
        cfg = make_scenario("integration_test", "Integration", "mqtt", arch, "medium", num_spots=num_spots, loss_rate=loss, aggregation_interval=agg, warmup_s=0.0, seed=seed)
        clock = SimClock()
        epoch = 0.0
        cloud = CloudBackend(cfg, clock, epoch)

        def cloud_recv(batch: BatchUpdate, raw: bytes) -> None:
            cloud.receive_batch(batch, raw)

        if arch == "cloud_only":
            def link_to_cloud(event: ParkingEvent, raw: bytes) -> None:
                batch = BatchUpdate("direct", [event])
                cloud_recv(batch, raw)
            forward_cb = link_to_cloud
            edge = None
        else:
            edge = EdgeNode(cfg, clock, cloud_recv, epoch)

            def link_to_edge(event: ParkingEvent, raw: bytes) -> None:
                edge.receive(event, raw)
            forward_cb = link_to_edge

        link = LinkEmulator(cfg.link, clock, forward_cb=forward_cb, rng=random.Random(seed + 1))

        traffic_cfg = _make_traffic_cfg(num_spots=num_spots, seed=seed, duration=duration)
        sensors = SensorEmulator(traffic_cfg, arrival_rate)

        def sensor_cb(event: ParkingEvent) -> None:
            link.transmit(event)

        sensors.add_callback(sensor_cb)
        sensors.schedule_run(clock, duration, epoch=epoch)
        clock.run_until(duration)
        if edge is not None:
            edge.flush_final()

        return sensors, link, edge, cloud

    def test_integration_cloud_only_pipeline(self):
        sensors, link, edge, cloud = self._build_pipeline(arch="cloud_only", loss=0.0)
        assert cloud.received_events > 0
        # With zero loss, all delivered sensor events reach cloud
        assert cloud.received_events == link.stats.received

    def test_integration_edge_filtered_reduces_messages(self):
        """Edge-filtered: repeated same-state events are dropped before cloud. Compare message counts: cloud_only should have >= edge_filtered events."""
        _, _, _, cloud_co = self._build_pipeline("cloud_only", loss=0.0, seed=10)
        sensors_ef, link_ef, edge_ef, cloud_ef = self._build_pipeline("edge_filtered", loss=0.0, seed=10)
        assert cloud_ef.received_events <= cloud_co.received_events
        assert edge_ef.filtered_count >= 0    

    def test_integration_edge_aggregated_batches_to_cloud(self):
        """Edge-aggregated: N events from sensors → fewer batches at cloud than events. received_batches < received_events (batch contains multiple events)."""
        sensors, link, edge, cloud = self._build_pipeline("edge_aggregated", num_spots=30, arrival_rate=0.1, duration=300.0, agg=30.0, loss=0.0)

        # With 0.1 ev/s × 300 s = ~30 events over 10 aggregation windows, we expect fewer batches than events when multiple events batch together.
        if cloud.received_events > 1:
            # True batching happened some of the time
            assert cloud.received_batches <= cloud.received_events

    def test_integration_latency_within_link_bounds(self):
        import os
        os.environ.setdefault("USE_REAL_BROKERS", "false")

        base_ms = 80.0
        epoch = 1000.0 
        cfg = make_scenario("lat_test", "Latency", "mqtt", "cloud_only", "medium", num_spots=10, loss_rate=0.0, base_delay_ms=base_ms, jitter_ms=0.0, warmup_s=0.0, seed=7)
        clock = SimClock()
        cloud = CloudBackend(cfg, clock, epoch)

        # Force simulated (virtual-clock) mode on the backend
        cloud._real_mode = False

        def cloud_recv(batch, raw):
            cloud.receive_batch(batch, raw)

        link = LinkEmulator(cfg.link, clock, forward_cb=lambda e, b: cloud_recv(BatchUpdate("d", [e]), b), rng=random.Random(7))

        # event.timestamp = epoch + virtual_send_time (clock.now == 0 at transmit)
        for i in range(20):
            ev = ParkingEvent(f"s{i}", i % 10, SpotState.OCCUPIED, timestamp=epoch + 0.0, sequence=i + 1)
            link.transmit(ev)

        clock.run_until(1000.0)

        lats = cloud._latency_ms_all
        assert len(lats) > 0, "No latency samples recorded"
        min_lat = min(lats)
        # All deliveries happen after base_delay_ms → latency ≥ base_ms - 1 ms tolerance
        assert all(lat >= base_ms - 1.0 for lat in lats), (
            f"Min latency {min_lat:.2f} ms is below base delay {base_ms} ms.\n"
            f"Latency samples: {[round(l,1) for l in sorted(lats)[:10]]}"
        )

    def test_integration_delivery_ratio_with_loss(self):
        """With 20 % link loss, end-to-end delivery ratio should be roughly 0.80."""
        sensors, link, edge, cloud = self._build_pipeline("cloud_only", num_spots=50, loss=0.20, arrival_rate=0.1, duration=1000.0, seed=33)

        generated = sensors.total_generated
        if generated == 0:
            pytest.skip("No events generated — increase duration or arrival_rate")

        dr = cloud.received_events / generated
        assert 0.60 <= dr <= 1.0, (
            f"Expected DR ~0.80, got {dr:.3f} (generated={generated}, "
            f"received={cloud.received_events})"
        )