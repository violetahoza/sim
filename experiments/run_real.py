from __future__ import annotations
import argparse
import asyncio
import copy
import logging
import random
import socket
import sys
import time

from simulator.models.models import BatchUpdate, ParkingEvent, SpotState
from simulator.config.config import ScenarioConfig, SCENARIO_REGISTRY, load_custom_scenarios, make_scenario, ARRIVAL_RATES
from simulator.sensors.sensor_emulator import SensorEmulator
from simulator.edge.edge_node import EdgeNode
from simulator.cloud.cloud_backend import CloudBackend
from simulator.link.link_emulator import LinkEmulator
from simulator.des.engine import SimClock
from experiments.runner import ExperimentRunner, save_results, _make_run_id, _protocol_seed_offset

logger = logging.getLogger(__name__)

_PORTS = {"mqtt": 1883, "amqp": 5672}
_REAL_PROTOCOLS = ("mqtt", "amqp")


def _make_real_backend(cfg, subscriber_cb):
    proto = cfg.protocol
    if proto == "mqtt":
        from simulator.protocols.mqtt_real import RealMQTTBackend
        return RealMQTTBackend(cfg.mqtt, subscriber_cb)
    elif proto == "amqp":
        from simulator.protocols.amqp_real import RealAMQPBackend
        return RealAMQPBackend(cfg.amqp, subscriber_cb)
    raise ValueError(
        f"Real mode v1 supports mqtt/amqp only; '{proto}' real backend is "
        f"deferred (D4). Run this scenario in simulated mode instead."
    )


async def run_real_for_runner(runner: "ExperimentRunner", cfg: ScenarioConfig):
    from simulator.cloud.db import make_engine, init_schema

    if cfg.protocol not in _REAL_PROTOCOLS:
        raise ValueError(
            f"Real mode v1 supports mqtt/amqp only; '{cfg.protocol}' real "
            f"backend is deferred (D4). Run this scenario in simulated mode."
        )

    runner._start_time = time.time()
    seed = cfg.random_seed or 42
    runner._run_id = _make_run_id()
    epoch = 0.0  

    clock = SimClock()
    arrival_rate = cfg.arrival_rate

    sensors = SensorEmulator(cfg.traffic, arrival_rate, wall_clock=True)
    cloud = CloudBackend(cfg, clock, epoch)

    engine = make_engine(None)
    if engine:
        init_schema(engine)
        import json as _json
        cloud.open_run(engine, config_json=_json.dumps(cfg.to_save_dict()))

    def _cloud_recv_real(batch: BatchUpdate, raw: bytes) -> None:
        cloud.receive_batch_real(batch, raw, time.time())

    backend = _make_real_backend(cfg, _cloud_recv_real)
    await backend.start()  

    arch = cfg.architecture
    _sensor_rng = random.Random(seed + _protocol_seed_offset(cfg.protocol))

    if arch == "cloud_only":
        backhaul_link = None
        edge = EdgeNode(cfg, clock, lambda b, p: None, epoch)

        def _sensor_link_cb(event: ParkingEvent, raw: bytes) -> None:
            batch = BatchUpdate(edge_id="direct", events=[event])
            payload = EdgeNode._serialize_batch(batch)
            backend.publish(batch, payload)

        link = LinkEmulator(cfg.link, clock, forward_cb=_sensor_link_cb, rng=_sensor_rng, wall_clock=True)
    else:
        _bh_link_cfg = cfg.backhaul_link.to_link_config()  # keep configured loss (NOT zeroed)
        _backhaul_rng = random.Random(seed + _protocol_seed_offset(cfg.protocol) + 5000)
        backhaul_link = LinkEmulator(_bh_link_cfg, clock, rng=_backhaul_rng, wall_clock=True)
        backhaul_link.stats = backhaul_link.stats.__class__(name="edge_to_cloud_backhaul")
        backhaul_link.set_batch_callback(backend.publish)

        def edge_to_cloud_cb(batch: BatchUpdate, raw: bytes) -> None:
            backhaul_link.transmit_batch(batch, raw)

        edge = EdgeNode(cfg, clock, edge_to_cloud_cb, epoch)
        backhaul_link.on_drop = lambda: edge.record_cloud_drop()
        backend.on_drop = lambda: edge.record_cloud_drop()
        edge.set_backhaul_link_stats(backhaul_link.stats)

        def _sensor_link_cb(event: ParkingEvent, raw: bytes) -> None:
            edge.receive(event, raw)

        link = LinkEmulator(cfg.link, clock, forward_cb=_sensor_link_cb, rng=_sensor_rng, wall_clock=True)
        edge.set_sensor_link_stats(link.stats)

    for i in range(cfg.num_spots):
        runner._spot_states[i] = "free"

    def sensor_cb(event: ParkingEvent) -> None:
        if not runner._cancelled:
            runner._spot_states[event.spot_id] = (event.state.value if isinstance(event.state, SpotState) else str(event.state))
            link.transmit(event)

    sensors.add_callback(sensor_cb)
    sensors.schedule_run(clock, cfg.sim_duration_s, epoch)

    logger.info(f"[{cfg.name}] REAL mode ({cfg.protocol}) — {cfg.sim_duration_s:.0f} s wall-clock …")

    _snapshot_interval = max(1, int(cfg.sim_duration_s / 50))

    def des_progress(virtual_now: float, end_time: float) -> None:
        if runner.progress_cb is None:
            return
        if int(virtual_now) % _snapshot_interval != 0:
            return
        es = edge.summary()
        snap = {
            "elapsed_s": round(time.time() - runner._start_time, 1),
            "simulated_elapsed_s": round(virtual_now, 0),
            "simulated_duration_s": end_time,
            "time_scale": 1.0,
            "sim_duration_s": cfg.sim_duration_s,
            "progress_pct": min(100, round(virtual_now / end_time * 100, 1)),
            "generated": sensors.total_generated,
            "heartbeats": sensors.heartbeats_generated,
            "heartbeat_interval_s": cfg.traffic.heartbeat_interval_s,
            "cloud_events": cloud.received_events,
            "occupancy": sensors.occupancy_snapshot(),
            "spot_states": dict(runner._spot_states),
            "edge": es,
            "mode": "real"
        }
        runner.progress_cb(snap)

    await clock.run_until_async(
        cfg.sim_duration_s, progress_cb=des_progress,
        cancelled_cb=lambda: runner._cancelled,
        steps=getattr(runner, "_des_steps", 50),
        real_mode=True, time_scale=1.0
    )
    edge.flush_final()

    await asyncio.sleep(getattr(runner, "_real_drain_s", 2.0))
    await backend.stop()

    protocol_bytes = backend.bytes_sent
    retransmits = getattr(backend, "retransmitted", 0) or 0     
    dup_deliveries = getattr(backend, "duplicates_delivered", 0) or 0 

    if arch == "cloud_only":
        frames_offered = backend.frames_offered
        frames_delivered_e2c = backend.frames_delivered
        frames_dropped_e2c = backend.frames_dropped
    else:
        frames_offered = backhaul_link.stats.sent
        frames_delivered_e2c = backend.frames_delivered
        frames_dropped_e2c = backhaul_link.stats.dropped + backend.frames_dropped
    first_pass_delivered = None   
    dup_events_at_cloud = getattr(cloud, "duplicate_events_at_cloud", 0)

    state_agreement = cloud.compute_state_agreement(sensors.final_spot_states())

    runner._edge_summary = edge.summary()
    metrics = runner._collect_metrics_simulated(
        cfg, sensors, link, edge, cloud, backhaul_link, protocol_bytes,
        retransmits=retransmits, dup_deliveries=dup_deliveries, state_agreement=state_agreement,
        frames_offered=frames_offered, frames_delivered_e2c=frames_delivered_e2c,
        frames_dropped_e2c=frames_dropped_e2c, first_pass_delivered=first_pass_delivered,
        dup_events_at_cloud=dup_events_at_cloud)
    runner._log_done(cfg, metrics, cloud_events=cloud.received_events)

    if runner.flush_cb:
        runner.flush_cb()

    cloud.flush_to_db(engine, metrics)
    return metrics


def _all_scenarios() -> dict:
    reg = dict(SCENARIO_REGISTRY)
    for c in load_custom_scenarios():
        reg[c.name] = c
    return reg


def _reachable(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _print_scenarios() -> None:
    reg = _all_scenarios()
    print(f"{len(reg)} scenarios:")
    for name in sorted(reg):
        s = reg[name]
        print(f"  {name:30s} {s.protocol:5s} {s.architecture:16s} "
              f"{s.traffic_level:6s} {s.num_spots:4d} spots  {s.sim_duration_s/3600:.1f}h")


def _apply_overrides(cfg, *, spots, duration, seed, backhaul_loss):
    cfg = copy.deepcopy(cfg)  
    if spots is not None:
        cfg.num_spots = spots
        cfg.traffic.num_spots = spots
        cfg.arrival_rate = spots * ARRIVAL_RATES[cfg.traffic_level]
    if duration is not None:
        cfg.sim_duration_s = duration
        cfg.traffic.sim_duration_s = duration
    if seed is not None:
        cfg.random_seed = seed
        cfg.traffic.random_seed = seed
    if backhaul_loss is not None:
        cfg.backhaul_link.packet_loss_rate = backhaul_loss
    return cfg


def _build_adhoc(args):
    name = args.name or f"cli_{args.protocol}_{args.arch}"
    cfg = make_scenario(
        name=name, description="CLI real-mode run",
        protocol=args.protocol, architecture=args.arch,
        traffic_level=args.traffic, num_spots=(args.spots or 20),
        loss_rate=(args.loss if args.loss is not None else 0.0)
    )
    cfg.sim_duration_s = args.duration if args.duration is not None else 30.0
    cfg.traffic.sim_duration_s = cfg.sim_duration_s
    if args.seed is not None:
        cfg.random_seed = args.seed
        cfg.traffic.random_seed = args.seed
    if args.backhaul_loss is not None:
        cfg.backhaul_link.packet_loss_rate = args.backhaul_loss
    if args.protocol == "mqtt":
        cfg.mqtt.qos = args.qos
    elif args.protocol == "amqp":
        cfg.amqp.exchange_type = args.amqp_exchange
        cfg.amqp.ack_mode = args.amqp_ack
        cfg.amqp.durable = args.amqp_durable
    return cfg


def _progress(snap: dict) -> None:
    pct = snap.get("progress_pct", 0.0)
    el = snap.get("elapsed_s", 0.0)
    gen = snap.get("generated", 0)
    cloud = snap.get("cloud_events", 0)
    sys.stdout.write(f"\r  [{pct:5.1f}%] elapsed={el:5.1f}s  generated={gen}  cloud={cloud}     ")
    sys.stdout.flush()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Run a scenario in REAL mode (live broker).")
    p.add_argument("--list", action="store_true", help="List available scenarios and exit.")
    p.add_argument("--scenario", help="Name of a preset scenario to run.")

    p.add_argument("--protocol", choices=_REAL_PROTOCOLS, help="Protocol for an ad-hoc run.")
    p.add_argument("--arch", default="cloud_only", choices=["cloud_only", "edge_filtered", "edge_aggregated"])
    p.add_argument("--traffic", default="peak", choices=["low", "medium", "peak"])
    p.add_argument("--name", help="Optional name for an ad-hoc scenario.")

    p.add_argument("--spots", type=int, help="Override number of spots.")
    p.add_argument("--duration", type=float, help="Override duration in seconds (== wall seconds).")
    p.add_argument("--seed", type=int, help="Override random seed.")
    p.add_argument("--loss", type=float, help="Sensor->edge access link loss rate (0..1).")
    p.add_argument("--backhaul-loss", dest="backhaul_loss", type=float, help="Emulated backhaul loss (0..1); sits BELOW the broker in real mode.")

    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2], help="MQTT QoS (ad-hoc).")
    p.add_argument("--amqp-exchange", dest="amqp_exchange", default="direct", choices=["direct", "topic", "fanout"])
    p.add_argument("--amqp-ack", dest="amqp_ack", default="manual", choices=["auto", "manual"])
    p.add_argument("--amqp-no-durable", dest="amqp_durable", action="store_false", default=True, help="Make AMQP messages non-durable (default: durable).")

    p.add_argument("--drain", type=float, default=2.0, help="Seconds to wait for in-flight round-trips before teardown.")
    p.add_argument("--max-duration", dest="max_duration", type=float, default=600.0, help="Refuse runs longer than this many wall-seconds unless --force.")
    p.add_argument("--force", action="store_true", help="Allow runs longer than --max-duration.")
    p.add_argument("--out", default="results", help="Results output directory.")
    p.add_argument("--no-save", dest="save", action="store_false", default=True, help="Do not write a results file.")

    args = p.parse_args(argv)

    if args.list:
        _print_scenarios()
        return 0

    if args.scenario:
        reg = _all_scenarios()
        base = reg.get(args.scenario)
        if base is None:
            print(f"Unknown scenario: {args.scenario!r}. Use --list to see options.",
                  file=sys.stderr)
            return 2
        cfg = _apply_overrides(base, spots=args.spots, duration=args.duration,
                               seed=args.seed, backhaul_loss=args.backhaul_loss)
    else:
        if not args.protocol:
            print("Provide either --scenario NAME or --protocol for an ad-hoc run "
                  "(see --help).", file=sys.stderr)
            return 2
        cfg = _build_adhoc(args)

    if cfg.protocol not in _REAL_PROTOCOLS:
        print(f"Real mode v1 supports {_REAL_PROTOCOLS}; scenario protocol is "
              f"{cfg.protocol!r}. Real CoAP is deferred. Run it in simulated mode instead.",
              file=sys.stderr)
        return 2

    if cfg.sim_duration_s > args.max_duration and not args.force:
        print(f"Refusing: real mode runs in WALL-CLOCK time, and duration="
              f"{cfg.sim_duration_s:.0f}s exceeds --max-duration={args.max_duration:.0f}s. "
              f"Shorten with --duration, or pass --force to override.", file=sys.stderr)
        return 2

    port = _PORTS[cfg.protocol]
    if not _reachable("localhost", port):
        print(f"{cfg.protocol} broker not reachable on localhost:{port}. "
              f"Start it with `docker compose up -d` and wait until "
              f"`docker compose ps` shows healthy.", file=sys.stderr)
        return 3

    print(f"Running REAL mode: {cfg.name}  "
          f"({cfg.protocol}, {cfg.architecture}, {cfg.traffic_level}, "
          f"{cfg.num_spots} spots, {cfg.sim_duration_s:.0f}s wall-clock)")

    runner = ExperimentRunner(cfg, progress_cb=_progress, real_mode=True)
    runner._real_drain_s = args.drain
    m = asyncio.run(run_real_for_runner(runner, cfg))
    print() 

    print("-" * 60)
    print(f"Generated          : {m.events_generated}")
    print(f"Cloud received     : {m.cloud_msgs_received_total}")
    print(f"Protocol wire bytes: {m.protocol_bytes}")
    print(f"Latency mean/p95/p99 ms: "
          f"{m.latency_mean_ms} / {m.latency_p95_ms} / {m.latency_p99_ms}")

    if args.save:
        path = save_results(m, args.out)
        print(f"Saved: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())