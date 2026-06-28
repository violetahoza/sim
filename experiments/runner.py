from __future__ import annotations
import logging
import math
import random
import time
from pathlib import Path
from typing import Optional, Callable
import numpy as np
import uuid

from simulator.models.models import BatchUpdate, ExperimentMetrics, ParkingEvent, SpotState
from simulator.config.config import ScenarioConfig
from simulator.sensors.sensor_emulator import SensorEmulator
from simulator.edge.edge_node import EdgeNode
from simulator.cloud.cloud_backend import CloudBackend
from simulator.link.link_emulator import LinkEmulator
from simulator.des.engine import SimClock

logger = logging.getLogger(__name__)

def _make_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%S", time.gmtime()) + "_" + uuid.uuid4().hex[:6]

def _stats(samples: list[float]):
    if not samples:
        return None, None, None, None, None, None
    a = np.array(samples)
    return (float(np.mean(a)), float(np.percentile(a, 50)), float(np.percentile(a, 95)), float(np.percentile(a, 99)), float(np.min(a)), float(np.max(a)))


def _r(value: Optional[float], ndigits: int = 2) -> Optional[float]:
    return round(value, ndigits) if value is not None else None


def save_results(metrics: ExperimentMetrics, output_dir: str = "results") -> str:
    out = Path(output_dir)
    out.mkdir(exist_ok=True)

    run_id = metrics.run_id or _make_run_id()
    stem = f"{metrics.scenario_name}__seed{metrics.seed}__{run_id}"

    json_path = out / f"{stem}.json"
    import json
    data = metrics.to_dict()
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Results saved to {json_path}")

    if metrics.scenario_log and metrics.architecture != "cloud_only":
        log_path = out / f"{stem}.log"
        _write_scenario_log(metrics, log_path)
        logger.info(f"Scenario log saved to {log_path}")

    return str(json_path)


def _write_scenario_log(metrics: ExperimentMetrics, path: Path) -> None:
    lines = [
        f"Scenario Log: {metrics.scenario_name}",
        f"Architecture: {metrics.architecture}   Protocol: {metrics.protocol}",
        f"Traffic: {metrics.traffic_level}   Spots: {metrics.num_spots}   "
        f"Duration: {metrics.sim_duration_s:.0f}s",
        "=" * 72,
        "",
        "SUMMARY",
        f"  Anomalies detected    : {metrics.anomalies_detected}",
        f"  Anomalies resolved    : {metrics.anomalies_resolved}",
        f"  Active at end         : {metrics.active_anomalies}",
        f"  Affected spots        : {metrics.anomaly_detected_spots}",
        f"  Quarantined at end    : {metrics.quarantined_spots_final}",
        f"  Quarantine suppressed : {metrics.quarantine_suppressed}",
        f"  Heartbeats suppressed : {metrics.heartbeats_suppressed}",
        f"  Adaptive mode switches: {metrics.adaptive_mode_switches}",
        "",
        "EVENT LOG",
        f"{'t_virtual(s)':>14}  {'event':<22}  detail",
        "-" * 72,
    ]
    for entry in sorted(metrics.scenario_log, key=lambda e: e.get("t_virtual", 0)):
        t = entry.get("t_virtual", 0)
        ev = entry.get("event", "")
        detail = entry.get("detail", "")
        lines.append(f"{t:>14.1f}  {ev:<22}  {detail}")

    if not metrics.scenario_log:
        lines.append("  (no anomaly or mode-switch events recorded)")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class ExperimentRunner:

    def __init__(self, config: ScenarioConfig, progress_cb: Optional[Callable] = None, flush_cb: Optional[Callable] = None, real_mode: bool = False) -> None:
        self.config = config
        self.progress_cb = progress_cb
        self.flush_cb = flush_cb
        self._cancelled = False
        self._start_time = 0.0
        self._spot_states: dict[int, str] = {}
        self._edge_summary: Optional[dict] = None
        self._fault_injector = None
        self._fault_true_spots: set = set()
        self._real_mode = real_mode

    def cancel(self) -> None:
        self._cancelled = True

    async def run(self) -> ExperimentMetrics:
        cfg = self.config
        if self._real_mode:
            from experiments.run_real import run_real_for_runner
            return await run_real_for_runner(self, cfg)
        return await self._run_simulated(cfg)

    async def _run_simulated(self, cfg: ScenarioConfig) -> ExperimentMetrics:
        from simulator.cloud.db import make_engine, init_schema

        self._start_time = time.time()
        seed = cfg.random_seed or 42
        self._run_id = _make_run_id()
        epoch = 0.0

        clock = SimClock()
        arrival_rate = cfg.arrival_rate

        sensors = SensorEmulator(cfg.traffic, arrival_rate)
        self._fault_true_spots = set()
        if cfg.faults:
            from simulator.sensors.fault_injector import FaultInjector, FaultSpec, FaultType
            fi = FaultInjector(rng=random.Random(seed + 9001))
            pool = list(range(cfg.num_spots))
            random.Random(seed + 9000).shuffle(pool)
            idx = 0
            for spec_dict in cfg.faults:
                count = int(spec_dict.get("count", 1))
                chosen = pool[idx:idx + count]
                idx += count
                spec = FaultSpec(
                    fault_type=FaultType(spec_dict["type"]),
                    stuck_state=spec_dict.get("stuck_state", "occupied"),
                    replay_count=int(spec_dict.get("replay_count", 3)),
                    flood_count=int(spec_dict.get("flood_count", 10))
                )
                fi.set_faults(chosen, spec)
                self._fault_true_spots.update(chosen)
            sensors.set_fault_injector(fi)
            self._fault_injector = fi
        cloud = CloudBackend(cfg, clock, epoch)

        engine = make_engine(None)
        if engine:
            init_schema(engine)
            import json as _json
            cloud.open_run(engine, config_json=_json.dumps(cfg.to_save_dict()))

        backend = _make_simulated_backend(cfg, clock, cloud.receive_batch, seed)
        arch = cfg.architecture

        _sensor_rng = random.Random(seed + 1)

        if arch == "cloud_only":
            backhaul_link = None
            edge = EdgeNode(cfg, clock, lambda b, p: None, epoch)

            def _sensor_link_cb(event: ParkingEvent, raw: bytes) -> None:
                from simulator.utils import encode_batch
                batch = BatchUpdate(edge_id="direct", events=[event])
                payload = encode_batch(batch)
                backend.publish(batch, payload)

            link = LinkEmulator(cfg.link, clock, forward_cb=_sensor_link_cb, rng=_sensor_rng)

        else:
            _bh_link_cfg = cfg.backhaul_link.to_link_config()
            _bh_link_cfg.packet_loss_rate = 0.0
            _backhaul_rng = random.Random(seed + 5000)
            backhaul_link = LinkEmulator(_bh_link_cfg, clock, rng=_backhaul_rng)
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

            link = LinkEmulator(cfg.link, clock, forward_cb=_sensor_link_cb, rng=_sensor_rng)
            edge.set_sensor_link_stats(link.stats)
            edge.set_backhaul_delivery_probe(lambda: (getattr(backend, "frames_offered", 0), getattr(backend, "first_pass_delivered", 0)))

        for i in range(cfg.num_spots):
            self._spot_states[i] = "free"

        def sensor_cb(event: ParkingEvent) -> None:
            if not self._cancelled:
                self._spot_states[event.spot_id] = (event.state.value if isinstance(event.state, SpotState) else str(event.state))
                link.transmit(event)

        sensors.add_callback(sensor_cb)
        sensors.schedule_run(clock, cfg.sim_duration_s, epoch)

        logger.info(f"[{cfg.name}] DES simulated — {cfg.sim_duration_s:.0f} s virtual …")

        _snapshot_interval = max(1, int(cfg.sim_duration_s / 50))

        def des_progress(virtual_now: float, end_time: float) -> None:
            if self.progress_cb is None:
                return
            if int(virtual_now) % _snapshot_interval != 0:
                return
            es = edge.summary()
            snap = {
                "elapsed_s": round(time.time() - self._start_time, 1),
                "wall_duration_s": round(end_time / cfg.traffic.time_scale, 1),
                "simulated_elapsed_s": round(virtual_now, 0),
                "simulated_duration_s": end_time,
                "time_scale": cfg.traffic.time_scale,
                "sim_duration_s": cfg.sim_duration_s,
                "progress_pct": min(100, round(virtual_now / end_time * 100, 1)),
                "generated": sensors.total_generated,
                "heartbeats": sensors.heartbeats_generated,
                "heartbeat_interval_s": cfg.traffic.heartbeat_interval_s,
                "cloud_events": cloud.received_events,
                "occupancy": sensors.occupancy_snapshot(),
                "spot_states": dict(self._spot_states),
                "edge": es
            }
            self.progress_cb(snap)

        await clock.run_until_async(cfg.sim_duration_s, progress_cb=des_progress, cancelled_cb=lambda: self._cancelled, steps=getattr(self, "_des_steps", 50))
        edge.flush_final()

        retransmits = getattr(backend, "retransmitted", 0) or getattr(backend, "retransmissions", 0)
        dup_deliveries = (getattr(backend, "duplicates_delivered", 0) or getattr(backend, "duplicates_suppressed", 0))
        protocol_bytes = backend.bytes_sent

        frames_offered = getattr(backend, "frames_offered", 0)
        frames_delivered_e2c = getattr(backend, "frames_delivered", 0)
        frames_dropped_e2c = getattr(backend, "frames_dropped", 0)
        first_pass_delivered = getattr(backend, "first_pass_delivered", 0)
        dup_events_at_cloud = getattr(cloud, "duplicate_events_at_cloud", 0)

        state_agreement = cloud.compute_state_agreement(sensors.final_spot_states())

        self._edge_summary = edge.summary()
        metrics = self._collect_metrics_simulated(
            cfg, sensors, link, edge, cloud, backhaul_link, protocol_bytes,
            retransmits=retransmits, dup_deliveries=dup_deliveries, state_agreement=state_agreement,
            frames_offered=frames_offered, frames_delivered_e2c=frames_delivered_e2c,
            frames_dropped_e2c=frames_dropped_e2c, first_pass_delivered=first_pass_delivered,
            dup_events_at_cloud=dup_events_at_cloud)
        self._log_done(cfg, metrics, cloud_events=cloud.received_events)

        if self.flush_cb:
            self.flush_cb()

        cloud.flush_to_db(engine, metrics)
        return metrics

    def _collect_metrics_simulated(self, cfg, sensors: SensorEmulator, link: LinkEmulator, edge: EdgeNode, cloud: CloudBackend, backhaul_link, protocol_bytes: int = 0,
        retransmits: int = 0, dup_deliveries: int = 0, state_agreement: Optional[float] = None,
        frames_offered: int = 0, frames_delivered_e2c: int = 0, frames_dropped_e2c: int = 0,
        first_pass_delivered: int = 0, dup_events_at_cloud: int = 0) -> ExperimentMetrics:

        post_samples = cloud.get_all_latency_samples()
        lat_mean, lat_p50, lat_p95, lat_p99, lat_min, lat_max = _stats(post_samples)

        sensor_events = sensors.total_generated
        state_changes_generated = sensors.state_changes_generated
        initial_snapshots = sensors.initial_snapshots_generated
        heartbeats_gen = sensors.heartbeats_generated
        dup_sends = sensors.duplicate_sends_generated

        _identity_sum = state_changes_generated + initial_snapshots + heartbeats_gen + dup_sends
        if _identity_sum != sensor_events:
            logger.warning(
                f"[{cfg.name}] Workload identity mismatch: "
                f"total={sensor_events} vs sum={_identity_sum} "
                f"(diff={sensor_events - _identity_sum})"
            )

        cloud_msgs_total = cloud.received_events
        cloud_transitions = cloud.transitions_received
        ls = link.stats
        es = self._edge_summary if self._edge_summary else edge.summary()
        arch = cfg.architecture

        s2e_msgs = ls.sent
        s2e_bytes = ls.total_bytes_sent
        s2e_bytes_recv = ls.total_bytes_received
        s2e_dr = ls.delivery_ratio if ls.sent > 0 else None
        s2e_dropped = ls.dropped
        s2e_received = ls.received

        if arch == "cloud_only":
            e2c_msgs = 0
            e2c_bytes = 0
            e2c_bytes_recv = 0
            e2c_delivered = 0
            e2c_dropped = 0
            backhaul_dr = None
            backhaul_first_pass = 1.0 
            filtered_events = 0
            heartbeats_suppressed = 0
            quarantine_suppressed = 0
            heartbeats_forwarded = 0
            forwarded_events = 0
        else:
            bhl = (backhaul_link.stats if backhaul_link is not None else es.get("link_stats", {}))
            if isinstance(bhl, dict):
                e2c_msgs = bhl.get("sent", 0)
                e2c_bytes = bhl.get("total_bytes_sent", 0)
                e2c_bytes_recv = bhl.get("total_bytes_received", 0)
            else:
                e2c_msgs = bhl.sent
                e2c_bytes = bhl.total_bytes_sent
                e2c_bytes_recv = bhl.total_bytes_received

            e2c_delivered = frames_delivered_e2c
            e2c_dropped = frames_dropped_e2c
            backhaul_dr = (e2c_delivered / frames_offered) if frames_offered > 0 else None
            backhaul_first_pass = (first_pass_delivered / frames_offered) if (first_pass_delivered is not None and frames_offered > 0) else None

            filtered_events = es.get("filtered", 0)
            heartbeats_suppressed = es.get("heartbeats_suppressed", 0)
            quarantine_suppressed = es.get("quarantine_suppressed", 0)
            heartbeats_forwarded = es.get("heartbeats_forwarded", 0)
            forwarded_events = es.get("forwarded_events", 0)

        if s2e_dr is None:
            physical_delivery_ratio = None
        else:
            fp = backhaul_first_pass if backhaul_first_pass is not None else 1.0
            physical_delivery_ratio = s2e_dr * fp

        e2e_unique = (min(cloud_transitions / state_changes_generated, 1.0) if state_changes_generated > 0 else None)
        cloud_reflection_ratio = state_agreement

        if arch == "cloud_only":
            aggregation_ratio = None
            message_reduction_ratio = None
            events_per_cloud_message = None
        else:
            aggregation_ratio = (e2c_msgs / forwarded_events) if forwarded_events > 0 else None
            message_reduction_ratio = (max(0.0, 1.0 - e2c_msgs / s2e_received) if s2e_received > 0 else None)
            events_per_cloud_message = (forwarded_events / e2c_msgs) if e2c_msgs > 0 else None

        true_spots = getattr(self, "_fault_true_spots", set())
        quarantined_spots = set(es.get("ever_quarantined", []))
        fault_injected = self._fault_injector.injected_count if self._fault_injector is not None else 0
        if true_spots:
            tp = len(quarantined_spots & true_spots)
            precision = (tp / len(quarantined_spots)) if quarantined_spots else 0.0
            recall = tp / len(true_spots)
            f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
        else:
            precision = recall = f1 = None

        return ExperimentMetrics(
            scenario_name=cfg.name,
            seed=cfg.random_seed if cfg.random_seed is not None else 42,
            run_id=getattr(self, "_run_id", ""),
            protocol=cfg.protocol,
            architecture=cfg.architecture,
            traffic_level=cfg.traffic_level,
            num_spots=cfg.num_spots,
            sim_duration_s=cfg.sim_duration_s,

            latency_mean_ms=_r(lat_mean),
            latency_p50_ms=_r(lat_p50),
            latency_p95_ms=_r(lat_p95),
            latency_p99_ms=_r(lat_p99),
            latency_min_ms=_r(lat_min),
            latency_max_ms=_r(lat_max),

            events_generated=sensor_events,
            valid_state_changes=state_changes_generated,
            initial_snapshots_generated=initial_snapshots,
            heartbeats_generated=heartbeats_gen,
            duplicate_sends_generated=dup_sends,
            heartbeat_interval_s=cfg.traffic.heartbeat_interval_s,

            sensor_to_edge_msgs=s2e_msgs,
            sensor_link_dropped=s2e_dropped,
            sensor_link_collisions=ls.collisions,
            sensor_to_edge_delivery_ratio=_r(s2e_dr, 4),
            sensor_to_edge_bytes=s2e_bytes,
            bytes_s2e_received=s2e_bytes_recv,

            filtered_events=filtered_events,
            heartbeats_suppressed=heartbeats_suppressed,
            quarantine_suppressed=quarantine_suppressed,
            heartbeats_forwarded=heartbeats_forwarded,
            events_forwarded_total=forwarded_events,

            edge_to_cloud_msgs=e2c_msgs,
            edge_to_cloud_bytes=e2c_bytes,
            edge_to_cloud_dropped=e2c_dropped,
            edge_to_cloud_delivered=e2c_delivered,
            bytes_e2c_received=e2c_bytes_recv,
            backhaul_delivery_ratio=_r(backhaul_dr, 4),

            retransmissions_total=retransmits,
            duplicate_deliveries=dup_deliveries,
            protocol_bytes=protocol_bytes,

            aggregation_ratio=_r(aggregation_ratio, 4),
            message_reduction_ratio=_r(message_reduction_ratio, 4),
            events_per_cloud_message=_r(events_per_cloud_message, 2),

            cloud_msgs_received_total=cloud_msgs_total,
            cloud_batches_received=getattr(cloud, "received_batches", 0),
            cloud_events_post_dedup=cloud_msgs_total - dup_events_at_cloud,
            cloud_state_changes_reflected=cloud_transitions,
            duplicate_events_at_cloud=dup_events_at_cloud,
            e2e_unique_delivery_ratio=_r(e2e_unique, 4),
            cloud_reflection_ratio=_r(cloud_reflection_ratio, 4),
            physical_delivery_ratio=_r(physical_delivery_ratio, 4),

            anomalies_detected=es.get("anomalies", 0),
            anomalies_resolved=es.get("resolved_anomalies", 0),
            active_anomalies=es.get("active_anomalies", 0),
            adaptive_mode_switches=es.get("mode_switches", 0),
            adaptive_window_adjustments=es.get("adaptive_window_adjustments", 0),
            adaptive_min_window_dr=_r(es.get("adaptive_min_window_dr"), 4),
            quarantined_spots_final=es.get("quarantined_count", 0),
            anomaly_detected_spots=es.get("detected_spots", 0),
            fault_injected_count=fault_injected,
            fault_true_count=len(true_spots),
            anomaly_precision=_r(precision) if precision is not None else None,
            anomaly_recall=_r(recall) if recall is not None else None,
            anomaly_f1=_r(f1) if f1 is not None else None,

            latency_samples=post_samples[-50_000:],
            scenario_log=es.get("event_log", []),

            final_spot_states=sensors.final_spot_states(),
            final_occupancy=sensors.occupancy_snapshot()
        )



    def _log_done(self, cfg, metrics: ExperimentMetrics, cloud_events: int) -> None:
        arch = cfg.architecture
        m = metrics
        lat_mean = m.latency_mean_ms if m.latency_mean_ms is not None else 0.0
        lat_p99 = m.latency_p99_ms if m.latency_p99_ms is not None else 0.0
        refl = m.cloud_reflection_ratio if m.cloud_reflection_ratio is not None else 0.0
        e2e = m.e2e_unique_delivery_ratio if m.e2e_unique_delivery_ratio is not None else 0.0
        msg_red = m.message_reduction_ratio if m.message_reduction_ratio is not None else 0.0
        if arch == "cloud_only":
            logger.info(
                f"[{cfg.name}] Done (cloud_only). "
                f"sensor_sent={m.sensor_to_edge_msgs}  "
                f"sensor_dropped={m.sensor_link_dropped}  "
                f"wire_delivered={m.sensor_to_edge_msgs - m.sensor_link_dropped}  "
                f"cloud_msgs_total={cloud_events}  "
                f"cloud_transitions={m.cloud_state_changes_reflected}  "
                f"e2e_unique={e2e:.1%}  "
                f"agreement={refl:.1%}  "
                f"lat={lat_mean:.1f}ms  p99={lat_p99:.1f}ms"
            )
        else:
            logger.info(
                f"[{cfg.name}] Done ({arch}). "
                f"sensor_sent={m.sensor_to_edge_msgs}  "
                f"filtered={m.filtered_events}  "
                f"  of which hb_suppressed={m.heartbeats_suppressed}"
                f"  quar_suppressed={m.quarantine_suppressed}  "
                f"forwarded={m.edge_to_cloud_msgs}  "
                f"e2c_delivered={m.edge_to_cloud_delivered}  "
                f"cloud_msgs_total={cloud_events}  "
                f"cloud_transitions={m.cloud_state_changes_reflected}  "
                f"e2c_dropped={m.edge_to_cloud_dropped}  "
                f"e2e_unique={e2e:.1%}  "
                f"agreement={refl:.1%}  "
                f"msg_reduction={msg_red:.1%}  "
                f"anomalies={m.anomalies_detected}  "
                f"quarantined={m.quarantined_spots_final}  "
                f"mode_switches={m.adaptive_mode_switches}  "
                f"lat={lat_mean:.1f}ms  p99={lat_p99:.1f}ms"
            )


BACKHAUL_CONGESTION_PEAK_HOURS = (8.0, 18.0)
BACKHAUL_CONGESTION_WIDTH_H = 1.5


def _make_loss_provider(cfg):
    if cfg.architecture == "cloud_only":
        return None
    peak = cfg.backhaul_link.loss_peak_rate
    if peak is None:
        return None
    floor = cfg.backhaul_link.packet_loss_rate
    start_hour = cfg.traffic.start_hour

    def provider(t: float) -> float:
        hour = (start_hour + t / 3600.0) % 24.0
        bump = 0.0
        for c in BACKHAUL_CONGESTION_PEAK_HOURS:
            d = abs(hour - c)
            d = min(d, 24.0 - d)
            bump = max(bump, math.exp(-0.5 * (d / BACKHAUL_CONGESTION_WIDTH_H) ** 2))
        return floor + (peak - floor) * bump

    return provider

def _make_simulated_backend(cfg, clock, cloud_recv, seed):
    from simulator.protocols.mqtt_client import SimulatedMQTTBackend
    from simulator.protocols.amqp_client import SimulatedAMQPBackend
    from simulator.protocols.coap_client import SimulatedCoAPBackend

    if cfg.architecture == "cloud_only":
        uplink_loss = 0.0
        downlink_loss = 0.0
        ack_one_way = cfg.link.base_delay_ms / 1000.0
        ack_jitter = cfg.link.jitter_ms / 1000.0
    else:
        uplink_loss = cfg.backhaul_link.packet_loss_rate
        downlink_loss = cfg.backhaul_link.downlink_loss_rate if cfg.backhaul_link.downlink_loss_rate is not None else uplink_loss
        ack_one_way = cfg.backhaul_link.base_delay_ms / 1000.0
        ack_jitter = cfg.backhaul_link.jitter_ms / 1000.0

    loss_provider = _make_loss_provider(cfg)
    proto = cfg.protocol
    if proto == "mqtt":
        return SimulatedMQTTBackend(cfg.mqtt, clock, cloud_recv, uplink_loss, seed + 2, ack_one_way, ack_jitter, downlink_loss, loss_provider)
    elif proto == "amqp":
        return SimulatedAMQPBackend(cfg.amqp, clock, cloud_recv, uplink_loss, seed + 2, ack_one_way, ack_jitter, downlink_loss, loss_provider)
    elif proto == "coap":
        return SimulatedCoAPBackend(cfg.coap, clock, cloud_recv, uplink_loss, seed + 2, ack_one_way, ack_jitter, downlink_loss, loss_provider)
    raise ValueError(f"Unknown protocol: {proto}")


def run_scenario_sync(cfg: ScenarioConfig, steps: int = 1, real_mode: bool = False) -> ExperimentMetrics:
    import asyncio
    runner = ExperimentRunner(cfg, real_mode=real_mode)
    runner._des_steps = steps
    return asyncio.run(runner.run())


def _cli_scenarios(include_custom: bool = False) -> list[ScenarioConfig]:
    from simulator.config.config import PREDEFINED_SCENARIOS, load_custom_scenarios
    scns = list(PREDEFINED_SCENARIOS)
    if include_custom:
        scns += load_custom_scenarios()
    return scns


def _group_key(cfg: ScenarioConfig) -> str:
    return (cfg.group or "").split(" ")[0].upper()


def _resolve_cli_scenarios(args) -> list[ScenarioConfig]:
    registry = {s.name: s for s in _cli_scenarios(include_custom=True)}
    chosen: list[ScenarioConfig] = []
    if args.all:
        chosen = _cli_scenarios(args.include_custom)
    else:
        for n in args.scenarios:
            if n not in registry:
                raise SystemExit(f"Unknown scenario: {n}\nUse --list to see available names.")
            chosen.append(registry[n])
        if args.group:
            wanted = {g.upper() for g in args.group}
            chosen += [s for s in _cli_scenarios(args.include_custom) if _group_key(s) in wanted]
    seen: set[str] = set()
    unique: list[ScenarioConfig] = []
    for s in chosen:
        if s.name not in seen:
            seen.add(s.name)
            unique.append(s)
    return unique


def _build_cli():
    import argparse
    ap = argparse.ArgumentParser(
        prog="python -m experiments.runner",
        description="Run one or more scenarios (single seed) and write their results JSON. " "For multi-seed runs with 95% confidence intervals, use 'python -m experiments.multiseed'."
    )
    ap.add_argument("scenarios", nargs="*", help="Scenario name(s) to run, e.g. B2_mqtt_qos1 E6_load_mqtt_peak.")
    ap.add_argument("--all", action="store_true", help="Run every predefined scenario.")
    ap.add_argument("--group", nargs="+", metavar="G", help="Run whole group(s) by leading letter, e.g. --group E A.")
    ap.add_argument("--seed", type=int, default=None, help="Override the seed for every run (default: each scenario's own seed).")
    ap.add_argument("--out-dir", default="results", help="Directory to write result JSONs (default: results).")
    ap.add_argument("--steps", type=int, default=1, help="DES progress granularity (does not affect results).")
    ap.add_argument("--include-custom", action="store_true", help="Include custom (user) scenarios with --all / --group.")
    ap.add_argument("--list", action="store_true", help="List available scenarios and exit.")
    ap.add_argument("--quiet", action="store_true", help="Silence per-run INFO logs (just print the summary line per run).")
    return ap


def main(argv: list[str] | None = None) -> None:
    import time, copy, logging
    args = _build_cli().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    if args.list:
        for s in _cli_scenarios(include_custom=True):
            tag = "" if s.is_builtin else "  (custom)"
            print(f"{s.name:24s} {s.protocol:5s} {s.architecture:16s} {s.traffic_level:7s} {s.group}{tag}")
        return

    scns = _resolve_cli_scenarios(args)
    if not scns:
        raise SystemExit("No scenarios selected. Pass scenario name(s), --group G, or --all (see --list).")

    seed_note = args.seed if args.seed is not None else "scenario default"
    print(f"Running {len(scns)} scenario(s) -> {args.out_dir}/  (seed: {seed_note})")
    t0 = time.time()
    for i, cfg in enumerate(scns, 1):
        if args.seed is not None:
            cfg = copy.deepcopy(cfg)
            cfg.random_seed = int(args.seed)
            if getattr(cfg, "traffic", None) is not None:
                cfg.traffic.random_seed = int(args.seed)
        t = time.time()
        m = run_scenario_sync(cfg, steps=args.steps)
        path = save_results(m, args.out_dir)
        lat = m.latency_mean_ms if m.latency_mean_ms is not None else 0.0
        e2e = m.e2e_unique_delivery_ratio
        print(f"[{i:2d}/{len(scns)}] {cfg.name:24s} {time.time()-t:5.1f}s  "
              f"lat_mean={lat:8.1f}ms  e2e={e2e}  -> {path}")
    print(f"Done: {len(scns)} run(s) in {time.time()-t0:.1f}s. Results in {args.out_dir}/")


if __name__ == "__main__":
    main()