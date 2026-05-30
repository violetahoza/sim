from __future__ import annotations
import asyncio
import logging
import random
import time
from pathlib import Path
from typing import Optional, Callable

import numpy as np

from simulator.models import BatchUpdate, ExperimentMetrics, ParkingEvent, SpotState
from simulator.config import ScenarioConfig
from simulator.sensors.sensor_emulator import SensorEmulator
from simulator.edge.edge_node import EdgeNode
from simulator.cloud.cloud_backend import CloudBackend
from simulator.link.link_emulator import LinkEmulator  
from simulator.des.engine import SimClock

logger = logging.getLogger(__name__)


def _stats(samples: list[float]):
    if not samples:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    a = np.array(samples)
    return (float(np.mean(a)), float(np.percentile(a, 50)), float(np.percentile(a, 95)), float(np.percentile(a, 99)), float(np.min(a)), float(np.max(a)))


def _protocol_seed_offset(protocol: str) -> int:
    return {"mqtt": 0, "coap": 1000, "amqp": 2000}.get(protocol, 0)


def save_results(metrics: ExperimentMetrics, output_dir: str = "results") -> str:
    out = Path(output_dir)
    out.mkdir(exist_ok=True)

    json_path = out / f"{metrics.scenario_name}.json"
    import json
    data = metrics.to_dict()
    data["latency_samples"] = metrics.latency_samples
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Results saved to {json_path}")

    if metrics.scenario_log and metrics.architecture != "cloud_only":
        log_path = out / f"{metrics.scenario_name}.log"
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

    def __init__(self, config: ScenarioConfig, progress_cb: Optional[Callable] = None, flush_cb: Optional[Callable] = None) -> None:
        self.config = config
        self.progress_cb = progress_cb
        self.flush_cb = flush_cb
        self._cancelled = False
        self._start_time = 0.0
        self._spot_states: dict[int, str] = {}
        self._edge_summary: Optional[dict] = None
        self._fault_injector = None

    def cancel(self) -> None:
        self._cancelled = True

    async def run(self) -> ExperimentMetrics:
        cfg = self.config
        use_real_brokers = getattr(cfg, "_use_real_brokers", False)
        if use_real_brokers:
            return await self._run_real(cfg)
        return await self._run_simulated(cfg)


    async def _run_simulated(self, cfg: ScenarioConfig) -> ExperimentMetrics:
        from simulator.db import make_engine, init_schema

        self._start_time = time.time()
        seed = cfg.random_seed or 42
        epoch = 0.0

        clock = SimClock()
        arrival_rate = cfg.arrival_rate

        sensors = SensorEmulator(cfg.traffic, arrival_rate)
        cloud = CloudBackend(cfg, clock, epoch)

        engine = make_engine(None)
        if engine:
            init_schema(engine)
            import json as _json
            cloud.open_run(engine, config_json=_json.dumps(cfg.to_save_dict()))

        backend = _make_simulated_backend(cfg, clock, cloud.receive_batch, seed)

        arch = cfg.architecture

        if arch == "cloud_only":
            backhaul_link = None
            edge = EdgeNode(cfg, clock, lambda b, p: None, epoch)

            def _sensor_link_cb(event: ParkingEvent, raw: bytes) -> None:
                batch = BatchUpdate(edge_id="direct", events=[event])
                payload = EdgeNode._serialize_batch(batch)
                backend.publish(batch, payload)

        else:
            _backhaul_rng = random.Random(seed + _protocol_seed_offset(cfg.protocol) + 5000)
            backhaul_link = LinkEmulator(cfg.backhaul_link.to_link_config(), clock, rng=_backhaul_rng)
            backhaul_link.stats = backhaul_link.stats.__class__(name="edge_to_cloud_backhaul")
            backhaul_link.set_batch_callback(backend.publish)

            def edge_to_cloud_cb(batch: BatchUpdate, raw: bytes) -> None:
                backhaul_link.transmit_batch(batch, raw)

            edge = EdgeNode(cfg, clock, edge_to_cloud_cb, epoch)
            backhaul_link.on_drop = lambda: edge.record_cloud_drop()
            backend.on_drop = lambda: edge.record_cloud_drop()
            edge.set_backhaul_link_stats(backhaul_link.stats)

        _sensor_rng = random.Random(seed + _protocol_seed_offset(cfg.protocol))

        if arch == "cloud_only":
            def _sensor_link_cb_co(event: ParkingEvent, raw: bytes) -> None:
                batch = BatchUpdate(edge_id="direct", events=[event])
                payload = EdgeNode._serialize_batch(batch)
                backend.publish(batch, payload)
            link = LinkEmulator(cfg.link, clock, forward_cb=_sensor_link_cb_co, rng=_sensor_rng)
        else:
            def _sensor_link_cb_edge(event: ParkingEvent, raw: bytes) -> None:
                edge.receive(event, raw)
            link = LinkEmulator(cfg.link, clock, forward_cb=_sensor_link_cb_edge, rng=_sensor_rng)
            edge.set_sensor_link_stats(link.stats)

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

        await clock.run_until_async(cfg.sim_duration_s, progress_cb=des_progress, cancelled_cb=lambda: self._cancelled)
        edge.flush_final()

        retransmits = getattr(backend, "retransmitted", 0)
        dup_deliveries = (getattr(backend, "duplicates_delivered", 0) or getattr(backend, "duplicates_suppressed", 0))
        protocol_bytes = backend.bytes_sent

        self._edge_summary = edge.summary()
        metrics = self._collect_metrics_simulated(cfg, sensors, link, edge, cloud, backhaul_link, protocol_bytes, retransmits=retransmits, dup_deliveries=dup_deliveries)
        self._log_done(cfg, metrics, cloud_events=cloud.received_events)

        if self.flush_cb:
            self.flush_cb()

        cloud.flush_to_db(engine, metrics)
        return metrics


    def _collect_metrics_simulated(self, cfg, sensors: SensorEmulator, link: LinkEmulator, edge: EdgeNode, cloud: CloudBackend, backhaul_link, protocol_bytes: int = 0,
        retransmits: int = 0, dup_deliveries: int = 0) -> ExperimentMetrics:
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

        cloud_events = cloud.received_events
        cloud_transitions = cloud.transitions_received
        ls = link.stats
        es = self._edge_summary if self._edge_summary else edge.summary()
        arch = cfg.architecture

        s2e_msgs = ls.sent
        s2e_bytes = ls.total_bytes_sent
        s2e_dr = ls.delivery_ratio
        s2e_dropped = ls.dropped

        if arch == "cloud_only":
            e2c_msgs = 0
            e2c_bytes = 0
            e2c_dropped = 0
            backhaul_dr = 1.0
            filtered_events = 0
            heartbeats_suppressed = 0
            quarantine_suppressed = 0
            heartbeats_forwarded  = 0
        else:
            bhl = (backhaul_link.stats if backhaul_link is not None else es.get("link_stats", {}))
            if isinstance(bhl, dict):
                e2c_msgs = bhl.get("sent", 0)
                e2c_bytes = bhl.get("total_bytes_sent", 0)
                e2c_dropped = bhl.get("dropped", 0)
                bhl_sent = bhl.get("sent", 0)
                bhl_recv = bhl.get("received", 0)
            else:
                e2c_msgs = bhl.sent
                e2c_bytes = bhl.total_bytes_sent
                e2c_dropped = bhl.dropped
                bhl_sent = bhl.sent
                bhl_recv = bhl.received

            backhaul_dr = bhl_recv / bhl_sent if bhl_sent > 0 else 1.0
            filtered_events = es.get("filtered", 0)
            heartbeats_suppressed = es.get("heartbeats_suppressed", 0)
            quarantine_suppressed = es.get("quarantine_suppressed", 0)
            heartbeats_forwarded = es.get("heartbeats_forwarded", 0)

        e2e_link_dr = s2e_dr * backhaul_dr
        cloud_transitions_eff = cloud_transitions or cloud_events
        cloud_reflection_ratio = (min(cloud_transitions_eff / state_changes_generated, 1.0) if state_changes_generated > 0 else 1.0)
        s2e_received = ls.received

        if arch == "cloud_only":
            aggregation_ratio = 0.0  
            message_reduction_ratio = 0.0
            events_per_cloud_message = 0.0
        elif arch == "edge_aggregated":
            aggregation_ratio = e2c_msgs / s2e_received if s2e_received > 0 else 1.0
            message_reduction_ratio = max(0.0, 1.0 - aggregation_ratio)
            events_per_cloud_message = (cloud_events / e2c_msgs if e2c_msgs > 0 else 0.0)
        else:
            aggregation_ratio = 0.0
            message_reduction_ratio = max(0.0, 1.0 - (e2c_msgs / s2e_received if s2e_received > 0 else 1.0))
            events_per_cloud_message = 1.0

        return ExperimentMetrics(
            scenario_name=cfg.name,
            protocol=cfg.protocol,
            architecture=cfg.architecture,
            traffic_level=cfg.traffic_level,
            num_spots=cfg.num_spots,
            sim_duration_s=cfg.sim_duration_s,

            latency_mean_ms=round(lat_mean, 2),
            latency_p50_ms=round(lat_p50, 2),
            latency_p95_ms=round(lat_p95, 2),
            latency_p99_ms=round(lat_p99, 2),
            latency_min_ms=round(lat_min, 2),
            latency_max_ms=round(lat_max, 2),

            events_generated=sensor_events,
            valid_state_changes=state_changes_generated,
            initial_snapshots_generated=initial_snapshots,
            heartbeats_generated=heartbeats_gen,
            duplicate_sends_generated=dup_sends,
            heartbeat_interval_s=cfg.traffic.heartbeat_interval_s,

            sensor_to_edge_msgs=s2e_msgs,
            sensor_link_dropped=s2e_dropped,
            sensor_to_edge_delivery_ratio=round(s2e_dr, 4),
            sensor_to_edge_bytes=s2e_bytes,

            filtered_events=filtered_events,
            heartbeats_suppressed=heartbeats_suppressed,
            quarantine_suppressed=quarantine_suppressed,
            heartbeats_forwarded=heartbeats_forwarded,

            edge_to_cloud_msgs=e2c_msgs,
            edge_to_cloud_bytes=e2c_bytes,
            edge_to_cloud_dropped=e2c_dropped,
            backhaul_delivery_ratio=round(backhaul_dr, 4),

            retransmissions_total=retransmits,
            duplicate_deliveries=dup_deliveries,
            protocol_bytes=protocol_bytes,

            aggregation_ratio=round(aggregation_ratio, 4),
            message_reduction_ratio=round(message_reduction_ratio, 4),
            events_per_cloud_message=round(events_per_cloud_message, 2),

            events_reflected_in_cloud=cloud_events,
            cloud_reflection_ratio=round(cloud_reflection_ratio, 4),
            physical_delivery_ratio=round(e2e_link_dr, 4), 

            anomalies_detected=es.get("anomalies", 0),
            anomalies_resolved=es.get("resolved_anomalies", 0),
            active_anomalies=es.get("active_anomalies", 0),
            adaptive_mode_switches=es.get("mode_switches", 0),
            quarantined_spots_final=es.get("quarantined_count", 0),
            anomaly_detected_spots=es.get("detected_spots", 0),

            broker_overhead_score=cloud.compute_broker_overhead_score(),

            latency_samples=post_samples[-50_000:],
            scenario_log=es.get("event_log", [])
        )

    async def _collect_metrics_real(self, cfg, sensors, link, edge_proc, cloud_proc, backend, retransmits: int = 0, dup_deliveries: int = 0) -> ExperimentMetrics:
        all_data = cloud_proc.get_all_data()
        post_samples: list[float] = all_data.get("samples", [])
        cloud_snapshot: dict = all_data.get("snapshot", {})

        lat_mean, lat_p50, lat_p95, lat_p99, lat_min, lat_max = _stats(post_samples)

        sensor_events = sensors.total_generated
        state_changes_generated = sensors.state_changes_generated
        initial_snapshots = sensors.initial_snapshots_generated
        heartbeats_gen = sensors.heartbeats_generated
        dup_sends = sensors.duplicate_sends_generated

        cloud_events = all_data.get("received_events", cloud_proc.received_events)
        cloud_transitions = (cloud_snapshot.get("transitions_received") or all_data.get("transitions_received") or cloud_events)
        ls = link.stats
        es = edge_proc.summary()
        arch = cfg.architecture

        s2e_msgs = ls.sent
        s2e_bytes = ls.total_bytes_sent
        s2e_dr = ls.delivery_ratio
        s2e_dropped = ls.dropped

        if arch == "cloud_only":
            e2c_msgs = 0
            e2c_bytes = 0
            e2c_dropped = 0
            backhaul_dr = 1.0
            filtered_events = 0
            heartbeats_suppressed = 0
            quarantine_suppressed = 0
            heartbeats_forwarded = 0
        else:
            bhl = es.get("link_stats", {})
            e2c_msgs = bhl.get("sent", 0)
            e2c_bytes = bhl.get("total_bytes_sent", 0)
            e2c_dropped = bhl.get("dropped", 0)
            bhl_sent = bhl.get("sent", 0)
            bhl_recv = bhl.get("received", 0)
            backhaul_dr = bhl_recv / bhl_sent if bhl_sent > 0 else 1.0
            filtered_events = es.get("filtered", 0)
            heartbeats_suppressed = es.get("heartbeats_suppressed", 0)
            quarantine_suppressed = es.get("quarantine_suppressed", 0)
            heartbeats_forwarded = es.get("heartbeats_forwarded", 0)

        e2e_link_dr = s2e_dr * backhaul_dr

        cloud_transitions_eff = cloud_transitions or cloud_events
        cloud_reflection_ratio = (min(cloud_transitions_eff / state_changes_generated, 1.0) if state_changes_generated > 0 else 1.0)

        s2e_received = ls.received
        if arch == "cloud_only":
            aggregation_ratio       = 0.0
            message_reduction_ratio = 0.0
            events_per_cloud_message = 0.0
        elif arch == "edge_aggregated":
            aggregation_ratio = e2c_msgs / s2e_received if s2e_received > 0 else 1.0
            message_reduction_ratio = max(0.0, 1.0 - aggregation_ratio)
            events_per_cloud_message = cloud_events / e2c_msgs if e2c_msgs > 0 else 0.0
        else:
            aggregation_ratio = 0.0
            message_reduction_ratio = max(0.0, 1.0 - (e2c_msgs / s2e_received if s2e_received > 0 else 1.0))
            events_per_cloud_message = 1.0

        return ExperimentMetrics(
            scenario_name=cfg.name,
            protocol=cfg.protocol,
            architecture=cfg.architecture,
            traffic_level=cfg.traffic_level,
            num_spots=cfg.num_spots,
            sim_duration_s=cfg.sim_duration_s,

            latency_mean_ms=round(lat_mean, 2),
            latency_p50_ms=round(lat_p50, 2),
            latency_p95_ms=round(lat_p95, 2),
            latency_p99_ms=round(lat_p99, 2),
            latency_min_ms=round(lat_min, 2),
            latency_max_ms=round(lat_max, 2),

            events_generated=sensor_events,
            valid_state_changes=state_changes_generated,
            initial_snapshots_generated=initial_snapshots,
            heartbeats_generated=heartbeats_gen,
            duplicate_sends_generated=dup_sends,
            heartbeat_interval_s=cfg.traffic.heartbeat_interval_s,

            sensor_to_edge_msgs=s2e_msgs,
            sensor_link_dropped=s2e_dropped,
            sensor_to_edge_delivery_ratio=round(s2e_dr, 4),
            sensor_to_edge_bytes=s2e_bytes,

            filtered_events=filtered_events,
            heartbeats_suppressed=heartbeats_suppressed,
            quarantine_suppressed=quarantine_suppressed,
            heartbeats_forwarded=heartbeats_forwarded,

            edge_to_cloud_msgs=e2c_msgs,
            edge_to_cloud_bytes=e2c_bytes,
            edge_to_cloud_dropped=e2c_dropped,
            backhaul_delivery_ratio=round(backhaul_dr, 4),

            retransmissions_total=retransmits,
            duplicate_deliveries=dup_deliveries,
            protocol_bytes=backend.bytes_sent,

            aggregation_ratio=round(aggregation_ratio, 4),
            message_reduction_ratio=round(message_reduction_ratio, 4),
            events_per_cloud_message=round(events_per_cloud_message, 2),

            events_reflected_in_cloud=cloud_events,
            cloud_reflection_ratio=round(cloud_reflection_ratio, 4),

            physical_delivery_ratio=round(e2e_link_dr, 4),

            anomalies_detected=es.get("anomalies", 0),
            anomalies_resolved=es.get("resolved_anomalies", 0),
            active_anomalies=es.get("active_anomalies", 0),
            adaptive_mode_switches=es.get("mode_switches", 0),
            quarantined_spots_final=es.get("quarantined_count", 0),
            anomaly_detected_spots=es.get("detected_spots", 0),

            broker_overhead_score=cloud_proc.compute_broker_overhead_score(),

            latency_samples=post_samples[-50_000:],
            scenario_log=es.get("event_log", [])
        )

    def _log_done(self, cfg, metrics: ExperimentMetrics, cloud_events: int) -> None:
        arch = cfg.architecture
        m = metrics
        if arch == "cloud_only":
            logger.info(
                f"[{cfg.name}] Done (cloud_only). "
                f"sensor_sent={m.sensor_to_edge_msgs}  "
                f"sensor_dropped={m.sensor_link_dropped}  "
                f"wire_delivered={m.sensor_to_edge_msgs - m.sensor_link_dropped}  "
                f"cloud_raw={cloud_events}  "
                f"cloud_unique={cloud_events - m.duplicate_deliveries}  "
                f"cloud_coverage={m.cloud_reflection_ratio:.1%}  "
                f"lat={m.latency_mean_ms:.1f}ms  p99={m.latency_p99_ms:.1f}ms"
            )
        else:
            logger.info(
                f"[{cfg.name}] Done ({arch}). "
                f"sensor_sent={m.sensor_to_edge_msgs}  "
                f"filtered={m.filtered_events}  "
                f"  of which hb_suppressed={m.heartbeats_suppressed}"
                f"  quar_suppressed={m.quarantine_suppressed}  "
                f"forwarded={m.edge_to_cloud_msgs}  "
                f"cloud_raw={cloud_events}  "
                f"cloud_unique={cloud_events - m.duplicate_deliveries}  "
                f"backhaul_dropped={m.edge_to_cloud_dropped}  "
                f"cloud_coverage={m.cloud_reflection_ratio:.1%}  "
                f"msg_reduction={m.message_reduction_ratio:.1%}  "
                f"anomalies={m.anomalies_detected}  "
                f"quarantined={m.quarantined_spots_final}  "
                f"mode_switches={m.adaptive_mode_switches}  "
                f"lat={m.latency_mean_ms:.1f}ms  p99={m.latency_p99_ms:.1f}ms"
            )


def _make_simulated_backend(cfg, clock, cloud_recv, seed):
    from simulator.protocols.mqtt_client import SimulatedMQTTBackend
    from simulator.protocols.amqp_client import SimulatedAMQPBackend
    from simulator.protocols.coap_client import SimulatedCoAPBackend

    if cfg.architecture == "cloud_only":
        proto_loss = cfg.link.packet_loss_rate
        ack_one_way = cfg.link.base_delay_ms / 1000.0
        ack_jitter = cfg.link.jitter_ms / 1000.0
    else:
        proto_loss = cfg.backhaul_link.packet_loss_rate
        ack_one_way = cfg.backhaul_link.base_delay_ms / 1000.0
        ack_jitter = cfg.backhaul_link.jitter_ms / 1000.0

    proto = cfg.protocol
    if proto == "mqtt":
        return SimulatedMQTTBackend(cfg.mqtt, clock, cloud_recv, proto_loss, seed + 2, ack_one_way, ack_jitter)
    elif proto == "amqp":
        return SimulatedAMQPBackend(cfg.amqp, clock, cloud_recv, proto_loss, seed + 2, ack_one_way, ack_jitter)
    elif proto == "coap":
        return SimulatedCoAPBackend(cfg.coap, clock, cloud_recv, proto_loss, seed + 2, ack_one_way, ack_jitter)
    raise ValueError(f"Unknown protocol: {proto}")