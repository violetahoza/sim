from __future__ import annotations
import asyncio
import json
import logging
import random
import time
from pathlib import Path
import numpy as np

from simulator.config import ScenarioConfig
from simulator.models import BatchUpdate, ExperimentMetrics, ParkingEvent, SpotState
from simulator.des.engine import SimClock
from simulator.protocols.broker_config import use_real_brokers
from simulator.sensors.sensor_emulator import SensorEmulator
from simulator.link.link_emulator import LinkEmulator
from simulator.edge.edge_node import EdgeNode
from simulator.cloud.cloud_backend import CloudBackend
from simulator.db import make_engine, init_schema
from simulator.edge.edge_process import EdgeWorkerProcess
from simulator.cloud.cloud_process import CloudWorkerProcess


logger = logging.getLogger(__name__)

_SF7_TX_J: float = 0.030      
_AA_BATTERY_J: float = 12_960.0

_PROTOCOL_SEED_OFFSET = {"mqtt": 1000, "coap": 2000, "amqp": 3000}


def _protocol_seed_offset(protocol: str) -> int:
    return _PROTOCOL_SEED_OFFSET.get(protocol, 9000)


class ExperimentRunner:
    def __init__(self, config: ScenarioConfig, progress_cb=None, flush_cb=None, fault_injector=None) -> None:
        self.config = config
        self.progress_cb = progress_cb
        self.flush_cb = flush_cb
        self._start_time: float = 0.0
        self._cancelled: bool = False
        self._spot_states: dict[int, str] = {}
        self._fault_injector = fault_injector
        self._edge_summary: dict = {} 

    def cancel(self) -> None:
        self._cancelled = True

    async def run(self) -> ExperimentMetrics:
        if use_real_brokers():
            return await self._run_real()
        return await self._run_simulated()

    async def _run_simulated(self) -> ExperimentMetrics:
        cfg = self.config
        self._start_time = time.time()
        epoch = self._start_time
        seed = cfg.random_seed

        engine = make_engine()
        if engine is not None:
            init_schema(engine)

        clock = SimClock()

        sensors = SensorEmulator(cfg.traffic, cfg.arrival_rate)
        if self._fault_injector is not None:
            sensors.set_fault_injector(self._fault_injector)

        cloud = CloudBackend(cfg, clock, epoch)

        config_json = json.dumps(cfg.to_save_dict())
        cloud.open_run(engine, config_json=config_json)

        def cloud_recv(batch: BatchUpdate, raw: bytes) -> None:
            cloud.receive_batch(batch, raw)

        backend = _make_simulated_backend(cfg, clock, cloud_recv, seed)
        arch = cfg.architecture

        _backhaul_rng = random.Random(seed + _protocol_seed_offset(cfg.protocol) + 5000)
        backhaul_link = LinkEmulator(cfg.backhaul_link.to_link_config(), clock, rng=_backhaul_rng)
        backhaul_link.stats = backhaul_link.stats.__class__(name="edge_to_cloud_backhaul")
        backhaul_link.set_batch_callback(backend.publish)

        def edge_to_cloud(batch: BatchUpdate, raw: bytes) -> None:
            backhaul_link.transmit_batch(batch, raw)

        edge = EdgeNode(cfg, clock, edge_to_cloud, epoch)

        if arch != "cloud_only":
            backhaul_link.on_drop = lambda: edge.record_cloud_drop()
            backend.on_drop = lambda: edge.record_cloud_drop()

        for i in range(cfg.num_spots):
            self._spot_states[i] = "free"

        _sensor_rng = random.Random(seed + _protocol_seed_offset(cfg.protocol))

        if arch == "cloud_only":
            def _sensor_link_cb(event: ParkingEvent, raw: bytes) -> None:
                batch = BatchUpdate(edge_id="direct", events=[event])
                payload = EdgeNode._serialize_batch(batch)
                backhaul_link.transmit_batch(batch, payload)
        else:
            def _sensor_link_cb(event: ParkingEvent, raw: bytes) -> None:
                edge.receive(event, raw)

        link = LinkEmulator(cfg.link, clock, forward_cb=_sensor_link_cb, rng=_sensor_rng)

        if arch != "cloud_only":
            edge.set_sensor_link_stats(link.stats)

        def sensor_cb(event: ParkingEvent) -> None:
            if not self._cancelled:
                self._spot_states[event.spot_id] = (
                    event.state.value
                    if isinstance(event.state, SpotState)
                    else str(event.state)
                )
                link.transmit(event)

        sensors.add_callback(sensor_cb)
        sensors.schedule_run(clock, cfg.sim_duration_s, epoch)

        logger.info(f"[{cfg.name}] DES simulated — {cfg.sim_duration_s:.0f} s virtual …")

        def des_progress(virtual_now: float, end_time: float) -> None:
            if self.progress_cb is None:
                return
            snap = {
                "elapsed_s": round(time.time() - self._start_time, 1),
                "wall_duration_s": round(end_time / cfg.traffic.time_scale, 1),
                "simulated_elapsed_s": round(virtual_now, 0),
                "simulated_duration_s": end_time,
                "time_scale": cfg.traffic.time_scale,
                "sim_duration_s": cfg.sim_duration_s,
                "progress_pct": min(100, round(virtual_now / end_time * 100, 1)),
                "generated": sensors.total_generated,
                "cloud_events": cloud.received_events,
                "occupancy": sensors.occupancy_snapshot(),
                "spot_states": dict(self._spot_states),
                "edge": edge.summary(include_cpu=False)
            }
            self.progress_cb(snap)

        await clock.run_until_async(cfg.sim_duration_s, progress_cb=des_progress, cancelled_cb=lambda: self._cancelled)
        edge.flush_final()

        retransmits = getattr(backend, "retransmitted", 0)
        dup_deliveries = getattr(backend, "duplicates_delivered", 0) or getattr(backend, "duplicates_suppressed", 0)
        protocol_bytes = backend.bytes_sent

        self._edge_summary = edge.summary(include_cpu=False)
        metrics = self._collect_metrics_simulated(cfg, sensors, link, edge, cloud, protocol_bytes, retransmits=retransmits, dup_deliveries=dup_deliveries)
        self._log_done(cfg, metrics, cloud)

        if self.flush_cb:
            self.flush_cb()

        cloud.flush_to_db(engine, metrics)
        return metrics

    def _collect_metrics_simulated(self, cfg, sensors, link, edge, cloud, protocol_bytes: int = 0, retransmits: int = 0, dup_deliveries: int = 0) -> ExperimentMetrics:
        post_samples = cloud.get_all_latency_samples()
        lat_mean, lat_p50, lat_p95, lat_p99, lat_min, lat_max = _stats(post_samples)

        sensor_events = sensors.total_generated
        cloud_events = cloud.received_events
        ls = link.stats
        es = self._edge_summary if self._edge_summary else edge.summary()
        cs = cloud.get_metrics_snapshot(include_cpu=False)
        arch = cfg.architecture

        if arch == "cloud_only":
            s2e_msgs, s2e_bytes = ls.sent, ls.total_bytes_sent
            e2c_msgs, e2c_bytes = 0, 0
            s2e_dr, e2c_dr = ls.delivery_ratio, 1.0
        else:
            s2e_msgs, s2e_bytes = ls.sent, ls.total_bytes_sent
            e2c_msgs = es.get("link_stats", {}).get("sent", 0)
            e2c_bytes = es.get("link_stats", {}).get("total_bytes_sent", 0)
            s2e_dr = ls.delivery_ratio
            forwarded = es.get("forwarded_events", 0)
            e2c_dr = (cloud_events / forwarded) if forwarded > 0 else 1.0

        filtered_events = es.get("filtered", 0)
        valid_state_changes = max(sensor_events - filtered_events, 0)
        events_reflected = cloud_events

        physical_delivery_ratio = s2e_dr * e2c_dr
        cloud_reflection_ratio = (events_reflected / valid_state_changes) if valid_state_changes > 0 else 1.0
        cloud_reflection_ratio = min(cloud_reflection_ratio, 1.0)

        cloud_msgs = e2c_msgs if arch != "cloud_only" else cloud_events
        agg_ratio = (cloud_msgs / sensor_events) if sensor_events > 0 else 1.0
        message_reduction_ratio = (1.0 - agg_ratio) if sensor_events > 0 else 0.0
        events_per_cloud_message = (events_reflected / cloud_msgs) if cloud_msgs > 0 else 0.0

        transport_total = s2e_msgs + retransmits
        e2c_dropped = es.get("link_stats", {}).get("dropped", 0)

        tx_per_sensor = s2e_msgs / max(cfg.num_spots, 1)
        energy_per_sensor_mj = tx_per_sensor * _SF7_TX_J * 1000.0  
        tx_rate_hz = tx_per_sensor / max(cfg.sim_duration_s, 1.0)  
        max_tx = _AA_BATTERY_J / _SF7_TX_J 
        battery_life_days = max_tx / max(tx_rate_hz * 86_400.0, 1e-12)

        fi = self._fault_injector
        return ExperimentMetrics(
            scenario_name=cfg.name,
            group=cfg.group,
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

            sensor_to_edge_msgs=s2e_msgs,
            edge_to_cloud_msgs=e2c_msgs,
            cloud_only_msgs=cloud_events if arch == "cloud_only" else 0,
            transport_msgs_total=transport_total,
            retransmissions_total=retransmits,
            duplicate_deliveries=dup_deliveries,

            sensor_to_edge_bytes=s2e_bytes,
            edge_to_cloud_bytes=e2c_bytes,
            protocol_bytes=protocol_bytes,

            sensor_to_edge_delivery_ratio=round(s2e_dr, 4),
            edge_to_cloud_delivery_ratio=round(e2c_dr, 4),
            end_to_end_delivery_ratio=round(cloud_reflection_ratio, 4),
            physical_delivery_ratio=round(physical_delivery_ratio, 4),
            cloud_reflection_ratio=round(cloud_reflection_ratio, 4),
            message_reduction_ratio=round(message_reduction_ratio, 4),
            events_per_cloud_message=round(events_per_cloud_message, 2),
            valid_state_changes=valid_state_changes,
            events_reflected_in_cloud=events_reflected,

            aggregation_ratio=round(agg_ratio, 4),
            filtered_events=filtered_events,
            anomalies_detected=es.get("anomalies", 0),
            anomalies_resolved=es.get("resolved_anomalies", 0),
            active_anomalies=es.get("active_anomalies", 0),
            adaptive_mode_switches=es.get("mode_switches", 0),
            edge_to_cloud_dropped=e2c_dropped,

            measurement_mode="simulated",
            edge_cpu_pct=-1.0,
            edge_mem_mb=es.get("mem_mb", 0.0),
            cloud_cpu_pct=-1.0,
            cloud_mem_mb=cs.get("mem_mb", 0.0),

            broker_overhead_score=cloud.compute_broker_overhead_score(),
            energy_per_sensor_mj=round(energy_per_sensor_mj, 3),
            battery_life_days=round(battery_life_days, 1),
            warmup_excluded_samples=cloud.warmup_excluded,

            fault_injected_count=fi.injected_count if fi is not None else 0,
            quarantined_spots_peak=es.get("quarantined_count", 0),
            anomaly_detected_spots=es.get("detected_spots", 0),
            
            events_generated=sensor_events,
            sensor_link_dropped=ls.dropped,

            latency_samples=post_samples[-50_000:]
        )

    async def _run_real(self) -> ExperimentMetrics:
        cfg = self.config
        self._start_time = time.time()
        epoch = self._start_time

        db_url_env = _db_url_from_env()
        cloud_proc = CloudWorkerProcess(cfg, epoch, db_url=db_url_env)
        cloud_proc.start()

        def cloud_recv(batch: BatchUpdate, raw: bytes) -> None:
            cloud_proc.receive_batch(batch, raw)

        backend = await _make_real_backend(cfg, cloud_recv)

        def on_batch_from_edge(batch: BatchUpdate, raw: bytes) -> None:
            backend.publish(batch, raw)

        edge_proc = EdgeWorkerProcess(cfg, epoch, on_batch=on_batch_from_edge)
        edge_proc.start()

        clock = SimClock()
        sensors = SensorEmulator(cfg.traffic, cfg.arrival_rate, wall_clock=True)
        arch = cfg.architecture

        def link_cb(event: ParkingEvent, raw: bytes) -> None:
            if arch == "cloud_only":
                batch = BatchUpdate(edge_id="direct", events=[event])
                backend.publish(batch, raw)
            else:
                edge_proc.receive(event, raw)
                edge_proc.drain()

        for i in range(cfg.num_spots):
            self._spot_states[i] = "free"

        _sensor_rng = random.Random(cfg.random_seed + _protocol_seed_offset(cfg.protocol))
        link = LinkEmulator(cfg.link, clock, forward_cb=link_cb, rng=_sensor_rng, wall_clock=True)

        def sensor_cb(event: ParkingEvent) -> None:
            if not self._cancelled:
                self._spot_states[event.spot_id] = (
                    event.state.value
                    if isinstance(event.state, SpotState)
                    else str(event.state)
                )
                link.transmit(event)

        sensors.add_callback(sensor_cb)
        sensors.schedule_run(clock, cfg.sim_duration_s, epoch)

        logger.info(f"[{cfg.name}] DES real-broker — {cfg.sim_duration_s:.0f} s virtual …")

        _snapshot_interval = max(1, int(cfg.sim_duration_s / 50))

        def des_progress(virtual_now: float, end_time: float) -> None:
            if int(virtual_now) % _snapshot_interval == 0:
                edge_proc.request_snapshot()
                cloud_proc.request_snapshot()
                edge_proc.drain()
            if self.progress_cb is None:
                return
            edge_summary = edge_proc.summary()
            snap = {
                "elapsed_s": round(time.time() - self._start_time, 1),
                "wall_duration_s": round(end_time / cfg.traffic.time_scale, 1),
                "simulated_elapsed_s": round(virtual_now, 0),
                "simulated_duration_s": end_time,
                "time_scale": cfg.traffic.time_scale,
                "sim_duration_s": cfg.sim_duration_s,
                "progress_pct": min(100, round(virtual_now / end_time * 100, 1)),
                "generated": sensors.total_generated,
                "cloud_events": cloud_proc.received_events,
                "occupancy": sensors.occupancy_snapshot(),
                "spot_states": dict(self._spot_states),
                "edge": edge_summary
            }
            self.progress_cb(snap)

        await clock.run_until_async(cfg.sim_duration_s, progress_cb=des_progress, cancelled_cb=lambda: self._cancelled, real_mode=True, time_scale=cfg.traffic.time_scale)

        edge_proc.flush_final()
        edge_proc.request_snapshot()
        edge_proc.drain()
        await asyncio.sleep(1.0)

        retransmits = getattr(backend, "retransmitted", 0)
        dup_deliveries = getattr(backend, "duplicates_delivered", 0) or 0
        metrics = await self._collect_metrics_real(cfg, sensors, link, edge_proc, cloud_proc, backend, retransmits=retransmits, dup_deliveries=dup_deliveries)
        self._log_done(cfg, metrics, None, cloud_events=cloud_proc.received_events)

        if self.flush_cb:
            self.flush_cb()

        cloud_proc.flush_to_db(metrics.to_dict())

        await backend.stop()
        edge_proc.stop()
        cloud_proc.stop()

        return metrics

    async def _collect_metrics_real(self, cfg, sensors, link, edge_proc, cloud_proc, backend, retransmits: int = 0, dup_deliveries: int = 0) -> ExperimentMetrics:
        all_data = cloud_proc.get_all_data()
        post_samples: list[float] = all_data.get("samples", [])
        cloud_snapshot: dict = all_data.get("snapshot", {})

        lat_mean, lat_p50, lat_p95, lat_p99, lat_min, lat_max = _stats(post_samples)

        sensor_events = sensors.total_generated
        cloud_events = all_data.get("received_events", cloud_proc.received_events)
        ls = link.stats
        es = edge_proc.summary()
        arch = cfg.architecture

        if arch == "cloud_only":
            s2e_msgs, s2e_bytes = ls.sent, ls.total_bytes_sent
            e2c_msgs, e2c_bytes = 0, 0
            s2e_dr, e2c_dr = ls.delivery_ratio, 1.0
        else:
            s2e_msgs, s2e_bytes = ls.sent, ls.total_bytes_sent
            e2c_msgs = es.get("link_stats", {}).get("sent", 0)
            e2c_bytes = es.get("link_stats", {}).get("total_bytes_sent", 0)
            s2e_dr = ls.delivery_ratio
            forwarded = es.get("forwarded_events", 0)
            e2c_dr = (cloud_events / forwarded) if forwarded > 0 else 1.0

        filtered_events = es.get("filtered", 0)
        valid_state_changes = max(sensor_events - filtered_events, 0)
        events_reflected = cloud_events

        physical_delivery_ratio = s2e_dr * e2c_dr
        cloud_reflection_ratio = (events_reflected / valid_state_changes) if valid_state_changes > 0 else 1.0
        cloud_reflection_ratio = min(cloud_reflection_ratio, 1.0)

        cloud_msgs = e2c_msgs if arch != "cloud_only" else cloud_events
        agg_ratio = (cloud_msgs / sensor_events) if sensor_events > 0 else 1.0
        message_reduction_ratio = (1.0 - agg_ratio) if sensor_events > 0 else 0.0
        events_per_cloud_message = (events_reflected / cloud_msgs) if cloud_msgs > 0 else 0.0
        e2c_dropped = es.get("link_stats", {}).get("dropped", 0)

        return ExperimentMetrics(
            scenario_name=cfg.name,
            group=cfg.group,
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

            sensor_to_edge_msgs=s2e_msgs,
            edge_to_cloud_msgs=e2c_msgs,
            cloud_only_msgs=cloud_events if arch == "cloud_only" else 0,
            transport_msgs_total=s2e_msgs + retransmits,
            retransmissions_total=retransmits,
            duplicate_deliveries=dup_deliveries,

            sensor_to_edge_bytes=s2e_bytes,
            edge_to_cloud_bytes=e2c_bytes,
            protocol_bytes=backend.bytes_sent,

            sensor_to_edge_delivery_ratio=round(s2e_dr, 4),
            edge_to_cloud_delivery_ratio=round(e2c_dr, 4),
            end_to_end_delivery_ratio=round(cloud_reflection_ratio, 4),
            physical_delivery_ratio=round(physical_delivery_ratio, 4),
            cloud_reflection_ratio=round(cloud_reflection_ratio, 4),
            message_reduction_ratio=round(message_reduction_ratio, 4),
            events_per_cloud_message=round(events_per_cloud_message, 2),
            valid_state_changes=valid_state_changes,
            events_reflected_in_cloud=events_reflected,

            aggregation_ratio=round(agg_ratio, 4),
            filtered_events=filtered_events,
            anomalies_detected=es.get("anomalies", 0),
            anomalies_resolved=es.get("resolved_anomalies", 0),
            active_anomalies=es.get("active_anomalies", 0),
            adaptive_mode_switches=es.get("mode_switches", 0),
            edge_to_cloud_dropped=e2c_dropped,

            measurement_mode="real",
            edge_cpu_pct=edge_proc.cpu_pct,
            edge_mem_mb=edge_proc.mem_mb,
            cloud_cpu_pct=cloud_snapshot.get("cpu_pct", 0.0),
            cloud_mem_mb=cloud_snapshot.get("mem_mb", 0.0),

            broker_overhead_score=cloud_proc.compute_broker_overhead_score(),
            latency_samples=post_samples[-50_000:]
        )

    def _log_done(self, cfg, metrics, cloud=None, cloud_events: int = 0) -> None:
        ev = cloud.received_events if cloud else cloud_events
        logger.info(
            f"[{cfg.name}] Done. "
            f"logical={metrics.sensor_to_edge_msgs}  "
            f"transport={metrics.transport_msgs_total}  "
            f"retransmits={metrics.retransmissions_total}  "
            f"cloud={ev}  "
            f"lat={metrics.latency_mean_ms:.1f}ms  "
            f"p99={metrics.latency_p99_ms:.1f}ms  "
            f"cloud_reflection={metrics.cloud_reflection_ratio:.1%}  "
            f"msg_reduction={metrics.message_reduction_ratio:.1%}  "
            f"filtered={metrics.filtered_events}  "
            f"e2c_dropped={metrics.edge_to_cloud_dropped}"
        )


def _make_simulated_backend(cfg, clock, cloud_recv, seed):
    from simulator.protocols.mqtt_client import SimulatedMQTTBackend
    from simulator.protocols.amqp_client import SimulatedAMQPBackend
    from simulator.protocols.coap_client import SimulatedCoAPBackend

    proto = cfg.protocol
    if proto == "mqtt":
        return SimulatedMQTTBackend(cfg.mqtt, clock, cloud_recv, cfg.backhaul_link.packet_loss_rate, seed + 2)
    elif proto == "amqp":
        return SimulatedAMQPBackend(cfg.amqp, clock, cloud_recv, cfg.backhaul_link.packet_loss_rate, seed + 2)
    elif proto == "coap":
        return SimulatedCoAPBackend(cfg.coap, clock, cloud_recv, cfg.backhaul_link.packet_loss_rate, seed + 2)
    raise ValueError(f"Unknown protocol: {proto}")


async def _make_real_backend(cfg, cloud_recv):
    from simulator.protocols.mqtt_client import RealMQTTBackend
    from simulator.protocols.amqp_client import RealAMQPBackend
    from simulator.protocols.coap_client import RealCoAPBackend
    from simulator.protocols.broker_config import MQTTBrokerConfig, AMQPBrokerConfig, CoAPBrokerConfig

    proto = cfg.protocol
    name  = cfg.name

    if proto == "mqtt":
        backend = RealMQTTBackend(cfg.mqtt, MQTTBrokerConfig.from_env(), cloud_recv, scenario_name=name)
    elif proto == "amqp":
        backend = RealAMQPBackend(cfg.amqp, AMQPBrokerConfig.from_env(), cloud_recv, scenario_name=name)
    elif proto == "coap":
        backend = RealCoAPBackend(cfg.coap, CoAPBrokerConfig.from_env(), cloud_recv, scenario_name=name)
    else:
        raise ValueError(f"Unknown protocol: {proto}")

    await backend.start()
    return backend


def _db_url_from_env() -> str | None:
    from simulator.utils import read_env_file
    return read_env_file().get("DB_URL") or None


def _stats(samples):
    if not samples:
        return (0.0,) * 6
    arr = np.array(samples)
    return (float(np.mean(arr)), float(np.percentile(arr, 50)), float(np.percentile(arr, 95)), float(np.percentile(arr, 99)), float(np.min(arr)), float(np.max(arr)))


def save_results(metrics: ExperimentMetrics, output_dir: str = "results") -> str:
    Path(output_dir).mkdir(exist_ok=True)
    path = Path(output_dir) / f"{metrics.scenario_name}.json"
    data = metrics.to_dict()
    data["latency_samples"] = metrics.latency_samples
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Results saved to {path}")
    return str(path)
