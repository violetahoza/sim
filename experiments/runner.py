from __future__ import annotations
import asyncio
import json
import logging
import random
import time
from pathlib import Path
from typing import Callable, Optional

import numpy as np

from simulator.config import ScenarioConfig
from simulator.models import BatchUpdate, ParkingEvent, ExperimentMetrics, SpotState
from simulator.des.engine import SimClock
from simulator.sensors.sensor_emulator import SensorEmulator
from simulator.link.link_emulator import LinkEmulator
from simulator.edge.edge_node import EdgeNode
from simulator.cloud.cloud_backend import CloudBackend
from simulator.protocols.mqtt_client import SimulatedMQTTBackend
from simulator.protocols.amqp_client import SimulatedAMQPBackend
from simulator.protocols.coap_client import SimulatedCoAPBackend
from simulator.db import make_engine, init_schema

logger = logging.getLogger(__name__)


class ExperimentRunner:
    def __init__(
        self,
        config: ScenarioConfig,
        progress_cb=None,
        flush_cb=None,
    ) -> None:
        self.config = config
        self.progress_cb = progress_cb
        self.flush_cb = flush_cb
        self._start_time: float = 0.0
        self._cancelled = False
        self._cancel_event = asyncio.Event()
        self._spot_states: dict[int, str] = {}

    def cancel(self) -> None:
        self._cancelled = True
        self._cancel_event.set()

    async def run(self) -> ExperimentMetrics:
        cfg = self.config
        self._start_time = time.time()
        epoch = self._start_time

        engine = make_engine()
        if engine is not None:
            init_schema(engine)

        clock = SimClock()
        sensors = SensorEmulator(cfg.traffic, cfg.arrival_rate)
        cloud = CloudBackend(cfg, clock, epoch)

        config_json = json.dumps(cfg.to_save_dict())
        cloud.open_run(engine, config_json=config_json)

        seed = cfg.random_seed
        proto = cfg.protocol

        def cloud_recv(batch: BatchUpdate, raw: bytes) -> None:
            cloud.receive_batch(batch, raw)

        if proto == "mqtt":
            backend = SimulatedMQTTBackend(cfg.mqtt, clock, cloud_recv, cfg.link.packet_loss_rate, seed + 2)
        elif proto == "amqp":
            backend = SimulatedAMQPBackend(cfg.amqp, clock, cloud_recv, cfg.link.packet_loss_rate, seed + 2)
        elif proto == "coap":
            backend = SimulatedCoAPBackend(cfg.coap, clock, cloud_recv, cfg.link.packet_loss_rate, seed + 2)
        else:
            raise ValueError(f"Unknown protocol: {proto}")

        arch = cfg.architecture

        def proto_publish(batch: BatchUpdate, raw: bytes) -> None:
            backend.publish(batch, raw)

        edge = EdgeNode(cfg, clock, proto_publish, epoch)

        for i in range(cfg.num_spots):
            self._spot_states[i] = "free"

        link = LinkEmulator(
            cfg.link, clock,
            forward_cb=self._make_link_cb(arch, edge, cloud, epoch),
            rng=random.Random(seed + 1),
        )

        edge.set_sensor_link_stats(link.stats)

        def sensor_cb(event: ParkingEvent) -> None:
            if not self._cancelled:
                self._spot_states[event.spot_id] = (
                    event.state.value if isinstance(event.state, SpotState) else str(event.state)
                )
                link.transmit(event)

        sensors.add_callback(sensor_cb)
        sensors.schedule_run(clock, cfg.sim_duration_s, epoch)

        logger.info(f"[{cfg.name}] Starting DES — {cfg.sim_duration_s:.0f} s virtual …")

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
                "edge": edge.summary(),
            }
            self.progress_cb(snap)

        await clock.run_until_async(
            cfg.sim_duration_s,
            progress_cb=des_progress,
            cancelled_cb=lambda: self._cancelled,
        )

        edge.flush_final()

        protocol_bytes = backend.bytes_sent
        metrics = self._collect_metrics(cfg, sensors, link, edge, cloud, protocol_bytes)
        logger.info(
            f"[{cfg.name}] Done. "
            f"events={metrics.sensor_to_edge_msgs}  cloud={cloud.received_events}  "
            f"lat={metrics.latency_mean_ms:.1f} ms  "
            f"warmup_excl={metrics.warmup_events_excluded}  "
            f"e2e_dr={metrics.end_to_end_delivery_ratio:.1%}  "
            f"filtered={metrics.filtered_events}"
        )

        if self.flush_cb:
            self.flush_cb()

        cloud.flush_to_db(engine, metrics)
        return metrics

    def _make_link_cb(self, arch: str, edge: EdgeNode, cloud: CloudBackend, epoch: float):
        if arch == "cloud_only":
            def cb(event: ParkingEvent, raw: bytes) -> None:
                batch = BatchUpdate(edge_id="direct", events=[event])
                cloud.receive_batch(batch, raw)
            return cb
        else:
            def cb(event: ParkingEvent, raw: bytes) -> None:
                edge.receive(event, raw)
            return cb

    def _collect_metrics(self, cfg, sensors, link, edge, cloud, protocol_bytes: int = 0) -> ExperimentMetrics:
        post_samples, all_samples = cloud.get_all_latency_samples()

        def _stats(samples):
            if not samples:
                return (0.0,) * 6
            arr = np.array(samples)
            return (
                float(np.mean(arr)), float(np.percentile(arr, 50)),
                float(np.percentile(arr, 95)), float(np.percentile(arr, 99)),
                float(np.min(arr)), float(np.max(arr)),
            )

        lat_mean, lat_p50, lat_p95, lat_p99, lat_min, lat_max = _stats(post_samples)
        lat_mean_all = float(np.mean(all_samples)) if all_samples else 0.0

        sensor_events = sensors.total_generated
        cloud_events = cloud.received_events
        ls = link.stats
        es = edge.summary()
        cs = cloud.get_metrics_snapshot()
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

        e2e_dr = (cloud_events / sensor_events) if sensor_events > 0 else 1.0
        cloud_msgs = e2c_msgs if arch != "cloud_only" else cloud_events
        agg_ratio = (cloud_msgs / sensor_events) if sensor_events > 0 else 1.0

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

            latency_mean_ms_with_warmup=round(lat_mean_all, 2),
            warmup_s=cfg.edge.warmup_s,
            warmup_events_excluded=cs.get("warmup_excluded", 0),

            sensor_to_edge_msgs=s2e_msgs,
            edge_to_cloud_msgs=e2c_msgs,
            cloud_only_msgs=cloud_events if arch == "cloud_only" else 0,
            sensor_to_edge_bytes=s2e_bytes,
            edge_to_cloud_bytes=e2c_bytes,
            protocol_bytes=protocol_bytes,

            sensor_to_edge_delivery_ratio=round(s2e_dr, 4),
            edge_to_cloud_delivery_ratio=round(e2c_dr, 4),
            end_to_end_delivery_ratio=round(e2e_dr, 4),

            aggregation_ratio=round(agg_ratio, 4),
            filtered_events=es.get("filtered", 0),
            anomalies_detected=es.get("anomalies", 0),
            adaptive_mode_switches=es.get("mode_switches", 0),

            edge_cpu_pct=es.get("cpu_pct", 0.0),
            edge_mem_mb=es.get("mem_mb", 0.0),
            cloud_cpu_pct=cs.get("cpu_pct", 0.0),
            cloud_mem_mb=cs.get("mem_mb", 0.0),

            broker_overhead_score=cloud.compute_broker_overhead_score(),

            latency_timeseries=cloud.get_latency_timeseries(),
            latency_samples=post_samples[-50_000:],
        )


def save_results(metrics: ExperimentMetrics, output_dir: str = "results") -> str:
    Path(output_dir).mkdir(exist_ok=True)
    path = Path(output_dir) / f"{metrics.scenario_name}.json"
    data = metrics.to_dict()
    data["latency_samples"] = metrics.latency_samples
    data["latency_timeseries"] = metrics.latency_timeseries
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Results saved to {path}")
    return str(path)