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
        progress_cb: Optional[Callable[[dict], None]] = None,
    ) -> None:
        self.config = config
        self.progress_cb = progress_cb
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

        cloud.open_run(engine)

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
            cfg.link,
            clock,
            forward_cb=self._make_link_cb(arch, edge, cloud, epoch),
            rng=random.Random(seed + 1),
        )

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
            wall_elapsed = time.time() - self._start_time
            wall_duration = end_time / cfg.traffic.time_scale
            snap = {
                "elapsed_s": round(wall_elapsed, 1),
                "wall_duration_s": round(wall_duration, 1),
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

        metrics = self._collect_metrics(cfg, sensors, link, edge, cloud)
        logger.info(
            f"[{cfg.name}] Done. events={metrics.sensor_to_edge_msgs} "
            f"cloud={cloud.received_events} lat={metrics.latency_mean_ms:.1f} ms "
            f"filtered={metrics.filtered_events}"
        )

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

    def _collect_metrics(self, cfg, sensors, link, edge, cloud) -> ExperimentMetrics:
        latency_samples = cloud.get_all_latency_samples()
        if latency_samples:
            arr = np.array(latency_samples)
            lat_mean = float(np.mean(arr))
            lat_p50  = float(np.percentile(arr, 50))
            lat_p95  = float(np.percentile(arr, 95))
            lat_p99  = float(np.percentile(arr, 99))
            lat_min  = float(np.min(arr))
            lat_max  = float(np.max(arr))
        else:
            lat_mean = lat_p50 = lat_p95 = lat_p99 = lat_min = lat_max = 0.0

        sensor_events = sensors.total_generated
        cloud_events  = cloud.received_events
        ls = link.stats
        es = edge.summary()
        cs = cloud.get_metrics_snapshot()
        arch = cfg.architecture

        if arch == "cloud_only":
            s2e_msgs  = ls.sent
            s2e_bytes = ls.total_bytes_sent
            e2c_msgs  = 0
            e2c_bytes = 0
            s2e_dr    = ls.delivery_ratio
            e2c_dr    = 1.0
        else:
            s2e_msgs  = ls.sent
            s2e_bytes = ls.total_bytes_sent
            e2c_msgs  = es.get("link_stats", {}).get("sent", 0)
            e2c_bytes = es.get("link_stats", {}).get("total_bytes_sent", 0)
            s2e_dr    = ls.delivery_ratio
            e2c_dr    = es.get("link_stats", {}).get("delivery_ratio", 1.0)

        cloud_msgs = e2c_msgs if arch != "cloud_only" else cloud_events
        agg_ratio  = (cloud_msgs / sensor_events) if sensor_events > 0 else 1.0

        return ExperimentMetrics(
            scenario_name    = cfg.name,
            protocol         = cfg.protocol,
            architecture     = cfg.architecture,
            traffic_level    = cfg.traffic_level,
            num_spots        = cfg.num_spots,
            sim_duration_s   = cfg.sim_duration_s,
            latency_mean_ms  = round(lat_mean, 2),
            latency_p50_ms   = round(lat_p50,  2),
            latency_p95_ms   = round(lat_p95,  2),
            latency_p99_ms   = round(lat_p99,  2),
            latency_min_ms   = round(lat_min,  2),
            latency_max_ms   = round(lat_max,  2),
            sensor_to_edge_msgs           = s2e_msgs,
            edge_to_cloud_msgs            = e2c_msgs,
            cloud_only_msgs               = cloud_events if arch == "cloud_only" else 0,
            sensor_to_edge_bytes          = s2e_bytes,
            edge_to_cloud_bytes           = e2c_bytes,
            sensor_to_edge_delivery_ratio = round(s2e_dr, 4),
            edge_to_cloud_delivery_ratio  = round(e2c_dr, 4),
            aggregation_ratio             = round(agg_ratio, 4),
            filtered_events               = es.get("filtered", 0),
            anomalies_detected            = es.get("anomalies", 0),
            edge_cpu_pct                  = es.get("cpu_pct", 0.0),
            edge_mem_mb                   = es.get("mem_mb",  0.0),
            cloud_cpu_pct                 = cs.get("cpu_pct", 0.0),
            cloud_mem_mb                  = cs.get("mem_mb",  0.0),
            latency_timeseries            = cloud.get_latency_timeseries(),
            latency_samples               = latency_samples[-50_000:],
        )


def save_results(metrics: ExperimentMetrics, output_dir: str = "results") -> str:
    Path(output_dir).mkdir(exist_ok=True)
    path = Path(output_dir) / f"{metrics.scenario_name}.json"
    data = metrics.to_dict()
    data["latency_samples"]    = metrics.latency_samples
    data["latency_timeseries"] = metrics.latency_timeseries
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Results saved to {path}")
    return str(path)