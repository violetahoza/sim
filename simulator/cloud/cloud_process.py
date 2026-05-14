from __future__ import annotations
import logging
import multiprocessing as mp
import time
from typing import TYPE_CHECKING
import psutil

if TYPE_CHECKING:
    from simulator.config import ScenarioConfig

logger = logging.getLogger(__name__)


def _cloud_worker(config_dict: dict, epoch: float, batch_queue: mp.Queue, metric_queue: mp.Queue, cmd_queue: mp.Queue, db_url: str | None) -> None:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)-8s cloud-worker: %(message)s", datefmt="%H:%M:%S")

    from simulator.config import ScenarioConfig
    from simulator.cloud.cloud_backend import CloudBackend
    from simulator.des.engine import SimClock
    from simulator.db import make_engine, init_schema

    cfg = ScenarioConfig.from_dict(config_dict)
    clock = SimClock()
    cloud = CloudBackend(cfg, clock, epoch)

    engine = make_engine(db_url)
    if engine:
        init_schema(engine)
        import json as _json
        cloud.open_run(engine, config_json=_json.dumps(config_dict))

    proc = psutil.Process()
    proc.cpu_percent(interval=None)

    metric_queue.put_nowait({"type": "ready"})

    import queue as _queue

    while True:
        while True:
            try:
                item = batch_queue.get_nowait()
                _handle_batch(item, cloud)
            except _queue.Empty:
                break
        try:
            cmd = cmd_queue.get(timeout=0.05)
        except _queue.Empty:
            continue

        ctype = cmd.get("type")

        if ctype == "snapshot_req":
            snap = cloud.get_metrics_snapshot()
            metric_queue.put_nowait({"type": "snapshot", "data": snap})
        elif ctype == "flush":
            metrics_dict = cmd.get("metrics_dict", {})
            if engine:
                try:
                    from simulator.models import ExperimentMetrics
                    m = ExperimentMetrics(**{
                        k: v for k, v in metrics_dict.items()
                        if k in ExperimentMetrics.__dataclass_fields__
                    })
                    cloud.flush_to_db(engine, m)
                except Exception:
                    logger.exception("cloud_worker: DB flush error")
            metric_queue.put_nowait({"type": "flushed"})
        elif ctype == "get_all":
            samples = cloud.get_all_latency_samples()
            metric_queue.put_nowait({
                "type": "all_samples",
                "samples": samples,
                "occupancy": cloud.get_occupancy(),
                "snapshot": cloud.get_metrics_snapshot(),
                "received_events": cloud.received_events,
            })
        elif ctype == "stop":
            break


def _handle_batch(item: dict, cloud) -> None:
    from simulator.protocols.mqtt_client import _batch_from_dict
    try:
        batch = _batch_from_dict(item["batch"])
        raw = bytes.fromhex(item["payload"])
        wall_arrival = item.get("wall_arrival")
        cloud.receive_batch_real(batch, raw, wall_arrival)
    except Exception:
        logger.exception("cloud_worker: batch handling error")


class CloudWorkerProcess:

    def __init__(self, config: "ScenarioConfig", epoch: float, db_url: str | None = None) -> None:
        self._config = config
        self._epoch = epoch
        self._db_url = db_url

        ctx = mp.get_context("spawn")
        self._batch_q: mp.Queue = ctx.Queue(maxsize=50_000)
        self._metric_q: mp.Queue = ctx.Queue(maxsize=1_000)
        self._cmd_q: mp.Queue = ctx.Queue(maxsize=100)

        self._proc: mp.Process = ctx.Process(
            target=_cloud_worker,
            args=(config.to_save_dict(), epoch, self._batch_q, self._metric_q, self._cmd_q, db_url),
            daemon=True,
            name="CloudWorker",
        )
        self._ready = False
        self._last_snapshot: dict = {}
        self.received_events: int = 0

    def start(self) -> None:
        self._proc.start()
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            self._drain_metrics(block=True, timeout=0.5)
            if self._ready:
                logger.info("[CloudWorker] Subprocess ready.")
                return
        raise RuntimeError("CloudWorker subprocess did not become ready in time")

    def stop(self) -> None:
        self._cmd_q.put_nowait({"type": "stop"})
        self._proc.join(timeout=10.0)
        if self._proc.is_alive():
            self._proc.terminate()

    def receive_batch(self, batch, raw: bytes) -> None:
        import time as _t
        self._batch_q.put_nowait({
            "batch": batch.to_dict(),
            "payload": raw.hex(),
            "wall_arrival": _t.time(),
        })
        self.received_events += len(batch.events)

    def request_snapshot(self) -> None:
        self._cmd_q.put_nowait({"type": "snapshot_req"})

    def get_metrics_snapshot(self) -> dict:
        self.request_snapshot()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            self._drain_metrics(block=True, timeout=0.2)
            snap = self._last_snapshot
            if snap:
                return snap
        return {}

    def get_all_data(self) -> dict:
        self._cmd_q.put_nowait({"type": "get_all"})
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            self._drain_metrics(block=True, timeout=0.5)
            if "samples" in self._last_snapshot:
                return self._last_snapshot
        return {}

    def flush_to_db(self, metrics_dict: dict) -> None:
        self._cmd_q.put_nowait({"type": "flush", "metrics_dict": metrics_dict})
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            self._drain_metrics(block=True, timeout=1.0)
            if self._last_snapshot.get("type") == "flushed":
                return

    def get_all_latency_samples(self) -> list[float]:
        data = self.get_all_data()
        return data.get("samples", [])

    def get_occupancy(self) -> dict:
        data = self.get_all_data()
        return data.get("occupancy", {})

    def compute_broker_overhead_score(self) -> float:
        cfg = self._config
        from simulator.cloud.cloud_backend import _BROKER_WEIGHT
        proto = cfg.protocol
        if proto == "mqtt":
            key = f"mqtt_qos{cfg.mqtt.qos}"
        elif proto == "amqp":
            key = f"amqp_{cfg.amqp.exchange_type}/{cfg.amqp.ack_mode}"
        elif proto == "coap":
            key = f"coap_{cfg.coap.mode}"
        else:
            key = ""
        weight = _BROKER_WEIGHT.get(key, 1.5)
        return round(weight * max(self.received_events, 1) / 1000, 4)

    def _drain_metrics(self, block: bool = False, timeout: float = 0.0) -> None:
        import queue as _q

        while True:
            try:
                msg = self._metric_q.get(block=block, timeout=timeout)
                block = False
            except (_q.Empty, Exception):
                break

            mtype = msg.get("type")
            if mtype == "ready":
                self._ready = True
            elif mtype == "snapshot":
                self._last_snapshot = msg["data"]
            elif mtype == "all_samples":
                self._last_snapshot = msg
            elif mtype == "flushed":
                self._last_snapshot = {"type": "flushed"}