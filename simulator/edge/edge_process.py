from __future__ import annotations
import json
import logging
import multiprocessing as mp
import time
from typing import Callable

from simulator.config import ScenarioConfig
from simulator.models import BatchUpdate, ParkingEvent, SpotState
from simulator.des.engine import SimClock
from simulator.edge.edge_node import EdgeNode

logger = logging.getLogger(__name__)


def _edge_worker(config_dict: dict, cmd_queue: mp.Queue, result_queue: mp.Queue, epoch: float) -> None:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)-8s edge-worker: %(message)s", datefmt="%H:%M:%S")

    cfg = ScenarioConfig.from_dict(config_dict)
    clock = SimClock()

    def cloud_cb(batch: BatchUpdate, payload: bytes) -> None:
        result_queue.put_nowait({"type": "batch", "batch": batch.to_dict(), "payload": payload.hex()})

    edge = EdgeNode(cfg, clock, cloud_cb, epoch)

    result_queue.put_nowait({"type": "ready"})

    while True:
        try:
            msg = cmd_queue.get(timeout=5.0)
        except Exception:
            clock.env.run(until=clock.now + 1.0)
            continue

        mtype = msg.get("type")

        if mtype == "event":
            ev_dict = msg["event"]
            state_raw = ev_dict.get("state", "free")
            try:
                state = SpotState(state_raw)
            except ValueError:
                state = SpotState.FREE
            event = ParkingEvent(
                sensor_id=ev_dict.get("sensor_id", ""),
                spot_id=int(ev_dict.get("spot_id", 0)),
                state=state,
                timestamp=float(ev_dict.get("timestamp", 0.0)),
                sequence=int(ev_dict.get("sequence", 0)),
                is_initial=bool(ev_dict.get("is_initial", False))
            )
            event_virtual = event.timestamp - epoch
            if event_virtual > clock.now:
                clock.env.run(until=event_virtual)
            raw = json.dumps(ev_dict).encode()
            edge.receive(event, raw)
        elif mtype == "flush":
            edge.flush_final()
        elif mtype == "snapshot_req":
            result_queue.put_nowait({"type": "snapshot", "summary": edge.summary()})
        elif mtype == "stop":
            break
        else:
            logger.warning(f"edge_worker: unknown message type {mtype!r}")


class EdgeWorkerProcess:

    def __init__(self, config: ScenarioConfig, epoch: float, on_batch: "Callable[[BatchUpdate, bytes], None]") -> None:
        self._config = config
        self._epoch = epoch
        self._on_batch = on_batch

        ctx = mp.get_context("spawn")
        self._cmd_q: mp.Queue = ctx.Queue(maxsize=10_000)
        self._result_q: mp.Queue = ctx.Queue(maxsize=10_000)
        self._proc: mp.Process = ctx.Process(target=_edge_worker, args=(config.to_save_dict(), self._cmd_q, self._result_q, epoch), daemon=True, name="EdgeWorker")

        self._last_snapshot: dict = {}
        self._ready = False

    def start(self) -> None:
        self._proc.start()
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            self._drain_results(block=True, timeout=0.5)
            if self._ready:
                logger.info("[EdgeWorker] Subprocess ready.")
                return
        raise RuntimeError("EdgeWorker subprocess did not become ready in time")

    def stop(self) -> None:
        self._cmd_q.put_nowait({"type": "stop"})
        self._proc.join(timeout=10.0)
        if self._proc.is_alive():
            self._proc.terminate()

    def receive(self, event: ParkingEvent, raw: bytes) -> None:
        self._cmd_q.put_nowait({"type": "event", "event": event.to_dict()})

    def flush_final(self) -> None:
        self._cmd_q.put_nowait({"type": "flush"})
        time.sleep(0.2)
        self._drain_results(block=False)

    def request_snapshot(self) -> None:
        self._cmd_q.put_nowait({"type": "snapshot_req"})

    def drain(self) -> None:
        self._drain_results(block=False)

    def summary(self) -> dict:
        return self._last_snapshot.get("summary", {
            "received": 0, "filtered": 0, "forwarded_events": 0,
            "anomalies": 0, "mode_switches": 0, "active_arch": "unknown",
            "link_stats": {"sent": 0, "received": 0, "dropped": 0, "total_bytes_sent": 0, "total_bytes_received": 0, "delivery_ratio": 1.0, "drop_rate": 0.0}
        })

    def _drain_results(self, block: bool = False, timeout: float = 0.0) -> None:
        import queue as _queue

        while True:
            try:
                msg = self._result_q.get(block=block, timeout=timeout)
                block = False
            except (_queue.Empty, Exception):
                break

            mtype = msg.get("type")
            if mtype == "ready":
                self._ready = True
            elif mtype == "batch":
                batch_dict = msg["batch"]
                raw = bytes.fromhex(msg["payload"])
                from simulator.protocols.mqtt_client import _batch_from_dict
                batch = _batch_from_dict(batch_dict)
                self._on_batch(batch, raw)
            elif mtype == "snapshot":
                self._last_snapshot = msg