from __future__ import annotations
import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from simulator.config import PREDEFINED_SCENARIOS, ScenarioConfig, make_scenario, load_custom_scenarios
from experiments.runner import ExperimentRunner, save_results

logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"

class SimState:
    def __init__(self) -> None:
        self.running: bool = False
        self.scenario_name: Optional[str] = None
        self.progress: dict = {}
        self.results: list[dict] = []
        self._sse_queues: list[asyncio.Queue] = []
        self._runner: Optional[ExperimentRunner] = None

    def _new_queue(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=300)
        self._sse_queues.append(q)
        return q

    def _remove_queue(self, q: asyncio.Queue) -> None:
        try:
            self._sse_queues.remove(q)
        except ValueError:
            pass

    def push(self, event_type: str, data: dict) -> None:
        for q in list(self._sse_queues):
            try:
                q.put_nowait({"type": event_type, "data": data})
            except asyncio.QueueFull:
                pass

    async def event_generator(self, request):
        q = self._new_queue()
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(q.get(), timeout=1.0)
                    yield {"event": item["type"], "data": json.dumps(item["data"])}
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": "{}"}
        finally:
            self._remove_queue(q)


state = SimState()

_custom_scenarios: list[ScenarioConfig] = load_custom_scenarios()

def all_scenarios() -> list[ScenarioConfig]:
    return PREDEFINED_SCENARIOS + _custom_scenarios

def find_scenario(name: str) -> Optional[ScenarioConfig]:
    return next((s for s in all_scenarios() if s.name == name), None)

def make_cfg_from_body(name: str, body: dict) -> ScenarioConfig:
    duration_h = max(0.5, min(720.0, float(body.get("sim_duration_h", 8.0))))
    num_spots = max(5, min(5000, int(body.get("num_spots", 50))))
    rate_limit = max(1.0, float(body.get("rate_limit", max(5.0, num_spots / 10.0))))
    initial_occ_raw = body.get("initial_occupancy")

    return make_scenario(
        name = name,
        description = body.get("description", name),
        protocol = body.get("protocol", "amqp"),
        architecture = body.get("architecture", "edge_aggregated"),
        traffic_level = body.get("traffic_level", "medium"),
        num_spots = num_spots,
        loss_rate = max(0.0, min(0.5, float(body.get("loss_rate", 0.02)))),
        aggregation_interval = max(5.0, min(300.0, float(body.get("aggregation_interval", 30.0)))),
        anomaly_detection = bool(body.get("anomaly_detection", True)),
        adaptive_edge = bool(body.get("adaptive_edge", False)),
        warmup_s = max(0.0, float(body.get("warmup_s", 60.0))),
        mqtt_qos = int(body.get("mqtt_qos", 1)),
        coap_mode = body.get("coap_mode", "CON"),
        amqp_exchange = body.get("amqp_exchange", "direct"),
        amqp_ack = body.get("amqp_ack", "manual"),
        amqp_durable = bool(body.get("amqp_durable", True)),
        sim_duration_s = duration_h * 3600.0,
        seed = int(body.get("seed", 42)),
        rate_limit = rate_limit,
        group = body.get("group", "User Scenarios"),
        group_order = int(body.get("group_order", 99)),
        is_builtin = False,
        base_delay_ms = float(body.get("base_delay_ms", 80.0)),
        jitter_ms = float(body.get("jitter_ms", 30.0)),
        max_payload_bytes = int(body.get("max_payload_bytes", 51)),
        payload_encoding_ratio = float(body.get("payload_encoding_ratio", 0.15)),
        parking_duration_cv = float(body.get("parking_duration_cv", 1.5)),
        time_scale = float(body.get("time_scale", 60.0)),
        use_time_of_day = bool(body.get("use_time_of_day", False)),
        start_hour = float(body.get("start_hour", 8.0)),
        initial_occupancy = float(initial_occ_raw) if initial_occ_raw is not None else None
    )

async def run_simulation(cfg: ScenarioConfig) -> None:
    state.running = True
    state.scenario_name = cfg.name
    state.progress = {}
    state.push("sim_started", {
        "scenario": cfg.name,
        "num_spots": cfg.num_spots,
        "sim_duration_s": cfg.sim_duration_s,
        "protocol": cfg.protocol,
        "architecture": cfg.architecture
    })

    def _progress(snap: dict) -> None:
        state.progress = snap
        state.push("progress", snap)

    runner = ExperimentRunner(
        cfg,
        progress_cb=_progress,
        flush_cb=lambda: state.push("sim_flushing", {"scenario": cfg.name}),
    )
    state._runner = runner
    try:
        metrics = await runner.run()
        result = metrics.to_dict()
        result["latency_samples"] = metrics.latency_samples
        result["source"] = "live"
        state.results.append(result)
        save_results(metrics, str(RESULTS_DIR))
        state.push("sim_complete", result)
        logger.info(f"Simulation '{cfg.name}' complete")
    except asyncio.CancelledError:
        state.push("sim_error", {"error": "Cancelled"})
    except Exception as exc:
        logger.exception(f"Simulation failed: {exc}")
        state.push("sim_error", {"error": str(exc)})
    finally:
        state.running = False
        state.scenario_name = None
        state._runner = None