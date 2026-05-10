from __future__ import annotations
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse
import uvicorn

sys.path.insert(0, str(Path(__file__).parent.parent))

from simulator.config import ( PREDEFINED_SCENARIOS, make_scenario, load_custom_scenarios, save_custom_scenarios, ScenarioConfig, )
from experiments.runner import ExperimentRunner, save_results
from simulator.db import make_engine, ScenarioRun, ParkingSpot, LatencyRecord, make_session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _load_dotenv():
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

_load_dotenv()
GROQ_API_KEY: str = os.environ.get("AI_API_KEY", "").strip()
app = FastAPI(title="Smart Parking IoT Simulator", version="3.0.0")
BASE_DIR = Path(__file__).parent
RESULTS_DIR = BASE_DIR.parent / "results"
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")
_custom_scenarios: list[ScenarioConfig] = load_custom_scenarios()

def _all_scenarios() -> list[ScenarioConfig]:
    return PREDEFINED_SCENARIOS + _custom_scenarios

def _find_scenario(name: str) -> Optional[ScenarioConfig]:
    for s in _all_scenarios():
        if s.name == name:
            return s
    return None

class SimState:
    def __init__(self):
        self.running = False
        self.scenario_name: Optional[str] = None
        self.progress: dict = {}
        self.results: list[dict] = []
        self._sse_queues: list[asyncio.Queue] = []
        self._runner: Optional[ExperimentRunner] = None

    def _new_queue(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=300)
        self._sse_queues.append(q)
        return q

    def _remove_queue(self, q: asyncio.Queue):
        try:
            self._sse_queues.remove(q)
        except ValueError:
            pass

    def push(self, event_type: str, data: dict):
        for q in list(self._sse_queues):
            try:
                q.put_nowait({"type": event_type, "data": data})
            except asyncio.QueueFull:
                pass

    async def event_generator(self, request: Request):
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

def _load_historical_results():
    if not RESULTS_DIR.exists():
        return
    files = sorted(RESULTS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
    for path in files:
        try:
            with open(path) as f:
                data = json.load(f)
            data.setdefault("source", "historical")
            data.setdefault("latency_samples", [])
            state.results.append(data)
        except Exception as e:
            logger.warning(f"Could not load historical result {path}: {e}")
    if state.results:
        logger.info(f"Loaded {len(state.results)} historical result(s) from {RESULTS_DIR}")

@app.on_event("startup")
async def startup_event():
    _load_historical_results()

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/scenarios")
async def list_scenarios():
    return [
        {
            "name": s.name,
            "description": s.description,
            "protocol": s.protocol,
            "architecture": s.architecture,
            "traffic_level": s.traffic_level,
            "num_spots": s.num_spots,
            "sim_duration_s": s.sim_duration_s,
            "group": getattr(s, "group", ""),
            "group_order": getattr(s, "group_order", 0),
            "is_builtin": getattr(s, "is_builtin", False),
            "loss_rate": s.link.packet_loss_rate,
            "rate_limit": s.link.rate_limit_msgs_per_sec,
            "aggregation_interval": s.edge.aggregation_interval_s,
            "mqtt_qos": s.mqtt.qos,
            "coap_mode": s.coap.mode,
            "amqp_exchange": s.amqp.exchange_type,
            "amqp_ack": s.amqp.ack_mode,
            "amqp_durable": s.amqp.durable,
            "seed": s.random_seed,
        }
        for s in _all_scenarios()
    ]

@app.post("/api/scenarios")
async def create_scenario(body: dict):
    name = (body.get("name") or "").strip().replace(" ", "_")
    if not name:
        raise HTTPException(400, "name is required")
    if _find_scenario(name):
        raise HTTPException(409, f"Scenario '{name}' already exists")
    try:
        cfg = _make_cfg_from_body(name, body)
    except Exception as e:
        raise HTTPException(400, f"Invalid parameters: {e}")
    _custom_scenarios.append(cfg)
    save_custom_scenarios(_custom_scenarios)
    return {"status": "created", "name": name}

@app.put("/api/scenarios/{scenario_name}")
async def update_scenario(scenario_name: str, body: dict):
    existing = _find_scenario(scenario_name)
    if existing is None:
        raise HTTPException(404, f"Scenario '{scenario_name}' not found")
    if existing.is_builtin:
        raise HTTPException(403, "Built-in scenarios cannot be edited")
    try:
        cfg = _make_cfg_from_body(scenario_name, body)
    except Exception as e:
        raise HTTPException(400, f"Invalid parameters: {e}")
    # Replace in list
    for i, s in enumerate(_custom_scenarios):
        if s.name == scenario_name:
            _custom_scenarios[i] = cfg
            break
    save_custom_scenarios(_custom_scenarios)
    return {"status": "updated", "name": scenario_name}


@app.delete("/api/scenarios/{scenario_name}")
async def delete_scenario(scenario_name: str):
    existing = _find_scenario(scenario_name)
    if existing is None:
        raise HTTPException(404, f"Scenario '{scenario_name}' not found")
    if existing.is_builtin:
        raise HTTPException(403, "Built-in scenarios cannot be deleted")
    global _custom_scenarios
    _custom_scenarios = [s for s in _custom_scenarios if s.name != scenario_name]
    save_custom_scenarios(_custom_scenarios)
    return {"status": "deleted", "name": scenario_name}

def _make_cfg_from_body(name: str, body: dict) -> ScenarioConfig:
    duration = float(body.get("sim_duration_s", 30.0))
    duration = max(5.0, min(7200.0, duration))
    initial_occ_raw = body.get("initial_occupancy")
    initial_occ = float(initial_occ_raw) if initial_occ_raw is not None else None
    return make_scenario(
        name=name,
        description=body.get("description", name),
        protocol=body.get("protocol", "amqp"),
        architecture=body.get("architecture", "edge_aggregated"),
        traffic_level=body.get("traffic_level", "medium"),
        num_spots=max(5, min(5000, int(body.get("num_spots", 50)))),
        loss_rate=max(0.0, min(0.5, float(body.get("loss_rate", 0.02)))),
        aggregation_interval=max(0.5, float(body.get("aggregation_interval", 2.0))),
        anomaly_detection=bool(body.get("anomaly_detection", True)),
        adaptive_edge=bool(body.get("adaptive_edge", False)),
        warmup_s=max(0.0, float(body.get("warmup_s", 60.0))),
        mqtt_qos=int(body.get("mqtt_qos", 1)),
        coap_mode=body.get("coap_mode", "CON"),
        amqp_exchange=body.get("amqp_exchange", "direct"),
        amqp_ack=body.get("amqp_ack", "manual"),
        amqp_durable=bool(body.get("amqp_durable", True)),
        sim_duration_s=duration,
        seed=int(body.get("seed", 42)),
        rate_limit=max(1.0, float(body.get("rate_limit", 10.0))),
        group=body.get("group", "User Scenarios"),
        group_order=int(body.get("group_order", 99)),
        is_builtin=False,
        base_delay_ms=float(body.get("base_delay_ms", 80.0)),
        jitter_ms=float(body.get("jitter_ms", 30.0)),
        max_payload_bytes=int(body.get("max_payload_bytes", 51)),
        payload_encoding_ratio=float(body.get("payload_encoding_ratio", 0.15)),
        parking_duration_cv=float(body.get("parking_duration_cv", 1.5)),
        time_scale=float(body.get("time_scale", 60.0)),
        use_time_of_day=bool(body.get("use_time_of_day", False)),
        start_hour=float(body.get("start_hour", 8.0)),
        initial_occupancy=initial_occ,
    )

@app.post("/api/run/preset/{scenario_name}")
async def run_preset(scenario_name: str):
    if state.running:
        raise HTTPException(400, "A simulation is already running.")
    cfg = _find_scenario(scenario_name)
    if not cfg:
        raise HTTPException(404, f"Unknown scenario: {scenario_name}")
    asyncio.create_task(_run_simulation(cfg))
    return {"status": "started", "scenario": scenario_name}

@app.post("/api/run/custom")
async def run_custom(body: dict):
    if state.running:
        raise HTTPException(400, "A simulation is already running.")
    try:
        cfg = _make_cfg_from_body(f"custom_{int(time.time())}", body)
    except Exception as e:
        raise HTTPException(400, f"Invalid parameters: {e}")
    asyncio.create_task(_run_simulation(cfg))
    return {"status": "started", "scenario": cfg.name}

@app.post("/api/stop")
async def stop_simulation():
    if not state.running or state._runner is None:
        return {"status": "not_running"}
    state._runner.cancel()
    return {"status": "stopping"}

@app.get("/api/status")
async def get_status():
    return {
        "running":  state.running,
        "scenario": state.scenario_name,
        "progress": state.progress,
    }

@app.get("/api/results")
async def get_results():
    return state.results

@app.get("/api/results/latest")
async def get_latest_result():
    return state.results[-1] if state.results else {}

@app.delete("/api/results")
async def clear_results():
    state.results.clear()
    if RESULTS_DIR.exists():
        for f in RESULTS_DIR.glob("*.json"):
            f.unlink(missing_ok=True)

    engine = make_engine()
    if engine is not None:
        session = make_session(engine)
        try:
            session.query(LatencyRecord).delete()
            session.query(ParkingSpot).delete()
            session.query(ScenarioRun).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            logger.warning(f"DB clear failed: {e}")
        finally:
            session.close()

    return {"status": "cleared"}

@app.get("/api/stream")
async def sse_stream(request: Request):
    return EventSourceResponse(state.event_generator(request))

@app.get("/api/ai_available")
async def ai_available():
    return {"available": bool(GROQ_API_KEY)}

async def _call_groq(system: str, user: str) -> str:
    import httpx
    async with httpx.AsyncClient(timeout=60.0) as c:
        r = await c.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user},
                ],
                "temperature": 0.3,
                "max_tokens":  1500,
            },
        )
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"]
        raise RuntimeError(
            f"Groq {r.status_code}: {r.json().get('error', {}).get('message', r.text)}"
        )

SYSTEM_PROMPT = """\
You are analysing output from a discrete-event smart parking simulator. The model generates Poisson arrivals and log-normal dwell times across configurable spot counts, 
then routes each state-change event through a link emulator (delay, jitter, packet loss, rate limiting) to one of 3 edge architectures before reaching the cloud.
Link model: LoRa SF7 + 4G default - 80 ms ±30 ms one-way. Deployment archetypes override this: WiFi 15 ms ±8 ms, NB-IoT connected 250 ms ±80 ms, NB-IoT eDRX
~5370 ms ±5120 ms, LoRa with repeater 130 ms ±50 ms. Packet loss applies to all architectures.
Edge modes:
  cloud_only - events forwarded raw, no processing
  edge_filtered - duplicate state-change events dropped before forwarding
  edge_aggregated - events batched for a fixed window then flushed as one message
Protocol overhead added on top of link latency:
  MQTT QoS 0: 0 ms   QoS 1: +5 ms (retransmit on loss)   QoS 2: +15 ms
  CoAP NON: +1 ms    CoAP CON: +8 ms (retransmit on loss, CBOR saves ~35% bytes)
  AMQP direct/auto: +2 ms   direct/manual: +8 ms   topic/manual/durable: +14 ms
aggregation_ratio = cloud_events / sensor_events. Below 1.0 means the edge is reducing cloud traffic; above 1.0 means it is adding overhead (typical for small, sparse lots).
Latency is measured sensor-emit to cloud-arrival and therefore includes the link delay and any protocol retransmit cycles. It does not include application-level processing time.
Ground your analysis in the specific numbers provided. Draw a clear line between what the simulation demonstrates and what real deployments would add (RF planning, backhaul SLAs,
hardware constraints). Write in prose — no tables."""

FOCUS_PROMPTS = {
    "general": "Give an end-to-end assessment: which configuration comes out ahead and why, how the latency/reliability/bandwidth numbers relate to each other, whether the results match what protocol theory would predict, and where the first real-world breaking point would be.",
    "latency": "Examine latency closely: what the spread between mean, P50, P95, and P99 tells you about tail behaviour and scheduling jitter, how the observed numbers map to the configured link delay, and whether any P99 spikes are traceable to retransmit cycles or aggregation window alignment.",
    "reliability": "Work through delivery reliability: compare measured delivery ratios against the configured loss rate for each scenario, identify which protocol mechanisms visibly compensated for loss, note where retransmit overhead shows up in latency, and say which configuration you would stake availability on.",
    "bandwidth": "Dig into bandwidth: calculate the exact byte savings edge aggregation delivers over cloud-only, explain what the aggregation_ratio says about event burstiness, locate the bottleneck link, and give a rough projection for a 10 000-sensor deployment running 24 hours a day.",
    "protocol": "Compare the protocols directly against each other: where does MQTT's QoS tiering, AMQP's broker guarantees, or CoAP's binary encoding actually move the numbers rather than just the theory? Note anything that contradicts the expected ordering.",
    "architecture": "Quantify the architecture trade-offs: measure the latency penalty and bandwidth saving of edge aggregation relative to cloud-only, assess how much filtering actually reduces cloud load, and give concrete guidance on which architecture fits a street-side installation versus a large structured car park.",
    "recommendation": "Write a deployment recommendation: name the single best protocol and architecture combination with the reasoning behind it, list the three most likely failure modes and what would mitigate them, and be explicit about what this simulation does not model.",
    "scale": "Focus on the scalability runs: describe how latency and delivery ratio shift as sensor count grows, identify which protocol holds up best under load, and estimate what the numbers imply for gateway hardware at 10 000 sensors.",
    }

def _build_prompt(summaries: list[dict], focus: str) -> str:
    fi = FOCUS_PROMPTS.get(focus, FOCUS_PROMPTS["general"])
    return f"""\
{fi}

Scenario results ({len(summaries)} run{'s' if len(summaries) != 1 else ''}):
{json.dumps(summaries, indent=2)}

Respond using these markdown sections — paragraphs only, no tables:

### 🔍 Key Findings
3–5 bullet points, each with a specific number from the data.

### 📊 Analysis
2–3 paragraphs explaining the mechanisms behind the numbers.

### 💡 Non-Obvious Insights
1–2 things that are surprising or only visible on close inspection.

### ✅ Recommendations
3–4 concrete action items, each tagged [Easy], [Medium], or [Hard]."""

@app.post("/api/interpret")
async def interpret_results(body: dict):
    results = body.get("results", [])
    focus   = body.get("focus", "general")
    if not results:
        raise HTTPException(400, "No results provided.")

    summaries = []
    for r in results:
        summaries.append({
            "scenario": r.get("scenario_name"),
            "protocol": r.get("protocol"),
            "architecture": r.get("architecture"),
            "traffic": r.get("traffic_level"),
            "spots": r.get("num_spots"),
            "sim_duration_s": r.get("sim_duration_s"),
            "latency_mean_ms": r.get("latency_mean_ms"),
            "latency_p50_ms": r.get("latency_p50_ms"),
            "latency_p95_ms": r.get("latency_p95_ms"),
            "latency_p99_ms": r.get("latency_p99_ms"),
            "delivery_ratio": r.get("sensor_to_edge_delivery_ratio"),
            "aggregation_ratio": r.get("aggregation_ratio"),
            "filtered_events": r.get("filtered_events"),
            "sensor_to_edge_bytes_kb": round(r.get("sensor_to_edge_bytes", 0) / 1024, 2),
            "edge_to_cloud_bytes_kb": round(r.get("edge_to_cloud_bytes", 0) / 1024, 2),
            "sensor_to_edge_msgs": r.get("sensor_to_edge_msgs"),
            "edge_to_cloud_msgs": r.get("edge_to_cloud_msgs"),
        })

    if GROQ_API_KEY:
        try:
            text = await _call_groq(SYSTEM_PROMPT, _build_prompt(summaries, focus))
            return {"interpretation": text, "model": "llama-3.3-70b", "powered_by": "Groq"}
        except Exception as e:
            logger.warning(f"Groq call failed: {e}")
            return {
                "interpretation": _rule_based_interpret(summaries),
                "model": "rule-based",
                "powered_by": "Built-in Analyzer",
                "warning": f"Groq unavailable ({e}) — showing built-in analysis",
            }

    return {
        "interpretation": _rule_based_interpret(summaries),
        "model": "rule-based",
        "powered_by": "Built-in Analyzer",
    }

def _rule_based_interpret(summaries: list[dict]) -> str:
    if not summaries:
        return "No results to analyse."

    lines: list[str] = ["## 📊 Simulation Analysis\n"]
    by_lat = sorted([s for s in summaries if s.get("latency_mean_ms")], key=lambda x: x["latency_mean_ms"])
    by_del = sorted([s for s in summaries if s.get("delivery_ratio") is not None], key=lambda x: x["delivery_ratio"], reverse=True)

    lines.append("### 🔍 Key Findings\n")
    if by_lat:
        best, worst = by_lat[0], by_lat[-1]
        lines.append(
            f"- 🥇 **Lowest latency**: `{best['scenario']}` — "
            f"mean {best['latency_mean_ms']:.1f} ms, "
            f"P95 {best.get('latency_p95_ms','?')} ms"
        )
        if len(by_lat) > 1:
            diff = worst["latency_mean_ms"] - best["latency_mean_ms"]
            lines.append(
                f"- 🐢 **Highest latency**: `{worst['scenario']}` — "
                f"{worst['latency_mean_ms']:.1f} ms ({diff:.1f} ms slower than best)"
            )
    if by_del:
        lines.append(
            f"- 📦 **Best delivery**: `{by_del[0]['scenario']}` "
            f"at {by_del[0]['delivery_ratio']*100:.2f}%"
        )
        if by_del[-1]["delivery_ratio"] < 0.98:
            lines.append(
                f"- ⚠️ **Worst delivery**: `{by_del[-1]['scenario']}` — "
                f"{by_del[-1]['delivery_ratio']*100:.2f}% "
                f"({(1-by_del[-1]['delivery_ratio'])*100:.2f}% loss)"
            )
    agg_list = [s for s in summaries if s.get("aggregation_ratio") and s["aggregation_ratio"] < 0.9]
    if agg_list:
        best_agg = min(agg_list, key=lambda x: x["aggregation_ratio"])
        lines.append(
            f"- 💾 **Best compression**: `{best_agg['scenario']}` — "
            f"{(1-best_agg['aggregation_ratio'])*100:.1f}% fewer cloud messages "
            f"(ratio {best_agg['aggregation_ratio']:.3f})"
        )

    lines.append("\n### 📊 Analysis\n")
    protos: dict = {}
    for s in summaries:
        protos.setdefault(s.get("protocol", "?"), []).append(s)
    if len(protos) > 1:
        parts = []
        for p, runs in protos.items():
            lats = [r["latency_mean_ms"] for r in runs if r.get("latency_mean_ms")]
            if lats:
                parts.append(f"**{p.upper()}** avg {sum(lats)/len(lats):.1f} ms")
        lines.append("Protocol latency averages: " + ", ".join(parts) + ".\n")

    archs: dict = {}
    for s in summaries:
        archs.setdefault(s.get("architecture", "?"), []).append(s)
    if len(archs) > 1:
        parts = []
        for a, runs in archs.items():
            lats = [r["latency_mean_ms"] for r in runs if r.get("latency_mean_ms")]
            bw   = sum(r.get("sensor_to_edge_bytes_kb", 0) for r in runs) / max(len(runs), 1)
            if lats:
                parts.append(f"`{a}` averaged {sum(lats)/len(lats):.1f} ms, {bw:.1f} KB S→E")
        lines.append("Architecture comparison: " + "; ".join(parts) + ".\n")

    lines.append("\n### ✅ Recommendations\n")
    if by_lat and by_del:
        balanced = sorted(
            [s for s in summaries if s.get("delivery_ratio", 0) > 0.95 and s.get("latency_mean_ms")],
            key=lambda x: x["latency_mean_ms"],
        )
        if balanced:
            r = balanced[0]
            lines.append(
                f"- ✅ **Best balanced**: `{r['scenario']}` — "
                f"{r['latency_mean_ms']:.1f} ms with {r['delivery_ratio']*100:.2f}% delivery [Easy]"
            )
    lines.append(
        "- 📡 **Default stack**: AMQP direct + manual-ACK + durable + edge_aggregated, "
        "2 s window for ≤200 sensors, 5 s for larger deployments [Easy]"
    )
    lines.append(
        "- 🔧 **Loss threshold**: switch to confirmable delivery "
        "(MQTT QoS 1+, CoAP CON, AMQP manual-ACK) when link loss exceeds ~3% [Easy]"
    )
    lines.append(
        "- 🏙️ **Large-scale tuning**: for 500+ sensors, raise rate limit to ≥100 msg/s "
        "and aggregation interval to 5–10 s to prevent token-bucket back-pressure [Medium]"
    )
    lines.append(
        "\n\n*Built-in analysis — set `AI_API_KEY` in `.env` for Groq-powered insights.*"
    )
    return "\n".join(lines)

async def _run_simulation(cfg: ScenarioConfig):
    state.running = True
    state.scenario_name = cfg.name
    state.progress = {}
    state.push("sim_started", {
        "scenario": cfg.name,
        "num_spots": cfg.num_spots,
        "sim_duration_s": cfg.sim_duration_s,
        "protocol": cfg.protocol,
        "architecture": cfg.architecture,
    })

    def progress_cb(snap: dict):
        state.progress = snap
        state.push("progress", snap)

    def flush_cb():
        state.push("sim_flushing", {"scenario": cfg.name})

    runner = ExperimentRunner(cfg, progress_cb=progress_cb, flush_cb=flush_cb)
    state._runner = runner
    try:
        metrics = await runner.run()
        result = metrics.to_dict()
        result["latency_samples"] = metrics.latency_samples
        result["source"] = "live"
        state.results.append(result)
        state.push("sim_flushing", {"scenario": cfg.name})
        save_results(metrics, str(RESULTS_DIR))
        state.push("sim_flushing", {"scenario": cfg.name})
        save_results(metrics, str(RESULTS_DIR))
        state.push("sim_complete", result)
        logger.info(f"Simulation '{cfg.name}' complete")
    except asyncio.CancelledError:
        state.push("sim_error", {"error": "Cancelled"})
    except Exception as e:
        logger.exception(f"Simulation failed: {e}")
        state.push("sim_error", {"error": str(e)})
    finally:
        state.running = False
        state.scenario_name = None
        state._runner = None

def start():
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="warning")

if __name__ == "__main__":
    start()