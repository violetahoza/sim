from __future__ import annotations
import asyncio
import logging
import time
from fastapi import APIRouter, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from simulator.config.config import save_custom_scenarios
from simulator.cloud.db import make_engine, ScenarioRun, ParkingSpot, LatencyRecord, make_session
from web.interpreter import GROQ_API_KEY, build_summaries, interpret
from web.state import state, _custom_scenarios, RESULTS_DIR, all_scenarios, find_scenario, make_cfg_from_body, run_simulation

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/api/scenarios")
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
            "group": s.group,
            "group_order": s.group_order,
            "is_builtin": s.is_builtin,
            "loss_rate": s.link.packet_loss_rate,
            "rate_limit": s.link.rate_limit_msgs_per_sec,
            "aggregation_interval": s.edge.aggregation_interval_s,
            "heartbeat_interval_s": s.traffic.heartbeat_interval_s,
            "mqtt_qos": s.mqtt.qos,
            "coap_mode": s.coap.mode,
            "amqp_exchange": s.amqp.exchange_type,
            "amqp_ack": s.amqp.ack_mode,
            "amqp_durable": s.amqp.durable,
            "seed": s.random_seed
        }
        for s in all_scenarios()
    ]

@router.post("/api/scenarios")
async def create_scenario(body: dict):
    name = (body.get("name") or "").strip().replace(" ", "_")
    if not name:
        raise HTTPException(400, "name is required")
    if find_scenario(name):
        raise HTTPException(409, f"Scenario '{name}' already exists")
    try:
        cfg = make_cfg_from_body(name, body)
    except Exception as exc:
        raise HTTPException(400, f"Invalid parameters: {exc}")
    _custom_scenarios.append(cfg)
    save_custom_scenarios(_custom_scenarios)
    return {"status": "created", "name": name}

@router.put("/api/scenarios/{scenario_name}")
async def update_scenario(scenario_name: str, body: dict):
    existing = find_scenario(scenario_name)
    if existing is None:
        raise HTTPException(404, f"Scenario '{scenario_name}' not found")
    if existing.is_builtin:
        raise HTTPException(403, "Built-in scenarios cannot be edited")
    try:
        cfg = make_cfg_from_body(scenario_name, body)
    except Exception as exc:
        raise HTTPException(400, f"Invalid parameters: {exc}")
    for i, s in enumerate(_custom_scenarios):
        if s.name == scenario_name:
            _custom_scenarios[i] = cfg
            break
    save_custom_scenarios(_custom_scenarios)
    return {"status": "updated", "name": scenario_name}

@router.delete("/api/scenarios/{scenario_name}")
async def delete_scenario(scenario_name: str):
    existing = find_scenario(scenario_name)
    if existing is None:
        raise HTTPException(404, f"Scenario '{scenario_name}' not found")
    if existing.is_builtin:
        raise HTTPException(403, "Built-in scenarios cannot be deleted")
    _custom_scenarios[:] = [s for s in _custom_scenarios if s.name != scenario_name]
    save_custom_scenarios(_custom_scenarios)
    return {"status": "deleted", "name": scenario_name}

@router.post("/api/run/preset/{scenario_name}")
async def run_preset(scenario_name: str):
    if state.running:
        raise HTTPException(400, "A simulation is already running.")
    cfg = find_scenario(scenario_name)
    if not cfg:
        raise HTTPException(404, f"Unknown scenario: {scenario_name}")
    asyncio.create_task(run_simulation(cfg))
    return {"status": "started", "scenario": scenario_name}

@router.post("/api/run/custom")
async def run_custom(body: dict):
    if state.running:
        raise HTTPException(400, "A simulation is already running.")
    try:
        cfg = make_cfg_from_body(f"custom_{int(time.time())}", body)
    except Exception as exc:
        raise HTTPException(400, f"Invalid parameters: {exc}")
    asyncio.create_task(run_simulation(cfg))
    return {"status": "started", "scenario": cfg.name}

@router.post("/api/stop")
async def stop_simulation():
    if not state.running or state._runner is None:
        return {"status": "not_running"}
    state._runner.cancel()
    return {"status": "stopping"}

@router.get("/api/status")
async def get_status():
    return { "running": state.running, "scenario": state.scenario_name, "progress": state.progress}

@router.get("/api/stream")
async def sse_stream(request: Request):
    return EventSourceResponse(state.event_generator(request))

@router.get("/api/results")
async def get_results():
    return state.results

@router.get("/api/results/latest")
async def get_latest_result():
    return state.results[-1] if state.results else {}

@router.delete("/api/results")
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
        except Exception as exc:
            session.rollback()
            logger.warning(f"DB clear failed: {exc}")
        finally:
            session.close()
    return {"status": "cleared"}

@router.get("/api/ai_available")
async def ai_available():
    return {"available": bool(GROQ_API_KEY)}

@router.post("/api/interpret")
async def interpret_results(body: dict):
    results = body.get("results", [])
    focus   = body.get("focus", "general")
    if not results:
        raise HTTPException(400, "No results provided.")
    return await interpret(build_summaries(results), focus)