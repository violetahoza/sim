from __future__ import annotations
import json
import logging
import httpx
from simulator.utils import get_groq_api_key

logger = logging.getLogger(__name__)

GROQ_API_KEY: str = get_groq_api_key()


SYSTEM_PROMPT = """\
You are analysing output from a discrete-event smart parking simulator. Ground every claim in the numbers provided in the user message. If the data does not support a claim, do not make it.

What the simulator actually models:

Traffic generation. Poisson arrivals scaled by an optional time-of-day curve, with parking dwell drawn from either a single log-normal (configurable CV) 
or a 90/10 short/long mixture (1500 s short, 14400 s long means). Each spot also emits periodic heartbeat messages at a configurable interval (default 60 s).\
The arrival rate is num_spots * base_rate where base_rate is 0.0028 / 0.0102 / 0.0182 events/s/spot for low / medium / peak.

Sensor → edge link. Configurable base delay + Gaussian jitter (one-way, ms), Gilbert-Elliot two-state loss model (good/bad), max payload bytes, 
token-bucket rate limit, and a payload encoding ratio that shrinks the serialised JSON to a wire size meant to mimic a compact binary encoding. A bounded queue drops on overflow.

Edge processing. Three modes: cloud_only (no edge - events forwarded raw direct to broker), edge_filtered (drop redundant repeats inside a duplicate window, drop quarantined 
spots, forward singletons), edge_aggregated (batch events over aggregation_interval_s with max_batch_size and max_event_age_s caps, flush as one message).
Anomaly detection runs five rules (R1 stuck, R2 silent, R3 rapid arrival, R4 stale sequence, R5 rapid state flip), with a cumulative-flag quarantine and a recovery rule based on consecutive valid events.

Edge → broker link. A separate backhaul link with its own delay/jitter/loss, generally faster and more reliable than the sensor link (default 30 ms ± 10 ms, 0.1 % loss).

Protocols. MQTT (QoS 0 fire-and-forget, QoS 1 with PUBACK retransmit, QoS 2 four-way handshake), CoAP (NON fire-and-forget, CON with ACK retransmit and exponential backoff per RFC 7252, with a CBOR encoding
ratio of 0.65 applied to bytes), AMQP (direct/topic/fanout exchanges, auto or manual ACK, optional durable). For each, the simulator tracks bytes_sent on the wire, 
retransmissions, and duplicate deliveries. Broker overhead is modelled as a small per-message service time (sub-millisecond at low load) plus an M/M/1
queueing approximation captured by broker_overhead_score (estimated mean wait in ms - higher = more queueing).

What the latency numbers mean. End-to-end latency is sensor-emit timestamp to cloud-arrival timestamp, measured in virtual simulated time. It includes the 
link propagation + jitter, the token-bucket wait under rate limiting, any aggregation window wait at the edge, and any protocol retransmit backoff. It does NOT include application-level processing on the cloud side.

What aggregation_ratio means. It is edge_to_cloud_msgs / sensor_to_edge_msgs_received_at_edge. Below 1.0 means the edge reduces message count to the cloud; above 1.0 would mean the edge added overhead.
message_reduction_ratio = 1 - aggregation_ratio, clamped at 0.

What cloud_reflection_ratio means. The fraction of real state changes (arrivals + departures) that produced a corresponding transition in the cloud's view. 
Distinct from physical_delivery_ratio, which is the product of per-link delivery ratios and reflects packet-level survival.

When you write your answer:
- Quote numbers from the provided summaries. Do not invent figures.
- Do not give per-protocol millisecond claims unless the data shows them.
- Do not claim CBOR or any encoding saves a specific percentage unless the bytes in the data confirm it.
- Note explicitly when a difference between scenarios is small enough that it could be within run-to-run noise.
- Write in prose, no tables.
"""

FOCUS_PROMPTS: dict[str, str] = {
    "general": "Give an end-to-end assessment grounded in the provided numbers: which configuration comes out ahead on which axis, how the latency/reliability/bandwidth numbers relate to each other, and where the first breaking point appears as conditions change.",
    "latency": "Examine latency closely: what the spread between mean, P50, P95, and P99 tells you about tail behaviour and scheduling jitter. Where P99 is much larger than mean, attribute it to a specific mechanism the simulator models (jitter, token-bucket queueing, aggregation window wait, retransmit backoff) using the configuration details visible in the summaries.",
    "reliability": "Work through delivery: compare measured cloud_reflection_ratio and physical_delivery_ratio against the configured loss rates. Identify where retransmits visibly compensated for loss (look at retransmissions_total vs delivery), and where they did not (events_reflected_in_cloud vs valid_state_changes). Say which configuration you would stake availability on, based on the data.",
    "bandwidth": "Calculate the byte savings of edge architectures vs cloud-only from the provided sensor_to_edge_bytes / edge_to_cloud_bytes / protocol_bytes. Explain what aggregation_ratio and events_per_cloud_message imply about event burstiness. Where bytes differ between protocols, attribute the gap to the encoding/framing details the simulator models.",
    "protocol": "Compare protocols directly using the numbers in the data - latency tails, retransmissions_total, duplicate_deliveries, protocol_bytes, broker_overhead_score. Flag anything that contradicts the expected ordering and reason about why the simulator produced that result.",
    "architecture": "Quantify the trade-offs from the data: the latency cost of aggregation vs cloud_only, the message_reduction_ratio achieved by edge_filtered and edge_aggregated, and which architecture the numbers favour for the scenarios shown. Be explicit about which architecture each comparison is using.",
    "recommendation": "Based strictly on the runs provided, name the configuration that best balances the measured axes and explain why using its numbers. List the failure modes visible in the data (high P99, low cloud_reflection_ratio, retransmit storms) and what would mitigate them. Be explicit about what the simulator does not model.",
    "scale": "Focus on how latency, delivery, and message counts shift as num_spots grows. Identify which protocol or architecture degrades most gracefully according to the data. Avoid extrapolating to scales not represented in the provided runs.",
}


def build_prompt(summaries: list[dict], focus: str) -> str:
    fi = FOCUS_PROMPTS.get(focus, FOCUS_PROMPTS["general"])
    n = len(summaries)
    return (
        f"{fi}\n\n"
        f"Scenario results ({n} run{'s' if n != 1 else ''}):\n"
        f"{json.dumps(summaries, indent=2)}\n\n"
        "Respond using these markdown sections - paragraphs only, no tables:\n\n"
        "### 🔍 Key Findings\n"
        "3–5 bullet points, each citing a specific number from the data.\n\n"
        "### 📊 Analysis\n"
        "2–3 paragraphs explaining the mechanisms behind the numbers.\n\n"
        "### 💡 Non-Obvious Insights\n"
        "1–2 things that are surprising or only visible on close inspection.\n\n"
        "### ✅ Recommendations\n"
        "3–4 concrete action items grounded in the data above, each tagged [Easy], [Medium], or [Hard]."
    )


def build_summaries(results: list[dict]) -> list[dict]:
    return [
        {
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
            "sensor_to_edge_delivery_ratio": r.get("sensor_to_edge_delivery_ratio"),
            "edge_to_cloud_delivery_ratio": r.get("edge_to_cloud_delivery_ratio"),
            "physical_delivery_ratio": r.get("physical_delivery_ratio"),
            "cloud_reflection_ratio": r.get("cloud_reflection_ratio"),
            "aggregation_ratio": r.get("aggregation_ratio"),
            "message_reduction_ratio": r.get("message_reduction_ratio"),
            "events_per_cloud_message": r.get("events_per_cloud_message"),
            "filtered_events": r.get("filtered_events"),
            "events_generated": r.get("events_generated"),
            "heartbeats_generated": r.get("heartbeats_generated"),
            "valid_state_changes": r.get("valid_state_changes"),
            "sensor_to_edge_msgs": r.get("sensor_to_edge_msgs"),
            "edge_to_cloud_msgs": r.get("edge_to_cloud_msgs"),
            "events_reflected_in_cloud": r.get("events_reflected_in_cloud"),
            "retransmissions_total": r.get("retransmissions_total"),
            "duplicate_deliveries": r.get("duplicate_deliveries"),
            "sensor_to_edge_bytes_kb": round(r.get("sensor_to_edge_bytes", 0) / 1024, 2),
            "edge_to_cloud_bytes_kb": round(r.get("edge_to_cloud_bytes", 0) / 1024, 2),
            "protocol_bytes_kb": round(r.get("protocol_bytes", 0) / 1024, 2),
            "broker_overhead_score_ms": r.get("broker_overhead_score")
        }
        for r in results
    ]


async def call_groq(system: str, user: str) -> str:
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.3,
                "max_tokens": 1500,
            },
        )
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"]
        raise RuntimeError(f"Groq {r.status_code}: {r.json().get('error', {}).get('message', r.text)}")


async def interpret(summaries: list[dict], focus: str) -> dict:
    if GROQ_API_KEY:
        try:
            text = await call_groq(SYSTEM_PROMPT, build_prompt(summaries, focus))
            return {"interpretation": text, "model": "llama-3.3-70b", "powered_by": "Groq"}
        except Exception as exc:
            logger.warning(f"Groq call failed: {exc}")
            return {
                "interpretation": rule_based_interpret(summaries),
                "model": "rule-based",
                "powered_by": "Built-in Analyzer",
                "warning": f"Groq unavailable ({exc}) - showing built-in analysis",
            }
    return {"interpretation": rule_based_interpret(summaries), "model": "rule-based", "powered_by": "Built-in Analyzer"}


def rule_based_interpret(summaries: list[dict]) -> str:
    if not summaries:
        return "No results to analyse."

    lines: list[str] = ["## 📊 Simulation Analysis\n"]

    by_lat = sorted([s for s in summaries if s.get("latency_mean_ms")], key=lambda x: x["latency_mean_ms"])
    by_del = sorted(
        [s for s in summaries if s.get("cloud_reflection_ratio") is not None],
        key=lambda x: x["cloud_reflection_ratio"],
        reverse=True
    )

    lines.append("### 🔍 Key Findings\n")
    if by_lat:
        best, worst = by_lat[0], by_lat[-1]
        lines.append(
            f"- 🥇 **Lowest latency**: `{best['scenario']}` - "
            f"mean {best['latency_mean_ms']:.1f} ms, P95 {best.get('latency_p95_ms', '?')} ms"
        )
        if len(by_lat) > 1 and worst["latency_mean_ms"] > best["latency_mean_ms"]:
            diff = worst["latency_mean_ms"] - best["latency_mean_ms"]
            lines.append(
                f"- 🐢 **Highest latency**: `{worst['scenario']}` - "
                f"{worst['latency_mean_ms']:.1f} ms ({diff:.1f} ms slower than best)"
            )
    if by_del:
        top = by_del[0]
        lines.append(
            f"- 📦 **Best cloud reflection**: `{top['scenario']}` "
            f"at {top['cloud_reflection_ratio'] * 100:.2f}% of real state changes captured"
        )
        if by_del[-1]["cloud_reflection_ratio"] < 0.98:
            tail = by_del[-1]
            lines.append(
                f"- ⚠️ **Worst cloud reflection**: `{tail['scenario']}` - "
                f"{tail['cloud_reflection_ratio'] * 100:.2f}% "
                f"({(1 - tail['cloud_reflection_ratio']) * 100:.2f}% of state changes missed)"
            )
    agg_list = [s for s in summaries if s.get("aggregation_ratio") and s["aggregation_ratio"] < 0.9]
    if agg_list:
        best_agg = min(agg_list, key=lambda x: x["aggregation_ratio"])
        lines.append(
            f"- 💾 **Best message reduction**: `{best_agg['scenario']}` - "
            f"{(1 - best_agg['aggregation_ratio']) * 100:.1f}% fewer messages to cloud "
            f"(aggregation_ratio {best_agg['aggregation_ratio']:.3f})"
        )

    lines.append("\n### 📊 Analysis\n")
    protos: dict[str, list] = {}
    for s in summaries:
        protos.setdefault(s.get("protocol", "?"), []).append(s)
    if len(protos) > 1:
        parts = []
        for p, runs in protos.items():
            lats = [r["latency_mean_ms"] for r in runs if r.get("latency_mean_ms")]
            if lats:
                parts.append(f"**{p.upper()}** avg {sum(lats) / len(lats):.1f} ms across {len(lats)} run(s)")
        lines.append("Protocol latency averages across the provided runs: " + ", ".join(parts) + ".\n")

    archs: dict[str, list] = {}
    for s in summaries:
        archs.setdefault(s.get("architecture", "?"), []).append(s)
    if len(archs) > 1:
        parts = []
        for a, runs in archs.items():
            lats = [r["latency_mean_ms"] for r in runs if r.get("latency_mean_ms")]
            bw = sum(r.get("edge_to_cloud_bytes_kb", 0) for r in runs) / max(len(runs), 1)
            if lats:
                parts.append(f"`{a}` averaged {sum(lats) / len(lats):.1f} ms, {bw:.1f} KB to cloud")
        lines.append("Architecture comparison: " + "; ".join(parts) + ".\n")

    lines.append("\n### ✅ Recommendations\n")
    balanced = sorted(
        [s for s in summaries
         if (s.get("cloud_reflection_ratio") or 0) > 0.95 and s.get("latency_mean_ms")],
        key=lambda x: x["latency_mean_ms"],
    )
    if balanced:
        r = balanced[0]
        lines.append(
            f"- ✅ **Best balanced run in this set**: `{r['scenario']}` - "
            f"{r['latency_mean_ms']:.1f} ms mean latency with "
            f"{r['cloud_reflection_ratio'] * 100:.2f}% cloud reflection [Easy]"
        )
    lines.append("- 🔎 **Repeat with multiple seeds** before drawing strong conclusions - single-run differences below a few percent may be noise [Easy]")
    lines.append("- 🧪 **Sweep one variable at a time** (e.g. aggregation_interval, loss_rate, or QoS) to separate effects [Medium]")
    lines.append("\n\n*Built-in analysis - set `AI_API_KEY` in `.env` for Groq-powered insights.*")
    return "\n".join(lines)