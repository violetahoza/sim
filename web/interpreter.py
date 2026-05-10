from __future__ import annotations
import json
import logging
import httpx
from simulator.utils import get_groq_api_key

logger = logging.getLogger(__name__)

GROQ_API_KEY: str = get_groq_api_key()


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

FOCUS_PROMPTS: dict[str, str] = {
    "general": "Give an end-to-end assessment: which configuration comes out ahead and why, how the latency/reliability/bandwidth numbers relate to each other, whether the results match what protocol theory would predict, and where the first real-world breaking point would be.",
    "latency": "Examine latency closely: what the spread between mean, P50, P95, and P99 tells you about tail behaviour and scheduling jitter, how the observed numbers map to the configured link delay, and whether any P99 spikes are traceable to retransmit cycles or aggregation window alignment.",
    "reliability": "Work through delivery reliability: compare measured delivery ratios against the configured loss rate for each scenario, identify which protocol mechanisms visibly compensated for loss, note where retransmit overhead shows up in latency, and say which configuration you would stake availability on.",
    "bandwidth": "Dig into bandwidth: calculate the exact byte savings edge aggregation delivers over cloud-only, explain what the aggregation_ratio says about event burstiness, locate the bottleneck link, and give a rough projection for a 10 000-sensor deployment running 24 hours a day.",
    "protocol": "Compare the protocols directly against each other: where does MQTT's QoS tiering, AMQP's broker guarantees, or CoAP's binary encoding actually move the numbers rather than just the theory? Note anything that contradicts the expected ordering.",
    "architecture": "Quantify the architecture trade-offs: measure the latency penalty and bandwidth saving of edge aggregation relative to cloud-only, assess how much filtering actually reduces cloud load, and give concrete guidance on which architecture fits a street-side installation versus a large structured car park.",
    "recommendation": "Write a deployment recommendation: name the single best protocol and architecture combination with the reasoning behind it, list the three most likely failure modes and what would mitigate them, and be explicit about what this simulation does not model.",
    "scale": "Focus on the scalability runs: describe how latency and delivery ratio shift as sensor count grows, identify which protocol holds up best under load, and estimate what the numbers imply for gateway hardware at 10 000 sensors.",
}


def build_prompt(summaries: list[dict], focus: str) -> str:
    fi = FOCUS_PROMPTS.get(focus, FOCUS_PROMPTS["general"])
    n = len(summaries)
    return (
        f"{fi}\n\n"
        f"Scenario results ({n} run{'s' if n != 1 else ''}):\n"
        f"{json.dumps(summaries, indent=2)}\n\n"
        "Respond using these markdown sections — paragraphs only, no tables:\n\n"
        "### 🔍 Key Findings\n"
        "3–5 bullet points, each with a specific number from the data.\n\n"
        "### 📊 Analysis\n"
        "2–3 paragraphs explaining the mechanisms behind the numbers.\n\n"
        "### 💡 Non-Obvious Insights\n"
        "1–2 things that are surprising or only visible on close inspection.\n\n"
        "### ✅ Recommendations\n"
        "3–4 concrete action items, each tagged [Easy], [Medium], or [Hard]."
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
            "delivery_ratio": r.get("sensor_to_edge_delivery_ratio"),
            "aggregation_ratio": r.get("aggregation_ratio"),
            "filtered_events": r.get("filtered_events"),
            "sensor_to_edge_bytes_kb": round(r.get("sensor_to_edge_bytes", 0) / 1024, 2),
            "edge_to_cloud_bytes_kb": round(r.get("edge_to_cloud_bytes", 0) / 1024, 2),
            "sensor_to_edge_msgs": r.get("sensor_to_edge_msgs"),
            "edge_to_cloud_msgs": r.get("edge_to_cloud_msgs")
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
                    {"role": "user",   "content": user},
                ],
                "temperature": 0.3,
                "max_tokens": 1500
            }
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
                "warning": f"Groq unavailable ({exc}) — showing built-in analysis"
            }
    return {"interpretation": rule_based_interpret(summaries), "model": "rule-based", "powered_by": "Built-in Analyzer"}

def rule_based_interpret(summaries: list[dict]) -> str:
    if not summaries:
        return "No results to analyse."

    lines: list[str] = ["## 📊 Simulation Analysis\n"]

    by_lat = sorted(
        [s for s in summaries if s.get("latency_mean_ms")],
        key=lambda x: x["latency_mean_ms"],
    )
    by_del = sorted(
        [s for s in summaries if s.get("delivery_ratio") is not None],
        key=lambda x: x["delivery_ratio"],
        reverse=True
    )

    lines.append("### 🔍 Key Findings\n")
    if by_lat:
        best, worst = by_lat[0], by_lat[-1]
        lines.append(
            f"- 🥇 **Lowest latency**: `{best['scenario']}` — "
            f"mean {best['latency_mean_ms']:.1f} ms, P95 {best.get('latency_p95_ms', '?')} ms"
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
            f"at {by_del[0]['delivery_ratio'] * 100:.2f}%"
        )
        if by_del[-1]["delivery_ratio"] < 0.98:
            lines.append(
                f"- ⚠️ **Worst delivery**: `{by_del[-1]['scenario']}` — "
                f"{by_del[-1]['delivery_ratio'] * 100:.2f}% "
                f"({(1 - by_del[-1]['delivery_ratio']) * 100:.2f}% loss)"
            )
    agg_list = [s for s in summaries if s.get("aggregation_ratio") and s["aggregation_ratio"] < 0.9]
    if agg_list:
        best_agg = min(agg_list, key=lambda x: x["aggregation_ratio"])
        lines.append(
            f"- 💾 **Best compression**: `{best_agg['scenario']}` — "
            f"{(1 - best_agg['aggregation_ratio']) * 100:.1f}% fewer cloud messages "
            f"(ratio {best_agg['aggregation_ratio']:.3f})"
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
                parts.append(f"**{p.upper()}** avg {sum(lats) / len(lats):.1f} ms")
        lines.append("Protocol latency averages: " + ", ".join(parts) + ".\n")

    archs: dict[str, list] = {}
    for s in summaries:
        archs.setdefault(s.get("architecture", "?"), []).append(s)
    if len(archs) > 1:
        parts = []
        for a, runs in archs.items():
            lats = [r["latency_mean_ms"] for r in runs if r.get("latency_mean_ms")]
            bw = sum(r.get("sensor_to_edge_bytes_kb", 0) for r in runs) / max(len(runs), 1)
            if lats:
                parts.append(f"`{a}` averaged {sum(lats) / len(lats):.1f} ms, {bw:.1f} KB S→E")
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
                f"{r['latency_mean_ms']:.1f} ms with {r['delivery_ratio'] * 100:.2f}% delivery [Easy]"
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