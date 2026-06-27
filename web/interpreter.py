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

Sensor → edge link. Configurable base delay + Gaussian jitter (one-way, ms) or LoRa airtime, Gilbert-Elliot two-state loss model (good/bad), max payload bytes, a FIXED
gateway token-bucket rate limit (does not scale with deployment size). Payloads are serialised with msgpack (a compact binary format); a bounded queue drops on overflow.
The sensors also share a fixed LoRa medium modelled as pure-ALOHA: overlapping transmissions on the same sub-channel collide and are lost, so collision loss and latency
RISE with the number of spots (the scalability sweep C1-C5 relies on this; frames_s2e_collisions reports it). The backhaul is point-to-point and has no contention.

Edge processing. Three modes: cloud_only (no edge - events forwarded raw direct to broker), edge_filtered (drop redundant repeats inside a duplicate window, drop quarantined
spots, forward singletons), edge_aggregated (batch events over aggregation_interval_s with max_batch_size and max_event_age_s caps, flush as one message).
Anomaly detection is a statistical detector: a deterministic sequence-integrity check (replayed/duplicated/out-of-order sequence numbers -> replay, flooding, flapping faults),
gap/dwell thresholds (silent sensor; stuck-in-occupied beyond the max plausible dwell), and population robust-z (MAD) outliers on per-spot event/flip rate. A spot is quarantined
only when its count of anomaly INCIDENTS inside a rolling persistence window stays elevated, so transient flags age out (precision is reported against the injected-fault ground truth).

Edge → broker link. A separate backhaul link with its own delay/jitter/loss, generally faster and more reliable than the sensor link (default 30 ms ± 10 ms, 0.1 % loss).

Protocols. MQTT (QoS 0 fire-and-forget, QoS 1 with PUBACK retransmit, QoS 2 four-way handshake), CoAP (NON fire-and-forget, CON with ACK retransmit and exponential backoff per RFC 7252; the same msgpack
payload as the others, with lighter UDP + CoAP framing overhead), AMQP (direct/topic/fanout exchanges, auto or manual ACK, optional durable). For each, the simulator tracks bytes_sent on the wire, 
retransmissions, and duplicate deliveries. 

What the latency numbers mean. End-to-end latency is sensor-emit timestamp to cloud-arrival timestamp, measured in virtual simulated time. It includes the
link propagation + jitter, the token-bucket wait under rate limiting, any aggregation window wait at the edge, and any protocol retransmit backoff. It does NOT include application-level processing on the cloud side.
With periodic edge aggregation, an event waits a uniformly random 0..aggregation_interval_s before the next flush, so the MEAN added latency is ~interval/2 and the MAX is ~interval
(NOT the full interval per event). A 15 s aggregation window therefore correctly yields a mean of ~7.5 s and a p95/max approaching 15 s, with the minimum near the bare network delay -
latencies below the interval are expected, not a bug.

aggregation_ratio = frames_e2c_sent / events_forwarded_total (≈1.0 means no batching; <1.0 means many forwarded events collapsed into fewer cloud frames).
message_reduction_ratio = 1 - frames_e2c_sent / frames_s2e_delivered (end-to-end reduction from filtering + aggregation).
Cloud intake: cloud_batches_received = protocol messages (batches) delivered; cloud_events_pre_dedup = events unpacked from them BEFORE the cloud deduplicates by (spot, sequence);
duplicate_events_at_cloud = events removed by that dedup (non-zero mainly under MQTT QoS1 and the replay/flooding faults); cloud_events_post_dedup = unique events kept.
e2e_unique_delivery_ratio = unique state changes applied at the cloud / state changes generated (event-flow reliability).
cloud_reflection_ratio = fraction of spots whose final cloud state matches sensor ground truth (state consistency, NOT delivery).
physical_delivery_ratio = first-pass survival (sensor-link × first-pass backhaul), before any retransmission.
Compare proto_bytes_sent across protocols only; link byte fields (bytes_s2e_*, bytes_e2c_*) use a different encoding basis and are not comparable to protocol bytes.

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
    "reliability": "Work through delivery: compare measured cloud_reflection_ratio and physical_delivery_ratio against the configured loss rates. Identify where retransmits visibly compensated for loss (look at retransmissions_total vs delivery), and where they did not (cloud_state_changes_reflected vs valid_state_changes). Say which configuration you would stake availability on, based on the data.",
    "bandwidth": "Calculate the byte savings of edge architectures vs cloud-only from the provided sensor_to_edge_bytes_kb / edge_to_cloud_bytes_kb / protocol_bytes_kb. Explain what aggregation_ratio and events_per_cloud_message imply about event burstiness. Where bytes differ between protocols, attribute the gap to the encoding/framing details the simulator models.",
    "protocol": "Compare protocols directly using the numbers in the data - latency tails, retransmissions_total, duplicate_deliveries, protocol_bytes_kb. Flag anything that contradicts the expected ordering and reason about why the simulator produced that result.",
    "architecture": "Quantify the trade-offs from the data: the latency cost of aggregation vs cloud_only, the message_reduction_ratio achieved by edge_filtered and edge_aggregated, and which architecture the numbers favour for the scenarios shown. Be explicit about which architecture each comparison is using.",
    "recommendation": "Based strictly on the runs provided, name the configuration that best balances the measured axes and explain why using its numbers. List the failure modes visible in the data (high P99, low cloud_reflection_ratio, retransmit storms) and what would mitigate them. Be explicit about what the simulator does not model.",
    "scale": "Focus on how latency, delivery, and message counts shift as num_spots grows. Identify which protocol or architecture degrades most gracefully according to the data. Avoid extrapolating to scales not represented in the provided runs.",
}

SINGLE_RUN_FOCUS_PROMPTS: dict[str, str] = {
    "general": "Give a close read of this single run: what its configuration (protocol, architecture, traffic level, spot count) would predict about its latency/reliability/bandwidth profile, and whether the measured numbers match that prediction. Call out anything in the numbers that looks off for this configuration.",
    "latency": "Examine this run's latency distribution: what the spread between mean, P50, P95, and P99 tells you about tail behaviour for this specific configuration. Attribute any large P99-vs-mean gap to a specific mechanism the simulator models (jitter, token-bucket queueing, aggregation window wait, retransmit backoff) given this run's link/edge settings.",
    "reliability": "Work through this run's delivery chain: compare cloud_reflection_ratio and physical_delivery_ratio against what the configured loss rates would predict. State whether retransmits (retransmissions_total) plausibly account for the gap between physical and cloud-level delivery. Say whether you'd trust this configuration for availability based on this run alone, and what you would NOT yet conclude from a single run.",
    "bandwidth": "Break down this run's byte budget: sensor_to_edge_bytes_kb vs edge_to_cloud_bytes_kb vs protocol_bytes_kb. Explain what aggregation_ratio and events_per_cloud_message imply about event burstiness for this run's traffic level and architecture.",
    "protocol": "Characterise this run's protocol behaviour using its own numbers - latency tail, retransmissions_total, duplicate_deliveries, protocol_bytes_kb. Note which of these are inherent to the protocol (e.g. QoS/ACK overhead) versus driven by this run's link conditions, and flag anything that looks inconsistent with the protocol's expected behaviour.",
    "architecture": "Explain what this run's architecture (cloud_only / edge_filtered / edge_aggregated) is doing to its numbers: the filtered/forwarded/aggregation_ratio figures and what they cost or save versus a hypothetical cloud_only baseline at the same traffic level - flagged clearly as a hypothetical since no such run is in the data.",
    "recommendation": "Based strictly on this one run, say whether its configuration looks production-ready and why, citing its own numbers. List any failure modes visible in this run (high P99, low cloud_reflection_ratio, retransmit storms) and what would mitigate them. Be explicit that a single run cannot establish robustness across seeds or conditions.",
    "scale": "This run has num_spots = a single value, so growth trends cannot be assessed. Instead, assess whether this run's per-spot message and byte rates (events_generated_total / heartbeats_generated_total / cloud_msgs_received, each divided by spots) look sustainable at this spot count, and name what would need to be measured (a sweep over num_spots) to actually answer a scaling question.",
}


def build_prompt(summaries: list[dict], focus: str) -> str:
    n = len(summaries)
    if n == 1:
        fi = SINGLE_RUN_FOCUS_PROMPTS.get(focus, SINGLE_RUN_FOCUS_PROMPTS["general"])
        run_desc = "Scenario result (1 run):"
        sections = (
            "Respond using these markdown sections - paragraphs only, no tables:\n\n"
            "### 🔍 Key Findings\n"
            "3–5 bullet points, each citing a specific number from the data.\n\n"
            "### 📊 Analysis\n"
            "2–3 paragraphs explaining the mechanisms behind the numbers for this configuration.\n\n"
            "### 💡 Non-Obvious Insights\n"
            "1–2 things that are surprising or only visible on close inspection of this run.\n\n"
            "### ✅ Recommendations\n"
            "3–4 concrete action items grounded in the data above, each tagged [Easy], [Medium], or [Hard]. "
            "At least one item should be about what additional run(s) would be needed to validate or stress-test this configuration further."
        )
    else:
        fi = FOCUS_PROMPTS.get(focus, FOCUS_PROMPTS["general"])
        run_desc = f"Scenario results ({n} runs):"
        sections = (
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
    return (
        f"{fi}\n\n"
        f"{run_desc}\n"
        f"{json.dumps(summaries, indent=2)}\n\n"
        f"{sections}"
    )


def build_summaries(results: list[dict]) -> list[dict]:
    def kb(v): return round((v or 0) / 1024, 2)
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
            "s2e_delivery_ratio": r.get("s2e_delivery_ratio"),
            "backhaul_delivery_ratio": r.get("backhaul_delivery_ratio"),
            "physical_delivery_ratio": r.get("physical_delivery_ratio"),
            "e2e_unique_delivery_ratio": r.get("e2e_unique_delivery_ratio"),
            "cloud_reflection_ratio": r.get("cloud_reflection_ratio"),
            "unique_state_changes_applied_at_cloud": r.get("unique_state_changes_applied_at_cloud"),
            "state_changes_generated_total": r.get("state_changes_generated_total"),
            "aggregation_ratio": r.get("aggregation_ratio"),
            "message_reduction_ratio": r.get("message_reduction_ratio"),
            "events_filtered_total": r.get("events_filtered_total"),
            "events_forwarded_total": r.get("events_forwarded_total"),
            "events_generated_total": r.get("events_generated_total"),
            "heartbeats_generated_total": r.get("heartbeats_generated_total"),
            "frames_s2e_sent": r.get("frames_s2e_sent"),
            "frames_e2c_sent": r.get("frames_e2c_sent"),
            "frames_e2c_delivered": r.get("frames_e2c_delivered"),
            "frames_e2c_dropped": r.get("frames_e2c_dropped"),
            "cloud_msgs_received": r.get("cloud_msgs_received"),
            "proto_retransmissions": r.get("proto_retransmissions"),
            "proto_duplicate_deliveries": r.get("proto_duplicate_deliveries"),
            "bytes_s2e_sent_kb": kb(r.get("bytes_s2e_sent")),
            "bytes_e2c_sent_kb": kb(r.get("bytes_e2c_sent")),
            "proto_bytes_sent_kb": kb(r.get("proto_bytes_sent")),
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


def _rule_based_single_run(s: dict) -> str:
    name = s.get("scenario") or "this run"
    proto = (s.get("protocol") or "?").upper()
    arch = s.get("architecture") or "?"
    traffic = s.get("traffic_level") or s.get("traffic") or "?"
    spots = s.get("spots") or s.get("num_spots")

    lines: list[str] = [f"## 📊 Simulation Analysis — `{name}`\n"]
    lines.append(f"*{proto} · {arch} · {traffic} traffic · {spots} spots*\n")

    lines.append("### 🔍 Key Findings\n")

    lat_mean = s.get("latency_mean_ms")
    lat_p95 = s.get("latency_p95_ms")
    lat_p99 = s.get("latency_p99_ms")
    if lat_mean is not None:
        lines.append(f"- ⏱️ **Latency**: mean {lat_mean:.1f} ms, P95 {lat_p95:.1f} ms, P99 {lat_p99:.1f} ms" if lat_p95 is not None and lat_p99 is not None else f"- ⏱️ **Latency**: mean {lat_mean:.1f} ms")
        if lat_p99 is not None and lat_mean and lat_p99 > lat_mean * 3:
            lines.append(f"- ⚠️ **Heavy tail**: P99 is {lat_p99 / lat_mean:.1f}× the mean — a minority of messages are waiting much longer than typical (jitter, queueing, or retransmit backoff)")

    refl = s.get("cloud_reflection_ratio")
    e2e = s.get("e2e_unique_delivery_ratio")
    if refl is not None:
        tag = "📦" if refl >= 0.98 else "⚠️"
        lines.append(f"- {tag} **Cloud state agreement**: {refl * 100:.2f}% of spots match sensor ground truth at the cloud")
    if e2e is not None and (refl is None or abs(e2e - refl) > 0.001 if refl is not None else True):
        lines.append(f"- 📨 **Unique delivery**: {e2e * 100:.2f}% of generated state changes were uniquely applied at the cloud")

    retransmits = s.get("proto_retransmissions")
    dups = s.get("proto_duplicate_deliveries")
    if retransmits:
        lines.append(f"- 🔁 **Retransmissions**: {retransmits} retransmission(s) recorded at the protocol layer")
    if dups:
        lines.append(f"- 🧬 **Duplicates**: {dups} duplicate delivery(ies) reached the cloud")

    agg = s.get("aggregation_ratio")
    msg_red = s.get("message_reduction_ratio")
    if agg is not None:
        lines.append(f"- 💾 **Aggregation ratio**: {agg:.3f} ({(1 - agg) * 100:.1f}% fewer cloud messages than forwarded events)")
    if msg_red is not None:
        lines.append(f"- 📉 **End-to-end message reduction**: {msg_red * 100:.1f}% fewer cloud messages than sensor-to-edge messages")

    lines.append("\n### 📊 Analysis\n")
    analysis_parts = []
    if lat_mean is not None and lat_p99 is not None:
        analysis_parts.append(
            f"At {proto} over `{arch}`, this run shows a mean end-to-end latency of {lat_mean:.1f} ms. "
            + (f"The P99 of {lat_p99:.1f} ms suggests link jitter and/or queueing dominate the tail rather than the mean case. "
               if lat_p99 > lat_mean * 2 else "The P99 stays close to the mean, suggesting a fairly stable link with little tail blow-up. ")
        )
    if refl is not None:
        analysis_parts.append(
            f"Cloud state agreement of {refl * 100:.2f}% "
            + ("indicates the sensor link and any edge/backhaul stages are reliably reflecting ground truth at the cloud. "
               if refl >= 0.98 else "is below the 98% mark — check the configured loss rates on the sensor and backhaul links, and whether retransmissions are configured for this protocol/QoS. ")
        )
    if arch != "cloud_only" and agg is not None:
        analysis_parts.append(
            f"With `{arch}`, the aggregation ratio of {agg:.3f} shows the edge is "
            + ("meaningfully batching events before forwarding to the cloud. " if agg < 0.9 else "passing through most events roughly 1:1, with little batching benefit at this traffic level. ")
        )
    if analysis_parts:
        lines.append(" ".join(analysis_parts) + "\n")
    else:
        lines.append("Limited metrics were available in this result to analyse in detail.\n")

    lines.append("\n### ✅ Recommendations\n")
    lines.append(f"- 🔎 **Treat this as one data point** — re-run `{name}` with a different seed to see how much of the above is run-to-run noise vs a structural property of the configuration [Easy]")
    if lat_p99 is not None and lat_mean is not None and lat_p99 > lat_mean * 3:
        lines.append("- 🧪 **Investigate the latency tail** — inspect link jitter, token-bucket rate limiting, or aggregation window settings that could be inflating P99 [Medium]")
    if refl is not None and refl < 0.98:
        lines.append("- 🛠️ **Improve delivery** — consider a higher QoS/ACK mode for this protocol or reduce configured loss rates, then re-run to confirm the effect [Medium]")
    lines.append("- 📐 **Compare against a baseline** — run the same traffic/spots with `cloud_only` (or a different protocol) and use 'All runs' or 'Last 3' scope to interpret the two side by side [Easy]")
    lines.append("\n\n*Built-in analysis - set `AI_API_KEY` in `.env` for Groq-powered insights.*")
    return "\n".join(lines)


def rule_based_interpret(summaries: list[dict]) -> str:
    if not summaries:
        return "No results to analyse."
    if len(summaries) == 1:
        return _rule_based_single_run(summaries[0])

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
            f"- 📦 **Best event delivery**: `{top['scenario']}` "
            f"at {top['e2e_unique_delivery_ratio'] * 100:.2f}% of generated state changes applied at cloud")
        if by_del[-1]["e2e_unique_delivery_ratio"] < 0.98:
            tail = by_del[-1]
            lines.append(
                f"- ⚠️ **Worst event delivery**: `{tail['scenario']}` - "
                f"{tail['e2e_unique_delivery_ratio'] * 100:.2f}% "
                f"({(1 - tail['e2e_unique_delivery_ratio']) * 100:.2f}% of state changes missed)")
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