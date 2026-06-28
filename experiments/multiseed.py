from __future__ import annotations

import argparse
import copy
import csv
import json
import logging
import sys
import time
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from simulator.config.config import PREDEFINED_SCENARIOS, ScenarioConfig, load_custom_scenarios
from experiments.runner import run_scenario_sync, save_results  
from experiments.aggregate import aggregate, HEADLINE_METRICS 

logger = logging.getLogger("multiseed")

DEFAULT_BASE_SEED = 1000
DEFAULT_NUM_SEEDS = 5

PRIMARY_METRICS = ["latency_mean_ms", "latency_p99_ms", "e2e_unique_delivery_ratio", "message_reduction_ratio", "proto_bytes_sent"]

DETAIL_COLS = ["protocol", "architecture", "traffic_level", "num_spots", "sim_duration_s", "access_loss", "backhaul_loss", "agg_interval_s",
                "mqtt_qos", "coap_mode", "amqp_exchange",  "amqp_ack", "amqp_durable", "heartbeat_interval_s"]


def _all_scenarios(include_custom: bool) -> list[ScenarioConfig]:
    scns = list(PREDEFINED_SCENARIOS)
    if include_custom:
        scns += load_custom_scenarios()
    return scns


def _resolve_scenarios(names: list[str], run_all: bool, include_custom: bool) -> list[ScenarioConfig]:
    if run_all or not names:
        return _all_scenarios(include_custom)
    registry = {s.name: s for s in _all_scenarios(include_custom=True)}
    chosen, missing = [], []
    for n in names:
        if n in registry:
            chosen.append(registry[n])
        else:
            missing.append(n)
    if missing:
        avail = ", ".join(sorted(registry))
        raise SystemExit(f"Unknown scenario(s): {', '.join(missing)}\nAvailable: {avail}")
    return chosen


def _resolve_seeds(args: argparse.Namespace) -> list[int]:
    if args.seed_list:
        seeds = [int(x) for x in args.seed_list.split(",") if x.strip() != ""]
        if not seeds:
            raise SystemExit("--seed-list was empty after parsing")
        return seeds
    base = args.base_seed if args.base_seed is not None else DEFAULT_BASE_SEED
    return [base + i for i in range(max(1, args.seeds))]


def _clone_with_seed(cfg: ScenarioConfig, seed: int) -> ScenarioConfig:
    c = copy.deepcopy(cfg)
    c.random_seed = int(seed)
    if getattr(c, "traffic", None) is not None:
        c.traffic.random_seed = int(seed)
    return c


def _scenario_details(cfg: ScenarioConfig) -> dict:
    link = getattr(cfg, "link", None)
    bh = getattr(cfg, "backhaul_link", None)
    edge = getattr(cfg, "edge", None)
    return {
        "protocol": cfg.protocol,
        "architecture": cfg.architecture,
        "traffic_level": cfg.traffic_level,
        "num_spots": cfg.num_spots,
        "sim_duration_s": cfg.sim_duration_s,
        "access_loss": getattr(link, "packet_loss_rate", None),
        "backhaul_loss": getattr(bh, "packet_loss_rate", None),
        "agg_interval_s": getattr(edge, "aggregation_interval_s", None),
        "mqtt_qos": getattr(getattr(cfg, "mqtt", None), "qos", None),
        "coap_mode": getattr(getattr(cfg, "coap", None), "mode", None),
        "amqp_exchange": getattr(getattr(cfg, "amqp", None), "exchange_type", None),
        "amqp_ack": getattr(getattr(cfg, "amqp", None), "ack_mode", None),
        "amqp_durable": getattr(getattr(cfg, "amqp", None), "durable", None),
        "heartbeat_interval_s": getattr(getattr(cfg, "traffic", None), "heartbeat_interval_s", None)
    }


def run_batch(scenarios: list[ScenarioConfig], seeds: list[int], batch_dir: Path, steps: int) -> tuple[dict, list, list, float]:
    batch_dir.mkdir(parents=True, exist_ok=True)
    details: dict[str, dict] = {}
    timings: list[dict] = []
    failures: list[dict] = []

    total = len(scenarios) * len(seeds)
    idx = 0
    t_batch = time.time()

    for si, cfg in enumerate(scenarios, 1):
        details[cfg.name] = _scenario_details(cfg)
        for seed in seeds:
            idx += 1
            label = f"[{si}/{len(scenarios)} {cfg.name}] seed={seed} (run {idx}/{total})"
            logger.info("%s starting...", label)
            t_run = time.time()
            try:
                metrics = run_scenario_sync(_clone_with_seed(cfg, seed), steps=steps)
                save_results(metrics, str(batch_dir))
                dt = time.time() - t_run
                timings.append({"scenario": cfg.name, "seed": seed, "seconds": round(dt, 2)})
                logger.info("%s done in %.1fs", label, dt)
            except Exception as exc: 
                dt = time.time() - t_run
                logger.error("%s FAILED after %.1fs: %s", label, dt, exc)
                failures.append({
                    "scenario": cfg.name, "seed": seed,
                    "error": str(exc), "traceback": traceback.format_exc()
                })

    return details, timings, failures, time.time() - t_batch



def _fmt_ci(s: dict | None) -> str:
    if s is None or s.get("mean") is None:
        return "n/a"
    if s.get("ci95_halfwidth") is None:
        return f"{s['mean']:.4g} (n={s['n']}, single run \u2014 no CI)"
    return (f"{s['mean']:.4g} \u00b1 {s['ci95_halfwidth']:.4g}  "
            f"[{s['ci95_low']:.4g}, {s['ci95_high']:.4g}] (n={s['n']})")


def _write_report_csv(path: Path, summary: dict, details: dict) -> None:
    fieldnames = (["scenario"] + DETAIL_COLS + ["n_runs", "seeds", "metric", "mean", "std", "ci95_low", "ci95_high", "ci95_halfwidth"])
    rows = []
    for scn in sorted(summary):
        block = summary[scn]
        d = details.get(scn, {})
        seeds_str = ";".join(str(s) for s in block.get("seeds", []))
        for metric in HEADLINE_METRICS:
            s = block["metrics"].get(metric)
            if s is None:
                continue
            row = {
                "scenario": scn,
                "n_runs": block.get("num_runs"),
                "seeds": seeds_str,
                "metric": metric,
                "mean": s.get("mean"),
                "std": s.get("std"),
                "ci95_low": s.get("ci95_low"),
                "ci95_high": s.get("ci95_high"),
                "ci95_halfwidth": s.get("ci95_halfwidth")
            }
            for c in DETAIL_COLS:
                row[c] = d.get(c)
            rows.append(row)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _write_report_log(path: Path, summary: dict, details: dict, seeds: list[int], failures: list, wall: float, args: argparse.Namespace) -> None:
    lines: list[str] = []
    lines.append("Multi-seed confidence-interval report")
    lines.append("=" * 78)
    lines.append(f"Generated (UTC) : {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    lines.append(f"Scenarios       : {len(summary)}")
    lines.append(f"Seeds           : {seeds}  (n={len(seeds)} per scenario)")
    lines.append(f"DES steps       : {args.steps}")
    lines.append(f"Total runs      : {len(summary) * len(seeds)}")
    lines.append(f"Failed runs     : {len(failures)}")
    lines.append(f"Wall time       : {wall:.1f}s")
    if len(seeds) < 2:
        lines.append("NOTE            : n < 2 \u2014 no confidence interval can be computed.")
    lines.append("")
    lines.append("CI shown as: mean \u00b1 95% CI half-width  [low, high] (n)   [Student-t, ddof=1]")
    lines.append("")

    for scn in sorted(summary):
        block = summary[scn]
        d = details.get(scn, {})
        lines.append("-" * 78)
        lines.append(f"SCENARIO: {scn}")
        lines.append(
            f"  {d.get('protocol')}/{d.get('architecture')}/{d.get('traffic_level')}   "
            f"spots={d.get('num_spots')}   dur={d.get('sim_duration_s')}s   "
            f"n_runs={block.get('num_runs')}"
        )
        lines.append(
            f"  access_loss={d.get('access_loss')}  backhaul_loss={d.get('backhaul_loss')}  "
            f"agg_interval_s={d.get('agg_interval_s')}  hb_interval_s={d.get('heartbeat_interval_s')}"
        )
        lines.append(
            f"  mqtt_qos={d.get('mqtt_qos')}  coap_mode={d.get('coap_mode')}  "
            f"amqp=({d.get('amqp_exchange')},{d.get('amqp_ack')},durable={d.get('amqp_durable')})"
        )
        lines.append(f"  seeds={block.get('seeds')}")
        lines.append("")
        for metric in HEADLINE_METRICS:
            s = block["metrics"].get(metric)
            lines.append(f"    {metric:34s} {_fmt_ci(s)}")
        lines.append("")

    if failures:
        lines.append("=" * 78)
        lines.append("FAILURES")
        for fl in failures:
            lines.append(f"  {fl['scenario']} seed={fl['seed']}: {fl['error']}")
        lines.append("  (full tracebacks in _batch_manifest.json)")
        lines.append("")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_reports(batch_dir: Path, details: dict, seeds: list[int], timings: list, failures: list, wall: float, args: argparse.Namespace) -> tuple[dict, Path, Path]:
    agg_dir = batch_dir / "aggregated"
    summary = aggregate(batch_dir, agg_dir)

    report_csv = batch_dir / "multiseed_report.csv"
    report_log = batch_dir / "multiseed_report.log"
    _write_report_csv(report_csv, summary, details)
    _write_report_log(report_log, summary, details, seeds, failures, wall, args)

    manifest = {
        "batch_dir": str(batch_dir),
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scenarios": list(details),
        "seeds": seeds,
        "num_runs_planned": len(details) * len(seeds),
        "num_runs_failed": len(failures),
        "wall_seconds": round(wall, 1),
        "steps": args.steps,
        "timings": timings,
        "failures": failures,
        "scenario_details": details
    }
    (batch_dir / "_batch_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return summary, report_csv, report_log


def _print_console_summary(summary: dict) -> None:
    print("\n" + "=" * 78)
    print("MULTI-SEED SUMMARY (mean \u00b1 95% CI half-width)")
    print("=" * 78)
    for scn in sorted(summary):
        block = summary[scn]
        print(f"\n{scn}  (n={block.get('num_runs')}, "
              f"{block.get('protocol')}/{block.get('architecture')}/{block.get('traffic_level')})")
        for metric in PRIMARY_METRICS:
            s = block["metrics"].get(metric)
            if s is None or s.get("mean") is None:
                continue
            print(f"    {metric:32s} {_fmt_ci(s)}")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="python -m experiments.multiseed",
        description="Run scenarios across multiple seeds and report 95% confidence intervals.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("scenarios", nargs="*", help="Scenario name(s) to run. Omit (or use --all) to run all predefined scenarios.")
    ap.add_argument("--all", action="store_true", help="Run all predefined scenarios (add --include-custom to also include user scenarios).")
    ap.add_argument("--include-custom", action="store_true", help="With --all, also include custom (user) scenarios.")
    ap.add_argument("--seeds", type=int, default=DEFAULT_NUM_SEEDS, help=f"Number of seeds per scenario (default {DEFAULT_NUM_SEEDS}).")
    ap.add_argument("--base-seed", type=int, default=None, help=f"First seed; seeds are base..base+seeds-1 (default {DEFAULT_BASE_SEED}).")
    ap.add_argument("--seed-list", default=None, help="Explicit comma-separated seeds, e.g. 7,8,9 (overrides --seeds/--base-seed).")
    ap.add_argument("--out-dir", default="results/multiseed", help="Base output directory; a batch_<UTC> subfolder is created inside it.")
    ap.add_argument("--tag", default=None, help="Optional label appended to the batch folder name.")
    ap.add_argument("--steps", type=int, default=1, help="DES progress granularity (default 1; does not affect results).")
    ap.add_argument("--list", action="store_true", help="List available scenario names and exit.")
    ap.add_argument("--quiet", action="store_true", help="Silence the per-run runner INFO logs.")
    return ap


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    if args.quiet:
        logging.getLogger("experiments.runner").setLevel(logging.WARNING)

    if args.list:
        for s in _all_scenarios(include_custom=True):
            tag = "" if s.is_builtin else "  (custom)"
            print(f"{s.name:30s} {s.protocol:5s} {s.architecture:16s} {s.traffic_level}{tag}")
        return

    scenarios = _resolve_scenarios(args.scenarios, args.all, args.include_custom)
    seeds = _resolve_seeds(args)
    if len(seeds) < 2:
        logger.warning("Only %d seed(s) requested; a confidence interval needs n >= 2.", len(seeds))

    out_base = Path(args.out_dir)
    if not out_base.is_absolute():
        out_base = REPO_ROOT / out_base
    batch_name = "batch_" + time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    if args.tag:
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in args.tag)
        batch_name += "_" + safe
    batch_dir = out_base / batch_name

    logger.info("Multi-seed batch: %d scenario(s) x %d seed(s) = %d run(s)", len(scenarios), len(seeds), len(scenarios) * len(seeds))
    logger.info("Seeds : %s", seeds)
    logger.info("Output: %s", batch_dir)

    details, timings, failures, wall = run_batch(scenarios, seeds, batch_dir, args.steps)
    summary, report_csv, report_log = write_reports(batch_dir, details, seeds, timings, failures, wall, args)

    _print_console_summary(summary)
    print()
    print(f"Per-run JSONs : {batch_dir}")
    print(f"Aggregated    : {batch_dir / 'aggregated'}")
    print(f"Report (CSV)  : {report_csv}")
    print(f"Report (log)  : {report_log}")
    print(f"Manifest      : {batch_dir / '_batch_manifest.json'}")
    if failures:
        print(f"\nWARNING: {len(failures)} run(s) failed \u2014 see the manifest for tracebacks.")


if __name__ == "__main__":
    main()