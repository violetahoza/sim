from __future__ import annotations
import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

HEADLINE_METRICS = [
    "latency_mean_ms", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
    "e2e_unique_delivery_ratio", "cloud_reflection_ratio",
    "physical_delivery_ratio", "backhaul_delivery_ratio",
    "message_reduction_ratio", "aggregation_ratio",
    "proto_bytes_sent", "frames_e2c_sent", "frames_e2c_delivered",
    "bytes_e2c_sent", "bytes_s2e_sent",
    "proto_retransmissions", "proto_duplicate_deliveries",
    "unique_state_changes_applied_at_cloud", "duplicate_events_at_cloud"
]

_T_975 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120, 
    17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042
}


def _t_critical(df: int) -> float:
    if df <= 0:
        return float("nan")
    if df in _T_975:
        return _T_975[df]
    return 1.960  


def _summarise(values: list[float]) -> dict:
    n = len(values)
    if n == 0:
        return {"n": 0, "mean": None, "std": None, "ci95_low": None, "ci95_high": None, "ci95_halfwidth": None}
    mean = statistics.fmean(values)
    if n == 1:
        return {"n": 1, "mean": round(mean, 6), "std": None, "ci95_low": None, "ci95_high": None, "ci95_halfwidth": None}
    std = statistics.stdev(values)  # sample std, ddof=1
    half = _t_critical(n - 1) * std / math.sqrt(n)
    return { "n": n, "mean": round(mean, 6), "std": round(std, 6), "ci95_low": round(mean - half, 6), "ci95_high": round(mean + half, 6), "ci95_halfwidth": round(half, 6)}


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and not (
        isinstance(v, float) and math.isnan(v))


def load_runs(results_dir: Path) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for path in sorted(results_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        name = data.get("scenario_name")
        if name:
            groups[name].append(data)
    return groups


def aggregate(results_dir: Path, out_dir: Path) -> dict:
    groups = load_runs(results_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, dict] = {}
    for scenario, runs in sorted(groups.items()):
        seeds = sorted({r.get("seed") for r in runs if r.get("seed") is not None})
        run_ids = [r.get("run_id") for r in runs]
        metrics_out: dict[str, dict] = {}
        for metric in HEADLINE_METRICS:
            vals = [r[metric] for r in runs if metric in r and _is_number(r.get(metric))]
            metrics_out[metric] = _summarise([float(v) for v in vals])

        scenario_block = {
            "scenario_name": scenario,
            "protocol": runs[0].get("protocol"),
            "architecture": runs[0].get("architecture"),
            "traffic_level": runs[0].get("traffic_level"),
            "num_runs": len(runs),
            "seeds": seeds,
            "run_ids": run_ids,
            "metrics": metrics_out
        }
        summary[scenario] = scenario_block
        (out_dir / f"{scenario}.json").write_text(json.dumps(scenario_block, indent=2))

    (out_dir / "_summary.json").write_text(json.dumps(summary, indent=2))
    _write_csv(summary, out_dir / "_summary.csv")
    return summary


def _write_csv(summary: dict, path: Path) -> None:
    rows = []
    for scenario, block in summary.items():
        for metric, s in block["metrics"].items():
            rows.append({
                "scenario": scenario,
                "protocol": block.get("protocol"),
                "architecture": block.get("architecture"),
                "traffic_level": block.get("traffic_level"),
                "metric": metric,
                "n": s["n"],
                "mean": s["mean"],
                "std": s["std"],
                "ci95_low": s["ci95_low"],
                "ci95_high": s["ci95_high"],
                "ci95_halfwidth": s["ci95_halfwidth"]
            })
    if not rows:
        return
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Aggregate per-run result JSONs into mean + 95% CI per scenario.")
    ap.add_argument("--results-dir", default="results")
    ap.add_argument("--out", default="results/aggregated")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    out_dir = Path(args.out)
    summary = aggregate(results_dir, out_dir)

    print(f"Aggregated {len(summary)} scenario(s) from {results_dir} -> {out_dir}")
    for scenario, block in sorted(summary.items()):
        n = block["num_runs"]
        lat = block["metrics"].get("latency_mean_ms", {})
        e2e = block["metrics"].get("e2e_unique_delivery_ratio", {})
        if n < 2:
            print(f"  {scenario:28s} n={n}  (single run; no CI)")
        else:
            print(f"  {scenario:28s} n={n}  "
                  f"lat_mean={lat.get('mean')}±{lat.get('ci95_halfwidth')}ms  "
                  f"e2e={e2e.get('mean')}±{e2e.get('ci95_halfwidth')}")


if __name__ == "__main__":
    main()