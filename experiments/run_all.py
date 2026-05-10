from __future__ import annotations
import asyncio
import csv
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from simulator.config import PREDEFINED_SCENARIOS, SCENARIO_REGISTRY
from experiments.runner import ExperimentRunner, save_results

OUTPUT_DIR = Path("results")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("run_all")


def _group_slug(group_name: str) -> str:
    slug = re.sub(r"[^\w\s]", "", group_name)  
    slug = re.sub(r"\s+", "_", slug.strip())    
    slug = slug.lower()[:50]                     
    return slug or "ungrouped"


async def _run_one(scenario) -> tuple:
    try:
        runner = ExperimentRunner(scenario)
        metrics = await runner.run()
        save_results(metrics, str(OUTPUT_DIR))
        return metrics, None
    except Exception as exc:
        logger.exception(f"[{scenario.name}] FAILED")
        return None, str(exc)


async def main(names: list[str] | None = None) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    if names:
        scenarios = [SCENARIO_REGISTRY[n] for n in names if n in SCENARIO_REGISTRY]
        missing = [n for n in names if n not in SCENARIO_REGISTRY]
        if missing:
            logger.warning(f"Unknown scenario(s) skipped: {missing}")
    else:
        scenarios = list(PREDEFINED_SCENARIOS)

    logger.info(f"Running {len(scenarios)} scenario(s) …")

    all_metrics = []
    for scenario in scenarios:
        logger.info(f"  → {scenario.name}  ({scenario.group})")
        metrics, err = await _run_one(scenario)
        if metrics is not None:
            all_metrics.append(metrics)
            logger.info(
                f"     lat_mean={metrics.latency_mean_ms:.1f} ms  "
                f"events={metrics.sensor_to_edge_msgs}  "
                f"agg_ratio={metrics.aggregation_ratio:.3f}"
            )
        else:
            logger.error(f"     {err}")

    if not all_metrics:
        logger.warning("No results to write.")
        return

    summary_path = OUTPUT_DIR / "summary.csv"
    rows = [m.to_dict() for m in all_metrics]
    with open(summary_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Summary → {summary_path}  ({len(rows)} rows)")

    groups: dict[str, list] = defaultdict(list)
    for m in all_metrics:
        groups[m.group].append(m)

    for group_name, group_metrics in sorted(groups.items()):
        slug = _group_slug(group_name)
        group_path = OUTPUT_DIR / f"group_{slug}.csv"
        g_rows = [m.to_dict() for m in group_metrics]
        with open(group_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=g_rows[0].keys())
            writer.writeheader()
            writer.writerows(g_rows)
        logger.info(f"  Group '{group_name}' → {group_path}  ({len(g_rows)} rows)")

    scale_metrics = [m for m in all_metrics if m.group == "E - Scalability"]
    if scale_metrics:
        ts_path = OUTPUT_DIR / "scale_latency_timeseries.csv"
        with open(ts_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["scenario", "group", "num_spots", "t_s", "mean_ms"])
            for m in scale_metrics:
                for point in m.latency_timeseries:
                    writer.writerow([m.scenario_name, m.group, m.num_spots, point["t_s"], point["mean_ms"]])
        logger.info(f"  Latency drift chart → {ts_path}")

    logger.info("Done.")


if __name__ == "__main__":
    scenario_filter = sys.argv[1:] or None
    asyncio.run(main(scenario_filter))