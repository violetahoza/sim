from __future__ import annotations
import asyncio
import csv
import dataclasses
import json
import logging
import sys
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from simulator.config import ScenarioConfig, SCENARIO_REGISTRY, PREDEFINED_SCENARIOS
from experiments.runner import ExperimentRunner, save_results
from simulator.models import ExperimentMetrics

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("results")


@dataclasses.dataclass
class MultiSeedResult:
    scenario_name: str
    group: str
    protocol: str
    architecture: str
    traffic_level: str
    n_reps: int
    seeds: list[int]

    reps: list[ExperimentMetrics] = dataclasses.field(default_factory=list)

    latency_mean_mean: float = 0.0
    latency_mean_std: float = 0.0
    latency_p50_mean: float = 0.0
    latency_p95_mean: float = 0.0
    latency_p99_mean: float = 0.0
    latency_p99_std: float = 0.0

    delivery_ratio_mean: float = 0.0
    delivery_ratio_std: float = 0.0
    delivery_ratio_min: float = 0.0

    aggregation_ratio_mean: float = 0.0
    filtered_mean: float = 0.0
    retransmissions_mean: float = 0.0

    conservation_ok: bool = True
    conservation_violations: int = 0

    def aggregate(self) -> None:
        if not self.reps:
            return

        def _arr(attr: str) -> np.ndarray:
            vals = [getattr(r, attr, 0.0) or 0.0 for r in self.reps]
            return np.array(vals, dtype=float)

        lat_means = _arr("latency_mean_ms")
        lat_p50 = _arr("latency_p50_ms")
        lat_p95 = _arr("latency_p95_ms")
        lat_p99 = _arr("latency_p99_ms")
        dr = _arr("end_to_end_delivery_ratio")
        agg = _arr("aggregation_ratio")
        filt = _arr("filtered_events")

        self.latency_mean_mean = float(np.mean(lat_means))
        self.latency_mean_std = float(np.std(lat_means))
        self.latency_p50_mean = float(np.mean(lat_p50))
        self.latency_p95_mean = float(np.mean(lat_p95))
        self.latency_p99_mean = float(np.mean(lat_p99))
        self.latency_p99_std = float(np.std(lat_p99))
        self.delivery_ratio_mean = float(np.mean(dr))
        self.delivery_ratio_std = float(np.std(dr))
        self.delivery_ratio_min = float(np.min(dr))
        self.aggregation_ratio_mean = float(np.mean(agg))
        self.filtered_mean = float(np.mean(filt))

    def summary_dict(self) -> dict:
        return {
            "scenario_name": self.scenario_name,
            "group": self.group,
            "protocol": self.protocol,
            "architecture": self.architecture,
            "traffic_level": self.traffic_level,
            "n_reps": self.n_reps,
            "seeds": self.seeds,
            "latency_mean_ms_mean": round(self.latency_mean_mean, 2),
            "latency_mean_ms_std": round(self.latency_mean_std, 2),
            "latency_p50_ms_mean": round(self.latency_p50_mean, 2),
            "latency_p95_ms_mean": round(self.latency_p95_mean, 2),
            "latency_p99_ms_mean": round(self.latency_p99_mean, 2),
            "latency_p99_ms_std": round(self.latency_p99_std, 2),
            "delivery_ratio_mean": round(self.delivery_ratio_mean, 4),
            "delivery_ratio_std": round(self.delivery_ratio_std, 4),
            "delivery_ratio_min": round(self.delivery_ratio_min, 4),
            "aggregation_ratio_mean": round(self.aggregation_ratio_mean, 4),
            "filtered_mean": round(self.filtered_mean, 1),
            "conservation_ok": self.conservation_ok,
            "conservation_violations": self.conservation_violations,
        }


def check_conservation(metrics: ExperimentMetrics, tolerance: float = 0.02) -> tuple[bool, str]:
    generated = metrics.sensor_to_edge_msgs
    if generated == 0:
        return True, "no events generated"

    delivered = metrics.edge_to_cloud_msgs or metrics.cloud_only_msgs
    dropped_link = generated - int(generated * metrics.sensor_to_edge_delivery_ratio)

    accounted = delivered + dropped_link
    ratio = accounted / generated if generated > 0 else 1.0

    if ratio > (1.0 + tolerance) or ratio < (1.0 - tolerance):
        return False, (
            f"conservation violation: generated={generated} "
            f"delivered={delivered} dropped_est={dropped_link} "
            f"ratio={ratio:.3f} (tolerance ±{tolerance:.0%})"
        )
    return True, f"ok (ratio={ratio:.3f})"


class MultiSeedRunner:

    def __init__(self, config: ScenarioConfig, n_reps: int = 5, seed_step: int = 100) -> None:
        self._base = config
        self.n_reps = n_reps
        self.seed_step = seed_step

    async def run(self) -> MultiSeedResult:
        base_seed = self._base.random_seed
        seeds = [base_seed + i * self.seed_step for i in range(self.n_reps)]

        result = MultiSeedResult(
            scenario_name=self._base.name,
            group=self._base.group,
            protocol=self._base.protocol,
            architecture=self._base.architecture,
            traffic_level=self._base.traffic_level,
            n_reps=self.n_reps,
            seeds=seeds
        )

        for i, seed in enumerate(seeds):
            rep_name = f"{self._base.name}_rep{i+1}_s{seed}"
            logger.info(f"[MultiSeed] Rep {i+1}/{self.n_reps} seed={seed} ({rep_name})")

            import copy, dataclasses as _dc
            cfg = copy.copy(self._base)
            cfg = _dc.replace(cfg, random_seed=seed, name=rep_name)
            import dataclasses as _dc2
            cfg.traffic = _dc2.replace(cfg.traffic, random_seed=seed)

            try:
                runner  = ExperimentRunner(cfg)
                metrics = await runner.run()
                save_results(metrics, str(OUTPUT_DIR))
            except Exception:
                logger.exception(f"[MultiSeed] Rep {i+1} FAILED")
                continue

            result.reps.append(metrics)

            ok, explanation = check_conservation(metrics)
            if not ok:
                logger.warning(f"[MultiSeed] [{rep_name}] {explanation}")
                result.conservation_violations += 1

            logger.info(
                f"[MultiSeed] Rep {i+1} done: "
                f"lat={metrics.latency_mean_ms:.1f}ms "
                f"p99={metrics.latency_p99_ms:.1f}ms "
                f"e2e_dr={metrics.end_to_end_delivery_ratio:.1%} "
                f"conservation={explanation}"
            )

        result.conservation_ok = result.conservation_violations == 0
        result.aggregate()
        return result


async def main(scenario_names: list[str] | None = None, n_reps: int = 5) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s", datefmt="%H:%M:%S")

    if scenario_names:
        scenarios = [SCENARIO_REGISTRY[n] for n in scenario_names if n in SCENARIO_REGISTRY]
        missing = [n for n in scenario_names if n not in SCENARIO_REGISTRY]
        if missing:
            logger.warning(f"Unknown scenario(s): {missing}")
    else:
        scenarios = list(PREDEFINED_SCENARIOS)

    all_summaries: list[dict] = []

    for scenario in scenarios:
        logger.info(f"\n{'='*60}")
        logger.info(f"Scenario: {scenario.name}  ({n_reps} reps)")
        logger.info(f"{'='*60}")
        runner = MultiSeedRunner(scenario, n_reps=n_reps)
        result = await runner.run()
        summary = result.summary_dict()
        all_summaries.append(summary)

        logger.info(
            f"  lat_mean={summary['latency_mean_ms_mean']:.1f} ± {summary['latency_mean_ms_std']:.1f} ms"
            f"  p99={summary['latency_p99_ms_mean']:.1f} ± {summary['latency_p99_ms_std']:.1f} ms"
            f"  dr={summary['delivery_ratio_mean']:.1%} ± {summary['delivery_ratio_std']:.1%}"
            f"  conservation={'✓' if summary['conservation_ok'] else '✗'}"
        )

        path = OUTPUT_DIR / f"{scenario.name}_multiseed.json"
        path.write_text(json.dumps(summary, indent=2))

    if all_summaries:
        csv_path = OUTPUT_DIR / "multiseed_summary.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_summaries[0].keys())
            writer.writeheader()
            for row in all_summaries:
                flat_row = {k: (json.dumps(v) if isinstance(v, list) else v) for k, v in row.items()}
                writer.writerow(flat_row)
        logger.info(f"\nMulti-seed summary → {csv_path}  ({len(all_summaries)} scenarios)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Multi-seed experiment runner")
    parser.add_argument("scenarios", nargs="*", help="Scenario name(s) to run (default: all)")
    parser.add_argument("--reps", type=int, default=5, help="Repetitions per scenario (default 5)")
    args = parser.parse_args()

    asyncio.run(main(args.scenarios or None, n_reps=args.reps))