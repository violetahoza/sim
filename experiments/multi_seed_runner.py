from __future__ import annotations
import asyncio
import csv
import dataclasses
import json
import logging
import sys
from pathlib import Path
import numpy as np
import math

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from simulator.config import ScenarioConfig, SCENARIO_REGISTRY, PREDEFINED_SCENARIOS
from experiments.runner import ExperimentRunner, save_results
from simulator.models import ExperimentMetrics

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("results")


_T_CRIT: dict[int, float] = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    15: 2.131, 20: 2.086, 30: 2.042, 60: 2.000, 120: 1.980,
}

def _t_critical(df: int) -> float:
    if df in _T_CRIT:
        return _T_CRIT[df]
    if df > 120:
        return 1.96
    keys = sorted(_T_CRIT)
    for i, k in enumerate(keys):
        if k > df:
            lo, hi = keys[i - 1], k
            frac = (df - lo) / (hi - lo)
            return _T_CRIT[lo] + frac * (_T_CRIT[hi] - _T_CRIT[lo])
    return 1.96


def _ci95(arr: np.ndarray) -> float:
    n = len(arr)
    if n < 2:
        return 0.0
    se = float(np.std(arr, ddof=1)) / math.sqrt(n)
    return se * _t_critical(n - 1)


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

    physical_delivery_ratio_mean: float = 0.0
    cloud_reflection_ratio_mean: float = 0.0
    message_reduction_ratio_mean: float = 0.0
    events_per_cloud_message_mean: float = 0.0

    aggregation_ratio_mean: float = 0.0
    filtered_mean: float = 0.0
    retransmissions_mean: float = 0.0

    latency_mean_ci95_ms: float = 0.0   
    latency_p99_ci95_ms: float = 0.0
    delivery_ratio_ci95: float = 0.0

    energy_mj_mean: float = 0.0
    battery_life_days_mean: float = 0.0

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
        dr = _arr("cloud_reflection_ratio")
        physical_dr = _arr("physical_delivery_ratio")
        reduction = _arr("message_reduction_ratio")
        events_per_msg = _arr("events_per_cloud_message")
        agg = _arr("aggregation_ratio")
        filt = _arr("filtered_events")
        energy = _arr("energy_per_sensor_mj")
        battery = _arr("battery_life_days")

        self.latency_mean_mean = float(np.mean(lat_means))
        self.latency_mean_std = float(np.std(lat_means))
        self.latency_p50_mean = float(np.mean(lat_p50))
        self.latency_p95_mean = float(np.mean(lat_p95))
        self.latency_p99_mean = float(np.mean(lat_p99))
        self.latency_p99_std = float(np.std(lat_p99))
        self.delivery_ratio_mean = float(np.mean(dr))
        self.delivery_ratio_std = float(np.std(dr))
        self.delivery_ratio_min = float(np.min(dr))
        self.physical_delivery_ratio_mean = float(np.mean(physical_dr))
        self.cloud_reflection_ratio_mean = float(np.mean(dr))
        self.message_reduction_ratio_mean = float(np.mean(reduction))
        self.events_per_cloud_message_mean = float(np.mean(events_per_msg))
        self.aggregation_ratio_mean = float(np.mean(agg))
        self.filtered_mean = float(np.mean(filt))
        self.energy_mj_mean = float(np.mean(energy))
        self.battery_life_days_mean = float(np.mean(battery))

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
            "latency_mean_ms_ci95": round(self.latency_mean_ci95_ms, 2),
            "latency_p50_ms_mean": round(self.latency_p50_mean, 2),
            "latency_p95_ms_mean": round(self.latency_p95_mean, 2),
            "latency_p99_ms_mean": round(self.latency_p99_mean, 2),
            "latency_p99_ms_std": round(self.latency_p99_std, 2),
            "latency_p99_ms_ci95": round(self.latency_p99_ci95_ms, 2),
            "delivery_ratio_mean": round(self.delivery_ratio_mean, 4),
            "delivery_ratio_std": round(self.delivery_ratio_std, 4),
            "delivery_ratio_ci95": round(self.delivery_ratio_ci95, 4),
            "delivery_ratio_min": round(self.delivery_ratio_min, 4),
            "physical_delivery_ratio_mean": round(self.physical_delivery_ratio_mean, 4),
            "cloud_reflection_ratio_mean": round(self.cloud_reflection_ratio_mean, 4),
            "message_reduction_ratio_mean": round(self.message_reduction_ratio_mean, 4),
            "events_per_cloud_message_mean": round(self.events_per_cloud_message_mean, 2),
            "aggregation_ratio_mean": round(self.aggregation_ratio_mean, 4),
            "filtered_mean": round(self.filtered_mean, 1),
            "energy_mj_mean": round(self.energy_mj_mean, 3),
            "battery_life_days_mean": round(self.battery_life_days_mean, 1),
            "conservation_ok": self.conservation_ok,
            "conservation_violations": self.conservation_violations,
        }


def check_conservation(metrics: ExperimentMetrics, tolerance: float = 0.02) -> tuple[bool, str]:
    generated = metrics.sensor_to_edge_msgs
    if generated == 0:
        return True, "no events generated"

    valid = metrics.valid_state_changes or max(generated - metrics.filtered_events, 0)
    reflected = metrics.events_reflected_in_cloud or metrics.cloud_only_msgs
    physical_drop_est = int(round(generated * (1.0 - metrics.physical_delivery_ratio)))

    accounted = reflected + metrics.filtered_events + physical_drop_est
    ratio = accounted / generated if generated > 0 else 1.0

    if ratio > (1.0 + tolerance) or ratio < (1.0 - tolerance):
        return False, (
            f"accounting warning: generated={generated} valid={valid} "
            f"reflected={reflected} filtered={metrics.filtered_events} "
            f"physical_drop_est={physical_drop_est} ratio={ratio:.3f} "
            f"(tolerance ±{tolerance:.0%})"
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
                f"cloud_reflection={metrics.cloud_reflection_ratio:.1%} "
                f"msg_reduction={metrics.message_reduction_ratio:.1%} "
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
            f"  cloud_reflection={summary['cloud_reflection_ratio_mean']:.1%} ± {summary['delivery_ratio_std']:.1%}"
            f"  msg_reduction={summary['message_reduction_ratio_mean']:.1%}"
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