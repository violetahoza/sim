from __future__ import annotations
import argparse, csv, os, sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def find_latest_summary(project_root: Path) -> Path:
    base = PROJECT_ROOT  / "results" / "multiseed"
    if not base.exists():
        sys.exit(f"ERROR  {base} does not exist.  Run the multiseed campaign first.")
    batches = sorted(base.glob("batch_*"), key=lambda p: p.name)
    if not batches:
        sys.exit(f"ERROR  no batch_* folders in {base}")
    latest = batches[-1] / "aggregated" / "_summary.csv"
    if not latest.exists():
        sys.exit(f"ERROR  {latest} not found.  Run the aggregation step first.")
    return latest

def load(csv_path: Path) -> dict:
    DATA: dict[str, dict] = defaultdict(dict)
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            n = int(row["n"]) if row["n"] else 0
            if n > 0:
                DATA[row["scenario"]][row["metric"]] = {"mean": float(row["mean"]), "ci95": float(row["ci95_halfwidth"])}
    return DATA

def get(DATA, scenario, metric):
    d = DATA.get(scenario, {}).get(metric, {})
    return d.get("mean"), d.get("ci95", 0)

NAVY = "#1B365D"
NAVY_LIGHT = "#2E5F8A"
ORANGE = "#D4762C"
TEAL = "#3A8A8A"
GREY_AXIS = "#666666"

def apply_style():
    plt.rcParams.update({"font.family":  "serif", "font.size": 10.5, "axes.titlesize": 12, "axes.titleweight": "bold", "axes.labelsize": 10, "axes.edgecolor": GREY_AXIS, "axes.linewidth": 0.6, "xtick.color": GREY_AXIS, "ytick.color": GREY_AXIS, "xtick.direction": "out", 
        "ytick.direction": "out", "xtick.major.width": 0.5, "ytick.major.width": 0.5, "figure.dpi": 300, "savefig.bbox": "tight", "savefig.pad_inches": 0.12, "savefig.facecolor": "white", "figure.facecolor": "white", "axes.facecolor": "white", "axes.grid": False})

def _bar_labels(ax, bars, fmt="{:.0f}", offset=0, fontsize=9.5):
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + offset, fmt.format(h), ha="center", va="bottom", fontsize=fontsize, fontweight="bold", color=NAVY)

def _save(fig, name, out_dir):
    fig.savefig(out_dir / f"{name}.pdf")
    fig.savefig(out_dir / f"{name}.png")
    plt.close(fig)
    print(f"  ✓ {name}")



def fig_groupA_reduction_latency(DATA, out):
    labels = ["Cloud-only", "Filtered", "Agg 2s", "Agg 5s", "Agg 15s", "Agg 30s", "Agg 60s"]
    scenarios = ["A1_cloud_only", "A2_edge_filtered", "A3_edge_agg_2s", "A4_edge_agg_5s", "A5_edge_agg_15s", "A6_edge_agg_30s", "A7_edge_agg_60s"]

    msg_red = []
    for sc in scenarios:
        v, _ = get(DATA, sc, "message_reduction_ratio")
        msg_red.append(v * 100 if v is not None else 0)

    med_lat = []
    for sc in scenarios:
        v, _ = get(DATA, sc, "latency_p50_ms")
        med_lat.append(v if v is not None else 0)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))

    bars1 = ax1.bar(labels, msg_red, color=NAVY, width=0.62, edgecolor="white", linewidth=0.3)
    ax1.set_title("Message Reduction vs. Cloud-Only (%)")
    ax1.set_ylim(0, 115)
    ax1.set_ylabel("")
    ax1.tick_params(axis="x", rotation=25)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    _bar_labels(ax1, bars1, offset=1)

    bars2 = ax2.bar(labels, med_lat, color=NAVY, width=0.62, edgecolor="white", linewidth=0.3)
    ax2.set_title("Median End-to-End Latency (ms)")
    ax2.set_ylabel("")
    ax2.tick_params(axis="x", rotation=25)
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    _bar_labels(ax2, bars2, fmt="{:,.0f}", offset=max(med_lat) * 0.015)

    plt.tight_layout(w_pad=3)
    _save(fig, "groupA_reduction_latency", out)


def fig_groupA_p95_bytes(DATA, out):
    labels = ["Cloud-only", "Filtered", "Agg 2s", "Agg 5s", "Agg 15s", "Agg 30s", "Agg 60s"]
    scenarios = ["A1_cloud_only", "A2_edge_filtered", "A3_edge_agg_2s", "A4_edge_agg_5s", "A5_edge_agg_15s", "A6_edge_agg_30s", "A7_edge_agg_60s"]

    p95 = [get(DATA, s, "latency_p95_ms")  for s in scenarios]
    byt = [(get(DATA, s, "proto_bytes_sent")[0] or 0) / 1024 for s in scenarios]
    bci = [(get(DATA, s, "proto_bytes_sent")[1] or 0) / 1024 for s in scenarios]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    x = np.arange(len(labels))

    bars1 = ax1.bar(x, [v for v, _ in p95], yerr=[c for _, c in p95], color=NAVY, width=0.58, capsize=3, error_kw={"lw": 0.8})
    ax1.set_title("P95 Latency by Architecture")
    ax1.set_ylabel("P95 Latency (ms)")
    ax1.set_xticks(x); ax1.set_xticklabels(labels, rotation=25, ha="right")
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax1.spines["top"].set_visible(False); ax1.spines["right"].set_visible(False)

    bars2 = ax2.bar(x, byt, yerr=bci, color=NAVY_LIGHT, width=0.58, capsize=3, error_kw={"lw": 0.8})
    ax2.set_title("Protocol Bytes Sent by Architecture")
    ax2.set_ylabel("Bytes Sent (KB)")
    ax2.set_xticks(x); ax2.set_xticklabels(labels, rotation=25, ha="right")
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax2.spines["top"].set_visible(False); ax2.spines["right"].set_visible(False)

    plt.tight_layout(w_pad=3)
    _save(fig, "groupA_p95_bytes", out)


def fig_groupA_anomaly(DATA, out):
    labels = ["A2: Filtered\n(no detection)", "A8: Filtered +\nAnomaly Detection"]
    scs = ["A2_edge_filtered", "A8_edge_anomaly"]

    p50 = [get(DATA, s, "latency_p50_ms")[0] or 0 for s in scs]
    p95 = [get(DATA, s, "latency_p95_ms")[0] or 0 for s in scs]
    p95c = [get(DATA, s, "latency_p95_ms")[1] or 0 for s in scs]
    delv = [get(DATA, s, "e2e_unique_delivery_ratio")[0] or 0 for s in scs]
    mred = [get(DATA, s, "message_reduction_ratio")[0] or 0 for s in scs]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.2))
    x = np.arange(2); w = 0.30

    b1 = ax1.bar(x - w/2, p50, w, color=NAVY_LIGHT, label="P50 (median)")
    b2 = ax1.bar(x + w/2, p95, w, yerr=p95c, color=NAVY, label="P95", capsize=4, error_kw={"lw": 0.8})
    ax1.set_title("Latency: Filtering vs. Detection")
    ax1.set_ylabel("Latency (ms)")
    ax1.set_xticks(x); ax1.set_xticklabels(labels, fontsize=9)
    ax1.legend(fontsize=8.5, frameon=False)
    ax1.spines["top"].set_visible(False); ax1.spines["right"].set_visible(False)
    _bar_labels(ax1, b1, fmt="{:.1f}", offset=2, fontsize=8.5)
    _bar_labels(ax1, b2, fmt="{:.1f}", offset=4, fontsize=8.5)

    b3 = ax2.bar(x - w/2, delv, w, color=NAVY, label="E2E Delivery")
    b4 = ax2.bar(x + w/2, mred, w, color=ORANGE, label="Msg Reduction")
    ax2.set_title("Delivery & Reduction")
    ax2.set_ylabel("Ratio")
    ax2.set_xticks(x); ax2.set_xticklabels(labels, fontsize=9)
    ax2.set_ylim(0, 1.05)
    ax2.legend(fontsize=8.5, frameon=False)
    ax2.spines["top"].set_visible(False); ax2.spines["right"].set_visible(False)
    _bar_labels(ax2, b3, fmt="{:.3f}", offset=0.01, fontsize=8.5)
    _bar_labels(ax2, b4, fmt="{:.3f}", offset=0.01, fontsize=8.5)

    plt.tight_layout(w_pad=3)
    _save(fig, "groupA_anomaly", out)


def fig_groupB_protocol(DATA, out):
    labels = ["MQTT\nQoS 0", "MQTT\nQoS 1", "MQTT\nQoS 2", "CoAP\nNON", "CoAP\nCON", "AMQP\nauto", "AMQP\nmanual", "AMQP\ntopic"]
    scs = ["B1_mqtt_qos0", "B2_mqtt_qos1", "B3_mqtt_qos2", "B4_coap_non", "B5_coap_con", "B6_amqp_auto", "B7_amqp_manual_durable", "B8_amqp_topic"]
    colors = [NAVY]*3 + [ORANGE]*2 + [NAVY_LIGHT]*3

    p95 = [get(DATA, s, "latency_p95_ms")  for s in scs]
    delv = [get(DATA, s, "e2e_unique_delivery_ratio") for s in scs]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    x = np.arange(len(labels))

    ax1.bar(x, [v for v, _ in p95], yerr=[c for _, c in p95], color=colors, width=0.62, capsize=2, error_kw={"lw": 0.7})
    ax1.set_title("P95 Latency by Protocol")
    ax1.set_ylabel("P95 Latency (ms)")
    ax1.set_xticks(x); ax1.set_xticklabels(labels, fontsize=8)
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax1.spines["top"].set_visible(False); ax1.spines["right"].set_visible(False)

    ax2.bar(x, [v for v, _ in delv], yerr=[c for _, c in delv], color=colors, width=0.62, capsize=2, error_kw={"lw": 0.7})
    ax2.set_title("E2E Delivery Ratio by Protocol")
    ax2.set_ylabel("Delivery Ratio")
    ax2.set_xticks(x); ax2.set_xticklabels(labels, fontsize=8)
    ax2.set_ylim(0.70, 0.85)
    ax2.spines["top"].set_visible(False); ax2.spines["right"].set_visible(False)

    plt.tight_layout(w_pad=3)
    _save(fig, "groupB_protocol", out)


def fig_groupC_scalability(DATA, out):
    spots = [50, 200, 500, 1000, 2000]
    scs = ["C1_scale_50", "C2_scale_200", "C3_scale_500", "C4_scale_1000", "C5_scale_2000"]

    lat = [get(DATA, s, "latency_p95_ms") for s in scs]
    dlv = [get(DATA, s, "e2e_unique_delivery_ratio") for s in scs]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.2))

    ax1.errorbar(spots, [v for v, _ in lat], yerr=[c for _, c in lat], marker="o", color=NAVY, linewidth=2.2, markersize=7, capsize=5, capthick=1.2)
    ax1.set_title("P95 Latency vs. Spot Count")
    ax1.set_xlabel("Number of Spots")
    ax1.set_ylabel("P95 Latency (ms)")
    ax1.set_xscale("log")
    ax1.set_xticks(spots); ax1.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax1.spines["top"].set_visible(False); ax1.spines["right"].set_visible(False)

    ax2.errorbar(spots, [v for v, _ in dlv], yerr=[c for _, c in dlv], marker="s", color=ORANGE, linewidth=2.2, markersize=7, capsize=5, capthick=1.2)
    ax2.set_title("E2E Delivery Ratio vs. Spot Count")
    ax2.set_xlabel("Number of Spots")
    ax2.set_ylabel("Delivery Ratio")
    ax2.set_xscale("log")
    ax2.set_xticks(spots); ax2.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax2.spines["top"].set_visible(False); ax2.spines["right"].set_visible(False)

    plt.tight_layout(w_pad=3)
    _save(fig, "groupC_scalability", out)


def fig_groupD_nstart(DATA, out):
    loss_labels = ["5%", "25%", "45%"]
    x = np.arange(3); w = 0.20

    mqtt = ["D1_mqtt_loss_low", "D2_mqtt_loss_med", "D3_mqtt_loss_high"]
    coap_1 = ["D4_coap_loss_low", "D5_coap_loss_med", "D6_coap_loss_high"]
    coap_4 = [None, "D10_coap_loss_med_nstart4", "D11_coap_loss_high_nstart4"]
    coap_u = [None, "D12_coap_loss_med_nstart_unbounded", "D13_coap_loss_high_nstart_unbounded"]

    def vals(scs, metric):
        out_v, out_c = [], []
        for s in scs:
            if s is None:
                out_v.append(None); out_c.append(0)
            else:
                v, c = get(DATA, s, metric)
                out_v.append(v); out_c.append(c)
        return out_v, out_c

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

    mv, _  = vals(mqtt, "latency_p95_ms")
    c1v, _ = vals(coap_1, "latency_p95_ms")
    c4v, _ = vals(coap_4, "latency_p95_ms")
    cuv, _ = vals(coap_u, "latency_p95_ms")

    ax1.bar(x - 1.5*w, mv, w, color=NAVY, label="MQTT QoS 1")
    ax1.bar(x - 0.5*w, c1v, w, color=ORANGE, label="CoAP CON (N=1)")
    if c4v[1] is not None:
        ax1.bar(x[1:] + 0.5*w, [c4v[1], c4v[2]], w, color=TEAL, label="CoAP CON (N=4)")
    if cuv[1] is not None:
        ax1.bar(x[1:] + 1.5*w, [cuv[1], cuv[2]], w, color=NAVY_LIGHT, label="CoAP CON (unb.)")

    ax1.set_yscale("log")
    ax1.set_title("P95 Latency vs. Backhaul Loss")
    ax1.set_ylabel("P95 Latency (ms, log scale)")
    ax1.set_xticks(x); ax1.set_xticklabels(loss_labels)
    ax1.set_xlabel("Backhaul Loss Rate")
    ax1.legend(fontsize=7.5, frameon=False, loc="upper left")
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax1.spines["top"].set_visible(False); ax1.spines["right"].set_visible(False)

    md, _ = vals(mqtt, "e2e_unique_delivery_ratio")
    c1d, _ = vals(coap_1, "e2e_unique_delivery_ratio")
    c4d, _ = vals(coap_4, "e2e_unique_delivery_ratio")
    cud, _ = vals(coap_u, "e2e_unique_delivery_ratio")

    ax2.bar(x - 1.5*w, md, w, color=NAVY, label="MQTT QoS 1")
    ax2.bar(x - 0.5*w, c1d, w, color=ORANGE, label="CoAP CON (N=1)")
    if c4d[1] is not None:
        ax2.bar(x[1:] + 0.5*w, [c4d[1], c4d[2]], w, color=TEAL, label="CoAP CON (N=4)")
    if cud[1] is not None:
        ax2.bar(x[1:] + 1.5*w, [cud[1], cud[2]], w, color=NAVY_LIGHT, label="CoAP CON (unb.)")

    ax2.set_title("E2E Delivery vs. Backhaul Loss")
    ax2.set_ylabel("Delivery Ratio")
    ax2.set_xticks(x); ax2.set_xticklabels(loss_labels)
    ax2.set_xlabel("Backhaul Loss Rate")
    ax2.legend(fontsize=7.5, frameon=False, loc="lower left")
    ax2.spines["top"].set_visible(False); ax2.spines["right"].set_visible(False)

    plt.tight_layout(w_pad=3)
    _save(fig, "groupD_nstart", out)


def fig_groupE_load_coap(DATA, out):
    traffic = ["Low", "Medium", "Peak"]

    mqtt_scs = ["E4_load_mqtt_low", "E5_load_mqtt_med", "E6_load_mqtt_peak"]
    coap_scs = ["E10_load_coap_low", "E11_load_coap_med", "E12_load_coap_peak"]

    mqtt_p95 = [get(DATA, s, "latency_p95_ms") for s in mqtt_scs]
    coap_p95 = [get(DATA, s, "latency_p95_ms") for s in coap_scs]

    fig, ax = plt.subplots(figsize=(7, 4.2))
    x = np.arange(3)

    ax.errorbar(x, [v for v, _ in mqtt_p95], yerr=[c for _, c in mqtt_p95], marker="o", label="MQTT QoS 1", color=NAVY, linewidth=2.2, markersize=7, capsize=5, capthick=1.2)
    ax.errorbar(x, [v for v, _ in coap_p95], yerr=[c for _, c in coap_p95], marker="s", label="CoAP CON", color=ORANGE, linewidth=2.2, markersize=7, capsize=5, capthick=1.2)

    ax.set_title("P95 Latency vs. Traffic Level (Edge-Filtered)")
    ax.set_ylabel("P95 Latency (ms)")
    ax.set_xticks(x); ax.set_xticklabels(traffic)
    ax.set_xlabel("Traffic Level")
    ax.legend(frameon=False)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)

    plt.tight_layout()
    _save(fig, "groupE_load_coap", out)


def main():
    parser = argparse.ArgumentParser(description="Generate thesis chapter charts")
    parser.add_argument("--csv", type=Path, default=None, help="Path to _summary.csv (default: auto-discover latest batch)")
    parser.add_argument("--out", type=Path, default=None, help="Output directory (default: figs/)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    csv_path = args.csv or find_latest_summary(project_root)
    out_dir  = args.out or (project_root / "figs")
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading  {csv_path}")
    print(f"Writing  {out_dir}/\n")

    DATA = load(csv_path)
    apply_style()

    fig_groupA_reduction_latency(DATA, out_dir)
    fig_groupA_p95_bytes(DATA, out_dir)
    fig_groupA_anomaly(DATA, out_dir)
    fig_groupB_protocol(DATA, out_dir)
    fig_groupC_scalability(DATA, out_dir)
    fig_groupD_nstart(DATA, out_dir)
    fig_groupE_load_coap(DATA, out_dir)

    print(f"\nDone — {7} figures in {out_dir}/")


if __name__ == "__main__":
    main()