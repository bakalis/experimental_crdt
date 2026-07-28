#!/usr/bin/env python3
"""
plot_summary.py
================
Generates graphs from the aggregated summary CSV (one row per
topology/size scenario). Every chart directly compares full mesh vs
overlay (as adjacent bars/lines in the same panel, not separate facets),
uses SENT bytes only, and switches to a log y-axis automatically when
values span too wide a range for a bar chart to stay readable.

Graphs:
  1. rounds_by_size
     – dissemination rounds earliest/avg/latest,
       as a connected line/marker plot (not bars), one line per
       topology, faceted by size (50 / 128).
  2. total_bytes_sent_earliest_vs_latest
  3. gc_total_and_avg_bytes_sent_own_stability_point
  4. avg_message_size_overlay_normal_vs_gc
     – overlay only; compares average message size for GC replicas
       vs normal replicas (earliest stability point), across sizes.

Each is written as PNG (matplotlib) + interactive HTML (plotly).

NOTE: fullmesh scenarios in the source data have no Normal-replica
columns populated — those bars are simply omitted (not shown as 0).

Usage:
    python plot_summary.py <summary.csv> [--out <output_dir>]
"""

import argparse
import math
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ------------------------- journal-style defaults -------------------------

plt.rcParams.update(
    {
        "font.size": 11,
        "axes.titlesize": 12,
        "axes.labelsize": 11,
        "legend.fontsize": 10,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "axes.linewidth": 1.2,
        "grid.linewidth": 0.8,
        "grid.alpha": 0.35,
        "savefig.dpi": 300,
    }
)

TOPOLOGY_ORDER = ["fullmesh", "overlay"]

# Grayscale-first colors (still distinct on color displays)
TOPOLOGY_COLORS = {"fullmesh": "#08306B", "overlay": "#E69F00"}

# Distinction channels for grayscale printing
TOPOLOGY_HATCHES = {"fullmesh": "///", "overlay": "xxx"}
TOPOLOGY_LINESTYLES = {"fullmesh": "-", "overlay": "--"}
TOPOLOGY_MARKERS = {"fullmesh": "o", "overlay": "s"}

LOG_RATIO_THRESHOLD = 15  # switch to log scale if max/min exceeds this ratio


def _topologies_present(df: pd.DataFrame) -> list[str]:
    present = set(df["topology"].unique())
    ordered = [t for t in TOPOLOGY_ORDER if t in present]
    return ordered or sorted(present)


def _sizes_present(df: pd.DataFrame) -> list[int]:
    return sorted(df["size"].unique())


def _save(fig_mpl, fig_plotly, outdir: Path, name: str) -> None:
    png_path = outdir / f"{name}.png"
    html_path = outdir / f"{name}.html"
    fig_mpl.savefig(png_path, dpi=300, bbox_inches="tight")
    plt.close(fig_mpl)
    fig_plotly.write_html(str(html_path))
    print(f"  wrote {png_path.name}, {html_path.name}")


def _needs_log(values) -> bool:
    vals = [
        v
        for v in values
        if v is not None and not (isinstance(v, float) and math.isnan(v)) and v > 0
    ]
    if len(vals) < 2:
        return False
    return max(vals) / min(vals) > LOG_RATIO_THRESHOLD


def _format_axis(ax, ygrid=True):
    if ygrid:
        ax.grid(axis="y", linestyle=":", alpha=0.35)
    ax.set_axisbelow(True)


def _grouped_bar_by_topology(
    ax, categories, topo_series: dict, ylabel: str, log_scale: bool | None = None
):
    """
    Grouped bar chart with one group of bars per topology, positioned
    within each category on the x-axis.
    topo_series = {topology: [values aligned to categories, NaN = omit that bar]}.
    """
    topologies = list(topo_series.keys())
    n = len(topologies)
    width = 0.8 / max(n, 1)
    x = list(range(len(categories)))

    all_vals = [v for series in topo_series.values() for v in series]
    if log_scale is None:
        log_scale = _needs_log(all_vals)

    drawn_vals = []

    for i, topo in enumerate(topologies):
        offset = (i - (n - 1) / 2) * width
        xs = [xi + offset for xi in x]
        vals = topo_series[topo]
        for xi, v in zip(xs, vals):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue

            drawn_vals.append(v)

            ax.bar(
                xi,
                v,
                width=width,
                color=TOPOLOGY_COLORS.get(topo),
                hatch=TOPOLOGY_HATCHES.get(topo, ""),
                edgecolor="black",
                linewidth=1.0,
                label=topo,
            )

    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_ylabel(ylabel + (" (log scale)" if log_scale else ""))
    if log_scale:
        ax.set_yscale("log")

    # Add top padding so horizontal labels are visible
    if drawn_vals:
        y_max = max(drawn_vals)
        if log_scale:
            ax.set_ylim(top=y_max * 1.35)  # multiplicative headroom for log scale
        else:
            ax.set_ylim(top=y_max * 1.12)  # additive-like headroom for linear scale

        # Label bars horizontally
        label_factor = 1.03 if log_scale else 1.01
        for patch in ax.patches:
            h = patch.get_height()
            if h is None or h <= 0:
                continue
            x_center = patch.get_x() + patch.get_width() / 2
            y_text = h * label_factor if log_scale else h + (y_max * 0.01)
            ax.text(
                x_center,
                y_text,
                f"{h:,.0f}",
                ha="center",
                va="bottom",
                fontsize=7,
                rotation=0,  # horizontal
                clip_on=False,
            )

    _format_axis(ax)

    # de-duplicate legend
    handles, labels = ax.get_legend_handles_labels()
    seen = dict(zip(labels, handles))
    ax.legend(seen.values(), seen.keys(), fontsize=9, frameon=True)
    return log_scale


def _plotly_grouped_bar_by_topology(
    fig, row, col, categories, topo_series: dict, log_scale: bool, showlegend: bool
):
    for topo, vals in topo_series.items():
        fig.add_trace(
            go.Bar(
                x=categories,
                y=vals,
                name=topo,
                marker=dict(
                    color=TOPOLOGY_COLORS.get(topo),
                    pattern_shape="/" if topo == "fullmesh" else "x",
                    line=dict(color="black", width=1.2),
                ),
                showlegend=showlegend,
            ),
            row=row,
            col=col,
        )
    if log_scale:
        fig.update_yaxes(type="log", row=row, col=col)


def _facet_by_size(sizes, figsize_per_cell=(6.4, 4.8)):
    fig_mpl, axes = plt.subplots(
        1,
        len(sizes),
        squeeze=False,
        figsize=(figsize_per_cell[0] * len(sizes), figsize_per_cell[1]),
    )
    fig_plotly = make_subplots(
        rows=1, cols=len(sizes), subplot_titles=[f"{n} servers" for n in sizes]
    )
    return fig_mpl, axes[0], fig_plotly


# ── 1. dissemination rounds — line/marker plot ────────────────────────────


def plot_rounds(df: pd.DataFrame, outdir: Path) -> None:
    topologies = _topologies_present(df)
    sizes = _sizes_present(df)
    fig_mpl, axes, fig_plotly = _facet_by_size(sizes)

    stages = ["earliest", "average", "latest"]

    all_vals = (
        df[["rounds_earliest", "rounds_avg", "rounds_latest"]].values.flatten().tolist()
    )
    log_scale = _needs_log(all_vals)

    for si, n in enumerate(sizes):
        ax = axes[si]
        subplot_vals = []  # collect values shown in this subplot for padding

        for topo in topologies:
            row = df[(df["topology"] == topo) & (df["size"] == n)]
            if row.empty:
                continue
            r = row.iloc[0]
            values = [r["rounds_earliest"], r["rounds_avg"], r["rounds_latest"]]
            subplot_vals.extend([v for v in values if pd.notna(v) and v > 0])

            ax.plot(
                stages,
                values,
                marker=TOPOLOGY_MARKERS.get(topo, "o"),
                linestyle=TOPOLOGY_LINESTYLES.get(topo, "-"),
                color=TOPOLOGY_COLORS.get(topo),
                markerfacecolor="white",
                markeredgecolor="black",
                markeredgewidth=1.0,
                label=topo,
                linewidth=2.4,
                markersize=7,
            )
            for x, v in zip(stages, values):
                ax.annotate(
                    f"{v:.0f}",
                    (x, v),
                    textcoords="offset points",
                    xytext=(0, 7),
                    ha="center",
                    fontsize=8,
                )

            fig_plotly.add_trace(
                go.Scatter(
                    x=stages,
                    y=values,
                    mode="lines+markers",
                    name=topo,
                    marker=dict(
                        symbol="circle-open" if topo == "fullmesh" else "square-open",
                        size=10,
                        line=dict(width=1.2, color="black"),
                    ),
                    line=dict(
                        color=TOPOLOGY_COLORS.get(topo),
                        dash="solid" if topo == "fullmesh" else "dash",
                        width=2.5,
                    ),
                    showlegend=(si == 0),
                ),
                row=1,
                col=si + 1,
            )

        ax.set_title(f"{n} servers")
        ax.set_ylabel("Dissemination round" + (" (log scale)" if log_scale else ""))
        if log_scale:
            ax.set_yscale("log")

        # top padding so highest point/annotation doesn't touch border
        if subplot_vals:
            y_max = max(subplot_vals)
            if log_scale:
                ax.set_ylim(top=y_max * 1.18)
            else:
                ax.set_ylim(top=y_max * 1.08)

        _format_axis(ax)
        ax.legend(fontsize=9, frameon=True)

    if log_scale:
        fig_plotly.update_yaxes(type="log")
        fig_plotly.update_yaxes(range=[None, None])  # keep auto-range
    else:
        # add similar top padding in plotly per subplot
        for si, n in enumerate(sizes):
            vals = []
            for topo in topologies:
                row = df[(df["topology"] == topo) & (df["size"] == n)]
                if row.empty:
                    continue
                r = row.iloc[0]
                vals.extend([r["rounds_earliest"], r["rounds_avg"], r["rounds_latest"]])
            vals = [v for v in vals if pd.notna(v)]
            if vals:
                fig_plotly.update_yaxes(range=[0, max(vals) * 1.08], row=1, col=si + 1)

    fig_mpl.suptitle(
        "Dissemination rounds: earliest → average → latest GC stability point (full mesh vs overlay)",
        fontsize=13,
    )
    fig_mpl.tight_layout()
    fig_plotly.update_layout(
        title="Dissemination rounds: earliest → average → latest GC stability point",
        height=480,
    )

    _save(fig_mpl, fig_plotly, outdir, "rounds_by_size")


# ── 2. total bytes sent: earliest vs latest stability point ───────────────


def plot_total_bytes_sent(df: pd.DataFrame, outdir: Path) -> None:
    topologies = _topologies_present(df)
    sizes = _sizes_present(df)
    fig_mpl, axes, fig_plotly = _facet_by_size(sizes)

    categories = ["earliest stability point", "latest stability point"]

    for si, n in enumerate(sizes):
        ax = axes[si]
        topo_series = {}
        for topo in topologies:
            row = df[(df["topology"] == topo) & (df["size"] == n)]
            if row.empty:
                topo_series[topo] = [float("nan")] * len(categories)
                continue
            r = row.iloc[0]
            topo_series[topo] = [
                r["total_bytes_earliest_sent"],
                r["total_bytes_latest_sent"],
            ]

        log_scale = _grouped_bar_by_topology(
            ax, categories, topo_series, "Total bytes sent"
        )
        ax.set_title(f"{n} servers")
        _plotly_grouped_bar_by_topology(
            fig_plotly,
            1,
            si + 1,
            categories,
            topo_series,
            log_scale,
            showlegend=(si == 0),
        )

    fig_mpl.suptitle(
        "Total bytes SENT: earliest vs latest stability point (full mesh vs overlay)",
        fontsize=13,
    )
    fig_mpl.tight_layout()
    fig_plotly.update_layout(
        title="Total bytes SENT: earliest vs latest stability point",
        height=480,
        barmode="group",
    )

    _save(fig_mpl, fig_plotly, outdir, "total_bytes_sent_earliest_vs_latest")


# ── 3. GC total & avg bytes sent, own stability point, across scenarios ───


def plot_gc_own_stability_point_bytes_sent(df: pd.DataFrame, outdir: Path) -> None:
    topologies = _topologies_present(df)
    sizes = _sizes_present(df)
    categories = [str(n) for n in sizes]

    fig_mpl, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5.2))
    fig_plotly = make_subplots(
        rows=1, cols=2, subplot_titles=["Total bytes sent", "Avg bytes / message sent"]
    )

    total_series = {}
    avg_series = {}
    for topo in topologies:
        totals, avgs = [], []
        for n in sizes:
            row = df[(df["topology"] == topo) & (df["size"] == n)]
            if row.empty:
                totals.append(float("nan"))
                avgs.append(float("nan"))
                continue
            r = row.iloc[0]
            totals.append(r["gc_total_own_sent"])
            avgs.append(r["gc_avg_own_sent"])
        total_series[topo] = totals
        avg_series[topo] = avgs

    log_total = _grouped_bar_by_topology(
        ax1, categories, total_series, "Total bytes sent"
    )
    ax1.set_title("GC total bytes sent, start → own stability point")
    ax1.set_xlabel("num servers")

    log_avg = _grouped_bar_by_topology(
        ax2, categories, avg_series, "Avg bytes / message sent"
    )
    ax2.set_title("GC avg bytes/message sent, start → own stability point")
    ax2.set_xlabel("num servers")

    fig_mpl.tight_layout()

    _plotly_grouped_bar_by_topology(
        fig_plotly, 1, 1, categories, total_series, log_total, showlegend=True
    )
    _plotly_grouped_bar_by_topology(
        fig_plotly, 1, 2, categories, avg_series, log_avg, showlegend=False
    )
    fig_plotly.update_layout(
        title="GC bytes SENT, start → own stability point",
        barmode="group",
        height=480,
    )

    _save(
        fig_mpl, fig_plotly, outdir, "gc_total_and_avg_bytes_sent_own_stability_point"
    )


# ── 4. overlay-only: average message size normal vs gc replicas ───────────


def plot_overlay_avg_message_size_normal_vs_gc(df: pd.DataFrame, outdir: Path) -> None:
    """
    Overlay-only chart:
      For each overlay size (e.g., 50, 128), compare average message size SENT for:
        - GC replicas
        - Normal replicas (earliest stability point)

    Labels are drawn horizontally above bars, with automatic top padding
    so labels are never clipped.
    """
    df_overlay = df[df["topology"] == "overlay"].copy()
    if df_overlay.empty:
        print(
            "  skipping avg_message_size_overlay_normal_vs_gc (no overlay rows found)"
        )
        return

    sizes = sorted(df_overlay["size"].unique())
    categories = [str(n) for n in sizes]

    gc_vals, normal_earliest_vals = [], []
    for n in sizes:
        row = df_overlay[df_overlay["size"] == n]
        if row.empty:
            gc_vals.append(float("nan"))
            normal_earliest_vals.append(float("nan"))
            continue
        r = row.iloc[0]
        gc_vals.append(r["gc_avg_own_sent"])
        normal_earliest_vals.append(r["normal_avg_msg_earliest_sent"])

    all_vals = gc_vals + normal_earliest_vals
    log_scale = _needs_log(all_vals)

    fig_mpl, ax = plt.subplots(figsize=(8.2, 5.2))
    series = {
        "GC replicas": gc_vals,
        "Normal replicas": normal_earliest_vals,
    }

    x = list(range(len(categories)))
    n_series = len(series)
    width = 0.8 / n_series

    color_map = {
        "GC replicas": "#009E73",
        "Normal replicas": "#999999",
    }
    hatch_map = {
        "GC replicas": "///",
        "Normal replicas": "xxx",
    }

    # Draw bars first (without labels), track drawn heights
    drawn_vals = []
    for i, (label, vals) in enumerate(series.items()):
        offset = (i - (n_series - 1) / 2) * width
        xs = [xi + offset for xi in x]
        for xi, v in zip(xs, vals):
            if pd.isna(v):
                continue
            drawn_vals.append(v)
            ax.bar(
                xi,
                v,
                width=width,
                color=color_map[label],
                hatch=hatch_map[label],
                edgecolor="black",
                linewidth=1.0,
                label=label,
            )

    # Axis config
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_xlabel("num servers")
    ax.set_ylabel(
        "Average message size (bytes, sent)" + (" (log scale)" if log_scale else "")
    )
    ax.set_title("Overlay: average message size (Normal vs GC replicas)")
    if log_scale:
        ax.set_yscale("log")
    _format_axis(ax)

    # Add headroom + horizontal value labels so nothing gets clipped
    if drawn_vals:
        y_max = max(drawn_vals)
        if log_scale:
            ax.set_ylim(top=y_max * 1.35)
        else:
            ax.set_ylim(top=y_max * 1.12)

        for patch in ax.patches:
            h = patch.get_height()
            if h is None or h <= 0:
                continue
            x_center = patch.get_x() + patch.get_width() / 2
            y_text = h * 1.03 if log_scale else h + (y_max * 0.01)
            ax.text(
                x_center,
                y_text,
                f"{h:,.0f}",
                ha="center",
                va="bottom",
                fontsize=8,
                rotation=0,  # horizontal
                clip_on=False,
            )

    handles, labels = ax.get_legend_handles_labels()
    dedup = dict(zip(labels, handles))
    ax.legend(dedup.values(), dedup.keys(), fontsize=10, frameon=True)

    # Plotly version (horizontal value labels with outside placement)
    fig_plotly = go.Figure()
    fig_plotly.add_bar(
        x=categories,
        y=gc_vals,
        name="GC replicas",
        marker=dict(
            color=color_map["GC replicas"],
            pattern_shape="/",
            line=dict(color="black", width=1.2),
        ),
        text=[f"{v:,.0f}" if not pd.isna(v) else "" for v in gc_vals],
        textposition="outside",
        cliponaxis=False,
    )
    fig_plotly.add_bar(
        x=categories,
        y=normal_earliest_vals,
        name="Normal replicas",
        marker=dict(
            color=color_map["Normal replicas"],
            pattern_shape="x",
            line=dict(color="black", width=1.2),
        ),
        text=[f"{v:,.0f}" if not pd.isna(v) else "" for v in normal_earliest_vals],
        textposition="outside",
        cliponaxis=False,
    )
    fig_plotly.update_layout(
        title="Overlay: average message size",
        barmode="group",
        xaxis_title="num servers",
        yaxis_title="Average message size (bytes, sent)",
        height=480,
    )
    if log_scale:
        fig_plotly.update_yaxes(type="log")

    _save(fig_mpl, fig_plotly, outdir, "avg_message_size_overlay_normal_vs_gc")


# ── main ───────────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("summary_csv", type=Path, help="Path to the aggregated summary.csv")
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output directory for graphs (default: ./graphs next to the CSV)",
    )
    args = ap.parse_args()

    if not args.summary_csv.is_file():
        print(f"Error: '{args.summary_csv}' not found.", file=sys.stderr)
        sys.exit(1)

    outdir = args.out or (args.summary_csv.parent / "graphs")
    outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.summary_csv)

    print("Generating dissemination-round graph …")
    plot_rounds(df, outdir)

    print("Generating bandwidth graphs (sent bytes only) …")
    plot_total_bytes_sent(df, outdir)
    plot_gc_own_stability_point_bytes_sent(df, outdir)

    print("Generating overlay-only average message-size graph …")
    plot_overlay_avg_message_size_normal_vs_gc(df, outdir)

    print(f"\nAll graphs written to: {outdir}")


if __name__ == "__main__":
    main()
