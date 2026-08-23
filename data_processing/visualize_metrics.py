#!/usr/bin/env python3
"""
plot_summary.py
================
Generates graphs from the aggregated summary CSV (one row per
topology/size scenario). Every chart directly compares full mesh vs
overlay (as adjacent bars/lines in the same panel, not separate facets),
uses SENT bytes only. All axes are LINEAR (no log scale) — see the
"axis scale policy" note below.

Graphs:
  1. rounds_by_size
     – GC stability dissemination rounds earliest/avg/latest,
       as a connected line/marker plot (not bars), one line per
       topology, faceted by size (32/ 64 / 128).
  2. data_dissemination_rounds_by_size
     – Data dissemination rounds (or_set adds → N) earliest/avg/latest,
       same line/marker format as (1), so the two "speed of
       convergence" metrics can be read the same way side by side.
  3. total_bytes_sent_earliest_vs_latest
  4. gc_total_and_avg_bytes_sent_own_stability_point
  5. avg_message_size_overlay_normal_vs_gc
     – overlay only; compares average message size for GC replicas
       vs normal replicas (earliest stability point), across sizes.

Each is written as PNG (matplotlib) + interactive HTML (plotly).

NOTE: fullmesh scenarios in the source data have no Normal-replica
columns populated — those bars are simply omitted (not shown as 0).

Axis scale policy:
    All axes are linear. Every value pair actually produced by this
    pipeline (round counts, byte totals, per-message averages within
    one panel) stays within a single order of magnitude, so a log
    axis would only obscure real differences behind a compressed
    scale and risk readers misjudging bar-height ratios. Totals and
    per-message averages are never plotted on the same axis (they
    live in separate subplots), which is the one comparison in this
    data that legitimately spans multiple orders of magnitude.

Usage:
    python plot_summary.py <summary.csv> [--out <output_dir>]
"""

import argparse
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


def _format_axis(ax, ygrid=True):
    if ygrid:
        ax.grid(axis="y", linestyle=":", alpha=0.35)
    ax.set_axisbelow(True)


def _grouped_bar_by_topology(ax, categories, topo_series: dict, ylabel: str):
    """
    Grouped bar chart with one group of bars per topology, positioned
    within each category on the x-axis. Always linear scale (see
    "axis scale policy" in the module docstring).
    topo_series = {topology: [values aligned to categories, NaN = omit that bar]}.
    """
    topologies = list(topo_series.keys())
    n = len(topologies)
    width = 0.8 / max(n, 1)
    x = list(range(len(categories)))

    drawn_vals = []

    for i, topo in enumerate(topologies):
        offset = (i - (n - 1) / 2) * width
        xs = [xi + offset for xi in x]
        vals = topo_series[topo]
        for xi, v in zip(xs, vals):
            if v is None or (isinstance(v, float) and pd.isna(v)):
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
    ax.set_ylabel(ylabel)

    # Add top padding so horizontal labels are visible
    if drawn_vals:
        y_max = max(drawn_vals)
        ax.set_ylim(top=y_max * 1.12)  # additive-like headroom for linear scale

        # Label bars horizontally
        for patch in ax.patches:
            h = patch.get_height()
            if h is None or h <= 0:
                continue
            x_center = patch.get_x() + patch.get_width() / 2
            y_text = h + (y_max * 0.01)
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


def _plotly_grouped_bar_by_topology(
    fig, row, col, categories, topo_series: dict, showlegend: bool
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


def _plot_round_progression(
    df: pd.DataFrame,
    outdir: Path,
    columns: tuple[str, str, str],
    out_name: str,
    ylabel: str,
    title: str,
) -> None:
    """
    Shared implementation for the two "rounds needed" line/marker plots
    (GC stability convergence and data dissemination convergence). Both
    use the same visual format: one line per topology (earliest →
    average → latest), faceted by size. Always linear scale.
    """
    col_earliest, col_avg, col_latest = columns

    topologies = _topologies_present(df)
    sizes = _sizes_present(df)
    fig_mpl, axes, fig_plotly = _facet_by_size(sizes)

    stages = ["earliest", "average", "latest"]

    for si, n in enumerate(sizes):
        ax = axes[si]
        subplot_vals = []  # collect values shown in this subplot for padding

        for topo in topologies:
            row = df[(df["topology"] == topo) & (df["size"] == n)]
            if row.empty:
                continue
            r = row.iloc[0]
            values = [r[col_earliest], r[col_avg], r[col_latest]]
            if all(pd.isna(v) for v in values):
                continue
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
                if pd.isna(v):
                    continue
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
        ax.set_ylabel(ylabel)

        # top padding so highest point/annotation doesn't touch border
        if subplot_vals:
            y_max = max(subplot_vals)
            ax.set_ylim(bottom=0, top=y_max * 1.08)

        _format_axis(ax)
        ax.legend(fontsize=9, frameon=True)

    # matching linear headroom in plotly, per subplot
    for si, n in enumerate(sizes):
        vals = []
        for topo in topologies:
            row = df[(df["topology"] == topo) & (df["size"] == n)]
            if row.empty:
                continue
            r = row.iloc[0]
            vals.extend([r[col_earliest], r[col_avg], r[col_latest]])
        vals = [v for v in vals if pd.notna(v)]
        if vals:
            fig_plotly.update_yaxes(range=[0, max(vals) * 1.08], row=1, col=si + 1)

    fig_mpl.suptitle(title, fontsize=13)
    fig_mpl.tight_layout()
    fig_plotly.update_layout(title=title, height=480)

    _save(fig_mpl, fig_plotly, outdir, out_name)


# ── 1. GC stability-convergence rounds — line/marker plot ─────────────────


def plot_rounds(df: pd.DataFrame, outdir: Path) -> None:
    _plot_round_progression(
        df,
        outdir,
        columns=("rounds_earliest", "rounds_avg", "rounds_latest"),
        out_name="rounds_by_size",
        ylabel="Rounds to reach GC stability",
        title=(
            "GC stability dissemination rounds: earliest → average → latest "
            "stability point (full mesh vs overlay)"
        ),
    )


# ── 2. data-dissemination-convergence rounds — line/marker plot ───────────


def plot_data_dissemination_rounds(df: pd.DataFrame, outdir: Path) -> None:
    """
    Same format as plot_rounds() above, but for the *data* dissemination
    metric (rounds needed for a server's or_set adds count to reach N,
    i.e. every replica), using the combined ALL-SERVERS columns so GC
    and Normal replicas are represented together per scenario, exactly
    as the stability-point chart represents "the system" per scenario.
    """
    required = {
        "all_data_diss_fastest_round",
        "all_data_diss_avg_round",
        "all_data_diss_slowest_round",
    }
    if not required.issubset(df.columns) or df[list(required)].isna().all().all():
        print(
            "  skipping data_dissemination_rounds_by_size "
            "(no data-dissemination columns found in CSV)"
        )
        return

    _plot_round_progression(
        df,
        outdir,
        columns=(
            "all_data_diss_fastest_round",
            "all_data_diss_avg_round",
            "all_data_diss_slowest_round",
        ),
        out_name="data_dissemination_rounds_by_size",
        ylabel="Rounds to reach N adds",
        title=(
            "Data dissemination rounds: fastest → average → slowest server to "
            "reach N adds (full mesh vs overlay)"
        ),
    )


# ── 3. total bytes sent: earliest vs latest stability point ───────────────


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

        _grouped_bar_by_topology(ax, categories, topo_series, "Total bytes sent")
        ax.set_title(f"{n} servers")
        _plotly_grouped_bar_by_topology(
            fig_plotly,
            1,
            si + 1,
            categories,
            topo_series,
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


# ── 4. GC total & avg bytes sent, own stability point, across scenarios ───


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

    _grouped_bar_by_topology(ax1, categories, total_series, "Total bytes sent")
    ax1.set_title("GC total bytes sent, start → own stability point")
    ax1.set_xlabel("num servers")

    _grouped_bar_by_topology(ax2, categories, avg_series, "Avg bytes / message sent")
    ax2.set_title("GC avg bytes/message sent, start → own stability point")
    ax2.set_xlabel("num servers")

    fig_mpl.tight_layout()

    _plotly_grouped_bar_by_topology(
        fig_plotly, 1, 1, categories, total_series, showlegend=True
    )
    _plotly_grouped_bar_by_topology(
        fig_plotly, 1, 2, categories, avg_series, showlegend=False
    )
    fig_plotly.update_layout(
        title="GC bytes SENT, start → own stability point",
        barmode="group",
        height=480,
    )

    _save(
        fig_mpl, fig_plotly, outdir, "gc_total_and_avg_bytes_sent_own_stability_point"
    )


# ── 5. overlay-only: average message size normal vs gc replicas ───────────


def plot_overlay_avg_message_size_normal_vs_gc(df: pd.DataFrame, outdir: Path) -> None:
    """
    Overlay-only chart:
      For each overlay size (e.g., 32, 64, 128), compare average message size SENT for:
        - GC replicas
        - Normal replicas (earliest stability point)

    Labels are drawn horizontally above bars, with automatic top padding
    so labels are never clipped. Always linear scale.
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
    ax.set_ylabel("Average message size (bytes, sent)")
    ax.set_title("Overlay: average message size (Normal vs GC replicas)")
    _format_axis(ax)

    # Add headroom + horizontal value labels so nothing gets clipped
    if drawn_vals:
        y_max = max(drawn_vals)
        ax.set_ylim(top=y_max * 1.12)

        for patch in ax.patches:
            h = patch.get_height()
            if h is None or h <= 0:
                continue
            x_center = patch.get_x() + patch.get_width() / 2
            y_text = h + (y_max * 0.01)
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

    print("Generating GC stability dissemination-round graph …")
    plot_rounds(df, outdir)

    print("Generating data dissemination-round graph …")
    plot_data_dissemination_rounds(df, outdir)

    print("Generating bandwidth graphs (sent bytes only) …")
    plot_total_bytes_sent(df, outdir)
    plot_gc_own_stability_point_bytes_sent(df, outdir)

    print("Generating overlay-only average message-size graph …")
    plot_overlay_avg_message_size_normal_vs_gc(df, outdir)

    print(f"\nAll graphs written to: {outdir}")


if __name__ == "__main__":
    main()
