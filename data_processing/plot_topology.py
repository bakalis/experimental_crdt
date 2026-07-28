#!/usr/bin/env python3
"""
Parse JSON log lines with event == "network_topology" and plot the
resulting node graph.

Usage:
    python plot_topology.py logs.jsonl [--out topology.png]

Each matching line is expected to look like:
{
  "fields": {
    "connect_node_ids": "gc-server-7,gc-server-5,normal-server-15,...",
    "event": "network_topology",
    "gc_replica": true,
    "node_id": "gc-server-1"
  },
  "level": "TRACE",
  "timestamp": "..."
}
"""

import argparse
import json
import sys

import matplotlib.pyplot as plt
import networkx as nx


def parse_topology(path):
    """Read the log file and return a networkx Graph of the topology."""
    graph = nx.Graph()

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # Skip lines that aren't valid JSON (e.g. stray output)
                continue

            fields = record.get("fields", {})
            if fields.get("event") != "network_topology":
                continue

            node_id = fields.get("node_id")
            if not node_id:
                continue

            is_gc = bool(fields.get("gc_replica", False))
            graph.add_node(node_id, gc_replica=is_gc)

            connect_ids = fields.get("connect_node_ids", "")
            for peer in connect_ids.split(","):
                peer = peer.strip()
                if peer:
                    graph.add_node(
                        peer,
                        gc_replica=graph.nodes.get(peer, {}).get("gc_replica", False),
                    )
                    graph.add_edge(node_id, peer)

    return graph


def plot_topology(graph, out_path):
    gc_nodes = [n for n, d in graph.nodes(data=True) if d.get("gc_replica")]
    normal_nodes = [n for n, d in graph.nodes(data=True) if not d.get("gc_replica")]

    # Layout: spread GC nodes (the well-connected core) using spring layout
    pos = nx.spring_layout(graph, seed=42, k=0.6)

    plt.figure(figsize=(12, 12))

    nx.draw_networkx_edges(graph, pos, alpha=0.25, width=0.6)

    nx.draw_networkx_nodes(
        graph,
        pos,
        nodelist=normal_nodes,
        node_color="#88c0d0",
        node_size=100,
        label="normal-server",
    )
    nx.draw_networkx_nodes(
        graph,
        pos,
        nodelist=gc_nodes,
        node_color="#bf616a",
        node_size=250,
        label="gc-server (replica)",
    )

    # Only label GC nodes to keep the plot readable
    gc_labels = {n: n for n in gc_nodes}
    nx.draw_networkx_labels(
        graph, pos, labels=gc_labels, font_size=8, font_weight="bold"
    )

    plt.legend(scatterpoints=1)
    plt.title(
        f"Network topology ({len(gc_nodes)} GC replicas, {len(normal_nodes)} normal servers)"
    )
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"Saved plot to {out_path}")
    print(f"Nodes: {graph.number_of_nodes()}, Edges: {graph.number_of_edges()}")


def main():
    parser = argparse.ArgumentParser(
        description="Plot network_topology events from a log file."
    )
    parser.add_argument(
        "logfile", help="Path to the log file (one JSON object per line)"
    )
    parser.add_argument(
        "--out",
        default="topology.png",
        help="Output image path (default: topology.png)",
    )
    args = parser.parse_args()

    graph = parse_topology(args.logfile)
    if graph.number_of_nodes() == 0:
        print("No network_topology events found.", file=sys.stderr)
        sys.exit(1)

    plot_topology(graph, args.out)


if __name__ == "__main__":
    main()
