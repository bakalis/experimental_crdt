#!/usr/bin/env python3
"""
Scan a directory for files matching {overlay|fullmesh}_{128|50}.txt
(report output produced by analyze_metrics.py), parse the summary
sections of each report, and write a single combined CSV with one
row per file.

Usage:
    python reports_to_csv.py <directory> [-o output.csv] [-r]

    <directory>   Directory to search for report files.
    -o / --output Output CSV path (default: report_summary.csv).
    -r / --recursive  Recurse into subdirectories.
"""

import argparse
import csv
import re
import sys
from pathlib import Path

FILENAME_RE = re.compile(r"^(overlay|fullmesh)_(128|50)\.txt$", re.IGNORECASE)

NUM = r"[-+]?[\d,]+(?:\.\d+)?"


def to_num(s: str) -> float:
    """Convert a comma-grouped numeric string ('1,234.56') to a float."""
    return float(s.replace(",", ""))


def find_section(text: str, header: str) -> str | None:
    """
    Return the body of a '═' boxed section whose title line is `header`,
    i.e. everything from the header line up to (but not including) the
    next '══...══' banner line (or end of text).
    """
    pattern = re.compile(
        r"═{5,}\s*\n\s*" + re.escape(header) + r"\s*\n═{5,}\s*\n"
        r"(.*?)(?=\n═{5,}|\Z)",
        re.DOTALL,
    )
    m = pattern.search(text)
    return m.group(1) if m else None


def extract_bytes_block(section: str, block_header: str) -> dict | None:
    """
    Within a section's text, find a 'Bytes (...):' block matching
    `block_header` (a literal substring to match, e.g.
    'Bytes (start → earliest cutoff)') and pull out:
        total_sent, total_recv,
        avg_msg_sent, avg_msg_recv,
        avg_total_server_sent, avg_total_server_recv (optional)
    Returns None if the block isn't found.
    """
    # Find the block_header line, then capture until blank-blank or next
    # "Bytes (" header or section end.
    idx = section.find(block_header)
    if idx == -1:
        return None
    body = section[idx:]

    # Stop this block at the next "Bytes (" occurrence (if any) after the
    # first line, so we don't swallow a following block.
    next_idx = body.find("Bytes (", len(block_header))
    if next_idx != -1:
        body = body[:next_idx]

    def grab(label: str, group: int = 1):
        pat = re.compile(re.escape(label) + r"\s*:?\s*(" + NUM + r")\s+(" + NUM + r")")
        mm = pat.search(body)
        if not mm:
            return None, None
        return to_num(mm.group(1)), to_num(mm.group(2))

    total_sent, total_recv = grab("Total bytes")
    _, _ = grab("Message count")  # not needed directly, avg msg used instead
    avg_msg_sent, avg_msg_recv = grab("Avg bytes / message")
    avg_srv_sent, avg_srv_recv = grab("Avg total bytes / server")

    if total_sent is None:
        return None

    return {
        "total_sent": total_sent,
        "total_recv": total_recv,
        "avg_msg_sent": avg_msg_sent,
        "avg_msg_recv": avg_msg_recv,
        "avg_total_server_sent": avg_srv_sent,
        "avg_total_server_recv": avg_srv_recv,
    }


def extract_gc_rounds(gc_section: str) -> dict:
    """Pull dissemination-round stats out of the GC summary section."""
    out = {"rounds_earliest": None, "rounds_avg": None, "rounds_latest": None}

    m = re.search(r"Cutoff dissemination round\s*:\s*avg=(" + NUM + r")", gc_section)
    if m:
        out["rounds_avg"] = to_num(m.group(1))

    m = re.search(r"Earliest dissemination round\s*:\s*(" + NUM + r")", gc_section)
    if m:
        out["rounds_earliest"] = to_num(m.group(1))

    m = re.search(r"Latest dissemination round\s*:\s*(" + NUM + r")", gc_section)
    if m:
        out["rounds_latest"] = to_num(m.group(1))

    return out


def parse_report(text: str) -> dict:
    """Parse a single report's text and return a flat dict of metrics."""
    row: dict = {}

    # ── GC summary section ──────────────────────────────────────────────
    gc_section = find_section(text, "SUMMARY – GC SERVERS")
    if gc_section:
        row.update(extract_gc_rounds(gc_section))
        gc_bytes = extract_bytes_block(
            gc_section, "Bytes (start → each server's own cutoff)"
        )
        if gc_bytes:
            row["gc_total_own_sent"] = gc_bytes["total_sent"]
            row["gc_total_own_recv"] = gc_bytes["total_recv"]
            row["gc_avg_own_sent"] = gc_bytes["avg_msg_sent"]
            row["gc_avg_own_recv"] = gc_bytes["avg_msg_recv"]
            row["gc_avg_total_server_own_sent"] = gc_bytes["avg_total_server_sent"]
            row["gc_avg_total_server_own_recv"] = gc_bytes["avg_total_server_recv"]
    else:
        for k in (
            "rounds_earliest",
            "rounds_avg",
            "rounds_latest",
            "gc_total_own_sent",
            "gc_total_own_recv",
            "gc_avg_own_sent",
            "gc_avg_own_recv",
            "gc_avg_total_server_own_sent",
            "gc_avg_total_server_own_recv",
        ):
            row[k] = None

    # ── NORMAL summary section (only present for overlay reports) ──────
    normal_section = find_section(text, "SUMMARY – NORMAL SERVERS")
    if normal_section:
        nb_earliest = extract_bytes_block(
            normal_section, "Bytes (start → earliest cutoff)"
        )
        nb_latest = extract_bytes_block(normal_section, "Bytes (start → latest cutoff)")
        if nb_earliest:
            row["normal_avg_msg_earliest_sent"] = nb_earliest["avg_msg_sent"]
            row["normal_avg_msg_earliest_recv"] = nb_earliest["avg_msg_recv"]
            row["normal_avg_total_server_earliest_sent"] = nb_earliest[
                "avg_total_server_sent"
            ]
            row["normal_avg_total_server_earliest_recv"] = nb_earliest[
                "avg_total_server_recv"
            ]
        if nb_latest:
            row["normal_avg_msg_latest_sent"] = nb_latest["avg_msg_sent"]
            row["normal_avg_msg_latest_recv"] = nb_latest["avg_msg_recv"]
            row["normal_avg_total_server_latest_sent"] = nb_latest[
                "avg_total_server_sent"
            ]
            row["normal_avg_total_server_latest_recv"] = nb_latest[
                "avg_total_server_recv"
            ]
    for k in (
        "normal_avg_msg_earliest_sent",
        "normal_avg_msg_earliest_recv",
        "normal_avg_total_server_earliest_sent",
        "normal_avg_total_server_earliest_recv",
        "normal_avg_msg_latest_sent",
        "normal_avg_msg_latest_recv",
        "normal_avg_total_server_latest_sent",
        "normal_avg_total_server_latest_recv",
    ):
        row.setdefault(k, None)

    # ── FINAL TOTAL (GC + Normal combined) section ──────────────────────
    final_section = find_section(text, "FINAL TOTAL – GC + NORMAL COMBINED")
    if final_section:
        fb_earliest = extract_bytes_block(
            final_section, "Bytes (start → earliest cutoff), GC + Normal"
        )
        fb_latest = extract_bytes_block(
            final_section, "Bytes (start → latest cutoff), GC + Normal"
        )
        if fb_earliest:
            row["total_bytes_earliest_sent"] = fb_earliest["total_sent"]
            row["total_bytes_earliest_recv"] = fb_earliest["total_recv"]
            row["avg_msg_earliest_sent"] = fb_earliest["avg_msg_sent"]
            row["avg_msg_earliest_recv"] = fb_earliest["avg_msg_recv"]
            row["avg_total_server_earliest_sent"] = fb_earliest["avg_total_server_sent"]
            row["avg_total_server_earliest_recv"] = fb_earliest["avg_total_server_recv"]
        if fb_latest:
            row["total_bytes_latest_sent"] = fb_latest["total_sent"]
            row["total_bytes_latest_recv"] = fb_latest["total_recv"]
            row["avg_msg_latest_sent"] = fb_latest["avg_msg_sent"]
            row["avg_msg_latest_recv"] = fb_latest["avg_msg_recv"]
            row["avg_total_server_latest_sent"] = fb_latest["avg_total_server_sent"]
            row["avg_total_server_latest_recv"] = fb_latest["avg_total_server_recv"]

    for k in (
        "total_bytes_earliest_sent",
        "total_bytes_earliest_recv",
        "avg_msg_earliest_sent",
        "avg_msg_earliest_recv",
        "avg_total_server_earliest_sent",
        "avg_total_server_earliest_recv",
        "total_bytes_latest_sent",
        "total_bytes_latest_recv",
        "avg_msg_latest_sent",
        "avg_msg_latest_recv",
        "avg_total_server_latest_sent",
        "avg_total_server_latest_recv",
    ):
        row.setdefault(k, None)

    return row


FIELDNAMES = [
    "scenario",
    "topology",
    "size",
    "rounds_earliest",
    "rounds_avg",
    "rounds_latest",
    "total_bytes_earliest_sent",
    "total_bytes_earliest_recv",
    "avg_msg_earliest_sent",
    "avg_msg_earliest_recv",
    "avg_total_server_earliest_sent",
    "avg_total_server_earliest_recv",
    "total_bytes_latest_sent",
    "total_bytes_latest_recv",
    "avg_msg_latest_sent",
    "avg_msg_latest_recv",
    "avg_total_server_latest_sent",
    "avg_total_server_latest_recv",
    "gc_total_own_sent",
    "gc_total_own_recv",
    "gc_avg_own_sent",
    "gc_avg_own_recv",
    "gc_avg_total_server_own_sent",
    "gc_avg_total_server_own_recv",
    "normal_avg_msg_earliest_sent",
    "normal_avg_msg_earliest_recv",
    "normal_avg_total_server_earliest_sent",
    "normal_avg_total_server_earliest_recv",
    "normal_avg_msg_latest_sent",
    "normal_avg_msg_latest_recv",
    "normal_avg_total_server_latest_sent",
    "normal_avg_total_server_latest_recv",
]


def find_report_files(directory: Path, recursive: bool) -> list[Path]:
    it = directory.rglob("*") if recursive else directory.iterdir()
    return sorted(p for p in it if p.is_file() and FILENAME_RE.match(p.name))


def build_row(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    topology, size = m.group(1).lower(), m.group(2)
    scenario = f"{topology}-{size}"

    text = path.read_text(errors="replace")
    metrics = parse_report(text)

    row = {"scenario": scenario, "topology": topology, "size": int(size)}
    row.update(metrics)
    return row


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Parse {overlay|fullmesh}_{128|50}.txt reports into a CSV."
    )
    ap.add_argument("directory", type=Path, help="Directory to search for reports.")
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("report_summary.csv"),
        help="Output CSV path (default: report_summary.csv).",
    )
    ap.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Recurse into subdirectories when searching for report files.",
    )
    args = ap.parse_args()

    if not args.directory.is_dir():
        print(f"Error: '{args.directory}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    files = find_report_files(args.directory, args.recursive)
    if not files:
        print(
            f"No files matching '(overlay|fullmesh)_(128|50).txt' found in "
            f"'{args.directory}'" + (" (recursive)" if args.recursive else "") + ".",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Found {len(files)} report file(s):")
    for f in files:
        print(f"  - {f}")

    rows = [build_row(f) for f in files]

    # Sort rows for stable, readable output: topology then size.
    rows.sort(key=lambda r: (r["topology"], r["size"]))

    with args.output.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"\nWrote {len(rows)} row(s) to {args.output}")


if __name__ == "__main__":
    main()
