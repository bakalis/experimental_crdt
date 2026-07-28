#!/usr/bin/env python3
"""
Analyze a single interleaved metrics.jsonl file containing events from
multiple servers. Server identity is taken from the node_id field on
each line.

  gc-server-*      → GC coordinator replicas
  normal-server-*  → Normal replicas

Measurement window:
  START  = latest  client_operation timestamp across ALL node_ids  (phase 0)
  END    = first   gc_coordinator_metrics where len(v_stable) == total servers
           (per GC server); global cutoff = earliest such timestamp

Windowing rules (byte / message totals):

  GC replicas:
    One window per server:  (START, this server's own cutoff)
    → totals + averages for send_envelope / receive_envelope bytes.

  Normal replicas:
    Two FIXED windows shared by all normal replicas, anchored on the
    earliest and latest cutoff timestamps observed across all GC replicas:
      - (START, earliest GC cutoff)
      - (START, latest GC cutoff)
    → totals + averages for send_envelope / receive_envelope bytes, for
      each window.

Only send_envelope / receive_envelope events strictly inside the relevant
open interval (START, END) are counted.
"""

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# ── data model ────────────────────────────────────────────────────────────────


@dataclass
class ServerStats:
    name: str
    kind: str  # "gc" | "normal"

    # GC: bytes in (START, own cutoff).
    # Normal: bytes in (START, earliest GC cutoff).
    sent_bytes: list[int] = field(default_factory=list)
    recv_bytes: list[int] = field(default_factory=list)

    # Normal only: bytes in (START, latest GC cutoff).
    sent_bytes_latest: list[int] = field(default_factory=list)
    recv_bytes_latest: list[int] = field(default_factory=list)

    # GC only: bytes in the two FIXED global windows (start→earliest cutoff,
    # start→latest cutoff), as opposed to sent_bytes/recv_bytes above which
    # are bounded by this server's OWN cutoff. Used for the combined
    # GC+Normal grand totals.
    sent_bytes_fixed_earliest: list[int] = field(default_factory=list)
    recv_bytes_fixed_earliest: list[int] = field(default_factory=list)
    sent_bytes_fixed_latest: list[int] = field(default_factory=list)
    recv_bytes_fixed_latest: list[int] = field(default_factory=list)

    start_timestamp: str | None = None  # global start (latest client_operation)
    cutoff_timestamp: str | None = None  # GC only: this server's own cutoff
    cutoff_round: int | None = None  # gc only: dissemination_round at cutoff
    first_round: int | None = None  # first gc_coordinator_metrics round after start
    last_round: int | None = None  # last gc_coordinator_metrics round seen (pre-cutoff)
    lines_processed: int = 0
    done: bool = field(default=False, repr=False)  # internal: stop accepting lines

    @property
    def rounds_in_window(self) -> int:
        """Round span covered while waiting for convergence: last round - first round."""
        end_round = (
            self.cutoff_round if self.cutoff_round is not None else self.last_round
        )
        if self.first_round is None or end_round is None:
            return 0
        return end_round - self.first_round

    # ── (START, own cutoff) for GC / (START, earliest cutoff) for Normal ──
    @property
    def total_sent(self) -> int:
        return sum(self.sent_bytes)

    @property
    def total_recv(self) -> int:
        return sum(self.recv_bytes)

    @property
    def avg_sent(self) -> float:
        return self.total_sent / len(self.sent_bytes) if self.sent_bytes else 0.0

    @property
    def avg_recv(self) -> float:
        return self.total_recv / len(self.recv_bytes) if self.recv_bytes else 0.0

    # ── (START, latest cutoff) — Normal replicas only ──────────────────────
    @property
    def total_sent_latest(self) -> int:
        return sum(self.sent_bytes_latest)

    @property
    def total_recv_latest(self) -> int:
        return sum(self.recv_bytes_latest)

    @property
    def avg_sent_latest(self) -> float:
        return (
            self.total_sent_latest / len(self.sent_bytes_latest)
            if self.sent_bytes_latest
            else 0.0
        )

    @property
    def avg_recv_latest(self) -> float:
        return (
            self.total_recv_latest / len(self.recv_bytes_latest)
            if self.recv_bytes_latest
            else 0.0
        )


# ── timestamp helpers ─────────────────────────────────────────────────────────


def parse_ts(ts_str: str) -> datetime:
    """Parse RFC-3339 / ISO-8601 (including nanoseconds) into an aware datetime."""
    ts = ts_str
    plus = ts.find("+", 19)
    z = ts.endswith("Z")
    tz = ts[plus:] if plus != -1 else ("+00:00" if z else "")
    body = ts[:plus] if plus != -1 else (ts[:-1] if z else ts)
    if "." in body:
        base, frac = body.split(".", 1)
        body = base + "." + frac[:6].ljust(6, "0")
    return datetime.fromisoformat(body + tz)


def server_kind(node_id: str) -> str | None:
    """Return 'gc' or 'normal' based on node_id prefix, or None if unrecognised."""
    if node_id.startswith("gc-server-"):
        return "gc"
    if node_id.startswith("normal-server-"):
        return "normal"
    return None


# ── phase 0: find global start ───────────────────────────────────────────────


def find_global_start(metrics_path: Path) -> datetime | None:
    """Single pass to find the latest client_operation timestamp across all node_ids."""
    latest: datetime | None = None
    with metrics_path.open() as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                entry = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if entry.get("fields", {}).get("event") != "client_operation":
                continue
            ts_str = entry.get("timestamp", "")
            if not ts_str:
                continue
            try:
                dt = parse_ts(ts_str)
            except ValueError:
                continue
            if latest is None or dt > latest:
                latest = dt
    return latest


# ── phase 1: discover server ids ─────────────────────────────────────────────


def discover_servers(metrics_path: Path) -> tuple[set[str], set[str]]:
    """Return (gc_ids, normal_ids) found in the file."""
    gc_ids: set[str] = set()
    normal_ids: set[str] = set()
    with metrics_path.open() as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                entry = json.loads(raw)
            except json.JSONDecodeError:
                continue
            node_id = entry.get("fields", {}).get("node_id", "")
            kind = server_kind(node_id)
            if kind == "gc":
                gc_ids.add(node_id)
            elif kind == "normal":
                normal_ids.add(node_id)
    return gc_ids, normal_ids


# ── phase 2: GC replicas — own-cutoff window ─────────────────────────────────


def analyze_gc(
    metrics_path: Path,
    gc_ids: set[str],
    normal_ids: set[str],
    global_start_dt: datetime | None,
) -> dict[str, ServerStats]:
    """
    Single pass, GC replicas only. For each GC server, accumulate
    send_envelope / receive_envelope bytes in (START, that server's own
    cutoff), where "own cutoff" is the first gc_coordinator_metrics event
    where v_stable has reached full convergence (len == total servers).
    """
    expected_servers = len(gc_ids) + len(normal_ids)

    servers: dict[str, ServerStats] = {}
    for nid in gc_ids:
        s = ServerStats(name=nid, kind="gc")
        if global_start_dt is not None:
            s.start_timestamp = global_start_dt.isoformat()
        servers[nid] = s

    with metrics_path.open() as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                entry = json.loads(raw)
            except json.JSONDecodeError as exc:
                print(f"  [WARN] skipping malformed line – {exc}", file=sys.stderr)
                continue

            fields = entry.get("fields", {})
            node_id = fields.get("node_id", "")
            event = fields.get("event", "")
            ts_str = entry.get("timestamp", "")

            stats = servers.get(node_id)
            if stats is None or stats.done:
                continue

            entry_dt: datetime | None = None
            if ts_str:
                try:
                    entry_dt = parse_ts(ts_str)
                except ValueError:
                    pass

            if global_start_dt is not None and entry_dt is not None:
                if entry_dt <= global_start_dt:
                    continue

            stats.lines_processed += 1

            if event == "gc_coordinator_metrics":
                rnd = fields.get("dissemination_round")
                v_stable = json.loads(fields.get("v_stable", "{}"))
                if len(v_stable) >= expected_servers:
                    stats.cutoff_timestamp = ts_str
                    stats.cutoff_round = rnd
                    stats.done = True
                else:
                    if rnd is not None:
                        if stats.first_round is None:
                            stats.first_round = rnd
                        stats.last_round = rnd

            elif event == "send_envelope":
                if (sz := fields.get("size_bytes")) is not None:
                    stats.sent_bytes.append(sz)

            elif event == "receive_envelope":
                if (sz := fields.get("size_bytes")) is not None:
                    stats.recv_bytes.append(sz)

    return servers


def analyze_gc_fixed_windows(
    metrics_path: Path,
    gc_servers: dict[str, ServerStats],
    gc_ids: set[str],
    global_start_dt: datetime | None,
    earliest_cutoff_dt: datetime | None,
    latest_cutoff_dt: datetime | None,
) -> None:
    """
    Extra pass, GC replicas only. Unlike analyze_gc() (which stops each GC
    server at its OWN cutoff), this accumulates send_envelope /
    receive_envelope bytes for each GC server into the same two FIXED
    global windows used for Normal replicas:
        sent_bytes_fixed_earliest / recv_bytes_fixed_earliest → (START, earliest_cutoff_dt)
        sent_bytes_fixed_latest   / recv_bytes_fixed_latest   → (START, latest_cutoff_dt)
    This lets GC and Normal byte totals be combined into a grand total over
    identical wall-clock windows.
    """
    if earliest_cutoff_dt is None and latest_cutoff_dt is None:
        return

    with metrics_path.open() as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                entry = json.loads(raw)
            except json.JSONDecodeError:
                continue

            fields = entry.get("fields", {})
            event = fields.get("event", "")
            if event not in ("send_envelope", "receive_envelope"):
                continue

            node_id = fields.get("node_id", "")
            if node_id not in gc_ids:
                continue
            stats = gc_servers.get(node_id)
            if stats is None:
                continue

            ts_str = entry.get("timestamp", "")
            if not ts_str:
                continue
            try:
                entry_dt = parse_ts(ts_str)
            except ValueError:
                continue

            if global_start_dt is not None and entry_dt <= global_start_dt:
                continue

            size = fields.get("size_bytes")
            if size is None:
                continue

            if earliest_cutoff_dt is not None and entry_dt < earliest_cutoff_dt:
                if event == "send_envelope":
                    stats.sent_bytes_fixed_earliest.append(size)
                else:
                    stats.recv_bytes_fixed_earliest.append(size)

            if latest_cutoff_dt is not None and entry_dt < latest_cutoff_dt:
                if event == "send_envelope":
                    stats.sent_bytes_fixed_latest.append(size)
                else:
                    stats.recv_bytes_fixed_latest.append(size)


def compute_global_cutoffs(
    gc_stats: list[ServerStats],
) -> tuple[datetime | None, datetime | None]:
    """Return (earliest_cutoff_dt, latest_cutoff_dt) across GC servers that reached one."""
    cutoff_dts = [parse_ts(s.cutoff_timestamp) for s in gc_stats if s.cutoff_timestamp]
    if not cutoff_dts:
        return None, None
    return min(cutoff_dts), max(cutoff_dts)


# ── phase 3: Normal replicas — start→earliest and start→latest windows ──────


def analyze_normal(
    metrics_path: Path,
    normal_ids: set[str],
    global_start_dt: datetime | None,
    earliest_cutoff_dt: datetime | None,
    latest_cutoff_dt: datetime | None,
) -> dict[str, ServerStats]:
    """
    Single pass, Normal replicas only. For each normal server, accumulate
    send_envelope / receive_envelope bytes into two fixed windows shared by
    all normal replicas:
        sent_bytes         / recv_bytes         → (START, earliest_cutoff_dt)
        sent_bytes_latest   / recv_bytes_latest  → (START, latest_cutoff_dt)
    """
    servers: dict[str, ServerStats] = {}
    for nid in normal_ids:
        s = ServerStats(name=nid, kind="normal")
        if global_start_dt is not None:
            s.start_timestamp = global_start_dt.isoformat()
        servers[nid] = s

    if earliest_cutoff_dt is None and latest_cutoff_dt is None:
        return servers  # nothing to anchor windows on

    with metrics_path.open() as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                entry = json.loads(raw)
            except json.JSONDecodeError:
                continue

            fields = entry.get("fields", {})
            event = fields.get("event", "")
            if event not in ("send_envelope", "receive_envelope"):
                continue

            node_id = fields.get("node_id", "")
            stats = servers.get(node_id)
            if stats is None:
                continue

            ts_str = entry.get("timestamp", "")
            if not ts_str:
                continue
            try:
                entry_dt = parse_ts(ts_str)
            except ValueError:
                continue

            if global_start_dt is not None and entry_dt <= global_start_dt:
                continue

            size = fields.get("size_bytes")
            if size is None:
                continue

            stats.lines_processed += 1

            if earliest_cutoff_dt is not None and entry_dt < earliest_cutoff_dt:
                if event == "send_envelope":
                    stats.sent_bytes.append(size)
                else:
                    stats.recv_bytes.append(size)

            if latest_cutoff_dt is not None and entry_dt < latest_cutoff_dt:
                if event == "send_envelope":
                    stats.sent_bytes_latest.append(size)
                else:
                    stats.recv_bytes_latest.append(size)

    return servers


# ── reporting ─────────────────────────────────────────────────────────────────


def _print_stats_table(
    total_sent,
    total_recv,
    cnt_sent,
    cnt_recv,
    avg_sent,
    avg_recv,
    indent="  ",
    num_servers: int | None = None,
) -> None:
    i = indent
    print(f"\n{i}{'Metric':<30} {'Sent':>12} {'Received':>12}")
    print(f"{i}{'──────':<30} {'────':>12} {'────────':>12}")
    print(f"{i}{'Total bytes':<30} {total_sent:>12,} {total_recv:>12,}")
    print(f"{i}{'Message count':<30} {cnt_sent:>12,} {cnt_recv:>12,}")
    print(f"{i}{'Avg bytes / message':<30} {avg_sent:>12,.2f} {avg_recv:>12,.2f}")
    if num_servers:
        avg_total_sent_per_server = total_sent / num_servers
        avg_total_recv_per_server = total_recv / num_servers
        print(
            f"{i}{'Avg total bytes / server':<30} "
            f"{avg_total_sent_per_server:>12,.2f} {avg_total_recv_per_server:>12,.2f}"
        )
    print()


def print_gc_server_report(stats: ServerStats) -> None:
    sep = "─" * 62
    print(f"\n{sep}")
    print(f"  Server : {stats.name}  [GC]")
    print(sep)
    print(f"  Lines processed (in window)    : {stats.lines_processed}")

    if stats.start_timestamp:
        print(f"  Window start                   : {stats.start_timestamp}")
    else:
        print(
            "  Window start                   : (no client_operation – from beginning)"
        )

    if stats.cutoff_timestamp:
        print(f"  Window end (own cutoff)        : {stats.cutoff_timestamp}")
        if stats.cutoff_round is not None:
            print(f"  Cutoff dissemination_round     : {stats.cutoff_round}")
    else:
        print(
            "  Window end                     : (v_stable never reached full – all lines)"
        )

    if stats.rounds_in_window > 0:
        last_round = (
            stats.cutoff_round
            if stats.cutoff_round is not None
            else stats.last_round
            if stats.last_round is not None
            else "?"
        )
        print(
            f"  Rounds in window               : {stats.rounds_in_window}  "
            f"(first={stats.first_round}, last={last_round})"
        )

    print("\n  Bytes (start → own cutoff):")
    _print_stats_table(
        stats.total_sent,
        stats.total_recv,
        len(stats.sent_bytes),
        len(stats.recv_bytes),
        stats.avg_sent,
        stats.avg_recv,
    )


def print_normal_server_report(
    stats: ServerStats,
    earliest_cutoff_dt: datetime | None,
    latest_cutoff_dt: datetime | None,
) -> None:
    sep = "─" * 62
    print(f"\n{sep}")
    print(f"  Server : {stats.name}  [NORMAL]")
    print(sep)
    print(f"  Lines processed (in windows)   : {stats.lines_processed}")

    if stats.start_timestamp:
        print(f"  Window start                   : {stats.start_timestamp}")
    else:
        print(
            "  Window start                   : (no client_operation – from beginning)"
        )

    if earliest_cutoff_dt is not None:
        print(f"  Earliest GC cutoff             : {earliest_cutoff_dt.isoformat()}")
    if latest_cutoff_dt is not None:
        print(f"  Latest GC cutoff               : {latest_cutoff_dt.isoformat()}")
    if earliest_cutoff_dt is None and latest_cutoff_dt is None:
        print("  (no GC cutoff observed – windows unavailable)")
        return

    if earliest_cutoff_dt is not None:
        print("\n  Bytes (start → earliest cutoff):")
        _print_stats_table(
            stats.total_sent,
            stats.total_recv,
            len(stats.sent_bytes),
            len(stats.recv_bytes),
            stats.avg_sent,
            stats.avg_recv,
        )

    if latest_cutoff_dt is not None:
        print("  Bytes (start → latest cutoff):")
        _print_stats_table(
            stats.total_sent_latest,
            stats.total_recv_latest,
            len(stats.sent_bytes_latest),
            len(stats.recv_bytes_latest),
            stats.avg_sent_latest,
            stats.avg_recv_latest,
        )


def print_gc_summary(title: str, group: list[ServerStats]) -> None:
    sep = "═" * 62
    print(f"\n{sep}")
    print(f"  {title}")
    print(sep)

    if not group:
        print("  (no servers in this group)\n")
        return

    print(f"  Servers : {', '.join(sorted(s.name for s in group))}")

    with_cutoff = [s for s in group if s.cutoff_timestamp]
    if with_cutoff:
        cutoff_dts = [(s, parse_ts(s.cutoff_timestamp)) for s in with_cutoff]
        earliest_cutoff = min(cutoff_dts, key=lambda x: x[1])
        latest_cutoff = max(cutoff_dts, key=lambda x: x[1])
        rounds_with_server = [
            (s, s.cutoff_round) for s in with_cutoff if s.cutoff_round is not None
        ]
        rounds = [r for _, r in rounds_with_server]
        avg_round = sum(rounds) / len(rounds) if rounds else None
        min_round = (
            min(rounds_with_server, key=lambda x: x[1]) if rounds_with_server else None
        )
        max_round = (
            max(rounds_with_server, key=lambda x: x[1]) if rounds_with_server else None
        )

        print(
            f"\n  Cutoff dissemination round     : avg={avg_round:.1f}"
            if avg_round is not None
            else "\n  Cutoff dissemination round     : n/a"
        )
        if min_round:
            print(
                f"  Earliest dissemination round   : {min_round[1]}  ({min_round[0].name})"
            )
        if max_round:
            print(
                f"  Latest dissemination round     : {max_round[1]}  ({max_round[0].name})"
            )
        print(
            f"  Earliest cutoff                : {earliest_cutoff[0].name}  @  {earliest_cutoff[1].isoformat()}"
        )
        print(
            f"  Latest cutoff                  : {latest_cutoff[0].name}  @  {latest_cutoff[1].isoformat()}"
        )

    with_rounds = [s for s in group if s.rounds_in_window > 0]
    if with_rounds:
        total_rounds = sum(s.rounds_in_window for s in with_rounds)
        avg_rounds = total_rounds / len(with_rounds)
        first_rounds = [
            (s, s.first_round) for s in with_rounds if s.first_round is not None
        ]
        cutoff_rounds = [
            (s, s.cutoff_round) for s in with_rounds if s.cutoff_round is not None
        ]
        min_first = min(first_rounds, key=lambda x: x[1]) if first_rounds else None
        max_first = max(first_rounds, key=lambda x: x[1]) if first_rounds else None
        min_last = min(cutoff_rounds, key=lambda x: x[1]) if cutoff_rounds else None
        max_last = max(cutoff_rounds, key=lambda x: x[1]) if cutoff_rounds else None
        print(
            f"\n  Rounds in window (per server)  : avg={avg_rounds:.1f}, total={total_rounds}"
        )
        if min_first:
            print(
                f"  First round in window          : {min_first[1]}  ({min_first[0].name}) "
                f"– {max_first[1]}  ({max_first[0].name})"
            )
        if min_last:
            print(
                f"  Last round in window           : {min_last[1]}  ({min_last[0].name}) "
                f"– {max_last[1]}  ({max_last[0].name})"
            )

    all_sent = [b for s in group for b in s.sent_bytes]
    all_recv = [b for s in group for b in s.recv_bytes]
    total_sent = sum(all_sent)
    total_recv = sum(all_recv)
    avg_sent = total_sent / len(all_sent) if all_sent else 0.0
    avg_recv = total_recv / len(all_recv) if all_recv else 0.0

    print("\n  Bytes (start → each server's own cutoff):")
    _print_stats_table(
        total_sent,
        total_recv,
        len(all_sent),
        len(all_recv),
        avg_sent,
        avg_recv,
        num_servers=len(group),
    )


def print_normal_summary(
    title: str,
    group: list[ServerStats],
    earliest_cutoff_dt: datetime | None,
    latest_cutoff_dt: datetime | None,
) -> None:
    sep = "═" * 62
    print(f"\n{sep}")
    print(f"  {title}")
    print(sep)

    if not group:
        print("  (no servers in this group)\n")
        return

    print(f"  Servers : {', '.join(sorted(s.name for s in group))}")
    if earliest_cutoff_dt is not None:
        print(f"  Earliest GC cutoff              : {earliest_cutoff_dt.isoformat()}")
    if latest_cutoff_dt is not None:
        print(f"  Latest GC cutoff                : {latest_cutoff_dt.isoformat()}")

    if earliest_cutoff_dt is not None:
        all_sent = [b for s in group for b in s.sent_bytes]
        all_recv = [b for s in group for b in s.recv_bytes]
        total_sent = sum(all_sent)
        total_recv = sum(all_recv)
        avg_sent = total_sent / len(all_sent) if all_sent else 0.0
        avg_recv = total_recv / len(all_recv) if all_recv else 0.0
        print("\n  Bytes (start → earliest cutoff):")
        _print_stats_table(
            total_sent,
            total_recv,
            len(all_sent),
            len(all_recv),
            avg_sent,
            avg_recv,
            num_servers=len(group),
        )

    if latest_cutoff_dt is not None:
        all_sent_l = [b for s in group for b in s.sent_bytes_latest]
        all_recv_l = [b for s in group for b in s.recv_bytes_latest]
        total_sent_l = sum(all_sent_l)
        total_recv_l = sum(all_recv_l)
        avg_sent_l = total_sent_l / len(all_sent_l) if all_sent_l else 0.0
        avg_recv_l = total_recv_l / len(all_recv_l) if all_recv_l else 0.0
        print("  Bytes (start → latest cutoff):")
        _print_stats_table(
            total_sent_l,
            total_recv_l,
            len(all_sent_l),
            len(all_recv_l),
            avg_sent_l,
            avg_recv_l,
            num_servers=len(group),
        )

    if earliest_cutoff_dt is None and latest_cutoff_dt is None:
        print("  (no GC cutoff observed – windows unavailable)\n")


def print_grand_total(
    gc_stats: list[ServerStats],
    normal_stats: list[ServerStats],
    earliest_cutoff_dt: datetime | None,
    latest_cutoff_dt: datetime | None,
) -> None:
    sep = "═" * 62
    print(f"\n{sep}")
    print("  FINAL TOTAL – GC + NORMAL COMBINED")
    print(sep)

    if earliest_cutoff_dt is None and latest_cutoff_dt is None:
        print("  (no GC cutoff observed – combined windows unavailable)\n")
        return

    total_servers = len(gc_stats) + len(normal_stats)
    print(
        f"  Servers : {len(gc_stats)} GC + {len(normal_stats)} Normal = {total_servers}"
    )
    if earliest_cutoff_dt is not None:
        print(f"  Earliest GC cutoff              : {earliest_cutoff_dt.isoformat()}")
    if latest_cutoff_dt is not None:
        print(f"  Latest GC cutoff                : {latest_cutoff_dt.isoformat()}")

    if earliest_cutoff_dt is not None:
        all_sent = [b for s in gc_stats for b in s.sent_bytes_fixed_earliest] + [
            b for s in normal_stats for b in s.sent_bytes
        ]
        all_recv = [b for s in gc_stats for b in s.recv_bytes_fixed_earliest] + [
            b for s in normal_stats for b in s.recv_bytes
        ]
        total_sent = sum(all_sent)
        total_recv = sum(all_recv)
        avg_sent = total_sent / len(all_sent) if all_sent else 0.0
        avg_recv = total_recv / len(all_recv) if all_recv else 0.0
        print("\n  Bytes (start → earliest cutoff), GC + Normal:")
        _print_stats_table(
            total_sent,
            total_recv,
            len(all_sent),
            len(all_recv),
            avg_sent,
            avg_recv,
            num_servers=total_servers,
        )

    if latest_cutoff_dt is not None:
        all_sent_l = [b for s in gc_stats for b in s.sent_bytes_fixed_latest] + [
            b for s in normal_stats for b in s.sent_bytes_latest
        ]
        all_recv_l = [b for s in gc_stats for b in s.recv_bytes_fixed_latest] + [
            b for s in normal_stats for b in s.recv_bytes_latest
        ]
        total_sent_l = sum(all_sent_l)
        total_recv_l = sum(all_recv_l)
        avg_sent_l = total_sent_l / len(all_sent_l) if all_sent_l else 0.0
        avg_recv_l = total_recv_l / len(all_recv_l) if all_recv_l else 0.0
        print("  Bytes (start → latest cutoff), GC + Normal:")
        _print_stats_table(
            total_sent_l,
            total_recv_l,
            len(all_sent_l),
            len(all_recv_l),
            avg_sent_l,
            avg_recv_l,
            num_servers=total_servers,
        )


# ── main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python analyze_metrics.py <metrics.jsonl>")
        sys.exit(1)

    metrics_path = Path(sys.argv[1])
    if not metrics_path.is_file():
        print(f"Error: '{metrics_path}' is not a file.")
        sys.exit(1)

    # ── phase 0: global start ─────────────────────────────────────────────────
    print("Scanning for latest client_operation …")
    global_start_dt = find_global_start(metrics_path)
    if global_start_dt is not None:
        print(f"  ➜  Global start : {global_start_dt.isoformat()}")
    else:
        print("  [WARN] No client_operation found – counting from beginning of file.")
    print()

    # ── phase 1: discover server ids ─────────────────────────────────────────
    print("Discovering servers …")
    gc_ids, normal_ids = discover_servers(metrics_path)
    print(f"  ➜  GC servers     : {len(gc_ids)}  ({', '.join(sorted(gc_ids))})")
    print(f"  ➜  Normal servers : {len(normal_ids)}  ({', '.join(sorted(normal_ids))})")
    print()

    if not gc_ids and not normal_ids:
        print(
            "No recognised servers found (expected node_id prefixes: gc-server-*, normal-server-*)."
        )
        sys.exit(1)

    # ── phase 2: GC replicas (own-cutoff window) ──────────────────────────────
    print("Analysing GC replicas …")
    gc_servers = analyze_gc(metrics_path, gc_ids, normal_ids, global_start_dt)
    gc_stats = sorted(gc_servers.values(), key=lambda s: s.name)

    earliest_cutoff_dt, latest_cutoff_dt = compute_global_cutoffs(gc_stats)
    if earliest_cutoff_dt is not None:
        print(f"  ➜  Earliest GC cutoff : {earliest_cutoff_dt.isoformat()}")
        print(f"  ➜  Latest GC cutoff   : {latest_cutoff_dt.isoformat()}")
        analyze_gc_fixed_windows(
            metrics_path,
            gc_servers,
            gc_ids,
            global_start_dt,
            earliest_cutoff_dt,
            latest_cutoff_dt,
        )
    else:
        print("  [WARN] No GC server reached full convergence.")
    print()

    # ── phase 3: Normal replicas (start→earliest, start→latest windows) ─────
    print("Analysing Normal replicas …")
    normal_servers = analyze_normal(
        metrics_path, normal_ids, global_start_dt, earliest_cutoff_dt, latest_cutoff_dt
    )
    normal_stats = sorted(normal_servers.values(), key=lambda s: s.name)
    print()

    # ── per-server reports ────────────────────────────────────────────────────
    if gc_stats:
        print("\n" + "━" * 62)
        print("  GC SERVERS – individual results")
        for s in gc_stats:
            print_gc_server_report(s)

    if normal_stats:
        print("\n" + "━" * 62)
        print("  NORMAL SERVERS – individual results")
        for s in normal_stats:
            print_normal_server_report(s, earliest_cutoff_dt, latest_cutoff_dt)

    # ── summaries ─────────────────────────────────────────────────────────────
    print_gc_summary("SUMMARY – GC SERVERS", gc_stats)
    if normal_stats:
        print_normal_summary(
            "SUMMARY – NORMAL SERVERS",
            normal_stats,
            earliest_cutoff_dt,
            latest_cutoff_dt,
        )

    print_grand_total(gc_stats, normal_stats, earliest_cutoff_dt, latest_cutoff_dt)


if __name__ == "__main__":
    main()
