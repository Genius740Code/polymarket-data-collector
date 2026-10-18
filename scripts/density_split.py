#!/usr/bin/env python
"""Density-split attribution for one asset-day (report-only, read-only).

For every market window of --asset on --date (from
markets_latest/markets_latest.parquet, deduped per condition_id keeping the
latest updated_at), classify the MISSING 500ms-grid ticks (expected 2/s per
market from market_start to market_end, clamped to the measured UTC day and
to a small freshness grace at "now" — the report never speculates about
ticks still in flight or in the future) into named buckets, COUNTS ONLY —
no rows are printed, no data is written:

- post_roll: missing ticks in (market_start, market_start+120s] — the lane
  roll window right after a market opens.
- outage: tick seconds where the asset had NO rows at all (per-second union
  of the day's covered tick coverage across all of its conditions) that
  intersect narrated coverage_gap ranges for the same day from
  data/collector_events (gap narration: [last_bucket, cur_bucket) ms).
  Narration beats inference: an outage range claims its ticks first.
- book_empty_skipped: missing ticks inside a market whose SAME-DAY evidence
  says it was dataless — the collector skips empty-book rows except a
  60-tick keepalive (collector.py P0 _bbo_empty skip). Conservative,
  condition-level, BOTH bars must hold:
    (a) the market emits book_empty_skipped events that day, or every row it
        did write is an all-BBO-null keepalive trail (null_rows == rows), and
    (b) the market is sparse: rows written are far below the 2/s grid
        (<40%), i.e. the skip really ran.
- other: the remainder, exact count (partial-coverage seconds, weak or no
  evidence either way — honest unknown, never a guess).

absent_row_total is also printed: the conservative intermediate bucket (all
missing ticks in zero-asset-row seconds). outage is a subset of it; the
four printed counters are disjoint (per-second first-match priority:
post_roll → outage → book_empty_skipped → other).

Budget: --budget-s caps the whole wall run like the scorecard; a phase that
outlives the budget degrades its columns to n/a(partial) honestly. Reads
are projected (only the columns needed) and streamed into tiny aggregates.
HONEST-GAP LAW (AGENT.md): gaps are truth; this script only measures them.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import time
from datetime import datetime, timezone
from collections import deque
from pathlib import Path

import pyarrow.parquet as pq

DATA_ROOT = Path("/home/fese/polymarket-data-collector/data")
TICKS_PER_SEC = 2                # expected 500ms-grid ticks per market per second
POST_ROLL_S = 120                # (market_start, start+120s] roll window
BES_SKIP_MAX_DENSITY = 0.40      # dataless evidence bar (b): rows < 40% of grid
FRESH_GRACE_S = 5                # do not count ticks of the newest seconds
EVENT_WORKERS = 4                # collector_events projected readers
HIVE_WORKERS = 4                 # per-file hive readers (3.9GB box: tiny)
EVENTS_FRAC = 0.35               # budget share for the collector_events scan
DAY_S = 86400
NS = 1_000_000_000

HIVE_COLS = ["condition_id", "ts_snapshot_ns",
             "up_bid", "up_ask", "down_bid", "down_ask"]


def _null(v) -> bool:
    return v is None or (isinstance(v, float) and v != v)


def _bbo_all_null(v) -> bool:
    return _null(v[0]) and _null(v[1]) and _null(v[2]) and _null(v[3])


class _MarketState:
    """Per-condition tiny aggregates only — never row data."""
    __slots__ = ("sec_counts", "rows", "null_rows")

    def __init__(self) -> None:
        self.sec_counts: dict = {}  # epoch second -> row count within it
        self.rows = 0
        self.null_rows = 0


def _day_bounds(date_str: str) -> int:
    """UTC midnight of the measured day, as an epoch second."""
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp())


def _parse_iso_utc(s):
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(str(s)).timestamp())
    except Exception:
        return None


def load_market_windows(markets_file: Path, asset: str, d0: int) -> dict:
    """condition_id -> (start_epoch_s, end_epoch_s) for windows overlapping
    the measured day; deduped per condition keeping the latest updated_at.
    Streamed projected batches, tiny dict only (never a table kept)."""
    best: dict = {}
    try:
        pf = pq.ParquetFile(str(markets_file))
        for batch in pf.iter_batches(batch_size=10_000, columns=[
            "asset", "market_start_ts", "market_end_ts", "condition_id", "updated_at",
        ]):
            a = batch.column("asset").to_pylist()
            ms = batch.column("market_start_ts").to_pylist()
            me = batch.column("market_end_ts").to_pylist()
            cid = batch.column("condition_id").to_pylist()
            up = batch.column("updated_at").to_pylist()
            for _a, _ms, _me, _c, _u in zip(a, ms, me, cid, up):
                if _a != asset or not _c:
                    continue
                s, e = _parse_iso_utc(_ms), _parse_iso_utc(_me)
                if s is None or e is None or e <= d0 or s >= d0 + DAY_S:
                    continue
                prev = best.get(_c)
                key = (str(_u) if _u else "", s, e)
                if prev is None or key > (prev[2], prev[0], prev[1]):
                    best[_c] = (s, e, key[0])
    except Exception:
        pass  # honest: no windows → an all-zero report below
    return {k: (m[0], m[1]) for k, m in best.items()}


def _safe_events_read(fp: Path):
    try:
        return pq.read_table(str(fp), columns=["event_type", "details"])
    except Exception:
        return None


def scan_collector_events(ev_dir: Path, asset: str, deadline: float) -> tuple:
    """Day collector_events, projected 2-column read, streamed into counts.

    Returns (coverage_gap ms-ranges, bes condition set, partial flag,
    files_read, files_total). The event file carries asset inside the
    details JSON for book_empty_skipped; coverage_gap narration is
    collector-wide (applies to every lane, conservative)."""
    ranges: list = []
    bes_conds: set = set()
    files_read = 0
    partial = False
    if not ev_dir.exists():
        return ranges, bes_conds, partial, files_read, 0
    parts = sorted(p for p in ev_dir.glob("*.parquet") if not p.name.endswith(".tmp"))
    total = len(parts)

    def _drain(dq, ex, it):
        while len(dq) < EVENT_WORKERS * 2:
            nxt = next(it, None)
            if nxt is None:
                return
            dq.append(ex.submit(_safe_events_read, nxt))

    with concurrent.futures.ThreadPoolExecutor(max_workers=EVENT_WORKERS) as ex:
        pending: deque = deque()
        it = iter(parts)
        _drain(pending, ex, it)
        while pending:
            if time.monotonic() > deadline:
                partial = True
                break
            fut = pending.popleft()
            tbl = fut.result()
            _drain(pending, ex, it)
            if tbl is None:
                continue
            files_read += 1
            try:
                types = tbl["event_type"].to_pylist()
                det = tbl["details"].to_pylist()
            except Exception:
                continue
            for et, d in zip(types, det):
                if not isinstance(d, str) or not d.startswith("{"):
                    continue
                try:
                    j = json.loads(d)
                except Exception:
                    continue
                if et == "coverage_gap":
                    last, cur = j.get("last_bucket"), j.get("cur_bucket")
                    if isinstance(last, int) and isinstance(cur, int) and cur > last:
                        ranges.append((last, cur))
                elif et == "book_empty_skipped" and j.get("asset") == asset:
                    c = j.get("condition_id")
                    if c:
                        bes_conds.add(str(c))
    return ranges, bes_conds, partial, files_read, total


def _safe_hive_read(fp: Path):
    try:
        return pq.read_table(str(fp), columns=HIVE_COLS)
    except Exception:
        return None


def scan_hive(asset_dir: Path, deadline: float) -> tuple:
    """Day raw hive for one asset: (condition -> _MarketState,
    asset-level covered-seconds set, partial, scanned, total).

    Projected 6-column reads streamed per file; a row counts as the
    keepalive trail only when ALL four best prices are null (observed,
    never imputed)."""
    state: dict = {}
    covered: set = set()
    partial = False
    scanned = 0
    if not asset_dir.exists():
        return state, covered, partial, scanned, 0
    parts = sorted(p for p in asset_dir.glob("*.parquet") if not p.name.endswith(".tmp"))
    total = len(parts)

    def _drain(dq, ex, it):
        while len(dq) < HIVE_WORKERS * 2:
            nxt = next(it, None)
            if nxt is None:
                return
            dq.append(ex.submit(_safe_hive_read, nxt))

    with concurrent.futures.ThreadPoolExecutor(max_workers=HIVE_WORKERS) as ex:
        pending: deque = deque()
        it = iter(parts)
        _drain(pending, ex, it)
        while pending:
            if time.monotonic() > deadline:
                partial = True
                break
            fut = pending.popleft()
            tbl = fut.result()
            _drain(pending, ex, it)
            if tbl is None:
                continue
            scanned += 1
            try:
                cid = tbl["condition_id"].to_pylist()
                ts = tbl["ts_snapshot_ns"].to_pylist()
                bbo = list(zip(
                    tbl["up_bid"].to_pylist(), tbl["up_ask"].to_pylist(),
                    tbl["down_bid"].to_pylist(), tbl["down_ask"].to_pylist(),
                ))
            except Exception:
                continue
            for c, t, b in zip(cid, ts, bbo):
                if not c or t is None:
                    continue
                st = state.setdefault(str(c), _MarketState())
                st.rows += 1
                if _bbo_all_null(b):
                    st.null_rows += 1
                sec = t // NS
                st.sec_counts[sec] = st.sec_counts.get(sec, 0) + 1
                covered.add(sec)
    return state, covered, partial, scanned, total


def _in_range_ms(ranges_ms: list, sec: int) -> bool:
    ms = sec * 1000
    return any(a <= ms < b for a, b in ranges_ms)


def _dataless(st, cond, bes_conds, expected_ticks) -> bool:
    """Bar (a) AND bar (b) per the module docstring: evidence is either the
    same-day narration (book_empty_skipped events for this condition) or an
    OBSERVED all-BBO-null keepalive trail — never anything imputed."""
    if st is None:
        return cond in bes_conds  # never a row: narration evidence only
    sparse = st.rows < BES_SKIP_MAX_DENSITY * expected_ticks
    trail = st.rows > 0 and st.null_rows == st.rows
    return (cond in bes_conds or trail) and sparse


def build_report(date_str, asset, windows, state, covered,
                 outage_ranges_ms, bes_conds, count_missing=False) -> tuple:
    """Attribution per missing tick-second, first-match priority:
    post_roll → outage → book_empty_skipped-dataless → other.
    Returns (one-row totals dict, worst-10 windows list)."""
    d0 = _day_bounds(date_str)
    end = min(d0 + DAY_S, int(time.time()) - FRESH_GRACE_S)
    buckets = {"post_roll": 0, "bes": 0, "outage": 0, "other": 0, "absent": 0}
    expected_tot = present_tot = 0
    worst: list = []
    for _c, (ms, me) in windows.items():
        lo = max(ms, d0)
        hi = min(me, end)
        if hi <= lo:
            continue
        secs = range(lo, hi)
        exp = len(secs) * TICKS_PER_SEC
        st = state.get(_c)
        per_m = {"post_roll": 0, "bes": 0, "outage": 0, "other": 0, "absent": 0, "missing": 0}
        # evidence decided once per market (same-day narration or trail + sparsity)
        dataless = _dataless(st, _c, bes_conds, exp)
        for sec in secs:
            have = min(st.sec_counts.get(sec, 0) if st else 0, TICKS_PER_SEC)
            present_tot += have
            miss = TICKS_PER_SEC - have
            if miss <= 0:
                continue
            per_m["missing"] += miss
            if sec <= ms + POST_ROLL_S:
                per_m["post_roll"] += miss
            elif _in_range_ms(outage_ranges_ms, sec):
                per_m["outage"] += miss
                per_m["absent"] += miss
            elif dataless:
                per_m["bes"] += miss
            elif sec not in covered:
                per_m["other"] += miss
                per_m["absent"] += miss
            else:
                per_m["other"] += miss
        expected_tot += exp
        for k in ("post_roll", "bes", "outage", "other", "absent"):
            buckets[k] += per_m[k]
        worst.append({"start": ms, "missing": per_m["missing"], "cond": _c})
    tot = {
        "windows": len(windows),
        "expected": expected_tot,
        "missing": max(0, expected_tot - present_tot) if count_missing else None,
        "post_roll": buckets["post_roll"] if count_missing else None,
        "bes": buckets["bes"] if count_missing else None,
        "outage": buckets["outage"] if count_missing else None,
        "other": buckets["other"] if count_missing else None,
        "absent_row_total": buckets["absent"] if count_missing else None,
    }
    worst.sort(key=lambda w: (-w["missing"], w["start"]))
    return tot, worst


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Density-split attribution (report-only): classify missing "
                    "500ms grid ticks of one asset-day into post_roll / "
                    "book_empty_skipped / outage / other buckets, counts only.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--date", default="2026-10-18", help="YYYY-MM-DD UTC")
    ap.add_argument("--asset", default="BTC", help="asset lane (e.g. BTC, ETH, SOL)")
    ap.add_argument("--budget-s", type=float, default=480.0,
                    help="wall-clock budget for the whole run (like the "
                         "scorecard); overrun degrades the affected columns "
                         "to n/a(partial), never invented values")
    ap.add_argument("--data-root", default=str(DATA_ROOT), help=argparse.SUPPRESS)
    return ap


def run_density_split(
    data_root: Path, date_str: str, asset: str, budget_s: float = 480.0
) -> dict:
    """Composable core: computes the report dict; main() only renders it."""
    started = time.monotonic()

    # Phase 1 — same-day narration: coverage_gap ranges + book_empty_skipped
    ev_deadline = started + EVENTS_FRAC * budget_s
    ranges, bes_conds, ev_partial, ev_read, ev_total = scan_collector_events(
        data_root / "collector_events" / f"date={date_str}", asset, ev_deadline
    )

    # Phase 2 — market windows (cheap)
    windows = load_market_windows(
        data_root / "markets_latest" / "markets_latest.parquet", asset,
        _day_bounds(date_str),
    )

    # Phase 3 — the day's raw hive: per-second coverage + keepalive trail
    state, covered, hive_partial, hs, ht = scan_hive(
        data_root / "book_snapshots_500ms" / f"date={date_str}" / f"asset={asset}",
        started + budget_s,
    )

    count_missing = not (hive_partial or time.monotonic() > started + budget_s)
    if count_missing:
        totals, worst = build_report(
            date_str, asset, windows, state, covered, ranges, bes_conds,
            count_missing=True,
        )
    else:
        totals, worst = build_report(
            date_str, asset, windows, state, covered, ranges, bes_conds,
            count_missing=False,
        )
        totals["missing"] = None
        worst = []

    return {
        "asset": asset,
        "date": date_str,
        "windows": len(windows),
        "partial": bool(ev_partial or not count_missing),
        "ev_partial": ev_partial,
        "ev_read": ev_read,
        "ev_total": ev_total,
        "ranges": len(ranges),
        "bes": len(bes_conds),
        "hive": (hive_partial, hs, ht),
        "totals": totals,
        "worst": worst[:10],
    }


def render_report(rep: dict) -> None:
    t = rep["totals"]
    print(f"=== density-split — {rep['asset']} {rep['date']} UTC ===")
    print(
        f"{'asset':<5} {'windows':>7} {'missing_total':>13} {'post_roll':>9} "
        f"{'book_empty_skipped':>19} {'outage':>6} {'other':>6}"
    )
    print("-" * 66)

    def fmt(v):
        return "n/a(partial)" if v is None else str(v)

    print(
        f"{rep['asset']:<5} {rep['windows']:>7} {fmt(t['missing']):>13} "
        f"{fmt(t['post_roll']):>9} {fmt(t['bes']):>19} "
        f"{fmt(t['outage']):>6} {fmt(t['other']):>6}"
    )
    if not rep["partial"]:
        print(
            f"info: expected_ticks={t['expected']} absent_row_total="
            f"{t['absent_row_total']} (outage is a subset of absent; "
            "printed buckets are disjoint)"
        )
    ev_note = ""
    if rep["ev_partial"]:
        ev_note = f", PARTIAL {rep['ev_read']}/{rep['ev_total']} — attributions may be undercounted"
    else:
        ev_note = f" ({rep['ev_read']}/{rep['ev_total']} files)"
    print(
        f"evidence: coverage_gap_ranges={rep['ranges']} "
        f"book_empty_skipped_conditions={rep['bes']} "
        f"from day collector_events{ev_note}"
    )
    hp, hs, ht = rep["hive"]
    if hp:
        print(f"evidence: hive scan PARTIAL {hs}/{ht} — buckets not asserted")
    print("worst-10 windows by missing ticks:")
    for w in rep["worst"]:
        print(f"  start={w['start']} missing={w['missing']} cond={w['cond'][:16]}…")
    if rep["partial"]:
        print("verdict: partial — counters degraded honestly (budget), never guessed")
    else:
        print("verdict: ok")


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)
    rep = run_density_split(Path(args.data_root), args.date, args.asset, args.budget_s)
    render_report(rep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
