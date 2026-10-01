"""PMData parity diff — pure parquet compare, no API calls.

Compares our :mod:`polymarket_collector.export_pmdata` output directory
against a PMData sample directory for the same slug/day:

- row counts (±2% tolerance flag),
- mid-price median diff in ticks,
- event-type coverage (``book`` / ``price_change`` / ``last_trade_price`` /
  ``tick_size_change`` present-or-missing per side).

Real-data-only: reads local parquet files only. NULL means gap — rows with
a missing side contribute no mid (never 0-filled, never interpolated).

Mid-price convention: ``(best_ask + best_bid) / 2`` from the first element
of the ``ask_prices`` / ``bid_prices`` depth lists when both are present,
else NULL. Sample files using scalar ``ask`` / ``bid`` (or ``best_*``)
columns are accepted via the same fallback chain.

Alignment: per-row diffs pair each sample row (with a mid and a timestamp)
to the nearest ours row (with a mid) within ``join_tolerance_ms``
(default 1000ms). The headline number is the median absolute pair diff in
ticks (``tick_size`` default 0.01). When no pairs align, the report falls
back to the diff-of-medians and flags the alignment gap.
"""

from __future__ import annotations

import argparse
import bisect
import datetime as _dt
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow.parquet as pq

REQUIRED_EVENT_TYPES = ("book", "price_change", "last_trade_price", "tick_size_change")

DEFAULT_COUNT_TOLERANCE = 0.02
DEFAULT_TICK_SIZE = 0.01
DEFAULT_JOIN_TOLERANCE_MS = 1000


def find_parquet_files(root: str | Path, slug: Optional[str] = None) -> List[Path]:
    """All ``*.parquet`` files under ``root`` preferring ``slug`` matches.

    When ``slug`` is given and at least one file name contains it
    (case-insensitive), only those files are returned. Otherwise every
    parquet file under ``root`` is returned (fallback for flat sample
    layouts) — the caller records which mode was used.
    """
    root_p = Path(root)
    all_files = sorted(
        p for p in root_p.rglob("*.parquet")
        if not p.name.endswith(".tmp") and p.is_file()
    )
    if slug:
        matched = [p for p in all_files if slug.lower() in p.name.lower()]
        if matched:
            return matched
    return all_files


def read_parquet_rows(paths: List[Path]) -> Tuple[List[dict], Dict[str, int]]:
    """Read parquet files to row dicts. Unreadable files are counted, never raised."""
    rows: List[dict] = []
    stats = {"files_ok": 0, "files_failed": 0, "rows_read": 0}
    for p in paths:
        try:
            table = pq.read_table(str(p))
            stats["files_ok"] += 1
            stats["rows_read"] += table.num_rows
            if table.num_rows:
                rows.extend(table.to_pylist())
            del table
        except Exception as e:
            stats["files_failed"] += 1
            print(f"[pmdata_diff] WARN failed to read {p}: {e}")
    return rows, stats


def _head(v: Any) -> Any:
    if isinstance(v, (list, tuple)):
        return v[0] if v else None
    return v


def row_mid(row: dict) -> Optional[float]:
    """Best-touch mid for a row, else None (gap — never 0-filled)."""
    ask = _head(row.get("ask_prices", row.get("asks", row.get("ask", row.get("best_ask")))))
    bid = _head(row.get("bid_prices", row.get("bids", row.get("bid", row.get("best_bid")))))
    if ask is None and "price" in row and row.get("event_type") in (None, "trade", "last_trade_price"):
        # Last-trade-price rows carry a single price, not a two-sided quote:
        # no mid is derivable from one side alone.
        return None
    try:
        a = float(ask) if ask is not None else None
        b = float(bid) if bid is not None else None
    except (TypeError, ValueError):
        return None
    if a is None or b is None:
        return None
    if a != a or b != b:
        return None
    if not (0.0 <= a <= 1.0 and 0.0 <= b <= 1.0):
        return None
    return (a + b) / 2.0


def row_ts_ms(row: dict) -> Optional[int]:
    """Event clock in epoch-ms: ``timestamp`` else ``local_timestamp`` (ns)."""
    ts = row.get("timestamp", row.get("ts_source", row.get("ts")))
    if ts is not None and not isinstance(ts, bool):
        try:
            f = float(ts)
            if f == f:
                return int(f) if f > 1e11 else int(f * 1000)
        except (TypeError, ValueError, OverflowError):
            pass
    loc = row.get("local_timestamp", row.get("ts_received_ns"))
    if loc is not None and not isinstance(loc, bool):
        try:
            return int(loc) // 1_000_000
        except (TypeError, ValueError):
            pass
    return None


def event_type_of(row: dict) -> str:
    et = row.get("event_type", row.get("type"))
    if isinstance(et, str) and et.strip():
        return et.strip().lower()
    return "unknown"


def _median(vals: List[float]) -> Optional[float]:
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return float(statistics.median(vals))


def align_pair_diffs(
    ours: List[Tuple[int, float]],
    sample: List[Tuple[int, float]],
    join_tolerance_ms: int,
) -> List[float]:
    """Absolute mid diffs pairing each sample point to the nearest ours point.

    Both inputs are ``(ts_ms, mid)`` sorted by ts. A pair is kept only when
    the nearest ours timestamp lands within ``join_tolerance_ms``.
    """
    if not ours or not sample:
        return []
    ours_sorted = sorted(ours, key=lambda p: p[0])
    ours_ts = [p[0] for p in ours_sorted]
    diffs: List[float] = []
    for sts, smid in sorted(sample, key=lambda p: p[0]):
        i = bisect.bisect_left(ours_ts, sts)
        best: Optional[float] = None
        for j in (i - 1, i):
            if 0 <= j < len(ours_sorted):
                gap = abs(ours_sorted[j][0] - sts)
                if gap <= join_tolerance_ms:
                    d = abs(smid - ours_sorted[j][1])
                    if best is None or d < best:
                        best = d
        if best is not None:
            diffs.append(best)
    return diffs


def compare_pmdata(
    ours_dir: str | Path,
    sample_dir: str | Path,
    slug: Optional[str] = None,
    *,
    tick_size: float = DEFAULT_TICK_SIZE,
    count_tolerance: float = DEFAULT_COUNT_TOLERANCE,
    join_tolerance_ms: int = DEFAULT_JOIN_TOLERANCE_MS,
) -> Dict[str, Any]:
    """Compare our export dir against a PMData sample dir. Pure parquet compare."""
    if tick_size is None or float(tick_size) <= 0:
        raise ValueError(f"tick_size must be > 0, got {tick_size!r}")
    tick_size = float(tick_size)

    ours_paths = find_parquet_files(ours_dir, slug)
    sample_paths = find_parquet_files(sample_dir, slug)
    ours_rows, ours_stats = read_parquet_rows(ours_paths)
    sample_rows, sample_stats = read_parquet_rows(sample_paths)

    ours_types = sorted({event_type_of(r) for r in ours_rows})
    sample_types = sorted({event_type_of(r) for r in sample_rows})
    coverage = []
    for et in REQUIRED_EVENT_TYPES:
        coverage.append({
            "event_type": et,
            "ours": et in ours_types,
            "sample": et in sample_types,
        })

    ours_mids = [(ts, m) for r in ours_rows
                 for ts, m in [((row_ts_ms(r)), row_mid(r))] if m is not None and ts is not None]
    sample_mids = [(ts, m) for r in sample_rows
                   for ts, m in [((row_ts_ms(r)), row_mid(r))] if m is not None and ts is not None]
    ours_mid_vals = [m for _, m in ours_mids]
    sample_mid_vals = [m for _, m in sample_mids]
    ours_median = _median(ours_mid_vals)
    sample_median = _median(sample_mid_vals)

    pair_diffs = align_pair_diffs(ours_mids, sample_mids, join_tolerance_ms)
    median_pair_diff = _median(pair_diffs)
    if median_pair_diff is not None:
        median_diff = median_pair_diff
        diff_basis = f"aligned-pairs (n={len(pair_diffs)}, tol={join_tolerance_ms}ms)"
        aligned_pairs = len(pair_diffs)
    elif ours_median is not None and sample_median is not None:
        median_diff = abs(ours_median - sample_median)
        diff_basis = "diff-of-medians (no aligned pairs — clocks do not overlap)"
        aligned_pairs = 0
    else:
        median_diff = None
        diff_basis = "no mids on one side — no diff computable (gap)"
        aligned_pairs = 0
    median_diff_ticks = (median_diff / tick_size) if median_diff is not None else None

    n_ours, n_sample = len(ours_rows), len(sample_rows)
    count_diff_pct = ((n_ours - n_sample) / n_sample) if n_sample else None
    count_pass = count_diff_pct is not None and abs(count_diff_pct) <= count_tolerance
    mid_pass = median_diff_ticks is not None and median_diff_ticks <= 1.0
    missing_required = [c["event_type"] for c in coverage if not (c["ours"] or c["sample"])]
    coverage_pass = not missing_required

    reasons = []
    if not count_pass:
        reasons.append("row-count outside ±{:.0%}".format(count_tolerance))
    if not mid_pass:
        reasons.append("mid-price median diff > 1 tick")
    if not coverage_pass:
        reasons.append(f"event types absent on both sides: {', '.join(missing_required)}")
    status = "PASS" if not reasons else "FAIL"

    return {
        "slug": slug,
        "ours_dir": str(ours_dir),
        "sample_dir": str(sample_dir),
        "tick_size": tick_size,
        "count_tolerance": count_tolerance,
        "join_tolerance_ms": join_tolerance_ms,
        "ours": {"files": [str(p) for p in ours_paths], "stats": ours_stats,
                 "rows": n_ours, "event_types": ours_types,
                 "mids": len(ours_mid_vals), "mid_median": ours_median},
        "sample": {"files": [str(p) for p in sample_paths], "stats": sample_stats,
                   "rows": n_sample, "event_types": sample_types,
                   "mids": len(sample_mid_vals), "mid_median": sample_median},
        "row_count": {"ours": n_ours, "sample": n_sample,
                      "diff_pct": count_diff_pct, "pass": count_pass},
        "mid_price": {"median_diff": median_diff,
                      "median_diff_ticks": median_diff_ticks,
                      "basis": diff_basis, "aligned_pairs": aligned_pairs,
                      "pass": mid_pass},
        "coverage": coverage,
        "missing_required_both_sides": missing_required,
        "coverage_pass": coverage_pass,
        "status": status,
        "reasons": reasons,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    """Render a comparison report as markdown."""
    lines = [
        "# PMData parity diff",
        "",
        f"- slug: `{report.get('slug') or '(all files)'}`",
        f"- ours: `{report['ours_dir']}`",
        f"- sample: `{report['sample_dir']}`",
        f"- tick_size: `{report['tick_size']}`",
        f"- verdict: **{report['status']}**",
    ]
    if report["reasons"]:
        lines.append(f"- reasons: {'; '.join(report['reasons'])}")
    lines += [
        "",
        "## Row counts",
        "",
        "| side | rows |",
        "| --- | --- |",
        f"| ours | {report['row_count']['ours']} |",
        f"| sample | {report['row_count']['sample']} |",
        "",
    ]
    dp = report["row_count"]["diff_pct"]
    dp_s = f"{dp:+.2%}" if dp is not None else "n/a (empty sample)"
    lines += [
        f"- diff: `{dp_s}` vs tolerance `±{report['count_tolerance']:.0%}` → "
        f"**{'PASS' if report['row_count']['pass'] else 'FAIL'}**",
        "",
        "## Mid price",
        "",
        f"- ours median: `{report['ours']['mid_median']}` (n={report['ours']['mids']})",
        f"- sample median: `{report['sample']['mid_median']}` (n={report['sample']['mids']})",
        f"- basis: {report['mid_price']['basis']}",
    ]
    md = report["mid_price"]["median_diff"]
    mdt = report["mid_price"]["median_diff_ticks"]
    lines += [
        f"- median diff: `{md}` = `{mdt}` ticks → "
        f"**{'PASS' if report['mid_price']['pass'] else 'FAIL'}** (≤1 tick)",
        "",
        "## Event-type coverage",
        "",
        "| event_type | ours | sample |",
        "| --- | --- | --- |",
    ]
    for c in report["coverage"]:
        o = "present" if c["ours"] else "missing"
        s = "present" if c["sample"] else "missing"
        lines.append(f"| {c['event_type']} | {o} | {s} |")
    lines += [
        "",
        f"ours-only extra types: "
        f"`{sorted(set(report['ours']['event_types']) - set(REQUIRED_EVENT_TYPES))}`",
        f"sample-only extra types: "
        f"`{sorted(set(report['sample']['event_types']) - set(REQUIRED_EVENT_TYPES))}`",
        "",
        f"coverage gate (every required type on ≥1 side): "
        f"**{'PASS' if report['coverage_pass'] else 'FAIL'}**",
    ]
    if report["missing_required_both_sides"]:
        lines.append(f"missing on both sides: `{report['missing_required_both_sides']}`")
    lines += [
        "",
        "## Files",
        "",
        f"- ours files ok/failed: "
        f"{report['ours']['stats']['files_ok']}/{report['ours']['stats']['files_failed']}",
        f"- sample files ok/failed: "
        f"{report['sample']['stats']['files_ok']}/{report['sample']['stats']['files_failed']}",
        "",
        "_Pure local parquet compare — no API calls. NULL = gap, never interpolated._",
        "",
    ]
    return "\n".join(lines)


def write_report(report: Dict[str, Any], reports_dir: str | Path = "reports") -> Path:
    """Write the markdown report to ``reports/pmdata_diff_*.md`` (tmp+rename)."""
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    slug = "".join(c if (c.isalnum() or c in "-_") else "_" for c in (report.get("slug") or "all"))
    stamp = _dt.datetime.now(tz=_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"pmdata_diff_{slug}_{stamp}.md"
    tmp = path.parent / (path.name + ".tmp")
    tmp.write_text(render_markdown(report), encoding="utf-8")
    import os as _os

    _os.replace(str(tmp), str(path))
    return path


def main(argv: Optional[List[str]] = None) -> int:
    """CLI: compare our export dir with a PMData sample dir for one slug/day."""
    ap = argparse.ArgumentParser(description="PMData parity diff (pure local parquet compare)")
    ap.add_argument("--ours", required=True, help="our export_pmdata out_dir")
    ap.add_argument("--sample", required=True, help="PMData sample dir (same slug/day)")
    ap.add_argument("--slug", default=None, help="market slug file filter")
    ap.add_argument("--tick-size", type=float, default=DEFAULT_TICK_SIZE)
    ap.add_argument("--count-tolerance", type=float, default=DEFAULT_COUNT_TOLERANCE)
    ap.add_argument("--join-tolerance-ms", type=int, default=DEFAULT_JOIN_TOLERANCE_MS)
    ap.add_argument("--reports-dir", default="reports")
    ap.add_argument("--no-write", action="store_true", help="print only, do not write a report file")
    args = ap.parse_args(argv)
    report = compare_pmdata(
        args.ours, args.sample, args.slug,
        tick_size=args.tick_size,
        count_tolerance=args.count_tolerance,
        join_tolerance_ms=args.join_tolerance_ms,
    )
    text = render_markdown(report)
    print(text)
    if not args.no_write:
        path = write_report(report, args.reports_dir)
        print(f"[pmdata_diff] wrote {path}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
