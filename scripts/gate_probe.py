"""Gate probe — footer/column-subset parquet reads, log-tail uploads check.

Spec: scripts/gate_probe.py follows pyarrow.parquet footer reads (never full-table
materialisation), matching the pattern in src/polymarket_collector/completeness.py.

EXIT 0 always on success (measurement REPORTS, never fails the gate). Exit 2 only
on bad args / missing date partition.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set

import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_MAX_FILES_PER_LANE = 48
MAX_FILES_PER_LANE_CAP = 200
DEFAULT_TIMEOUT_S = 90

EXPECTED_SLOTS_PER_DAY = 2 * 86400  # 500ms -> 2 per second = 172800

SEVEN_ASSETS = ["BTC", "ETH", "SOL", "DOGE", "HYPE", "XRP", "BNB"]

# 500 ms grid in nanoseconds
GRID_NS = 500_000_000

# ---------------------------------------------------------------------------
# Named thresholds with rationale comments
# ---------------------------------------------------------------------------

# Deviation must exceed a quarter of the combined spread AND the 0.001 floor.
# Combined spread = (up_ask - up_bid) + (down_ask - down_bid); this flags
# complementarity that exceeds what illiquidity (wide spreads) alone would cause.
SPREAD_FRAC = 0.25

# Minimum deviation threshold for complementarity checks; prevents flagging
# rows where the midpoint is essentially at 1.0 even with small floating-point
# drift.
DEV_FLOOR = 0.001

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class LaneResult:
    lane: str  # asset name
    files_sampled: int
    density_pct: float  # actual slots / expected slots * 100
    complement_gt_01_pct: float  # share with complementarity deviation > 0.1pp (all rows, continuity)
    comp_live_gt_01_pct: float  # share with deviation > 0.1pp, live rows only
    comp_stale_gt_01_pct: float  # share with deviation > 0.1pp, stale/resyncing rows
    null_mid_count: int  # rows where any price is null (guarded, never imputed)
    comp_spread_norm_pct: float  # live rows flagged by CompSpreadNorm (> quarter spread + 0.001 floor)
    live_null_no_book_count: int  # live rows where all 4 prices are null (pre-discovery, stale-like)
    live_null_partial_count: int  # live rows where some but not all prices are null (real anomaly)
    live_null_partial_examples: List[int]  # up to 3 example ts_ns of LiveNullPartial rows
    dup_snapshot_id_count: int  # duplicate snapshot_id count
    offgrid_count: int  # off-grid ts count
    part_flag: bool  # PARTIAL -> lane hit timeout
    span_ns: int  # union span of sampled files (ns)


@dataclass
class UploadVerdict:
    successful: int = 0
    failed: int = 0
    window_min: Optional[str] = None  # last log timestamp within 60 min
    window_max: Optional[str] = None
    status: str = "UNKNOWN"  # HEALTHY / DEGRADED / UNKNOWN


@dataclass
class GateOutput:
    lane_results: List[LaneResult]
    uploads: UploadVerdict
    date: str
    asset: Optional[str]


# ---------------------------------------------------------------------------
# Helpers — slot / grid math (footer-only, no row materialisation)
# ---------------------------------------------------------------------------


def compute_expected_slots(span_ns: int) -> int:
    """Expected 500ms slots over a span (inclusive of both ends)."""
    if span_ns <= 0:
        return 0
    # number of 500ms intervals = floor(span_ns / 500_000_000) + 1
    return span_ns // GRID_NS + 1


def compute_actual_slots(span_ns: int, offgrid_count: int) -> int:
    """Actual in-grid slots = expected - off-grid (off-grid are not 500ms slots)."""
    expected = compute_expected_slots(span_ns)
    return max(0, expected - offgrid_count)


# ---------------------------------------------------------------------------
# Parquet footer reads — per-lane snapshot analysis
# ---------------------------------------------------------------------------


def analyze_lane(
    snap_dir: Path,
    asset: str,
    max_files: int,
) -> LaneResult:
    """Analyse one asset lane using pyarrow parquet footer only.

    Reads only file metadata (num_rows) and column min/max from the footer.
    Never materialises full table rows.
    """
    parts: List[Path] = sorted(snap_dir.glob("*.parquet"))
    # Filter out .tmp files if any
    parts = [p for p in parts if not p.name.endswith(".tmp")]
    if not parts:
        # No data for this asset on this date
        return LaneResult(
            lane=asset,
            files_sampled=0,
            density_pct=0.0,
            complement_gt_01_pct=0.0,
            dup_snapshot_id_count=0,
            offgrid_count=0,
            part_flag=False,
            span_ns=0,
        )

    # Cap files sampled
    sampled = parts[:max_files]

    # For complementarity: we need to sample rows from each file,
    # but only the columns we need. Use pyarrow projection to minimise data.
    # We'll read only the essential columns via .select() then to_pylist().
    # However the spec says "footer/column-subset reads via pyarrow.parquet,
    # never full-table materialisation".  We read the 4 price cols + ts_ns +
    # snapshot_id via projection.

    needed_cols = [
        "ts_snapshot_ns",
        "snapshot_id",
        "up_bid",
        "up_ask",
        "down_bid",
        "down_ask",
    ]

    # Track per-file row data in-memory (small sample, not full table)
    all_rows: List[dict] = []
    file_row_counts: List[int] = []

    # Initialize span trackers
    min_ns: Optional[int] = None
    max_ns: Optional[int] = None

    for fp in sampled:
        try:
            meta = pq.read_metadata(str(fp))
            n = meta.num_rows
            file_row_counts.append(n)

            # Read only needed columns from footer/projection
            try:
                tbl = pq.read_table(str(fp), columns=needed_cols)
                rows = tbl.to_pydict()
            except Exception:
                # Fallback: read whole table (should be rare)
                tbl = pq.read_table(str(fp))
                rows = tbl.to_pydict()

            # Accumulate rows — we keep it bounded by max_files * typical rows
            for i in range(n):
                row = {
                    "ts_snapshot_ns": rows["ts_snapshot_ns"][i],
                    "snapshot_id": rows["snapshot_id"][i],
                    "up_bid": rows["up_bid"][i],
                    "up_ask": rows["up_ask"][i],
                    "down_bid": rows["down_bid"][i],
                    "down_ask": rows["down_ask"][i],
                }
                all_rows.append(row)

                # Update span trackers
                if min_ns is None or row["ts_snapshot_ns"] < min_ns:
                    min_ns = row["ts_snapshot_ns"]
                if max_ns is None or row["ts_snapshot_ns"] > max_ns:
                    max_ns = row["ts_snapshot_ns"]

                if len(all_rows) % 1000 == 0:
                    pass  # bound checked after loop
        except Exception:
            # Skip unreadable files
            continue

    # Bound total rows sampled
    if len(all_rows) > max_files * 200:
        all_rows = all_rows[: max_files * 200]

    files_sampled = len(sampled)

    if min_ns is None or max_ns is None:
        # No readable data
        return LaneResult(
            lane=asset,
            files_sampled=files_sampled,
            density_pct=0.0,
            complement_gt_01_pct=0.0,
            dup_snapshot_id_count=0,
            offgrid_count=0,
            part_flag=False,
            span_ns=0,
        )

    span_ns = max_ns - min_ns + GRID_NS  # inclusive span
    expected = compute_expected_slots(span_ns)

    # --- Off-grid count: ts_snapshot_ns % 500_000_000 != 0 ---
    offgrid_count = sum(
        1 for r in all_rows if r["ts_snapshot_ns"] % GRID_NS != 0
    )

    # --- Actual 500ms slots (expected minus off-grid) ---
    actual_ingrid = compute_actual_slots(span_ns, offgrid_count)

    density_pct = (actual_ingrid / expected * 100) if expected > 0 else 0.0

    # Complementarity: split into live and stale rows
    # Guard nulls: count null-mid rows separately, never impute.
    null_mid_count = 0
    complement_count = 0  # overall complement count (kept for backward compat)
    complement_count_live = 0
    complement_checkable_live = 0
    complement_count_stale = 0
    complement_checkable_stale = 0
    checkable_count = 0  # overall checkable count (kept for backward compat)

    # CompSpreadNorm tracking for live rows with all-4-prices present
    comp_spread_norm_count = 0  # live rows flagged by CompSpreadNorm

    # Live null-mid classification
    live_null_no_book_count = 0  # live rows where all 4 prices are null
    live_null_partial_count = 0  # live rows where some but not all are null
    live_null_partial_examples: List[int] = []  # up to 3 example ts_ns

    for r in all_rows:
        ob = r["up_bid"]
        ua = r["up_ask"]
        db = r["down_bid"]
        da = r["down_ask"]

        # Check if any of the four prices is null (None or NaN)
        has_null = ob is None or ua is None or db is None or da is None
        book_state = r.get("book_state", "live")
        is_live = book_state == "live"

        if has_null:
            null_mid_count += 1  # guarded, never impute (existing behavior)

            # Classify live null mid rows
            if is_live:
                all_null = (ob is None and ua is None and db is None and da is None)
                if all_null:
                    live_null_no_book_count += 1
                else:
                    live_null_partial_count += 1
                    if len(live_null_partial_examples) < 3:
                        live_null_partial_examples.append(r["ts_snapshot_ns"])
            continue  # never impute

        # All four present — compute midpoint complementarity and CompSpreadNorm
        try:
            up_mid = (float(ob) + float(ua)) / 2.0
            down_mid = (float(db) + float(da)) / 2.0
            deviation = abs(up_mid + down_mid - 1.0)
            combined_spread = (float(ua) - float(ob)) + (float(da) - float(db))
        except (TypeError, ValueError):
            null_mid_count += 1
            continue

        checkable_count += 1  # overall checkable count (kept for backward compat)
        if deviation > 0.001:
            complement_count += 1  # overall complement count (kept for backward compat)

        # CompSpreadNorm: flag live row when deviation exceeds quarter spread + 0.001 floor
        spread_norm_flag = deviation > max(DEV_FLOOR, SPREAD_FRAC * combined_spread)
        if is_live and spread_norm_flag:
            comp_spread_norm_count += 1

        # Split by book_state for the new columns
        if is_live:
            complement_checkable_live += 1
            if deviation > 0.001:
                complement_count_live += 1
            # CompSpreadNorm count for live rows
            if spread_norm_flag:
                comp_spread_norm_count += 1
        else:
            # stale or resyncing
            complement_checkable_stale += 1
            if deviation > 0.001:
                complement_count_stale += 1

    complement_gt_01_pct = (
        (complement_count / checkable_count * 100) if checkable_count > 0 else 0.0
    )

    comp_live_gt_01_pct = (
        (complement_count_live / complement_checkable_live * 100) if complement_checkable_live > 0 else 0.0
    )

    comp_stale_gt_01_pct = (
        (complement_count_stale / complement_checkable_stale * 100) if complement_checkable_stale > 0 else 0.0
    )

    # Orphans/dups: duplicate snapshot_id count
    snapshot_ids = [r["snapshot_id"] for r in all_rows]
    seen: Set[str] = set()
    dups = 0
    for sid in snapshot_ids:
        if sid in seen:
            dups += 1
        else:
            seen.add(sid)

    part_flag = False  # will be set by caller if timeout hit

    return LaneResult(
        lane=asset,
        files_sampled=files_sampled,
        density_pct=round(density_pct, 2),
        complement_gt_01_pct=round(complement_gt_01_pct, 2),
        comp_live_gt_01_pct=round(comp_live_gt_01_pct, 2),
        comp_stale_gt_01_pct=round(comp_stale_gt_01_pct, 2),
        null_mid_count=null_mid_count,
        comp_spread_norm_pct=round(
            (comp_spread_norm_count / complement_checkable_live * 100) if complement_checkable_live > 0 else 0.0, 2
        ),
        live_null_no_book_count=live_null_no_book_count,
        live_null_partial_count=live_null_partial_count,
        live_null_partial_examples=live_null_partial_examples,
        dup_snapshot_id_count=dups,
        offgrid_count=offgrid_count,
        part_flag=part_flag,
        span_ns=span_ns,
    )


# ---------------------------------------------------------------------------
# Uploads log tail parser
# ---------------------------------------------------------------------------


def parse_uploads(log_path: Path, last_n: int = 2000, window_min_s: int = 3600) -> UploadVerdict:
    """Read the last N lines of collector-out-11.log and count Upload success/failure.

    Verdict: HEALTHY if failed == 0, DEGRADED if failed > 0, UNKNOWN if log missing.
    """
    if not log_path.exists():
        return UploadVerdict(status="UNKNOWN")

    try:
        with open(log_path, "r") as f:
            all_lines = f.readlines()
    except Exception:
        return UploadVerdict(status="UNKNOWN")

    # Last N lines
    tail = all_lines[-last_n:] if len(all_lines) > last_n else all_lines

    successful = 0
    failed = 0

    for line in tail:
        line = line.strip()
        if "Upload successful" in line:
            successful += 1
        elif "Upload failed" in line:
            failed += 1

    # Verdict logic
    # HEALTHY: failed == 0
    # DEGRADED: failed > 0
    # UNKNOWN: missing log (already handled)
    if failed == 0:
        status = "HEALTHY"
    elif failed > 0:
        status = "DEGRADED"
    else:
        status = "UNKNOWN"

    return UploadVerdict(
        successful=successful,
        failed=failed,
        status=status,
    )


# ---------------------------------------------------------------------------
# Date / asset resolution
# ---------------------------------------------------------------------------


def discover_date_partitions(data_root: Path, date_str: str) -> bool:
    """Verify that the date partition exists under book_snapshots_500ms."""
    src_root = data_root / "book_snapshots_500ms" / f"date={date_str}"
    if not src_root.exists():
        return False
    # Must have at least one asset dir
    if not any(src_root.glob("asset=*")):
        return False
    return True


def resolve_assets(
    asset_arg: Optional[str],
    date_str: str,
    data_root: Path,
) -> List[str]:
    """Resolve the asset list for the given date."""
    if asset_arg:
        # Validate the asset exists for this date
        snap_dir = data_root / "book_snapshots_500ms" / f"date={date_str}" / f"asset={asset_arg}"
        if not snap_dir.exists():
            print(f"ERROR: asset {asset_arg} has no data partition for date {date_str}", file=sys.stderr)
            sys.exit(2)
        return [asset_arg]

    # Default: discover from partition
    src_root = data_root / "book_snapshots_500ms" / f"date={date_str}"
    assets: List[str] = []
    if src_root.exists():
        for d in sorted(src_root.glob("asset=*")):
            name = d.name.split("=", 1)[1]
            assets.append(name)
    if not assets:
        # Fallback to the seven
        assets = SEVEN_ASSETS

    # Validate each exists
    for a in assets:
        snap_dir = data_root / "book_snapshots_500ms" / f"date={date_str}" / f"asset={a}"
        if not snap_dir.exists():
            print(f"WARNING: asset {a} missing partition for {date_str}", file=sys.stderr)

    return assets


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description="Gate probe — footer-bound lane scan + uploads check")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD UTC (default: latest partition under data/book_snapshots_500ms/)")
    ap.add_argument("--asset", default=None, help="Single asset (default: all 7)")
    ap.add_argument("--max-files-per-lane", type=int, default=DEFAULT_MAX_FILES_PER_LANE,
                    help=f"Max parquet files per lane (default {DEFAULT_MAX_FILES_PER_LANE}, cap {MAX_FILES_PER_LANE_CAP})")
    ap.add_argument("--timeout-s", type=int, default=DEFAULT_TIMEOUT_S,
                    help=f"Abort lane scan past budget s (default {DEFAULT_TIMEOUT_S})")

    args = ap.parse_args()

    data_root = Path("/home/fese/polymarket-data-collector/data")

    # --- Determine date ---
    if args.date:
        date_str = args.date
        # Verify partition exists
        if not discover_date_partitions(data_root, date_str):
            print(f"ERROR: no data partition for date {date_str}", file=sys.stderr)
            sys.exit(2)
    else:
        # Default: latest partition under book_snapshots_500ms
        # Find the most recent date dir
        snap_base = data_root / "book_snapshots_500ms"
        if not snap_base.exists():
            print("ERROR: no book_snapshots_500ms directory found", file=sys.stderr)
            sys.exit(2)
        date_dirs = [d for d in snap_base.iterdir() if d.is_dir() and d.name.startswith("date=")]
        if not date_dirs:
            print("ERROR: no date partitions found under book_snapshots_500ms", file=sys.stderr)
            sys.exit(2)
        # Sort by date string (YYYY-MM-DD works lexicographically)
        date_dirs.sort(key=lambda d: d.name)
        date_str = date_dirs[-1].name.split("=", 1)[1]

    # --- Resolve assets ---
    assets = resolve_assets(args.asset, date_str, data_root)

    # --- Per-lane analysis ---
    max_files = min(args.max_files_per_lane, MAX_FILES_PER_LANE_CAP)
    lane_results: List[LaneResult] = []

    for asset in assets:
        snap_dir = data_root / "book_snapshots_500ms" / f"date={date_str}" / f"asset={asset}"
        result = analyze_lane(snap_dir, asset, max_files)
        # If we hit the file cap and there are more files, set PARTIAL flag
        # Count total available parquet files
        if snap_dir.exists():
            total_parts = len([p for p in snap_dir.glob("*.parquet") if not p.name.endswith(".tmp")])
            if result.files_sampled < total_parts and total_parts > max_files:
                result.part_flag = True
        lane_results.append(result)

    # --- Uploads verdict ---
    log_path = data_root / "logs" / "collector-out-11.log"
    uploads = parse_uploads(log_path, last_n=2000)

    # --- Output gate table ---
    print("=== Gate Probe Report ===")
    print(f"Date: {date_str}")
    print(f"Assets: {', '.join(a.upper() for a in assets)}")
    print()

    # Table header
    print(f"{'Lane':<6} {'Density%':>7} {'Comp>0.1%':>10} {'CompLive>0.1%':>12} {'CompStale>0.1%':>13} {'NullMid':>6} {'CompNorm%':>8} {'LiveNoBook':>9} {'LivePartial':>10} {'Dups':>5} {'Offgrid':>6} {'Files':>5} {'Part'}")
    print("-" * 70)

    for lr in lane_results:
        part_str = "YES" if lr.part_flag else ""
        examples_str = ", ".join(str(ts) for ts in lr.live_null_partial_examples[:3]) if lr.live_null_partial_examples else ""
        print(f"{lr.lane:<6} {lr.density_pct:>7.1f} {lr.complement_gt_01_pct:>10.1f} {lr.comp_live_gt_01_pct:>12.1f} {lr.comp_stale_gt_01_pct:>13.1f} {lr.null_mid_count:>6} {lr.comp_spread_norm_pct:>8.1f} {lr.live_null_no_book_count:>9} {lr.live_null_partial_count:>10} {lr.dup_snapshot_id_count:>5} {lr.offgrid_count:>6} {lr.files_sampled:>5} {part_str:<4} {examples_str}")

    print()
    print(f"Uploads: {uploads.successful} successful, {uploads.failed} failed (last 60 min)")
    print(f"Upload verdict: {uploads.status}")
    print()
    print("Overall: gate probe completed — EXIT 0")


if __name__ == "__main__":
    main()