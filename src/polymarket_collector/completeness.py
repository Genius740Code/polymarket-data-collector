"""Data completeness tracking — §15.

Per asset per day: expected vs actual snapshots, missing intervals, disconnect
duration, resync counts, sequence gaps, write failures, rollover misses, etc.
Research code should default to book_snapshots_clean (§9B).
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import pyarrow.parquet as pq

from .storage.parquet_io import read_files, read_table


@dataclass
class DailyCompleteness:
    date: str  # YYYY-MM-DD UTC
    asset: str
    expected_snapshots: int  # 172800 per day (2/sec * 86400)
    actual_snapshots: int = 0
    actual_clean_snapshots: int = 0
    missing_intervals: int = 0
    disconnect_duration_ms: int = 0
    resync_episode_count: int = 0
    total_gap_ms: int = 0
    sequence_gaps: int = 0
    duplicate_events: int = 0
    write_failures: int = 0
    rollover_misses: int = 0
    coverage_gaps: int = 0
    process_downtime_ms: int = 0
    resolution_stuck: int = 0
    sanity_violations: int = 0

    def completeness_ratio(self) -> float:
        if self.expected_snapshots == 0:
            return 0.0
        return self.actual_clean_snapshots / self.expected_snapshots

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "asset": self.asset,
            "expected_snapshots": self.expected_snapshots,
            "actual_snapshots": self.actual_snapshots,
            "actual_clean_snapshots": self.actual_clean_snapshots,
            "missing_intervals": self.missing_intervals,
            "disconnect_duration_ms": self.disconnect_duration_ms,
            "resync_episode_count": self.resync_episode_count,
            "total_gap_ms": self.total_gap_ms,
            "sequence_gaps": self.sequence_gaps,
            "duplicate_events": self.duplicate_events,
            "write_failures": self.write_failures,
            "rollover_misses": self.rollover_misses,
            "coverage_gaps": self.coverage_gaps,
            "process_downtime_ms": self.process_downtime_ms,
            "resolution_stuck": self.resolution_stuck,
            "sanity_violations": self.sanity_violations,
            "completeness_ratio": self.completeness_ratio(),
        }


def compute_daily_completeness(data_dir: str | Path, date_str: str) -> List[DailyCompleteness]:
    """Compute completeness for a given UTC date (§15)."""
    base = Path(data_dir)
    expected_per_day = 2 * 86400  # 500ms → 2 per second
    results: List[DailyCompleteness] = []

    # discover assets from book_snapshots_500ms partition
    src_root = base / "book_snapshots_500ms" / f"date={date_str}"
    assets: List[str] = []
    if src_root.exists():
        for d in src_root.glob("asset=*"):
            assets.append(d.name.split("=", 1)[1])
    # fallback to config assets
    if not assets:
        assets = ["BTC", "ETH", "SOL"]

    for asset in assets:
        dc = DailyCompleteness(date=date_str, asset=asset, expected_snapshots=expected_per_day)
        # count snapshots
        # PERF: footer-only row counts — previously decompressed + decoded every
        # 120-col snapshot row just to learn COUNT(*). pq.read_metadata num_rows
        # is exact; nulls/dedup untouched (no rows read).
        snap_dir = base / "book_snapshots_500ms" / f"date={date_str}" / f"asset={asset}"
        clean_dir = base / "book_snapshots_clean" / f"date={date_str}" / f"asset={asset}"
        for p in [snap_dir, clean_dir]:
            if p.exists():
                try:
                    parts = [x for x in p.glob("*.parquet") if not x.name.endswith(".tmp")]
                except Exception:
                    parts = []
                cnt = 0
                _unreadable = 0
                for _fp in parts:
                    try:
                        cnt += pq.read_metadata(str(_fp)).num_rows
                    except Exception:
                        _unreadable += 1
                if parts and cnt == 0 and _unreadable == len(parts):
                    print(f"[completeness] ERROR all {len(parts)} files unreadable for {p} — counts would be silently wrong")
                if "clean" in str(p):
                    dc.actual_clean_snapshots = cnt
                else:
                    dc.actual_snapshots = cnt
        dc.missing_intervals = max(0, dc.expected_snapshots - dc.actual_clean_snapshots)

        # aggregate collector_events for this date/asset
        # PERF: hoisted — single CE/RE read per date (was N_assets × full
        # re-read + full to_pylist). Per-asset filtering below preserves the
        # exact predicate incl. NULL-asset rows.
        if asset == assets[0]:
            ce_root = base / "collector_events" / f"date={date_str}"
            _ce_rows: list = []
            if ce_root.exists():
                try:
                    ce_parts = [x for x in ce_root.glob("*.parquet") if not x.name.endswith(".tmp")]
                except Exception:
                    ce_parts = []
                if ce_parts:
                    try:
                        ce_tbl = read_files(ce_parts, label="completeness collector_events")
                    except Exception:
                        ce_tbl = None
                    if ce_tbl is not None:
                        try:
                            # project to needed cols only when present
                            _names = set(ce_tbl.schema.names)
                            _proj = [c for c in ("asset", "event_type") if c in _names]
                            _ce_src = ce_tbl.select(_proj) if _proj else ce_tbl
                            _ce_rows = _ce_src.to_pylist()
                        except Exception:
                            try:
                                _ce_rows = ce_tbl.to_pylist()
                            except Exception:
                                _ce_rows = []
            re_root = base / "resync_episodes" / f"date={date_str}"
            _re_rows: list = []
            if re_root.exists():
                try:
                    re_parts = [x for x in re_root.glob("*.parquet") if not x.name.endswith(".tmp")]
                except Exception:
                    re_parts = []
                if re_parts:
                    try:
                        re_tbl = read_files(re_parts, label="completeness resync_episodes")
                    except Exception:
                        re_tbl = None
                    if re_tbl is not None:
                        try:
                            _names = set(re_tbl.schema.names)
                            _proj = [c for c in ("asset", "gap_duration_ms") if c in _names]
                            _re_src = re_tbl.select(_proj) if _proj else re_tbl
                            _re_rows = _re_src.to_pylist()
                        except Exception:
                            try:
                                _re_rows = re_tbl.to_pylist()
                            except Exception:
                                _re_rows = []
            # cache on function attribute for the remaining assets in this call
            compute_daily_completeness._ce_rows = _ce_rows  # type: ignore[attr-defined]
            compute_daily_completeness._re_rows = _re_rows  # type: ignore[attr-defined]
        else:
            _ce_rows = getattr(compute_daily_completeness, "_ce_rows", [])  # type: ignore[attr-defined]
            _re_rows = getattr(compute_daily_completeness, "_re_rows", [])  # type: ignore[attr-defined]
        for row in _ce_rows:
            if row.get("asset") and row["asset"].upper() != asset.upper():
                continue
            et = row.get("event_type")
            if et == "sequence_gap":
                dc.sequence_gaps += 1
            elif et == "duplicate_event":
                dc.duplicate_events += 1
            elif et == "write_failed":
                dc.write_failures += 1
            elif et == "rollover_miss":
                dc.rollover_misses += 1
            elif et == "coverage_gap":
                dc.coverage_gaps += 1
            elif et == "resolution_stuck":
                dc.resolution_stuck += 1
            elif et == "book_anomaly":
                dc.sanity_violations += 1

        # resync episodes (rows hoisted above — single read per date)
        for row in _re_rows:
                    if row.get("asset", "").upper() == asset.upper():
                        dc.resync_episode_count += 1
                        if row.get("gap_duration_ms"):
                            dc.total_gap_ms += int(row["gap_duration_ms"])
                            dc.disconnect_duration_ms += int(row["gap_duration_ms"])

        results.append(dc)
    return results
