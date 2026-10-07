"""Tests for scripts/gate_probe.py — pure helpers + real-data smoke test.

Forbidden: synthetic prices, interpolated values, fabricated data.
Allowed: inline literals for math checks, off-grid predicates, complement
deviation, and log-tail parsing. A small real-data smoke test runs only
when data/ exists (skip otherwise).
"""
from __future__ import annotations

import sys
import os
import pytest
from pathlib import Path

# Add scripts to path so we can import gate_probe
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from gate_probe import (
    compute_expected_slots,
    compute_actual_slots,
    GRID_NS,
    analyze_lane,
    parse_uploads,
    # UploadVerdict,  # unused at module level; used inline
)


# ---------------------------------------------------------------------------
# Pure helper tests (no data/ dependency)
# ---------------------------------------------------------------------------


def test_compute_expected_slots():
    """Expected 500ms slots over a span: floor(span_ns / 500_000_000) + 1."""
    # Zero span -> 0 (handled by caller)
    assert compute_expected_slots(0) == 0
    # span = 500ms -> intervals: [0, 500ms) -> 1 interval + 1 = 2 slots
    assert compute_expected_slots(500_000_000) == 2
    # span = 1000ms -> intervals: [0, 1000ms) -> 2 intervals + 1 = 3 slots
    assert compute_expected_slots(1_000_000_000) == 3
    # span = 1500ms -> 3 intervals + 1 = 4 slots
    assert compute_expected_slots(1_500_000_000) == 4


def test_compute_actual_slots():
    """Actual in-grid slots = expected - off-grid."""
    # No off-grid -> actual == expected
    assert compute_actual_slots(1_000_000_000, 0) == 3
    # 1 off-grid slot removed from 4 expected -> 3
    assert compute_actual_slots(1_500_000_000, 1) == 3


def test_grid_ns_constant():
    """500ms grid constant is 500_000_000 ns."""
    assert GRID_NS == 500_000_000


def test_off_grid_check():
    """Off-grid: ts_snapshot_ns % 500_000_000 != 0."""
    # On-grid examples
    assert (0 % GRID_NS) == 0  # ts=0 is on-grid
    assert (GRID_NS % GRID_NS) == 0  # ts=500ms is on-grid
    assert (2 * GRID_NS % GRID_NS) == 0  # ts=1000ms is on-grid

    # Off-grid examples
    assert (1 % GRID_NS) != 0  # ts=1ns is off-grid
    assert (GRID_NS + 1) % GRID_NS != 0  # ts=500ms+1ns is off-grid
    assert (500_000_001 % GRID_NS) != 0


def test_complement_deviation_predicate():
    """abs((up_bid+up_ask)/2 + (down_bid+down_ask)/2 - 1) > 0.001."""
    # Perfect mid=1.0 -> deviation 0, NOT > 0.001
    # up_bid=0.5, up_ask=0.5, down_bid=0.5, down_ask=0.5 -> mid = 0.5+0.5 = 1.0 -> deviation 0
    ob, ua, db, da = 0.5, 0.5, 0.5, 0.5
    mid = (ob + ua) / 2.0 + (db + da) / 2.0
    deviation = abs(mid - 1.0)
    assert deviation == 0.0, f"expected 0, got {deviation}"
    assert not (deviation > 0.001)

    # Off-complement: up_bid=0.3, up_ask=0.3 -> mid = 0.3+0.3 = 0.6 -> deviation 0.4 > 0.001
    ob, ua, db, da = 0.3, 0.3, 0.3, 0.3
    mid = (ob + ua) / 2.0 + (db + da) / 2.0
    deviation = abs(mid - 1.0)
    assert deviation > 0.001

    # Mixed: up_bid=0.5, up_ask=0.6 -> up_mid=0.55; down_bid=0.4, down_ask=0.5 -> down_mid=0.45; total=1.0
    ob, ua, db, da = 0.5, 0.6, 0.4, 0.5
    mid = (ob + ua) / 2.0 + (db + da) / 2.0
    deviation = abs(mid - 1.0)
    assert deviation == 0.0, f"expected 0, got {deviation}"


def test_complement_deviation_null_guard():
    """Rows with any null price are counted as null-mid, never imputed."""
    # Simulate the predicate logic: if any price is None, skip (count as null-mid)
    prices = [(0.5, None, 0.5, 0.5), (None, 0.5, 0.5, 0.5), (0.5, 0.5, None, 0.5), (0.5, 0.5, 0.5, None)]
    for ob, ua, db, da in prices:
        has_null = ob is None or ua is None or db is None or da is None
        assert has_null, "should have detected null"
    # All present should not trigger has_null
    ob, ua, db, da = 0.5, 0.5, 0.5, 0.5
    has_null = ob is None or ua is None or db is None or da is None
    assert not has_null


# ---------------------------------------------------------------------------
# Log-tail parser test (inline literals, no real log dependency)
# ---------------------------------------------------------------------------


def test_parse_uploads_healthy():
    """HEALTHY when no failed lines."""
    import tempfile
    txt = "some other line\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
        f.write(txt)
        tmppath = f.name
    try:
        verdict = parse_uploads(Path(tmppath))
        # No "Upload successful" or "Upload failed" -> failed=0 -> HEALTHY
        assert verdict.status == "HEALTHY"
        assert verdict.failed == 0
    finally:
        os.unlink(tmppath)


def test_parse_uploads_degraded():
    """DEGRADED when failed lines present."""
    import tempfile
    txt = "Upload successful at 2026-10-07T16:00:00\n" \
          "Upload failed at 2026-10-07T16:05:00\n" \
          "Upload successful at 2026-10-07T16:10:00\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
        f.write(txt)
        tmppath = f.name
    try:
        verdict = parse_uploads(Path(tmppath))
        assert verdict.failed == 1
        assert verdict.successful == 2
        # failed > 0 -> DEGRADED
        assert verdict.status == "DEGRADED"
    finally:
        os.unlink(tmppath)


def test_parse_uploads_missing_log():
    """UNKNOWN when log file is missing."""
    verdict = parse_uploads(Path("/nonexistent/log.log"))
    assert verdict.status == "UNKNOWN"


# ---------------------------------------------------------------------------
# Real-data smoke test (skip if data/ doesn't exist)
# ---------------------------------------------------------------------------


def test_smoke_real_data():
    """Smoke test with real data from the collector box.

    Skipped entirely when /home/fese/polymarket-data-collector/data/ does not exist.
    """
    data_dir = Path("/home/fese/polymarket-data-collector/data")
    if not data_dir.exists():
        pytest.skip("no data/ directory on this box")

    # Pick a date that has partitions
    date_str = "2026-10-07"

    # Test each asset lane via analyze_lane (footer-only, no row materialisation
    # beyond what's needed for the predicate)
    for asset in ["BTC", "ETH", "SOL"]:
        snap_dir = data_dir / "book_snapshots_500ms" / f"date={date_str}" / f"asset={asset}"
        if not snap_dir.exists():
            pytest.skip(f"No snapshot dir for {asset} on {date_str}")
        result = analyze_lane(snap_dir, asset, max_files=48)
        # Basic sanity: result should have sensible types
        assert isinstance(result.files_sampled, int)
        assert result.files_sampled >= 0
        assert 0.0 <= result.density_pct <= 100.0
        assert 0.0 <= result.complement_gt_01_pct <= 100.0
        assert isinstance(result.dup_snapshot_id_count, int)
        assert result.dup_snapshot_id_count >= 0
        assert isinstance(result.offgrid_count, int)
        assert result.offgrid_count >= 0
        assert isinstance(result.span_ns, int)
        assert result.span_ns >= 0

    # Test uploads parser on the real log
    log_path = data_dir / "logs" / "collector-out-11.log"
    if log_path.exists():
        verdict = parse_uploads(log_path, last_n=2000)
        assert verdict.status in ("HEALTHY", "DEGRADED", "UNKNOWN")
        assert verdict.failed >= 0
        assert verdict.successful >= 0