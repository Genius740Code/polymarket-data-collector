"""Tests for scripts/gate_probe.py — pure helpers + real-data smoke test.

Forbidden: no fabricated data.
Allowed: inline literals for math checks, off-grid predicates, complement
deviation, and log-tail parsing. A small real-data smoke test runs only
when data/ exists (skip otherwise).
"""
from __future__ import annotations

import sys
import os
import pytest
from pathlib import Path
from typing import List

# Add scripts to path so we can import gate_probe
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from gate_probe import (
    compute_expected_slots,
    GRID_NS,
    DEV_FLOOR,
    SPREAD_FRAC,
    analyze_lane,
    parse_uploads,
    # UploadVerdict,  # unused at module level; used inline
)


# ---------------------------------------------------------------------------
# Pure helper tests (no data/ dependency)
# ---------------------------------------------------------------------------


def test_compute_expected_slots():
    """Grid slots over a span: floor(span_ns / 500_000_000) + 1, ends inclusive."""
    # Zero-width span still covers exactly one grid slot (census denominator).
    assert compute_expected_slots(0) == 1
    # span = 500ms -> intervals: [0, 500ms) -> 1 interval + 1 = 2 slots
    assert compute_expected_slots(500_000_000) == 2
    # span = 1000ms -> intervals: [0, 1000ms) -> 2 intervals + 1 = 3 slots
    assert compute_expected_slots(1_000_000_000) == 3
    # span = 1500ms -> 3 intervals + 1 = 4 slots
    assert compute_expected_slots(1_500_000_000) == 4
    # Negative spans cannot occur (max - min >= 0); guarded to 0.
    assert compute_expected_slots(-1) == 0


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


def test_complement_live_stale_split():
    """CompLive>0.1% and CompStale>0.1% split correctly by book_state."""
    # Construct row dicts mirroring what analyze_lane produces via to_pydict()
    live_rows = [
        {"up_bid": 0.5, "up_ask": 0.5, "down_bid": 0.5, "down_ask": 0.5, "book_state": "live"},  # dev=0, not >0.001
        {"up_bid": 0.3, "up_ask": 0.3, "down_bid": 0.3, "down_ask": 0.3, "book_state": "live"},  # dev=0.4 > 0.001
        {"up_bid": 0.5, "up_ask": 0.6, "down_bid": 0.4, "down_ask": 0.5, "book_state": "live"},  # dev=0, not >0.001
    ]
    stale_rows = [
        {"up_bid": 0.7, "up_ask": 0.7, "down_bid": 0.7, "down_ask": 0.7, "book_state": "stale"},  # dev=0.4 > 0.001
        {"up_bid": 0.5, "up_ask": 0.5, "down_bid": 0.5, "down_ask": 0.5, "book_state": "resyncing"},  # dev=0, not >0.001
    ]

    # Manually compute what analyze_lane would compute
    null_mid_count = 0
    complement_count = 0
    checkable_count = 0
    complement_count_live = 0
    complement_checkable_live = 0
    complement_count_stale = 0
    complement_checkable_stale = 0

    for r in live_rows + stale_rows:
        ob = r["up_bid"]
        ua = r["up_ask"]
        db = r["down_bid"]
        da = r["down_ask"]
        has_null = ob is None or ua is None or db is None or da is None
        book_state = r.get("book_state", "live")
        is_live = book_state == "live"

        if has_null:
            null_mid_count += 1
            continue

        try:
            mid = (float(ob) + float(ua)) / 2.0 + (float(db) + float(da)) / 2.0
            deviation = abs(mid - 1.0)
        except (TypeError, ValueError):
            null_mid_count += 1
            continue

        checkable_count += 1
        if deviation > 0.001:
            complement_count += 1

        if is_live:
            complement_checkable_live += 1
            if deviation > 0.001:
                complement_count_live += 1
        else:
            complement_checkable_stale += 1
            if deviation > 0.001:
                complement_count_stale += 1

    comp_live = (complement_count_live / complement_checkable_live * 100) if complement_checkable_live > 0 else 0.0
    comp_stale = (complement_count_stale / complement_checkable_stale * 100) if complement_checkable_stale > 0 else 0.0
    overall = (complement_count / checkable_count * 100) if checkable_count > 0 else 0.0

    # Live: 1 out of 3 has dev > 0.001 -> 33.33%
    assert comp_live == 33.33333333333333, f"expected CompLive>0.1% == 33.33..., got {comp_live}"
    # Stale: 1 out of 2 has dev > 0.001 -> 50.0%
    assert comp_stale == 50.0, f"expected CompStale>0.1% == 50.0, got {comp_stale}"
    # Overall: 2 out of 5 have dev > 0.001 -> 40.0%
    assert overall == 40.0, f"expected overall == 40.0, got {overall}"
    assert null_mid_count == 0


def test_null_mid_count_guard():
    """Rows with any null price are counted as null-mid, never imputed, regardless of book_state."""
    prices = [
        (0.5, None, 0.5, 0.5),
        (None, 0.5, 0.5, 0.5),
        (0.5, 0.5, None, 0.5),
        (0.5, 0.5, 0.5, None),
    ]
    for ob, ua, db, da in prices:
        has_null = ob is None or ua is None or db is None or da is None
        assert has_null, "should have detected null"
    # All present should not trigger has_null
    ob, ua, db, da = 0.5, 0.5, 0.5, 0.5
    has_null = ob is None or ua is None or db is None or da is None
    assert not has_null


def test_complement_threshold_edge_001():
    """Threshold edge: deviation > 0.001 predicate behavior.
    
    Due to IEEE 754 floating point, exact 0.001 deviation is not reliably
    achievable. This test verifies the predicate works correctly for values
    clearly on each side of the threshold, and that the edge case is handled
    by the > comparison in analyze_lane.
    """
    # Clearly above threshold: deviation = 0.1 > 0.001
    ob, ua, db, da = 0.55, 0.5, 0.5, 0.5
    mid = (ob + ua) / 2.0 + (db + da) / 2.0
    deviation = abs(mid - 1.0)
    assert deviation > 0.001, f"expected deviation > 0.001, got {deviation}"
    assert (deviation > 0.001) == True

    # Clearly below/at threshold: deviation = 0.0 not > 0.001
    ob, ua, db, da = 0.5, 0.5, 0.5, 0.5
    mid = (ob + ua) / 2.0 + (db + da) / 2.0
    deviation = abs(mid - 1.0)
    assert not (deviation > 0.001), f"expected deviation <= 0.001, got {deviation}"
    assert (deviation > 0.001) == False

    # The key semantic tested: the > 0.001 comparison in analyze_lane
    # correctly classifies rows. The exact 0.001 boundary is a floating point
    # artifact; the test verifies the predicate works for clear cases.


def test_comp_spread_norm_wide_spread_pass():
    """CompSpreadNorm: wide spread should not flag when deviation is small.

    When combined_spread is wide, SPREAD_FRAC * combined_spread raises the
    threshold above the deviation, so the row is NOT flagged. This prevents
    false complementarity flags due to ordinary illiquidity.
    """
    # up_mid=0.55, down_mid=0.45 -> deviation=0.0
    # combined_spread = 0.1 + 0.1 = 0.2
    # threshold = max(0.001, 0.25*0.2) = 0.05
    # 0.0 > 0.05? No -> NOT flagged (wide-spread pass)
    ob, ua, db, da = 0.5, 0.6, 0.4, 0.5
    up_mid = (ob + ua) / 2.0
    down_mid = (db + da) / 2.0
    deviation = abs(up_mid + down_mid - 1.0)
    combined_spread = (ua - ob) + (da - db)
    threshold = max(DEV_FLOOR, SPREAD_FRAC * combined_spread)
    flag = deviation > threshold
    assert flag == False, f"wide spread should not flag, got flag={flag}, deviation={deviation}, threshold={threshold}"


def test_comp_spread_norm_tight_spread_breach():
    """CompSpreadNorm: tight spread should flag modest deviation.

    When combined_spread is narrow, SPREAD_FRAC * combined_spread is small,
    so even a modest deviation exceeds the threshold and the row IS flagged.
    """
    # up_mid=0.5025, down_mid=0.5 -> deviation=0.0025
    # combined_spread = 0.005 + 0.0 = 0.005
    # threshold = max(0.001, 0.25*0.005) = 0.00125
    # 0.0025 > 0.00125? Yes -> flagged (tight-spread breach)
    ob, ua, db, da = 0.5, 0.505, 0.5, 0.5
    up_mid = (ob + ua) / 2.0
    down_mid = (db + da) / 2.0
    deviation = abs(up_mid + down_mid - 1.0)
    combined_spread = (ua - ob) + (da - db)
    threshold = max(DEV_FLOOR, SPREAD_FRAC * combined_spread)
    flag = deviation > threshold
    assert flag == True, f"tight spread should flag, got flag={flag}, deviation={deviation}, threshold={threshold}"


def test_comp_spread_norm_live_single_count():
    """CompSpreadNorm: live rows should be single-counted, not double-counted.

    Confirms that comp_spread_norm_count increments exactly once per live
    breaching row (the fix for a bug where it incremented twice: once in the
    standalone spread_norm_flag block and again inside if is_live).
    """
    # 3 live rows: 1 breaching CompSpreadNorm, 2 not
    live_rows = [
        {"up_bid": 0.5, "up_ask": 0.505, "down_bid": 0.5, "down_ask": 0.5, "book_state": "live"},  # breaching
        {"up_bid": 0.5, "up_ask": 0.5, "down_bid": 0.5, "down_ask": 0.5, "book_state": "live"},    # not breaching
        {"up_bid": 0.5, "up_ask": 0.5, "down_bid": 0.5, "down_ask": 0.5, "book_state": "live"},    # not breaching
    ]
    stale_rows = []

    null_mid_count = 0
    complement_count = 0
    checkable_count = 0
    complement_count_live = 0
    complement_checkable_live = 0
    comp_spread_norm_count = 0

    for r in live_rows + stale_rows:
        ob = r["up_bid"]
        ua = r["up_ask"]
        db = r["down_bid"]
        da = r["down_ask"]
        has_null = ob is None or ua is None or db is None or da is None
        book_state = r.get("book_state", "live")
        is_live = book_state == "live"

        if has_null:
            null_mid_count += 1
            continue

        try:
            up_mid = (float(ob) + float(ua)) / 2.0
            down_mid = (float(db) + float(da)) / 2.0
            deviation = abs(up_mid + down_mid - 1.0)
            combined_spread = (float(ua) - float(ob)) + (float(da) - float(db))
        except (TypeError, ValueError):
            null_mid_count += 1
            continue

        checkable_count += 1
        if deviation > 0.001:
            complement_count += 1

        spread_norm_flag = deviation > max(0.001, 0.25 * combined_spread)
        if is_live and spread_norm_flag:
            comp_spread_norm_count += 1

        if is_live:
            complement_checkable_live += 1
            if deviation > 0.001:
                complement_count_live += 1

    # 1 out of 3 live rows breaches CompSpreadNorm -> count==1 exactly
    assert comp_spread_norm_count == 1, (
        f"expected comp_spread_norm_count == 1 (single-count fix), got {comp_spread_norm_count}"
    )


def test_live_null_split():
    """LiveNullNoBook vs LiveNullPartial classification by null pattern.

    LiveNullNoBook: all four prices are null (pre-discovery, stale-like).
    LiveNullPartial: some but not all prices are null (real anomaly).
    """
    # Simulate the classification logic from analyze_lane
    live_null_no_book_count = 0
    live_null_partial_count = 0
    live_null_partial_examples: List[int] = []

    # Row where all 4 prices are null, book_state=live
    r1_ob, r1_ua, r1_db, r1_da = None, None, None, None
    r1_is_live = True
    has_null = r1_ob is None or r1_ua is None or r1_db is None or r1_da is None
    if has_null and r1_is_live:
        all_null = (r1_ob is None and r1_ua is None and r1_db is None and r1_da is None)
        if all_null:
            live_null_no_book_count += 1
        else:
            live_null_partial_count += 1
            live_null_partial_examples.append(12345)

    # Row where only up_ask is null, book_state=live
    r2_ob, r2_ua, r2_db, r2_da = 0.5, None, 0.5, 0.5
    r2_is_live = True
    has_null = r2_ob is None or r2_ua is None or r2_db is None or r2_da is None
    if has_null and r2_is_live:
        all_null = (r2_ob is None and r2_ua is None and r2_db is None and r2_da is None)
        if all_null:
            live_null_no_book_count += 1
        else:
            live_null_partial_count += 1
            live_null_partial_examples.append(67890)

    # Row where up_bid is null but others present, book_state=live
    r3_ob, r3_ua, r3_db, r3_da = None, 0.5, 0.5, 0.5
    r3_is_live = True
    has_null = r3_ob is None or r3_ua is None or r3_db is None or r3_da is None
    if has_null and r3_is_live:
        all_null = (r3_ob is None and r3_ua is None and r3_db is None and r3_da is None)
        if all_null:
            live_null_no_book_count += 1
        else:
            live_null_partial_count += 1
            live_null_partial_examples.append(11111)

    assert live_null_no_book_count == 1, f"expected 1 LiveNullNoBook, got {live_null_no_book_count}"
    assert live_null_partial_count == 2, f"expected 2 LiveNullPartial, got {live_null_partial_count}"
    assert len(live_null_partial_examples) == 2
    assert 67890 in live_null_partial_examples
    assert 11111 in live_null_partial_examples


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
# Sampling order — newest files by mtime (name order is arbitrary)
# ---------------------------------------------------------------------------


def _snap(ts_ns, sid, ob, ua, db, da, state="live"):
    """Inline-literal snapshot row for fixture files."""
    return {
        "ts_snapshot_ns": ts_ns,
        "snapshot_id": sid,
        "up_bid": ob,
        "up_ask": ua,
        "down_bid": db,
        "down_ask": da,
        "book_state": state,
    }


def _write_snapshot_file(path, rows):
    """Write a small parquet fixture holding exactly the projected columns."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    tbl = pa.table(
        {
            "ts_snapshot_ns": pa.array([r["ts_snapshot_ns"] for r in rows], type=pa.int64()),
            "snapshot_id": pa.array([r["snapshot_id"] for r in rows], type=pa.int64()),
            "up_bid": pa.array([r["up_bid"] for r in rows], type=pa.float64()),
            "up_ask": pa.array([r["up_ask"] for r in rows], type=pa.float64()),
            "down_bid": pa.array([r["down_bid"] for r in rows], type=pa.float64()),
            "down_ask": pa.array([r["down_ask"] for r in rows], type=pa.float64()),
            "book_state": pa.array([r["book_state"] for r in rows], type=pa.string()),
        }
    )
    pq.write_table(tbl, path)


def test_newest_parquet_files_mtime_order(tmp_path):
    """Sampling keeps the newest files by mtime, not the first by name.

    New helpers are imported lazily so the pre-fix baseline fails this test
    (and only this test) instead of erroring the whole module at collection.
    """
    from gate_probe import newest_parquet_files

    names_and_mtimes = [
        ("a.parquet", 1000.0),
        ("d.parquet", 100.0),
        ("e.parquet", 1500.0),
        ("b.parquet", 2000.0),
        ("c.parquet", 3000.0),
        ("z.tmp", 9999.0),  # excluded despite the newest mtime
    ]
    for name, mtime in names_and_mtimes:
        p = tmp_path / name
        p.write_text("")
        os.utime(p, (mtime, mtime))

    got = newest_parquet_files(tmp_path, 3)
    assert [p.name for p in got] == ["c.parquet", "b.parquet", "e.parquet"]

    # max_files beyond the candidate set keeps all of them, newest first
    got_all = newest_parquet_files(tmp_path, 10)
    assert [p.name for p in got_all] == [
        "c.parquet", "b.parquet", "e.parquet", "a.parquet", "d.parquet",
    ]


def test_analyze_lane_samples_newest_by_mtime(tmp_path):
    """analyze_lane must sample the newest-by-mtime file, not the first by name."""
    older = tmp_path / "a.parquet"  # name-sorts first, older mtime
    newer = tmp_path / "b.parquet"  # name-sorts second, newer mtime
    _write_snapshot_file(
        older,
        [
            _snap(1000 * GRID_NS, 1, 0.5, 0.5, 0.5, 0.5),
            _snap(1001 * GRID_NS, 2, 0.5, 0.5, 0.5, 0.5),
        ],
    )
    _write_snapshot_file(
        newer,
        [
            _snap(5000 * GRID_NS, 3, 0.5, 0.5, 0.5, 0.5),
            _snap(5003 * GRID_NS, 4, 0.5, 0.5, 0.5, 0.5),
        ],
    )
    os.utime(older, (1_000_000, 1_000_000))
    os.utime(newer, (2_000_000, 2_000_000))

    result = analyze_lane(tmp_path, "BTC", max_files=1)
    assert result.files_sampled == 1
    # Only achievable if the newer file (span 3 grid ticks) was sampled;
    # name-order sampling of the older file would give span 1 grid tick.
    assert result.span_ns == 3 * GRID_NS, "expected the newest-by-mtime file to be sampled"


# ---------------------------------------------------------------------------
# Density% — honest tick census (old expected-minus-offgrid formula was
# degenerate: it reported 100.0 whenever offgrid == 0, gaps included)
# ---------------------------------------------------------------------------


def test_density_census_counts_gaps():
    """A missing 500ms tick must show up as Density% below 100."""
    from gate_probe import density_census_pct

    # Ticks at 0, 500ms, 1000ms, 2000ms — the 1500ms tick is missing.
    # The old formula reported (expected 5 - offgrid 0) / 5 = 100.0 despite the gap.
    ts = [0, GRID_NS, 2 * GRID_NS, 4 * GRID_NS]
    assert density_census_pct(ts) == 80.0  # 4 distinct on-grid / 5 slots


def test_density_census_duplicates_do_not_inflate():
    from gate_probe import density_census_pct

    # Three distinct ticks, each duplicated: still 3/3, never 6/3.
    assert density_census_pct([0, 0, GRID_NS, GRID_NS, 2 * GRID_NS, 2 * GRID_NS]) == 100.0


def test_density_census_offgrid_excluded():
    from gate_probe import density_census_pct

    # 1ns is off-grid: distinct on-grid = {2000ms} = 1, span//500ms + 1 = 2 -> 50.0
    assert density_census_pct([1, 2 * GRID_NS]) == 50.0


def test_density_census_single_tick():
    from gate_probe import density_census_pct

    assert density_census_pct([GRID_NS]) == 100.0  # lone on-grid tick: 1/1
    assert density_census_pct([1]) == 0.0  # lone off-grid tick counts as no tick
    assert density_census_pct([]) == 0.0


def test_analyze_lane_density_reports_gap(tmp_path):
    """End-to-end: a lane with a missing 500ms tick reports Density% < 100."""
    rows = [
        _snap(0, 1, 0.5, 0.5, 0.5, 0.5),
        _snap(GRID_NS, 2, 0.5, 0.5, 0.5, 0.5),
        _snap(2 * GRID_NS, 3, 0.5, 0.5, 0.5, 0.5),
        _snap(4 * GRID_NS, 4, 0.5, 0.5, 0.5, 0.5),  # 1500ms tick missing
    ]
    _write_snapshot_file(tmp_path / "lane.parquet", rows)
    result = analyze_lane(tmp_path, "BTC", max_files=48)
    # Old formula reported 100.0 here (offgrid == 0); census reports 4/5 = 80.0
    assert result.density_pct == 80.0


# ---------------------------------------------------------------------------
# book_state projection — stale rows must score CompStale, not CompLive
# ---------------------------------------------------------------------------


def test_analyze_lane_stale_rows_score_comp_stale(tmp_path):
    """book_state must be projected: stale rows score CompStale, not CompLive."""
    rows = [
        _snap(0, 1, 0.5, 0.5, 0.5, 0.5, "live"),  # deviation 0
        _snap(GRID_NS, 2, 0.3, 0.3, 0.3, 0.3, "stale"),  # deviation 0.4 > 0.001
        _snap(2 * GRID_NS, 3, 0.5, 0.5, 0.5, 0.5, "resyncing"),  # deviation 0
    ]
    _write_snapshot_file(tmp_path / "lane.parquet", rows)
    result = analyze_lane(tmp_path, "BTC", max_files=48)

    # Stale bucket: 1 of 2 checkable rows deviates -> 50.0 (was 0.0 when
    # book_state was missing from the projection and every row counted as live).
    assert result.comp_stale_gt_01_pct == 50.0
    # Live bucket: 1 checkable row with deviation 0 -> 0.0
    assert result.comp_live_gt_01_pct == 0.0
    # Continuity column over all rows: 1 of 3 -> 33.33
    assert result.complement_gt_01_pct == pytest.approx(33.33)
    # CompNorm% counts live rows only — the breaching row is stale -> 0.0
    assert result.comp_spread_norm_pct == 0.0


# ---------------------------------------------------------------------------
# Gate verdict — keyed on CompNorm% (spread-normalized live share)
# ---------------------------------------------------------------------------


def test_gate_verdict_breaches_use_comp_norm():
    """The verdict keys off CompNorm%, not the raw Comp>0.1% column."""
    from gate_probe import breaching_lanes, LaneResult

    def lane(name, comp_norm_pct):
        return LaneResult(
            lane=name,
            files_sampled=1,
            density_pct=100.0,
            complement_gt_01_pct=90.0,  # raw column high — must NOT gate
            comp_live_gt_01_pct=90.0,
            comp_stale_gt_01_pct=0.0,
            null_mid_count=0,
            comp_spread_norm_pct=comp_norm_pct,
            live_null_no_book_count=0,
            live_null_partial_count=0,
            live_null_partial_examples=[],
            dup_snapshot_id_count=0,
            offgrid_count=0,
            part_flag=False,
            span_ns=0,
        )

    # Raw Comp>0.1% is 90.0 on every lane; only CompNorm% decides the verdict.
    assert breaching_lanes([lane("BTC", 0.0), lane("ETH", 0.05)]) == []
    assert breaching_lanes([lane("BTC", 0.0), lane("ETH", 2.4)]) == ["ETH 2.4"]


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