"""Tests for scripts/scorecard.py — tiny parquet tempdirs, no live data needed.

Covers: bounded T1 scan counters (identity nulls, approx duplicated-key census,
out-of-range/zero best prices, book_state counts, orphan resync ids, off-grid),
the G6 depth-recompute spot check (consistent + mismatched depth columns),
budget/partial behavior, the settlement official-share block, resolution of
date/assets, resync-id set loading, and the approx dup-set contract.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import scorecard  # noqa: E402

DATE = "2025-01-01"
TS_BASE = 1_700_000_000_000_000_000  # on 500ms grid when offset by grid multiples


def _rows(**overrides):
    row = {
        "condition_id": "cid-1",
        "ts_snapshot_ns": TS_BASE,
        "snapshot_id": "s1",
        "market_id": "m1",
        "series_id": "sr1",
        "book_state": "live",
        "resync_id": "",
        "up_bid": 0.49,
        "up_ask": 0.51,
        "down_bid": 0.48,
        "down_ask": 0.50,
    }
    row.update(overrides)
    return row


def _write_asset(base: Path, asset: str, rows_per_file: list) -> Path:
    d = base / "book_snapshots_500ms" / f"date={DATE}" / f"asset={asset}"
    d.mkdir(parents=True, exist_ok=True)
    for i, rows in enumerate(rows_per_file):
        pq.write_table(pa.Table.from_pylist(rows), str(d / f"part-{i:03d}.parquet"))
    return d


# ---------------------------------------------------------------------------
# ApproxHashDupSet contract
# ---------------------------------------------------------------------------


def test_approx_dup_set_exact_zero_and_repeat():
    s = scorecard.ApproxHashDupSet(bits=16, ways=3)
    key = b"cid|123"
    assert s.add_if_new(key) is True  # new
    assert s.add_if_new(key) is False  # duplicate: always caught (no false negative)
    assert s.items == 1


def test_approx_dup_set_many_keys_all_counted():
    s = scorecard.ApproxHashDupSet(bits=18, ways=3)
    for i in range(2000):
        assert s.add_if_new(f"c{i}\x00{i}".encode()) is True
    assert s.items == 2000
    # a second pass flags them all as seen (dup counting depends on this)
    for i in range(2000):
        assert s.add_if_new(f"c{i}\x00{i}".encode()) is False


# ---------------------------------------------------------------------------
# T1 bounded scan
# ---------------------------------------------------------------------------


def test_t1_scan_counters(tmp_path):
    # every row gets its own on-grid ts so the dup census is exactly one
    good = _rows(ts_snapshot_ns=TS_BASE)
    null_ident = _rows(ts_snapshot_ns=TS_BASE + 500_000_000, condition_id=None)
    bad_price = _rows(ts_snapshot_ns=TS_BASE + 1_000_000_000, up_bid=1.5)
    zero_price = _rows(ts_snapshot_ns=TS_BASE + 1_500_000_000, up_bid=0.0, up_ask=0.52)
    dup_key = _rows(ts_snapshot_ns=TS_BASE)  # same (condition_id, ts) as good → dup
    orphan = _rows(ts_snapshot_ns=TS_BASE + 2_000_000_000, resync_id="rid-missing")
    known_rid = _rows(ts_snapshot_ns=TS_BASE + 2_500_000_000, resync_id="rid-known")
    offgrid = _rows(ts_snapshot_ns=TS_BASE + 3_000_000_001)
    stale = _rows(ts_snapshot_ns=TS_BASE + 3_500_000_000, book_state="stale")
    asset_dir = _write_asset(tmp_path, "BTC", [[good, null_ident, bad_price],
                                               [zero_price, dup_key, orphan],
                                               [known_rid, offgrid, stale]])
    (tmp_path / "resync_episodes" / f"date={DATE}").mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist([{"resync_id": "rid-known"}]),
        str(tmp_path / "resync_episodes" / f"date={DATE}" / "part.parquet"),
    )
    ids, overflow, unread = scorecard.load_resync_ids(
        tmp_path / "resync_episodes" / f"date={DATE}", deadline=time.monotonic() + 60
    )
    assert ids == {"rid-known"} and not overflow and unread == 0

    deadline = time.monotonic() + 120
    block = scorecard.scan_lane_t1(asset_dir, ids, deadline=deadline)
    assert not block.partial
    assert block.files_scanned == 3 and block.files_total == 3
    assert block.rows_scanned == 9
    # identity nulls: condition_id None once
    assert block.ident_nulls.get("condition_id") == 1
    assert sum(block.ident_nulls.values()) == 1
    # duplicate (condition_id, ts): the good row + its repeat — small census → exact
    assert block.dup_key_count == 1
    assert block.out_of_range_rows == 1
    assert block.zero_price_rows == 1
    assert block.orphan_resync_rows == 1  # blank ignored, rid-known present, rid-missing orphan
    assert block.offgrid_rows == 1
    assert block.state_counts == {"live": 8, "stale": 1}
    assert block.t1_total() == 5  # nulls(1) + dup(1) + badpx(1) + orph(1) + offgrid(1)


def test_t1_scan_empty_dir_and_missing_columns(tmp_path):
    d = tmp_path / "book_snapshots_500ms" / f"date={DATE}" / "asset=SOL"
    d.mkdir(parents=True)
    block = scorecard.scan_lane_t1(d, set(), deadline=time.monotonic() + 60)
    assert block.files_total == 0 and block.rows_scanned == 0
    assert block.g6_done  # nothing to sample → honest zero, not partial


def test_t1_scan_budget_partial(tmp_path):
    rows = [_rows(ts_snapshot_ns=TS_BASE + i * 500_000_000) for i in range(5)]
    asset_dir = _write_asset(tmp_path, "ETH", [rows, rows])
    # deadline already in the past → partial, counters stay zero, no crash
    block = scorecard.scan_lane_t1(asset_dir, set(), deadline=time.monotonic() - 1.0)
    assert block.partial
    assert block.files_scanned == 0
    assert block.g6_done is False


def test_t1_scan_skips_tmp_and_unreadable(tmp_path):
    asset_dir = tmp_path / "book_snapshots_500ms" / f"date={DATE}" / "asset=XRP"
    asset_dir.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([_rows()]), str(asset_dir / "part-000.parquet"))
    pq.write_table(
        pa.Table.from_pylist([_rows(ts_snapshot_ns=TS_BASE + 500_000_000)]),
        str(asset_dir / "part-001.parquet.tmp"),  # atomic tmp never part of the day
    )
    (asset_dir / "broken.parquet").write_bytes(b"notarquet")
    block = scorecard.scan_lane_t1(asset_dir, set(), deadline=time.monotonic() + 60)
    assert block.files_total == 2  # .tmp excluded by day_files
    assert block.files_scanned == 1
    assert block.files_skipped == 1  # unreadable counted, never fatal
    assert block.rows_scanned == 1


# ---------------------------------------------------------------------------
# G6 depth recompute
# ---------------------------------------------------------------------------


def _g6_row(depth_overrides=None):
    """One row: up book best 0.50; levels 0.50(+2), 0.495(+3), 0.46(+1) → d1=5, d5=6, d10=6
    (0.495 is 0.005c away → 1c bucket; 0.46 is 4c away → 5c bucket; 0.30 breaks the walk).
    Down side left fully null → stored depths must be None too."""
    row = {
        "ts_snapshot_ns": TS_BASE,
        "up_bid": 0.50,
        "up_ask": None,
        "down_bid": None,
        "down_ask": None,
        "book_state": "live",
    }
    for outc, side in scorecard.G6_SIDES:
        for lvl in range(1, 101):
            row[f"{outc}_{side}_level_{lvl}_price"] = None
            row[f"{outc}_{side}_level_{lvl}_size"] = None
        for thc in (1, 5, 10):
            row[f"{outc}_{side}_depth_{thc}c"] = None
    row["up_bid_level_1_price"] = 0.50
    row["up_bid_level_1_size"] = 2.0
    row["up_bid_level_2_price"] = 0.495
    row["up_bid_level_2_size"] = 3.0
    row["up_bid_level_3_price"] = 0.46
    row["up_bid_level_3_size"] = 1.0
    row["up_bid_level_4_price"] = 0.30  # outside 10c window → walk breaks
    row["up_bid_level_4_size"] = 100.0
    row["up_bid_depth_1c"] = 5.0
    row["up_bid_depth_5c"] = 6.0
    row["up_bid_depth_10c"] = 6.0
    if depth_overrides:
        for k, v in depth_overrides.items():
            row[k] = v
    return row


def test_g6_depth_consistent(tmp_path):
    fp = tmp_path / "g6ok.parquet"
    pq.write_table(pa.Table.from_pylist([_g6_row()]), str(fp))
    checked, mismatch = scorecard._g6_check_file(fp, deadline=time.monotonic() + 60)
    assert checked == 1 and mismatch == 0


def test_g6_depth_mismatch_within_tolerance_and_nulls(tmp_path):
    bad = _g6_row({"up_bid_depth_5c": 6.0 + 1e-3})  # beyond DEPTH_TOL
    nanish_null = _g6_row({"up_bid_depth_10c": None})  # stored None vs expected 6.0
    fp = tmp_path / "g6bad.parquet"
    pq.write_table(pa.Table.from_pylist([bad, nanish_null]), str(fp))
    checked, mismatch = scorecard._g6_check_file(fp, deadline=time.monotonic() + 60)
    assert checked == 2 and mismatch == 2


def test_g6_depth_downside_null_consistent(tmp_path):
    # all-null down book with all-null stored depths must NOT count as mismatch
    row = _g6_row()
    fp = tmp_path / "g6nullet.parquet"
    pq.write_table(pa.Table.from_pylist([row]), str(fp))
    checked, mismatch = scorecard._g6_check_file(fp, deadline=time.monotonic() + 60)
    assert checked == 1 and mismatch == 0


# ---------------------------------------------------------------------------
# Settlement official share
# ---------------------------------------------------------------------------


def test_official_settlement_share(tmp_path):
    fp = tmp_path / "markets_latest.parquet"
    rows = [
        {"status": "resolved", "settlement_source": "on_chain_confirmed"},
        {"status": "resolved", "settlement_source": "polymarket_official"},
        {"status": "resolved", "settlement_source": "inferred_nearest"},
        {"status": "resolved", "settlement_source": "other_source"},
        {"status": "active", "settlement_source": "on_chain_confirmed"},
        {"status": "closed", "settlement_source": None},
    ]
    pq.write_table(pa.Table.from_pylist(rows), str(fp))
    counts, partial = scorecard.official_settlement_share(fp)
    assert not partial
    assert counts == {"settled": 4, "official": 2, "inferred": 1, "other": 1}
    share = counts["official"] / counts["settled"] * 100
    assert share == pytest.approx(50.0)


def test_official_settlement_share_missing_file(tmp_path):
    counts, partial = scorecard.official_settlement_share(tmp_path / "missing.parquet")
    assert counts["settled"] == 0 and partial


# ---------------------------------------------------------------------------
# Resolution / verdict helpers
# ---------------------------------------------------------------------------


def test_resolve_date_latest_partition(tmp_path):
    (tmp_path / "book_snapshots_500ms" / "date=2025-01-02").mkdir(parents=True)
    (tmp_path / "book_snapshots_500ms" / "date=2025-01-03" / "asset=BTC").mkdir(parents=True)
    assert scorecard.resolve_date(tmp_path, None) == "2025-01-03"
    assert scorecard.resolve_date(tmp_path, "2025-01-02") == "2025-01-02"
    with pytest.raises(SystemExit):
        scorecard.resolve_date(tmp_path, "1999-12-31")


def test_resolve_assets_from_partition_and_filter(tmp_path):
    root = tmp_path / "book_snapshots_500ms" / f"date={DATE}"
    for a in ("BTC", "ETH", "SOL"):
        (root / f"asset={a}").mkdir(parents=True)
    assert scorecard.resolve_assets(tmp_path, DATE, None) == ["BTC", "ETH", "SOL"]
    assert scorecard.resolve_assets(tmp_path, DATE, "btc, sol") == ["BTC", "SOL"]
    with pytest.raises(SystemExit):
        scorecard.resolve_assets(tmp_path, DATE, "XXX")


def test_gate_verdict_watch_on_partial():
    probe = {"BTC": (99.99, 0.0, 10, False)}
    block = scorecard.T1Block(asset="BTC", partial=True)
    official = {"settled": 10, "official": 10, "inferred": 0, "other": 0}
    status, fails = scorecard.gate_verdict(probe, {"BTC": block}, official, 100.0, True)
    assert status == "WATCH"
    assert any("partial" in f for f in fails)


def test_gate_verdict_fail_on_official_share():
    probe = {"BTC": (99.99, 0.0, 10, False)}
    block = scorecard.T1Block(asset="BTC")
    official = {"settled": 10, "official": 9, "inferred": 1, "other": 0}
    status, fails = scorecard.gate_verdict(probe, {"BTC": block}, official, 90.0, False)
    assert status == "FAIL"
    assert any("official-share=90.0%" in f for f in fails)


def test_gate_verdict_pass():
    probe = {"BTC": (100.0, 0.0, 10, False), "ETH": (99.5, 0.05, 10, False)}
    block = scorecard.T1Block(asset="BTC")
    official = {"settled": 10, "official": 10, "inferred": 0, "other": 0}
    status, fails = scorecard.gate_verdict(probe, {"BTC": block, "ETH": block}, official, 100.0, False)
    assert status == "PASS" and fails == []


def test_gate_verdict_fail_density_and_compnorm():
    probe = {"BTC": (97.0, 0.5, 10, False)}
    block = scorecard.T1Block(asset="BTC")
    official = {"settled": 1, "official": 1, "inferred": 0, "other": 0}
    status, fails = scorecard.gate_verdict(probe, {"BTC": block}, official, 100.0, False)
    assert status == "FAIL"
    assert any("density=97%" in f for f in fails)
    assert any("CompNorm=0.5%" in f for f in fails)


def test_run_scorecard_end_to_end_minimal(tmp_path):
    """End-to-end on a tiny real-file layout: completeness child, probe, scan,
    official share, lanes state — all offline, all from real files."""
    base = tmp_path / "data"
    rows = [_rows(resync_id="r1" if i == 0 else "", ts_snapshot_ns=TS_BASE + i * 500_000_000)
            for i in range(4)]
    _write_asset(base, "BTC", [rows])
    (base / "resync_episodes" / f"date={DATE}").mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"resync_id": "r1"}]),
        str(base / "resync_episodes" / f"date={DATE}" / "part.parquet"),
    )
    (base / "markets_latest").mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"status": "resolved", "settlement_source": "polymarket_official"}]),
        str(base / "markets_latest" / "markets_latest.parquet"),
    )
    snap = scorecard.run_scorecard(base, DATE, ["BTC"], budget_s=120.0, probe_files=48)
    assert snap["blocks"]["BTC"].files_scanned == 1
    assert snap["blocks"]["BTC"].rows_scanned == 4
    assert snap["blocks"]["BTC"].dup_key_count == 0  # distinct ts per row → 0 (exact)
    assert snap["official_share"] == pytest.approx(100.0)
    assert snap["lanes_state"] == []  # no kaggle_staging dir → nothing fabricated
