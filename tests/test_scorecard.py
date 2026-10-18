"""Tests for scripts/scorecard.py — tiny parquet tempdirs, no live data needed.

Covers: bounded T1 scan counters (identity nulls, approx duplicated-key census,
out-of-range/zero best prices, book_state counts, orphan resync ids, off-grid),
the G6 depth-recompute spot check (consistent + mismatched depth columns),
budget/partial behavior, the settlement official-share block, resolution of
date/assets, resync-id set loading, and the approx dup-set contract.
"""
from __future__ import annotations

import json
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
# A1: orphan check vs the FULL resync-episode table
# ---------------------------------------------------------------------------


def test_load_resync_ids_full_tree_dedupe_and_unread(tmp_path):
    (tmp_path / "resync_episodes" / "date=2025-01-01").mkdir(parents=True)
    (tmp_path / "resync_episodes" / "date=2025-01-02").mkdir(parents=True)
    d1 = tmp_path / "resync_episodes" / "date=2025-01-01"
    d2 = tmp_path / "resync_episodes" / "date=2025-01-02"
    pq.write_table(
        pa.Table.from_pylist([{"resync_id": "rid-day-started"}]),
        str(d1 / "p.parquet"),
    )
    # the SAME episode id persisted under an adjacent-day partition: dedupe
    pq.write_table(
        pa.Table.from_pylist([{"resync_id": "rid-day-started"}]),
        str(d2 / "p.parquet"),
    )
    pq.write_table(
        pa.Table.from_pylist([{"resync_id": "rid-adjacent-day"}]),
        str(d2 / "q.parquet"),
    )
    (d2 / "inflight.parquet.tmp").write_bytes(b"partial write")  # skip
    (d2 / "broken.parquet").write_bytes(b"notarquet")  # counted as unread
    ids, overflow, unread = scorecard.load_resync_ids(
        tmp_path / "resync_episodes", deadline=time.monotonic() + 60
    )
    assert ids == {"rid-day-started", "rid-adjacent-day"}  # full tree, deduped
    assert not overflow
    assert unread == 1  # unreadable file counted, never hidden


def test_load_resync_ids_unread_on_expired_deadline(tmp_path):
    # a deadline-cut load must NEVER look like a complete one
    d1 = tmp_path / "resync_episodes" / "date=2025-01-01"
    d1.mkdir(parents=True)
    for i in range(3):
        pq.write_table(
            pa.Table.from_pylist([{"resync_id": f"rid-{i}"}]),
            str(d1 / f"p{i}.parquet"),
        )
    ids, overflow, unread = scorecard.load_resync_ids(
        tmp_path / "resync_episodes", deadline=time.monotonic() - 1.0
    )
    assert ids == set() and not overflow and unread == 3


def test_orphan_rows_checked_against_full_set(tmp_path):
    # rows whose episode started the previous day live NEXT day's rows with
    # the rid persisted in the adjacent-day partition: not orphans anymore.
    asset_dir = _write_asset(tmp_path, "BTC", [
        [_rows(resync_id="rid-adjacent-day")],
        [_rows(ts_snapshot_ns=TS_BASE + 500_000_000, resync_id="rid-really-missing")],
        [_rows(ts_snapshot_ns=TS_BASE + 1_000_000_000, resync_id="")],
    ])
    d2 = tmp_path / "resync_episodes" / "date=2025-01-02"
    d2.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"resync_id": "rid-adjacent-day"}]),
        str(d2 / "p.parquet"),
    )
    ids, overflow, unread = scorecard.load_resync_ids(
        tmp_path / "resync_episodes", deadline=time.monotonic() + 60
    )
    assert ids == {"rid-adjacent-day"}
    block = scorecard.scan_lane_t1(asset_dir, ids, deadline=time.monotonic() + 60)
    assert block.orphan_resync_rows == 1  # only the rid absent from ALL partitions
    assert block.t1_total() == 1


def test_run_scorecard_orphan_zero_when_episode_in_adjacent_partition(tmp_path):
    base = tmp_path / "data"
    rows = [_rows(resync_id="rid-next-day", ts_snapshot_ns=TS_BASE + i * 500_000_000)
            for i in range(3)]
    _write_asset(base, "BTC", [rows])
    d2 = base / "resync_episodes" / "date=2025-01-02"
    d2.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"resync_id": "rid-next-day"}]),
        str(d2 / "p.parquet"),
    )
    (base / "markets_latest").mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"status": "resolved", "settlement_source": "polymarket_official"}]),
        str(base / "markets_latest" / "markets_latest.parquet"),
    )
    snap = scorecard.run_scorecard(base, DATE, ["BTC"], budget_s=120.0, probe_files=48)
    assert snap["blocks"]["BTC"].orphan_resync_rows == 0
    assert snap["resync_unread"] == 0 and not snap["resync_overflow"]


# ---------------------------------------------------------------------------
# A2: per-lane fair slices
# ---------------------------------------------------------------------------


def test_run_scorecard_fair_lane_slices(tmp_path, monkeypatch):
    base = tmp_path / "data"
    rows = [_rows(ts_snapshot_ns=TS_BASE + i * 500_000_000) for i in range(3)]
    for a in ("A", "B", "C", "D", "E", "F", "G"):
        _write_asset(base, a, [rows])
    (base / "resync_episodes").mkdir(parents=True)  # empty episode table
    (base / "markets_latest").mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"status": "resolved", "settlement_source": "polymarket_official"}]),
        str(base / "markets_latest" / "markets_latest.parquet"),
    )
    seen = []
    state = {"t0": time.monotonic()}

    def probe_stub(asset_dir, asset, max_files):
        return (99.0, 0.0, 1, False)

    def scan_stub(asset_dir, rids, deadline, **kw):
        seen.append((asset_dir.name.split("=", 1)[-1], deadline, time.monotonic()))
        b = scorecard.T1Block(asset=asset_dir.name)
        b.files_scanned = 1
        b.rows_scanned = 3
        return b

    monkeypatch.setattr(scorecard, "probe_lane", probe_stub)
    monkeypatch.setattr(scorecard, "scan_lane_t1", scan_stub)
    state["t0"] = time.monotonic()
    snap = scorecard.run_scorecard(
        base, DATE, ["A", "B", "C", "D", "E", "F", "G"], budget_s=120.0, probe_files=48
    )
    assert [name for name, _d, _t in seen] == ["A", "B", "C", "D", "E", "F", "G"]
    run_end = state["t0"] + 120.0
    # every lane's individual deadline respects the run budget — no lane can
    # starve another via a shared global deadline
    for name, deadline, called_at in seen:
        assert deadline <= run_end + 0.01  # microsecond float slack
        assert deadline > called_at  # a positive slice, not an expired one
    # all lanes complete, none partial, probe intact, verdict not WATCH
    for a in "ABCDEFG":
        assert not snap["blocks"][a].partial
        assert snap["probe"][a] == (99.0, 0.0, 1, False)


def test_run_scorecard_zero_budget_marks_pending_lanes_partial(tmp_path, monkeypatch):
    base = tmp_path / "data"
    rows = [_rows(ts_snapshot_ns=TS_BASE + i * 500_000_000) for i in range(3)]
    for a in ("A", "B"):
        _write_asset(base, a, [rows])
    (base / "resync_episodes").mkdir(parents=True)
    (base / "markets_latest").mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"status": "resolved", "settlement_source": "polymarket_official"}]),
        str(base / "markets_latest" / "markets_latest.parquet"),
    )
    monkeypatch.setattr(scorecard, "probe_lane", lambda *_: (None, None, 0, True))
    monkeypatch.setattr(scorecard, "scan_lane_t1", lambda *_: scorecard.T1Block(asset="x"))
    snap = scorecard.run_scorecard(base, DATE, ["A", "B"], budget_s=0.0, probe_files=48)
    for a in ("A", "B"):
        b = snap["blocks"][a]
        assert b.partial and b.files_scanned == 0  # honest partial, nothing claimed
        assert snap["probe"][a] == (None, None, 0, True)


# ---------------------------------------------------------------------------
# Density-split attribution (scripts/density_split.py)
# ---------------------------------------------------------------------------

import density_split  # noqa: E402  (same scripts dir, added to sys.path above)

D0 = 1735689600  # epoch seconds of 2025-01-01T00:00:00Z (DATE, UTC midnight)


def _sec(sec_offset):
    return D0 + sec_offset


def test_density_split_build_report_buckets():
    # two 300s windows:
    #  - "A": rows only at sec 51 (1 tick) → sec 51..170 missing → post_roll;
    #    sec 250 absent (asset uncovered, no narration, no evidence) → other;
    #    sec 100 covered-but-missing (some other market had a row) → other.
    #  - "B": every row it wrote was an all-BBO-null keepalive, and it is
    #    sparse → sustained no-row ticks attribute to book_empty_skipped.
    windows = {
        "cond-A": (_sec(50), _sec(350)),
        "cond-B": (_sec(350), _sec(650)),
    }
    st_a = density_split._MarketState()
    st_a.sec_counts[_sec(51)] = 1  # 1 of 2 ticks in that second
    st_a.rows = 1
    st_a.null_rows = 0
    st_b = density_split._MarketState()
    st_b.sec_counts[_sec(360)] = 1  # one keepalive tick, next-second gap
    st_b.sec_counts[_sec(390)] = 1
    st_b.rows = 2
    st_b.null_rows = 2
    state = {"cond-A": st_a, "cond-B": st_b}
    covered = {_sec(51), _sec(100), _sec(360), _sec(390), _sec(500)}
    outage = [(_sec(200) * 1000, _sec(230) * 1000)]  # narrated outage range
    tot, worst = density_split.build_report(
        DATE, "BTC", windows, state, covered, outage, set(), count_missing=True
    )
    assert tot["windows"] == 2
    # keepalive trail of lane B + one on-grid trail tick of lane A: one trail
    # tick at sec 360 and one at 390 are present; all other in-window ticks are
    # post_roll evidence for the roll attribution.
    assert tot["post_roll"] == 241 + 240
    assert tot["outage"] == density_split.TICKS_PER_SEC * 30  # secs 200..229 in narration
    assert tot["bes"] > 0  # the sparse all-keepalive trail of lane B
    assert tot["other"] > 0  # partial/absent seconds without narration or evidence
    assert tot["bes"] > 0
    assert tot["other"] > 0
    # disjoint buckets, outage ⊆ absent_row_total
    assert (
        tot["post_roll"] + tot["outage"] + tot["bes"] + tot["other"] == tot["missing"]
    )
    assert tot["absent_row_total"] >= tot["outage"]
    assert [w["missing"] for w in worst] == sorted(
        [w["missing"] for w in worst], reverse=True
    )


def test_density_split_dataless_evidence_bars():
    beat = density_split._MarketState()
    beat.rows = 1000
    beat.null_rows = 1000
    # near-full-grid write market (dense trail) is NOT dataless
    assert not density_split._dataless(beat, "c", set(), 1000 * density_split.TICKS_PER_SEC)
    trail = density_split._MarketState()
    trail.rows = 3
    trail.null_rows = 3
    assert density_split._dataless(trail, "c", set(), 600)  # all-keepalive trail
    trail_half = density_split._MarketState()
    trail_half.rows = 3
    trail_half.null_rows = 2
    assert not density_split._dataless(trail_half, "c", set(), 600)  # live rows exist
    narrated = density_split._MarketState()
    narrated.rows = 5
    narrated.null_rows = 3
    assert density_split._dataless(narrated, "c", {"c"}, 600)  # narration buys in when sparse
    assert density_split._dataless(None, "c", {"c"}, 600)  # narration for a never-row market
    assert not density_split._dataless(None, "c", set(), 600)  # no evidence → not claimed


def test_density_split_scan_hive(tmp_path):
    d = tmp_path / "book_snapshots_500ms" / f"date={DATE}" / "asset=BTC"
    d.mkdir(parents=True)
    base = _sec(10)
    rows = [
        {"condition_id": "c1", "ts_snapshot_ns": base, "up_bid": 0.5, "up_ask": None, "down_bid": None, "down_ask": None},
        {"condition_id": "c1", "ts_snapshot_ns": base + density_split.NS // 2, "up_bid": None, "up_ask": None, "down_bid": None, "down_ask": None},
        {"condition_id": "c2", "ts_snapshot_ns": base + density_split.NS, "up_bid": 0.4, "up_ask": 0.6, "down_bid": None, "down_ask": None},
    ]
    pq.write_table(pa.Table.from_pylist(rows), str(d / "part-000.parquet"))
    pq.write_table(
        pa.Table.from_pylist([{"condition_id": "c9", "ts_snapshot_ns": base, "up_bid": None,
                               "up_ask": None, "down_bid": None, "down_ask": None}]),
        str(d / "part-001.parquet.tmp"),
    )
    state, covered, partial, scanned, total = density_split.scan_hive(
        d, deadline=time.monotonic() + 60
    )
    assert not partial and scanned == 1 and total == 1  # .tmp skipped by glob
    assert set(state) == {"c1", "c2"}
    assert state["c1"].rows == 2 and state["c1"].null_rows == 1
    assert state["c2"].sec_counts[base // density_split.NS + 1] == 1
    assert covered == {base // density_split.NS, base // density_split.NS + 1}


def test_density_split_scan_hive_missing_dir(tmp_path):
    state, covered, partial, scanned, total = density_split.scan_hive(
        tmp_path / "nope", deadline=time.monotonic() + 1
    )
    assert state == {} and covered == set() and not partial and scanned == 0 and total == 0


def test_density_split_scan_events(tmp_path):
    d = tmp_path / "collector_events" / f"date={DATE}"
    d.mkdir(parents=True)
    rows = [
        {"event_type": "coverage_gap", "details": json.dumps(
            {"last_bucket": D0 * 1000, "cur_bucket": (D0 + 60) * 1000})},
        {"event_type": "book_empty_skipped", "details": json.dumps(
            {"asset": "BTC", "condition_id": "c-bes"})},
        {"event_type": "book_empty_skipped", "details": json.dumps(
            {"asset": "ETH", "condition_id": "c-other-lane"})},
        {"event_type": "coverage_gap", "details": "not-json"},
    ]
    pq.write_table(pa.Table.from_pylist(rows), str(d / "part-000.parquet"))
    ranges, bes, partial, read, total = density_split.scan_collector_events(
        d, "BTC", deadline=time.monotonic() + 60
    )
    assert not partial and read == 1 and total == 1
    assert ranges == [(D0 * 1000, (D0 + 60) * 1000)]
    assert bes == {"c-bes"}  # other-lane events never attribute to BTC


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
