"""Tests for backfill_pmxt: offline fixtures only, no network.

Covers: dry-run needs listing, marked writes that leave live files byte
identical, idempotent re-runs, invalid-row drops, resolution sidecar
isolation, and export_pmdata shape compatibility (pmdata_diff gate path).
"""

import datetime as _dt
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.backfill_pmxt import (
    BACKFILL_SOURCE,
    coerce_ts_source_ms,
    dedup_key,
    enrich_resolutions,
    enrich_trades,
    filter_archive,
    needs_backfill,
    run_backfill,
)
from polymarket_collector.export_pmdata import export_pmdata_layout
from polymarket_collector.pmdata_diff import compare_pmdata

DAY = "2026-09-20"
DAY_START_MS = int(_dt.datetime.fromisoformat(
    f"{DAY}T00:00:00+00:00").timestamp() * 1000)

CID_A = "0x" + "aa" * 32  # has live rows -> never backfilled
CID_B = "0x" + "bb" * 32  # pre-cutover gap -> backfill target
SLUG_A = "btc-updown-5m-1758326400"
SLUG_B = "btc-updown-5m-1758326700"
UP_A, DOWN_A = "1001", "1002"
UP_B, DOWN_B = "2001", "2002"


def _ns(ms: int) -> int:
    return ms * 1_000_000


def _markets_rows():
    return [
        {"condition_id": CID_A, "slug": SLUG_A, "series_id": "BTC-5m",
         "asset": "BTC", "window_index": 1, "window_size_seconds": 300,
         "up_token_id": UP_A, "down_token_id": DOWN_A,
         "market_start_ts_ms": DAY_START_MS + 300_000,
         "market_end_ts_ms": DAY_START_MS + 600_000},
        {"condition_id": CID_B, "slug": SLUG_B, "series_id": "BTC-5m",
         "asset": "BTC", "window_index": 2, "window_size_seconds": 300,
         "up_token_id": UP_B, "down_token_id": DOWN_B,
         "market_start_ts_ms": DAY_START_MS + 600_000,
         "market_end_ts_ms": DAY_START_MS + 900_000},
    ]


def _write(path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path), compression="zstd")


def _write_union(path, rows) -> None:
    """Archive write with a unioned schema (heterogeneous record kinds).

    ``pa.Table.from_pylist`` takes the first row's keys as the schema and
    would silently drop other kinds' columns; real hourly archives carry a
    uniform schema, so the fixture unions keys explicitly (missing -> NULL).
    """
    keys: list = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    full = [{k: r.get(k) for k in keys} for r in rows]
    _write(path, full)


def _sha(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture()
def hive(tmp_path):
    data = tmp_path / "data"
    _write(data / "markets_latest" / "markets_latest.parquet", _markets_rows())
    t0 = DAY_START_MS + 301_000
    _write(data / "trades" / f"date={DAY}" / "asset=BTC" / "live.parquet", [
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC",
         "trade_id": "live-1", "transaction_hash": "0xlive",
         "token_id": UP_A, "outcome": "up", "price": 0.60, "size": 5.0,
         "side": "buy", "ts_source": t0, "ts_received_ns": _ns(t0)},
    ])
    return data


@pytest.fixture()
def archive(tmp_path):
    t1 = DAY_START_MS + 601_000
    rows = [
        # in-scope backfill trade for the gap window (token resolves market).
        {"record_kind": "trades", "token_id": UP_B, "price": 0.62,
         "size": 10.0, "side": "buy", "ts_source": t1,
         "transaction_hash": "0xback1", "trade_id": "pmxt-b1"},
        # duplicate of the live row -> must be skipped, never duplicated.
        {"record_kind": "trades", "condition_id": CID_A, "token_id": UP_A,
         "price": 0.60, "size": 5.0, "side": "buy", "ts_source": t1 - 300_000,
         "transaction_hash": "0xlive", "trade_id": "live-1"},
        # out-of-range price -> dropped and counted, never adjusted.
        {"record_kind": "trades", "token_id": UP_B, "price": 1.62,
         "size": 3.0, "side": "buy", "ts_source": t1 + 1000,
         "trade_id": "pmxt-bad"},
        # book event + snapshot for the gap window.
        {"record_kind": "book_events", "condition_id": CID_B,
         "token_id": UP_B, "event_type": "price_change", "ts_source": t1,
         "event_id": "pmxt-ev-1", "new_best_bid": 0.61,
         "new_best_ask": 0.64},
        {"record_kind": "snapshots", "condition_id": CID_B,
         "ts_snapshot_ns": _ns(t1), "snapshot_id": "pmxt-sn-1",
         "up_bid": 0.61, "up_ask": 0.64},
        # official outcome + strike for the gap window.
        {"condition_id": CID_B, "winner": True,
         "resolution_outcome": "up", "settlement_price": 1.0},
        {"strike_price": 110000.0, "asset": "BTC",
         "window_start_ts_ms": DAY_START_MS + 600_000,
         "window_end_ts_ms": DAY_START_MS + 900_000},
    ]
    p = tmp_path / "pmxt-day.parquet"
    _write_union(p, rows)
    return p


def test_coerce_ts_source_ms_units_and_garbage() -> None:
    assert coerce_ts_source_ms(1_758_326_010_00) == 1_758_326_010_00
    assert coerce_ts_source_ms(1_758_326) == 1_758_326_000  # seconds -> ms
    assert coerce_ts_source_ms("1758326010") == 1758326010000
    assert coerce_ts_source_ms(None) is None
    assert coerce_ts_source_ms(True) is None
    assert coerce_ts_source_ms("not-a-clock") is None
    assert coerce_ts_source_ms(float("nan")) is None


def test_dry_run_lists_gap_window_and_writes_nothing(hive, archive) -> None:
    before = {str(p): _sha(p) for p in hive.rglob("*.parquet")}
    needs = needs_backfill(hive, DAY, "BTC", "5m")
    cids = {w["condition_id"] for w in needs["windows"]}
    assert CID_B in cids and CID_A not in cids
    summary = run_backfill(hive, [archive], day=DAY, asset="BTC",
                           timeframe="5m", dry_run=True)
    assert summary["dry_run"] is True
    assert summary["writes"] == {}
    assert {str(p): _sha(p) for p in hive.rglob("*.parquet")} == before


def test_backfill_writes_marked_rows_without_touching_live(hive, archive) -> None:
    live_path = (hive / "trades" / f"date={DAY}" / "asset=BTC"
                 / "live.parquet")
    markets_path = hive / "markets_latest" / "markets_latest.parquet"
    live_hash, markets_hash = _sha(live_path), _sha(markets_path)
    summary = run_backfill(hive, [archive], day=DAY, asset="BTC",
                           timeframe="5m")
    assert summary.get("error") is None
    assert summary["writes"]["trades"]["wrote"] == 1  # dup + bad row dropped
    assert summary["enrich"]["trades"]["dropped_bad_price"] == 1
    # live files byte-identical; markets_latest untouched (sidecar instead).
    assert _sha(live_path) == live_hash
    assert _sha(markets_path) == markets_hash
    # every new file is clearly marked and carries the source marker.
    new_files = [p for p in hive.rglob("*.parquet")
                 if p.name.startswith("backfill_pmxt-")]
    assert new_files, "no backfilled files written"
    assert not any("live.parquet" in str(p) for p in new_files)
    back_trades = [p for p in new_files
                   if f"date={DAY}" in str(p) and "trades" in str(p)]
    assert back_trades
    rows = pq.read_table(str(back_trades[0])).to_pylist()
    assert len(rows) == 1
    row = rows[0]
    assert row["source"] == BACKFILL_SOURCE
    assert row["condition_id"] == CID_B
    assert row["outcome"] == "up"  # resolved via the market token pair
    assert row["ts_received_ns"] is None  # never received live
    assert row["ts_source"] == DAY_START_MS + 601_000
    # resolution sidecar written, live markets hive untouched.
    sidecars = list((hive / "_backfill_pmxt_resolutions").rglob("*.parquet"))
    assert len(sidecars) == 1
    reso = pq.read_table(str(sidecars[0])).to_pylist()
    assert reso[0]["condition_id"] == CID_B
    assert reso[0]["resolution_outcome"] == "up"
    assert reso[0]["settlement_source"] == "polymarket_official"
    assert reso[0]["source"] == BACKFILL_SOURCE


def test_idempotent_rerun_writes_zero(hive, archive) -> None:
    first = run_backfill(hive, [archive], day=DAY, asset="BTC",
                         timeframe="5m")
    assert first["writes"]["trades"]["wrote"] == 1
    count_files = len(list(hive.rglob("backfill_pmxt-*.parquet")))
    count_rows = sum(pq.read_table(str(p)).num_rows
                     for p in hive.rglob("backfill_pmxt-*.parquet"))
    second = run_backfill(hive, [archive], day=DAY, asset="BTC",
                          timeframe="5m")
    assert second["writes"]["trades"]["wrote"] == 0
    assert second["writes"]["trades"]["dedup"]["dropped_dup_stored"] >= 1
    assert len(list(hive.rglob("backfill_pmxt-*.parquet"))) == count_files
    assert sum(pq.read_table(str(p)).num_rows
               for p in hive.rglob("backfill_pmxt-*.parquet")) == count_rows


def test_enriched_rows_match_export_shape_and_gate(hive, archive, tmp_path) -> None:
    run_backfill(hive, [archive], day=DAY, asset="BTC", timeframe="5m")
    out = tmp_path / "pmdata"
    manifest = export_pmdata_layout(hive, out, "BTC", "5m", DAY)
    assert manifest["totals"]["trades_rows"] == 2  # 1 live + 1 backfilled
    # the live gap remains a live gap (honest), but one backfilled row now
    # covers the window — visible in coverage, separable by source.
    from polymarket_collector.backfill_pmxt import live_condition_coverage
    cov = live_condition_coverage(hive, DAY, "BTC")
    # one backfilled row per tick dataset (trades/book_events/snapshots).
    assert cov[CID_B]["live"] == 0 and cov[CID_B]["backfilled"] == 3
    assert cov[CID_A]["live"] == 1 and cov[CID_A]["backfilled"] == 0
    # pmdata_diff gate path: export vs identical sample passes offline.
    sample = tmp_path / "sample"
    for rel in manifest["files"]:
        src = out / rel
        dst = sample / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(src.read_bytes())
    # pmdata_diff gate path: export vs identical sample passes the numeric
    # gate offline (row-count ±2%, mid-price ≤1 tick). Event-type coverage
    # stays informational here — the tiny fixture carries no
    # last_trade_price/tick_size_change rows on either side.
    report = compare_pmdata(out, sample, SLUG_B)
    assert report["row_count"]["pass"] is True
    assert report["row_count"]["diff_pct"] == 0.0
    assert report["mid_price"]["pass"] is True


def test_filter_archive_day_scope_and_offline(archive) -> None:
    res = filter_archive(archive, day_str=DAY)
    assert res["stats"]["files_ok"] == 1
    assert res["stats"]["rows_read"] == 7
    # clock-bearing rows outside the day are dropped; clock-less rows
    # (resolutions/strikes) pass through — market-day placement decides
    # their partition later, never the filter.
    other = filter_archive(archive, day_str="2026-09-21")
    assert other["rows"] != []
    assert all(r.get("ts_source") is None and r.get("timestamp") is None
               and r.get("ts_snapshot_ns") is None for r in other["rows"])
    missing = filter_archive(archive.parent / "nope.parquet", day_str=DAY)
    assert missing["rows"] == [] and missing["stats"]["files_failed"] == 1


def test_enrich_drops_unresolvable_and_bad_rows() -> None:
    kept, stats = enrich_trades(
        [{"token_id": "9999", "price": 0.5, "size": 1.0,
          "ts_source": DAY_START_MS + 1, "trade_id": "x"}],
        {}, {})
    assert kept == [] and stats["dropped_no_market"] == 1
    assert dedup_key({"trade_id": "t"}, "trades") == ("trade_id", "t")
    assert dedup_key({}, "trades") is None
    reso_rows, reso_stats = enrich_resolutions(
        [{"condition_id": CID_B, "resolution_outcome": "sideways"}],
        {CID_B: {"asset": "BTC", "slug": SLUG_B, "series_id": "BTC-5m",
                 "window_index": 2}})
    assert reso_rows == [] and reso_stats["dropped_no_outcome"] == 1
