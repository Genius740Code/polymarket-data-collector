"""Tests for export_pmdata (spec checkboxes 5-6): per-slug layout, ZIP, YES-only, nulls, manifest."""

import datetime as _dt
import hashlib
import json
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.export_pmdata import export_pmdata_layout
from polymarket_collector.onchain import ORDERFILLED_V2_TOPIC, parse_order_filled_fills
from polymarket_collector.onchain import onchain_rows_from_fills
from polymarket_collector.storage.schemas import ONCHAIN_FILLS_SCHEMA, SCHEMAS

DAY = "2026-09-25"
DAY_START_MS = int(_dt.datetime.fromisoformat(f"{DAY}T00:00:00+00:00").timestamp() * 1000)

CID_A = "0x" + "aa" * 32
CID_B = "0x" + "bb" * 32
CID_C = "0x" + "cc" * 32  # BTC-15m lane (must be filtered out of the 5m export)
SLUG_A = "btc-updown-5m-1758758700"
SLUG_B = "btc-updown-5m-1758759000"
UP_A, DOWN_A = "1001", "1002"
UP_B, DOWN_B = "2001", "2002"


def _ns(ms: int) -> int:
    return ms * 1_000_000


def _markets_rows():
    win_a = (DAY_START_MS + 300_000, DAY_START_MS + 600_000)
    win_b = (DAY_START_MS + 600_000, DAY_START_MS + 900_000)
    return [
        {"condition_id": CID_A, "slug": SLUG_A, "series_id": "BTC-5m", "asset": "BTC",
         "window_size_seconds": 300, "up_token_id": UP_A, "down_token_id": DOWN_A,
         "market_start_ts_ms": win_a[0], "market_end_ts_ms": win_a[1]},
        {"condition_id": CID_B, "slug": SLUG_B, "series_id": "BTC-5m", "asset": "BTC",
         "window_size_seconds": 300, "up_token_id": UP_B, "down_token_id": DOWN_B,
         "market_start_ts_ms": win_b[0], "market_end_ts_ms": win_b[1]},
        {"condition_id": CID_C, "slug": "btc-updown-15m-1758758400", "series_id": "BTC-15m",
         "asset": "BTC", "window_size_seconds": 900, "up_token_id": "3001",
         "down_token_id": "3002", "market_start_ts_ms": DAY_START_MS,
         "market_end_ts_ms": DAY_START_MS + 900_000},
    ]


def _write(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path), compression="zstd")


@pytest.fixture()
def hive(tmp_path: Path) -> Path:
    data = tmp_path / "data"
    _write(data / "markets_latest" / "markets_latest.parquet", _markets_rows())
    t0 = DAY_START_MS + 301_000
    snaps = [
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC",
         "ts_snapshot_ns": _ns(t0), "ts_snapshot_utc": "2026-09-25T00:05:01.000Z",
         "up_ask_level_1_price": 0.65, "up_ask_level_1_size": 100.0,
         "up_ask_level_2_price": 0.66, "up_ask_level_2_size": 50.0,
         "up_bid_level_1_price": 0.60, "up_bid_level_1_size": 120.0,
         "up_bid_level_2_price": 0.59, "up_bid_level_2_size": 60.0,
         "down_bid": 0.35, "down_ask": 0.40},
        # empty YES-bid side: honest NULLs, never 0.
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC",
         "ts_snapshot_ns": _ns(t0 + 500), "ts_snapshot_utc": "2026-09-25T00:05:01.500Z",
         "up_ask_level_1_price": 0.70, "up_ask_level_1_size": 80.0,
         "up_bid_level_1_price": None, "up_bid_level_1_size": None,
         "down_bid": None, "down_ask": 0.30},
        {"condition_id": CID_B, "series_id": "BTC-5m", "asset": "BTC",
         "ts_snapshot_ns": _ns(t0), "ts_snapshot_utc": "2026-09-25T00:05:01.000Z",
         "up_ask_level_1_price": 0.55, "up_ask_level_1_size": 200.0,
         "up_bid_level_1_price": 0.50, "up_bid_level_1_size": 210.0,
         "down_bid": 0.45, "down_ask": 0.50},
        # other lane: filtered, counted.
        {"condition_id": CID_C, "series_id": "BTC-15m", "asset": "BTC",
         "ts_snapshot_ns": _ns(t0), "ts_snapshot_utc": "2026-09-25T00:05:01.000Z",
         "up_ask_level_1_price": 0.51, "up_ask_level_1_size": 10.0,
         "up_bid_level_1_price": 0.49, "up_bid_level_1_size": 10.0},
    ]
    _write(data / "book_snapshots_500ms" / f"date={DAY}" / "asset=BTC" / "snap.parquet", snaps)
    events = [
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC", "outcome": "up",
         "event_type": "price_change", "ts_source": t0 + 10, "ts_received_ns": _ns(t0 + 10) + 5000,
         "new_best_bid": 0.61, "new_best_ask": 0.64,
         "new_bid_size": 110.0, "new_ask_size": 90.0},
        # wire clock absent: timestamp must stay NULL (no received-time fallback).
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC", "outcome": "up",
         "event_type": "price_change", "ts_source": None, "ts_received_ns": _ns(t0 + 20),
         "new_best_bid": 0.62, "new_best_ask": 0.63,
         "new_bid_size": 100.0, "new_ask_size": 95.0},
        # NO-side row: excluded by YES-only normalization.
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC", "outcome": "down",
         "event_type": "price_change", "ts_source": t0 + 30, "ts_received_ns": _ns(t0 + 30),
         "new_best_bid": 0.36, "new_best_ask": 0.39,
         "new_bid_size": 100.0, "new_ask_size": 100.0},
        {"condition_id": CID_B, "series_id": "BTC-5m", "asset": "BTC", "outcome": "up",
         "event_type": "price_change", "ts_source": t0 + 40, "ts_received_ns": _ns(t0 + 40),
         "new_best_bid": 0.51, "new_best_ask": 0.54,
         "new_bid_size": 200.0, "new_ask_size": 190.0},
        # unresolvable market: honest gap, counted.
        {"condition_id": None, "series_id": "BTC-5m", "asset": "BTC", "outcome": "up",
         "event_type": "price_change", "ts_source": t0 + 50, "ts_received_ns": _ns(t0 + 50),
         "new_best_bid": 0.5, "new_best_ask": 0.55,
         "new_bid_size": 1.0, "new_ask_size": 1.0},
    ]
    _write(data / "book_events" / f"date={DAY}" / "asset=BTC" / "ev.parquet", events)
    trades = [
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC", "outcome": "up",
         "token_id": UP_A, "trade_id": "t1", "transaction_hash": "0xabc", "price": 0.62,
         "size": 10.0, "fee": None, "side": "buy",
         "ts_source": t0 + 100, "ts_received_ns": _ns(t0 + 100)},
        # reconciled row: never received live -> local stays NULL.
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC", "outcome": "up",
         "token_id": UP_A, "trade_id": "api-1", "transaction_hash": "0xdef", "price": 0.63,
         "size": 5.0, "fee": None, "side": "buy", "source": "api_reconciled",
         "ts_source": t0 + 200, "ts_received_ns": None, "ts_backfilled_ns": _ns(t0 + 200)},
        {"condition_id": CID_B, "series_id": "BTC-5m", "asset": "BTC", "outcome": "down",
         "token_id": DOWN_B, "trade_id": "t2", "transaction_hash": "0xghi", "price": 0.46,
         "size": 7.0, "fee": None, "side": "BUY",  # legacy upper -> normalized lower
         "ts_source": t0 + 300, "ts_received_ns": _ns(t0 + 300)},
    ]
    _write(data / "trades" / f"date={DAY}" / "asset=BTC" / "tr.parquet", trades)
    chainlink = [
        {"asset": "BTC", "event_id": "c1", "price": 110000.0,
         "ts_source": t0, "ts_received_ns": _ns(t0)},
        {"asset": "BTC", "event_id": "c2", "price": 110010.0,
         "ts_source": t0 + 1000, "ts_received_ns": _ns(t0 + 1000)},
    ]
    _write(data / "chainlink_events" / f"date={DAY}" / "asset=BTC" / "cl.parquet", chainlink)
    return data


@pytest.fixture()
def exported(hive: Path, tmp_path: Path) -> tuple:
    out = tmp_path / "pmdata"
    manifest = export_pmdata_layout(hive, out, "BTC", "5m", DAY)
    return out, manifest


def test_per_slug_round_trip(exported: tuple) -> None:
    out, _ = exported
    for slug in (SLUG_A, SLUG_B):
        for kind in ("l2", "trades"):
            p = out / kind / f"{slug}.parquet"
            assert p.exists(), f"missing {kind}/{slug}"
            t = pq.read_table(str(p))
            assert t.num_rows > 0
    l2 = pq.read_table(str(out / "l2" / f"{SLUG_A}.parquet")).to_pylist()
    assert {r["event_type"] for r in l2} >= {"snapshot", "price_change"}
    tr = pq.read_table(str(out / "trades" / f"{SLUG_A}.parquet")).to_pylist()
    assert len(tr) == 2 and all(r["event_type"] == "trade" for r in tr)
    # 15m lane market must not leak into the 5m export.
    assert not (out / "l2" / "btc-updown-15m-1758758400.parquet").exists()


def test_zip_contains_expected_slugs(exported: tuple) -> None:
    out, manifest = exported
    zp = out / "BTC-5m.zip"
    assert zp.exists()
    names = set(zipfile.ZipFile(str(zp)).namelist())
    for slug in (SLUG_A, SLUG_B):
        assert f"l2/{slug}.parquet" in names
        assert f"trades/{slug}.parquet" in names
    assert manifest["zip"] is not None
    assert manifest["zip"]["members"] == len(manifest["files"])
    assert manifest["zip"]["path"] == "BTC-5m.zip"


def test_yes_only_math(exported: tuple) -> None:
    out, manifest = exported
    rows = pq.read_table(str(out / "l2" / f"{SLUG_A}.parquet")).to_pylist()
    t0 = DAY_START_MS + 301_000
    snap = next(r for r in rows if r["event_type"] == "snapshot" and r["timestamp"] == t0)
    # YES (Up) side exported verbatim, best-first full depth.
    assert snap["ask_prices"] == [0.65, 0.66]
    assert snap["ask_sizes"] == [100.0, 50.0]
    assert snap["bid_prices"] == [0.60, 0.59]
    # NO side is the documented complement, not a stored column.
    assert snap["bid_prices"] is not None and abs((1 - snap["ask_prices"][0]) - 0.35) < 1e-9
    assert abs((1 - snap["bid_prices"][0]) - 0.40) < 1e-9
    # NO-side book_events are excluded (counted, never stored).
    assert manifest["skipped"]["skipped_down_events"] == 1
    for r in rows:
        for col in ("ask_prices", "bid_prices"):
            assert r[col] is None or 0.39 not in list(r[col])


def test_null_not_zero(exported: tuple) -> None:
    out, _ = exported
    rows = pq.read_table(str(out / "l2" / f"{SLUG_A}.parquet")).to_pylist()
    t0 = DAY_START_MS + 301_000
    thin = next(r for r in rows if r["event_type"] == "snapshot" and r["timestamp"] == t0 + 500)
    assert thin["bid_prices"] is None and thin["bid_sizes"] is None, "empty side must be NULL"
    assert thin["ask_prices"] == [0.70]
    # no fabricated zeros anywhere in the exported depth.
    for slug in (SLUG_A, SLUG_B):
        for r in pq.read_table(str(out / "l2" / f"{slug}.parquet")).to_pylist():
            for col in ("ask_prices", "ask_sizes", "bid_prices", "bid_sizes"):
                assert r[col] is None or 0.0 not in list(r[col]), f"0-guess in {col}"


def test_manifest_counts_match(exported: tuple) -> None:
    out, manifest = exported
    total = 0
    for rel, info in manifest["files"].items():
        p = out / rel
        assert p.exists()
        n = pq.read_table(str(p)).num_rows
        assert n == info["rows"], f"{rel} manifest count mismatch"
        assert hashlib.sha256(p.read_bytes()).hexdigest() == info["sha256"]
        total += n
    assert total == (manifest["totals"]["l2_rows"] + manifest["totals"]["trades_rows"]
                     + manifest["totals"]["onchain_rows"])
    assert manifest["reads"]["chainlink_events"]["rows_kept"] == 2
    assert manifest["reads"]["book_snapshots_500ms"]["rows_kept"] == 3  # 15m row filtered
    assert manifest["asset"] == "BTC" and manifest["timeframe"] == "5m" and manifest["date"] == DAY
    # manifest.json on disk matches the returned dict (minus generation time drift).
    on_disk = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert on_disk["files"] == manifest["files"]
    assert on_disk["totals"] == manifest["totals"]


def test_timestamp_policy_null_never_fallback(exported: tuple) -> None:
    out, _ = exported
    rows = pq.read_table(str(out / "l2" / f"{SLUG_A}.parquet")).to_pylist()
    t0 = DAY_START_MS + 301_000
    clockless = [r for r in rows if r["event_type"] == "price_change" and r["timestamp"] is None]
    assert len(clockless) == 1, "wire-absent row must keep timestamp NULL"
    assert clockless[0]["local_timestamp"] == _ns(t0 + 20), "local keeps the collector clock"
    tr = pq.read_table(str(out / "trades" / f"{SLUG_A}.parquet")).to_pylist()
    rec = next(r for r in tr if r["trade_id"] == "api-1")
    assert rec["local_timestamp"] is None, "reconciled rows were never received live"
    assert rec["timestamp"] == t0 + 200
    b2 = next(r for r in pq.read_table(str(out / "trades" / f"{SLUG_B}.parquet")).to_pylist())
    assert b2["side"] == "buy", "legacy uppercase sides normalize to lower"


def test_onchain_schema_and_helper() -> None:
    assert SCHEMAS["onchain_fills"] is ONCHAIN_FILLS_SCHEMA
    assert [f.name for f in ONCHAIN_FILLS_SCHEMA] == [
        "tx_hash", "token_id", "condition_id", "maker", "taker",
        "price", "size", "fee", "side", "exchange_version", "builder",
    ]

    def _word(v: int) -> str:
        return format(v, "064x")

    def _addr(a: str) -> str:
        return "0x" + "00" * 12 + a.lower().removeprefix("0x")

    maker, taker = "0x" + "01" * 20, "0x" + "02" * 20
    logs = [{
        "transactionHash": "0xFILL",
        "topics": [ORDERFILLED_V2_TOPIC, "0x" + "11" * 32, _addr(maker), _addr(taker)],
        "data": "0x" + _word(0) + _word(int(UP_A)) + _word(100) + _word(50) + _word(1) + _word(0) + _word(0),
    }]
    fills = parse_order_filled_fills(logs)
    assert fills[0]["exchange_version"] == "v2"
    rows = onchain_rows_from_fills(fills, {UP_A: CID_A})
    assert len(rows) == 1
    r = rows[0]
    assert r["condition_id"] == CID_A and r["side"] == "buy"
    assert r["maker"] == maker and r["taker"] == taker
    # not on the current decode: NULL, never 0-guessed.
    assert r["price"] is None and r["size"] is None and r["fee"] is None and r["builder"] is None
    # unresolvable token stays NULL (never guessed).
    assert onchain_rows_from_fills(fills, {})[0]["condition_id"] is None
    # schema round-trip.
    t = pa.Table.from_pylist(rows, schema=ONCHAIN_FILLS_SCHEMA)
    assert t.schema.equals(ONCHAIN_FILLS_SCHEMA)


def test_onchain_extra_fills_grouped_per_slug(hive: Path, tmp_path: Path) -> None:
    def _word(v: int) -> str:
        return format(v, "064x")

    def _addr(a: str) -> str:
        return "0x" + "00" * 12 + a.lower().removeprefix("0x")

    logs = [{
        "transactionHash": "0xFILL",
        "topics": [ORDERFILLED_V2_TOPIC, "0x" + "11" * 32,
                   _addr("0x" + "01" * 20), _addr("0x" + "02" * 20)],
        "data": "0x" + _word(1) + _word(int(UP_A)) + _word(100) + _word(50) + _word(1) + _word(0) + _word(0),
    }]
    fills = parse_order_filled_fills(logs)
    out = tmp_path / "pmdata2"
    manifest = export_pmdata_layout(hive, out, "BTC", "5m", DAY, extra_fills=fills)
    p = out / "onchain_fills" / f"{SLUG_A}.parquet"
    assert p.exists()
    rows = pq.read_table(str(p)).to_pylist()
    assert len(rows) == 1 and rows[0]["side"] == "sell"
    assert rows[0]["exchange_version"] == "v2"
    assert manifest["totals"]["onchain_rows"] == 1
    assert f"onchain_fills/{SLUG_A}.parquet" in set(zipfile.ZipFile(str(out / "BTC-5m.zip")).namelist())


def test_flat_layout_asset_filter(tmp_path: Path) -> None:
    data = tmp_path / "flat"
    _write(data / "markets_latest" / "markets_latest.parquet", _markets_rows())
    t0 = DAY_START_MS + 301_000
    _write(data / "book_snapshots_500ms" / "part.parquet", [
        {"condition_id": CID_A, "series_id": "BTC-5m", "asset": "BTC",
         "ts_snapshot_ns": _ns(t0),
         "up_ask_level_1_price": 0.65, "up_ask_level_1_size": 10.0,
         "up_bid_level_1_price": 0.60, "up_bid_level_1_size": 10.0},
        {"condition_id": "0x" + "ee" * 32, "series_id": "ETH-5m", "asset": "ETH",
         "ts_snapshot_ns": _ns(t0),
         "up_ask_level_1_price": 0.55, "up_ask_level_1_size": 10.0,
         "up_bid_level_1_price": 0.50, "up_bid_level_1_size": 10.0},
    ])
    out = tmp_path / "pmflat"
    manifest = export_pmdata_layout(data, out, "BTC", "5m", DAY)
    assert manifest["totals"]["l2_rows"] == 1
    assert manifest["skipped"].get("skipped_other_asset", 0) >= 1
