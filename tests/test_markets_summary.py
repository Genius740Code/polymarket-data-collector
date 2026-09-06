"""markets_summary derived export — one row per condition_id (39th staging file)."""
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage.export import (
    build_markets_summary,
    export_per_asset_single_file,
    _load_markets_latest_rows,
)
from polymarket_collector.storage.schemas import (
    MARKETS_SCHEMA,
    MARKETS_SUMMARY_SCHEMA,
    TRADES_SCHEMA,
    CHAINLINK_SCHEMA,
)


def _write_rows(base: Path, dataset: str, asset: str, date: str, rows: list, schema: pa.Schema):
    d = base / dataset / f"date={date}" / f"asset={asset}"
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), str(d / "part_0.parquet"), compression="zstd")


@pytest.fixture
def hive(tmp_path) -> Path:
    base = tmp_path / "data"
    base.mkdir()
    # two markets: one resolved up, one unresolved
    markets = [
        {
            "updated_at": "2026-09-05T20:40:00Z", "recorded_at": None,
            "market_start_ts": "2026-09-05T20:35:00Z", "market_end_ts": "2026-09-05T20:40:00Z",
            "market_start_ts_ms": 1788640500000, "market_end_ts_ms": 1788640800000,
            "resolution_ts": None, "condition_id": "0xAAA", "market_id": "1",
            "slug": "btc-updown-5m-1788640500", "series_id": "BTC-5m", "window_index": 1,
            "window_label": "5m", "window_size_seconds": 300, "asset": "BTC",
            "up_token_id": "UP1", "down_token_id": "DN1", "status": "resolved",
            "resolution_outcome": "up", "question": None, "tick_size": 0.01,
            "minimum_order_size": None, "minimum_notional": None,
            "reported_volume": None, "reported_liquidity": None,
            "settlement_price": 1.0,
            "settlement_ts_utc": None,
            "resolution_confirmed_at": None, "settlement_source": "polymarket_official",
        },
        {
            "updated_at": "2026-09-05T20:46:00Z", "recorded_at": None,
            "market_start_ts": "2026-09-05T20:40:00Z", "market_end_ts": "2026-09-05T20:45:00Z",
            "market_start_ts_ms": 1788640800000, "market_end_ts_ms": 1788641100000,
            "resolution_ts": None, "condition_id": "0xBBB", "market_id": "2",
            "slug": "eth-updown-5m-1788640800", "series_id": "ETH-5m", "window_index": 2,
            "window_label": "5m", "window_size_seconds": 300, "asset": "ETH",
            "up_token_id": "UP2", "down_token_id": "DN2", "status": "active",
            "resolution_outcome": "unknown", "question": None, "tick_size": 0.01,
            "minimum_order_size": None, "minimum_notional": None,
            "reported_volume": None, "reported_liquidity": None,
            "settlement_price": None,
            "settlement_ts_utc": None,
            "resolution_confirmed_at": None, "settlement_source": None,
        },
    ]
    # markets_latest single file
    ml = base / "markets_latest"
    ml.mkdir()
    pq.write_table(pa.Table.from_pylist(markets, schema=MARKETS_SCHEMA), str(ml / "markets_latest.parquet"))

    # clean snapshots: BTC market — up mid 0.50 → 0.90, down mid 0.50 → 0.10
    snaps = []
    for i, (ub, ua, db, da) in enumerate([(0.49, 0.51, 0.49, 0.51), (0.60, 0.62, 0.38, 0.40), (0.89, 0.91, 0.09, 0.11)]):
        snaps.append({
            "ts_snapshot_utc": f"2026-09-05T20:3{i}:00Z", "ts_snapshot_ns": 1788640500000000000 + i * 500_000_000,
            "condition_id": "0xAAA", "market_id": "1", "series_id": "BTC-5m", "window_index": 1,
            "asset": "BTC", "snapshot_id": f"s{i}", "up_token_id": "UP1", "down_token_id": "DN1",
            "up_bid": ub, "up_ask": ua, "up_bid_size": 10.0, "up_ask_size": 10.0,
            "down_bid": db, "down_ask": da, "down_bid_size": 10.0, "down_ask_size": 10.0,
            "market_time_remaining_ms": 0, "up_book_age_ms": None, "down_book_age_ms": None,
            "is_rollover_window": False, "book_state": "live", "resync_id": None, "book_crossed": False,
        })
    _write_rows(base, "book_snapshots_clean", "BTC", "2026-09-05", snaps, None)  # schema set below

    # chainlink ticks: one at window start +100ms, one at end +100ms
    cl = [
        {"ts_source": "2026-09-05T20:35:00Z", "ts_received_ns": 1, "asset": "BTC",
         "event_id": "e1", "symbol": "btc/usd", "source": "chainlink_rtds", "price": 80000.0, "report_id": None},
        {"ts_source": "2026-09-05T20:40:00Z", "ts_received_ns": 2, "asset": "BTC",
         "event_id": "e2", "symbol": "btc/usd", "source": "chainlink_rtds", "price": 80100.0, "report_id": None},
        # ETH window start tick is 60s away — beyond the 5s tolerance, must stay NULL
        {"ts_source": "2026-09-05T20:41:00Z", "ts_received_ns": 3, "asset": "ETH",
         "event_id": "e3", "symbol": "eth/usd", "source": "chainlink_rtds", "price": 2500.0, "report_id": None},
    ]
    d = base / "chainlink_events" / "date=2026-09-05" / "asset=BTC"
    d.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(cl[:2], schema=CHAINLINK_SCHEMA), str(d / "part_0.parquet"))
    d2 = base / "chainlink_events" / "date=2026-09-05" / "asset=ETH"
    d2.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(cl[2:], schema=CHAINLINK_SCHEMA), str(d2 / "part_0.parquet"))

    # trades: two fills from trader W1, one from W2 (notional = price*size)
    trades = [
        {"ts_source": "2026-09-05T20:36:00Z", "ts_received_ns": 1, "condition_id": "0xAAA", "market_id": "1",
         "series_id": "BTC-5m", "window_index": 1, "asset": "BTC", "trade_id": "t1",
         "transaction_hash": "0xh1", "token_id": "UP1", "outcome": "up", "price": 0.5, "size": 10.0,
         "notional": 5.0, "fee": None, "fee_is_estimated": None, "side": "buy", "aggressor_side": "buy",
         "maker_wallet": "0xM1", "taker_wallet": "0xW1", "wallet": "0xW1"},
        {"ts_source": "2026-09-05T20:37:00Z", "ts_received_ns": 2, "condition_id": "0xAAA", "market_id": "1",
         "series_id": "BTC-5m", "window_index": 1, "asset": "BTC", "trade_id": "t2",
         "transaction_hash": "0xh2", "token_id": "UP1", "outcome": "up", "price": 0.6, "size": 10.0,
         "notional": 6.0, "fee": None, "fee_is_estimated": None, "side": "sell", "aggressor_side": "sell",
         "maker_wallet": "0xW1", "taker_wallet": "0xM2", "wallet": "0xM2"},
        {"ts_source": "2026-09-05T20:38:00Z", "ts_received_ns": 3, "condition_id": "0xAAA", "market_id": "1",
         "series_id": "BTC-5m", "window_index": 1, "asset": "BTC", "trade_id": "t3",
         "transaction_hash": "0xh3", "token_id": "DN1", "outcome": "down", "price": 0.4, "size": 10.0,
         "notional": 4.0, "fee": None, "fee_is_estimated": None, "side": "buy", "aggressor_side": "buy",
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
    ]
    d3 = base / "trades" / "date=2026-09-05" / "asset=BTC"
    d3.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(trades, schema=TRADES_SCHEMA), str(d3 / "part_0.parquet"))
    return base


def test_build_markets_summary_values(hive: Path):
    t = build_markets_summary(hive)
    assert t.schema.names == MARKETS_SUMMARY_SCHEMA.names
    rows = {r["condition_id"]: r for r in t.to_pylist()}
    assert set(rows) == {"0xAAA", "0xBBB"}

    a = rows["0xAAA"]
    assert a["asset"] == "BTC" and a["slug"] == "btc-updown-5m-1788640500"
    assert a["resolution_outcome"] == "up" and a["settlement_price"] == 1.0
    assert a["settlement_source"] == "polymarket_official"
    # trades: 3 fills, volume 5+6+4=15, 2 distinct non-null wallets (null never counted)
    assert a["fill_count"] == 3
    assert a["traded_volume"] == pytest.approx(15.0)
    assert a["unique_traders"] == 2
    # up mid: first (0.49+0.51)/2=0.50, last (0.89+0.91)/2=0.90, high 0.90, low 0.50
    assert a["up_open"] == pytest.approx(0.50)
    assert a["up_close"] == pytest.approx(0.90)
    assert a["up_high"] == pytest.approx(0.90)
    assert a["up_low"] == pytest.approx(0.50)
    # down mid: last (0.09+0.11)/2=0.10
    assert a["down_close"] == pytest.approx(0.10)
    # spread: mean of (0.02, 0.02, 0.02) = 0.02
    assert a["avg_spread_up"] == pytest.approx(0.02)
    assert a["snapshot_count"] == 3
    # chainlink: nearest tick within tolerance of both boundaries
    assert a["underlying_open"] == pytest.approx(80000.0)
    assert a["underlying_close"] == pytest.approx(80100.0)
    assert a["underlying_open_ts_utc"] == "2026-09-05T20:35:00Z"
    # applied tolerances recorded per row (open 10s per K-2, close 5s)
    assert a["underlying_open_tolerance_s"] == 10
    assert a["underlying_close_tolerance_s"] == 5

    b = rows["0xBBB"]
    # unresolved market: resolution fields null, no snapshots/trades → nulls, never zeros
    assert b["resolution_outcome"] == "unknown" and b["settlement_price"] is None
    assert b["fill_count"] is None and b["traded_volume"] is None
    assert b["up_open"] is None and b["snapshot_count"] is None
    # chainlink tick 60s away from window start — beyond the 10s open tolerance → NULL (honest gap)
    assert b["underlying_open"] is None


def test_summary_sorted_and_monotonic_guard(hive: Path):
    t = build_markets_summary(hive)
    starts = [r["window_start_ts_ms"] for r in t.to_pylist()]
    assert starts == sorted(starts)

    # export writes the file; a re-export with fewer markets must not shrink it
    out = hive / "staging"
    stats = export_per_asset_single_file(hive, out_dir=out, datasets=["markets_summary"], assets=["BTC"])
    assert stats[str((out / "markets_summary.parquet").relative_to(hive))] == 2
    # now wipe the hive sources — monotonic guard must keep the prior file
    for p in (hive / "markets_latest").glob("*.parquet"):
        p.unlink()
    stats2 = export_per_asset_single_file(hive, out_dir=out, datasets=["markets_summary"], assets=["BTC"])
    assert stats2[str((out / "markets_summary.parquet").relative_to(hive))] == 2
    assert pq.read_table(out / "markets_summary.parquet").num_rows == 2


def test_staging_includes_summary_file(hive: Path):
    out = hive / "staging"
    export_per_asset_single_file(hive, out_dir=out, datasets=["book_snapshots_500ms", "trades", "book_events", "chainlink_events", "markets_log", "collector_events", "resync_episodes", "markets_summary"], assets=["BTC"])
    assert (out / "markets_summary.parquet").exists()
