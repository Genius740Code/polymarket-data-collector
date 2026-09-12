"""P0 audit fixes (kaggle_null_audit_2026-09-09): E1 market_id hex, E9 chainlink dedup, E2 window_index.

Real-data-only: missing stays NULL, never a sentinel.
"""
import pyarrow as pa
import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.rollover import (
    MarketInfo,
    clean_market_id,
    is_hex_condition_id,
)
from polymarket_collector.storage.parquet_writer import ParquetWriter
from polymarket_collector.storage.schemas import TRADES_SCHEMA, snapshot_schema

HEX_CID = "0x" + "ab" * 32  # 0x + 64 hex


# ---------------------------------------------------------------- E1
def test_e1_clean_market_id_rejects_hex_and_missing():
    assert clean_market_id(None) is None
    assert clean_market_id("") is None
    assert clean_market_id(HEX_CID) is None
    assert clean_market_id("4349753") == "4349753"
    assert clean_market_id(4349753) == "4349753"
    assert clean_market_id("mid-BTC") == "mid-BTC"


def test_e1_is_hex_condition_id():
    assert is_hex_condition_id(HEX_CID) is True
    assert is_hex_condition_id("4349753") is False
    assert is_hex_condition_id(None) is False
    assert is_hex_condition_id("mid-1") is False


def test_e1_market_info_never_hex():
    m = MarketInfo(
        condition_id=HEX_CID, market_id=None, asset="BTC",
        up_token_id="u", down_token_id="d",
        market_start_ts_ms=1, market_end_ts_ms=2,
        window_index=3, series_id="BTC-5m",
    )
    assert m.market_id is None
    # even an explicitly hex market_id must not survive the book layer
    b = OrderBookState(
        asset="BTC", condition_id=HEX_CID, market_id=HEX_CID,
        series_id="BTC-5m", window_index=3,
        up_token_id="u", down_token_id="d", market_end_ts_ms=2,
    )
    assert b.market_id is None
    assert b.snapshot().market_id is None
    assert b.snapshot().to_flat_dict()["market_id"] is None


def test_e1_snapshot_schema_allows_null_market_id():
    schema = snapshot_schema(10)
    assert schema.field("market_id").nullable is True


# ---------------------------------------------------------------- E9
def _chainlink_row(asset="BTC", event_id="e1", price=90000.0, ts=1_000):
    return {
        "ts_source": "2026-09-09T11:20:19Z",
        "ts_received_ns": ts,
        "asset": asset,
        "event_id": event_id,
        "price": price,
        "report_id": None,
    }


def test_e9_chainlink_dedup_null_report_id(tmp_path):
    w = ParquetWriter(data_dir=str(tmp_path), wal_enabled=False)
    row = _chainlink_row()
    key = w._dedup_key("chainlink_events", row)
    assert key == ("BTC", "e1"), f"dedup key must be (asset,event_id), got {key}"
    assert w.append("chainlink_events", dict(row), asset="BTC", date_str="2026-09-09") is True
    # burst duplicate (same asset+event_id, NULL report_id) must be dropped
    # (idempotent True, but no second buffered row)
    assert w.append("chainlink_events", dict(row), asset="BTC", date_str="2026-09-09") is True
    buffered = [b for b in w._buffer if b.dataset == "chainlink_events"]
    assert len(buffered) == 1, f"duplicate must not buffer twice, got {len(buffered)}"
    # distinct event_id still flows
    assert w.append("chainlink_events", dict(_chainlink_row(event_id="e2")), asset="BTC", date_str="2026-09-09") is True


# ---------------------------------------------------------------- E2
def test_e2_trades_window_index_nullable():
    assert TRADES_SCHEMA.field("window_index").nullable is True
    assert TRADES_SCHEMA.field("market_id").nullable is True


def test_e2_export_drops_null_and_zero_window_index(tmp_path):
    from polymarket_collector.storage.export import _read_dataset_per_asset

    base = tmp_path / "data"
    tdir = base / "trades" / "date=2026-09-09" / "asset=BTC"
    tdir.mkdir(parents=True)
    rows = [
        {"ts_source": None, "ts_received_ns": 1, "condition_id": "c1",
         "market_id": "1", "series_id": "BTC-5m", "window_index": 100,
         "asset": "BTC", "trade_id": "t1", "transaction_hash": None,
         "token_id": "tok", "outcome": "up", "price": 0.5, "size": 1.0,
         "notional": 0.5, "fee": None, "fee_is_estimated": None,
         "side": "buy", "aggressor_side": "buy",
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
        {"ts_source": None, "ts_received_ns": 2, "condition_id": "c2",
         "market_id": "2", "series_id": "BTC-5m", "window_index": 0,
         "asset": "BTC", "trade_id": "t2", "transaction_hash": None,
         "token_id": "tok", "outcome": "up", "price": 0.5, "size": 1.0,
         "notional": 0.5, "fee": None, "fee_is_estimated": None,
         "side": "buy", "aggressor_side": "buy",
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
        {"ts_source": None, "ts_received_ns": 3, "condition_id": "c3",
         "market_id": "3", "series_id": "BTC-5m", "window_index": None,
         "asset": "BTC", "trade_id": "t3", "transaction_hash": None,
         "token_id": "tok", "outcome": "up", "price": 0.5, "size": 1.0,
         "notional": 0.5, "fee": None, "fee_is_estimated": None,
         "side": "sell", "aggressor_side": "sell",
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
    ]
    tbl = pa.Table.from_pylist(rows, schema=TRADES_SCHEMA)
    import pyarrow.parquet as pq

    pq.write_table(tbl, str(tdir / "trades_1.parquet"))
    out = _read_dataset_per_asset(base, "trades", "BTC")
    assert out is not None
    got = sorted(r["trade_id"] for r in out.to_pylist())
    assert got == ["t1"], f"only the honest window_index row may ship, got {got}"
