"""P1 audit fixes (kaggle_null_audit_2026-09-09): E5 side casing, E6 book_age, E7 fee.

E3 (clean BBO documentation) is docs-only — no code test.
"""
import time

import pyarrow as pa

from polymarket_collector.book import OrderBookState
from polymarket_collector.storage.parquet_writer import ParquetWriter
from polymarket_collector.storage.schemas import TRADES_SCHEMA


def _book() -> OrderBookState:
    return OrderBookState(
        asset="BTC", condition_id="cid-1", market_id="mid-1",
        series_id="BTC-5m", window_index=1,
        up_token_id="up-tok", down_token_id="down-tok",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
    )


# ---------------------------------------------------------------- E5
def test_e5_writer_normalizes_side_to_lowercase(tmp_path):
    w = ParquetWriter(data_dir=str(tmp_path), wal_enabled=False)
    row = {
        "ts_received_ns": time.time_ns(), "condition_id": "c1", "market_id": "m1",
        "series_id": "BTC-5m", "window_index": 1, "asset": "BTC", "trade_id": "t1",
        "token_id": "tok", "outcome": "up", "price": 0.5, "size": 1.0,
        "side": "BUY", "aggressor_side": "SELL",
    }
    assert w.append("trades", row, asset="BTC", date_str="2026-09-09") is True
    buffered = [b for b in w._buffer if b.dataset == "trades"]
    assert len(buffered) == 1
    assert buffered[0].row["side"] == "buy"
    assert buffered[0].row["aggressor_side"] == "sell"
    # caller's dict must not be mutated
    assert row["side"] == "BUY"


def test_e5_export_read_lowercases_legacy_sides(tmp_path):
    from polymarket_collector.storage.export import _read_dataset_per_asset

    base = tmp_path / "data"
    tdir = base / "trades" / "date=2026-09-09" / "asset=BTC"
    tdir.mkdir(parents=True)
    rows = [{
        "ts_source": None, "ts_received_ns": 1, "condition_id": "c1",
        "market_id": "1", "series_id": "BTC-5m", "window_index": 100,
        "asset": "BTC", "trade_id": "t1", "transaction_hash": None,
        "token_id": "tok", "outcome": "up", "price": 0.5, "size": 1.0,
        "notional": 0.5, "fee": None, "fee_is_estimated": None,
        "side": "BUY", "aggressor_side": "BUY",
        "maker_wallet": None, "taker_wallet": None, "wallet": None,
    }]
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows, schema=TRADES_SCHEMA), str(tdir / "t.parquet"))
    out = _read_dataset_per_asset(base, "trades", "BTC")
    got = out.to_pylist()[0]
    assert got["side"] == "buy" and got["aggressor_side"] == "buy"


# ---------------------------------------------------------------- E6
def test_e6_book_age_none_before_first_update_then_ages():
    b = _book()
    snap0 = b.snapshot()
    assert snap0.up_book_age_ms is None and snap0.down_book_age_ms is None
    # full book frame for the up outcome only
    msg = {
        "bids": [{"price": "0.55", "size": "10"}],
        "asks": [{"price": "0.60", "size": "10"}],
        "token_id": "up-tok",
        "timestamp": str(int(time.time() * 1000)),
        "hash": "h" * 32,
    }
    ok, _ = b.apply_ws_message(msg)
    assert ok
    snap1 = b.snapshot()
    assert snap1.up_book_age_ms is not None and snap1.up_book_age_ms < 5_000
    assert snap1.down_book_age_ms is None  # untouched side stays NULL (honest)
    # a snapshot 8s later must show ~8s of age, not 0
    future_ms = int(time.time() * 1000) + 8_000
    snap2 = b.snapshot(ts_ms=future_ms)
    assert snap2.up_book_age_ms is not None and snap2.up_book_age_ms >= 7_000, (
        f"book age must advance with time, got {snap2.up_book_age_ms}"
    )


# ---------------------------------------------------------------- E7
def test_e7_zero_fee_flag_is_null_collector(tmp_path):
    """CLOB fee_rate_bps=0 → fee 0.0 kept, fee_is_estimated NULL (not False/True)."""
    from types import SimpleNamespace

    from polymarket_collector.collector import Collector
    from polymarket_collector.config import CollectorConfig

    cfg = CollectorConfig()
    cfg.storage.data_dir = str(tmp_path / "data")
    cfg.storage.wal_dir = str(tmp_path / "_wal")
    cfg.raw_archive.path = str(tmp_path / "raw")
    cfg.raw_archive.enabled = False
    cfg.cursor_store.path = str(tmp_path / "cursor")
    cfg.assets = ["BTC"]
    c = Collector(cfg)
    captured = []
    c.writer.append = lambda dataset, row, asset=None, **kw: captured.append((dataset, row)) or True
    c.rollover = SimpleNamespace(active_markets=lambda a: [])

    msg = {
        "type": "last_trade_price", "asset_id": "up-tok", "token_id": "up-tok",
        "price": 0.55, "size": 10.0, "side": "BUY", "fee_rate_bps": "0",
        "timestamp": str(int(time.time() * 1000)),
    }
    assert c._handle_trade_message(msg, "BTC", time.time_ns()) is True
    assert len(captured) == 1
    row = captured[0][1]
    assert row["fee"] == 0.0
    assert row["fee_is_estimated"] is None
    assert row["side"] == "buy" and row["aggressor_side"] == "buy"  # E5 via collector
    assert row["window_index"] is None and row["market_id"] is None  # E1+E2 via collector
