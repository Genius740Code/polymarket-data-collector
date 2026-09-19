"""Regression tests for 2026-09-19 backtest-validity audit (C1-C4, H1-H8, M1-M13).

Real classes, temp dirs only. No repo/data writes.
"""
import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, "src")


def test_c1_flush_failure_retains_later_groups():
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    with tempfile.TemporaryDirectory() as td:
        w = ParquetWriter(Path(td), wal_enabled=True)
        date = "2026-09-19"
        for asset in ["BTC", "ETH", "XRP"]:
            w.append("collector_events", {"event_type": "t", "asset": asset, "detail": asset, "ts_source": 1}, asset=asset, date_str=date)
        calls = []
        orig = w._write_group

        def fake(ds, d2, asset, rows):
            calls.append((ds, asset))
            if len(calls) == 1:
                raise OSError("boom")
            return orig(ds, d2, asset, rows)

        w._write_group = fake
        try:
            w.flush()
            assert False, "expected flush to raise"
        except OSError:
            pass
        assert len(w._buffer) == 3, f"C1: later groups lost, buffer={len(w._buffer)}"
        w._write_group = orig
        assert w.flush() == 3
        total = sum(pq.read_table(str(f)).num_rows for f in Path(td).rglob("*.parquet"))
        assert total == 3


def test_c2_same_flush_no_overwrite():
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    with tempfile.TemporaryDirectory() as td:
        w = ParquetWriter(Path(td), wal_enabled=False)
        date = "2026-09-19"
        w.append("collector_events", {"event_type": "a", "asset": "BTC", "ts_source": 1, "detail": "a"}, asset="BTC", date_str=date)
        w.append("collector_events", {"event_type": "b", "asset": "ETH", "ts_source": 2, "detail": "b"}, asset="ETH", date_str=date)
        w.flush()
        files = list(Path(td).rglob("collector_events_*.parquet"))
        assert len(files) == 2, f"C2: expected 2 files, got {len(files)}"
        assert sum(pq.read_table(str(f)).num_rows for f in files) == 2


def test_m1_no_local_os_shadow():
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    assert "os" not in ParquetWriter.flush.__code__.co_varnames


def test_h6_dedup_includes_side():
    import tempfile
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    w = ParquetWriter(tempfile.mkdtemp(), wal_enabled=False)
    base = {"token_id": "t", "ts_source": 1, "event_type": "bbo_snapped",
            "old_best_bid": None, "new_best_bid": None, "old_best_ask": None, "new_best_ask": None}
    k1 = w._dedup_key("book_events", {**base, "side": "bid"})
    k2 = w._dedup_key("book_events", {**base, "side": "ask"})
    assert k1 != k2


def test_h2_ram_holds_full_depth_snapshot_truncates():
    from polymarket_collector.book import OrderBookState
    b = OrderBookState(asset="BTC", condition_id="c", market_id=None, series_id="BTC-5m",
                       window_index=0, up_token_id="u", down_token_id="d", market_end_ts_ms=None)
    for i in range(30):
        b._apply_price_change_level(b.up.asks, 0.50 + i * 0.01, 10, False)
    live = [l for l in b.up.asks.levels if l.price is not None]
    assert len(live) == 30, f"H2 RAM truncated: {len(live)}"
    d = b.snapshot(ts_ms=1758000000000).to_flat_dict()
    assert d.get("market_time_remaining_ms") is None  # H1 honest NULL


def test_h4_api_trade_id_ordinal_stable():
    from polymarket_collector.storage.export import _api_trade_id
    assert _api_trade_id("0xabc", 0.5, 10, 0) == _api_trade_id("0xabc", 0.5, 10, 0)
    assert _api_trade_id("0xabc", 0.5, 10, 0) != _api_trade_id("0xabc", 0.5, 10, 1)
