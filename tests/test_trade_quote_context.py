"""Trade quote context (3.4.0) — live fills carry the contemporaneous RAM-book
BBO + book_state; missing books and reconciled fills stay NULL."""
import time
from types import SimpleNamespace

from polymarket_collector.book import OrderBookState
from polymarket_collector.storage.schemas import TRADES_SCHEMA


def _book(**kw):
    base = dict(asset="BTC", condition_id="0xabc", market_id=None,
                series_id="BTC-5m", window_index=1, up_token_id="tok-xyz",
                down_token_id="tok-down", market_end_ts_ms=int(time.time() * 1000) + 300_000)
    base.update(kw)
    return OrderBookState(**base)


def _collector(tmp_path):
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
    return c, captured


def _trade_msg(**kw):
    m = {"type": "last_trade_price", "asset_id": "tok-xyz",
         "price": 0.55, "size": 10.0, "side": "BUY",
         "timestamp": str(int(time.time() * 1000))}
    m.update(kw)
    return m


def _market():
    return SimpleNamespace(condition_id="0xabc", market_id=None, series_id="BTC-5m",
                           window_index=1, up_token_id="tok-xyz", down_token_id="tok-down")


def test_schema_has_nullable_quote_cols():
    for col in ("up_bid", "up_ask", "up_bid_size", "up_ask_size",
                "down_bid", "down_ask", "down_bid_size", "down_ask_size",
                "quote_book_state"):
        assert col in TRADES_SCHEMA.names
        assert TRADES_SCHEMA.field(col).nullable is True


def test_live_trade_stamps_ram_bbo(tmp_path):
    c, captured = _collector(tmp_path)
    c.markets["0xabc"] = _market()
    b = _book()
    b.replace_from_rest_snapshot({
        "up_bids": [["0.54", "100"]], "up_asks": [["0.56", "80"]],
        "down_bids": [["0.43", "90"]], "down_asks": [["0.45", "70"]],
    })
    c.books["0xabc"] = b
    assert c._handle_trade_message(_trade_msg(), "BTC", time.time_ns()) is True
    row = captured[0][1]
    assert (row["up_bid"], row["up_ask"]) == (0.54, 0.56)
    assert (row["up_bid_size"], row["up_ask_size"]) == (100.0, 80.0)
    assert (row["down_bid"], row["down_ask"]) == (0.43, 0.45)
    assert (row["down_bid_size"], row["down_ask_size"]) == (90.0, 70.0)
    assert row["quote_book_state"] == "live"
    # slippage is exact: no snapshot join needed
    mid = (row["up_bid"] + row["up_ask"]) / 2
    assert row["price"] - mid == 0.55 - 0.55


def test_empty_side_stamps_null_price_and_size(tmp_path):
    c, captured = _collector(tmp_path)
    c.markets["0xabc"] = _market()
    b = _book()
    b.replace_from_rest_snapshot({
        "up_bids": [], "up_asks": [["0.56", "80"]],
        "down_bids": [["0.43", "90"]], "down_asks": [["0.45", "70"]],
    })
    c.books["0xabc"] = b
    assert c._handle_trade_message(_trade_msg(), "BTC", time.time_ns()) is True
    row = captured[0][1]
    assert row["up_bid"] is None and row["up_bid_size"] is None  # null-vs-zero
    assert row["up_ask"] == 0.56 and row["up_ask_size"] == 80.0


def test_missing_book_stays_null(tmp_path):
    c, captured = _collector(tmp_path)
    # unresolvable market: no book, condition stays NULL (C3) and so do quotes
    assert c._handle_trade_message(_trade_msg(), "BTC", time.time_ns()) is True
    row = captured[0][1]
    assert row["condition_id"] is None
    for col in ("up_bid", "up_ask", "up_bid_size", "up_ask_size",
                "down_bid", "down_ask", "down_bid_size", "down_ask_size",
                "quote_book_state"):
        assert row[col] is None


def test_stale_book_stamps_state_honestly(tmp_path):
    c, captured = _collector(tmp_path)
    c.markets["0xabc"] = _market()
    b = _book()
    b.replace_from_rest_snapshot({"up_bids": [["0.54", "100"]], "up_asks": [["0.56", "80"]]})
    b.mark_stale(resync_id="r1")
    c.books["0xabc"] = b
    assert c._handle_trade_message(_trade_msg(), "BTC", time.time_ns()) is True
    row = captured[0][1]
    assert row["quote_book_state"] == "stale"
    assert row["up_bid"] == 0.54  # raw levels still stamped; state tells the truth
