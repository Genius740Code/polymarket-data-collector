"""P2 audit fix (kaggle_null_audit_2026-09-09): E4 zero sentinel → NULL."""
import time

from polymarket_collector.book import OrderBookState


def _book() -> OrderBookState:
    return OrderBookState(
        asset="BTC", condition_id="cid-1", market_id="mid-1",
        series_id="BTC-5m", window_index=1,
        up_token_id="up-tok", down_token_id="down-tok",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
    )


def test_e4_zero_price_levels_dropped_not_stored():
    b = _book()
    msg = {
        "bids": [["0.0", "10"], ["0.55", "5"]],
        "asks": [["0.0", "7"]],
        "token_id": "up-tok",
        "timestamp": str(int(time.time() * 1000)),
        "hash": "h" * 32,
    }
    ok, _ = b.apply_ws_message(msg)
    assert ok
    # 0-price levels are sentinels: best bid is the real quote, ask side empty
    assert b.up.bids.best_price() == 0.55
    assert b.up.asks.best_price() is None
    snap = b.snapshot().to_flat_dict()
    assert snap["up_bid"] == 0.55
    assert snap["up_ask"] is None and snap["up_ask_size"] is None
    # no 0.0 anywhere in L1/BBO columns
    for col in ("up_bid", "up_ask", "down_bid", "down_ask",
                "up_bid_level_1_price", "up_ask_level_1_price"):
        assert snap[col] is None or snap[col] != 0.0, f"{col} must never be 0.0"


def test_e4_zero_price_change_removes_level():
    b = _book()
    seed = {
        "bids": [["0.55", "10"]],
        "asks": [["0.60", "10"]],
        "token_id": "up-tok",
        "timestamp": str(int(time.time() * 1000)),
        "hash": "h" * 32,
    }
    assert b.apply_ws_message(seed)[0]
    assert b.up.bids.best_price() == 0.55
    # a 0-price delta for the bid side removes the quote instead of storing 0
    b._apply_price_change_level(b.up.bids, 0.0, 5.0, True)
    assert b.up.bids.best_price() == 0.55
    snap = b.snapshot().to_flat_dict()
    assert snap["up_bid"] == 0.55
