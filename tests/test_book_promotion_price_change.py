"""Finding #4: price_change frames promote a stale/resyncing book.

A stale book revives on a price_change frame only when BOTH outcomes hold
two-sided, sane (0 <= bid < ask <= 1), uncrossed tops consistent with the
frame's exchange-reported bests. Real exchange values only.
"""
import time

from polymarket_collector.book import OrderBookState
from polymarket_collector.enums import BookState

TS = "1788649335000"


def make_book():
    return OrderBookState(
        asset="BTC", condition_id="cid-1", market_id="mid-1",
        series_id="BTC-5MIN", window_index=42,
        up_token_id="up-123", down_token_id="down-456",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
        l2_levels=20,
    )


def pc_frame(entries):
    return {"event_type": "price_change", "market": "0xm", "timestamp": TS,
            "price_changes": entries}


def entry(token, price, size, side, bid, ask):
    return {"asset_id": token, "price": str(price), "size": str(size),
            "side": side, "best_bid": str(bid), "best_ask": str(ask)}


def test_stale_good_tops_promotes():
    b = make_book()
    b.mark_stale("r1")
    ok, _ = b.apply_ws_message(pc_frame([
        entry("up-123", 0.50, 10, "BUY", 0.50, 0.55),
        entry("up-123", 0.55, 10, "SELL", 0.50, 0.55),
        entry("down-456", 0.44, 10, "BUY", 0.44, 0.49),
        entry("down-456", 0.49, 10, "SELL", 0.44, 0.49),
    ]))
    assert ok is True
    assert b.book_state == BookState.live


def test_one_sided_stays_stale():
    b = make_book()
    b.mark_stale("r1")
    ok, _ = b.apply_ws_message(pc_frame([
        entry("up-123", 0.50, 10, "BUY", 0.50, 0.55),
        entry("up-123", 0.55, 10, "SELL", 0.50, 0.55),
        entry("down-456", 0.44, 10, "BUY", 0.44, 0.49),
    ]))
    assert ok is True
    assert b.book_state == BookState.stale


def test_crossed_stays_stale():
    b = make_book()
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.70, 10]],
                        "asks": [[0.65, 10]]})  # no hash: crossed, stays live-path untouched
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.30, 10]],
                        "asks": [[0.35, 10]]})
    b.mark_stale("r1")
    ok, _ = b.apply_ws_message(pc_frame([
        entry("up-123", 0.70, 99, "BUY", 0.70, 0.65),
        entry("down-456", 0.30, 99, "BUY", 0.30, 0.35),
        entry("down-456", 0.35, 99, "SELL", 0.30, 0.35),
    ]))
    assert ok is True
    assert b.book_state == BookState.stale


def test_live_untouched():
    b = make_book()
    assert b.book_state == BookState.live
    ok, reason = b.apply_ws_message(pc_frame([
        entry("up-123", 0.50, 10, "BUY", 0.50, 0.55),
        entry("up-123", 0.55, 10, "SELL", 0.50, 0.55),
        entry("down-456", 0.44, 10, "BUY", 0.44, 0.49),
        entry("down-456", 0.49, 10, "SELL", 0.44, 0.49),
    ]))
    assert ok is True
    assert reason is None
    assert b.book_state == BookState.live
    assert b.resync_id is None


def test_h2_marked_book_revives_on_next_good_price_change():
    b = make_book()
    # frame 1: DOWN ask side empty while exchange reports ask 0.55 -> H2 stale
    ok, _ = b.apply_ws_message(pc_frame([
        entry("up-123", 0.50, 10, "BUY", 0.50, 0.55),
        entry("up-123", 0.55, 10, "SELL", 0.50, 0.55),
        entry("down-456", 0.44, 10, "BUY", 0.44, 0.55),
    ]))
    assert ok is True
    assert b.book_state == BookState.stale
    # frame 2: real DOWN ask level arrives with matching exchange bests
    ok, _ = b.apply_ws_message(pc_frame([
        entry("down-456", 0.55, 10, "SELL", 0.44, 0.55),
    ]))
    assert ok is True
    assert b.book_state == BookState.live
