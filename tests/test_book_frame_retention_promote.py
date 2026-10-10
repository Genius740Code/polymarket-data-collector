"""Finding #5: full-book-frame promotions retain resync_id so the sweep completes.

A4 tests that a hashed book frame promotes a stale book to live — but they do
not check that resync_id is preserved across mark_live(). This module adds
verification that resync_id survives promotion so the healed-episode sweep can
close the episode and keep row tags joined.
"""
from polymarket_collector.book import OrderBookState
from polymarket_collector.enums import BookState


def make_book(asset="BTC", condition_id="cid-1"):
    return OrderBookState(
        asset=asset,
        condition_id=condition_id,
        market_id="mid-1",
        series_id="BTC-5MIN",
        window_index=42,
        up_token_id="up-123",
        down_token_id="down-456",
        market_end_ts_ms=int(__import__("time").time() * 1000) + 300_000,
        l2_levels=20,
    )


def _live_book_frame(ts=None):
    """Realistic CLOB `book` frame (live probe 2026-09-05 shape)."""
    m = {
        "event_type": "book",
        "asset_id": "up-123",
        "market": "0xm",
        "bids": [{"price": "0.50", "size": "10"}],
        "asks": [{"price": "0.52", "size": "10"}],
        "hash": "0df9ed199b15f73551ec79f2b0d43cb805c0dafa",
    }
    if ts is not None:
        m["timestamp"] = ts
    return m


def test_frame_promoted_book_retains_resync_id():
    """A full-book-frame-promoted book retains resync_id so sweep completes."""
    b = make_book()
    b.mark_stale("r1")
    assert b.resync_id == "r1"
    assert b.book_state == BookState.stale
    applied, reason = b.apply_ws_message(_live_book_frame(ts="1788649334527"))
    assert applied is True
    assert reason is None
    assert b.book_state == BookState.live
    # resync_id must survive mark_live() so the sweep can close the episode
    assert b.resync_id == "r1"


def test_frame_promoted_book_retains_resync_id_one_sided():
    """Same retention for one_sided (weather) full-book-frame promotion."""
    from polymarket_collector.book import OrderBookState
    import time as _t
    b = OrderBookState(
        asset="LONDON", condition_id="cid-f10", market_id="mid-1",
        series_id="WEATHER-HIGH-1D", window_index=42,
        up_token_id="up-123", down_token_id="down-456",
        market_end_ts_ms=int(_t.time() * 1000) + 300_000,
        l2_levels=20, one_sided_promotion=True,
    )
    b.mark_stale("r2")
    assert b.resync_id == "r2"
    f = {"event_type": "book", "asset_id": "up-123", "market": "0xm",
         "asks": [{"price": "0.52", "size": "10"}],
         "hash": "0df9ed199b15f73551ec79f2b0d43cb805c0dafa"}
    applied, reason = b.apply_ws_message(f)
    assert applied is True
    assert b.book_state == BookState.live
    assert b.resync_id == "r2"


def test_live_book_untouched_resync_id_none():
    """Live books (already live) have resync_id cleared or remain None — no-op."""
    b = make_book()
    assert b.book_state == BookState.live
    assert b.resync_id is None
    applied, reason = b.apply_ws_message(_live_book_frame(ts="1788649334527"))
    assert applied is True
    assert reason is None
    assert b.book_state == BookState.live
    assert b.resync_id is None