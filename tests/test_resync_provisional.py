"""WS-provisional promotion for REST-blind fresh markets (constant literals only)."""
import time

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import (
    ResyncManager,
    WS_PROVISIONAL_MIN_FRAMES,
    ws_provisional_eligible,
)


def _book(cid="cid-fresh", asset="BTC"):
    return OrderBookState(
        asset=asset,
        condition_id=cid,
        market_id="mid-1",
        series_id="BTC-5MIN",
        window_index=1,
        up_token_id="up-123",
        down_token_id="down-456",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
    )


def _mgr(events):
    cfg = CollectorConfig()
    return ResyncManager(
        cfg,
        rest_fetcher=None,  # unused on the provisional path
        on_event=lambda t, d: events.append((t, d)),
    )


def test_eligible_gate_two_sided_sane_only():
    assert ws_provisional_eligible(0.45, 0.47) is True
    assert ws_provisional_eligible(0.0, 1.0) is True
    # one-sided / missing side
    assert ws_provisional_eligible(0.999, None) is False
    assert ws_provisional_eligible(None, 0.47) is False
    assert ws_provisional_eligible(float("nan"), 0.47) is False
    # crossed / inverted
    assert ws_provisional_eligible(0.55, 0.45) is False
    assert ws_provisional_eligible(0.5, 0.5) is False
    # out of range
    assert ws_provisional_eligible(-0.1, 0.5) is False
    assert ws_provisional_eligible(0.5, 1.5) is False
    assert ws_provisional_eligible("x", "y") is False


def test_consistent_frames_promote_after_n():
    events = []
    mgr = _mgr(events)
    assert WS_PROVISIONAL_MIN_FRAMES == 3
    assert mgr.note_ws_frame("c1", 0.45, 0.47) == 1
    assert mgr.should_promote_provisional("c1") is False
    assert mgr.note_ws_frame("c1", 0.45, 0.47) == 2
    assert mgr.should_promote_provisional("c1") is False
    assert mgr.note_ws_frame("c1", 0.45, 0.47) == 3
    assert mgr.should_promote_provisional("c1") is True


def test_changed_tick_restarts_streak():
    events = []
    mgr = _mgr(events)
    mgr.note_ws_frame("c1", 0.45, 0.47)
    mgr.note_ws_frame("c1", 0.45, 0.47)
    assert mgr.note_ws_frame("c1", 0.46, 0.48) == 1
    assert mgr.should_promote_provisional("c1") is False


def test_ineligible_frame_clears_streak():
    events = []
    mgr = _mgr(events)
    mgr.note_ws_frame("c1", 0.45, 0.47)
    mgr.note_ws_frame("c1", 0.45, 0.47)
    assert mgr.note_ws_frame("c1", 0.999, None) == 0
    assert mgr.should_promote_provisional("c1") is False


def test_rest_verified_books_keep_rest_discipline():
    events = []
    mgr = _mgr(events)
    mgr.note_fetch_ok("c1")  # a successful REST fetch happened before
    mgr.note_ws_frame("c1", 0.45, 0.47)
    mgr.note_ws_frame("c1", 0.45, 0.47)
    mgr.note_ws_frame("c1", 0.45, 0.47)
    assert mgr.should_promote_provisional("c1") is False
    books = {"c1": _book("c1")}
    books["c1"].mark_stale(resync_id="r1")
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47) is False
    assert books["c1"].book_state.value == "stale"


def test_no_book_stays_stale_fail_closed():
    events = []
    mgr = _mgr(events)
    for _ in range(WS_PROVISIONAL_MIN_FRAMES):
        mgr.note_ws_frame("c-ghost", 0.45, 0.47)
    assert mgr.should_promote_provisional("c-ghost") is True
    assert mgr.try_ws_provisional_promote("c-ghost", {}, 0.45, 0.47) is False


@pytest.mark.asyncio
async def test_provisional_promote_marks_live_with_provenance_event():
    events = []
    mgr = _mgr(events)
    books = {"cid-fresh": _book("cid-fresh")}
    rid = mgr.handle_disconnect("BTC", "cid-fresh", reason="market_added", books=books)
    assert books["cid-fresh"].book_state.value == "stale"

    # below threshold: no promotion, honestly stale
    assert mgr.try_ws_provisional_promote("cid-fresh", books, 0.45, 0.47, rid) is False
    assert mgr.try_ws_provisional_promote("cid-fresh", books, 0.45, 0.47, rid) is False
    assert books["cid-fresh"].book_state.value == "stale"

    assert mgr.try_ws_provisional_promote("cid-fresh", books, 0.45, 0.47, rid) is True
    assert books["cid-fresh"].book_state.value == "live"
    prov = [d for t, d in events
            if getattr(t, "value", t) == "resync_completed" and d.get("provisional") is True]
    assert prov, "provisional origin must be visible via collector_events"
    assert prov[0]["verified"] == "ws"
    assert prov[0]["condition_id"] == "cid-fresh"
    assert mgr.is_finished(rid) is True
