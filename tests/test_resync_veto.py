"""_rest_verified veto semantics (measured relaxation 2026-10-10, constant literals only).

Prod evidence (collector_events date=2026-10-10, newest-30-file span
16:33:59-16:36:43Z + book_snapshots_500ms): REST-verified markets whose REST
later went fetch_none sat stale while WS streamed a sane two-sided book —
2.25 of 17.70 span stale-row-minutes (12.7% > 5% gate; 0x91cbeded… held a
69s contiguous stale run with frames applied throughout). The veto now lifts
only on a genuine REST failure (fetch_none) since the last REST success and
re-arms on the next success; confirmed books with healthy REST keep REST
discipline.
"""
import time

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager, WS_PROVISIONAL_MIN_FRAMES


def _book(cid="cid-veto", asset="BTC"):
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


def _bank_frames(mgr, cid, n=WS_PROVISIONAL_MIN_FRAMES):
    for _ in range(n):
        mgr.note_ws_frame(cid, 0.45, 0.47)


def test_verified_without_failure_keeps_veto():
    """No REST failure since the last success → REST discipline still owns the book."""
    events = []
    mgr = _mgr(events)
    mgr.note_fetch_ok("c1")
    _bank_frames(mgr, "c1")
    assert mgr.should_promote_provisional("c1") is False
    books = {"c1": _book("c1")}
    books["c1"].mark_stale(resync_id="r1")
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47) is False
    assert books["c1"].book_state.value == "stale"


def test_fetch_none_after_success_lifts_veto():
    """A genuine REST failure since the last success re-opens the provisional path."""
    events = []
    mgr = _mgr(events)
    mgr.note_fetch_ok("c1")  # REST confirmed the book once
    assert "c1" in mgr._rest_verified
    mgr.note_fetch_none("c1")  # REST then genuinely failed (200-empty/404)
    # the collector's call-site gate reads dict membership — must be lifted
    assert "c1" not in mgr._rest_verified
    assert mgr.should_promote_provisional("c1") is False  # frames not banked yet
    books = {"c1": _book("c1")}
    books["c1"].mark_stale(resync_id="r1")
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47) is False
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47) is False
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47) is True
    assert books["c1"].book_state.value == "live"
    prov = [d for t, d in events
            if getattr(t, "value", t) == "resync_completed" and d.get("provisional") is True]
    assert prov, "lifted veto must still carry provisional provenance"
    assert prov[0]["verified"] == "ws"
    assert prov[0]["condition_id"] == "c1"


def test_rest_success_re_arms_veto():
    """After the lift, the next REST success restores REST discipline."""
    events = []
    mgr = _mgr(events)
    mgr.note_fetch_ok("c1")
    mgr.note_fetch_none("c1")
    mgr.note_fetch_ok("c1")  # REST re-confirmed
    assert "c1" in mgr._rest_verified
    _bank_frames(mgr, "c1")
    assert mgr.should_promote_provisional("c1") is False
    books = {"c1": _book("c1")}
    books["c1"].mark_stale(resync_id="r1")
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47) is False
    assert books["c1"].book_state.value == "stale"


def test_rate_limited_does_not_lift_veto():
    """A 429 is transient (bounded backoff owns it) — the veto stays armed."""
    events = []
    mgr = _mgr(events)
    mgr.note_fetch_ok("c1")
    mgr.note_rate_limited("c1", retry_after_s=1.0)
    assert "c1" in mgr._rest_verified
    _bank_frames(mgr, "c1")
    assert mgr.should_promote_provisional("c1") is False


def test_fetch_none_on_never_verified_is_total():
    """Lifting a veto that was never set changes nothing and never raises."""
    events = []
    mgr = _mgr(events)
    assert mgr.note_fetch_none("c-unknown") == 1
    assert "c-unknown" not in mgr._rest_verified
    _bank_frames(mgr, "c-unknown")
    assert mgr.should_promote_provisional("c-unknown") is True


def test_streak_and_quiet_accounting_unaffected():
    """fetch_none streak growth + terminal quiet still work with the lift in place."""
    events = []
    mgr = _mgr(events)
    mgr.note_fetch_ok("c1")
    for i in range(1, 6):
        assert mgr.note_fetch_none("c1") == i
    assert mgr.fetch_none_quiet("c1") is True
    assert "c1" not in mgr._rest_verified  # quiet and lifted veto coexist honestly


@pytest.mark.asyncio
async def test_lifted_veto_promotion_completes_linked_episode():
    """End-to-end: verify → fetch_none lift → stale episode → provisional promote closes it."""
    events = []
    mgr = _mgr(events)
    books = {"cid-veto": _book("cid-veto")}
    rid = mgr.handle_disconnect("BTC", "cid-veto", reason="market_added", books=books)
    assert books["cid-veto"].book_state.value == "stale"
    mgr.note_fetch_ok("cid-veto")  # REST verified the book once
    mgr.note_fetch_none("cid-veto")  # REST then failed — veto lifted
    assert mgr.try_ws_provisional_promote("cid-veto", books, 0.45, 0.47, rid) is False
    assert mgr.try_ws_provisional_promote("cid-veto", books, 0.45, 0.47, rid) is False
    assert mgr.try_ws_provisional_promote("cid-veto", books, 0.45, 0.47, rid) is True
    assert books["cid-veto"].book_state.value == "live"
    assert mgr.is_finished(rid) is True
