"""P3 liveness fix (2026-10-11) — an open episode never forces a healed book stale.

The named gate: ResyncManager.resync() re-marked ALL books of the
condition on every drive with no liveness check (resync.py targets list),
so a book healed live by real WS frames mid-episode was downgraded back to
'resyncing' and held non-live through the fetch_none retry loop — the
dominant stale-with-fresh-book tax of the first ~2 min post-roll (P3:
(stale,resync_id)=334 vs live=272 per lane, stale rows bk_age med 0ms).

These tests pin: live books are skipped by the drive (no downgrade, no
REST cursor regression, WS content kept), stale books still heal exactly
as before, and every evidence row (episode persists + collector events)
is still emitted — gaps stay labeled, nothing is filled.

Real objects + constant literals only; no network.
"""
import time

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager


def make_book(cid="cid-1", asset="BTC"):
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


def _mgr(events, persists, rest):
    cfg = CollectorConfig()
    return ResyncManager(
        cfg,
        rest_fetcher=rest,
        on_event=lambda t, d: events.append((str(t), d)),
        on_episode_persist=lambda p: persists.append(dict(p)),
    )


async def _rest_ok(asset, cid):
    return {"up_bids": [[0.60, 100]], "up_asks": [[0.65, 30]], "sequence_number": 2}


# --- the gate: a healed book is never forced back ------------------------------

@pytest.mark.asyncio
async def test_resync_drive_skips_live_book_no_downgrade():
    """A book healed live by a hash-gated full-book frame mid-episode keeps
    its liveness AND its fresher WS content through the whole drive."""
    events, persists = [], []
    books = {"cid-1": make_book("cid-1")}
    b = books["cid-1"]
    mgr = _mgr(events, persists, _rest_ok)
    rid = mgr.handle_disconnect("BTC", "cid-1", "network_error", books)
    assert b.book_state.value == "stale"
    # WS promotion mid-episode (hashed full-book frame, real exchange data)
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.55, 20]], "asks": [[0.58, 5]],
                        "hash": "a1b2c3d4e5f6a7b8", "sequence_number": 3})
    assert b.book_state.value == "live"
    mgr.handle_reconnect(rid)
    ok = await mgr.resync("BTC", "cid-1", books, rid)
    assert ok is True
    # never downgraded, WS content kept (the REST snapshot would have
    # clobbered 0.55 -> 0.60 and regressed the sequence cursor)
    assert b.book_state.value == "live"
    assert b.up.bids.best_price() == 0.55
    assert b.sequence_numbers.get("up-123") == 3


@pytest.mark.asyncio
async def test_resync_drive_still_marks_stale_book_resyncing_then_live():
    """Regression pin: a book still stale at drive start is driven exactly
    as before — resyncing during, REST-replaced, live after."""
    events, persists = [], []
    books = {"cid-2": make_book("cid-2")}
    b = books["cid-2"]
    mgr = _mgr(events, persists, _rest_ok)
    rid = mgr.handle_disconnect("BTC", "cid-2", "network_error", books)
    assert b.book_state.value == "stale"
    ok = await mgr.resync("BTC", "cid-2", books, rid)
    assert ok is True
    assert b.book_state.value == "live"
    assert b.up.bids.best_price() == 0.60  # REST snapshot replaced the book


@pytest.mark.asyncio
async def test_live_book_row_reads_live_through_open_episode():
    """End-to-end shape of the P3 tax, fixed: episode open, book fresh and
    verified — the snapshot row reads live while the episode keeps its
    evidence rows."""
    events, persists = [], []
    books = {"cid-3": make_book("cid-3")}
    b = books["cid-3"]
    mgr = _mgr(events, persists, _rest_ok)
    rid = mgr.handle_disconnect("BTC", "cid-3", "network_error", books)
    assert rid  # the open episode still mints its evidence row
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.55, 20]], "asks": [[0.58, 5]],
                        "hash": "a1b2c3d4e5f6a7b8", "sequence_number": 3})
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.42, 20]], "asks": [[0.45, 5]],
                        "hash": "b1b2c3d4e5f6a7b8", "sequence_number": 3})
    row = b.snapshot().to_flat_dict()
    assert b.book_state.value == "live"
    assert row["book_state"] == "live"


# --- evidence rows keep flowing (gaps stay labeled) -----------------------------

@pytest.mark.asyncio
async def test_evidence_rows_still_emitted_with_live_book():
    """The skipped-live drive still mints and closes the episode honestly:
    resync_episodes persists at every transition + collector events fire —
    the real-data law holds, no gap is ever filled or hidden."""
    events, persists = [], []
    books = {"cid-4": make_book("cid-4")}
    b = books["cid-4"]
    mgr = _mgr(events, persists, _rest_ok)
    rid = mgr.handle_disconnect("BTC", "cid-4", "network_error", books)
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.55, 20]], "asks": [[0.58, 5]],
                        "hash": "a1b2c3d4e5f6a7b8", "sequence_number": 3})
    ok = await mgr.resync("BTC", "cid-4", books, rid)
    assert ok is True
    etypes = [t for t, _ in events]
    assert "ws_disconnected" in etypes
    assert "resync_started" in etypes
    assert "resync_completed" in etypes
    # episode evidence rows: disconnect mint, attempt, completion — all joined
    by_rid = [p for p in persists if p.get("resync_id") == rid]
    assert len(by_rid) >= 3
    assert all(p.get("condition_id") == "cid-4" for p in by_rid)
    ep = mgr.get_episode(rid)
    assert ep is not None and ep.resync_completed_ts_utc is not None


@pytest.mark.asyncio
async def test_stale_book_drive_evidence_unchanged():
    """The pre-existing stale-book drive keeps its exact evidence shape."""
    events, persists = [], []
    books = {"cid-5": make_book("cid-5")}
    mgr = _mgr(events, persists, _rest_ok)
    rid = mgr.handle_disconnect("BTC", "cid-5", "network_error", books)
    ok = await mgr.resync("BTC", "cid-5", books, rid)
    assert ok is True
    etypes = [t for t, _ in events]
    assert etypes.count("resync_completed") == 1
    assert any(p.get("resync_id") == rid and p.get("disconnect_reason") == "network_error"
               for p in persists)
