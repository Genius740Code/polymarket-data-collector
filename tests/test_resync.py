"""Tests for §1A resync — disconnect, buffer-and-replay, retry/escalation, drift fallback."""
import asyncio
import time
import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager, exponential_backoff


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


def test_exponential_backoff_with_jitter():
    d0 = exponential_backoff(0, 500, 30000, jitter=False)
    assert d0 == 0.5
    d1 = exponential_backoff(1, 500, 30000, jitter=False)
    assert d1 == 1.0
    d2 = exponential_backoff(2, 500, 30000, jitter=False)
    assert d2 == 2.0
    # capped
    d10 = exponential_backoff(10, 500, 30000, jitter=False)
    assert d10 == 30.0


@pytest.mark.asyncio
async def test_disconnect_marks_stale_and_resync_replaces():
    cfg = CollectorConfig()
    events = []
    books = {"cid-1": make_book("cid-1")}
    # initial state
    books["cid-1"].apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 1})
    assert books["cid-1"].up.bids.best_price() == 0.5

    async def rest_fetch(asset, cid):
        return {"up_bids": [[0.60, 100]], "up_asks": [[0.65, 30]], "sequence_number": 2}

    mgr = ResyncManager(cfg, rest_fetcher=rest_fetch, on_event=lambda t, d: events.append(str(t)))

    # disconnect
    resync_id = mgr.handle_disconnect("BTC", "cid-1", reason="network_error", books=books)
    assert books["cid-1"].book_state.value == "stale"
    assert any("ws_disconnected" in e for e in events)

    # reconnect + resync
    mgr.handle_reconnect(resync_id)
    # buffer a delta that arrives during REST fetch (gap)
    mgr.buffer_message(resync_id, {"token_id": "up-123", "bids": [[0.61, 50]], "sequence_number": 3})

    ok = await mgr.resync("BTC", "cid-1", books, resync_id)
    assert ok is True
    # book should have been replaced by REST then replayed buffer
    assert books["cid-1"].book_state.value == "live"
    # after replay, best should be 0.61 (buffered) if patch logic kept both, or at least not 0.5
    assert books["cid-1"].up.bids.best_price() in (0.61, 0.60)  # patch retains both or replaces

    ep = mgr.get_episode(resync_id)
    assert ep.gap_duration_ms is not None
    assert ep.resync_attempt_count == 1


@pytest.mark.asyncio
async def test_buffer_replay_discards_old_sequence():
    cfg = CollectorConfig()
    books = {"cid-1": make_book("cid-1")}
    books["cid-1"].apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 1})

    async def rest_fetch(asset, cid):
        # REST snapshot seq = 5 covers up to 5
        return {"up_bids": [[0.60, 100]], "sequence_number": 5}

    mgr = ResyncManager(cfg, rest_fetcher=rest_fetch, on_event=lambda *_: None)
    rid = mgr.handle_disconnect("BTC", "cid-1", reason="test", books=books)
    mgr.handle_reconnect(rid)
    # buffer messages: seq 4 (old → should be discarded), seq 6 (new → apply)
    mgr.buffer_message(rid, {"token_id": "up-123", "bids": [[0.90, 1]], "sequence_number": 4})
    mgr.buffer_message(rid, {"token_id": "up-123", "bids": [[0.62, 20]], "sequence_number": 6})

    await mgr.resync("BTC", "cid-1", books, rid)
    # The 0.90 (old) should have been discarded; book should not be 0.90
    prices = [lvl.price for lvl in books["cid-1"].up.bids.levels if lvl.price is not None]
    assert 0.90 not in prices


@pytest.mark.asyncio
async def test_resync_retry_and_escalation():
    cfg = CollectorConfig()
    cfg.ws.max_resync_duration_seconds = 1  # short for test
    cfg.ws.resync_rest_backoff_initial_ms = 100
    cfg.ws.resync_rest_backoff_max_ms = 200
    books = {"cid-1": make_book("cid-1")}
    events = []

    async def failing_fetch(asset, cid):
        raise RuntimeError("5xx")

    mgr = ResyncManager(cfg, rest_fetcher=failing_fetch, on_event=lambda t, d: events.append(t))

    rid = mgr.handle_disconnect("BTC", "cid-1", reason="test", books=books)
    mgr.handle_reconnect(rid)

    start = time.monotonic()
    ok = await mgr.resync("BTC", "cid-1", books, rid)
    elapsed = time.monotonic() - start
    assert ok is False  # escalated after max duration
    assert elapsed >= 1.0
    ep = mgr.get_episode(rid)
    assert ep.resync_attempt_count > 1
    # at least one resync_failed + escalation logged
    assert any("resync_failed" in str(e) for e in events)
    # book stays stale
    assert books["cid-1"].book_state.value in ("stale", "resyncing")


@pytest.mark.asyncio
async def test_sequence_gap_triggers_resync():
    cfg = CollectorConfig()
    books = {"cid-1": make_book("cid-1")}
    books["cid-1"].apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 1})
    mgr = ResyncManager(cfg, rest_fetcher=lambda a, c: {"up_bids": []}, on_event=lambda *_: None)
    # gap: expected 2 got 5
    rid = mgr.handle_sequence_gap("BTC", "cid-1", books, expected=2, received=5)
    assert books["cid-1"].book_state.value == "stale"
    assert rid in mgr._episodes


@pytest.mark.asyncio
async def test_drift_check_triggers_stale():
    cfg = CollectorConfig()
    books = {"cid-1": make_book("cid-1")}
    books["cid-1"].apply_ws_message({"token_id": "up-123", "bids": [[0.50, 10]], "asks": [[0.60, 10]], "sequence_number": 1})

    # REST returns different book → drift
    async def drift_fetch(asset, cid):
        return {"up_bids": [[0.99, 10]], "up_asks": [[0.60, 10]]}

    mgr = ResyncManager(cfg, rest_fetcher=drift_fetch, on_event=lambda *_: None)
    rid = await mgr.periodic_drift_check("BTC", "cid-1", books)
    assert rid is not None
    assert books["cid-1"].book_state.value == "stale"

    # no drift
    async def same_fetch(asset, cid):
        return {"up_bids": [[0.50, 10]], "up_asks": [[0.60, 10]]}
    mgr2 = ResyncManager(cfg, rest_fetcher=same_fetch, on_event=lambda *_: None)
    books2 = {"cid-1": make_book("cid-1")}
    books2["cid-1"].apply_ws_message({"token_id": "up-123", "bids": [[0.50, 10]], "asks": [[0.60, 10]], "sequence_number": 1})
    rid2 = await mgr2.periodic_drift_check("BTC", "cid-1", books2)
    assert rid2 is None
    assert books2["cid-1"].book_state.value == "live"
