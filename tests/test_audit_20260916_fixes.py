"""Audit 2026-09-16 regression tests: escalated episodes must never re-drive.

Data evidence: resync_started 654 vs resync_completed 11/day, episodes with
attempt counts 300-507, every 150s recycle burning 60s of futile REST retries.
"""
import asyncio
import time

import pytest

from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager
from tests.test_resync import make_book


def _failing_fetch(asset, cid):
    async def _go(a, c):
        raise RuntimeError("REST fetch returned None")
    return _go


@pytest.mark.asyncio
async def test_escalated_resync_is_noop():
    cfg = CollectorConfig()
    cfg.ws.max_resync_duration_seconds = 1
    cfg.ws.resync_rest_backoff_initial_ms = 50
    cfg.ws.resync_rest_backoff_max_ms = 100
    books = {"cid-1": make_book("cid-1")}
    events = []
    mgr = ResyncManager(cfg, rest_fetcher=_failing_fetch(None, None),
                        on_event=lambda t, d: events.append((str(t), d)))
    rid = mgr.handle_disconnect("BTC", "cid-1", reason="test", books=books)
    mgr.handle_reconnect(rid)
    ok = await mgr.resync("BTC", "cid-1", books, rid)
    assert ok is False
    assert rid in mgr._escalated
    attempts_after_escalation = mgr.get_episode(rid).resync_attempt_count
    n_events = len(events)

    # Re-drive (what the reconnect loop did every recycle): must no-op fast,
    # no new attempts, no new escalation events.
    start = time.monotonic()
    ok2 = await mgr.resync("BTC", "cid-1", books, rid)
    elapsed = time.monotonic() - start
    assert ok2 is False
    assert mgr.get_episode(rid).resync_attempt_count == attempts_after_escalation
    assert len(events) == n_events
    assert elapsed < 1.0


def test_is_finished_gates_redrive_scan():
    cfg = CollectorConfig()
    books = {"cid-1": make_book("cid-1")}
    mgr = ResyncManager(cfg, rest_fetcher=_failing_fetch(None, None),
                        on_event=lambda *_: None)
    rid = mgr.handle_disconnect("BTC", "cid-1", reason="test", books=books)
    # open episode: eligible for (re)drive
    assert mgr.is_finished(rid) is False
    # escalated episode: the collector reconnect scan must skip it
    mgr._escalated.add(rid)
    assert mgr.is_finished(rid) is True


def test_overflow_event_carries_asset():
    cfg = CollectorConfig()
    books = {"cid-1": make_book("cid-1")}
    events = []
    mgr = ResyncManager(cfg, rest_fetcher=_failing_fetch(None, None),
                        on_event=lambda t, d: events.append((str(t), d)))
    mgr.MAX_BUFFERED_MSGS_PER_EPISODE = 5
    rid = mgr.handle_disconnect("BTC", "cid-1", reason="test", books=books)
    for i in range(7):
        mgr.buffer_message(rid, {"seq": i})
    overflows = [d for t, d in events if t.endswith("book_anomaly")
                 and isinstance(d, dict) and d.get("reason") == "resync_buffer_overflow"]
    assert overflows, "expected at least one overflow event"
    assert overflows[0]["asset"] == "BTC"
    assert overflows[0]["resync_id"] == rid
