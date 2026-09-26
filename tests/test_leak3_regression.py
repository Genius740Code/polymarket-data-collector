"""P0 leak hunt session 3 (2026-09-25) — steady-state RSS growth regression tests.

Leak shape (measured prod 2026-09-25, fresh reseed 2.9h): 1494/2538 resync
episodes never reached a final state (1457 with ZERO resync attempts — resync()
is only driven from the reconnect path; the sweep only closes episodes whose
book went live). Each pinned its replay buffer: the buffer age deadline was
enforced only LAZILY (on the next buffer_message call for the same episode),
which never comes for a quiet feed (window rolled while stale), so up to
MAX_BUFFERED_MSGS_PER_EPISODE parsed WS frames — full-ladder `book` frames are
multi-KB — stayed in RAM forever. 20-min 7-asset repro: tracemalloc 36.9MB /
518k retained orjson dicts at the loads() site, RSS ~22MB/min.

Fixed paths under test:
  1. ResyncManager.reap_expired_buffers — active deadline-based retirement
     (mirrors the lazy path: one honest resync_buffer_retired event, buffer +
     deadline popped, never replayed/re-armed)
  2. fresh buffers are never reaped mid-resync
  3. escalated/completed episodes lose leftover buffers
  4. Collector._persist_resync_episode — persisted-marking on FIRST successful
     append (not only final), so the 6h ended-market eviction can reclaim
     never-final episodes; failed append still discards (unwritten is never
     dropped by eviction)
  5. end-to-end: never-final-but-persisted episode of an ended market is
     evicted by the 6h RAM hygiene tick
"""
import time

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager


def make_book(cid="cid-1", asset="BTC"):
    return OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id="up-123", down_token_id="down-456",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
    )


def make_mgr(events=None):
    async def rest_fetch(asset, cid):
        return {"up_bids": [[0.60, 100]], "up_asks": [[0.65, 30]], "sequence_number": 10}
    return ResyncManager(
        CollectorConfig(), rest_fetcher=rest_fetch,
        on_event=(lambda t, d: events.append((str(t), d))) if events is not None else lambda t, d: None,
    )


def make_collector(tmp_path):
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": str(tmp_path / "cursor_state")},
                          timeframes=["5m"])
    return Collector(cfg)


# 1. active deadline reaping ---------------------------------------------------
def test_reap_expired_buffers_frees_quiet_feed():
    """The prod leak: episode opens, messages buffer, the feed goes quiet
    (window rolls while stale) — the lazy retirement never fires, so the
    deadline must be enforced ACTIVELY. Buffer popped, one honest event,
    never re-armed."""
    events = []
    mgr = make_mgr(events)
    rid = mgr.handle_disconnect("BTC", "cid-x", "test", {})
    for i in range(10):
        mgr.buffer_message(rid, {"sequence_number": i, "bids": [[0.5, 1]]})
    assert len(mgr._buffers[rid]) == 10
    # simulate the age deadline passing with no further messages on this feed
    mgr._buffer_deadline[rid] = time.monotonic() - 1
    reaped = mgr.reap_expired_buffers()
    assert reaped == 1
    assert rid not in mgr._buffers
    assert rid not in mgr._buffer_deadline
    assert rid in mgr._buffer_retired
    retired_events = [d for t, d in events if t.endswith("book_anomaly") and d.get("reason") == "resync_buffer_retired"]
    assert len(retired_events) == 1
    assert retired_events[0]["resync_id"] == rid
    # idempotent: a second sweep must not re-emit or resurrect anything
    assert mgr.reap_expired_buffers() == 0
    retired_events2 = [d for t, d in events if t.endswith("book_anomaly") and d.get("reason") == "resync_buffer_retired"]
    assert len(retired_events2) == 1
    # the retired buffer is dead: no re-arm, no routing to it
    assert not mgr.buffer_live(rid)
    mgr.buffer_message(rid, {"late": True})  # late message must not resurrect
    assert rid not in mgr._buffers
    assert mgr.newest_open_buffer_id("BTC") == ""


# 2. fresh buffers survive -----------------------------------------------------
def test_reap_keeps_fresh_buffers():
    """A resync in progress (deadline in the future) must keep its buffer."""
    mgr = make_mgr()
    rid = mgr.handle_disconnect("BTC", None, "test", {})
    mgr.buffer_message(rid, {"m": 1})
    assert mgr.reap_expired_buffers() == 0
    assert len(mgr._buffers[rid]) == 1
    assert mgr.buffer_live(rid)


# 3. dead-episode leftovers ----------------------------------------------------
def test_reap_pops_dead_episode_leftover_buffers():
    """Escalated/completed episodes must not hold buffer RAM even if a state
    transition race left the deque behind."""
    mgr = make_mgr()
    rid_open = mgr.handle_disconnect("BTC", None, "test", {})
    rid_esc = mgr.handle_disconnect("BTC", "cid-e", "test", {})
    rid_done = mgr.handle_disconnect("BTC", "cid-d", "test", {})
    mgr._escalated.add(rid_esc)
    mgr._episodes[rid_done].resync_completed_ts_utc = "2026-09-25T00:00:00Z"
    reaped = mgr.reap_expired_buffers()
    assert reaped == 2
    assert rid_esc not in mgr._buffers
    assert rid_done not in mgr._buffers
    assert rid_open in mgr._buffers  # open + fresh stays


# 4. persisted-marking on first successful append -------------------------------
def test_persist_resync_episode_marks_persisted_on_first_append(tmp_path):
    """Never-final episodes must count as persisted once ANY transition row
    landed in parquet — that is exactly what the 6h eviction guard checks.
    A failed append still discards, so an unwritten episode is never dropped."""
    c = make_collector(tmp_path)
    rid = c.resync.handle_disconnect("BTC", "cid-p", "test", {})
    ep = c.resync._episodes[rid].to_dict()  # NOT final (no completed/escalated)
    c._persist_resync_episode(ep)
    assert rid in c._episode_persisted
    # a later failed append (backpressure) must un-mark it again
    orig_append = c.writer.append
    c.writer.append = lambda *a, **k: False
    try:
        c._persist_resync_episode({**ep, "resync_attempt_count": 1})
        assert rid not in c._episode_persisted
    finally:
        c.writer.append = orig_append
    # and a successful re-append re-marks it
    c._persist_resync_episode({**ep, "resync_attempt_count": 2})
    assert rid in c._episode_persisted


# 6. reaper runs even while an export holds the kaggle lock ---------------------
@pytest.mark.asyncio
async def test_flush_loop_reaps_while_export_holds_kaggle_lock(tmp_path):
    """The 1d-lane export holds _kaggle_lock for many minutes (27min measured
    2026-09-25 — each trades worker gets a 420s Data-API budget). With the
    sweep/reap BELOW the lock acquisition, the flush loop parked at
    `async with self._kaggle_lock` for the whole export: prod ebuf climbed
    25k->332k buffered msgs (+600MB RSS) and the PM2 memory cap killed the
    process mid-export, cancelling the tick so slow lanes never verified.
    Episode hygiene must run BEFORE the lock section, every flush tick."""
    import asyncio
    c = make_collector(tmp_path)
    c._running = True
    reaps: list = []
    c.resync.close_healed_episodes = lambda books: 0
    c.resync.reap_expired_buffers = lambda: reaps.append(1) or 0
    c.config.storage.flush_interval_seconds = 0.05

    async def _noop_drift():
        return None
    c._periodic_drift_tick = _noop_drift
    c._disk_guard_tick = lambda: None
    c.writer.flush = lambda: 0
    c._persist_cursor_sync = lambda: None
    c.markets_log.flush_staging = lambda: None

    async with c._kaggle_lock:  # "export" holds the lock for the whole test
        task = asyncio.create_task(c._flush_loop())
        try:
            await asyncio.sleep(0.4)  # several flush ticks elapse
        finally:
            c._running = False
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
    assert len(reaps) >= 3, (
        f"reaper ran only {len(reaps)} tick(s) while the export held the "
        "kaggle lock — episode hygiene is starved by long export ticks again"
    )


# 5. end-to-end: never-final episode of an ended market is evicted --------------
def test_never_final_persisted_episode_evicted_after_market_end(tmp_path):
    """The exact prod leak shape: the episode never completes (book ended
    stale, resync() never driven) but every transition was persisted. The 6h
    ended-market tick must reclaim its RAM (episode + _episode_latest)."""
    c = make_collector(tmp_path)
    rid = c.resync.handle_disconnect("BTC", "cid-old3", "test", {})
    c.resync.buffer_message(rid, {"m": 1})
    c._persist_resync_episode(c.resync._episodes[rid].to_dict())  # open, persisted
    assert c.resync._episodes[rid].resync_completed_ts_utc is None  # never final

    class FakeMarket:
        asset = "BTC"
        condition_id = "cid-old3"
        market_end_ts_ms = int(time.time() * 1000) - 7 * 3600 * 1000  # ended >6h ago
        market_start_ts_ms = market_end_ts_ms - 300_000
        window_index = 1
        def to_markets_row(self):
            return {"condition_id": "cid-old3", "status": "resolved", "resolution_outcome": "up"}
    c.markets["cid-old3"] = FakeMarket()
    c.books["cid-old3"] = make_book("cid-old3")

    c._memory_eviction_tick(int(time.time() * 1000))
    assert "cid-old3" not in c.markets
    assert rid not in c.resync._episodes
    assert rid not in c.resync._buffers
    assert rid not in c._episode_latest
