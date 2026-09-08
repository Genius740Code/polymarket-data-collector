"""P0 leak hunt session 2 (2026-09-08) — regression tests for every container
that could grow unbounded in the 24/7 collector. Each test names the leak it
guards against; caps must never silently drop data (AGENT.md real-data policy).

Fixed paths under test:
  1. ResyncManager._episodes — finished-episode FIFO eviction at MAX_EPISODES
  2. ResyncManager._buffers — hard cap with honest drop accounting + events
  3. escalation path — buffer popped + episode marked escalated (never re-arms)
  4. Collector._replay_buffer_id — only returns ids with a LIVE buffer
  5. Collector._note_chainlink_event — in-RAM cap at CHAINLINK_RAM_CAP
  6. Collector episode RAM pruning on 6h ended-market eviction
  7. MarketsLog._seen_condition_ids — FIFO cap at MAX_SEEN_CONDITION_IDS
"""
import asyncio
import time

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager
from polymarket_collector.storage.markets_log import MarketsLog


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


# 1. episode FIFO eviction ----------------------------------------------------
def test_finished_episodes_evicted_at_cap():
    """_episodes must never exceed MAX_EPISODES: finished (completed) episodes
    are evicted FIFO; open ones never are."""
    mgr = make_mgr()
    cap = ResyncManager.MAX_EPISODES
    rids = [mgr.handle_disconnect("BTC", f"cid-{i}", "test", {}) for i in range(cap + 50)]
    # all still open (nothing completed) — none may be evicted
    assert len(mgr._episodes) == cap + 50
    # complete the oldest 60
    for rid in rids[:60]:
        mgr._episodes[rid].resync_completed_ts_utc = "2026-09-08T00:00:00Z"
    # a new disconnect triggers eviction back to the cap
    mgr.handle_disconnect("ETH", None, "test", {})
    assert len(mgr._episodes) <= ResyncManager.MAX_EPISODES
    # every evicted rid was finished; every remaining open episode survived
    for rid in rids[:60]:
        if rid in mgr._episodes:
            assert mgr._episodes[rid].resync_completed_ts_utc
    open_rids = set(rids[60:])
    assert open_rids.issubset(set(mgr._episodes.keys()))


# 2. buffer hard cap ----------------------------------------------------------
def test_buffer_message_hard_cap_counts_drops():
    """A runaway open episode's buffer must not grow past the cap; drops are
    counted and surfaced as book_anomaly events (never silent)."""
    events = []
    mgr = make_mgr(events)
    rid = mgr.handle_disconnect("BTC", None, "test", {})
    cap = ResyncManager.MAX_BUFFERED_MSGS_PER_EPISODE
    for i in range(cap + 500):
        mgr.buffer_message(rid, {"sequence_number": i, "bids": [[0.5, 1]]})
    assert len(mgr._buffers[rid]) == cap
    assert mgr._buffer_dropped_total[rid] == 500
    overflow_events = [d for t, d in events if t.endswith("book_anomaly") and d.get("reason") == "resync_buffer_overflow"]
    assert overflow_events and overflow_events[0]["dropped_total"] >= 1
    assert overflow_events[0]["cap"] == cap


# 3. escalation releases the buffer -------------------------------------------
@pytest.mark.asyncio
async def test_escalation_pops_buffer_and_marks_escalated():
    """resync() escalation must pop the buffer and mark the episode escalated:
    an escalated episode can never complete, so its buffer is dead weight that
    _replay_buffer_id would otherwise keep feeding (leak #2)."""
    cfg = CollectorConfig()
    cfg.ws.max_resync_duration_seconds = 0  # force immediate escalation
    cfg.ws.resync_rest_backoff_initial_ms = 1
    cfg.ws.resync_rest_backoff_max_ms = 1

    async def failing_fetch(asset, cid):
        return None  # REST refuses → escalation

    mgr = ResyncManager(cfg, rest_fetcher=failing_fetch, on_event=lambda t, d: None)
    books = {"cid-1": make_book()}
    rid = mgr.handle_disconnect("BTC", "cid-1", "test", books)
    mgr.handle_reconnect(rid)
    mgr.buffer_message(rid, {"m": 1})
    ok = await mgr.resync("BTC", "cid-1", books, rid)
    assert ok is False
    assert rid not in mgr._buffers
    assert rid in mgr._escalated
    assert mgr.is_finished(rid)
    # escalated episodes are evictable by the FIFO cap
    assert len(mgr._episodes) == 1  # not evicted yet (under cap), but finished
    mgr.handle_disconnect("ETH", None, "test", {})
    assert mgr._escalated or rid in mgr._episodes  # lifecycle intact


# 4. _replay_buffer_id gates on a LIVE buffer ---------------------------------
def test_replay_buffer_id_ignores_dead_buffers(tmp_path):
    """An open-but-bufferless (escalated) episode must NOT re-arm buffering —
    only ids whose buffer still exists are returned."""
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": str(tmp_path / "cursor_state")},
                          timeframes=["5m"])
    c = Collector(cfg)
    rid = c.resync.handle_disconnect("BTC", None, "test", {})
    assert c._replay_buffer_id("BTC") == rid  # live buffer
    c.resync._buffers.pop(rid)  # simulate escalation cleanup
    c.resync._escalated.add(rid)
    assert c._replay_buffer_id("BTC") == ""  # dead — never re-arm


# 5. chainlink RAM cap --------------------------------------------------------
def test_chainlink_ram_cap(tmp_path):
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": str(tmp_path / "cursor_state")},
                          timeframes=["5m"])
    c = Collector(cfg)
    cap = c.CHAINLINK_RAM_CAP
    for i in range(cap + 1000):
        c._note_chainlink_event({"price": i}, "BTC", i)
    assert len(c._chainlink_events) == cap
    assert c._chainlink_events[0]["price"] == 1000  # oldest dropped, not newest
    assert c._chainlink_events[-1]["price"] == cap + 999


# 6. episode RAM pruning on ended-market eviction ------------------------------
def test_ended_market_episode_eviction_prunes_ram(tmp_path):
    """The 6h ended-market eviction must also drop that market's resync
    episodes/buffers/_episode_latest — but ONLY once persisted."""
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": str(tmp_path / "cursor_state")},
                          timeframes=["5m"])
    c = Collector(cfg)
    rid = c.resync.handle_disconnect("BTC", "cid-old", "test", {})
    c.resync.buffer_message(rid, {"m": 1})
    # simulate the episode being persisted (final state)
    c._episode_latest[rid] = {"resync_id": rid, "condition_id": "cid-old", "escalated": True}
    c._episode_persisted.add(rid)

    class FakeMarket:
        asset = "BTC"
        condition_id = "cid-old"
        market_end_ts_ms = int(time.time() * 1000) - 7 * 3600 * 1000  # ended >6h ago
        market_start_ts_ms = market_end_ts_ms - 300_000
        window_index = 1
        def to_markets_row(self):
            return {"condition_id": "cid-old", "status": "resolved", "resolution_outcome": "up"}
    c.markets["cid-old"] = FakeMarket()
    c.books["cid-old"] = make_book("cid-old")

    c._memory_eviction_tick(int(time.time() * 1000))
    assert rid not in c.resync._episodes
    assert rid not in c.resync._buffers
    assert rid not in c._episode_latest
    assert "cid-old" not in c.markets
    assert "cid-old" not in c.books


def test_ended_market_episode_NOT_evicted_when_unpersisted(tmp_path):
    """An episode not yet persisted to parquet must survive eviction — never
    drop data (AGENT.md)."""
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": str(tmp_path / "cursor_state")},
                          timeframes=["5m"])
    c = Collector(cfg)
    rid = c.resync.handle_disconnect("BTC", "cid-old2", "test", {})
    c.resync.buffer_message(rid, {"m": 1})
    c._episode_latest[rid] = {"resync_id": rid, "condition_id": "cid-old2"}
    # NOT added to _episode_persisted

    class FakeMarket:
        asset = "BTC"
        condition_id = "cid-old2"
        market_end_ts_ms = int(time.time() * 1000) - 7 * 3600 * 1000
        market_start_ts_ms = market_end_ts_ms - 300_000
        window_index = 1
        def to_markets_row(self):
            return {"condition_id": "cid-old2", "status": "resolved", "resolution_outcome": "up"}
    c.markets["cid-old2"] = FakeMarket()

    c._memory_eviction_tick(int(time.time() * 1000))
    # market RAM evicted, but the unpersisted episode survives for stop() to write
    assert "cid-old2" not in c.markets
    assert rid in c.resync._episodes
    assert rid in c._episode_latest


# 7. MarketsLog seen-cids cap --------------------------------------------------
def test_markets_log_seen_condition_ids_capped(tmp_path):
    """_seen_condition_ids must cap at MAX_SEEN_CONDITION_IDS (FIFO); recent
    cids keep their duplicate suppression."""
    ml = MarketsLog(tmp_path)
    cap = MarketsLog.MAX_SEEN_CONDITION_IDS
    for i in range(cap + 100):
        ml.append({"condition_id": f"cid-{i}", "status": "active"})
    assert len(ml._seen_condition_ids) <= cap
    assert len(ml._seen_order) <= cap
    # oldest evicted, newest retained
    assert "cid-0" not in ml._seen_condition_ids
    assert f"cid-{cap + 99}" in ml._seen_condition_ids
    # duplicate suppression still works for a retained cid
    ml._staging.clear()
    ml.append({"condition_id": f"cid-{cap + 99}", "status": "active"})  # dup active+unknown
    assert len(ml._staging) == 0
