"""Stale-healing fix tests (2026-09-26 dead-market churn).

Prod evidence (2026-09-26): of 2,841 unique conditions with resync attempts,
2,186 (77%) were ENDED markets and 648 unknown to every registry — only 7
still open. The reconnect walk drove resync() for every stale book, each dead
market burning a full escalation; live books behind them starved and every
snapshot row shipped stale. Orphan books (cursor recovery for windows
discovery never returns: fake `{cid}-UP` tokens, end None, no market record)
churned forever outside every existing skip/eviction path.

Fixed paths under test:
  1. ResyncManager.resync pre-check: ended markets supersede with ZERO REST
     attempts; unknown markets still proceed (discovery may lag).
  2. resync() fast-abandon: fetch_none on an ended market supersedes
     immediately instead of riding the backoff/escalation burn.
  3. Collector._reconnect_resync_walk: current windows driven first;
     old orphans (no market record past grace) superseded without burn;
     young orphans still driven.
  4. Collector._heal_book_bg: ended markets never hit REST.
  5. Collector._memory_eviction_tick: old orphan books (+persisted episodes)
     evicted; young orphans and in-market books kept.
"""
import asyncio
import time
import types

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager


def make_cfg(tmpdir: str) -> CollectorConfig:
    cfg = CollectorConfig()
    cfg.storage.data_dir = tmpdir
    cfg.storage.wal_dir = tmpdir + "/_wal"
    cfg.raw_archive.path = tmpdir + "/raw_ws_archive"
    cfg.cursor_store.path = tmpdir + "/cursor_state"
    cfg.ws.max_resync_duration_seconds = 2
    cfg.ws.resync_rest_backoff_initial_ms = 20
    cfg.ws.resync_rest_backoff_max_ms = 50
    return cfg


def make_mgr(tmpdir, fetch=None, events=None, resolver=None):
    calls = []

    async def rest_fetch(asset, cid):
        calls.append(cid)
        if fetch is not None:
            return fetch(asset, cid)
        return {"up_bids": [[0.60, 100]], "up_asks": [[0.65, 30]],
                "down_bids": [[0.35, 100]], "down_asks": [[0.40, 30]],
                "sequence_number": 10}

    mgr = ResyncManager(
        make_cfg(tmpdir), rest_fetcher=rest_fetch,
        on_event=(lambda t, d: events.append((str(t), d))) if events is not None else lambda t, d: None,
    )
    if resolver is not None:
        mgr.market_status_resolver = resolver
    return mgr, calls


def make_book(cid="cid-1", asset="BTC", end_ms="future", tokens=None):
    now = int(time.time() * 1000)
    end = None if end_ms == "none" else (now + 3600_000 if end_ms == "future" else now - 3600_000)
    up, down = tokens or ("up-123", "down-456")
    b = OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id=up, down_token_id=down,
        market_end_ts_ms=end,
    )
    b.mark_stale()
    return b


def open_episode(mgr, asset, cid):
    rid = mgr.handle_disconnect(asset, cid, reason="test", books={})
    assert rid in mgr._episodes
    return rid


# 1. resync() pre-check ---------------------------------------------------------

def test_resync_precheck_supersedes_ended_market_with_zero_attempts(tmp_path):
    events = []
    mgr, calls = make_mgr(str(tmp_path), events=events,
                          resolver=lambda cid: (int(time.time() * 1000) - 1000, "resolved"))
    rid = open_episode(mgr, "BTC", "cid-dead")
    ok = asyncio.run(mgr.resync("BTC", "cid-dead", {}, rid))
    assert ok is False
    assert calls == []
    ep = mgr._episodes[rid]
    assert mgr.is_finished(rid)
    assert any("resync_failed" in str(t) and d.get("superseded") for t, d in events)


def test_resync_unknown_market_still_proceeds(tmp_path):
    mgr, calls = make_mgr(str(tmp_path), resolver=lambda cid: None)
    books = {}
    b = make_book(cid="cid-new", end_ms="future")
    books["cid-new"] = b
    rid = open_episode(mgr, "BTC", "cid-new")
    ok = asyncio.run(mgr.resync("BTC", "cid-new", books, rid))
    assert ok is True
    assert calls == ["cid-new"]
    assert b.book_state.value == "live"


def test_resync_fetch_none_on_ended_market_abandons_fast(tmp_path):
    mgr, calls = make_mgr(str(tmp_path), fetch=lambda a, c: None)
    now = int(time.time() * 1000)
    state = {"open": True}

    def flip_resolver(cid):
        # open at entry (pre-check passes), ended once attempts start
        if state["open"]:
            state["open"] = False
            return (now + 3600_000, "active")
        return (now - 1000, "resolved")

    mgr.market_status_resolver = flip_resolver
    rid = open_episode(mgr, "BTC", "cid-flip")
    ok = asyncio.run(mgr.resync("BTC", "cid-flip", {}, rid))
    assert ok is False
    assert len(calls) == 1  # abandoned after the first fetch_none, no escalation burn
    assert mgr.is_finished(rid)


# 2. walk ordering + orphan skip -------------------------------------------------

def make_collector(tmp_path):
    import os
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": os.path.join(str(tmp_path), "cursor_state")},
                          timeframes=["5m"])
    return Collector(cfg)


def plant_book(col, cid, end_kind, age_ms=0, tokens=None):
    b = make_book(cid=cid, end_ms=end_kind, tokens=tokens)
    if age_ms:
        b.created_ms = int(time.time() * 1000) - age_ms
    col.books[cid] = b
    try:
        col._index_book(b)
    except Exception:
        pass
    return b


def test_walk_drives_current_first_skips_old_orphans(tmp_path):
    col = make_collector(tmp_path)
    now = int(time.time() * 1000)
    fetched = []

    async def fake_fetch(asset, cid):
        fetched.append(cid)
        return {"up_bids": [[0.60, 100]], "up_asks": [[0.65, 30]],
                "down_bids": [[0.35, 100]], "down_asks": [[0.40, 30]],
                "sequence_number": 10}

    col.resync.rest_fetcher = fake_fetch
    # markets registry knows only the open market
    col.markets["cid-open"] = types.SimpleNamespace(market_end_ts_ms=now + 3600_000, status="active")
    plant_book(col, "cid-open", "future")
    plant_book(col, "cid-orphan-old", "none", age_ms=3600_000)
    plant_book(col, "cid-orphan-young", "none", age_ms=60_000)
    asyncio.run(col._reconnect_resync_walk({"BTC"}, now))
    assert "cid-open" in fetched  # current window driven (and healed live)
    assert "cid-orphan-old" not in fetched  # dead orphan: no burn
    assert col.books["cid-open"].book_state.value == "live"
    # next walk (open book healed away) drives the young orphan normally
    col.books.pop("cid-open")
    asyncio.run(col._reconnect_resync_walk({"BTC"}, now))
    assert "cid-orphan-young" in fetched  # grace: discovery may still arrive
    assert col.books["cid-orphan-old"].book_state.value == "stale"


# 3. background heal + eviction ---------------------------------------------------

def test_heal_bg_skips_ended_market(tmp_path):
    col = make_collector(tmp_path)
    calls = []

    async def fake_heal(book, market):
        calls.append(book.condition_id)
        return True

    col._fetch_and_apply_rest_book = fake_heal
    now = int(time.time() * 1000)
    b = make_book(cid="cid-x", end_ms="future")
    asyncio.run(col._heal_book_bg(b, types.SimpleNamespace(market_end_ts_ms=now - 1000)))
    assert calls == []
    asyncio.run(col._heal_book_bg(b, types.SimpleNamespace(market_end_ts_ms=now + 3600_000)))
    assert calls == ["cid-x"]


def test_eviction_drops_old_orphans_keeps_young_and_market_books(tmp_path):
    col = make_collector(tmp_path)
    now = int(time.time() * 1000)
    old = plant_book(col, "cid-old-orphan", "none", age_ms=7 * 3600_000)
    young = plant_book(col, "cid-young-orphan", "none", age_ms=60_000)
    kept = plant_book(col, "cid-known", "future")
    col.markets["cid-known"] = types.SimpleNamespace(market_end_ts_ms=now + 3600_000, status="active")
    for cid in ("cid-old-orphan", "cid-young-orphan", "cid-known"):
        rid = col.resync.handle_disconnect("BTC", cid, reason="test", books={})
        col._episode_persisted.add(rid)
    col._memory_eviction_tick(now)
    assert "cid-old-orphan" not in col.books
    assert "cid-young-orphan" in col.books
    assert "cid-known" in col.books
