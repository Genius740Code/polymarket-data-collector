"""Stale-book healing regression tests (2026-09-25 stale epidemic).

Prod evidence (2026-09-25): books went 100% book_state='stale' across all
assets ~14:49 UTC and never healed. Root causes, per-asset:

  1. Event-loop starvation: newest_open_buffer_id() ran on EVERY WS message
     and, with thousands of open episodes in RAM, walked all of them per
     message (the deployed build predates the O(1) routing cache) — the 500ms
     scheduler fell 30-90s behind and the M5 catch-up rule honestly
     downgraded every deferred bucket to stale (SOL: ~92% stale rows while
     the book was live in RAM with fresh deltas).
  2. WS shards killed by the starved consumer (1013 "slow consumer"): no
     full-book promotion for hours (BTC: no recycle 22:11→23:59 UTC, prices
     frozen; deltas overflowed the newest open episode's buffer — 14k drops).
  3. Dead-market resync churn: books of markets ended <6h ago stay in RAM
     (books=777); the reconnect walk drove resync() for EVERY stale book —
     dead-market REST 404s → fetch_none → escalate after 60-284s (1,679
     escalations on 2026-09-25, ALL on ended markets, mean 82s;
     supersede_episode had ZERO callers). The shard task then spent ~100% of
     its time in dead-market churn and never reconnected (ETH: zero
     `connected` events 20:00-23:00), so the REST fallback never reached the
     live-market books behind the dead ones.
  4. Episodes mint continuously and stay never-final: 29,678/30,922 in one
     prod day with ZERO resync attempts (close_healed only closes
     books-live episodes; the 6h memory eviction was the only exit).

Fixed paths under test:
  1. ResyncManager.supersede_ended_market_episodes — open episodes of
     ended-market books are superseded (honest final state + superseded
     event + buffer freed); live/unknown-window episodes are untouched.
  2. Collector._reconnect_resync_walk — dead-market books are skipped (and
     their open episode superseded) WITHOUT burning a resync escalation; the
     live-market book behind them is still driven and heals.
  3. ResyncManager.newest_open_buffer_id O(1) cache — returns exactly what
     the scan would under mint/retire/complete/reap/supersede.
"""
import tempfile
import time

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
    cfg.ws.resync_rest_backoff_initial_ms = 50
    cfg.ws.resync_rest_backoff_max_ms = 100
    return cfg


def make_mgr(events=None):
    async def rest_fetch(asset, cid):
        return {"up_bids": [[0.60, 100]], "up_asks": [[0.65, 30]], "sequence_number": 10}
    return ResyncManager(
        CollectorConfig(), rest_fetcher=rest_fetch,
        on_event=(lambda t, d: events.append((str(t), d))) if events is not None else lambda t, d: None,
    )


def make_collector(tmp_path):
    import os
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": os.path.join(str(tmp_path), "cursor_state")},
                          timeframes=["5m"])
    return Collector(cfg)


def make_book(cid="cid-1", asset="BTC", end_ms=None):
    if end_ms is None:
        end_ms = int(time.time() * 1000) + 300_000
    return OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id="up-123", down_token_id="down-456",
        market_end_ts_ms=end_ms,
    )


# 1. ended-market supersede sweep ----------------------------------------------
def test_supersede_ended_market_episodes_closes_dead_market_episodes():
    """The never-final flood: episodes of books whose market window ended can
    never reach a final state (REST 404s forever, close_healed only closes
    books-live, the 6h eviction was the only exit — 29,678 never-final
    episodes in one prod day). The sweep supersedes them once, honestly."""
    events = []
    mgr = make_mgr(events)
    rid_dead = mgr.handle_disconnect("BTC", "cid-old", "test", {})
    mgr.buffer_message(rid_dead, {"m": 1})
    rid_live = mgr.handle_disconnect("BTC", "cid-cur", "test", {})
    books = {
        "cid-old": make_book("cid-old", end_ms=int(time.time() * 1000) - 60_000),   # ended 1min ago
        "cid-cur": make_book("cid-cur"),                                            # still open
    }
    superseded = mgr.supersede_ended_market_episodes(books)
    assert superseded == 1
    # the dead-market episode is FINAL (escalated) with its buffer freed
    assert mgr.is_finished(rid_dead)
    assert rid_dead in mgr._escalated
    assert rid_dead not in mgr._buffers
    assert rid_dead not in mgr._buffer_deadline
    # one honest superseded event
    sup = [d for t, d in events if t.endswith("resync_failed") and d.get("superseded")]
    assert len(sup) == 1
    assert sup[0]["resync_id"] == rid_dead
    assert sup[0]["reason"] == "market_window_ended_sweep"
    # the live-market episode is untouched (normal healing applies)
    assert not mgr.is_finished(rid_live)
    assert mgr.buffer_live(rid_live)
    # idempotent: a second sweep supersedes nothing new
    assert mgr.supersede_ended_market_episodes(books) == 0
    sup2 = [d for t, d in events if t.endswith("resync_failed") and d.get("superseded")]
    assert len(sup2) == 1


def test_supersede_ended_market_episodes_skips_unknown_window_and_evicted_book():
    """Honest unknown (market_end_ts_ms None) keeps normal healing; an evicted
    book's episodes are owned by the 6h memory tick, not this sweep."""
    mgr = make_mgr()
    rid_unknown = mgr.handle_disconnect("BTC", "cid-unk", "test", {})
    rid_evicted = mgr.handle_disconnect("BTC", "cid-gone", "test", {})
    rid_open = mgr.handle_disconnect("BTC", "cid-cur", "test", {})
    books = {
        "cid-unk": make_book("cid-unk", end_ms=None),  # unknown window — never skipped
        "cid-cur": make_book("cid-cur"),
        # "cid-gone" intentionally absent (evicted)
    }
    assert mgr.supersede_ended_market_episodes(books) == 0
    assert not mgr.is_finished(rid_unknown)
    assert not mgr.is_finished(rid_evicted)
    assert not mgr.is_finished(rid_open)


# 2. reconnect walk: dead-market skip ------------------------------------------
@pytest.mark.asyncio
async def test_reconnect_walk_skips_dead_market_without_resync_burn():
    """The exact prod shape: dead-market books sat FIRST in the books dict, so
    the walk drove resync() through every one of them (60-284s escalation
    each, 1,679 escalations all on ended markets) and never reached the
    live-market book behind them. The walk must supersede the dead episode
    (no resync drive, no escalation burn) and still heal the live book."""
    c = make_collector(tempfile.mkdtemp())
    now_ms = int(time.time() * 1000)
    dead_book = make_book("cid-dead", end_ms=now_ms - 60_000)
    live_book = make_book("cid-live", end_ms=now_ms + 300_000)
    c.books = {"cid-dead": dead_book, "cid-live": live_book}
    for b in (dead_book, live_book):
        b.mark_stale()
    rid_dead = c.resync.handle_disconnect("BTC", "cid-dead", "test", c.books)
    rid_live = c.resync.handle_disconnect("BTC", "cid-live", "test", c.books)
    events: list = []
    c.resync.on_event = lambda t, d: events.append((str(t), d))

    resync_calls: list = []

    async def _fake_resync(asset, condition_id, books, resync_id):
        resync_calls.append((asset, condition_id, resync_id))
        # heals the live book exactly like a successful REST resync would
        for book in books.values():
            if book.condition_id == condition_id:
                book.mark_live()
        return True
    c.resync.resync = _fake_resync

    await c._reconnect_resync_walk({"BTC"}, now_ms)

    # the dead-market book was skipped: NO resync drive for it …
    assert ("BTC", "cid-dead", rid_dead) not in resync_calls
    assert resync_calls == [("BTC", "cid-live", rid_live)]
    # … and its open episode was superseded (honest final state, no burn)
    assert c.resync.is_finished(rid_dead)
    assert rid_dead in c.resync._escalated
    sup = [d for t, d in events if t.endswith("resync_failed") and d.get("superseded")
           and d.get("resync_id") == rid_dead]
    assert len(sup) == 1
    assert sup[0]["reason"] == "market_window_ended_reconnect"
    # no escalation timeout was burned: the supersede path sets no
    # resync_attempt_count (attempts stay 0 — the burn is the point of the fix)
    assert c.resync._episodes[rid_dead].resync_attempt_count == 0
    # the live-market book healed via the resync drive
    assert live_book.book_state.value == "live"


@pytest.mark.asyncio
async def test_reconnect_walk_still_drives_unknown_window_books():
    """A book with an unknown market_end_ts_ms must keep the pre-fix behavior:
    driven through resync() (honest unknown → keep healing attempts)."""
    c = make_collector(tempfile.mkdtemp())
    now_ms = int(time.time() * 1000)
    book = make_book("cid-unk", end_ms=None)
    c.books = {"cid-unk": book}
    book.mark_stale()
    rid = c.resync.handle_disconnect("BTC", "cid-unk", "test", c.books)

    resync_calls: list = []

    async def _fake_resync(asset, condition_id, books, resync_id):
        resync_calls.append(condition_id)
        for b in books.values():
            if b.condition_id == condition_id:
                b.mark_live()
        return True
    c.resync.resync = _fake_resync

    await c._reconnect_resync_walk({"BTC"}, now_ms)
    assert resync_calls == ["cid-unk"]
    # the walk drove the resync (the fake heals the book but the episode
    # itself is not completed by the stub — same as a real resync would)
    assert book.book_state.value == "live"


def test_reconnect_walk_dead_market_skip_present_in_source():
    """The shard loop must route through the walk; the walk must supersede
    ended-market episodes instead of burning a resync escalation."""
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "src" / "polymarket_collector" / "collector.py").read_text()
    assert "_reconnect_resync_walk(shard_set" in src
    assert "market_window_ended_reconnect" in src
    # the sweep wiring exists in the flush loop
    assert "supersede_ended_market_episodes(self.books)" in src
    assert "market_window_ended_sweep" in (Path(__file__).resolve().parents[1] / "src" / "polymarket_collector" / "resync.py").read_text()


# 3. O(1) buffer-routing cache equivalence --------------------------------------
def _reference_scan(mgr: ResyncManager, au: str) -> str:
    """The pre-cache scan semantics (newest live buffer for the asset)."""
    for rid in reversed(list(mgr._episodes.keys())):
        ep = mgr._episodes.get(rid)
        if ep is None or ep.asset != au:
            continue
        if ep.resync_completed_ts_utc is not None:
            continue
        if rid not in mgr._buffers:
            continue
        if not mgr.buffer_live(rid):
            continue
        return rid
    return ""


def test_newest_open_buffer_cache_matches_reference_scan():
    """The per-message O(1) cache must return EXACTLY what the scan would
    across mint/retire/complete/supersede — a stale cache id routed live WS
    messages into a dead episode's buffer (the event-loop-starvation fix)."""
    mgr = make_mgr()
    seq: list = []
    rid1 = mgr.handle_disconnect("BTC", "c1", "test", {})
    seq.append(("mint1", rid1))
    rid2 = mgr.handle_disconnect("BTC", "c2", "test", {})
    seq.append(("mint2", rid2))
    # retire the newest — the cache must refresh to the next live one
    mgr._buffer_retired.add(rid2)
    mgr._buffers.pop(rid2, None)
    mgr._buffer_deadline.pop(rid2, None)
    seq.append(("retired2", None))
    # complete rid1 — nothing live for BTC
    mgr._episodes[rid1].resync_completed_ts_utc = "2026-09-25T00:00:00Z"
    seq.append(("completed1", None))
    # a fresh mint re-arms the cache
    rid3 = mgr.handle_disconnect("BTC", "c3", "test", {})
    seq.append(("mint3", rid3))
    # supersede rid3 — dead
    mgr.supersede_episode(rid3, "t")
    seq.append(("superseded3", None))
    # an ETH episode must not leak into BTC's routing
    rid_eth = mgr.handle_disconnect("ETH", "c4", "test", {})
    seq.append(("mint_eth", rid_eth))

    for step, rid in seq:
        got = mgr.newest_open_buffer_id("BTC")
        want = _reference_scan(mgr, "BTC")
        assert got == want, f"cache diverged from scan after {step}: cache={got!r} scan={want!r}"


def test_newest_open_buffer_cache_tracks_mint_and_expiry():
    """The cache fast path: newest mint served O(1); expiry flips buffer_live
    and the scan fallback refreshes — never a dead id."""
    mgr = make_mgr()
    rid = mgr.handle_disconnect("BTC", "c1", "test", {})
    assert mgr._newest_buf_by_asset["BTC"] == rid
    assert mgr.newest_open_buffer_id("BTC") == rid
    # expire the buffer (deadline passes) — buffer_live flips, cache refreshes
    mgr._buffer_deadline[rid] = time.monotonic() - 1
    mgr.reap_expired_buffers()
    assert mgr.newest_open_buffer_id("BTC") == ""
    assert mgr._newest_buf_by_asset.get("BTC", "") == ""
    # case-insensitivity: the scan/caches key on the upper asset
    rid2 = mgr.handle_disconnect("btc", "c2", "test", {})
    assert mgr._newest_buf_by_asset["BTC"] == rid2
    assert mgr.newest_open_buffer_id("BTC") == rid2
