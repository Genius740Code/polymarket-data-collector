"""Reaper independence + episode-dict eviction bounds (2026-10-04).

Covers the resync-reaper resync (independent 60s cadence, lock-free) and the
_episode_latest finished-only FIFO + never-final TTL close-as-unresolved:

  1. _resync_reaper_tick reaps expired buffers + closes healed episodes while
     the kaggle lock is held by a simulated export (never contends it).
  2. Buffer bytes pinned during an export-hold drop to zero after one tick.
  3. _episode_latest evicts finished+persisted past N=500 (oldest first) and
     never evicts open or unwritten entries.
  4. close_expired_unresolved closes abandoned never-final episodes with the
     gap evidence row kept (disconnect_ts preserved, no completion stamp, no
     snapshot fills) and future resync() drives no-op (no retry burn).
  5. The TTL spares young episodes, live-buffer episodes, and recently
     driven ones.
  6. _reaper_loop ticks on its own cadence while the export holds the lock.

Fakes/monkeypatch only — real ResyncManager + real Collector (tmp data_dir).
"""
import asyncio
import datetime
import os
import time

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager


def _cfg(tmp_path):
    return CollectorConfig(
        assets=["BTC"],
        storage={"data_dir": str(tmp_path)},
        cursor_store={"path": os.path.join(str(tmp_path), "cursor_state")},
        timeframes=["5m"],
    )


def _book(cid="cid-1", asset="BTC"):
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


def _mgr(events=None, persists=None):
    async def _never_fetch(asset, cid):
        raise AssertionError("no REST drive expected")

    return ResyncManager(
        CollectorConfig(assets=["BTC"]),
        rest_fetcher=_never_fetch,
        on_event=(lambda t, d: (events if events is not None else []).append((t, d))),
        on_episode_persist=(lambda d: (persists if persists is not None else []).append(dict(d))),
    )


def _iso_hours_ago(hours):
    return (datetime.datetime.now(tz=datetime.timezone.utc)
            - datetime.timedelta(hours=hours)).isoformat().replace("+00:00", "Z")


def _iso_now():
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


# 1. reaper runs without the lock -------------------------------------------

async def test_reaper_tick_reaps_and_closes_while_export_holds_lock(tmp_path):
    col = Collector(_cfg(tmp_path))
    books = {"cid-1": _book("cid-1"), "cid-2": _book("cid-2")}
    col.books.update(books)
    rid_healed = col.resync.handle_disconnect("BTC", "cid-1", reason="t", books=books)
    rid_stuck = col.resync.handle_disconnect("BTC", "cid-2", reason="t", books=books)
    # one book heals via promotion (the path that never reaches resync())
    books["cid-1"].mark_live()
    assert books["cid-2"].book_state.value == "stale"
    for rid in (rid_healed, rid_stuck):
        for i in range(50):
            col.resync.buffer_message(rid, {"token_id": "up-123", "seq": i})
        col.resync._buffer_deadline[rid] = time.monotonic() - 1  # expired
    pinned_before = sum(len(q) for q in col.resync._buffers.values())
    assert pinned_before == 100

    # simulated export: holds the kaggle lock for the whole tick
    await col._kaggle_lock.acquire()
    try:
        assert col._kaggle_lock.locked()
        out = col._resync_reaper_tick()
        # the tick never released / stole the export's lock
        assert col._kaggle_lock.locked()
    finally:
        col._kaggle_lock.release()
    assert out["reaped"] >= 1  # expired buffer retired despite the held lock
    assert out["closed"] == 1  # healed episode closed despite the held lock
    assert sum(len(q) for q in col.resync._buffers.values()) == 0
    assert col.resync.get_episode(rid_stuck).resync_completed_ts_utc is None  # still honestly open
    assert books["cid-2"].book_state.value == "stale"  # reap never heals books


# 2. buffer bytes bounded under export-hold ---------------------------------

async def test_buffer_bytes_bounded_under_export_hold(tmp_path):
    col = Collector(_cfg(tmp_path))
    books = {}
    for i in range(20):
        cid = f"cid-buf-{i}"
        books[cid] = _book(cid)
    col.books.update(books)
    for cid in books:
        rid = col.resync.handle_disconnect("BTC", cid, reason="t", books=books)
        for j in range(200):
            col.resync.buffer_message(rid, {"token_id": "up-123", "seq": j, "pad": "x" * 64})
        col.resync._buffer_deadline[rid] = time.monotonic() - 1
    pinned_msgs = sum(len(q) for q in col.resync._buffers.values())
    pinned_bytes = sum(len(repr(m)) for q in col.resync._buffers.values() for m in q)
    assert pinned_msgs == 20 * 200 and pinned_bytes > 0

    await col._kaggle_lock.acquire()
    try:
        out = col._resync_reaper_tick()
    finally:
        col._kaggle_lock.release()
    assert out["reaped"] == 20
    assert sum(len(q) for q in col.resync._buffers.values()) == 0
    assert sum(len(repr(m)) for q in col.resync._buffers.values() for m in q) == 0
    # episodes themselves stay (open, retired) — only dead buffers are freed
    assert len(col.resync._episodes) == 20
    assert len(col.resync._buffer_retired) == 20


# 3. _episode_latest finished-only FIFO -------------------------------------

async def test_episode_latest_finished_cap_evicts_oldest_first(tmp_path):
    col = Collector(_cfg(tmp_path))
    # 600 finished+persisted entries, already evicted from resync (ep None = done)
    for i in range(600):
        rid = f"rid-old-{i:04d}"
        col._episode_latest[rid] = {"resync_id": rid, "asset": "BTC",
                                    "disconnect_ts_utc": _iso_now(),
                                    "resync_completed_ts_utc": _iso_now()}
        col._episode_persisted.add(rid)
    # 3 finished but UNWRITTEN (append failed) — the only copy, never evict
    for i in range(3):
        rid = f"rid-unwritten-{i}"
        col._episode_latest[rid] = {"resync_id": rid, "asset": "BTC",
                                    "disconnect_ts_utc": _iso_now(),
                                    "resync_completed_ts_utc": _iso_now()}
    # insert-path hook: minting a real open episode trims the finished backlog
    books = {"cid-open": _book("cid-open")}
    col.books.update(books)
    rid_open = col.resync.handle_disconnect("BTC", "cid-open", reason="t", books=books)
    assert len(col._episode_latest) <= col.MAX_EPISODE_LATEST
    assert rid_open in col._episode_latest  # open entries are never evicted
    for i in range(3):
        assert f"rid-unwritten-{i}" in col._episode_latest  # unwritten never dropped
    assert "rid-old-0000" not in col._episode_latest  # oldest finished evicted first
    # direct call is idempotent once under cap
    assert col._evict_finished_episode_latest() == 0


# 4. never-final TTL closes with evidence row --------------------------------

def test_never_final_ttl_close_keeps_gap_evidence():
    events, persists = [], []
    mgr = _mgr(events, persists)
    book = _book("cid-ttl")
    rid = mgr.handle_disconnect("BTC", "cid-ttl", reason="quiet_feed", books={"cid-ttl": book})
    ep = mgr.get_episode(rid)
    ep.disconnect_ts_utc = _iso_hours_ago(2)  # abandoned long ago
    mgr.buffer_message(rid, {"token_id": "up-123", "seq": 1})
    mgr._buffer_deadline[rid] = time.monotonic() - 1  # buffer dead
    assert ep.resync_attempt_count == 0  # never driven — the forensics shape

    closed = mgr.close_expired_unresolved()
    assert closed == 1
    assert mgr.is_finished(rid)
    assert rid not in mgr._buffers  # buffer freed
    assert book.book_state.value == "stale"  # book untouched — gap stays a gap
    final = mgr.get_episode(rid)
    assert final.resync_completed_ts_utc is None  # never claimed healed
    terminal_rows = [p for p in persists if p.get("resync_id") == rid and p.get("superseded") is True]
    assert terminal_rows, "TTL close must persist a terminal episode row"
    assert terminal_rows[-1]["disconnect_ts_utc"] == ep.disconnect_ts_utc  # gap evidence kept
    assert terminal_rows[-1]["supersede_reason"] == "never_final_ttl_expired"
    assert all("resync_id" in p for p in persists)  # episode rows only — no fills
    fail_events = [d for t, d in events if "resync_failed" in str(t)]
    assert any(d.get("unresolved") is True and d.get("resync_id") == rid for d in fail_events)


async def test_ttl_closed_episode_no_ops_future_drives():
    fetched = []

    async def _count_fetch(asset, cid):
        fetched.append(cid)
        return None

    mgr = ResyncManager(CollectorConfig(assets=["BTC"]), rest_fetcher=_count_fetch,
                        on_event=lambda *_: None, on_episode_persist=lambda *_: None)
    book = _book("cid-ttl2")
    rid = mgr.handle_disconnect("BTC", "cid-ttl2", reason="t", books={"cid-ttl2": book})
    mgr.get_episode(rid).disconnect_ts_utc = _iso_hours_ago(2)
    mgr._buffer_deadline[rid] = time.monotonic() - 1
    assert mgr.close_expired_unresolved() == 1
    assert await mgr.resync("BTC", "cid-ttl2", {"cid-ttl2": book}, rid) is False
    assert fetched == []  # escalated no-op — no retry burn


# 5. TTL spares the living ----------------------------------------------------

def test_never_final_ttl_spares_young_live_and_active():
    mgr = _mgr()
    books = {"cid-young": _book("cid-young"), "cid-livebuf": _book("cid-livebuf"),
             "cid-active": _book("cid-active")}
    r_young = mgr.handle_disconnect("BTC", "cid-young", reason="t", books=books)
    r_livebuf = mgr.handle_disconnect("BTC", "cid-livebuf", reason="t", books=books)
    r_active = mgr.handle_disconnect("BTC", "cid-active", reason="t", books=books)
    # old disconnect but buffer still live (deadline in the future)
    mgr.get_episode(r_livebuf).disconnect_ts_utc = _iso_hours_ago(2)
    mgr.buffer_message(r_livebuf, {"token_id": "up-123"})
    # old disconnect, dead buffer, but driven minutes ago (outage recovery)
    ep_active = mgr.get_episode(r_active)
    ep_active.disconnect_ts_utc = _iso_hours_ago(2)
    ep_active.resync_attempt_count = 3
    ep_active.resync_rest_fetch_ts_utc = _iso_now()
    mgr._buffer_deadline[r_active] = time.monotonic() - 1

    assert mgr.close_expired_unresolved() == 0
    for rid in (r_young, r_livebuf, r_active):
        assert not mgr.is_finished(rid)
    assert mgr.buffer_live(r_livebuf)  # live buffer untouched
    assert len(mgr._buffers[r_livebuf]) == 1


def test_never_final_ttl_unparseable_clock_fails_open():
    mgr = _mgr()
    books = {"cid-badclock": _book("cid-badclock")}
    rid = mgr.handle_disconnect("BTC", "cid-badclock", reason="t", books=books)
    mgr.get_episode(rid).disconnect_ts_utc = "not-a-timestamp"
    mgr._buffer_deadline[rid] = time.monotonic() - 1
    assert mgr.close_expired_unresolved() == 0
    assert not mgr.is_finished(rid)


# 6. independent loop ----------------------------------------------------------

async def test_reaper_loop_ticks_while_export_holds_lock(tmp_path):
    col = Collector(_cfg(tmp_path))
    books = {"cid-loop": _book("cid-loop")}
    col.books.update(books)
    rid = col.resync.handle_disconnect("BTC", "cid-loop", reason="t", books=books)
    col.resync.buffer_message(rid, {"token_id": "up-123"})
    col.resync._buffer_deadline[rid] = time.monotonic() - 1

    await col._kaggle_lock.acquire()
    col.REAPER_INTERVAL_S = 0.05
    col._running = True
    task = asyncio.create_task(col._reaper_loop(), name="reaper-test")
    try:
        await asyncio.sleep(0.3)
        assert rid in col.resync._buffer_retired  # reaped on the 60s-timer path, lock held
        assert col._kaggle_lock.locked()
    finally:
        col._running = False
        try:
            await asyncio.wait_for(task, timeout=5)
        except Exception:
            task.cancel()
        col._kaggle_lock.release()
