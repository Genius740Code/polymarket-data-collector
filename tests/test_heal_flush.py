"""Heal-tick / flush-loop decoupling (finding #7, 2026-10-09 audit).

The flush loop used to AWAIT _background_heal_tick(max_books=40) inline —
up to 40 sequential resync() drives, each allowed up to
max_resync_duration_seconds (config/collector.yaml:224 = 60s), parked the
parquet flush behind heal REST traffic: scheduler-lag coupling on a
saturated 2-core box plus slow-consumer-close risk.

Fix under test: the loop only SCHEDULES a detached, bounded pass —
  * the parquet flush proceeds independent of heal outcomes;
  * a GLOBAL cap (HEAL_TICK_CONCURRENCY) bounds in-flight drives;
  * a per-pass wall-clock budget (HEAL_TICK_PASS_BUDGET_S) stops NEW
    drive starts past the deadline; started drives finish detached and
    still emit their heal events (fail-closed evidence semantics).

Stubs + patched clocks only — no network, no sockets, no market data,
no real REST. Everything writes under tmp_path. Sync-test harness with
asyncio.run scenarios, matching the neighbor suites (no async plugin).
"""
import asyncio
import os
import time

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig


def _make_collector(tmp_path, flush_interval_s=60):
    cfg = CollectorConfig(
        assets=["BTC"],
        storage={"data_dir": str(tmp_path), "flush_interval_seconds": flush_interval_s},
        cursor_store={"path": os.path.join(str(tmp_path), "cursor_state")},
        timeframes=["5m"],
    )
    return Collector(cfg)


def _stale_book(cid, asset="BTC"):
    b = OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id=f"up-{cid}", down_token_id=f"dn-{cid}",
        market_end_ts_ms=int(time.time() * 1000) + 3600_000,
    )
    b.mark_stale()
    return b


async def _until(pred, timeout_s=5.0, step_s=0.01):
    _end = time.monotonic() + timeout_s
    while time.monotonic() < _end:
        if pred():
            return True
        await asyncio.sleep(step_s)
    return False


# 1. flush never waits on heal outcomes ----------------------------------------

def test_flush_proceeds_while_heal_drive_blocked(tmp_path, monkeypatch):
    """Parquet flush ticks land while a heal drive is stuck mid-REST.

    Regression for finding #7: with the old inline `await
    _background_heal_tick(max_books=40)` this scenario hangs — the flush
    parks behind the first blocked resync() and writer.flush never runs.
    """

    async def _scenario():
        coll = _make_collector(tmp_path, flush_interval_s=1)
        for i in range(3):
            cid = f"cid-block-{i}"
            coll.books[cid] = _stale_book(cid)

        entered = asyncio.Event()
        gate = asyncio.Event()
        drv = {"inflight": 0}

        async def stub_resync(asset, condition_id, books, resync_id):
            drv["inflight"] += 1
            entered.set()
            await gate.wait()  # drive never completes until the test releases it
            drv["inflight"] -= 1
            return True

        monkeypatch.setattr(coll.resync, "resync", stub_resync)

        # writer.flush is shared by other paths (episode persistence), so the
        # flush LOOP's completed tick is marked where only it goes right after
        # its parquet flush: _persist_cursor_sync. Each tick records how many
        # heal drives were in flight at that moment.
        ticks = []
        monkeypatch.setattr(coll, "_persist_cursor_sync", lambda: ticks.append(drv["inflight"]))
        flushes = []
        monkeypatch.setattr(coll.writer, "flush", lambda: flushes.append(drv["inflight"]) or 0)

        async def _noop_drift():
            return None

        monkeypatch.setattr(coll, "_periodic_drift_tick", _noop_drift)
        monkeypatch.setattr(coll, "_disk_guard_tick", lambda: None)

        coll._running = True
        loop_task = asyncio.create_task(coll._flush_loop())
        try:
            assert await asyncio.wait_for(_until(entered.is_set), timeout=10)
            assert drv["inflight"] >= 1  # a heal drive is parked mid-REST
            assert await asyncio.wait_for(
                _until(lambda: sum(1 for n in ticks if n > 0) >= 2), timeout=10)
            # >=2 flush-loop ticks completed while heal drives were still
            # blocked mid-REST: the flush never waited on heal outcomes
            assert len(flushes) >= 2
            assert drv["inflight"] >= 1  # ...and the heal is STILL not done
        finally:
            gate.set()
            coll._running = False
            loop_task.cancel()
            await asyncio.gather(loop_task, return_exceptions=True)

    asyncio.run(_scenario())


# 2. global in-flight drive cap --------------------------------------------------

def test_heal_pass_respects_global_concurrency_cap(tmp_path, monkeypatch):
    """At most HEAL_TICK_CONCURRENCY drives hold REST slots at once, and a
    second scheduled pass never double-drives a book with a live drive."""

    async def _scenario():
        coll = _make_collector(tmp_path)
        cids = [f"cid-cap-{i}" for i in range(6)]
        for cid in cids:
            coll.books[cid] = _stale_book(cid)

        gate = asyncio.Event()
        drv = {"inflight": 0, "peak": 0, "calls": []}

        async def stub_resync(asset, condition_id, books, resync_id):
            drv["calls"].append(condition_id)
            drv["inflight"] += 1
            drv["peak"] = max(drv["peak"], drv["inflight"])
            await gate.wait()
            drv["inflight"] -= 1
            return True

        monkeypatch.setattr(coll.resync, "resync", stub_resync)
        coll._running = True
        try:
            coll._schedule_heal_tick_pass()
            shared = coll._heal_tick_priv
            # 6 drive tasks start; the global cap admits exactly HEAL_TICK_CONCURRENCY
            assert await _until(lambda: drv["inflight"] == coll.HEAL_TICK_CONCURRENCY)
            assert drv["inflight"] == coll.HEAL_TICK_CONCURRENCY
            # the pass itself already returned: drives finish DETACHED
            assert not getattr(coll, "_heal_tick_pass_pending", False)
            # overlap check: a second scheduled pass must not re-drive books
            # whose drives are already in flight
            coll._schedule_heal_tick_pass()
            assert await _until(lambda: not getattr(coll, "_heal_tick_pass_pending", False))
            assert drv["calls"].count(drv["calls"][0]) == 1, drv["calls"]
            gate.set()
            assert await _until(lambda: len(drv["calls"]) == len(cids))
            assert sorted(drv["calls"]) == sorted(cids)  # each book healed exactly once
            assert drv["peak"] <= coll.HEAL_TICK_CONCURRENCY
            assert await _until(lambda: not shared["tasks"] and not shared["inflight"])
        finally:
            gate.set()
            coll._running = False
            await asyncio.sleep(0)

    asyncio.run(_scenario())


# 3. per-pass wall-clock budget ----------------------------------------------------

def test_heal_pass_budget_stops_new_drive_starts(tmp_path, monkeypatch):
    """Past the pass budget no NEW drives start; the started ones still run
    to completion detached (their events land whenever they finish)."""

    async def _scenario():
        coll = _make_collector(tmp_path)
        cids = [f"cid-bud-{i}" for i in range(6)]
        for cid in cids:
            coll.books[cid] = _stale_book(cid)

        done = []

        async def stub_resync(asset, condition_id, books, resync_id):
            await asyncio.sleep(0)
            done.append(condition_id)
            return True

        monkeypatch.setattr(coll.resync, "resync", stub_resync)

        # patched clock: deadline computed at t=100 (+20s budget); the first
        # two start-checks fit under it, the third lands past it (t=500).
        tick = iter([100.0, 100.0, 100.0, 500.0])
        monkeypatch.setattr(coll, "_heal_tick_clock", lambda: next(tick, 500.0))

        coll._running = True
        try:
            coll._schedule_heal_tick_pass()
            shared = coll._heal_tick_priv
            # both started drives ran to completion detached
            assert await _until(lambda: not shared["tasks"])
            assert len(done) == 2, done
            assert sorted(done) == sorted(cids[:2])  # books past the budget: never driven
            assert not shared["inflight"]
            assert not getattr(coll, "_heal_tick_pass_pending", False)
        finally:
            coll._running = False
            await asyncio.sleep(0)

    asyncio.run(_scenario())
