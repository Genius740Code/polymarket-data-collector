"""Audit 2026-09-16 round-2 fixes: M1 (1d market_id NULL), C4 (zombie buffers),
C2 (ended-window supersede), M2 (duplicate_event throttle).

Data evidence: *-1d market_id NULL on all 7 assets while markets_latest knows
the ids; XRP 1.36M drops in one never-completing episode; resync 404-retry
live-lock on expired windows; duplicate_event ~8/s noise bloat.
"""
import time

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager
from tests.test_resync import make_book


def _mk_book(**kw):
    base = dict(asset="BTC", condition_id="0x" + "ab" * 32, market_id=None,
                series_id="BTC-1d", window_index=20766,
                up_token_id="1", down_token_id="2",
                market_end_ts_ms=int(time.time() * 1000) + 3_600_000)
    base.update(kw)
    return OrderBookState(**base)


# -- M1: heal_market_id -----------------------------------------------------
def test_heal_market_id_fills_none():
    b = _mk_book(market_id=None)
    assert b.market_id is None
    assert b.heal_market_id("4558958") is True
    assert b.market_id == "4558958"


def test_heal_market_id_keeps_existing_and_rejects_hex():
    b = _mk_book(market_id="4558958")
    assert b.heal_market_id("9999999") is False
    assert b.market_id == "4558958"
    b2 = _mk_book(market_id=None)
    assert b2.heal_market_id("0x" + "ab" * 32) is False
    assert b2.market_id is None


def test_export_market_id_map_heals_nulls():
    pa = pytest.importorskip("pyarrow")
    from polymarket_collector.storage.export import _apply_market_id_map
    t = pa.table({"condition_id": ["cid-1", "cid-2", "cid-3"],
                  "market_id": [None, "0x" + "ab" * 32, "keep"]})
    out = _apply_market_id_map(t, {"cid-1": "111", "cid-2": "222"})
    assert out.column("market_id").to_pylist() == ["111", "222", "keep"]


def test_export_market_id_map_unknown_cid_stays_null():
    pa = pytest.importorskip("pyarrow")
    from polymarket_collector.storage.export import _apply_market_id_map
    t = pa.table({"condition_id": ["cid-x"], "market_id": [None]})
    out = _apply_market_id_map(t, {"other": "111"})
    assert out.column("market_id").to_pylist() == [None]


# -- C4: zombie buffer retirement -------------------------------------------
def test_buffer_live_retires_after_deadline():
    cfg = CollectorConfig()
    books = {"cid-1": make_book("cid-1")}
    events = []
    mgr = ResyncManager(cfg, rest_fetcher=None,
                        on_event=lambda t, d: events.append((str(t), d)))
    rid = mgr.handle_disconnect("BTC", "cid-1", reason="test", books=books)
    assert mgr.buffer_live(rid) is True
    # force-expire the deadline (no sleeping: deadline is monotonic-based)
    mgr._buffer_deadline[rid] = time.monotonic() - 1.0
    assert mgr.buffer_live(rid) is False
    mgr.buffer_message(rid, {"seq": 1})
    mgr.buffer_message(rid, {"seq": 2})
    # refused, not appended; retired buffer is popped (audit 2026-09-21), so
    # .get() — nothing accumulates on the zombie either way
    assert len(mgr._buffers.get(rid, [])) == 0  # refused, not appended
    retired = [d for t, d in events if isinstance(d, dict)
               and d.get("reason") == "resync_buffer_retired"]
    assert len(retired) == 1  # exactly one honest event, then silence
    assert retired[0]["asset"] == "BTC"


def test_buffer_live_false_when_finished_or_missing():
    cfg = CollectorConfig()
    mgr = ResyncManager(cfg, rest_fetcher=None, on_event=lambda *_: None)
    assert mgr.buffer_live("nope") is False


# -- C2: supersede ended-window episodes ------------------------------------
def test_supersede_episode_closes_and_escalates():
    cfg = CollectorConfig()
    books = {"cid-1": make_book("cid-1")}
    events = []
    persisted = []
    mgr = ResyncManager(cfg, rest_fetcher=None,
                        on_event=lambda t, d: events.append((str(t), d)),
                        on_episode_persist=persisted.append)
    rid = mgr.handle_disconnect("BTC", "cid-1", reason="test", books=books)
    assert mgr.supersede_episode(rid, reason="market_ended") is True
    assert mgr.is_finished(rid) is True
    assert mgr.buffer_live(rid) is False
    fails = [d for t, d in events if t.endswith("resync_failed")]
    assert fails and fails[0]["superseded"] is True
    assert fails[0]["reason"] == "market_ended"
    # idempotent: second call is a no-op
    assert mgr.supersede_episode(rid, reason="market_ended") is False


# -- M2: duplicate_event throttle -------------------------------------------
def test_duplicate_event_throttled_but_counted(tmp_path):
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    events = []
    w = ParquetWriter(str(tmp_path), wal_enabled=False,
                      on_event=lambda t, d: events.append((str(t), d)))
    row = {"asset": "BTC", "condition_id": "cid-1",
           "ts_snapshot_ns": 1_789_566_968_000_000_000}
    assert w.append("book_snapshots_500ms", row) is True
    # same key 5 more times: dropped silently except the first event
    for _ in range(5):
        assert w.append("book_snapshots_500ms", row) is True
    assert w._dupevent_count["book_snapshots_500ms"] == 5
    assert len(events) == 1
    assert events[0][1]["dropped_total"] == 1


# -- C6: noon-ET 1d window flooring ------------------------------------------
def test_noon_et_floor_vectors():
    from polymarket_collector.rollover import _noon_et_floor
    import datetime as dt
    # 2026-09-16T16:10:00Z = 12:10 EDT -> anchor today 12:00 ET = 16:00Z
    assert _noon_et_floor(1789575000) == 1789574400
    # 2026-09-16T10:00:00Z = 06:00 ET -> anchor yesterday noon ET = 09-15 16:00Z
    assert _noon_et_floor(1789552800) == 1789488000
    # winter (EST, UTC-5): 2026-12-16T16:30:00Z = 11:30 EST -> anchor 12-15 17:00Z
    assert _noon_et_floor(int(dt.datetime(2026, 12, 16, 16, 30, tzinfo=dt.timezone.utc).timestamp())) == \
        int(dt.datetime(2026, 12, 15, 17, 0, tzinfo=dt.timezone.utc).timestamp())
    # exact boundary noon ET belongs to the new window
    assert _noon_et_floor(1789574400) == 1789574400


def test_1d_slug_after_noon_et_names_next_window():
    from polymarket_collector.rollover import MarketDiscovery
    d = MarketDiscovery(rest_market_url="", poll_interval_s=60.0, window_size_seconds=86400)
    # 16:10 UTC Sep 16 (after noon ET): floored ts must yield the Sep-17 slug
    ts = d._ts_for_after(1789575000 * 1000)
    assert d._slug_for("BTC", ts) == "bitcoin-up-or-down-on-september-17-2026"
    # 10:00 UTC Sep 16 (before noon ET): still the Sep-16 slug
    ts2 = d._ts_for_after(1789552800 * 1000)
    assert d._slug_for("BTC", ts2) == "bitcoin-up-or-down-on-september-16-2026"


def test_5m_floor_unchanged():
    from polymarket_collector.rollover import MarketDiscovery
    d = MarketDiscovery(rest_market_url="", poll_interval_s=2.0, window_size_seconds=300)
    assert d._ts_for_after(1789575000 * 1000) == 1789575000 // 300 * 300
