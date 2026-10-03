"""Dual-WS chaos tests (2026-10-03 dual-socket workstream, branch perfect/pmdata-parity).

Real-data-only: WS frame dicts below are test FIXTURES for the pure
ws_pool dedup/state helpers (never written to data/). All live% / episode-rate
numbers come from parquet probes, not fixtures.

Covers (contract 1B):
  A. A-kill/B-continues: killing conn A mints NO episode while B flows
     (no fan-out), subscription state survives (no resubscribe storm).
  B. 1006-storm dedup: a redelivered frame is counted once (accepted==1,
     rest are duplicates), including multi-entry price_changes fan-out.
  C. Proactive recycle vs server-kill: OUR-close at <=280s mints no episode
     (fresh full-book relive); abnormal 1006 death mints exactly one episode
     per affected book with the real reason.
  D. Single-socket fallback: when A recycles, B alone carries the shard;
     when both die, NOTHING is selected (honest stale, never fabricated).
  E. Silence watchdog: data-dead socket marks books stale + persists an
     episode (never fills); a fresh conn with no frames yet never trips it.

SOAK PLAN (12h, no restart needed — pm2 id 11 keeps running):
  1. Log lines to watch (logs/collector-out-11.log):
     - "[ws:<SHARD>] planned recycle — reconnecting" (OUR-close, light relive)
     - "1006" close lines (server-kill, REST walk kept)
     - "[mem] rss=.. buf=.. eps=.." every ~5m -> CPU/RSS trend
  2. 1006-episode rate query (parquet, read-only):
       python3 scripts/measure_1006_rate.py
     Baseline 2026-10-03 20:49-22:07 UTC: 254 1006-ep/h (328/2095 rows in
     last-400 resync_episodes files). Soak PASS if rate drops vs baseline
     and no per-book fan-out growth (episodes/book/recycle stays ~1).
  3. live% query (parquet, last-3-files-per-asset, never full scan):
       python3 scripts/measure_1006_rate.py  (prints both numbers)
     Baseline 22:04 UTC: 4.87% live (3234 stale / 167 live / 29 resyncing,
     n=3430). Re-measured ~22:10 UTC: 38.68% live (n=2464) post-recycle.
     Soak PASS if live% holds above baseline window after each recycle.
  4. CPU/RSS: grep "\\[mem\\]" logs/collector-out-11.log | tail; collector
     mem rss~649MB buf~12684 eps~273, host CPU 94.8% at baseline.
"""
import time

from polymarket_collector.book import OrderBookState
from polymarket_collector.config import CollectorConfig
from polymarket_collector.ingest.ws_pool import (
    RECYCLE_MAX_S,
    RECYCLE_TARGET_S,
    SILENCE_WATCHDOG_S,
    ConnectionState,
    FrameDedup,
    ShardPool,
    ShardSubscriptions,
    should_recycle,
    silence_exceeded,
)
from polymarket_collector.resync import ResyncManager


def _make_cfg(tmpdir: str) -> CollectorConfig:
    cfg = CollectorConfig()
    cfg.storage.data_dir = tmpdir
    cfg.storage.wal_dir = tmpdir + "/_wal"
    cfg.raw_archive.path = tmpdir + "/raw_ws_archive"
    cfg.cursor_store.path = tmpdir + "/_cursor_state"
    cfg.ws.max_resync_duration_seconds = 2
    cfg.ws.resync_rest_backoff_initial_ms = 10
    cfg.ws.resync_rest_backoff_max_ms = 30
    return cfg


def _make_mgr(tmpdir: str) -> ResyncManager:
    async def _never(asset, cid):
        return None

    return ResyncManager(_make_cfg(tmpdir), rest_fetcher=_never)


def _make_book(cid="cid-dual-1", asset="BTC"):
    now = int(time.time() * 1000)
    return OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id="up-123", down_token_id="down-456",
        market_end_ts_ms=now + 3600_000,
    )


def _frame(token="tok-1", seq=10, ts_ms=1_790_000_000_000):
    # WS frame FIXTURE (test-only, never persisted): one token, seq + ts.
    return {"asset_id": token, "sequence_number": seq, "timestamp": ts_ms,
            "price": 0.60}


def _fan_frame(entries):
    # WS frame FIXTURE: multi-entry price_changes (rollover dual-tracking).
    return {"price_changes": [
        {"asset_id": tok, "sequence_number": seq, "timestamp": 1_790_000_000_000}
        for tok, seq in entries]}


# A. A-kill / B-continues -----------------------------------------------------

def test_a_kill_b_continues_no_episode_fanout(tmp_path):
    """Killing A while B flows must mint ZERO episodes and keep subscriptions."""
    now_ns = time.time_ns()
    pool = ShardPool(shard=["BTC"])
    pool.subs.initial_payload(["tok-1", "tok-2"])
    pool.conn_a.established_ns = now_ns - int(300 * 1e9)  # A past recycle ceiling
    pool.conn_b.established_ns = now_ns - int(10 * 1e9)
    pool.conn_b.note_frame(now_ns)  # B flows
    assert pool.conns_needing_recycle(now_ns) == ["A"]
    assert pool.conns_gone_silent(now_ns) == []
    # B-frames still accepted once each after the A-kill (continuity, no gap).
    assert pool.dedup.check_message(_frame()) is False
    assert pool.dedup.check_message(_frame()) is True  # A/B redelivery dupes
    assert pool.dedup.accepted == 1
    # Subscription state survives the single-conn kill: no reset, no storm.
    assert pool.subs.subscribed_tokens == {"tok-1", "tok-2"}
    mgr = _make_mgr(str(tmp_path))
    assert mgr._episodes == {}, "no disconnect minted while B flows"


# B. 1006-storm dedup ----------------------------------------------------------

def test_1006_storm_redelivery_counted_once():
    """50 redeliveries of one frame (post-1006 A/B overlap) count once."""
    dd = FrameDedup()
    for i in range(50):
        dup = dd.check_message(_frame())
        assert dup == (i > 0), f"delivery {i}: dup={dup}"
    assert dd.accepted == 1
    assert dd.duplicates == 49
    assert dd.check_message(_frame(seq=11)) is False  # new seq still flows


def test_1006_partial_fanout_frame_overlap():
    """Multi-entry frame: full redelivery dupes; partially-new delivers."""
    dd = FrameDedup()
    assert dd.check_message(_fan_frame([("t1", 1), ("t2", 2)])) is False
    assert dd.check_message(_fan_frame([("t1", 1), ("t2", 2)])) is True
    # t2 advanced on one leg: partially new -> deliver, record the new key.
    assert dd.check_message(_fan_frame([("t1", 1), ("t2", 3)])) is False
    assert dd.check_message(_fan_frame([("t1", 1), ("t2", 3)])) is True
    # keyless frames never dedupe (never drop on nothing).
    assert dd.check_message({"price": 0.5}) is False
    assert dd.check_message({"price": 0.5}) is False


# C. Proactive recycle vs server-kill ------------------------------------------

def test_proactive_recycle_thresholds_never_pass_server_kill():
    """OUR-close ceiling (280s) stays ahead of the ~5min server-side kill."""
    assert RECYCLE_TARGET_S < RECYCLE_MAX_S <= 300
    assert should_recycle(RECYCLE_MAX_S - 0.1) is False
    assert should_recycle(RECYCLE_MAX_S) is True
    assert should_recycle(600.0) is True
    now_ns = time.time_ns()
    conn = ConnectionState(name="A", established_ns=now_ns - int(100 * 1e9))
    assert conn.needs_recycle(now_ns) is False
    conn.established_ns = now_ns - int(281 * 1e9)
    assert conn.needs_recycle(now_ns) is True


def test_server_kill_mints_exactly_one_episode_per_book(tmp_path):
    """Abnormal 1006 death: one honest episode per affected book, stale, open."""
    mgr = _make_mgr(str(tmp_path))
    books = {"cid-k1": _make_book("cid-k1"), "cid-k2": _make_book("cid-k2")}
    rids = [mgr.handle_disconnect("BTC", cid, reason="ws_connection_close:1006:",
                                  books={cid: books[cid]})
            for cid in books]
    assert len(set(rids)) == 2, "one episode per affected book, no fan-out"
    for cid, rid in zip(books, rids):
        assert books[cid].book_state.value == "stale"
        ep = mgr._episodes[rid]
        assert "1006" in ep.disconnect_reason
        assert mgr.is_finished(rid) is False, "server-kill episode stays open"


# D. Single-socket fallback selection ------------------------------------------

def _carriers(pool: ShardPool, now_ns: int):
    """Conns healthy enough to carry the shard (test-side selection over
    real pool state: OUR recycle takes a conn, it never takes traffic)."""
    busy = set(pool.conns_needing_recycle(now_ns)) | set(pool.conns_gone_silent(now_ns))
    return [c.name for c in (pool.conn_a, pool.conn_b) if c.name not in busy]


def test_single_socket_fallback_selection():
    now_ns = time.time_ns()
    pool = ShardPool(shard=["BTC"])
    pool.conn_a.established_ns = now_ns - int(10 * 1e9)
    pool.conn_b.established_ns = now_ns - int(10 * 1e9)
    pool.conn_a.note_frame(now_ns)
    pool.conn_b.note_frame(now_ns)
    assert _carriers(pool, now_ns) == ["A", "B"]
    # A hits the recycle ceiling: B alone carries (single-socket fallback).
    pool.conn_a.established_ns = now_ns - int(300 * 1e9)
    assert _carriers(pool, now_ns) == ["B"]
    # Both data-dead: nothing selected — honest stale, never fabricated.
    stale_ns = now_ns - int(600 * 1e9)
    pool.conn_a.note_frame(stale_ns)
    pool.conn_b.note_frame(stale_ns)
    pool.conn_b.established_ns = now_ns - int(300 * 1e9)
    assert _carriers(pool, now_ns) == []


# E. Silence watchdog: stale + episode, never fill ------------------------------

def test_silence_watchdog_boundaries():
    now_ns = time.time_ns()
    assert silence_exceeded(None, now_ns) is False  # fresh conn never killed
    assert silence_exceeded(now_ns - int((SILENCE_WATCHDOG_S - 1) * 1e9), now_ns) is False
    assert silence_exceeded(now_ns - int((SILENCE_WATCHDOG_S + 1) * 1e9), now_ns) is True


def test_silence_watchdog_stale_plus_episode_not_fill(tmp_path):
    """Data-dead socket: book goes stale with a real-reason episode; a
    reconnect alone never completes it (no fill — only a real heal does)."""
    mgr = _make_mgr(str(tmp_path))
    b = _make_book("cid-sil")
    now_ns = time.time_ns()
    assert silence_exceeded(b_last_data_ns(now_ns), now_ns) is True
    rid = mgr.handle_disconnect("BTC", "cid-sil", reason="data_staleness_30s_forcing_reconnect",
                                books={"cid-sil": b})
    assert b.book_state.value == "stale"
    assert mgr._episodes[rid].disconnect_reason == "data_staleness_30s_forcing_reconnect"
    mgr.handle_reconnect(rid)
    assert mgr.is_finished(rid) is False, "reconnect alone must not fill the gap"
    assert b.book_state.value == "stale", "book stays stale until a real heal"


def b_last_data_ns(now_ns: int) -> int:
    """Last-data clock for a book that has been data-dead past the watchdog."""
    return now_ns - int((SILENCE_WATCHDOG_S + 5) * 1e9)


def test_subscriptions_hot_add_delta_only():
    """One socket carries the shard union; later tokens hot-add the delta only."""
    subs = ShardSubscriptions()
    first = subs.initial_payload(["tok-1", "tok-2"])
    assert first == {"assets_ids": ["tok-1", "tok-2"], "type": "market"}
    assert subs.hot_add_payload(["tok-1", "tok-2"]) is None  # no resubscribe
    delta = subs.hot_add_payload(["tok-2", "tok-3"])
    assert delta is not None and delta["assets_ids"] == ["tok-3"]
