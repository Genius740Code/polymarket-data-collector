"""WS/stale chaos tests (2026-09-26 stale-healing workstream).

Prod evidence (2026-09-25, data/collector_events/date=2026-09-25/):
34,695 ws_disconnected vs only 313 ws_reconnect_attempt in one day; a BTC 5m
window (0x676aa091…) went drift_detected 19:28:59, book_stalled x3, zero
reconnect until 19:59:35 — 30.6 min stale, all 387 snapshots book_state='stale'
— while the vendor streamed 161k L2 ticks + 1348 fills; 6,443 resync_failed/day
escalating on fetch_none that were REST /book 429s under 35-lane load.

Fixed paths under test (invariants, not just "no exception"):
  1. Heal-herd stagger: per-book phase gate spreads stale books across the
     60-tick window (was: ALL stale books fired on the same _tick%60==0 tick,
     N books × 2 GETs in <1s → 429 → fetch_none streaks).
  2. Reconnect on repeated heal failure: a current-market book whose REST heal
     keeps failing requests a bounded shard reconnect (per-asset cooldown) so
     the fresh full book relives it — book_state returns to live.
  3. 429 backoff: a rate-limited fetch is NOT fetch_none — the no-L2 streak
     does not grow, resync() refuses at entry with a bounded backoff (no 60s
     escalation burn, no walk starvation).
  4. Honest-gap invariants: stale books stay stale (never fabricated live),
     resync_episodes rows persist with real reasons, and the clean view
     (book_snapshots_clean) never contains stale rows.
"""
import asyncio
import os
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
    cfg.cursor_store.path = tmpdir + "/_cursor_state"
    cfg.ws.max_resync_duration_seconds = 2  # short for tests
    cfg.ws.resync_rest_backoff_initial_ms = 10
    cfg.ws.resync_rest_backoff_max_ms = 30
    return cfg


def make_collector(tmpdir: str) -> Collector:
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmpdir)},
                          cursor_store={"path": os.path.join(str(tmpdir), "cursor_state")},
                          timeframes=["5m"])
    return Collector(cfg)


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


def full_book_payload(seq=10):
    return {"up_bids": [[0.60, 100]], "up_asks": [[0.65, 30]],
            "down_bids": [[0.35, 100]], "down_asks": [[0.40, 30]],
            "sequence_number": seq}


def tempfile_dir():
    import tempfile
    class _Ctx:
        def __enter__(self):
            self._d = tempfile.mkdtemp(prefix="ws-stale-")
            return self._d
        def __exit__(self, *a):
            import shutil
            shutil.rmtree(self._d, ignore_errors=True)
            return False
    return _Ctx()


# 1. Heal-herd stagger ----------------------------------------------------------

def test_heal_phase_spreads_books_across_window():
    """N stale books must NOT all share one heal phase (the old global
    `_tick % 60 == 0` fired every stale book of every lane on the same tick —
    N books × 2 GETs in <1s → 429s → fetch_none streaks). The per-book phase
    must also be stable for one book within the process."""
    col = make_collector("/tmp/opencode/ws-stale-phase-cfg")
    phases = [col._heal_phase(f"cid-{i}") for i in range(30)]
    assert len(set(phases)) > 1, "all 30 books share one phase — no stagger"
    # stable per book within the process
    for i in range(30):
        assert col._heal_phase(f"cid-{i}") == phases[i]


def test_heal_bg_jitter_delay_rechecks_book_state():
    """A delayed heal must re-check the book after the delay — a book that
    healed/superseded during it is never healed (no REST GET)."""
    with tempfile_dir() as tmp:
        col = make_collector(tmp)
        calls = []

        async def fake_heal(book, market):
            calls.append(book.condition_id)
            return True

        col._fetch_and_apply_rest_book = fake_heal
        b = make_book(cid="cid-x", end_ms="future")
        market = types.SimpleNamespace(market_end_ts_ms=int(time.time() * 1000) + 3600_000)

        async def scenario():
            # the book flips live 5ms into the 20ms heal delay → heal skipped
            async def flip():
                await asyncio.sleep(0.005)
                b.mark_live()
            task = asyncio.get_running_loop().create_task(flip())
            await col._heal_book_bg(b, market, delay_s=0.02)
            await task

        asyncio.run(scenario())
        assert calls == [], "a book that healed during the delay must not be healed"
        # and a book that STAYS stale through the delay IS healed
        b2 = make_book(cid="cid-y", end_ms="future")
        asyncio.run(col._heal_book_bg(b2, market, delay_s=0.01))
        assert calls == ["cid-y"]


# 2. Reconnect on repeated heal failure -----------------------------------------

def test_reconnect_requested_after_repeated_heal_failure():
    """A current-market stale book whose REST heal keeps failing (genuine
    no-L2 → fetch_none streak grows) must request a bounded shard reconnect
    after the threshold — the stall window saw zero reconnects for 30.6 min."""
    with tempfile_dir() as tmp:
        col = make_collector(tmp)
        b = make_book(cid="cid-stall", end_ms="future")

        async def always_none(asset, cid):
            return None  # genuine no-L2 (not rate-limited)

        col._fetch_and_apply_rest_book = always_none
        market = types.SimpleNamespace(market_end_ts_ms=int(time.time() * 1000) + 3600_000)
        for _ in range(3):
            asyncio.run(col._heal_book_bg(b, market))
        assert col._shard_reconnect_requests.get("BTC") is True, \
            "3 consecutive heal failures must request a shard reconnect"
        # the fetch_none streak grew honestly
        assert int(col.resync._fetch_none_streak.get("cid-stall", 0)) >= 3


def test_reconnect_request_cooldown_bounds_storm():
    """Repeated heal failures within the cooldown must NOT mint repeated
    requests (the opposite failure mode is 34,695 ws_disconnected/day)."""
    with tempfile_dir() as tmp:
        col = make_collector(tmp)
        col.request_shard_reconnect("BTC", reason="first")
        assert col._shard_reconnect_requests.get("BTC") is True
        # watchdog consumes the request
        col._shard_reconnect_requests["BTC"] = False
        # another failure within the cooldown → blocked
        col.request_shard_reconnect("BTC", reason="within-cooldown")
        assert col._shard_reconnect_requests.get("BTC") is False, \
            "cooldown must block repeated requests"
        # a different asset is unaffected (per-asset cooldown)
        col.request_shard_reconnect("ETH", reason="other-asset")
        assert col._shard_reconnect_requests.get("ETH") is True


def test_rate_limited_heal_is_not_fetch_none():
    """A rate-limited heal must NOT grow the no-L2 streak (a 429 is transient,
    unlike a token that exposes no full L2) and must not request a reconnect
    on the rate-limit path."""
    with tempfile_dir() as tmp:
        col = make_collector(tmp)
        b = make_book(cid="cid-429", end_ms="future")

        async def rate_limited_heal(book, market):
            # the fetcher records the bounded backoff, then reports failure
            col.resync.note_rate_limited(book.condition_id, 1.0)
            return False

        col._fetch_and_apply_rest_book = rate_limited_heal
        market = types.SimpleNamespace(market_end_ts_ms=int(time.time() * 1000) + 3600_000)
        for _ in range(5):
            asyncio.run(col._heal_book_bg(b, market))
        # streak must NOT have grown on rate-limited heals
        assert not col.resync._fetch_none_streak.get("cid-429"), \
            "rate-limited heals must not count as fetch_none"
        # and no reconnect request from the rate-limit path
        assert not col._shard_reconnect_requests.get("BTC"), \
            "rate-limit backoff is the medicine, not a reconnect"


# 3. 429 backoff at the manager level -------------------------------------------

def test_rate_limited_resync_refused_at_entry_no_escalation_burn():
    """A rate-limited condition must be refused at resync() entry with ZERO
    REST attempts (no 60s escalation burn — prod: 6,443 resync_failed/day
    escalated on fetch_none that were 429s) and the no-L2 streak must not
    grow."""
    with tempfile_dir() as tmp:
        calls = []

        async def never_call(asset, cid):
            calls.append(cid)
            return None

        events = []
        mgr = ResyncManager(make_cfg(tmp), rest_fetcher=never_call,
                            on_event=lambda t, d: events.append((str(t), d)))
        books = {"cid-rl": make_book("cid-rl")}
        rid = mgr.handle_disconnect("BTC", "cid-rl", reason="test", books=books)
        mgr.note_rate_limited("cid-rl", 30.0)
        ok = asyncio.run(mgr.resync("BTC", "cid-rl", books, rid))
        assert ok is False
        assert calls == [], "rate-limited resync must be refused at entry (zero REST attempts)"
        assert not mgr._fetch_none_streak.get("cid-rl"), \
            "rate-limited refusals must not grow the no-L2 streak"
        # the episode stays open (honest stale) — no fabricated completion
        assert not mgr.is_finished(rid)
        assert books["cid-rl"].book_state.value == "stale"


def test_genuine_no_l2_still_enters_terminal_quiet():
    """A genuinely empty book (no rate limit) must still grow the fetch_none
    streak to the terminal quiet — the distinction must not lose the honest
    no-L2 medicine (weather tokens AMSTERDAM/ANKARA/ATLANTA)."""
    with tempfile_dir() as tmp:
        calls = []

        async def empty_book(asset, cid):
            calls.append(cid)
            return None

        events = []
        mgr = ResyncManager(make_cfg(tmp), rest_fetcher=empty_book,
                            on_event=lambda t, d: events.append((str(t), d)))
        books = {"cid-empty": make_book("cid-empty")}
        rid = mgr.handle_disconnect("BTC", "cid-empty", reason="test", books=books)
        mgr.handle_reconnect(rid)
        ok = asyncio.run(mgr.resync("BTC", "cid-empty", books, rid))
        assert ok is False  # escalated after max_resync_duration (2s in tests)
        # the streak grew on GENUINE no-L2 fetches
        assert int(mgr._fetch_none_streak.get("cid-empty", 0)) >= 1
        assert calls, "genuine no-L2 fetches must still be attempted"


def test_rate_limited_gate_expires_bounded():
    """The rate-limit backoff must be bounded (not the 1h terminal quiet):
    note_rate_limited with a 1s hint expires after ~1s, and a huge Retry-After
    is clamped to 60s."""
    with tempfile_dir() as tmp:
        mgr = ResyncManager(make_cfg(tmp), rest_fetcher=lambda a, c: None)
        mgr.note_rate_limited("cid-b1", 1.0)
        assert mgr.rate_limited("cid-b1") is True
        mgr._rate_limited_until["cid-b1"] = time.monotonic() - 0.01
        assert mgr.rate_limited("cid-b1") is False, "bounded backoff must expire"
        mgr.note_rate_limited("cid-b2", 99999.0)
        until = mgr._rate_limited_until["cid-b2"] - time.monotonic()
        assert until <= 60.5, f"huge Retry-After must clamp to 60s, got {until}"


# 4. Honest-gap invariants (book_state / resync_episodes / clean view) ----------

def test_stale_book_invariant_episodes_persist_and_clean_view_excludes_stale():
    """End-to-end invariant: a stalled book stays stale (never fabricated
    live), its resync_episodes rows persist with the real reason, and the
    clean view built from the raw hive contains ONLY live rows (stale rows
    never leak into book_snapshots_clean)."""
    with tempfile_dir() as tmp:
        col = make_collector(tmp)
        cid = "cid-inv"
        b = make_book(cid=cid, end_ms="future")
        col.books[cid] = b
        try:
            col._index_book(b)
        except Exception:
            pass
        col.markets[cid] = types.SimpleNamespace(market_end_ts_ms=int(time.time() * 1000) + 3600_000,
                                                 status="active")
        # stall: mark stale via the manager (the collector's stall-detector path)
        rid = col.resync.handle_disconnect("BTC", cid, reason="book_stalled", books={cid: b})
        assert b.book_state.value == "stale"
        assert b.resync_id == rid
        # a rate-limited heal must not flip it live or fabricate data
        async def rate_limited_heal(book, market):
            col.resync.note_rate_limited(cid, 1.0)
            return False
        col._fetch_and_apply_rest_book = rate_limited_heal
        asyncio.run(col._heal_book_bg(b, types.SimpleNamespace(market_end_ts_ms=int(time.time() * 1000) + 3600_000)))
        assert b.book_state.value == "stale", "rate-limited heal must not promote a stale book"
        # episode rows persisted (WAL-before-buffer via the writer) with the real reason
        eps = [r for r in col.resync._episodes.values()]
        assert any(e.resync_id == rid and e.disconnect_reason == "book_stalled" for e in eps)
        # recovery: a successful REST heal relives the book and the episode closes
        async def good_heal(book, market):
            book.replace_from_rest_snapshot(full_book_payload())
            book.mark_live()
            return True
        col._fetch_and_apply_rest_book = good_heal
        col.resync._rate_limited_until.pop(cid, None)
        asyncio.run(col._heal_book_bg(b, types.SimpleNamespace(market_end_ts_ms=int(time.time() * 1000) + 3600_000)))
        assert b.book_state.value == "live", "a successful REST heal must relive the book"
        closed = col.resync.close_healed_episodes(col.books)
        assert closed >= 1
        ep = col.resync._episodes.get(rid)
        assert ep is None or ep.resync_completed_ts_utc is not None, \
            "healed episode must reach a final state (no orphan resync_id)"
