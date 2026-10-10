"""Guards for price promotion, pre-warm throttle, and the no-WS hoist.

Real objects only: Collector on tmp dirs, OrderBookState, RolloverManager
with inline MarketInfo literals and local fetch replacements (no network;
every quote below is a hand-set literal driving real code paths).
"""
import tempfile
import time
from pathlib import Path

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.enums import BookState
from polymarket_collector.rollover import MarketInfo, RolloverManager

TS = "1788649335000"


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


def pc_frame(entries):
    return {"event_type": "price_change", "market": "0xm", "timestamp": TS,
            "price_changes": entries}


def entry(token, price, size, side, bid, ask):
    return {"asset_id": token, "price": str(price), "size": str(size),
            "side": side, "best_bid": str(bid), "best_ask": str(ask)}


def two_sided_frame():
    return pc_frame([
        entry("up-123", 0.50, 10, "BUY", 0.50, 0.55),
        entry("up-123", 0.55, 10, "SELL", 0.50, 0.55),
        entry("down-456", 0.44, 10, "BUY", 0.44, 0.49),
        entry("down-456", 0.49, 10, "SELL", 0.44, 0.49),
    ])


def test_price_change_promotion_links_episode_for_close():
    """Promotion keeps the open episode linked so the healed-episode sweep
    completes it (same resync_completed shape as the WS-provisional path)."""
    col = Collector(make_cfg(tempfile.mkdtemp()))
    b = OrderBookState("BTC", "c-promo", None, "BTC-5M", 0, "up-123",
                       "down-456", 9999999999999)
    col.books = {b.condition_id: b}
    rid = col.resync.handle_disconnect("BTC", b.condition_id, reason="t",
                                       books=col.books)
    assert b.book_state.value == "stale"
    assert b.resync_id == rid
    ok, _ = b.apply_ws_message(two_sided_frame())
    assert ok is True
    assert b.book_state == BookState.live
    assert b.resync_id == rid, "promotion must link the episode, not clear it"
    assert col.resync.close_healed_episodes(col.books) == 1
    assert col.resync._episodes[rid].resync_completed_ts_utc is not None


def _market(cid, end_offset_ms, window_index=0, asset="BTC"):
    now = int(time.time() * 1000)
    return MarketInfo(
        condition_id=cid,
        market_id=f"mid-{cid}",
        asset=asset,
        up_token_id=f"{cid}-UP",
        down_token_id=f"{cid}-DOWN",
        market_start_ts_ms=now,
        market_end_ts_ms=now + end_offset_ms,
        window_index=window_index,
        series_id=f"{asset}-5MIN",
    )


def _mgr(events):
    cfg = CollectorConfig()
    cfg.rollover_lead_seconds = 30
    return RolloverManager(cfg, on_event=lambda t, d: events.append((t, d)))


@pytest.mark.asyncio
async def test_prewarm_discovery_leaves_guard_set():
    """Pre-warm discovery must not clear the once-per-window guard."""
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)
    state.current = _market("cur", 10_000, window_index=3)
    end_ms = state.current.market_end_ts_ms
    state.rollover_started_for_ts = end_ms  # main path already emitted
    fresh = _market("pre", 310_000, window_index=4)

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        return fresh

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    subscribed = []

    async def subscribe(market):
        subscribed.append(market.condition_id)

    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) == "market_added"
    assert subscribed == ["pre"]
    assert state.next is not None and state.next.condition_id == "pre"
    assert state.rollover_started_for_ts == end_ms, "discovery must not re-arm the window"
    assert [t for t, _ in events].count("rollover_started") == 0


@pytest.mark.asyncio
async def test_prewarm_emit_then_discovery_keeps_guard():
    """Pre-warm emission followed by discovery keeps the guard (no re-arm)."""
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)
    state.current = _market("cur", 10_000, window_index=3)
    end_ms = state.current.market_end_ts_ms
    fresh = _market("pre", 310_000, window_index=4)

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        return fresh

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    async def subscribe(market):
        pass

    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) == "market_added"
    assert state.rollover_started_for_ts == end_ms
    assert [t for t, _ in events].count("rollover_started") == 1


def test_no_ws_hoist_skips_live_books():
    """Fresh books are born live: no stale mark, no episode minted for them;
    genuinely stale books still link honestly."""
    col = Collector(make_cfg(tempfile.mkdtemp()))
    n0 = len(col.resync._episodes)
    fresh = OrderBookState("BTC", "c-fresh", "mid-1", "BTC-5M", 0, "up-123",
                           "down-456", 9999999999999)
    assert fresh.book_state.value == "live"
    if getattr(getattr(fresh, "book_state", None), "value", "") != "live":
        col._link_stale_book_episode(fresh, "BTC", "market_added")
    assert fresh.book_state.value == "live"
    assert fresh.resync_id is None
    assert len(col.resync._episodes) == n0
    fresh.mark_stale(resync_id="orphan-x")
    if getattr(getattr(fresh, "book_state", None), "value", "") != "live":
        col._link_stale_book_episode(fresh, "BTC", "market_added")
    assert fresh.book_state.value == "stale"
    assert fresh.resync_id in col.resync._episodes


def test_no_ws_hoist_site_guards_link_on_liveness():
    """The no-WS discovery branch must gate its link call on liveness —
    a bare call re-stales live-born books."""
    src = Path(__file__).resolve().parents[1] / "src" / "polymarket_collector" / "collector.py"
    lines = src.read_text(encoding="utf-8").splitlines()
    start = next(i for i, l in enumerate(lines) if "async def _run_shard_loop_single" in l)
    end = next((i for i in range(start + 1, len(lines))
                if lines[i].startswith("    def ") or lines[i].startswith("    async def ")),
               len(lines))
    link = next(i for i in range(start, end)
                if "_link_stale_book_episode(_nb," in lines[i] and "market_added" in lines[i])
    window = "\n".join(lines[max(start, link - 10):link])
    assert "book_state" in window and "live" in window, "hoist-site link must sit under a liveness guard"
