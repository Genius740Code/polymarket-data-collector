"""Pre-warm bounded retry — deterministic next-slug discovery independent of settle.

Next-window slugs are a pure function of (asset, window ts); Gamma publication
of the fresh slug lags (sometimes past settle), costing the fresh-window
opening. RolloverManager.prewarm_next_window retries the exact NEXT slug on
its own 10s cadence from T-lead onward with a hard stop at window end +60s,
consulting no settlement state; on discovery it subscribes and routes through
the existing notify_settlement path.

Real data only: inline MarketInfo literals + local stub fetch (no network, no
generated prices/books).
"""
import time

import pytest

from polymarket_collector.config import CollectorConfig
from polymarket_collector.rollover import (
    MarketDiscovery,
    MarketInfo,
    RolloverManager,
    _daily_slug_for,
    _hourly_slug_for,
)


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


# --- slug determinism: next slug computable from current.end, no Gamma read ---

def test_next_slug_deterministic_5m():
    """Same (asset, ts) -> same slug; any ts inside a window maps to it."""
    d = MarketDiscovery(rest_market_url="", window_size_seconds=300)
    win = 1791642600  # live-observed 5m window start (2026-10-10)
    assert d._slug_for("BTC", win) == "btc-updown-5m-1791642600"
    assert d._slug_for("btc", win) == d._slug_for("BTC", win)
    # _ts_for_after floors any ts inside the NEXT window to its start, so the
    # NEXT slug follows deterministically from current.market_end_ts_ms.
    nxt_end_ms = (win + 300) * 1000
    assert d._ts_for_after(nxt_end_ms) == win + 300
    assert d._ts_for_after(nxt_end_ms + 123_456) == win + 300
    assert d._slug_for("BTC", d._ts_for_after(nxt_end_ms)) == "btc-updown-5m-1791642900"


def test_next_slug_adjacent_windows_distinct_by_exact_window():
    for ws, label in ((300, "5m"), (900, "15m"), (14400, "4h")):
        d = MarketDiscovery(rest_market_url="", window_size_seconds=ws)
        base = 1791642600 // ws * ws
        s0 = d._slug_for("ETH", base)
        s1 = d._slug_for("ETH", base + ws)
        assert s0 != s1
        assert s0 == f"eth-updown-{label}-{base}"
        assert s1 == f"eth-updown-{label}-{base + ws}"


def test_next_slug_deterministic_1h_1d_vectors():
    """Live-verified ET families are closed-form in (asset, ts) as well."""
    assert _hourly_slug_for("BTC", 1788890400) == "bitcoin-up-or-down-september-8-2026-2pm-et"
    assert _daily_slug_for("BTC", 1789344000) == "bitcoin-up-or-down-on-september-14-2026"
    d1h = MarketDiscovery(rest_market_url="", window_size_seconds=3600)
    assert d1h._slug_for("BTC", 1788890400) == "bitcoin-up-or-down-september-8-2026-2pm-et"
    d1d = MarketDiscovery(rest_market_url="", window_size_seconds=86400)
    assert d1d._slug_for("BTC", 1789344000) == "bitcoin-up-or-down-on-september-14-2026"


# --- pre-warm trigger timing + retry bound (direct method calls) ---

@pytest.mark.asyncio
async def test_prewarm_idle_before_lead():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)
    state.current = _market("cur", 120_000)  # ends in 120s; lead is 30s

    calls = []

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        calls.append((asset, after_ts_ms, strict_adjacent))
        return None

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    async def subscribe(market):
        raise AssertionError("must not subscribe before T-lead")

    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) is None
    assert calls == []
    assert state.prewarm_done_for_ts is None


@pytest.mark.asyncio
async def test_prewarm_polls_in_lead_with_10s_retry():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    t0 = int(time.time() * 1000)
    state.current = _market("cur", 10_000, window_index=3)  # inside 30s lead
    end_ms = state.current.market_end_ts_ms

    calls = []

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        calls.append((asset, after_ts_ms, strict_adjacent))
        return None

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    async def subscribe(market):
        raise AssertionError("nothing discovered")

    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=t0) is None
    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=t0 + 1_000) is None
    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=t0 + 9_999) is None
    assert len(calls) == 1  # single attempt inside the 10s retry window
    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=t0 + 10_000) is None
    assert len(calls) == 2
    asset, after, adjacent = calls[0]
    assert asset == "BTC" and after == end_ms and adjacent is True


@pytest.mark.asyncio
async def test_prewarm_stops_after_end_plus_60s():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)
    state.current = _market("cur", -61_000)  # ended 61s ago
    end_ms = state.current.market_end_ts_ms

    calls = []

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        calls.append(asset)
        return None

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    async def subscribe(market):
        raise AssertionError("must not subscribe past the stop bound")

    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) is None
    assert calls == []
    assert state.prewarm_done_for_ts == end_ms
    # stop marker persists: no further attempts for this window
    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms + 50_000) is None
    assert calls == []


@pytest.mark.asyncio
async def test_prewarm_tail_still_active_inside_end_plus_60s():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)
    state.current = _market("cur", -59_000)  # ended 59s ago: inside the tail

    calls = []

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        calls.append(asset)
        return None

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    async def subscribe(market):
        pass

    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) is None
    assert calls == ["BTC"]


# --- discovery outcome: subscribe + existing notify_settlement path ---

@pytest.mark.asyncio
async def test_prewarm_hit_subscribes_and_routes_settle_path():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)
    state.current = _market("cur", 10_000, window_index=3)
    fresh = _market("pre", 310_000, window_index=4)

    calls = []

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        calls.append((asset, after_ts_ms, strict_adjacent))
        return fresh

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    subscribed = []

    async def subscribe(market):
        subscribed.append(market.condition_id)

    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) == "market_added"
    assert subscribed == ["pre"]
    assert state.next is not None and state.next.condition_id == "pre"
    assert state.is_rollover_window is True
    kinds = [t for t, _ in events]
    assert "market_added" in kinds
    # existing notify_settlement path ran: throttle reset + event emitted
    assert "settlement_discovery" in kinds
    assert state.last_discovery_attempt_ms is None
    # done for this window: no second poll, no double subscribe
    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms + 11_000) is None
    assert len(calls) == 1 and subscribed == ["pre"]


@pytest.mark.asyncio
async def test_prewarm_skips_when_next_present_or_no_current():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)

    calls = []

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        calls.append(asset)
        return _market("x", 300_000)

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    async def subscribe(market):
        raise AssertionError("must not subscribe")

    # no current: initial discovery owns it
    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) is None
    # next already present: nothing to pre-warm
    state.current = _market("cur", 10_000)
    state.next = _market("nxt", 310_000)
    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) is None
    assert calls == []


@pytest.mark.asyncio
async def test_prewarm_never_raises():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)
    state.current = _market("cur", 10_000)

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        raise RuntimeError("wire down")

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    async def subscribe(market):
        raise RuntimeError("subscribe down")

    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=now_ms) is None
    assert await mgr.prewarm_next_window("NOPE", subscribe, now_ms=now_ms) is None
    assert await mgr.prewarm_next_window("BTC", subscribe, now_ms=None) is None


# --- wiring: throttled lookahead tick falls through to pre-warm ---

@pytest.mark.asyncio
async def test_throttled_lookahead_tick_falls_through_to_prewarm():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    now_ms = int(time.time() * 1000)
    state.current = _market("cur", 10_000, window_index=3)  # inside 30s lead
    # main poll parked on backoff: this tick would previously idle
    state.last_discovery_attempt_ms = now_ms
    mgr.discoveries["5m"]._backoff_s = 120.0
    fresh = _market("pre", 310_000, window_index=4)

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        return fresh

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    subscribed = []

    async def subscribe(market):
        subscribed.append(market.condition_id)

    # no settlement happened anywhere: pre-warm fires on time-window alone
    assert await mgr.check_and_roll("BTC", subscribe, now_ms=now_ms) == "market_added"
    assert subscribed == ["pre"]
    assert state.next is not None and state.next.condition_id == "pre"
