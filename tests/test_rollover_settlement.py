"""Fresh-window discovery trigger — settlement/promotion resets discovery throttle."""
import time

import pytest

from polymarket_collector.config import CollectorConfig
from polymarket_collector.rollover import RolloverManager, MarketInfo


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
async def test_notify_settlement_resets_throttle_for_immediate_poll():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    # lane waiting on initial discovery with inflated backoff from the old window
    mgr.discovery._backoff_s = 8.0
    state.last_discovery_attempt_ms = int(time.time() * 1000)

    reset = mgr.notify_settlement("BTC")
    assert reset >= 1
    assert mgr.discovery._backoff_s == pytest.approx(mgr.discovery.poll_interval)
    assert state.last_discovery_attempt_ms is None
    assert any(t == "settlement_discovery" for t, _ in events)

    # the very next tick polls immediately instead of riding backoff
    calls = []

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        calls.append(asset)
        return _market("fresh", 300_000, window_index=9)

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore

    async def subscribe(market):
        return None

    await mgr.check_and_roll("BTC", subscribe, now_ms=int(time.time() * 1000))
    assert calls == ["BTC"]
    assert state.current is not None and state.current.condition_id == "fresh"


@pytest.mark.asyncio
async def test_promotion_drops_throttle_for_fresh_window():
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    state.current = _market("old", -1_000, window_index=7)  # ended: promotes on next tick
    mgr.discovery._backoff_s = 8.0

    async def subscribe(market):
        return None

    now_ms = int(time.time() * 1000)
    ev = await mgr.check_and_roll("BTC", subscribe, now_ms=now_ms)
    assert ev == "coverage_gap"  # next was None: honest gap, no invented market
    assert state.current is None
    # promotion cleared the throttle for the fresh-window poll
    assert state.last_discovery_attempt_ms is None
    assert mgr.discovery._backoff_s == pytest.approx(mgr.discovery.poll_interval)

    calls = []

    async def stub_fetch(asset, after_ts_ms, strict_adjacent=False):
        calls.append(asset)
        return _market("fresh", 300_000, window_index=8)

    mgr.discoveries["5m"].fetch_next_market = stub_fetch  # type: ignore
    await mgr.check_and_roll("BTC", subscribe, now_ms=int(time.time() * 1000))
    assert calls == ["BTC"]
    assert state.current is not None and state.current.condition_id == "fresh"


def test_notify_settlement_unknown_asset_is_total():
    events = []
    mgr = _mgr(events)
    assert mgr.notify_settlement("NOPE") == 0
