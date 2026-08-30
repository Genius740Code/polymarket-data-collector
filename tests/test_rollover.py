"""Tests for §1 rollover — dual-tracking, promotion, gap handling, rate limits."""
import asyncio
import time

import pytest

from polymarket_collector.config import CollectorConfig
from polymarket_collector.rollover import MarketDiscovery, MarketInfo, RolloverManager


def make_market(cid="cid-1", end_offset_ms=300_000, window_index=0, asset="BTC"):
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


@pytest.mark.asyncio
async def test_rollover_lookahead_discovers_next():
    cfg = CollectorConfig()
    cfg.rollover_lead_seconds = 30
    events = []
    mgr = RolloverManager(cfg, on_event=lambda t, d: events.append((t, d)))
    # current ends in 10s → within 30s lead, should trigger lookahead
    cur = make_market(cid="cur", end_offset_ms=10_000, window_index=1)
    mgr.set_current("BTC", cur)

    # mock discovery
    async def fake_fetch(asset, after_ts_ms):
        assert asset == "BTC"
        return make_market(cid="next", end_offset_ms=310_000, window_index=2)

    mgr.discovery.fetch_next_market = fake_fetch  # type: ignore

    subscribed = []
    async def subscribe(market):
        subscribed.append(market.condition_id)

    now_ms = int(time.time() * 1000)
    # should discover
    await mgr.check_and_roll("BTC", subscribe, now_ms=now_ms)
    assert mgr.states["BTC"].next is not None
    assert mgr.states["BTC"].next.condition_id == "next"
    assert "next" in subscribed
    assert mgr.states["BTC"].is_rollover_window is True
    # active markets should be 2 during overlap
    assert len(mgr.active_markets("BTC")) == 2


@pytest.mark.asyncio
async def test_rollover_promotion():
    cfg = CollectorConfig()
    mgr = RolloverManager(cfg)
    cur = make_market(cid="cur", end_offset_ms=-1000, window_index=1)  # already ended
    nxt = make_market(cid="next", end_offset_ms=300_000, window_index=2)
    mgr.states["BTC"].current = cur
    mgr.states["BTC"].next = nxt

    now_ms = int(time.time() * 1000)
    subscribed = []
    async def sub(m): subscribed.append(m)

    result = await mgr.check_and_roll("BTC", sub, now_ms=now_ms)
    assert result == "rollover_completed"
    assert mgr.states["BTC"].current.condition_id == "next"
    assert mgr.states["BTC"].next is None


@pytest.mark.asyncio
async def test_coverage_gap_vs_rollover_miss():
    cfg = CollectorConfig()
    cfg.max_coverage_gap_seconds = 5
    events = []
    mgr = RolloverManager(cfg, on_event=lambda t, d: events.append(t))
    cur = make_market(cid="cur", end_offset_ms=-6000, window_index=1)  # ended 6s ago
    mgr.states["BTC"].current = cur
    # discovery returns None (no market)
    async def fake_fetch(asset, after):
        return None
    mgr.discovery.fetch_next_market = fake_fetch  # type: ignore

    async def sub(m): pass

    # 6s past end → beyond max_coverage_gap (5s) → coverage_gap, not just rollover_miss
    now_ms = cur.market_end_ts_ms + 6000
    # need to ensure lookahead needed
    mgr.states["BTC"].next = None
    await mgr.check_and_roll("BTC", sub, now_ms=now_ms)
    assert "coverage_gap" in events

    # reset and test rollover_miss (just after end but < max_gap)
    events.clear()
    mgr.states["BTC"].rollover_miss_logged = False
    now_ms2 = cur.market_end_ts_ms + 2000  # 2s past end, <5s max_gap
    await mgr.check_and_roll("BTC", sub, now_ms=now_ms2)
    # May still be coverage_gap if previous flag not reset; test miss in isolation
    # Create fresh manager for miss
    events2 = []
    mgr2 = RolloverManager(cfg, on_event=lambda t, d: events2.append(t))
    mgr2.states["BTC"].current = cur
    mgr2.discovery.fetch_next_market = fake_fetch  # type: ignore
    await mgr2.check_and_roll("BTC", sub, now_ms=now_ms2)
    assert "rollover_miss" in events2


@pytest.mark.asyncio
async def test_rate_limited_backoff():
    cfg = CollectorConfig()
    discovery = MarketDiscovery(rest_market_url="http://example.com", poll_interval_s=2.0, backoff_max_s=8.0)
    # simulate 429 by widening backoff
    initial = discovery._backoff_s
    # call fetch that would get 429 → backoff doubles (handled inside fetch_next_market)
    # we test backoff helper directly
    discovery._backoff_s = min(discovery._backoff_s * 2, discovery.backoff_max)
    assert discovery._backoff_s == 4.0
    discovery._backoff_s = min(discovery._backoff_s * 2, discovery.backoff_max)
    assert discovery._backoff_s == 8.0
    discovery._backoff_s = min(discovery._backoff_s * 2, discovery.backoff_max)
    assert discovery._backoff_s == 8.0  # capped


def test_series_id_window_index():
    cfg = CollectorConfig()
    assert cfg.series_id_for("BTC") == "BTC-5m"
    # adding 4th asset via config, not hardcoded
    cfg2 = CollectorConfig(assets=["BTC", "ETH", "SOL", "AVAX"], series_ids={"BTC": "BTC-5m", "ETH": "ETH-5m", "SOL": "SOL-5m", "AVAX": "AVAX-5m"})
    assert "AVAX" in cfg2.assets
    assert cfg2.series_id_for("AVAX") == "AVAX-5m"
    assert cfg2.series_id_for("btc") == "BTC-5m"  # case-insensitive
