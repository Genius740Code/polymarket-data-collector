"""WeatherDiscovery._passes_liquidity_filter — P0 thin-bucket contract.

Unknown (None/NaN/unparseable) = INCLUDE; near-term events (ending within
near_term_bypass_hours) skip the floor entirely; only confirmed-low
far-future brackets are rejected.
"""
import math
import time

from polymarket_collector.config import LiquidityFilterConfig
from polymarket_collector.rollover import MarketInfo
from polymarket_collector.weather_discovery import WeatherDiscovery


def _lf(**kw):
    base = dict(enabled=True, min_liquidity=500.0, min_volume=50.0,
                near_term_bypass_hours=36.0)
    base.update(kw)
    return LiquidityFilterConfig(**base)


def _market(cid, end_ms, vol=None, liq=None):
    return MarketInfo(
        condition_id=cid, market_id="1", asset="LONDON",
        up_token_id=f"{cid}-UP", down_token_id=f"{cid}-DOWN",
        market_start_ts_ms=end_ms - 86400 * 1000, market_end_ts_ms=end_ms,
        window_index=1, series_id="WEATHER-HIGH-1D",
        reported_volume=vol, reported_liquidity=liq,
    )


def _disc(**kw):
    events = []
    d = WeatherDiscovery("high", on_event=lambda t, p: events.append((t, p)),
                         liquidity_filter=_lf(**kw))
    return d, events


def test_unknown_always_passes():
    d, _ = _disc()
    now = int(time.time() * 1000)
    far = now + 48 * 3600 * 1000  # far-future: bypass must NOT save these
    for v in (None, float("nan"), "nan", ""):
        assert d._passes_liquidity_filter(_market(f"u-{v}", far, vol=v, liq=v)) is True


def test_near_term_bypass_ignores_confirmed_low():
    d, events = _disc()
    now = int(time.time() * 1000)
    # ends in 5h with terrible numbers -> still collected
    assert d._passes_liquidity_filter(
        _market("near", now + 5 * 3600 * 1000, vol=5.0, liq=41.0)) is True
    assert events == []
    # just-ended (grace window) also bypasses
    assert d._passes_liquidity_filter(
        _market("grace", now - 2 * 3600 * 1000, vol=1.0, liq=1.0)) is True


def test_far_future_confirmed_low_rejected():
    d, events = _disc()
    now = int(time.time() * 1000)
    assert d._passes_liquidity_filter(
        _market("far", now + 48 * 3600 * 1000, vol=5.0, liq=600.0)) is False
    assert events and events[0][0] == "low_liquidity"
    assert events[0][1]["ends_in_h"] is not None
    assert events[0][1]["ends_in_h"] > 36.0


def test_far_future_adequate_passes():
    d, _ = _disc()
    now = int(time.time() * 1000)
    assert d._passes_liquidity_filter(
        _market("ok", now + 48 * 3600 * 1000, vol=100.0, liq=600.0)) is True


def test_disabled_collects_all():
    d, _ = _disc(enabled=False)
    now = int(time.time() * 1000)
    assert d._passes_liquidity_filter(
        _market("x", now + 48 * 3600 * 1000, vol=0.0, liq=0.0)) is True


def test_bypass_zero_restores_old_floor():
    d, _ = _disc(near_term_bypass_hours=0.0)
    now = int(time.time() * 1000)
    assert d._passes_liquidity_filter(
        _market("y", now + 5 * 3600 * 1000, vol=5.0, liq=600.0)) is False
    assert math.isnan(float("nan"))  # sanity: file parses, math import used
