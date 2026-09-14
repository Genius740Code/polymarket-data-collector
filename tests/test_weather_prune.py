"""WeatherManager.prune_ended — bounded _known growth, row-neutral pruning."""
import time

from polymarket_collector.rollover import MarketInfo
from polymarket_collector.weather_manager import WeatherManager


class _Kaggle:
    dataset_prefix = "gghgg1/polymarket-weather-low"


class _Cfg:
    assets = ["LONDON"]
    discovery_poll_interval_seconds = 60
    kaggle = _Kaggle()
    series_ids = {"LONDON": "WEATHER-LOW-1D"}

    def __getattr__(self, name):
        if name == "liquidity_filter":
            return None
        raise AttributeError(name)


def _market(cid, end_ms):
    return MarketInfo(
        condition_id=cid, market_id="1", asset="LONDON",
        up_token_id=f"{cid}-UP", down_token_id=f"{cid}-DOWN",
        market_start_ts_ms=end_ms - 86400 * 1000, market_end_ts_ms=end_ms,
        window_index=1, series_id="WEATHER-LOW-1D",
    )


def test_prune_ended_drops_only_old_brackets():
    mgr = WeatherManager(_Cfg())
    now_ms = int(time.time() * 1000)
    mgr._known = {
        "old": _market("old", now_ms - 7 * 3600 * 1000),   # ended 7h ago
        "edge": _market("edge", now_ms - 6 * 3600 * 1000 + 60_000),  # 1min inside grace
        "live": _market("live", now_ms + 3600 * 1000),     # ends in 1h
    }
    n = mgr.prune_ended(now_ms)
    assert n == 1
    assert set(mgr._known) == {"edge", "live"}
    # active_markets still resolves the live bracket (row path unchanged)
    assert [m.condition_id for m in mgr.active_markets("LONDON")] == ["live"]


def test_prune_ended_empty_is_noop():
    mgr = WeatherManager(_Cfg())
    assert mgr.prune_ended(int(time.time() * 1000)) == 0
