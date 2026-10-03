"""_nearest_chainlink: bisect index agrees with the legacy linear scan."""
import random
from collections import deque

from polymarket_collector.collector import Collector


def _collector_with_ticks(n=1500, seed=4):
    c = Collector.__new__(Collector)
    c._chainlink_events = deque(maxlen=20000)
    random.seed(seed)
    for i in range(n):
        ts = 1000000 + i * 1000 + random.choice([0, 0, -500])
        c._note_chainlink_event({"asset": "BTC", "price": 100.0 + i * 0.01}, "BTC", ts)
    for i in range(n // 3):
        ts = 1000000 + i * 3000
        c._note_chainlink_event({"asset": "ETH", "price": 50.0}, "ETH", ts)
    return c


def test_bisect_matches_linear():
    c = _collector_with_ticks()
    random.seed(9)
    for _ in range(100):
        q = 1000000 + random.randint(0, 2000000)
        idx = c._cl_ts_by_asset
        c._cl_ts_by_asset = {}
        try:
            lin = c._nearest_chainlink(q, "BTC", 2000)
        finally:
            c._cl_ts_by_asset = idx
        got = c._nearest_chainlink(q, "BTC", 2000)
        assert (got is None) == (lin is None)
        if got is not None:
            assert abs(got["price"] - lin["price"]) < 1e-9


def test_asset_isolation_and_tolerance():
    c = _collector_with_ticks()
    assert c._nearest_chainlink(10**12, "DOGE", 2000) is None
    assert c._nearest_chainlink(999, "BTC", 2000) is None
    got = c._nearest_chainlink(1001500, "BTC", 2000)
    assert got is not None and got["price"] >= 100.0


def test_index_bounded_and_sorted():
    c = _collector_with_ticks(n=20000, seed=5)
    tsl, _ = c._cl_ts_by_asset["BTC"]
    assert list(tsl) == sorted(tsl)
    assert len(tsl) <= 6000
