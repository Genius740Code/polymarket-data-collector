"""Planned-recycle hot-swap: no stale marks, no episodes, grace-covered I-8."""
import time
from types import SimpleNamespace

from polymarket_collector.collector import Collector


def _stub():
    return SimpleNamespace(
        books={},
        _ws_connected={"BTC": True},
        resync=SimpleNamespace(_episodes={}),
        on_event=None,
    )


def test_hotswap_sets_grace_without_episodes():
    s = _stub()
    Collector._planned_recycle_hotswap(s, ["BTC"])
    assert s._ws_connected["BTC"] is False
    assert Collector._recycle_grace_live(s, "BTC") is True
    assert s.resync._episodes == {}


def test_grace_expires():
    s = _stub()
    s._recycle_grace_until = {"BTC": time.time() - 1.0}
    assert Collector._recycle_grace_live(s, "BTC") is False
    assert Collector._recycle_grace_live(s, "ETH") is False


def test_hotswap_never_raises_on_junk():
    s = SimpleNamespace()
    Collector._planned_recycle_hotswap(s, None)
    Collector._planned_recycle_hotswap(None, ["BTC"])
    assert Collector._recycle_grace_live(None, "BTC") is False
