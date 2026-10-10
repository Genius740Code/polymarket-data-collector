"""429 capped-sleep twins parity with the owned-client branch.

The shared-client 429 sites (``_fetch_rest_book`` shared branch and the
``_fetch_and_apply_rest_book`` GET fallback) must mirror the fixed owned
branch exactly: sleep ``min(Retry-After, 2s)``, record a bounded per-book
``note_rate_limited``, record the ``rate_limited`` outcome, and scale the
shared note to the actual sleep. Noting the full hint to the shared
cooldown while skipping the per-book note lets resync classify the miss
as fetch_none, feeding the 5-strike quiet streak.

Both tests run through the shared-client path (``_get_rest_client``
returns a live stand-in) — forcing ``None`` would mask the twins behind
the owned branch. Stubs + patched clocks only — no network, no sockets,
no market data. Every 429 stays an honest rate limit; totals never raise.
"""
import asyncio
import os
import time
import types

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.ingest import heal as heal_mod
from polymarket_collector.ingest.heal import (
    heal_backoff_remaining,
    heal_rate_limited,
    reset_heal_rate_limit,
)


@pytest.fixture(autouse=True)
def _clean_shared_cooldown():
    reset_heal_rate_limit()
    yield
    reset_heal_rate_limit()


class _Resp:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self._body = body if body is not None else []
        self.headers = headers or {}

    def json(self):
        return self._body


class _Resp429:
    status_code = 429
    headers = {"retry-after": "60"}

    def json(self):
        return {}


class _SharedPostDownGet429:
    """Shared-client stand-in: POST unusable, every GET 429 with a 60s hint."""

    def __init__(self):
        self.posts = 0
        self.gets = 0

    async def post(self, url, json=None):
        self.posts += 1
        raise TimeoutError("venue down")

    async def get(self, url, params=None):
        self.gets += 1
        return _Resp429()


class _SharedPostEmptyGet429:
    """Shared-client stand-in: POST 200-empty, every GET 429 with a 60s hint."""

    def __init__(self):
        self.posts = 0
        self.gets = 0

    async def post(self, url, json=None):
        self.posts += 1
        return _Resp(200, [])

    async def get(self, url, params=None):
        self.gets += 1
        return _Resp429()


def _collector(tmp_path):
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": os.path.join(str(tmp_path), "cursor_state")},
                          timeframes=["5m"])
    return Collector(cfg)


def _market():
    return types.SimpleNamespace(
        market_end_ts_ms=int(time.time() * 1000) + 3600_000, status="active",
        up_token_id="up-tok", down_token_id="dn-tok")


def _stale_book(cid, asset="BTC"):
    b = OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id="up-tok", down_token_id="dn-tok",
        market_end_ts_ms=int(time.time() * 1000) + 3600_000,
    )
    b.mark_stale()
    return b


def _outcome(col, cid):
    return (getattr(col, "_heal_last_outcome", None) or {}).get(cid)


def _streak(col, cid):
    return int(col.resync._fetch_none_streak.get(cid, 0) or 0)


def _patched_sleep(monkeypatch):
    slept = []
    _real_sleep = asyncio.sleep

    async def _rec_sleep(delay):
        slept.append(delay)
        await _real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", _rec_sleep)
    return slept


def test_rest_book_shared_branch_mirrors_owned_capped_sleep(tmp_path, monkeypatch):
    col = _collector(tmp_path)
    cid = "cid-twin-rest"
    col.markets[cid] = _market()
    shared = _SharedPostDownGet429()
    col._get_rest_client = lambda: shared  # shared path, NOT the owned branch
    slept = _patched_sleep(monkeypatch)
    out = asyncio.run(col._fetch_rest_book("BTC", cid))
    assert out is None
    assert shared.gets >= 1  # the GET fallback really ran on the shared client
    assert slept == [2.0]  # slept the cap, not the 60s hint
    remaining = heal_backoff_remaining()
    assert 0.5 <= remaining <= 2.5, remaining  # scaled; pre-fix parked ~60s
    assert heal_rate_limited() is True
    assert col.resync.rate_limited(cid) is True  # per-book backoff keeps full hint
    assert _outcome(col, cid) == "rate_limited"
    assert _streak(col, cid) == 0
    assert heal_mod._heal_429_streak == 1


def test_heal_get_fallback_mirrors_owned_capped_sleep(tmp_path, monkeypatch):
    col = _collector(tmp_path)
    cid = "cid-twin-heal"
    col.markets[cid] = _market()
    shared = _SharedPostEmptyGet429()
    col._get_rest_client = lambda: shared  # shared path, NOT the owned branch
    slept = _patched_sleep(monkeypatch)
    book = _stale_book(cid)
    ok = asyncio.run(col._fetch_and_apply_rest_book(book, _market()))
    assert not ok
    assert shared.gets >= 1  # the GET fallback really ran on the shared client
    assert slept == [2.0]  # slept the cap, not the 60s hint
    remaining = heal_backoff_remaining()
    assert 0.5 <= remaining <= 2.5, remaining  # scaled; pre-fix parked ~60s
    assert heal_rate_limited() is True
    assert col.resync.rate_limited(cid) is True  # per-book backoff keeps full hint
    assert _outcome(col, cid) == "rate_limited"
    assert _streak(col, cid) == 0
    assert book.book_state.value == "stale"  # kept stale, never promoted
