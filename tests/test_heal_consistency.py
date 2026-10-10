"""Detached heal-drive consistency (follow-up to the heal-cluster fix).

The detached pass (_schedule_heal_tick_pass / _heal_tick_drive) used to
call resync() directly, bypassing three gates the inline heal path owns:

  1. the 5s per-book attempt cap (_heal_last_attempt_monotonic);
  2. the shared-429 early-return (heal_rate_limited) — without it a drive
     spins the resync retry loop while the fetcher returns None at once;
  3. outcome recording (_heal_last_outcome strike-discipline vocabulary).

And _heal_tick_candidates mirrored the pre-fix gates (no shared-cooldown
check), scheduling REST during the shared cooldown. Plus the capped-sleep
site in _fetch_rest_book slept min(Retry-After, 2s) yet parked the shared
cooldown behind the full hint.

Stubs + patched clocks only — no network, no sockets, no market data.
Every transport fault stays an honest miss; totals never raise.
"""
import asyncio
import os
import time
import types

import pytest

import httpx

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.ingest import heal as heal_mod
from polymarket_collector.ingest.heal import (
    heal_backoff_remaining,
    heal_rate_limited,
    note_heal_rate_limited,
    reset_heal_rate_limit,
)


@pytest.fixture(autouse=True)
def _clean_shared_cooldown():
    reset_heal_rate_limit()
    yield
    reset_heal_rate_limit()


def _collector(tmp_path):
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": os.path.join(str(tmp_path), "cursor_state")},
                          timeframes=["5m"])
    return Collector(cfg)


def _stale_book(cid, asset="BTC"):
    b = OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id=f"up-{cid}", down_token_id=f"dn-{cid}",
        market_end_ts_ms=int(time.time() * 1000) + 3600_000,
    )
    b.mark_stale()
    return b


def _market():
    return types.SimpleNamespace(
        market_end_ts_ms=int(time.time() * 1000) + 3600_000, status="active",
        up_token_id="up-tok", down_token_id="dn-tok")


def _outcome(col, cid):
    return (getattr(col, "_heal_last_outcome", None) or {}).get(cid)


# 1. detached drive respects the 5s attempt cap -------------------------------


def test_detached_drive_respects_attempt_cap(tmp_path):
    col = _collector(tmp_path)
    cid = "cid-cap-detached"
    col.books[cid] = _stale_book(cid)
    book = col.books[cid]

    # Drive twice through ONE stub to count attempts across drives.
    async def _scenario_counted():
        seen = []

        async def stub_resync(asset, condition_id, books, resync_id):
            seen.append(condition_id)
            return True

        col.resync.resync = stub_resync
        col._running = True
        shared = col._heal_tick_state()
        ps = {"pending": 0, "healed": 0}
        await col._heal_tick_drive(book, shared, ps)
        await col._heal_tick_drive(book, shared, ps)
        return seen, ps

    seen, ps = asyncio.run(_scenario_counted())
    assert seen == [cid]  # second drive capped: exactly one REST drive
    assert ps["healed"] == 1
    assert _outcome(col, cid) == "ok"


# 2. detached drive early-returns during the shared cooldown ------------------


def test_detached_drive_respects_shared_cooldown(tmp_path):
    col = _collector(tmp_path)
    cid = "cid-cool-detached"
    col.books[cid] = _stale_book(cid)
    book = col.books[cid]
    note_heal_rate_limited(60.0)
    assert heal_rate_limited() is True

    async def _scenario():
        seen = []

        async def stub_resync(asset, condition_id, books, resync_id):
            seen.append(condition_id)
            return True

        col.resync.resync = stub_resync
        col._running = True
        shared = col._heal_tick_state()
        ps = {"pending": 0, "healed": 0}
        await col._heal_tick_drive(book, shared, ps)
        return seen, ps

    seen, ps = asyncio.run(_scenario())
    assert seen == []  # zero REST burn during the shared cooldown
    assert ps["healed"] == 0
    assert _outcome(col, cid) == "rate_limited"
    assert col.resync.rate_limited(cid) is True  # bounded per-book backoff noted
    assert int(col.resync._fetch_none_streak.get(cid, 0) or 0) == 0


def test_detached_drive_records_unknown_on_plain_failure(tmp_path):
    col = _collector(tmp_path)
    cid = "cid-unk-detached"
    col.books[cid] = _stale_book(cid)
    book = col.books[cid]

    async def _scenario():
        seen = []

        async def stub_resync(asset, condition_id, books, resync_id):
            seen.append(condition_id)
            return False  # honest miss, no per-book backoff behind it

        col.resync.resync = stub_resync
        col._running = True
        shared = col._heal_tick_state()
        ps = {"pending": 0, "healed": 0}
        await col._heal_tick_drive(book, shared, ps)
        return seen, ps

    seen, ps = asyncio.run(_scenario())
    assert seen == [cid]
    assert ps["healed"] == 0
    assert _outcome(col, cid) == "unknown"  # never invents genuine_empty
    assert int(col.resync._fetch_none_streak.get(cid, 0) or 0) == 0


# 3. candidates schedule nothing during the shared cooldown -------------------


def test_candidates_skip_during_shared_cooldown(tmp_path):
    col = _collector(tmp_path)
    cid = "cid-cand-cool"
    col.books[cid] = _stale_book(cid)
    assert col._heal_tick_candidates(40, 60) != []
    note_heal_rate_limited(60.0)
    assert heal_rate_limited() is True
    assert col._heal_tick_candidates(40, 60) == []
    reset_heal_rate_limit()
    assert col._heal_tick_candidates(40, 60) != []


# 4. capped sleep scales the shared note to the actual sleep ------------------


class _Resp429:
    status_code = 429
    headers = {"retry-after": "60"}

    def json(self):
        return {}


class _StubOwnedClient:
    """httpx.AsyncClient stand-in forcing the owned-client branch."""

    def __init__(self, *args, **kwargs):
        self.gets = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def aclose(self):
        return None

    async def post(self, url, json=None):
        raise TimeoutError("venue down")

    async def get(self, url, params=None):
        self.gets += 1
        return _Resp429()


def test_capped_sleep_scales_shared_note(tmp_path, monkeypatch):
    col = _collector(tmp_path)
    cid = "cid-sleep-scale"
    col.markets[cid] = _market()
    col._get_rest_client = lambda: None  # force the owned-client branch
    monkeypatch.setattr(httpx, "AsyncClient", _StubOwnedClient)

    slept = []
    _real_sleep = asyncio.sleep

    async def _rec_sleep(delay):
        slept.append(delay)
        await _real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", _rec_sleep)
    out = asyncio.run(col._fetch_rest_book("BTC", cid))
    assert out is None
    assert slept == [2.0]  # slept the cap, not the 60s hint
    remaining = heal_backoff_remaining()
    assert 0.5 <= remaining <= 2.5, remaining  # scaled; pre-fix parked U(30,60)s
    assert heal_rate_limited() is True
    assert col.resync.rate_limited(cid) is True  # per-book backoff keeps full hint
    assert _outcome(col, cid) == "rate_limited"
    assert int(col.resync._fetch_none_streak.get(cid, 0) or 0) == 0
    assert heal_mod._heal_429_streak == 1
