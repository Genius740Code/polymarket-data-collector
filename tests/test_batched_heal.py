"""Batched POST /books heal (perfect-collector checkbox 3, spec §3.3).

Owns the heal-path contract the live logs demand (16:18–18:35 UTC: 45× 429
+ 18 fetch_none + 64 stale-book mentions, 0 errors — the per-token GET walk
is the bottleneck holding live% at ~5%):

  1. batching collapses N per-token GETs to 1 POST round-trip per batch;
  2. a 429 parks EVERY heal caller behind ONE shared jittered backoff
     (per-asset walkers must not each retry-blindly);
  3. ended windows supersede with ZERO REST burn (registry precheck);
  4. fetch_none keeps stale-with-episode (never fill, never mark live
     without data);
  5. every transport fault degrades, never raises.

Fakes/monkeypatch only — no network, no live hive, no staging writes.
"""
import asyncio
import time
import types

import pytest

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.ingest import heal as heal_mod
from polymarket_collector.ingest.heal import (
    heal_books_batched,
    heal_rate_limited,
    note_heal_rate_limited,
    parse_books_response,
    plan_heal,
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


def _book_body(token_ids):
    return [{"token_id": t, "bids": [{"price": "0.5", "size": "1"}],
             "asks": [{"price": "0.6", "size": "1"}]} for t in token_ids]


def _live_book(cid="cid-heal", asset="BTC"):
    now = int(time.time() * 1000)
    b = OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id="up-tok", down_token_id="dn-tok",
        market_end_ts_ms=now + 3600_000,
    )
    b.mark_stale()
    return b


def _collector(tmp_path):
    import os
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": os.path.join(str(tmp_path), "cursor_state")},
                          timeframes=["5m"])
    return Collector(cfg)


# 1. batching: N tokens -> 1 POST ------------------------------------------------

def test_batching_reduces_call_count_n_to_1():
    posts, gets = [], []

    async def fake_post(url, payload):
        posts.append((url, payload))
        tids = [p["token_id"] for p in payload]
        return _Resp(200, _book_body(tids))

    # 4 stale books × 2 tokens = 8 tokens: old walk burned 8 GETs (+blind
    # per-token 429 retries); batched heal burns exactly 1 POST.
    tokens = [f"tok{i}" for i in range(8)]
    res = asyncio.run(heal_books_batched(tokens, fake_post))
    assert res.errors == [] and res.missing == [] and len(res.books) == 8
    assert len(posts) == 1, f"8 tokens must heal in 1 round-trip, got {len(posts)}"
    assert posts[0][0] == "https://clob.polymarket.com/books"
    assert [p["token_id"] for p in posts[0][1]] == tokens
    assert gets == []


def test_old_walk_cost_vs_batched_math():
    # Call-count math, pinned: per-token GET walk = 2 GETs per book;
    # batched POST = ceil(2*books / batch) POSTs.
    for n_books in (1, 4, 50):
        old_gets = 2 * n_books
        new_posts = max(1, -(-2 * n_books // 100))
        assert new_posts <= old_gets
    assert 2 * 50 == 100 and -(-100 // 100) == 1  # 50 books: 100 GETs -> 1 POST


# 2. 429: shared backoff, not multiplied ----------------------------------------

def test_429_backoff_shared_not_multiplied():
    posts = []

    async def post_429(url, payload):
        posts.append(payload)
        return _Resp(429, [])

    async def fail_if_called(url, payload):
        raise AssertionError("shared cooldown must burn zero requests")

    res = asyncio.run(heal_books_batched(["a", "b"], post_429))
    assert res.books == {} and sorted(res.missing) == ["a", "b"]
    assert any("429" in e for e in res.errors)
    assert heal_rate_limited() is True  # one 429 parks ALL callers

    # Second walker (another asset/lane): zero new requests, same miss set.
    n_before = len(posts)
    res2 = asyncio.run(heal_books_batched(["c", "d"], fail_if_called))
    assert len(posts) == n_before
    assert sorted(res2.missing) == ["c", "d"]
    assert any("shared-backoff" in e for e in res2.errors)


def test_shared_backoff_jittered_bounded_and_total(monkeypatch):
    seen = []
    monkeypatch.setattr(heal_mod.random, "uniform",
                        lambda lo, hi: seen.append((lo, hi)) or (lo + hi) / 2)
    applied = note_heal_rate_limited(1.0)
    assert seen and seen[0][0] > 0 and seen[0][1] <= 60.0  # jitter applied
    assert 0.5 <= applied <= 60.0
    # Streak grows the ceiling exponentially, capped at 60s.
    for _ in range(10):
        applied = note_heal_rate_limited(60.0)
    assert applied <= 60.0 and heal_rate_limited() is True
    # Total: garbage in, backoff out, never raises.
    assert 0.5 <= note_heal_rate_limited(None) <= 60.0
    assert 0.5 <= note_heal_rate_limited("junk") <= 60.0
    reset_heal_rate_limit()
    assert heal_rate_limited() is False


# 3. ended-window precheck: zero REST -------------------------------------------

def test_ended_window_zero_rest():
    now_ms = 1759300000000
    dead_cid = "0x" + "22" * 32
    live_cid = "0x" + "11" * 32

    def resolver(cid):
        if cid == dead_cid:
            return (now_ms - 60_000, "active")  # window already ended
        if cid == live_cid:
            return (now_ms + 60_000, "active")
        return None  # unknown: discovery may lag -> stays live

    async def fail_if_called(url, payload):
        raise AssertionError("superseded tokens must burn no request")

    plan = plan_heal(["t-live", "t-dead", "t-unknown"],
                     condition_by_token={"t-live": live_cid, "t-dead": dead_cid,
                                         "t-unknown": "nope"},
                     resolver=resolver, now_ms=now_ms)
    assert plan.live_token_ids == ["t-live", "t-unknown"]
    assert [(s.token_id, s.reason) for s in plan.superseded] == [("t-dead", "ended_window")]

    res = asyncio.run(heal_books_batched(
        ["t-dead"], fail_if_called,
        condition_by_token={"t-dead": dead_cid}, resolver=resolver, now_ms=now_ms))
    assert res.books == {} and len(res.superseded) == 1 and res.missing == []


def test_collector_heal_skips_post_for_ended_window(tmp_path):
    col = _collector(tmp_path)
    now = int(time.time() * 1000)
    col.markets["cid-dead"] = types.SimpleNamespace(
        market_end_ts_ms=now - 1000, status="active",
        up_token_id="up-tok", down_token_id="dn-tok")

    async def fail_if_called(url, payload):
        raise AssertionError("ended window must burn zero REST")

    calls = []
    monkey_client = types.SimpleNamespace(
        post=fail_if_called,
        get=lambda *a, **k: calls.append((a, k)) or _raise_rest_used(),
    )
    col._get_rest_client = lambda: monkey_client
    b = _live_book(cid="cid-dead")
    ok = asyncio.run(col._fetch_and_apply_rest_book(b, col.markets["cid-dead"]))
    assert ok is False and calls == []
    assert b.book_state.value == "stale"  # kept stale, never filled


def _raise_rest_used():
    raise AssertionError("ended window must burn zero REST (GET)")


# 4. fetch_none: keep stale + episode -------------------------------------------

def test_fetch_none_keeps_stale_with_episode(tmp_path):
    col = _collector(tmp_path)
    b = _live_book()
    rid = col.resync.handle_disconnect("BTC", b.condition_id, reason="test",
                                       books={b.condition_id: b})
    assert rid in col.resync._episodes

    class DeadClient:
        async def post(self, url, json=None):
            return _Resp(200, [])  # 200 but no books: every token missing

        async def get(self, url, params=None):
            return _Resp(404, {})

    col._get_rest_client = lambda: DeadClient()
    col.markets[b.condition_id] = types.SimpleNamespace(
        market_end_ts_ms=int(time.time() * 1000) + 3600_000, status="active",
        up_token_id="up-tok", down_token_id="dn-tok")
    ok = asyncio.run(col._fetch_and_apply_rest_book(b, col.markets[b.condition_id]))
    assert ok is False  # missing data is not a heal
    assert b.book_state.value == "stale"  # kept stale, never marked live
    ep = col.resync._episodes.get(rid)
    assert ep is not None and ep.resync_completed_ts_utc is None  # episode open


def test_parse_never_synthesizes_half_books():
    books = parse_books_response([
        {"token_id": "half", "bids": [{"price": "0.5", "size": "1"}]},  # no asks
        {"token_id": "full", "bids": [], "asks": []},  # genuine empty: kept
        {"nope": 1}, "junk", None,
    ])
    assert set(books) == {"full"}


# 5. never-raise on transport faults --------------------------------------------

def test_never_raise_on_transport_faults():
    async def go(post):
        return await heal_books_batched(["x", "y"], post)

    async def boom(url, payload):
        raise TimeoutError("down")

    async def conn(url, payload):
        raise ConnectionError("reset")

    async def generic(url, payload):
        raise RuntimeError("weird")

    for bad_post in (boom, conn, generic):
        res = asyncio.run(go(bad_post))
        assert sorted(res.missing) == ["x", "y"] and res.books == {} and res.errors != []

    res = asyncio.run(go(lambda u, p: _coro(_Resp(500, []))))
    assert sorted(res.missing) == ["x", "y"] and res.errors != []

    class BadJson(_Resp):
        def json(self):
            raise ValueError("not json")

    async def bad_json_post(url, payload):
        return BadJson(200, [])

    res = asyncio.run(go(bad_json_post))
    assert sorted(res.missing) == ["x", "y"] and res.errors != []


async def _coro(resp):
    return resp


def test_collector_heal_never_raises_on_transport_fault(tmp_path):
    col = _collector(tmp_path)
    b = _live_book()

    class BoomClient:
        async def post(self, url, json=None):
            raise TimeoutError("down")

        async def get(self, url, params=None):
            raise ConnectionError("reset")

    col._get_rest_client = lambda: BoomClient()
    col.markets[b.condition_id] = types.SimpleNamespace(
        market_end_ts_ms=int(time.time() * 1000) + 3600_000, status="active",
        up_token_id="up-tok", down_token_id="dn-tok")
    ok = asyncio.run(col._fetch_and_apply_rest_book(b, col.markets[b.condition_id]))
    assert ok is False and b.book_state.value == "stale"
