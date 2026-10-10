"""pmdata-parity heal fixes: strike discipline, 429 decay, bypass close, trigger gate.

Covers the three audit fixes owned by heal.py + the collector heal call sites:

  1. fetch_none strikes count ONLY genuine 200-empty/404; timeouts,
     transport errors and bad bodies are unknown (no increment). REST
     attempts cap at ~1/5s per book; a fresh WS book-content frame clears
     streak + quiet.
  2. The shared 429 streak decays after 60s clean or any 2xx; while the
     shared cooldown is active the heal path returns early (POST *and* GET
     fallback burn zero requests).
  3. First-bucket heal fires only for stale/resyncing books, paced per book
     (live one-sided books no longer heal every tick).

Stubs only — no network, no live hive, no staging writes. Real data law:
no invented books; every transport fault stays an honest miss.
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
    classify_heal_failure,
    heal_books_batched,
    heal_rate_limited,
    note_heal_rate_limited,
    reset_heal_rate_limit,
    should_heal_trigger,
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


class _BadBody(_Resp):
    def json(self):
        raise ValueError("not json")


def _book_body(token_ids):
    return [{"token_id": t, "bids": [{"price": "0.5", "size": "1"}],
             "asks": [{"price": "0.6", "size": "1"}]} for t in token_ids]


class _EmptyBoth:
    """Genuine empty venue: POST 200 with no books, GET 404."""

    async def post(self, url, json=None):
        return _Resp(200, [])

    async def get(self, url, params=None):
        return _Resp(404, {})


class _BoomBoth:
    """Transport down: timeouts on POST, connection resets on GET."""

    async def post(self, url, json=None):
        raise TimeoutError("down")

    async def get(self, url, params=None):
        raise ConnectionError("reset")


class _BadBodyBoth:
    """200 with an unparsable body on both paths."""

    async def post(self, url, json=None):
        return _BadBody(200, [])

    async def get(self, url, params=None):
        return _BadBody(200, {})


class _ServerErrorBoth:
    """Transient 500s — unknown, never a strike."""

    async def post(self, url, json=None):
        return _Resp(500, [])

    async def get(self, url, params=None):
        return _Resp(500, {})


class _FullBooks:
    """Healthy venue: one POST heals both tokens."""

    async def post(self, url, json=None):
        tids = [p["token_id"] for p in (json or [])]
        return _Resp(200, _book_body(tids))

    async def get(self, url, params=None):  # pragma: no cover
        raise AssertionError("GET fallback must not run after a POST heal")


class _Counting:
    def __init__(self, inner):
        self.inner = inner
        self.posts = 0
        self.gets = 0

    async def post(self, url, json=None):
        self.posts += 1
        return await self.inner.post(url, json=json)

    async def get(self, url, params=None):
        self.gets += 1
        return await self.inner.get(url, params=params)


def _collector(tmp_path):
    import os
    cfg = CollectorConfig(assets=["BTC"],
                          storage={"data_dir": str(tmp_path)},
                          cursor_store={"path": os.path.join(str(tmp_path), "cursor_state")},
                          timeframes=["5m"])
    return Collector(cfg)


def _stale_book(cid="cid-heal", asset="BTC"):
    now = int(time.time() * 1000)
    b = OrderBookState(
        asset=asset, condition_id=cid, market_id="mid-1", series_id="BTC-5MIN",
        window_index=1, up_token_id="up-tok", down_token_id="dn-tok",
        market_end_ts_ms=now + 3600_000,
    )
    b.mark_stale()
    return b


def _market():
    return types.SimpleNamespace(
        market_end_ts_ms=int(time.time() * 1000) + 3600_000, status="active",
        up_token_id="up-tok", down_token_id="dn-tok")


def _clear_caps(col):
    col.__dict__.pop("_heal_last_attempt_monotonic", None)
    col.__dict__.pop("_heal_last_trigger_monotonic", None)


def _streak(col, cid="cid-heal"):
    return int(col.resync._fetch_none_streak.get(cid, 0) or 0)


# 1. strike discipline: genuine-empty strikes, everything else does not -----


def test_classify_failure_by_error_class():
    assert classify_heal_failure([]) == "genuine_empty"  # 200, token absent
    assert classify_heal_failure(None) == "genuine_empty"
    assert classify_heal_failure(["bad_status:404"]) == "genuine_empty"
    assert classify_heal_failure(["bad_status:404", "bad_status:404"]) == "genuine_empty"
    assert classify_heal_failure(["post_failed:TimeoutError: down"]) == "unknown"
    assert classify_heal_failure(["post_failed:ConnectionError: reset"]) == "unknown"
    assert classify_heal_failure(["bad_body:not json"]) == "unknown"
    assert classify_heal_failure(["bad_status:500"]) == "unknown"
    assert classify_heal_failure(["bad_status:400"]) == "unknown"
    assert classify_heal_failure(["bad_status:404", "post_failed:x"]) == "unknown"
    assert classify_heal_failure(["rate_limited:429"]) == "rate_limited"
    assert classify_heal_failure(["bad_status:404", "rate_limited:shared-backoff"]) == "rate_limited"
    assert classify_heal_failure(object()) == "unknown"


def _run_bg(col, book, market):
    _clear_caps(col)
    asyncio.run(col._heal_book_bg(book, market))


def test_genuine_empty_strikes_to_quiet(tmp_path):
    col = _collector(tmp_path)
    col._get_rest_client = lambda: _EmptyBoth()
    b = _stale_book()
    m = _market()
    for _ in range(5):
        _run_bg(col, b, m)
    assert _streak(col) == 5
    assert col.resync.fetch_none_quiet("cid-heal") is True
    assert b.book_state.value == "stale"  # kept stale, never promoted
    assert (getattr(col, "_heal_last_outcome", {}) or {}).get("cid-heal") == "genuine_empty"


@pytest.mark.parametrize("client_cls", [_BoomBoth, _BadBodyBoth, _ServerErrorBoth])
def test_unknown_never_strikes(tmp_path, client_cls):
    col = _collector(tmp_path)
    col._get_rest_client = client_cls
    b = _stale_book()
    m = _market()
    for _ in range(5):
        _run_bg(col, b, m)
    assert _streak(col) == 0
    assert col.resync.fetch_none_quiet("cid-heal") is False
    assert b.book_state.value == "stale"
    assert (getattr(col, "_heal_last_outcome", {}) or {}).get("cid-heal") == "unknown"


def test_attempt_cap_burns_one_round(tmp_path):
    col = _collector(tmp_path)
    counting = _Counting(_EmptyBoth())
    col._get_rest_client = lambda: counting
    b = _stale_book()
    m = _market()
    _clear_caps(col)
    asyncio.run(col._heal_book_bg(b, m))  # burns 1 POST + 2 GETs
    first_posts, first_gets = counting.posts, counting.gets
    assert first_posts == 1 and first_gets == 2
    asyncio.run(col._heal_book_bg(b, m))  # capped: burns nothing
    asyncio.run(col._heal_book_bg(b, m))  # capped: burns nothing
    assert (counting.posts, counting.gets) == (first_posts, first_gets)
    assert _streak(col) == 1  # capped rounds record no strike either


def test_ws_frame_clears_suppression(tmp_path):
    col = _collector(tmp_path)
    col._get_rest_client = lambda: _EmptyBoth()
    b = _stale_book()
    m = _market()
    for _ in range(5):
        _run_bg(col, b, m)
    assert col.resync.fetch_none_quiet("cid-heal") is True
    # A fresh WS book-content frame lands (frame clock stamped by the WS path).
    col._last_frame_ns_per_book["cid-heal"] = time.time_ns()
    _run_bg(col, b, m)
    assert col.resync.fetch_none_quiet("cid-heal") is False
    assert _streak(col) == 1  # old streak dropped; only this round struck


# 2. 429 decay + early return ----------------------------------------------


def test_429_streak_decays_when_clean():
    for _ in range(3):
        note_heal_rate_limited(1.0)
    assert heal_rate_limited() is True
    assert heal_mod._heal_429_streak == 3
    # Wire goes quiet past the clean window: decay, no parking.
    heal_mod._heal_429_until = 0.0
    heal_mod._heal_429_last_event = time.monotonic() - 61.0
    assert heal_rate_limited() is False
    assert heal_mod._heal_429_streak == 0


def test_2xx_resets_429_streak():
    note_heal_rate_limited(1.0)
    note_heal_rate_limited(1.0)
    assert heal_rate_limited() is True
    # Cooldown expires but the streak is still banked (wire quiet < 60s).
    heal_mod._heal_429_until = 0.0
    assert heal_mod._heal_429_streak == 2

    async def stub_post(url, payload):
        tids = [p["token_id"] for p in payload]
        return _Resp(200, _book_body(tids))

    res = asyncio.run(heal_books_batched(["a", "b"], stub_post))
    assert len(res.books) == 2
    assert heal_rate_limited() is False
    assert heal_mod._heal_429_streak == 0


def test_rate_limited_early_return_skips_post_and_get(tmp_path):
    col = _collector(tmp_path)
    counting = _Counting(_FullBooks())
    col._get_rest_client = lambda: counting
    b = _stale_book()
    m = _market()
    col.markets["cid-heal"] = m
    note_heal_rate_limited(60.0)
    assert heal_rate_limited() is True
    ok = asyncio.run(col._fetch_and_apply_rest_book(b, m))
    assert ok is False
    assert (counting.posts, counting.gets) == (0, 0)
    assert (getattr(col, "_heal_last_outcome", {}) or {}).get("cid-heal") == "rate_limited"
    assert _streak(col) == 0  # limited rounds never strike
    assert col.resync.rate_limited("cid-heal") is True  # bounded backoff noted
    assert asyncio.run(col._fetch_rest_book("BTC", "cid-heal")) is None
    assert (counting.posts, counting.gets) == (0, 0)


def test_post_heal_is_one_round_trip(tmp_path):
    col = _collector(tmp_path)
    counting = _Counting(_FullBooks())
    col._get_rest_client = lambda: counting
    b = _stale_book()
    ok = asyncio.run(col._fetch_and_apply_rest_book(b, _market()))
    assert ok is True
    assert (counting.posts, counting.gets) == (1, 0)  # 2 tokens, 1 POST, 0 GETs
    assert b.book_state.value == "live"  # real REST data promotes
    assert (getattr(col, "_heal_last_outcome", {}) or {}).get("cid-heal") == "ok"


# 3. trigger gate: live one-sided books must not heal-spam ------------------


def test_should_heal_trigger_gate():
    now = time.monotonic()
    assert should_heal_trigger("live", "c", {}, now) is False
    assert should_heal_trigger("LIVE", "c", {}, now) is False
    assert should_heal_trigger("", "c", {}, now) is False
    assert should_heal_trigger(None, "c", {}, now) is False
    assert should_heal_trigger("stale", "c", {}, now) is True
    assert should_heal_trigger("resyncing", "c", {}, now) is True
    assert should_heal_trigger("Stale", "c", {}, now) is True
    assert should_heal_trigger("stale", "c", {"c": now}, now) is False
    assert should_heal_trigger("stale", "c", {"c": now - 10.0}, now) is False
    assert should_heal_trigger("stale", "c", {"c": now - 31.0}, now) is True
    assert should_heal_trigger("stale", "c", None, now) is True


def test_live_book_refuses_trigger_real_state():
    b = _stale_book()
    now = time.monotonic()
    assert should_heal_trigger(b.book_state.value, b.condition_id, {}, now) is True
    b.mark_live()
    assert b.book_state.value == "live"
    assert should_heal_trigger(b.book_state.value, b.condition_id, {}, now) is False
