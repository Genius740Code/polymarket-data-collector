"""Rollover/resync wiring in collector.py (follow-up to 6833227).

1. Resolution settle triggers RolloverManager.notify_settlement so the fresh
   window is discovery-polled immediately (not after old-window backoff).
2. WS frames feed ResyncManager WS-provisional promotion for markets with
   zero REST-verified book (gaps only — a REST-verified book is never
   overridden or downgraded).

Real data only: inline literals, real Collector/OrderBookState/ResyncManager,
no mocks, no synthetic generation.
"""
import asyncio
import time

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.rollover import MarketInfo


import pytest


def make_collector(tmp_path):
    return Collector(CollectorConfig(
        assets=["BTC"],
        storage={"data_dir": str(tmp_path)},
        cursor_store={"path": str(tmp_path / "cursor_state")},
        timeframes=["5m"],
    ))


def _market(cid, asset, start_ms, end_ms, window_index=7):
    return MarketInfo(
        condition_id=cid,
        market_id=f"mid-{cid}",
        asset=asset,
        up_token_id=f"{cid}-UP",
        down_token_id=f"{cid}-DOWN",
        market_start_ts_ms=start_ms,
        market_end_ts_ms=end_ms,
        window_index=window_index,
        series_id=f"{asset}-5MIN",
        window_label="5m",
        window_size_seconds=300,
    )


def _pc_frame(up_tok, ts_ms):
    """Two-sided sane price_change frame: up bid 0.45 / ask 0.47."""
    return {
        "event_type": "price_change",
        "market": "wire-mkt",
        "timestamp": str(ts_ms),
        "price_changes": [
            {"asset_id": up_tok, "price": "0.45", "size": "10", "side": "BUY"},
            {"asset_id": up_tok, "price": "0.47", "size": "10", "side": "SELL"},
        ],
    }


def _stale_book(col, cid, asset="BTC"):
    book = OrderBookState(
        asset=asset,
        condition_id=cid,
        market_id=f"mid-{cid}",
        series_id=f"{asset}-5MIN",
        window_index=7,
        up_token_id=f"{cid}-UP",
        down_token_id=f"{cid}-DOWN",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
        l2_levels=20,
    )
    book.mark_stale(resync_id="rid-provisional-1")
    col.books[cid] = book
    col._index_book(book)
    return book


@pytest.mark.asyncio
async def test_settle_triggers_discovery_notify(tmp_path, monkeypatch):
    col = make_collector(tmp_path)
    now_ms = int(time.time() * 1000)
    cid = "settle-cid-1"
    col.markets[cid] = _market(cid, "BTC", now_ms - 400_000, now_ms - 5_000)
    col._chainlink_events.append(
        {"asset": "BTC", "price": 100.0, "_ts_ms": now_ms - 401_000,
         "ts_source": now_ms - 401_000})
    col._chainlink_events.append(
        {"asset": "BTC", "price": 101.0, "_ts_ms": now_ms - 5_500,
         "ts_source": now_ms - 5_500})
    # lane parked on old-window backoff: settle must drop it for immediate poll
    state = col.rollover.states[("BTC", "5m")]
    state.last_discovery_attempt_ms = now_ms
    try:
        col.rollover.discoveries["5m"]._backoff_s = 8.0
    except Exception:
        pass

    real_sleep = asyncio.sleep
    calls = {"n": 0}

    async def fast_sleep(s):
        calls["n"] += 1
        if calls["n"] >= 2:
            col._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    col._running = True
    await col._resolution_stuck_loop()

    assert cid in col._resolved_cids
    assert state.last_discovery_attempt_ms is None


def test_provisional_promotion_fires_only_with_no_rest_book(tmp_path):
    col = make_collector(tmp_path)
    cid = "prov-cid-1"
    book = _stale_book(col, cid)
    frame = _pc_frame(f"{cid}-UP", int(time.time() * 1000))

    col._handle_shard_frame(dict(frame), ["BTC"], source_conn="A")
    assert book.book_state.value == "stale"
    col._handle_shard_frame(dict(frame), ["BTC"], source_conn="B")
    assert book.book_state.value == "stale"
    # third consecutive consistent frame promotes the gap book live
    col._handle_shard_frame(dict(frame), ["BTC"], source_conn="A")
    assert book.book_state.value == "live"
    # streak bank cleared on promotion
    assert cid not in col.resync._ws_provisional


def test_rest_book_never_overridden(tmp_path):
    col = make_collector(tmp_path)
    cid = "prov-cid-2"
    book = _stale_book(col, cid)
    col.resync.note_fetch_ok(cid)  # REST-verified: REST discipline owns it
    frame = _pc_frame(f"{cid}-UP", int(time.time() * 1000))
    for leg in ("A", "B", "A", "B", "A"):
        col._handle_shard_frame(dict(frame), ["BTC"], source_conn=leg)
    assert book.book_state.value == "stale"
    # not even a provisional streak is banked on a REST-verified book
    assert cid not in col.resync._ws_provisional


def test_provisional_never_downgrades_live_book(tmp_path):
    col = make_collector(tmp_path)
    cid = "prov-cid-3"
    book = _stale_book(col, cid)
    book.mark_live()
    frame = _pc_frame(f"{cid}-UP", int(time.time() * 1000))
    col._handle_shard_frame(dict(frame), ["BTC"], source_conn="A")
    assert book.book_state.value == "live"
