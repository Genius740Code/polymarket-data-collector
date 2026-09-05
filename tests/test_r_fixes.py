"""Regression guards for the R-1..R-5 fixes (post-K-fix round).

- R-1  pre-warm snapshot gate: no snapshot rows are written before the gate,
       rows flow once the gate clears (cold-start window becomes warm).
- R-2  wallet backfill from BOTH data-api legs: taker on the BUY leg, maker on
       the SELL leg; fills the API has no wallet for stay NULL (never fabricated).
- R-3  reconciled api- rows get the API's own outcome label and a fee derived
       from the market's exchange-reported rate, flagged fee_is_estimated=True.
- R-4  Windows loop policy installs without error (selector loop, no Proactor
       teardown noise).
- R-5  scheduler catch-up emits `scheduler_lag`, NOT `backpressure`.
"""
import asyncio
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.enums import CollectorEventType
from polymarket_collector.rollover import MarketInfo
from polymarket_collector.storage.export import _backfill_trade_wallets
from polymarket_collector.storage.schemas import TRADES_SCHEMA


def make_cfg(tmpdir: str, assets: list | None = None) -> CollectorConfig:
    cfg = CollectorConfig()
    cfg.storage.data_dir = tmpdir
    cfg.storage.wal_dir = tmpdir + "/_wal"
    cfg.raw_archive.path = tmpdir + "/raw_ws_archive"
    cfg.raw_archive.enabled = False
    cfg.cursor_store.path = tmpdir + "/cursor_state"
    if assets:
        cfg.assets = assets
    return cfg


def _market(now_ms: int, asset: str = "BTC") -> MarketInfo:
    return MarketInfo(
        condition_id=f"cid-{asset}-1",
        market_id=f"mid-{asset}",
        asset=asset,
        up_token_id="up-tok",
        down_token_id="down-tok",
        market_start_ts_ms=now_ms - 60_000,
        market_end_ts_ms=now_ms + 60_000,
        window_index=now_ms // 300_000,
        series_id=f"{asset}-5MIN",
    )


# ---------------------------------------------------------------- R-1
@pytest.mark.asyncio
async def test_r1_prewarm_gate_blocks_then_releases_snapshots(tmp_path, monkeypatch):
    """Snapshots are gated until _snapshot_start_ms, then flow immediately."""
    cfg = make_cfg(str(tmp_path), assets=["BTC"])
    c = Collector(cfg)
    now_ms = int(time.time() * 1000)
    m = _market(now_ms)
    c.rollover = SimpleNamespace(
        active_markets=lambda a: [m],
        states={"BTC": SimpleNamespace(is_rollover_window=False)},
    )
    # avoid real REST heal calls from the test
    async def _no_heal(book, market):
        return None
    monkeypatch.setattr(c, "_heal_book_bg", _no_heal)

    appends: list = []

    def _fake_append(dataset, row, asset=None, **kw):
        appends.append(dataset)
        return True

    monkeypatch.setattr(c.writer, "append", _fake_append)

    c._running = True
    c._last_snapshot_bucket_ms = None
    c._snapshot_start_ms = now_ms + 10_000  # gate in the future (pre-warm)
    task = asyncio.create_task(c._snapshot_loop())
    await asyncio.sleep(1.6)
    gated = [d for d in appends if d == "book_snapshots_500ms"]
    assert gated == [], f"snapshots leaked through the pre-warm gate: {len(gated)} rows"

    c._snapshot_start_ms = None  # boundary reached — collector warm, gate opens
    await asyncio.sleep(1.6)
    c._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    flowing = [d for d in appends if d == "book_snapshots_500ms"]
    assert flowing, "no snapshots written after the gate cleared"


# ---------------------------------------------------------------- R-2 + R-3
def _trade_row(**over) -> dict:
    base = {
        "ts_source": str(int((time.time() - 60) * 1000)),
        "ts_received_ns": time.time_ns(),
        "condition_id": "cid-r2",
        "market_id": "mid-r2",
        "series_id": "BTC-5MIN",
        "window_index": 1,
        "asset": "BTC",
        "trade_id": "t-1",
        "transaction_hash": "0xb" * 8,
        "token_id": "up-tok",
        "outcome": "unknown",
        "price": 0.5,
        "size": 10.0,
        "notional": 5.0,
        "fee": 0.0,
        "fee_is_estimated": False,
        "side": "BUY",
        "aggressor_side": "BUY",
        "sequence_number": 1,
        "maker_wallet": None,
        "taker_wallet": None,
        "wallet": None,
    }
    base.update(over)
    return base


class _FakeResp:
    status_code = 200

    def __init__(self, rows):
        self._rows = rows

    def json(self):
        return self._rows


def test_r2_r3_wallet_backfill_both_legs_and_reconciliation(monkeypatch):
    """R-2: maker+taker wallets from the API's two legs; R-3: outcome + derived
    fee on reconciled rows. Fills the API has no wallet for stay NULL."""
    legs = [
        # both legs of one fill: SELL = maker, BUY = taker (same tx/price/size)
        {"transactionHash": "0xb" * 8, "proxyWallet": "0xmaker", "side": "SELL",
         "asset": "up-tok", "price": 0.5, "size": 10.0, "timestamp": time.time() - 60,
         "outcome": "Up"},
        {"transactionHash": "0xb" * 8, "proxyWallet": "0xtaker", "side": "BUY",
         "asset": "up-tok", "price": 0.5, "size": 10.0, "timestamp": time.time() - 60,
         "outcome": "Up"},
    ]
    taker_rows = [
        # the streamed fill (matched — no insert) + one missing fill to reconcile
        {"transactionHash": "0xb" * 8, "proxyWallet": "0xtaker", "side": "BUY",
         "asset": "up-tok", "price": 0.5, "size": 10.0, "timestamp": time.time() - 60,
         "outcome": "Up"},
        {"transactionHash": "0xc" * 8, "proxyWallet": "0xw3", "side": "BUY",
         "asset": "up-tok", "price": 0.6, "size": 5.0, "timestamp": time.time() - 50,
         "outcome": "Down"},
        # a fill the API itself has no wallet for — must stay NULL
        {"transactionHash": "0xd" * 8, "proxyWallet": "", "side": "BUY",
         "asset": "up-tok", "price": 0.7, "size": 2.0, "timestamp": time.time() - 40,
         "outcome": "Up"},
    ]

    def fake_get(url, params=None, timeout=None):
        params = params or {}
        if params.get("takerOnly") == "false":
            return _FakeResp(legs)
        return _FakeResp(taker_rows)

    import httpx
    monkeypatch.setattr(httpx, "get", fake_get)

    # one streamed row: fee 0.0 exchange-reported on notional 5.0 → rate 0.0
    table = pa.Table.from_pylist([_trade_row()], schema=TRADES_SCHEMA)
    out = _backfill_trade_wallets(table, Path("."), asset="BTC")
    rows = out.to_pylist()
    assert len(rows) == 3, f"expected 1 streamed + 2 reconciled rows, got {len(rows)}"

    streamed = [r for r in rows if r["trade_id"] == "t-1"][0]
    # R-2: taker from the BUY leg, maker from the SELL leg, outcome from the API
    assert streamed["taker_wallet"] == "0xtaker"
    assert streamed["maker_wallet"] == "0xmaker"
    assert streamed["wallet"] == "0xtaker"
    assert streamed["outcome"] == "up"

    api_rows = [r for r in rows if str(r["trade_id"]).startswith("api-")]
    by_tx = {r["transaction_hash"]: r for r in api_rows}
    filled = by_tx["0xc" * 8]
    assert filled["taker_wallet"] == "0xw3"
    # R-3: outcome from the API's own label, fee derived from the streamed rate
    assert filled["outcome"] == "down"
    assert filled["fee"] == 0.0
    assert filled["fee_is_estimated"] is True

    # R-2 honesty: the API has no wallet for this fill → NULL is kept
    unknown = by_tx["0xd" * 8]
    assert unknown["wallet"] is None and unknown["taker_wallet"] is None
    assert unknown["outcome"] == "up"


# ---------------------------------------------------------------- R-4
def test_r4_windows_loop_policy_installs():
    from polymarket_collector.cli import _install_windows_loop_policy
    _install_windows_loop_policy()
    if sys.platform == "win32":
        import asyncio
        assert isinstance(asyncio.get_event_loop_policy(), asyncio.WindowsSelectorEventLoopPolicy)


# ---------------------------------------------------------------- R-5
@pytest.mark.asyncio
async def test_r5_missed_buckets_emit_scheduler_lag_not_backpressure(tmp_path):
    """A 3-bucket catch-up on the first tick must emit scheduler_lag — the
    catch-up signal is not writer backpressure (dropped_total stayed 0)."""
    cfg = make_cfg(str(tmp_path), assets=["BTC"])
    c = Collector(cfg)
    events: list = []
    c._collector_event = lambda t, d: events.append(t)
    c.rollover = SimpleNamespace(active_markets=lambda a: [], states={})
    now_ms = int(time.time() * 1000)
    c._running = True
    c._snapshot_start_ms = None
    # leave the loop 3 buckets (1500ms) behind → first iteration is catch-up
    c._last_snapshot_bucket_ms = ((now_ms // 500) * 500) - 1500
    task = asyncio.create_task(c._snapshot_loop())
    await asyncio.sleep(1.0)
    c._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert CollectorEventType.scheduler_lag in events, "expected a scheduler_lag event on catch-up"
    assert CollectorEventType.backpressure not in events, "catch-up must not be reported as backpressure"
