"""Regression guards for the B-round fixes (post-R round, backtest quality).

- B-5: hive trades enrichment write-back fills NULLs only, never overwrites,
  and is idempotent.
- B-7: resolution backfill upgrades ended markets to the OFFICIAL CLOB outcome
  (tokens[].winner) via append-only log + compact; unsettled markets are left
  for the next run.
- B-6: book_snapshots_clean is part of the per-asset staging datasets.
"""
import time
from pathlib import Path

import pyarrow as pa
import pytest

from polymarket_collector.storage.export import (
    PER_ASSET_DATASETS,
    _writeback_enriched_trades,
)
from polymarket_collector.storage.markets_log import MarketsLog
from polymarket_collector.storage.schemas import SCHEMAS, TRADES_SCHEMA


# ---------------------------------------------------------------- B-5
def _trade_row(**over) -> dict:
    base = {
        "ts_source": str(int(time.time() * 1000)),
        "ts_received_ns": time.time_ns(),
        "condition_id": "cid-b5",
        "market_id": "mid-b5",
        "series_id": "BTC-5MIN",
        "window_index": 1,
        "asset": "BTC",
        "trade_id": "t-1",
        "transaction_hash": "0x" + "1" * 8,
        "token_id": "tok",
        "outcome": "unknown",
        "price": 0.5,
        "size": 10.0,
        "notional": 5.0,
        "fee": None,
        "fee_is_estimated": None,
        "side": "BUY",
        "aggressor_side": "BUY",
        "sequence_number": 1,
        "maker_wallet": None,
        "taker_wallet": None,
        "wallet": None,
    }
    base.update(over)
    return base


def test_b5_writeback_fills_nulls_only_and_is_idempotent(tmp_path):
    from polymarket_collector.storage.parquet_io import read_table as pio_read
    hive = tmp_path / "trades" / "date=2026-09-05" / "asset=BTC"
    hive.mkdir(parents=True)
    part = hive / "trades_1.parquet"
    # hive state at collection time: wallets/outcome/fee all NULL
    rows = [_trade_row(trade_id="t-1"), _trade_row(trade_id="t-2", outcome="up")]
    pa.Table.from_pylist(rows, schema=TRADES_SCHEMA)
    tbl = pa.table({c: pa.array([r[c] for r in rows], type=TRADES_SCHEMA.field(c).type) for c in TRADES_SCHEMA.names})
    import pyarrow.parquet as pq
    pq.write_table(tbl, str(part))

    # enrichment: t-1 fully enriched, t-2 untouched (still NULL where it was NULL)
    enriched_rows = [
        _trade_row(trade_id="t-1", maker_wallet="0xmaker", taker_wallet="0xtaker",
                   wallet="0xtaker", outcome="up", fee=0.0, fee_is_estimated=True),
        _trade_row(trade_id="t-2", maker_wallet=None, taker_wallet=None,
                   wallet=None, outcome="up", fee=None, fee_is_estimated=None),
        # api- row: staging-only insert, must NOT be written back
        _trade_row(trade_id="api-abcdef-1", maker_wallet="0xzz", taker_wallet="0xzz", wallet="0xzz"),
    ]
    enriched = pa.Table.from_pylist(enriched_rows, schema=TRADES_SCHEMA)

    n = _writeback_enriched_trades(tmp_path, "BTC", enriched)
    assert n == 1, "exactly the one changed part file should be rewritten"

    got = pio_read(part).to_pylist()
    by_id = {r["trade_id"]: r for r in got}
    t1 = by_id["t-1"]
    assert t1["maker_wallet"] == "0xmaker" and t1["wallet"] == "0xtaker"
    assert t1["outcome"] == "up" and t1["fee"] == 0.0 and t1["fee_is_estimated"] is True
    # non-NULL values were never overwritten
    t2 = by_id["t-2"]
    assert t2["outcome"] == "up" and t2["wallet"] is None
    # api- row not injected into the hive
    assert "api-abcdef-1" not in by_id

    # idempotent: re-running changes nothing
    n2 = _writeback_enriched_trades(tmp_path, "BTC", enriched)
    assert n2 == 0


# ---------------------------------------------------------------- B-6
def test_b6_clean_view_is_a_staging_per_asset_dataset():
    assert "book_snapshots_clean" in PER_ASSET_DATASETS
    # schema resolvable for the export path
    from polymarket_collector.storage.export import _get_schema
    assert _get_schema("book_snapshots_clean", 20) is not None


# ---------------------------------------------------------------- B-7
def _market_row(cid: str, asset: str, widx: int, status: str, outcome: str = "unknown") -> dict:
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
    end_ms = (int(time.time()) - 600) * 1000  # ended 10 min ago
    return {
        "updated_at": now_iso,
        "recorded_at": now_iso,
        "market_start_ts": None,
        "market_end_ts": None,
        "market_start_ts_ms": end_ms - 300_000,
        "market_end_ts_ms": end_ms,
        "resolution_ts": None,
        "condition_id": cid,
        "market_id": cid,
        "series_id": f"{asset}-5MIN",
        "window_index": widx,
        "window_label": "5m",
        "window_size_seconds": 300,
        "asset": asset,
        "up_token_id": "up",
        "down_token_id": "down",
        "status": status,
        "resolution_outcome": outcome,
        "question": None,
        "resolution_rule": None,
        "resolution_source": None,
        "tick_size": None,
        "minimum_order_size": None,
        "minimum_notional": None,
        "fee_information": None,
        "reported_volume": None,
        "reported_liquidity": None,
        "settlement_price": None,
        "settlement_ts_utc": None,
        "resolution_confirmed_at": None,
        "settlement_source": None,
    }


class _Resp:
    status_code = 200

    def __init__(self, j):
        self._j = j

    def json(self):
        return self._j


def test_b7_resolution_backfill_official_outcome(tmp_path, monkeypatch):
    import httpx
    from polymarket_collector.resolution_backfill import backfill_resolutions

    log = MarketsLog(tmp_path)
    log.append(_market_row("0xaaa", "BTC", 100, status="closed"))
    log.append(_market_row("0xbbb", "ETH", 101, status="resolved", outcome="up"))
    log.append(_market_row("0xccc", "SOL", 102, status="active"))
    log.flush_staging()
    log.compact()

    def fake_get(url, timeout=None, headers=None):
        if "0xaaa" in url:
            # settled: Down won
            return _Resp({"closed": True, "tokens": [
                {"outcome": "Up", "price": 0, "winner": False},
                {"outcome": "Down", "price": 1, "winner": True},
            ]})
        if "0xbbb" in url:
            return _Resp({"closed": True, "tokens": [
                {"outcome": "Up", "price": 1, "winner": True},
                {"outcome": "Down", "price": 0, "winner": False},
            ]})
        if "0xccc" in url:
            # not settled yet — must be left pending
            return _Resp({"closed": False, "tokens": [
                {"outcome": "Up", "price": 0.5, "winner": False},
                {"outcome": "Down", "price": 0.5, "winner": False},
            ]})
        raise AssertionError(url)

    monkeypatch.setattr(httpx, "get", fake_get)
    stats = backfill_resolutions(tmp_path)

    # 0xbbb is resolved-but-not-official → candidate for inferred→official upgrade
    assert stats["candidates"] == 3
    assert stats["resolved"] == 1 and stats["upgraded"] == 1 and stats["pending"] == 1

    latest = {r["condition_id"]: r for r in MarketsLog(tmp_path).load_latest()}
    aaa = latest["0xaaa"]
    assert aaa["status"] == "resolved"
    assert aaa["resolution_outcome"] == "down"
    assert aaa["settlement_source"] == "polymarket_official"
    assert aaa["settlement_price"] == 1.0
    # already-resolved market upgraded in place (outcome unchanged, now official)
    assert latest["0xbbb"]["resolution_outcome"] == "up"
    assert latest["0xbbb"]["settlement_source"] == "polymarket_official"
    # unsettled market stays unresolved (pending)
    assert latest["0xccc"]["status"] in ("active", "closed")
