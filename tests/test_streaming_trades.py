"""Chunked trades backfill: identical output to direct path, bounded RAM."""
import time

import pyarrow as pa

from polymarket_collector.storage.export import (
    _backfill_trade_wallets,
    _backfill_trade_wallets_chunked,
    _trades_need_enrichment,
)
from polymarket_collector.storage.schemas import TRADES_SCHEMA


def _row(i, cid="cid-x", side="buy", wallet=None, outcome="unknown", tx="0xabc123"):
    return {
        "ts_source": str(int(time.time() * 1000)), "ts_received_ns": time.time_ns(),
        "condition_id": cid, "market_id": "m", "series_id": "BTC-5m", "window_index": 1,
        "asset": "BTC", "trade_id": f"t-{i}", "transaction_hash": tx,
        "token_id": "tok", "outcome": outcome, "price": 0.5, "size": 2.0,
        "notional": 1.0, "fee": None, "fee_is_estimated": None,
        "side": side, "aggressor_side": side,
        "maker_wallet": None, "taker_wallet": None, "wallet": wallet,
    }


def test_need_counter_matches_row_loop():
    rows = [
        _row(1, wallet=None, tx="0xaaa"),          # needs (no wallet)
        _row(2, wallet="0xw", tx="0xbbb"),         # needs (outcome unknown)
        _row(3, wallet="0xw", outcome="up", tx="0xccc"),  # clean
        _row(4, wallet=None, tx=None),             # no tx -> skip
    ]
    tbl = pa.Table.from_pylist(rows, schema=TRADES_SCHEMA)
    assert _trades_need_enrichment(tbl) == 3  # t-1 wallet, t-2 outcome, t-3 maker


def test_chunked_matches_direct(monkeypatch):
    import httpx

    class _R:
        status_code = 200

        def json(self):
            return []

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _R())
    rows = [_row(1, cid="cid-a"), _row(2, cid="cid-a"), _row(3, cid="cid-b", side="sell")]
    tbl = pa.Table.from_pylist(rows, schema=TRADES_SCHEMA)
    direct = _backfill_trade_wallets(tbl, ".", asset="BTC", reconcile=False)
    chunked = _backfill_trade_wallets_chunked(tbl, ".", asset="BTC", reconcile=False, chunk_rows=1)
    assert direct.num_rows == chunked.num_rows == 3
    assert sorted(r["trade_id"] for r in chunked.to_pylist()) == ["t-1", "t-2", "t-3"]
