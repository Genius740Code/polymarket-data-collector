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


def _hive_rows():
    # two files, mixed lanes, legacy uppercase sides, one other-TF row.
    # Built ONCE per test and planted into both hives so timestamps match.
    f1 = [_row(1, cid="cid-a"), _row(2, cid="cid-a", side="SELL"),
          _row(3, cid="cid-b", side="buy")]
    f2 = [_row(4, cid="cid-a"), _row(5, cid="cid-b", outcome="up", wallet="0xw")]
    other = dict(_row(6, cid="cid-a"))
    other["series_id"] = "BTC-15m"
    f2.append(other)
    return [("date=2026-09-11/asset=BTC/a.parquet", f1),
            ("date=2026-09-11/asset=BTC/b.parquet", f2)]


def _plant(base, files_rows):
    import pyarrow.parquet as pq

    d = base / "trades"
    for name, rows in files_rows:
        p = d / name
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=TRADES_SCHEMA), str(p))
    return base


def _norm(rows):
    out = []
    for r in sorted(rows, key=lambda r: str(r.get("trade_id"))):
        r = dict(r)
        if str(r.get("trade_id", "")).startswith("api-"):
            r["trade_id"] = "api-NORM"
            r["ts_received_ns"] = 0
        out.append(r)
    return out


def test_stream_matches_legacy_no_enrichment(tmp_path):
    from polymarket_collector.storage.export import (
        _read_dataset_per_asset,
        _stream_export_trades_dataset,
    )
    import pyarrow.parquet as pq

    # one shared row set (time.time() stamps) planted into both hives
    rows_once = _hive_rows()
    base1 = _plant(tmp_path / "h1", rows_once)
    base2 = _plant(tmp_path / "h2", rows_once)
    legacy = _read_dataset_per_asset(base1, "trades", "BTC", timeframe_label="5m",
                                     deadline_s=-1, reconcile=False)
    out = tmp_path / "BTC_trades.parquet"
    n = _stream_export_trades_dataset(base2, "BTC", out, "5m", deadline_s=-1)
    assert n and n > 0
    streamed = pq.read_table(str(out))
    assert _norm(legacy.to_pylist()) == _norm(streamed.to_pylist())
    assert set(streamed.schema.names) == set(legacy.schema.names)


def test_stream_matches_legacy_with_fake_api(tmp_path, monkeypatch):
    import polymarket_collector.storage.export as E
    import pyarrow.parquet as pq

    TX = "0x" + "ab" * 32
    MISSING_TX = "0x" + "cd" * 32

    def _fake(cid, taker_only=False, oldest_needed_ms=None, max_pages=60):
        if cid != "cid-a":
            return []
        if taker_only:
            return [
                {"transactionHash": TX, "price": 0.5, "size": 2.0, "side": "BUY",
                 "proxyWallet": "0xTAKER", "asset_id": "tok"},
                {"transactionHash": MISSING_TX, "price": 0.1, "size": 1.0, "side": "BUY",
                 "proxyWallet": "0xNEW", "asset_id": "tok"},
            ]
        return [
            {"transactionHash": TX, "price": 0.5, "size": 2.0, "side": "SELL",
             "proxyWallet": "0xMAKER"},
            {"transactionHash": TX, "price": 0.5, "size": 2.0, "side": "BUY",
             "proxyWallet": "0xTAKER"},
        ]

    monkeypatch.setattr(E, "_fetch_market_trades", _fake)
    rows = [_row(1, cid="cid-a", tx=TX, side="buy"),
            _row(2, cid="cid-b", tx="0x" + "ee" * 32, side="sell")]
    base1 = _plant(tmp_path / "h1", [("date=2026-09-11/asset=BTC/a.parquet", rows)])
    base2 = _plant(tmp_path / "h2", [("date=2026-09-11/asset=BTC/a.parquet", rows)])
    legacy = E._read_dataset_per_asset(base1, "trades", "BTC", timeframe_label="5m")
    out = tmp_path / "BTC_trades.parquet"
    n = E._stream_export_trades_dataset(base2, "BTC", out, "5m")
    assert n == 3  # 2 streamed + 1 api- insert
    streamed = pq.read_table(str(out))
    L = _norm(legacy.to_pylist())
    S = _norm(streamed.to_pylist())
    assert L == S
    by_id = {r["trade_id"]: r for r in S}
    assert by_id["t-1"]["taker_wallet"] == "0xTAKER"
    assert by_id["t-1"]["maker_wallet"] == "0xMAKER"
    assert by_id["t-1"]["wallet"] == "0xTAKER"
    assert by_id["api-NORM"]["transaction_hash"] == MISSING_TX
    assert by_id["api-NORM"]["taker_wallet"] == "0xNEW"


def test_stream_empty_and_cutoff(tmp_path):
    from polymarket_collector.storage.export import _stream_export_trades_dataset
    import time as _t

    base = tmp_path / "data"
    out = tmp_path / "BTC_trades.parquet"
    assert _stream_export_trades_dataset(base, "BTC", out, "5m") == 0
    assert not out.exists()
    # cutoff excludes the only file -> same as empty
    rows_once = _hive_rows()
    _plant(base, rows_once)
    io = {}
    n = _stream_export_trades_dataset(base, "BTC", out, "5m",
                                      cutoff_ts=_t.time() - 100000, io_stats=io)
    assert n == 0 and not out.exists()
