"""H5-ZERO + H5-NAME + H5-MAKER regression guards (wallet/trades enrichment).

- H5-ZERO: markets with ZERO local trades rows but real Data-API fills (CLOB
  stream churn / missed subscribe) get their fills reconciled as api- rows —
  staging only, honest timestamps (ts_received_ns NULL, ts_backfilled_ns =
  event time), fee NULL (no exchange-reported rate for a 0-trade market),
  never into the live path, never overwriting live rows, never resurrecting
  windows outside the local span (pruned history stays pruned).
- H5-NAME: second_pass_enrich_trades ran with two NameErrors (`_narrow_tbls`,
  `_read_dataset_per_asset_files` — neither defined anywhere) and died at the
  first asset every run, so enrichment round 2 never healed wallet NULLs.
- H5-MAKER: the api- insert maker attribution is side-aware — side=BUY →
  maker on the SELL leg, side=SELL → maker on the BUY leg. The old code
  always used the SELL pool, so SELL-side reconciled rows self-attributed
  the taker's own wallet as maker_wallet.

Deterministic: temp dirs only, no network (httpx/_fetch_market_trades mocked).
"""
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import polymarket_collector.storage.export as E
from polymarket_collector.storage.export import (
    _backfill_trade_wallets_chunked,
    _reconcile_zero_trade_markets,
    _stream_export_trades_dataset,
    second_pass_enrich_trades,
)
from polymarket_collector.storage.schemas import TRADES_SCHEMA

T0_MS = 1_700_000_000_000  # fixed epoch-ms (deterministic)
T0_S = T0_MS // 1000


def _row(i, cid="cid-a", side="buy", wallet=None, outcome="up", tx="0xabc123",
         ts_ms=T0_MS, ts_received_ns=None):
    return {
        "ts_source": ts_ms, "ts_received_ns": ts_received_ns,
        "ts_received_ns_estimated": None, "source": "live", "ts_backfilled_ns": None,
        "condition_id": cid, "market_id": "m", "series_id": "BTC-5m", "window_index": 1,
        "asset": "BTC", "trade_id": f"t-{i}", "transaction_hash": tx,
        "token_id": "tok", "outcome": outcome, "price": 0.5, "size": 2.0,
        "notional": 1.0, "fee": None, "fee_is_estimated": None,
        "side": side, "aggressor_side": side,
        "maker_wallet": None, "taker_wallet": None, "wallet": wallet,
    }


def _plant(base, files_rows):
    d = base / "trades"
    for name, rows in files_rows:
        p = d / name
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=TRADES_SCHEMA), str(p))
    return base


def _plant_markets_latest(base, rows):
    p = base / "markets_latest" / "markets_latest.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(p))
    return base


def _market_row(cid, series="BTC-5m", start_ms=T0_MS - 60_000, end_ms=T0_MS + 60_000,
                asset="BTC"):
    return {
        "condition_id": cid, "market_id": f"mid-{cid}", "asset": asset,
        "series_id": series, "window_index": 42, "window_size_seconds": 300,
        "market_start_ts_ms": start_ms, "market_end_ts_ms": end_ms,
    }


def _api_fill(tx, price, size, side, wallet, outcome="Up", ts=T0_S):
    return {"transactionHash": tx, "price": price, "size": size, "side": side,
            "proxyWallet": wallet, "outcome": outcome, "timestamp": ts,
            "asset_id": "tok"}


# ---------------------------------------------------------------- H5-ZERO


def test_zero_trade_market_reconciled_into_staging(tmp_path, monkeypatch):
    """A cid with ZERO local rows but real Data-API fills is reconciled into
    the staging build as api- rows with honest timestamps; live rows are
    untouched and a rebuild is idempotent (same deterministic trade_ids)."""
    TX1 = "0x" + "11" * 32
    TX2 = "0x" + "22" * 32

    def _fake(cid, taker_only=False, oldest_needed_ms=None, max_pages=60):
        if cid != "cid-b":
            return []
        if taker_only:
            return [
                _api_fill(TX1, 0.5, 2, "BUY", "0xTAKER1"),
                _api_fill(TX2, 0.6, 3, "SELL", "0xTAKER2"),
            ]
        # both legs: BUY fill → taker BUY leg + maker SELL leg;
        # SELL fill → taker SELL leg + maker BUY leg.
        return [
            _api_fill(TX1, 0.5, 2, "BUY", "0xTAKER1"),
            _api_fill(TX1, 0.5, 2, "SELL", "0xMAKER1"),
            _api_fill(TX2, 0.6, 3, "SELL", "0xTAKER2"),
            _api_fill(TX2, 0.6, 3, "BUY", "0xMAKER2"),
        ]

    monkeypatch.setattr(E, "_fetch_market_trades", _fake)
    base = _plant(tmp_path / "data", [("date=2026-09-25/asset=BTC/a.parquet",
                                       [_row(1, cid="cid-a")])])
    _plant_markets_latest(base, [_market_row("cid-a"), _market_row("cid-b")])
    out = tmp_path / "BTC_trades.parquet"
    n = _stream_export_trades_dataset(base, "BTC", out, "5m")
    assert n == 3  # 1 streamed live + 2 api- inserts for the 0-row market
    streamed = pq.read_table(str(out)).to_pylist()
    live = [r for r in streamed if r.get("source") == "live"]
    api = [r for r in streamed if r.get("source") == "api_reconciled"]
    assert len(live) == 1 and live[0]["trade_id"] == "t-1"
    assert live[0]["condition_id"] == "cid-a"  # live rows untouched
    assert len(api) == 2
    by_id = {r["transaction_hash"]: r for r in api}
    b1, b2 = by_id[TX1], by_id[TX2]
    for r in (b1, b2):
        # honest reconciled timestamps: NULL receive clock, event-time backfill
        assert r["ts_received_ns"] is None
        assert r["ts_received_ns_estimated"] is None
        assert r["ts_backfilled_ns"] == T0_MS * 1_000_000
        assert r["ts_source"] == T0_S * 1000
        assert r["condition_id"] == "cid-b"
        # authoritative lane context from markets_latest — no sentinels
        assert r["series_id"] == "BTC-5m"
        assert r["window_index"] == T0_S // 300
        assert r["market_id"] == "mid-cid-b"
        assert r["asset"] == "BTC"
        # a 0-trade market has no streamed rows → no exchange-reported rate
        # → fee stays NULL (never fabricated, never cross-market)
        assert r["fee"] is None
        assert r["fee_is_estimated"] is None
    # wallets: taker from the API, maker SIDE-AWARE (H5-MAKER)
    assert b1["side"] == "buy" and b1["taker_wallet"] == "0xTAKER1"
    assert b1["maker_wallet"] == "0xMAKER1"
    assert b1["wallet"] == "0xTAKER1"
    assert b2["side"] == "sell" and b2["taker_wallet"] == "0xTAKER2"
    assert b2["maker_wallet"] == "0xMAKER2"  # old code: self-attributed 0xTAKER2
    assert b2["wallet"] == "0xTAKER2"
    # idempotent rebuild: same deterministic trade_ids, never duplicated
    n2 = _stream_export_trades_dataset(base, "BTC", out2 := tmp_path / "BTC_trades2.parquet", "5m")
    assert n2 == 3
    api2 = [r for r in pq.read_table(str(out2)).to_pylist() if r.get("source") == "api_reconciled"]
    assert sorted(r["trade_id"] for r in api2) == sorted(r["trade_id"] for r in api)


def test_zero_trade_sell_row_maker_from_buy_pool(tmp_path, monkeypatch):
    """H5-MAKER: for a SELL-side reconciled row the maker comes from the BUY
    leg pool — the SELL pool holds the taker's OWN leg, so using it
    self-attributed the taker's wallet as maker_wallet."""
    TX = "0x" + "33" * 32

    def _fake(cid, taker_only=False, oldest_needed_ms=None, max_pages=60):
        if cid != "cid-b":
            return []
        if taker_only:
            return [_api_fill(TX, 0.6, 3, "SELL", "0xTAKER")]
        return [
            _api_fill(TX, 0.6, 3, "SELL", "0xTAKER"),
            _api_fill(TX, 0.6, 3, "BUY", "0xMAKER"),
        ]

    monkeypatch.setattr(E, "_fetch_market_trades", _fake)
    base = tmp_path / "data"
    _plant_markets_latest(base, [_market_row("cid-b")])
    ins = list(_reconcile_zero_trade_markets(
        base, "BTC", known_cids=set(), span_lo_ms=T0_MS, span_hi_ms=T0_MS,
        series_want="BTC-5m"))
    assert len(ins) == 1
    rows = ins[0].to_pylist()
    assert len(rows) == 1
    r = rows[0]
    assert r["side"] == "sell"
    assert r["taker_wallet"] == "0xTAKER"
    assert r["maker_wallet"] == "0xMAKER"
    assert r["maker_wallet"] != r["taker_wallet"]


def test_zero_trade_outside_span_skipped(tmp_path, monkeypatch):
    """Windows entirely outside the local span are not holes in the shipped
    data — never reconciled (pruned history must not resurrect), and the
    Data-API is never queried for them."""
    calls = []

    def _fake(cid, taker_only=False, oldest_needed_ms=None, max_pages=60):
        calls.append(cid)
        return [_api_fill("0x" + "44" * 32, 0.5, 2, "BUY", "0xT")]

    monkeypatch.setattr(E, "_fetch_market_trades", _fake)
    base = tmp_path / "data"
    _plant_markets_latest(base, [
        _market_row("cid-old", start_ms=T0_MS - 10_000_000, end_ms=T0_MS - 9_000_000),
        _market_row("cid-future", start_ms=T0_MS + 9_000_000, end_ms=T0_MS + 10_000_000),
    ])
    ins = list(_reconcile_zero_trade_markets(
        base, "BTC", known_cids=set(), span_lo_ms=T0_MS, span_hi_ms=T0_MS,
        series_want="BTC-5m"))
    assert ins == []
    assert calls == []


def test_zero_trade_no_span_skipped(tmp_path, monkeypatch):
    """No local span (empty hive) → no candidates, no Data-API calls."""
    calls = []

    def _fake(cid, taker_only=False, oldest_needed_ms=None, max_pages=60):
        calls.append(cid)
        return []

    monkeypatch.setattr(E, "_fetch_market_trades", _fake)
    base = tmp_path / "data"
    _plant_markets_latest(base, [_market_row("cid-b")])
    ins = list(_reconcile_zero_trade_markets(
        base, "BTC", known_cids=set(), span_lo_ms=None, span_hi_ms=None,
        series_want="BTC-5m"))
    assert ins == [] and calls == []


def test_zero_trade_known_cid_skipped(tmp_path, monkeypatch):
    """A cid that already has local rows is never reconciled here (the
    have-set paths own it) — no duplicate inserts."""
    calls = []

    def _fake(cid, taker_only=False, oldest_needed_ms=None, max_pages=60):
        calls.append(cid)
        return []

    monkeypatch.setattr(E, "_fetch_market_trades", _fake)
    base = tmp_path / "data"
    _plant_markets_latest(base, [_market_row("cid-a")])
    ins = list(_reconcile_zero_trade_markets(
        base, "BTC", known_cids={"cid-a"}, span_lo_ms=T0_MS, span_hi_ms=T0_MS,
        series_want="BTC-5m"))
    assert ins == [] and calls == []


def test_chunked_zero_trade_reconcile_lane_match(tmp_path, monkeypatch):
    """The chunked-path hook appends api- rows for 0-row markets matching the
    lane series only — a 15m market's fills can never leak into the 5m lane
    staging build (the lane filter ran before the hook)."""
    TX = "0x" + "55" * 32

    def _fake(cid, taker_only=False, oldest_needed_ms=None, max_pages=60):
        if cid not in ("cid-b", "cid-c"):
            return []
        return [_api_fill(f"0x{cid[-1]}" * 32 or TX, 0.5, 2, "BUY", f"0xT-{cid}")]

    monkeypatch.setattr(E, "_fetch_market_trades", _fake)
    rows = [_row(1, cid="cid-a")]
    base = _plant(tmp_path / "data", [("date=2026-09-25/asset=BTC/a.parquet", rows)])
    _plant_markets_latest(base, [
        _market_row("cid-a"), _market_row("cid-b", series="BTC-5m"),
        _market_row("cid-c", series="BTC-15m"),
    ])
    out = _backfill_trade_wallets_chunked(
        pa.Table.from_pylist(rows, schema=TRADES_SCHEMA), base, asset="BTC",
        reconcile=True, timeframe_label="5m", chunk_rows=1)
    cids = set(r["condition_id"] for r in out.to_pylist())
    assert "cid-b" in cids  # 0-row 5m market reconciled into the lane
    assert "cid-c" not in cids  # 15m market never leaks into the 5m lane
    assert "cid-a" in cids  # live rows kept
    api_b = [r for r in out.to_pylist() if r.get("condition_id") == "cid-b"]
    assert all(r["source"] == "api_reconciled" and r["ts_received_ns"] is None
               for r in api_b)


# ---------------------------------------------------------------- H5-NAME


def test_second_pass_enrichment_runs_and_fills(tmp_path, monkeypatch):
    """H5-NAME: second_pass_enrich_trades no longer dies with NameError
    (`_narrow_tbls` / `_read_dataset_per_asset_files` were undefined) — it
    runs, and fills still-NULL wallets from the Data-API (NULLs only)."""
    import httpx

    class _R:
        status_code = 200

        def json(self):
            # both legs of one fill: SELL = maker, BUY = taker
            return [
                {"transactionHash": "0xabc123", "price": 0.5, "size": 2.0,
                 "side": "SELL", "proxyWallet": "0xmaker"},
                {"transactionHash": "0xabc123", "price": 0.5, "size": 2.0,
                 "side": "BUY", "proxyWallet": "0xtaker"},
            ]

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _R())
    captured = {}

    def _fake_wb(data_dir, asset, enriched):
        captured["rows"] = enriched.to_pylist()
        return 0

    monkeypatch.setattr(E, "_writeback_enriched_trades", _fake_wb)
    # ts_received_ns is OLD (>900s) so the freshness guard does not defer
    base = _plant(tmp_path / "data", [("date=2026-09-25/asset=BTC/a.parquet",
                                       [_row(1, cid="cid-x", outcome="up",
                                             ts_received_ns=1_700_000_000_000_000_000)])])
    stats = second_pass_enrich_trades(base, assets=["BTC"])
    assert stats["deferred"] is False
    assert stats["rows_needed"] == 1
    assert stats["assets_scanned"] == 1
    rows = captured.get("rows") or []
    assert len(rows) == 1
    r = rows[0]
    assert r["taker_wallet"] == "0xtaker"
    assert r["maker_wallet"] == "0xmaker"
    assert r["wallet"] == "0xtaker"
    # nothing fabricated: non-NULL values are never overwritten
    assert r["price"] == 0.5 and r["size"] == 2.0


def test_second_pass_defers_on_fresh_data(tmp_path, monkeypatch):
    """Fresh local data (<900s) → the pass defers (data-api coverage not yet
    healed) without touching anything."""
    import httpx

    calls = []

    class _R:
        status_code = 200

        def json(self):
            calls.append(1)
            return []

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _R())
    base = _plant(tmp_path / "data", [("date=2026-09-25/asset=BTC/a.parquet",
                                       [_row(1, cid="cid-x",
                                             ts_received_ns=time.time_ns())])])
    stats = second_pass_enrich_trades(base, assets=["BTC"])
    assert stats["deferred"] is True
    assert stats["rows_needed"] == 0
    assert calls == []
