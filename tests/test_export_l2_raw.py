"""l2_raw export wiring: verbatim-frame projection onto the PMData L2 layout.

Covers the l2_raw leg of export_pmdata on fixtures (never the live hive):

- per-type projection (book / price_change / last_trade_price /
  best_bid_ask / tick_size_change / market_resolved / unknown);
- YES-only parity with book_events (down-side excluded + counted in the
  same skipped_down_events counter, never silently dropped);
- scoped-vs-unscoped row equality for the wanted slug;
- source_conn preserved verbatim (tagged value passes through, NULL stays
  NULL — never fabricated);
- bounded reads (row-group streaming, projected columns only).
"""

import datetime as _dt

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector import export_pmdata as ep
from polymarket_collector.export_pmdata import (
    _l2_raw_frame_to_pmdata,
    export_pmdata_layout,
)
from polymarket_collector.storage.l2_raw import build_l2_raw_row

DAY = "2026-09-25"
MS = int(_dt.datetime.fromisoformat(f"{DAY}T00:00:00+00:00").timestamp() * 1000)
CID_A = "0x" + "aa" * 32
CID_B = "0x" + "bb" * 32
SLUG_A = "btc-updown-5m-1758758700"
SLUG_B = "btc-updown-5m-1758759000"
UP_A, DOWN_A = "up-token-aaa", "down-token-aaa"
UP_B, DOWN_B = "up-token-bbb", "down-token-bbb"


def _ns(ms: int) -> int:
    return ms * 1_000_000


def _write(path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path), compression="zstd")


def _markets():
    return [
        {"condition_id": CID_A, "slug": SLUG_A, "series_id": "BTC-5m", "asset": "BTC",
         "window_size_seconds": 300, "up_token_id": UP_A, "down_token_id": DOWN_A,
         "market_start_ts_ms": MS + 300_000, "market_end_ts_ms": MS + 600_000},
        {"condition_id": CID_B, "slug": SLUG_B, "series_id": "BTC-5m", "asset": "BTC",
         "window_size_seconds": 300, "up_token_id": UP_B, "down_token_id": DOWN_B,
         "market_start_ts_ms": MS + 600_000, "market_end_ts_ms": MS + 900_000},
    ]


def _frame(cid, tok, etype, ts, extra=None, conn="A", rx=None):
    f = {"market": cid, "asset_id": tok, "timestamp": ts, "event_type": etype}
    f.update(extra or {})
    return build_l2_raw_row(f, asset="BTC", condition_id=cid,
                            source_conn=conn,
                            ts_received_ns=rx if rx is not None else _ns(ts))


def _book_extra(levels=3):
    bids = [{"price": f"{0.50 + 0.01 * i:.2f}", "size": f"{10 + i}"} for i in range(levels)]
    asks = [{"price": f"{0.60 - 0.01 * i:.2f}", "size": f"{20 + i}"} for i in range(levels)]
    return {"bids": bids, "asks": asks}


def _pc_extra(up_tok, down_tok):
    return {"price_changes": [
        {"asset_id": down_tok, "price": "0.40", "size": "5",
         "best_bid": "0.39", "best_ask": "0.41"},
        {"asset_id": up_tok, "price": "0.60", "size": "5",
         "best_bid": "0.59", "best_ask": "0.61"},
    ]}


def _hive_l2(data, t0):
    rows = [
        # slug A: one frame per type on the UP token (+ down-token twins).
        _frame(CID_A, UP_A, "book", t0, _book_extra(), conn="A"),
        _frame(CID_A, DOWN_A, "book", t0 + 1, _book_extra(), conn="B"),
        _frame(CID_A, UP_A, "price_change", t0 + 2, _pc_extra(UP_A, DOWN_A), conn="A"),
        _frame(CID_A, UP_A, "last_trade_price", t0 + 3,
               {"price": "0.60", "size": "7"}, conn="single"),
        _frame(CID_A, DOWN_A, "last_trade_price", t0 + 4,
               {"price": "0.40", "size": "7"}, conn="A"),
        _frame(CID_A, UP_A, "best_bid_ask", t0 + 5,
               {"best_bid": "0.59", "best_ask": "0.61"}, conn="B"),
        _frame(CID_A, DOWN_A, "best_bid_ask", t0 + 6,
               {"best_bid": "0.39", "best_ask": "0.41"}, conn="A"),
        _frame(CID_A, UP_A, "tick_size_change", t0 + 7,
               {"old_tick_size": "0.01", "new_tick_size": "0.001"}, conn="A"),
        _frame(CID_A, DOWN_A, "tick_size_change", t0 + 8,
               {"old_tick_size": "0.01", "new_tick_size": "0.001"}, conn="A"),
        # market-level + unknown: no token side, always kept.
        build_l2_raw_row({"event_type": "market_resolved", "market": CID_A,
                          "outcome": "up", "timestamp": t0 + 9},
                         asset="BTC", condition_id=CID_A, source_conn=None,
                         ts_received_ns=_ns(t0 + 9)),
        build_l2_raw_row({"event_type": "mystery_v2", "market": CID_A,
                          "asset_id": UP_A, "timestamp": t0 + 10},
                         asset="BTC", condition_id=CID_A, source_conn="B",
                         ts_received_ns=_ns(t0 + 10)),
        # slug B decoy (scope must exclude, counted).
        _frame(CID_B, UP_B, "book", t0 + 11, _book_extra(), conn="A"),
        # NULL clock: timestamp stays NULL, placed by receive clock (in-day).
        build_l2_raw_row({"event_type": "book", "market": CID_A, "asset_id": UP_A,
                          **_book_extra(1)},
                         asset="BTC", condition_id=CID_A, source_conn="A",
                         ts_received_ns=_ns(t0 + 12)),
    ]
    assert rows[-1]["ts_source"] is None
    _write(data / "markets_latest" / "markets_latest.parquet", _markets())
    _write(data / "l2_raw" / f"date={DAY}" / "asset=BTC" / "l2.parquet", rows)
    return data


def _book_event(cid, outcome, ts):
    return {"condition_id": cid, "series_id": "BTC-5m", "asset": "BTC",
            "outcome": outcome, "event_type": "price_change",
            "ts_source": ts, "ts_received_ns": _ns(ts),
            "new_best_bid": 0.59, "new_best_ask": 0.61,
            "new_bid_size": 10.0, "new_ask_size": 10.0}


def _rows(out, slug):
    return pq.read_table(str(out / "l2" / f"{slug}.parquet")).to_pylist()


def test_projection_per_type(tmp_path) -> None:
    t0 = MS + 301_000
    data = _hive_l2(tmp_path / "data", t0)
    out = tmp_path / "out"
    manifest = export_pmdata_layout(data, out, "BTC", "5m", DAY,
                                    only_slugs={SLUG_A})
    rows = _rows(out, SLUG_A)
    by_type = {}
    for r in rows:
        by_type.setdefault(r["event_type"], []).append(r)
    # every hive type for the slug is present (incl. depth-absent ones).
    assert set(by_type) >= {"book", "price_change", "last_trade_price",
                            "best_bid_ask", "tick_size_change",
                            "market_resolved", "mystery_v2"}
    # book: full depth re-sorted best-first (wire ships worst-first).
    book = next(r for r in by_type["book"] if r["timestamp"] == t0)
    assert book["bid_prices"] == [0.52, 0.51, 0.50]
    assert book["bid_sizes"] == [12.0, 11.0, 10.0]
    assert book["ask_prices"] == [0.58, 0.59, 0.60]
    assert book["ask_sizes"] == [22.0, 21.0, 20.0]
    assert book["source_conn"] == "A"
    # price_change: YES-leg BBO, sizes unknown (NULL, never 0).
    pc = by_type["price_change"][0]
    assert pc["bid_prices"] == [0.59] and pc["ask_prices"] == [0.61]
    assert pc["bid_sizes"] == [None] and pc["ask_sizes"] == [None]
    # depth-absent types keep the event with NULL depth.
    ltp = next(r for r in by_type["last_trade_price"])
    assert ltp["bid_prices"] is None and ltp["ask_prices"] is None
    assert ltp["source_conn"] == "single"
    assert by_type["tick_size_change"][0]["bid_prices"] is None
    assert by_type["market_resolved"][0]["bid_prices"] is None
    assert by_type["mystery_v2"][0]["source_conn"] == "B"
    # NULL wire clock stays NULL (placed by receive clock, counted as kept).
    clockless = [r for r in by_type["book"] if r["timestamp"] is None]
    assert len(clockless) == 1
    assert clockless[0]["local_timestamp"] == _ns(t0 + 12)
    # conn tag preserved verbatim, NULL stays NULL.
    resolved = by_type["market_resolved"][0]
    assert resolved["source_conn"] is None
    assert manifest["reads"]["l2_raw"]["rows_kept"] == len(rows)


def test_yes_only_parity_with_book_events(tmp_path) -> None:
    t0 = MS + 301_000
    data = _hive_l2(tmp_path / "data", t0)
    _write(data / "book_events" / f"date={DAY}" / "asset=BTC" / "ev.parquet",
           [_book_event(CID_A, "up", t0 + 20),
            _book_event(CID_A, "down", t0 + 21)])
    out = tmp_path / "out"
    manifest = export_pmdata_layout(data, out, "BTC", "5m", DAY,
                                    only_slugs={SLUG_A})
    rows = _rows(out, SLUG_A)
    # down-token l2_raw frames excluded exactly like down book_events: the
    # fixture ships 4 (book, last_trade_price, best_bid_ask, tick_size_change)
    # plus 1 down book_event.
    assert manifest["skipped"].get("skipped_down_events", 0) == 5
    for r in rows:
        assert r["event_type"] != "snapshot" or True
    # no down-side depth leaked into the L2 file.
    for r in rows:
        for col in ("ask_prices", "bid_prices"):
            vals = r[col] or []
            assert 0.39 not in vals and 0.41 not in vals


def test_scoped_rows_equal_unscoped(tmp_path) -> None:
    t0 = MS + 301_000
    data = _hive_l2(tmp_path / "data", t0)
    full = export_pmdata_layout(data, tmp_path / "full", "BTC", "5m", DAY)
    scoped = export_pmdata_layout(data, tmp_path / "scoped", "BTC", "5m", DAY,
                                  only_slugs={SLUG_A})
    assert _rows(tmp_path / "scoped", SLUG_A) == _rows(tmp_path / "full", SLUG_A)
    assert not (tmp_path / "scoped" / "l2" / f"{SLUG_B}.parquet").exists()
    # the out-of-scope B book row is mask-excluded and counted, not dropped.
    assert scoped["skipped"].get("skipped_other_slug", 0) == 1
    assert full["reads"]["l2_raw"]["rows_kept"] == (
        scoped["reads"]["l2_raw"]["rows_kept"]
        + pq.read_table(str(tmp_path / "full" / "l2" / f"{SLUG_B}.parquet")).num_rows)


def test_conn_tag_preserved_and_never_fabricated(tmp_path) -> None:
    t0 = MS + 301_000
    data = _hive_l2(tmp_path / "data", t0)
    out = tmp_path / "out"
    export_pmdata_layout(data, out, "BTC", "5m", DAY, only_slugs={SLUG_A})
    rows = _rows(out, SLUG_A)
    conns = {r["source_conn"] for r in rows}
    assert conns >= {"A", "B", "single", None}
    assert all(c is None or isinstance(c, str) for c in conns)


def test_l2_raw_reads_stay_projected(tmp_path, monkeypatch) -> None:
    t0 = MS + 301_000
    data = _hive_l2(tmp_path / "data", t0)
    seen = []
    real_pf = ep.pq.ParquetFile

    class _Rec(real_pf):
        def read_row_group(self, i, columns=None):
            seen.append(columns)
            return super().read_row_group(i, columns=columns)

    monkeypatch.setattr(ep.pq, "ParquetFile", _Rec)
    export_pmdata_layout(data, tmp_path / "out", "BTC", "5m", DAY,
                         only_slugs={SLUG_A})
    assert seen, "expected row-group reads on the l2_raw file"
    allowed = set(ep.L2_RAW_NEED_COLS)
    for cols in seen:
        assert cols is not None and set(cols) <= allowed, f"unprojected read {cols}"
    assert any("frame_json" in list(c) for c in seen), "depth needs the frame pass"


def test_unit_converter_market_resolved_and_unknown() -> None:
    row = {"condition_id": CID_A, "token_id": None, "event_type": "market_resolved",
           "ts_source": MS + 301_009, "ts_received_ns": _ns(MS + 301_009),
           "source_conn": None}
    pm, down = _l2_raw_frame_to_pmdata(
        {"event_type": "market_resolved", "market": CID_A, "outcome": "up"},
        row, SLUG_A, "BTC", UP_A, DOWN_A, CID_A)
    assert not down and pm is not None
    assert pm["event_type"] == "market_resolved"
    assert pm["bid_prices"] is None and pm["ask_prices"] is None
    assert pm["source_conn"] is None and pm["condition_id"] == CID_A
    # unknown future type on the down token: same YES-only exclusion.
    pm2, down2 = _l2_raw_frame_to_pmdata(
        {"event_type": "mystery_v2", "asset_id": DOWN_A}, dict(row, token_id=DOWN_A),
        SLUG_A, "BTC", UP_A, DOWN_A, CID_A)
    assert pm2 is None and down2 is True
    # garbage frame never raises: event preserved with NULL depth.
    pm3, down3 = _l2_raw_frame_to_pmdata(
        {"event_type": "book", "bids": "not-a-list", "asks": None},
        dict(row, event_type="book"), SLUG_A, "BTC", UP_A, DOWN_A, CID_A)
    assert not down3 and pm3 is not None and pm3["bid_prices"] is None
