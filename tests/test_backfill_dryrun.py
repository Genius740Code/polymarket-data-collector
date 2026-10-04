"""Bounded dry-run tests for backfill_pmxt (offline fixtures, no network).

Guards the needs_backfill slow-path fix: scoped coverage must touch only
the date=/asset= partition files (bounded file-touch count) and must
return window lists identical to the legacy unscoped full-hive reference
scan on writer-conformant layouts. Dry-run writes nothing.
"""

import datetime as _dt

import pyarrow as pa
import pyarrow.parquet as pq

import polymarket_collector.backfill_pmxt as bq
from polymarket_collector.backfill_pmxt import (
    BACKFILL_SOURCE,
    _list_partition_files,
    live_condition_coverage,
    needs_backfill,
    run_backfill,
)

DAY = "2026-09-20"
DAY2 = "2026-09-21"
DAY_START_MS = int(_dt.datetime.fromisoformat(
    f"{DAY}T00:00:00+00:00").timestamp() * 1000)
DAY2_START_MS = int(_dt.datetime.fromisoformat(
    f"{DAY2}T00:00:00+00:00").timestamp() * 1000)

CID_LIVE = "0x" + "aa" * 32  # live rows -> never backfilled
CID_GAP = "0x" + "bb" * 32  # no live rows -> backfill target
CID_ETH = "0x" + "cc" * 32  # other asset, same day
CID_NEXT = "0x" + "dd" * 32  # window day with NO tick partition at all


def _markets_rows():
    return [
        {"condition_id": CID_LIVE, "slug": "btc-updown-5m-1",
         "series_id": "BTC-5m", "asset": "BTC", "window_index": 1,
         "window_size_seconds": 300, "up_token_id": "1001",
         "down_token_id": "1002",
         "market_start_ts_ms": DAY_START_MS + 300_000,
         "market_end_ts_ms": DAY_START_MS + 600_000},
        {"condition_id": CID_GAP, "slug": "btc-updown-5m-2",
         "series_id": "BTC-5m", "asset": "BTC", "window_index": 2,
         "window_size_seconds": 300, "up_token_id": "2001",
         "down_token_id": "2002",
         "market_start_ts_ms": DAY_START_MS + 600_000,
         "market_end_ts_ms": DAY_START_MS + 900_000},
        {"condition_id": CID_ETH, "slug": "eth-updown-5m-1",
         "series_id": "ETH-5m", "asset": "ETH", "window_index": 1,
         "window_size_seconds": 300, "up_token_id": "3001",
         "down_token_id": "3002",
         "market_start_ts_ms": DAY_START_MS + 300_000,
         "market_end_ts_ms": DAY_START_MS + 600_000},
        {"condition_id": CID_NEXT, "slug": "btc-updown-5m-3",
         "series_id": "BTC-5m", "asset": "BTC", "window_index": 3,
         "window_size_seconds": 300, "up_token_id": "4001",
         "down_token_id": "4002",
         "market_start_ts_ms": DAY2_START_MS + 300_000,
         "market_end_ts_ms": DAY2_START_MS + 600_000},
    ]


def _write(path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path), compression="zstd")


def _hive(tmp_path):
    data = tmp_path / "data"
    _write(data / "markets_latest" / "markets_latest.parquet",
           _markets_rows())
    btc = data / "trades" / f"date={DAY}" / "asset=BTC"
    _write(btc / "live.parquet", [
        {"condition_id": CID_LIVE, "source": "live", "price": 0.6},
        {"condition_id": CID_LIVE, "source": "live", "price": 0.61},
        {"condition_id": CID_LIVE, "source": "live", "price": 0.62},
        # source-marked row inside a live-named file still counts backfilled.
        {"condition_id": CID_LIVE, "source": BACKFILL_SOURCE, "price": 0.6},
        {"condition_id": None, "source": "live", "price": 0.5},
        {"condition_id": "", "source": "live", "price": 0.5},
    ])
    _write(btc / "backfill_pmxt-trades-x.parquet", [
        {"condition_id": CID_GAP, "source": BACKFILL_SOURCE, "price": 0.6},
        {"condition_id": CID_GAP, "source": "live", "price": 0.61},
        {"condition_id": CID_LIVE, "source": "live", "price": 0.6},
    ])
    # no source column at all -> rows count live (legacy shape).
    _write(btc / "nosource.parquet", [
        {"condition_id": CID_LIVE, "price": 0.6},
        {"condition_id": CID_LIVE, "price": 0.61},
    ])
    # non-string condition_id -> vectorized path bails to the row loop.
    _write(btc / "intcid.parquet", [
        {"condition_id": 7, "source": "live", "price": 0.5},
        {"condition_id": 7, "source": "live", "price": 0.51},
    ])
    _write(data / "trades" / f"date={DAY}" / "asset=ETH" / "live.parquet", [
        {"condition_id": CID_ETH, "source": "live", "price": 0.5},
        {"condition_id": CID_ETH, "source": "live", "price": 0.51},
    ])
    _write(data / "book_events" / f"date={DAY}" / "asset=BTC" / "live.parquet",
           [{"condition_id": CID_LIVE, "source": "live",
             "event_id": "e1"}])
    return data


def _reference_coverage(data):
    """Legacy semantics: unscoped full-hive scan + per-row counting."""
    cov = {}
    for ds in bq.TICK_DATASETS:
        root = data / ds
        if not root.exists():
            continue
        for p in sorted(root.rglob("*.parquet"), key=str):
            if p.name.endswith(".tmp"):
                continue
            t = bq._read_key_columns(p, ["condition_id", "source"])
            if t is None or t.num_rows == 0:
                continue
            is_bf = p.name.startswith(bq.BACKFILL_PREFIX)
            for r in t.to_pylist():
                cid = r.get("condition_id")
                if not cid:
                    continue
                entry = cov.setdefault(
                    str(cid), {"live": 0, "backfilled": 0})
                if is_bf or r.get("source") == BACKFILL_SOURCE:
                    entry["backfilled"] += 1
                else:
                    entry["live"] += 1
    return cov


def _reference_windows(data, day, asset, timeframe):
    cov = _reference_coverage(data)
    cid_info, _ = bq.load_markets_maps(data)
    want_lane = f"{asset.upper()}-{timeframe}" if (
        asset and timeframe) else None
    windows = []
    for cid, info in cid_info.items():
        if asset and str(info.get("asset") or "").upper() != asset.upper():
            continue
        if want_lane and info.get("series_id") not in (want_lane, None):
            continue
        win_day = bq._utc_day_str(info.get("market_start_ts_ms"))
        if day and win_day != day:
            continue
        entry = cov.get(cid, {"live": 0, "backfilled": 0})
        if entry.get("live"):
            continue
        windows.append(cid)
    return sorted(windows)


def test_scoped_coverage_matches_reference(tmp_path) -> None:
    data = _hive(tmp_path)
    got = live_condition_coverage(data, DAY, "BTC")
    # Equivalence is over candidate condition IDs: needs_backfill only
    # consults coverage for markets matching the scope, and on a
    # writer-conformant hive every in-scope row lives in the scoped
    # partition (other-asset partitions carry only other-asset rows).
    candidates = set(_reference_windows(data, DAY, "BTC", "5m"))
    candidates.add(CID_LIVE)  # covered-live markets are consulted too
    ref = _reference_coverage(data)
    assert {c: got.get(c) for c in candidates} == {
        c: ref.get(c) for c in candidates}
    assert got[CID_LIVE] == {"live": 6, "backfilled": 2}
    assert got[CID_GAP] == {"live": 0, "backfilled": 2}
    assert got["7"] == {"live": 2, "backfilled": 0}


def test_scoped_scan_touches_partition_files_only(tmp_path, monkeypatch) -> None:
    data = _hive(tmp_path)
    opened = []

    orig = bq._read_key_columns

    def counting(path, columns):
        opened.append(path)
        return orig(path, columns)

    monkeypatch.setattr(bq, "_read_key_columns", counting)
    live_condition_coverage(data, DAY, "BTC")
    scoped = set()
    for ds in bq.TICK_DATASETS:
        scoped.update(_list_partition_files(data, ds, DAY, "BTC"))
    assert opened, "expected scoped files to be opened"
    assert set(opened) == set(scoped)
    assert len(opened) == 5  # 4 trades + 1 book_events partition files


def test_empty_scope_lists_gap_with_zero_opens(tmp_path, monkeypatch) -> None:
    data = _hive(tmp_path)
    opened = []
    orig = bq._read_key_columns

    def counting(path, columns):
        opened.append(path)
        return orig(path, columns)

    monkeypatch.setattr(bq, "_read_key_columns", counting)
    # DAY2 has markets but no tick partition: bounded (no full-hive scan).
    assert _list_partition_files(data, "trades", DAY2, "BTC") == []
    needs = needs_backfill(data, DAY2, "BTC", "5m")
    assert opened == []
    cids = {w["condition_id"] for w in needs["windows"]}
    assert cids == {CID_NEXT}
    assert needs["windows"] == [
        w for w in needs["windows"]]  # shape unchanged
    # identical window list to the legacy unscoped reference scan.
    assert cids == set(_reference_windows(data, DAY2, "BTC", "5m"))


def test_window_lists_match_reference_before_after(tmp_path) -> None:
    data = _hive(tmp_path)
    for day, asset, tf in [(DAY, "BTC", "5m"), (DAY, "ETH", None),
                           (DAY2, "BTC", "5m")]:
        got = sorted(w["condition_id"]
                     for w in needs_backfill(data, day, asset, tf)["windows"])
        assert got == _reference_windows(data, day, asset, tf)


def test_dry_run_writes_nothing(tmp_path) -> None:
    data = _hive(tmp_path)
    before = sorted(str(p) for p in data.rglob("*.parquet"))
    summary = run_backfill(data, [], day=DAY, asset="BTC", timeframe="5m",
                           dry_run=True)
    assert summary["dry_run"] is True
    assert summary["writes"] == {}
    cids = {w["condition_id"] for w in summary["needs"]["windows"]}
    assert CID_GAP in cids and CID_LIVE not in cids
    assert sorted(str(p) for p in data.rglob("*.parquet")) == before
