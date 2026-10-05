"""Bounded PMData export: scoped-vs-unscoped row equality + read bounds.

The live hive cannot be scanned wholesale (143k-row x 842-col snapshot
files OOM small boxes; ~23k parquet files hive-wide). The export therefore
stays bounded: partition-pruned date=/asset= files + the single
markets_latest file, column-projected to the converter inputs, with an
optional per-slug scope. These tests pin that contract on fixtures:

- scoped (only_slugs) per-slug parquet rows equal the unscoped run's rows
  for the same slug (identical results, smaller work);
- the scoped run never touches files outside the pruned set (decoy garbage
  files elsewhere in the hive are never read).
"""

import datetime as _dt

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector import export_pmdata as ep
from polymarket_collector.export_pmdata import export_pmdata_layout

DAY = "2026-09-25"
MS = int(_dt.datetime.fromisoformat(f"{DAY}T00:00:00+00:00").timestamp() * 1000)
CID_A = "0x" + "aa" * 32
CID_B = "0x" + "bb" * 32
SLUG_A = "btc-updown-5m-1758758700"
SLUG_B = "btc-updown-5m-1758759000"


def _ns(ms: int) -> int:
    return ms * 1_000_000


def _write(path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path), compression="zstd")


def _markets():
    return [
        {"condition_id": CID_A, "slug": SLUG_A, "series_id": "BTC-5m", "asset": "BTC",
         "window_size_seconds": 300, "up_token_id": "1001", "down_token_id": "1002",
         "market_start_ts_ms": MS + 300_000, "market_end_ts_ms": MS + 600_000},
        {"condition_id": CID_B, "slug": SLUG_B, "series_id": "BTC-5m", "asset": "BTC",
         "window_size_seconds": 300, "up_token_id": "2001", "down_token_id": "2002",
         "market_start_ts_ms": MS + 600_000, "market_end_ts_ms": MS + 900_000},
    ]


def _snaps(cid, t0):
    return {"condition_id": cid, "series_id": "BTC-5m", "asset": "BTC",
            "ts_snapshot_ns": _ns(t0), "ts_snapshot_utc": "2026-09-25T00:05:01.000Z",
            "up_ask_level_1_price": 0.65, "up_ask_level_1_size": 10.0,
            "up_bid_level_1_price": 0.60, "up_bid_level_1_size": 10.0}


def _event(cid, t0, i):
    return {"condition_id": cid, "series_id": "BTC-5m", "asset": "BTC", "outcome": "up",
            "event_type": "price_change", "ts_source": t0 + i, "ts_received_ns": _ns(t0 + i),
            "new_best_bid": 0.61, "new_best_ask": 0.64,
            "new_bid_size": 10.0, "new_ask_size": 10.0}


def _trade(cid, t0, tid):
    return {"condition_id": cid, "series_id": "BTC-5m", "asset": "BTC", "outcome": "up",
            "token_id": "1001", "trade_id": tid, "transaction_hash": "0x" + tid,
            "price": 0.62, "size": 1.0, "fee": None, "side": "buy",
            "ts_source": t0, "ts_received_ns": _ns(t0)}


def _hive(data, *, with_decoys=False):
    _write(data / "markets_latest" / "markets_latest.parquet", _markets())
    t0 = MS + 301_000
    part = f"date={DAY}/asset=BTC"
    _write(data / "book_snapshots_500ms" / part / "snap.parquet",
           [_snaps(CID_A, t0), _snaps(CID_B, t0 + 500)])
    _write(data / "book_events" / part / "ev.parquet",
           [_event(CID_A, t0, 1), _event(CID_B, t0, 2)])
    _write(data / "trades" / part / "tr.parquet",
           [_trade(CID_A, t0, "t1"), _trade(CID_B, t0, "t2")])
    _write(data / "chainlink_events" / part / "cl.parquet",
           [{"asset": "BTC", "event_id": "c1", "price": 110000.0,
             "ts_source": t0, "ts_received_ns": _ns(t0)}])
    if with_decoys:
        # A full-hive rglob fallback would trip over these; the pruned run
        # must never touch them.
        other = data / "book_snapshots_500ms" / "date=2026-09-26/asset=BTC"
        other.mkdir(parents=True, exist_ok=True)
        (other / "decoy.parquet").write_bytes(b"not a parquet file")
        flat = data / "book_events" / "flat-decoy.parquet"
        flat.parent.mkdir(parents=True, exist_ok=True)
        flat.write_bytes(b"not a parquet file either")
        ml = data / "markets_log" / "date=2026-09-25"
        ml.mkdir(parents=True, exist_ok=True)
        (ml / "decoy.parquet").write_bytes(b"markets_log must stay unread")
    return data


def _rows(out, kind, slug):
    return pq.read_table(str(out / kind / f"{slug}.parquet")).to_pylist()


def test_scoped_rows_equal_unscoped(tmp_path) -> None:
    data = _hive(tmp_path / "data")
    full = export_pmdata_layout(data, tmp_path / "full", "BTC", "5m", DAY)
    scoped = export_pmdata_layout(data, tmp_path / "scoped", "BTC", "5m", DAY,
                                  only_slugs={SLUG_A})
    assert full["totals"]["l2_rows"] == 4 and full["totals"]["trades_rows"] == 2
    for kind in ("l2", "trades"):
        assert _rows(tmp_path / "scoped", kind, SLUG_A) == _rows(tmp_path / "full", kind, SLUG_A)
    # the other slug's files are absent from the scoped slice, counted honestly.
    assert not (tmp_path / "scoped" / "l2" / f"{SLUG_B}.parquet").exists()
    assert not (tmp_path / "scoped" / "trades" / f"{SLUG_B}.parquet").exists()
    assert scoped["skipped"].get("skipped_other_slug", 0) == 3  # 1 snap + 1 event + 1 trade
    assert scoped["totals"]["l2_rows"] == 2 and scoped["totals"]["trades_rows"] == 1
    assert "full" not in str(scoped["files"])  # manifest paths are relative


def test_scoped_reads_stay_pruned_and_projected(tmp_path, monkeypatch) -> None:
    data = _hive(tmp_path / "data", with_decoys=True)
    seen = []

    real_read = ep.read_table

    def spy(path, columns=None):
        seen.append((str(path), columns))
        return real_read(path, columns=columns)

    monkeypatch.setattr(ep, "read_table", spy)
    manifest = export_pmdata_layout(data, tmp_path / "out", "BTC", "5m", DAY,
                                     only_slugs={SLUG_A})
    # every hive read landed inside the pruned partition set (or markets_latest);
    # decoy garbage was never opened.
    for path, _cols in seen:
        assert "decoy" not in path, f"unbounded read touched {path}"
        assert "markets_log" not in path, f"markets_log fallback hit {path}"
    pruned = {f"date={DAY}/asset=BTC", "markets_latest"}
    assert seen, "expected at least the markets + partition reads"
    assert all(any(tag in p for tag in pruned) for p, _ in seen)
    # projection active on every dataset read (None only for markets_latest).
    by_file = {p: c for p, c in seen}
    for p, cols in by_file.items():
        if "markets_latest" in p:
            continue
        assert cols, f"unprojected full-column read of {p}"
    # files_ok counts prove only the pruned files were opened.
    for ds in ("book_snapshots_500ms", "book_events", "trades", "chainlink_events"):
        assert manifest["reads"][ds]["files_ok"] == 1, ds
        assert manifest["reads"][ds]["files_failed"] == 0, ds
