"""Streaming export parity: streamed staging must equal legacy Table path row-for-row."""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.export import (
    _load_market_id_map,
    _read_dataset_per_asset,
    _stream_export_asset_dataset,
)
from polymarket_collector.storage.schemas import snapshot_schema

HEX = "0x" + "cd" * 32


def _snap(i, ts, cid="0xaaa", mid="9", series="BTC-5m", up_bid=0.5):
    base = {
        "ts_snapshot_utc": "2026-09-10T00:00:00.000Z", "ts_snapshot_ns": ts,
        "condition_id": cid, "market_id": mid, "series_id": series,
        "window_index": 1, "asset": "BTC", "snapshot_id": f"s{i}",
        "up_token_id": "u", "down_token_id": "d",
        "up_bid": up_bid, "up_ask": 0.6, "up_bid_size": 1.0, "up_ask_size": 1.0,
        "down_bid": 0.4, "down_ask": 0.5, "down_bid_size": 1.0, "down_ask_size": 1.0,
        "market_time_remaining_ms": 100, "up_book_age_ms": 5, "down_book_age_ms": 5,
        "is_rollover_window": False, "book_state": "live", "resync_id": None,
        "book_crossed": False, "up_book_hash": None, "down_book_hash": None,
    }
    for o in ("up", "down"):
        for s in ("bid", "ask"):
            for lvl in range(1, 11):
                base[f"{o}_{s}_level_{lvl}_price"] = 0.5
                base[f"{o}_{s}_level_{lvl}_size"] = 1.0
            for th in (1, 5, 10):
                base[f"{o}_{s}_depth_{th}c"] = 5.0
    return base


def _hive(tmp_path: Path):
    base = tmp_path / "data"
    d = base / "book_snapshots_500ms" / "date=2026-09-10" / "asset=BTC"
    d.mkdir(parents=True)
    ss = snapshot_schema(10)
    # file 1: rows ts 300,100 (unsorted), one hex market_id, one dup (ts 100 twice)
    pq.write_table(pa.Table.from_pylist([
        _snap(1, 300), _snap(2, 100), _snap(3, 100), _snap(4, 200, mid=HEX),
    ], schema=ss), str(d / "a.parquet"))
    # file 2: other-TF row (must be filtered), null-BBO row
    pq.write_table(pa.Table.from_pylist([
        _snap(5, 150, series="BTC-15m"), _snap(6, 250, up_bid=None),
    ], schema=ss), str(d / "b.parquet"))
    ml = base / "markets_latest"
    ml.mkdir(parents=True)
    pq.write_table(pa.table({"condition_id": ["0xaaa"], "market_id": ["42"]}), str(ml / "markets_latest.parquet"))
    return base


def _keymap(rows):
    return sorted((r["ts_snapshot_ns"], r["snapshot_id"], r["market_id"], r["up_bid"]) for r in rows)


def test_stream_matches_table_path(tmp_path):
    base = _hive(tmp_path)
    legacy = _read_dataset_per_asset(base, "book_snapshots_500ms", "BTC", timeframe_label="5m")
    assert legacy is not None and legacy.num_rows > 0
    out = tmp_path / "BTC_book_snapshots_500ms.parquet"
    tmp = tmp_path / "tmp.parquet.tmp"
    mmap = _load_market_id_map(base)
    assert mmap == {"0xaaa": "42"}
    n = _stream_export_asset_dataset(base, "book_snapshots_500ms", "BTC", tmp, "5m", 10, mmap)
    import os

    os.replace(str(tmp), str(out))
    streamed = pq.read_table(out)
    assert n == streamed.num_rows == legacy.num_rows
    assert _keymap(streamed.to_pylist()) == _keymap(legacy.to_pylist())
    # specifics: dup ts=100 collapsed to one, hex healed to 42, 15m filtered
    assert streamed.num_rows == 4  # 300,100,200,250(null bbo)
    assert {r["market_id"] for r in streamed.to_pylist()} == {"9", "42"}
