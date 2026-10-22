"""Backlog #5a — bounded flush batches: a single flush() splits an oversized
group into cap-sized serialize+write passes.

Measured context (2026-10-22, [mem] stdout telemetry on this box): live RSS
swung 484MB -> 747MB -> 490MB in 5-minute steps exactly when the writer buffer
reached ~14k rows, and buffers reached 23-33k rows ([flush] 32685 rows) with an
export tick active — each swing was ONE pq.write_table serializing the ENTIRE
group (the 842-col full-depth snapshot family, ~10KB/row on disk). This file
pins the write-batch cap:

1. wide-family batch budget: the serialize+write transient of one capped
   batch, measured on the real 842-column schema, must stay under ~64MB;
2. a 20k-row snapshot flush is SPLIT — no write pass exceeds the cap, the
   flush still returns the TOTAL row count, and nothing is dropped;
3. a narrow group above its cap still completes — two slices totaling the
   buffer, no rows lost.

Fixtures are canned rows in the REAL schemas (real column names and key
shapes, prices in 0..1, hex ids). Every row is distinct so dedup never fires
and row counts prove nothing was dropped.
"""
import datetime as _dt
import os

import pyarrow.parquet as pq

from polymarket_collector.storage.parquet_writer import ParquetWriter
from polymarket_collector.storage.schemas import SCHEMAS, snapshot_schema

CID = "0x" + "ab" * 32
UP_TOKEN = "0x" + "01" * 32
DOWN_TOKEN = "0x" + "02" * 32
DATE = "2026-10-22"
NS0 = int(_dt.datetime(2026, 10, 22, 12, tzinfo=_dt.timezone.utc).timestamp()) * 1_000_000_000
BATCH_BUDGET_MB = 64.0  # #5a budget for ONE serialize+write pass (widest family)


def _iso(ns):
    return _dt.datetime.fromtimestamp(ns / 1e9, tz=_dt.timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def _rss_mb():
    with open("/proc/self/statm") as fh:
        return int(fh.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1e6


def _writer(tmp_path, **kw):
    return ParquetWriter(
        data_dir=tmp_path / "data",
        wal_enabled=False,
        flush_interval_seconds=10 ** 9,
        flush_row_count_threshold=10 ** 9,
        buffer_max_rows=200_000,
        l2_levels=10,
        **kw,
    )


def _recording_write_table(monkeypatch):
    """Record table.num_rows of every pq.write_table pass (the serialize+write
    boundary the cap bounds). The writer module looks the attribute up at call
    time, so patching the module object instruments the real write path."""
    passes = []
    real = pq.write_table

    def _rec(table, where, **kwargs):
        passes.append(int(table.num_rows))
        return real(table, where, **kwargs)

    monkeypatch.setattr(pq, "write_table", _rec)
    return passes


def _part_files(data_dir, dataset):
    return sorted((data_dir / dataset).rglob("*.parquet"))


def _on_disk_rows(files):
    return sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files)


def _full_depth_row():
    """One canned row carrying every column of the REAL 842-col full-depth
    snapshot schema (prices 0..1, 100 levels). Level values are shared
    objects; the per-row footprint that matters for the batch transient (the
    842-key dict) is real."""
    assert len(snapshot_schema(100).names) == 842  # the measured live width
    row = {}
    for i, name in enumerate(snapshot_schema(100).names):
        if name.endswith("_price"):
            row[name] = round(0.90 - (i % 97) * 0.001, 6)
        elif name.endswith("_size"):
            row[name] = float(10 + (i % 89))
        else:
            row[name] = None
    row.update({
        "condition_id": CID, "market_id": "12345", "series_id": "BTC-5m",
        "window_index": 7, "asset": "BTC", "snapshot_id": "sid-0",
        "up_token_id": UP_TOKEN, "down_token_id": DOWN_TOKEN,
        "up_bid": 0.90, "up_ask": 0.92, "up_bid_size": 10.0, "up_ask_size": 5.0,
        "down_bid": 0.08, "down_ask": 0.10, "down_bid_size": 3.0, "down_ask_size": 4.0,
        "is_rollover_window": False, "book_state": "fresh", "resync_id": None,
        "book_crossed": False, "market_time_remaining_ms": 240_000,
        "up_book_age_ms": 12, "down_book_age_ms": 12,
        "up_book_hash": "0x" + "aa" * 32, "down_book_hash": "0x" + "bb" * 32,
        "underlying_price": 60000.0, "underlying_ts_ns": NS0,
        "underlying_age_ms": 500, "source": "live",
    })
    return row


def _wide_rows(tpl, n, tag):
    """n distinct full-width rows: 500ms-grid bucket steps (real dedup-key
    spacing) so every row is unique."""
    rows = []
    for i in range(n):
        ns = NS0 + i * 500_000_000
        r = dict(tpl)
        r["ts_snapshot_ns"] = ns
        r["ts_snapshot_utc"] = _iso(ns)
        r["ts_source"] = ns // 1_000_000
        r["ts_received_ns"] = ns
        r["snapshot_id"] = f"{tag}-{i}"
        rows.append(r)
    return rows


def _snapshot_row_template():
    """Canned row covering the REAL 10-level snapshot schema (the cap family,
    default width) — same field shapes as book.OrderBookState flat rows."""
    row = {}
    for i, name in enumerate(snapshot_schema(10).names):
        if name.endswith("_price"):
            row[name] = round(0.90 - (i % 7) * 0.001, 6)
        elif name.endswith("_size"):
            row[name] = float(10 + (i % 9))
        else:
            row[name] = None
    row.update({
        "ts_snapshot_utc": _iso(NS0), "ts_snapshot_ns": NS0,
        "condition_id": CID, "market_id": "12345", "series_id": "BTC-5m",
        "window_index": 7, "asset": "BTC", "snapshot_id": "sid-0",
        "up_token_id": UP_TOKEN, "down_token_id": DOWN_TOKEN,
        "up_bid": 0.90, "up_ask": 0.92, "up_bid_size": 10.0, "up_ask_size": 5.0,
        "down_bid": 0.08, "down_ask": 0.10, "down_bid_size": 3.0, "down_ask_size": 4.0,
        "is_rollover_window": False, "book_state": "fresh", "resync_id": None,
        "book_crossed": False, "market_time_remaining_ms": 240_000,
        "up_book_age_ms": 12, "down_book_age_ms": 12,
        "up_book_hash": "0x" + "aa" * 32, "down_book_hash": "0x" + "bb" * 32,
        "underlying_price": 60000.0, "underlying_ts_ns": NS0,
        "underlying_age_ms": 500, "source": "live",
        "ts_source": NS0 // 1_000_000, "ts_received_ns": NS0,
    })
    return row


def _trades_row_template():
    """Canned row covering the REAL trades schema (narrow width)."""
    row = {name: None for name in SCHEMAS["trades"].names}
    row.update({
        "ts_source": NS0 // 1_000_000, "ts_received_ns": NS0, "source": "live",
        "condition_id": CID, "market_id": "12345", "series_id": "BTC-5m",
        "window_index": 7, "asset": "BTC", "trade_id": "tid-0",
        "transaction_hash": "0x" + "cd" * 32, "token_id": UP_TOKEN,
        "outcome": "up", "price": 0.5, "size": 2.0, "side": "buy",
        "aggressor_side": "buy", "quote_book_state": "live",
        "maker_wallet": "0x" + "ee" * 20, "taker_wallet": "0x" + "ef" * 20,
        "wallet": "0x" + "ef" * 20,
        "up_bid": 0.49, "up_ask": 0.51, "up_bid_size": 10.0, "up_ask_size": 5.0,
        "down_bid": 0.49, "down_ask": 0.51, "down_bid_size": 10.0, "down_ask_size": 5.0,
    })
    return row


def test_wide_family_batch_peak_budget(tmp_path):
    """The widest family (book_snapshots_500ms, 842-col full-depth schema)
    must keep ONE serialize+write pass under ~64MB: measure the real per-row
    transient of a full-width _write_group pass and project it to the cap."""
    wide_cap = ParquetWriter._WRITE_BATCH_ROW_CAPS["book_snapshots_500ms"]
    assert wide_cap > 0
    w = _writer(tmp_path, l2_full=True)
    tpl = _full_depth_row()
    # warmup pass absorbs one-time first-touch (writer code paths, arena
    # growth) so the measured pass reflects steady per-row cost
    w._write_group("book_snapshots_500ms", DATE, "BTC", _wide_rows(tpl, 50, "warm"))
    batch = _wide_rows(tpl, 600, "meas")
    rss0 = _rss_mb()
    w._write_group("book_snapshots_500ms", DATE, "BTC", batch)
    rss1 = _rss_mb()
    per_row_kb = (rss1 - rss0) * 1e3 / 600
    assert per_row_kb > 0, "no transient measured — RSS delta was <= 0"
    projected_mb = per_row_kb * wide_cap / 1e3
    assert projected_mb < BATCH_BUDGET_MB, (
        f"cap {wide_cap} rows x measured {per_row_kb:.1f}KB/row projects "
        f"{projected_mb:.1f}MB per serialize+write pass (budget {BATCH_BUDGET_MB:.0f}MB)"
    )
    w.close()


def test_snapshot_20k_flush_split_under_cap(tmp_path, monkeypatch):
    """A 20k-row snapshot buffer flushed by ONE flush() call: the write is
    SPLIT so no pass exceeds the cap, the return value stays the TOTAL row
    count (the [flush] report contract), and every row lands on disk."""
    wide_cap = ParquetWriter._WRITE_BATCH_ROW_CAPS["book_snapshots_500ms"]
    assert wide_cap < 20_000, "cap must bind for a 20k-row snapshot buffer"
    w = _writer(tmp_path)  # default l2_levels=10 -> real 122-col snapshot schema
    passes = _recording_write_table(monkeypatch)
    tpl = _snapshot_row_template()
    n_rows = 20_000
    for i in range(n_rows):
        ns = NS0 + i * 500_000_000
        row = dict(tpl)
        row["ts_snapshot_ns"] = ns
        row["ts_snapshot_utc"] = _iso(ns)
        row["ts_source"] = ns // 1_000_000
        row["ts_received_ns"] = ns
        row["snapshot_id"] = f"sid-{i}"
        assert w.append("book_snapshots_500ms", row, asset="BTC") is True
    assert len(w._buffer) == n_rows, "every append buffered (no auto-flush fired)"
    assert passes == [], "nothing serialized before flush()"
    total = w.flush()
    assert total == n_rows, "flush() returns the TOTAL row count across slices"
    assert len(w._buffer) == 0 and len(w._l2_buffer) == 0, "buffer emptied"
    assert len(passes) >= 2, f"single flush() must be SPLIT, saw passes={passes}"
    assert max(passes) <= wide_cap, f"a pass exceeded the cap: {passes}"
    assert sum(passes) == n_rows, "slices total the buffer exactly"
    files = _part_files(tmp_path / "data", "book_snapshots_500ms")
    assert files, "expected sliced part files"
    assert _on_disk_rows(files) == n_rows, "on-disk row count matches the buffer"
    ts_seen = set()
    for f in files:
        ts_seen.update(
            pq.read_table(str(f), columns=["ts_snapshot_ns"]).column(0).to_pylist()
        )
    assert len(ts_seen) == n_rows, "no row dropped, no row duplicated"
    w.close()


def test_narrow_trades_above_cap_two_slices(tmp_path, monkeypatch):
    """A narrow group above the default cap still completes: the buffer is
    written in exactly two slices totaling the buffer — no rows lost."""
    default_cap = ParquetWriter.MAX_ROWS_PER_WRITE_BATCH
    assert default_cap > 0
    n_rows = default_cap + 5_000  # above the cap -> two slices
    w = _writer(tmp_path)
    passes = _recording_write_table(monkeypatch)
    tpl = _trades_row_template()
    for i in range(n_rows):
        row = dict(tpl)
        row["trade_id"] = f"tid-{i}"
        assert w.append("trades", row, asset="BTC") is True
    assert len(w._buffer) == n_rows
    total = w.flush()
    assert total == n_rows, "flush() returns the TOTAL row count across slices"
    assert passes == [default_cap, n_rows - default_cap], (
        f"expected exactly two capped slices, saw passes={passes}"
    )
    assert len(w._buffer) == 0
    files = _part_files(tmp_path / "data", "trades")
    assert _on_disk_rows(files) == n_rows, "on-disk row count matches the buffer"
    ids_seen = set()
    for f in files:
        ids_seen.update(
            pq.read_table(str(f), columns=["trade_id"]).column(0).to_pylist()
        )
    assert len(ids_seen) == n_rows, "no row dropped, no row duplicated"
    w.close()
