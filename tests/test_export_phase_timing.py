"""Export worker phase timing + snapshot coalescing (2026-10-10).

Measured per-file split on the real hive (uncontended, ~180-row flush
files): scan/footer (open+footer+schema+project) ~34%, aggregate
(decode+transform kernels) ~49%, write (per-batch normalize + amortized
row-group flush) ~12% — aggregate dominates, so the fix coalesces decoded
chunks to ~coalesce_rows before one transform+yield. These tests pin:

- the log-only phase lines (scan/aggregate/write elapsed seconds),
- coalescing row-content parity vs the per-file path,
- fail-closed accounting when a coalesced batch cannot transform,
- the per-member fallback when mixed vintages cannot concat,
- the 1000MB rss cap and worker budget staying untouched.
"""

import inspect
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.export import (
    _build_worker_main,
    _phase_timed_iter,
    _stream_export_asset_dataset,
    _stream_export_trades_dataset,
)
from polymarket_collector.storage.schemas import SCHEMAS, snapshot_schema


def _snap(i: int, ts: int, cid: str = "0xaaa", mid: str = "9",
          series: str = "BTC-5m", up_bid=0.5) -> dict:
    row = {
        "ts_snapshot_utc": "2026-10-10T00:00:00.000Z", "ts_snapshot_ns": ts,
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
                row[f"{o}_{s}_level_{lvl}_price"] = 0.5
                row[f"{o}_{s}_level_{lvl}_size"] = 1.0
            for th in (1, 5, 10):
                row[f"{o}_{s}_depth_{th}c"] = 5.0
    return row


def _snap_hive(tmp_path: Path, n_files: int = 3, rows_per_file: int = 2,
               series_type=pa.string(), series_values=None) -> Path:
    base = tmp_path / "data"
    d = base / "book_snapshots_500ms" / "date=2026-10-10" / "asset=BTC"
    d.mkdir(parents=True)
    ss = snapshot_schema(10)
    fields = [pa.field(f.name, series_type if f.name == "series_id" else f.type,
                       nullable=True) for f in ss]
    variant_schema = pa.schema(fields)
    for fi in range(n_files):
        rows = []
        for r in range(rows_per_file):
            idx = fi * rows_per_file + r
            if series_values is not None:
                rows.append(_snap(idx, 1000 + idx, series=series_values[fi]))
            else:
                rows.append(_snap(idx, 1000 + idx))
        pq.write_table(pa.Table.from_pylist(rows, schema=variant_schema),
                       str(d / f"f{fi}.parquet"))
    ml = base / "markets_latest"
    ml.mkdir(parents=True)
    pq.write_table(pa.table({"condition_id": ["0xaaa"], "market_id": ["42"]}),
                   str(ml / "markets_latest.parquet"))
    return base


def _keys_of(path: Path):
    t = pq.read_table(str(path), columns=["asset", "condition_id", "ts_snapshot_ns", "up_bid"])
    return sorted(zip(t.column("asset").to_pylist(), t.column("condition_id").to_pylist(),
                      t.column("ts_snapshot_ns").to_pylist(), t.column("up_bid").to_pylist()))


def test_phase_timed_iter_passthrough_and_lines(tmp_path, capsys):
    tables = [pa.table({"a": [i, i + 1]}) for i in range(3)]
    out = list(_phase_timed_iter(iter(tables), "X/test"))
    assert len(out) == 3
    for orig, got in zip(tables, out):
        assert got is orig
        assert got.column("a").to_pylist() == orig.column("a").to_pylist()
    logged = capsys.readouterr().out
    assert "phase X/test aggregate: batches=3 rows=6 elapsed=" in logged
    assert "phase X/test write: elapsed=" in logged


def test_phase_timed_iter_empty_stream_still_logs(capsys):
    assert list(_phase_timed_iter(iter([]), "X/empty")) == []
    logged = capsys.readouterr().out
    assert "phase X/empty aggregate: batches=0 rows=0 elapsed=" in logged
    assert "phase X/empty write: elapsed=" in logged


def test_stream_snapshot_logs_all_three_phase_lines(tmp_path, capsys):
    base = _snap_hive(tmp_path)
    io: dict = {}
    tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    rows = _stream_export_asset_dataset(base, "book_snapshots_500ms", "BTC",
                                        tmp, "5m", 10, {}, io_stats=io)
    assert rows == 6
    assert io.get("files_ok") == 3 and io.get("files_failed", 0) == 0
    logged = capsys.readouterr().out
    assert "phase BTC/book_snapshots_500ms scan: 3 files elapsed=" in logged
    assert "phase BTC/book_snapshots_500ms aggregate: batches=1 rows=6 elapsed=" in logged
    assert "phase BTC/book_snapshots_500ms write: elapsed=" in logged


def test_coalescing_row_content_matches_per_file_batches(tmp_path):
    base = _snap_hive(tmp_path, n_files=5, rows_per_file=2)
    mmap = {"0xaaa": "42"}
    # per-file batches (huge threshold -> flush per generator tail only)
    out_default = tmp_path / "default.parquet.tmp"
    n_def = _stream_export_asset_dataset(
        base, "book_snapshots_500ms", "BTC", out_default, "5m", 10, mmap)
    # coalesced across files (tiny threshold -> multi-file batches)
    out_co = tmp_path / "coalesced.parquet.tmp"
    n_co = _stream_export_asset_dataset(
        base, "book_snapshots_500ms", "BTC", out_co, "5m", 10, mmap,
        coalesce_rows=3)
    assert n_def == n_co == 10
    assert _keys_of(out_default) == _keys_of(out_co)
    # dup rows across files still collapse under coalescing (first wins)
    d = base / "book_snapshots_500ms" / "date=2026-10-10" / "asset=BTC"
    pq.write_table(pa.Table.from_pylist([_snap(99, 1000)], schema=snapshot_schema(10)),
                   str(d / "dup.parquet"))
    out_dup = tmp_path / "dup.parquet.tmp"
    n_dup = _stream_export_asset_dataset(
        base, "book_snapshots_500ms", "BTC", out_dup, "5m", 10, mmap,
        coalesce_rows=4)
    assert n_dup == 10  # the duplicate key added no row


def test_coalesced_batch_transform_failure_fails_closed(tmp_path, capsys):
    # every file carries a series_id column of the WRONG type: concat
    # succeeds (same vintage), the lane filter cannot compare int64 to a
    # string -> coalesced transform fails -> ALL member files counted
    # failed (fail closed, staging must not ship these rows). Decode-level
    # files_ok still counts them (transform is deferred, so a file can be
    # both decode-ok and batch-failed); the coverage gate keys off
    # files_failed, never files_ok.
    base = _snap_hive(tmp_path, n_files=2, rows_per_file=2,
                      series_type=pa.int64(), series_values=[7, 7])
    io: dict = {}
    tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    rows = _stream_export_asset_dataset(
        base, "book_snapshots_500ms", "BTC", tmp, "5m", 10, {},
        io_stats=io, coalesce_rows=2)
    assert rows == 0
    assert io.get("files_failed") == 2
    assert "coalesced transform failed" in capsys.readouterr().out


def test_mixed_vintages_fall_back_per_member(tmp_path, capsys):
    # one wrong-typed file written FIRST so it coalesces with a good file:
    # concat of the two vintages raises -> per-member fallback (the
    # pre-2026-10-10 behavior): only the failing member is dropped and
    # counted, the good members still ship, silently like the old path.
    base = _snap_hive(tmp_path, n_files=2, rows_per_file=2)
    d = base / "book_snapshots_500ms" / "date=2026-10-10" / "asset=BTC"
    ss = snapshot_schema(10)
    bad = [pa.field(f.name, pa.int64() if f.name == "series_id" else f.type,
                    nullable=True) for f in ss]
    pq.write_table(pa.Table.from_pylist([_snap(90, 9000, series=5)],
                                        schema=pa.schema(bad)),
                   str(d / "aa_bad_first.parquet"))
    import os

    _old = (d / "aa_bad_first.parquet").stat().st_mtime - 3600
    os.utime(d / "aa_bad_first.parquet", (_old, _old))  # sorts before the good files
    io: dict = {}
    tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    rows = _stream_export_asset_dataset(
        base, "book_snapshots_500ms", "BTC", tmp, "5m", 10, {},
        io_stats=io, coalesce_rows=3)
    assert rows == 4  # the two good files ship 2 rows each; the bad one drops
    assert io.get("files_failed") == 1
    assert set(pq.read_table(str(tmp)).column("series_id").to_pylist()) == {"BTC-5m"}
    out = capsys.readouterr().out
    assert "coalesced transform failed" not in out  # fallback, not batch-drop
    assert "phase BTC/book_snapshots_500ms aggregate: batches=2 rows=4" in out


def test_decode_error_mid_file_keeps_pending_rollback(tmp_path):
    base = _snap_hive(tmp_path, n_files=3, rows_per_file=2)
    d = base / "book_snapshots_500ms" / "date=2026-10-10" / "asset=BTC"
    corrupt = d / "corrupt.parquet"
    corrupt.write_bytes(b"PAR1garbage-not-a-parquet-file")
    io: dict = {}
    tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    rows = _stream_export_asset_dataset(
        base, "book_snapshots_500ms", "BTC", tmp, "5m", 10, {},
        io_stats=io, coalesce_rows=2)
    assert rows == 6  # 3 good files ship; the corrupt one contributes nothing
    assert io.get("files_failed") == 1
    assert io.get("files_ok") == 3


def test_decode_error_keeps_pending_threshold_sane(tmp_path, capsys):
    # Regression (2026-10-10): a file that fails at decode (footer intact,
    # page data damaged — pyarrow raises on the first batch) must restore
    # the pending-row count to its file-start snapshot. Decrementing the
    # file's contribution instead could drive the count negative when a
    # mid-file flush already shipped rows, skewing every later threshold
    # check (good files would merge into one giant tail batch instead of
    # flushing per threshold). The damaged file sorts FIRST so good files
    # follow it and any skew is observable.
    base = _snap_hive(tmp_path, n_files=3, rows_per_file=2)
    d = base / "book_snapshots_500ms" / "date=2026-10-10" / "asset=BTC"
    mid = d / "a_midcorrupt.parquet"
    pq.write_table(pa.Table.from_pylist([_snap(50, 5000), _snap(51, 5100)],
                                        schema=snapshot_schema(10)),
                   str(mid), row_group_size=1)  # two row groups
    raw = bytearray(mid.read_bytes())
    # footer boundary: last 8 bytes are <uint32 footer_len> + b"PAR1" —
    # damage 32 bytes inside the data region (before the footer) so the
    # file OPENS but fails at decode.
    _foot_len = int.from_bytes(raw[-8:-4], "little")
    _data_end = len(raw) - 8 - _foot_len
    raw[_data_end - 32:_data_end] = b"\x00\xff" * 16
    mid.write_bytes(bytes(raw))
    import os

    _old = mid.stat().st_mtime - 3600
    os.utime(mid, (_old, _old))  # AFTER the damage write: sorts first

    try:
        pf = pq.ParquetFile(str(mid))
        _opens = True
        try:
            list(pf.iter_batches(batch_size=20000))
            _raises = False
        except Exception:
            _raises = True
    except Exception:
        _opens, _raises = False, False
    assert _opens and _raises, "corrupt fixture must open and fail at decode"

    io: dict = {}
    tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    rows = _stream_export_asset_dataset(
        base, "book_snapshots_500ms", "BTC", tmp, "5m", 10, {},
        io_stats=io, coalesce_rows=2)
    # the damaged file contributes nothing and is failed; each good file
    # still crosses the 2-row threshold on its own (a skewed counter would
    # merge them into one tail batch -> batches=1)
    assert rows == 6
    assert io.get("files_failed") == 1
    assert io.get("files_ok") == 3
    out = capsys.readouterr().out
    assert "phase BTC/book_snapshots_500ms aggregate: batches=3 rows=6" in out


def test_trades_build_logs_scan_aggregate_write(tmp_path, capsys):
    base = tmp_path / "data"
    d = base / "trades" / "date=2026-10-10" / "asset=BTC"
    d.mkdir(parents=True)
    ts = SCHEMAS["trades"]
    rows = [{
        "ts_source": 1790000000000 + i, "condition_id": None, "series_id": "BTC-5m",
        "asset": "BTC", "trade_id": f"t{i}", "token_id": f"tok{i}",
        "price": 0.5, "size": 2.0, "transaction_hash": None, "wallet": None,
    } for i in range(3)]
    pq.write_table(pa.Table.from_pylist(rows, schema=ts), str(d / "t1.parquet"))
    io: dict = {}
    tmp = tmp_path / "BTC_trades.parquet.tmp"
    n = _stream_export_trades_dataset(base, "BTC", tmp, "5m", io_stats=io)
    assert n == 3
    logged = capsys.readouterr().out
    assert "phase BTC/trades scan: 1 files elapsed=" in logged
    assert "phase BTC/trades aggregate: batches=" in logged
    assert "phase BTC/trades write: elapsed=" in logged


def test_guards_intact_rss_cap_budget_and_scan_line():
    src_worker = inspect.getsource(_build_worker_main)
    assert "_limit_mb: int = 1000" in src_worker  # rss cap never raised
    assert "rss-cap-abort" in src_worker
    src_snap = inspect.getsource(_stream_export_asset_dataset)
    assert "coalesce_rows: int = 20000" in src_snap
    assert "_phase_timed_iter(_gen()" in src_snap  # phase lines wrap the stream
    src_trades = inspect.getsource(_stream_export_trades_dataset)
    assert "_phase_timed_iter(_gen()" in src_trades
    assert "phase {asset_upper}/trades scan:" in src_trades
    assert "fail closed" in src_trades
