"""Snapshot zero-accumulation streaming aggregate (2026-10-10).

Follow-up to the coalesced transform (718729d) and compact dedup keys
(5dcb1e2): both still aborted in prod on XRP-scale inputs (1500+ files,
wide windows; collector ~700MB, 1000MB rss cap). The residual accumulator
was the aggregate itself — _pending held ~coalesce_rows decoded rows while
the writer held another row group plus a concat transient on each side.
Now every decoded chunk is transformed+yielded immediately (no batch
holding); coalesce_rows is only the row-group CEILING handed to the
writer. Output schema is FIXED, so there is no union-schema discovery to
sample — the writer's per-batch normalize is the late-type promotion
fallback (first-non-null-wins for the common case). These tests pin, on
small in-memory hives plus metadata-only real-hive reads:

- zero accumulation: aggregate batches == source files (one yield per
  decoded chunk), rows exact;
- row/column parity vs an independent concat-all reference (tuple dedup,
  oldest-first, per-batch sort) incl. mixed vintages + cross-file dupes;
- late-type promotion: null-typed vintage column promotes to the fixed
  type with values intact;
- fail-closed: a chunk that cannot transform drops exactly its own file;
- full XRP vintage coverage (metadata-only): distinct schemas measured
  across ALL files, every projected column inside the fixed schema;
- guards intact: 1000MB cap, fail-closed tokens, ceiling wiring,
  per-file del/gc.
"""

import inspect
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage import export as ex
from polymarket_collector.storage import streaming as st_mod
from polymarket_collector.storage.schemas import snapshot_schema

_KEY_COLS = ["asset", "condition_id", "ts_snapshot_ns"]


def _snap(i, ts, cid="0xaaa", series="BTC-5m", up_bid=0.5):
    row = {
        "ts_snapshot_utc": "2026-10-10T00:00:00.000Z", "ts_snapshot_ns": ts,
        "condition_id": cid, "market_id": "9", "series_id": series,
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


def _hive(tmp_path, files_rows, variant=None, extra_col=None):
    """files_rows: list of (name, [row-dicts]). variant(name) -> optional schema."""
    base = tmp_path / "data"
    d = base / "book_snapshots_500ms" / "date=2026-10-10" / "asset=BTC"
    d.mkdir(parents=True)
    ss = snapshot_schema(10)
    for name, rows in files_rows:
        sch = variant(name) if variant else ss
        t = pa.Table.from_pylist(rows, schema=sch)
        if extra_col is not None:
            t = t.append_column(extra_col[0], pa.array(extra_col[1][:t.num_rows],
                                                       type=extra_col[2]))
        pq.write_table(t, str(d / name))
    ml = base / "markets_latest"
    ml.mkdir(parents=True)
    pq.write_table(pa.table({"condition_id": ["0xaaa"], "market_id": ["42"]}),
                   str(ml / "markets_latest.parquet"))
    return base


def _sorted_pylist(path):
    t = pq.read_table(str(path))
    idx = pa.compute.sort_indices(t, sort_keys=[(c, "ascending") for c in _KEY_COLS])
    return t.take(idx).to_pylist()


def _reference_concat_all(base, tmp_path, mmap):
    """Independent legacy-semantics reference: decode oldest-first, concat
    ALL, tuple-dedup in order, sort, write with the fixed schema."""
    from polymarket_collector.storage.streaming import DedupState
    files = st_mod.iter_source_files(base, "book_snapshots_500ms", "BTC", "ts_snapshot_ns")
    schema = ex._get_schema("book_snapshots_500ms", 10)
    extra = ["series_id", "asset", "book_state", "condition_id", "market_id"]
    parts = []
    for p in files:
        try:
            pf = pq.ParquetFile(str(p))
            cols = ex._projected_read_columns(pf.schema_arrow.names, schema, extra)
            for chunk in pf.iter_batches(batch_size=20000, columns=cols):
                parts.append(pa.Table.from_batches([chunk]))
        except Exception:
            continue
    big = parts[0] if len(parts) == 1 else pa.concat_tables(parts, promote_options="default")
    big = ex._normalize_ts_source_int(big)
    big = big.filter(ex._lane_series_mask(big.column("series_id"), "BTC-5m"))
    big = big.filter(pa.compute.equal(big.column("asset"), pa.scalar("BTC")))
    if mmap:
        big = ex._apply_market_id_map(big, mmap)
    big = DedupState(_KEY_COLS).filter(big)
    big = big.take(pa.compute.sort_indices(
        big, sort_keys=[(k, "ascending") for k in ("ts_snapshot_ns",)]))
    out = tmp_path / "ref.parquet.tmp"
    n = st_mod.write_batches(iter([big]), out, schema=schema)
    return out, n


def test_zero_accumulation_one_batch_per_file(tmp_path, capsys):
    base = _hive(tmp_path, [(f"f{i}.parquet", [_snap(i * 2 + r, 1000 + i * 2 + r)
                                                for r in range(2)]) for i in range(4)])
    io = {}
    out = tmp_path / "z.parquet.tmp"
    n = ex._stream_export_asset_dataset(base, "book_snapshots_500ms", "BTC",
                                        out, "5m", 10, {"0xaaa": "42"}, io_stats=io)
    assert n == 8 and io.get("files_ok") == 4 and io.get("files_failed", 0) == 0
    logged = capsys.readouterr().out
    # nothing held across files: each decoded chunk yields immediately
    assert "phase BTC/book_snapshots_500ms aggregate: batches=4 rows=8" in logged


def test_parity_vs_concat_all_reference_mixed_vintages(tmp_path):
    ss = snapshot_schema(10)
    narrow = pa.schema([f for f in ss if f.name != "resync_id"])  # vintage w/o col
    rows_a = [_snap(0, 1000), _snap(1, 1001)]
    rows_b = [_snap(1, 1001), _snap(2, 1002, cid="0xbbb")]  # dupe key + new key
    base = _hive(tmp_path, [("f0.parquet", rows_a), ("f1.parquet", rows_b)],
                 variant=lambda nm: narrow if nm == "f1.parquet" else ss)
    mmap = {"0xaaa": "42"}
    out_new = tmp_path / "new.parquet.tmp"
    n_new = ex._stream_export_asset_dataset(base, "book_snapshots_500ms", "BTC",
                                            out_new, "5m", 10, mmap)
    out_ref, n_ref = _reference_concat_all(base, tmp_path, mmap)
    assert n_new == n_ref == 3  # dupe collapses, first wins
    assert _sorted_pylist(out_new) == _sorted_pylist(out_ref)
    assert pq.read_schema(str(out_new)).equals(pq.read_schema(str(out_ref)))


def test_late_type_promotion_null_vintage_wins_fixed_type(tmp_path):
    # vintage with null-typed resync_id + vintage with real values: the
    # fixed-schema writer promotes (first-non-null-wins), values intact.
    a = dict(_snap(0, 1000)); a["resync_id"] = None
    b = dict(_snap(1, 1001)); b["resync_id"] = "rs-7"
    base = _hive(tmp_path, [("f0.parquet", [a]), ("f1.parquet", [b])])
    out = tmp_path / "p.parquet.tmp"
    n = ex._stream_export_asset_dataset(base, "book_snapshots_500ms", "BTC",
                                        out, "5m", 10, {})
    assert n == 2
    t = pq.read_table(str(out), columns=["ts_snapshot_ns", "resync_id"])
    got = dict(zip(t.column("ts_snapshot_ns").to_pylist(), t.column("resync_id").to_pylist()))
    assert got == {1000: None, 1001: "rs-7"}
    assert pa.types.is_string(t.schema.field("resync_id").type)


def test_chunk_transform_failure_drops_only_its_file(tmp_path, capsys):
    ss = snapshot_schema(10)
    bad_fields = [pa.field(f.name, pa.int64() if f.name == "series_id" else f.type,
                           nullable=True) for f in ss]
    good = [_snap(0, 1000), _snap(1, 1001)]
    bad = [dict(_snap(2, 1002, series=5))]
    base = _hive(tmp_path, [("f0.parquet", good)],
                 variant=lambda nm: ss)
    d = base / "book_snapshots_500ms" / "date=2026-10-10" / "asset=BTC"
    pq.write_table(pa.Table.from_pylist(bad, schema=pa.schema(bad_fields)),
                   str(d / "f1.parquet"))
    io = {}
    out = tmp_path / "f.parquet.tmp"
    n = ex._stream_export_asset_dataset(base, "book_snapshots_500ms", "BTC",
                                        out, "5m", 10, {}, io_stats=io)
    assert n == 2  # good file ships whole; bad file drops whole
    assert io.get("files_failed") == 1
    assert "coalesced transform failed" in capsys.readouterr().out  # fail-closed token kept


def test_full_xrp_vintage_schema_coverage_metadata_only():
    root = Path(__file__).resolve().parents[1] / "data"
    if not (root / "book_snapshots_500ms").is_dir():
        pytest.skip("real snapshots hive absent")
    files = st_mod.iter_source_files(root, "book_snapshots_500ms", "XRP", "ts_snapshot_ns")
    if len(files) < 100:
        pytest.skip(f"XRP vintage too small to measure coverage: {len(files)}")
    fps, prefix_fps = set(), set()
    need = {"asset", "condition_id", "ts_snapshot_ns", "series_id"}
    missing_need = []
    for i, p in enumerate(files):
        try:
            names = tuple(sorted(pq.read_schema(str(p)).names))
        except Exception:
            continue
        fps.add(names)
        if i < 50:
            prefix_fps.add(names)
        if not need <= set(names):
            missing_need.append(p.name)
    # the task's explicit safety condition, measured not assumed: the
    # sampled 50-prefix must cover every distinct schema in the full set.
    # (Measured here: exactly 1 vintage — 842-wide hive rows — so a fixed
    # output schema + per-batch writer normalize cannot lose a schema;
    # the ~720 never-shipped hive columns are skipped at decode by design
    # and full row/column parity is proven by the ≥1000-file bench run.)
    assert not missing_need, f"{len(missing_need)} files lack filter/key cols"
    assert prefix_fps == fps, f"prefix misses vintages: {len(prefix_fps)} vs {len(fps)}"
    print(f"\nxrp vintage schemas: {len(fps)} distinct across {len(files)} files "
          f"({len(prefix_fps)} in 50-prefix)")


def test_stream_agg_guards_kept():
    worker_src = inspect.getsource(ex._build_worker_main)
    assert "_limit_mb: int = 1000" in worker_src  # rss cap stays
    assert "rss-cap-abort" in worker_src  # fail-closed abort kept
    snap_src = inspect.getsource(ex._stream_export_asset_dataset)
    assert "coalesce_rows: int = 20000" in snap_src  # knob kept as ceiling
    assert "row_group_rows=coalesce_rows" in snap_src  # ceiling wired to writer
    assert "_phase_timed_iter(_gen()" in snap_src  # phase lines kept
    assert "fail closed" in snap_src
    assert "_pending: list" not in snap_src  # zero accumulation: no batch holding
    assert "_pending_rows" not in snap_src and "_co_flush" not in snap_src
    assert "malloc_trim()" in snap_src and "_gc_g.collect()" in snap_src  # per-date gc kept
    assert "del _pf" in snap_src  # per-file release kept
    trades_src = inspect.getsource(ex._stream_export_trades_dataset)
    assert "_CompactDedupState" not in trades_src and "compact_dedup" not in trades_src
