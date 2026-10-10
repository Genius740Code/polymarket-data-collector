"""Snapshot-worker memory path: compact dedup peak fix (2026-10-10).

Worker phase lines are lost on rss-cap-abort (the watcher calls os._exit),
so the peak was reproduced locally on the real XRP hive (1126 files, 1.45M
rows read, 721K 1h-lane rows): base ~110MB + per-batch Arrow transient
spikes ~130MB (independent of coalesce_rows — a 2000-row run peaked higher
than 20000, so coalescing stands absolved and keeps its 67ms/file win) +
DedupState.seen ~215MB growing linearly (~300B/key: per-row Python tuples
holding 66-char condition_id hex strings). The seen set is the only
unbounded term and the peak driver. The fix stores 16-byte key digests
(~57B/key all-in) with identical first-wins behavior; projected peak at 2x
rows stays <600MB against the untouched 1000MB cap. These tests pin, on
real hive files only:

- digest equality matching tuple equality on real keys,
- filter first-wins parity vs streaming.DedupState on real batches,
- per-key bytes bounded (~5x under the tuple path),
- staging-build row parity compact-vs-legacy on a real subset,
- guards intact: 1000MB cap, fail-closed, phase-line format, trades path.
"""

import inspect
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage import export as ex
from polymarket_collector.storage import streaming as st_mod
from polymarket_collector.storage.streaming import DedupState as _TupleDedup

_KEY_COLS = ["asset", "condition_id", "ts_snapshot_ns"]


def _real_snap_files(asset="XRP", limit=60):
    base = Path(__file__).resolve().parents[1] / "data" / "book_snapshots_500ms"
    if not base.is_dir():
        pytest.skip("real snapshots hive absent")
    files = sorted(
        (p for p in base.rglob("*.parquet")
         if not p.name.endswith(".tmp") and f"asset={asset}" in str(p)),
        key=lambda p: p.stat().st_size,
    )
    files = [p for p in files if p.stat().st_size > 0][:limit]
    if len(files) < 10:
        pytest.skip("too few real hive files")
    return files


def _read_keys(files, cols=_KEY_COLS, cap_rows=60000):
    rows = []
    for p in files:
        try:
            sch = pq.read_schema(str(p))
            t = pq.read_table(str(p), columns=[c for c in cols if c in sch.names])
        except Exception:
            continue
        if t.num_rows == 0 or any(c not in t.schema.names for c in cols):
            continue
        rows.extend(zip(*(t.column(c).to_pylist() for c in cols)))
        if len(rows) >= cap_rows:
            break
    if not rows:
        pytest.skip("no key rows in real subset")
    return rows


def test_compact_digest_matches_tuple_equality_on_real_keys() -> None:
    rows = _read_keys(_real_snap_files(), cap_rows=400)
    assert len(rows) >= 50
    digests = [ex._compact_key_digest(tuple(r)) for r in rows]
    assert all(isinstance(d, bytes) and len(d) == 16 for d in digests)
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            assert (digests[i] == digests[j]) == (tuple(rows[i]) == tuple(rows[j]))


def test_compact_filter_first_wins_matches_legacy_on_real_batches() -> None:
    files = _real_snap_files(limit=12)
    tables = []
    for p in files[:6]:
        try:
            sch = pq.read_schema(str(p))
            t = pq.read_table(str(p), columns=[c for c in _KEY_COLS if c in sch.names])
        except Exception:
            continue
        if t.num_rows and all(c in t.schema.names for c in _KEY_COLS):
            tables.append(t)
    assert tables, "no usable real batches"
    # slow path with cross-batch duplicates: feed every batch twice
    for cls in (ex._CompactDedupState, _TupleDedup):
        st = cls(_KEY_COLS)
        kept_first, kept_second = [], []
        for t in tables:
            kept_first.append(st.filter(t))
        for t in tables:
            kept_second.append(st.filter(t))
        total = sum(t.num_rows for t in tables)
        assert st.kept + st.dupes == 2 * total
        assert sum(t.num_rows for t in kept_second) == 0  # all repeats dropped
        if cls is ex._CompactDedupState:
            c_rows = sum(t.num_rows for t in kept_first)
        else:
            l_rows = sum(t.num_rows for t in kept_first)
    assert c_rows == l_rows
    # identical kept key sets: legacy tuples re-digested must equal compact seen
    lc, cc = _TupleDedup(_KEY_COLS), ex._CompactDedupState(_KEY_COLS)
    for t in tables:
        lc.filter(t)
        cc.filter(t)
    assert cc.seen == {ex._compact_key_digest(k) for k in lc.seen}
    assert cc.kept == lc.kept and cc.dupes == lc.dupes


def test_compact_seen_bytes_per_key_bounded() -> None:
    rows = _read_keys(_real_snap_files(), cap_rows=40000)
    lc, cc = _TupleDedup(_KEY_COLS), ex._CompactDedupState(_KEY_COLS)
    t = pa.table({c: [r[i] for r in rows] for i, c in enumerate(_KEY_COLS)})
    lc.filter(t)
    cc.filter(t)
    assert len(cc.seen) == len(lc.seen) > 1000
    # fixed-width entries: table overhead is shared, entry contents carry
    # the win (~49B/digest vs ~250B/tuple contents — scale-free comparison)
    legacy_contents = sum(
        sys.getsizeof(k) + sum(sys.getsizeof(x) for x in k) for k in lc.seen)
    compact_contents = sum(sys.getsizeof(d) for d in cc.seen)
    assert {len(d) for d in cc.seen} == {16}
    assert compact_contents * 4 < legacy_contents


def test_compact_passthrough_empty_and_missing_cols() -> None:
    st = ex._CompactDedupState(_KEY_COLS)
    empty = pa.table({c: pa.array([], type=pa.string() if c != "ts_snapshot_ns" else pa.int64())
                      for c in _KEY_COLS})
    assert st.filter(empty).num_rows == 0 and st.kept == 0 and st.dupes == 0
    narrow = pa.table({"asset": ["XRP"], "condition_id": ["0xabc"]})
    assert st.filter(narrow).num_rows == 1 and st.kept == 0  # missing key col: untouched


def _symlink_subset(tmp_path: Path, files):
    hv = tmp_path / "hv"
    for p in files:
        try:
            rel = p.relative_to(p.parents[3])
        except Exception:
            continue
        dst = hv / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            dst.symlink_to(p)
    return hv


def _sorted_rows(path: Path):
    t = pq.read_table(str(path))
    idx = pa.compute.sort_indices(t, sort_keys=[(c, "ascending") for c in _KEY_COLS
                                                if c in t.schema.names])
    return t.take(idx).to_pylist()


def test_snapshot_build_parity_compact_vs_legacy_real_subset(tmp_path) -> None:
    files = _real_snap_files(limit=40)
    hv = _symlink_subset(tmp_path, files)
    out_c = tmp_path / "compact.parquet"
    out_l = tmp_path / "legacy.parquet"
    io_c, io_l = {}, {}
    n_c = ex._stream_export_asset_dataset(
        hv, "book_snapshots_500ms", "XRP", out_c, "1h", 10, {},
        io_stats=io_c)  # compact_dedup=True default, coalesced
    n_l = ex._stream_export_asset_dataset(
        hv, "book_snapshots_500ms", "XRP", out_l, "1h", 10, {},
        io_stats=io_l, compact_dedup=False, coalesce_rows=1)  # legacy tuples, per-chunk
    assert n_c == n_l > 0
    assert io_c.get("rows_read") == io_l.get("rows_read")
    assert io_c.get("files_ok") == io_l.get("files_ok")
    assert _sorted_rows(out_c) == _sorted_rows(out_l)


def test_snapshot_memory_path_guards_kept() -> None:
    worker_src = inspect.getsource(ex._build_worker_main)
    assert "_limit_mb: int = 1000" in worker_src  # cap stays, never raised
    assert "rss-cap-abort" in worker_src  # fail-closed abort path kept
    timed_src = inspect.getsource(ex._phase_timed_iter)
    for token in ("phase ", "aggregate:", "write:", "elapsed=", "rss="):
        assert token in timed_src  # phase-line format frozen (monitors parse it)
    snap_src = inspect.getsource(ex._stream_export_asset_dataset)
    assert "coalesce_rows: int = 20000" in snap_src  # coalescing win kept
    assert "fail closed" in snap_src
    trades_src = inspect.getsource(ex._stream_export_trades_dataset)
    assert "_CompactDedupState" not in trades_src and "compact_dedup" not in trades_src
    stream_src = inspect.getsource(st_mod)
    assert "blake2b" not in stream_src  # shared DedupState byte-identical path
