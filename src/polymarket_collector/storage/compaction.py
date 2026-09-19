"""Periodic compaction — §10A.

Merges small flushed files into larger partition files. Writes atomically
(temp + rename) so a compactor crash never corrupts settled data.
"""
from __future__ import annotations

import argparse
import uuid
from pathlib import Path

import pyarrow.parquet as pq


def _os_replace_safe(src, dst):
    """Atomic tmp->final rename that works on Windows (os.replace overwrites; Path.rename raises WinError 183 if dst exists)."""
    import os as _os
    _os.replace(str(src), str(dst))


from .parquet_io import read_table
import pyarrow as pa


def compact_dataset(dataset_path: Path, temp_suffix: str = ".tmp") -> int:
    """Compact one dataset partition (e.g. data/book_snapshots_500ms/date=.../asset=...).

    Merges all {dataset}_{ts_ms}.parquet files in a leaf partition dir into one.
    Uses the SAME naming pattern as the writer: {dataset}_{ts_ms}.parquet.
    Returns number of rows after compaction. No-op if <=1 file.
    """
    if not dataset_path.is_dir():
        return 0
    # Match writer's naming pattern: {dataset}_{ts_ms}.parquet
    # e.g. book_snapshots_500ms_1700000000000.parquet
    # C8 (audit 2026-09-16): ALSO accept part-compacted-*.parquet inputs.
    # Old compactions concatenated flush files 1:1 into row groups (~111
    # rows/group, 6115 groups in one 241MB file) whose footer thrift-parse
    # costs ~700MB transient per open — tripping every export worker's
    # rss-cap. Re-compacting with coalescing repairs those monsters.
    import re
    parts = []
    for p in dataset_path.iterdir():
        if p.is_file() and p.suffix == ".parquet" and not p.name.endswith(temp_suffix):
            # Accept the writer's naming pattern: {dataset}_{ts_ms}.parquet
            if re.match(r".+_\d+\.parquet$", p.name):
                parts.append(p)
            elif p.name.startswith("part-compacted-"):
                parts.append(p)
    parts = sorted(parts)
    if len(parts) <= 1:
        return 0
    # C8: smallest-first so transient footer-parse spikes from legacy
    # monsters (~700MB for a 241MB/6k-group file) land last, on trim-clean
    # heap, instead of pinning the peak for the whole run.
    try:
        parts = sorted(parts, key=lambda p: p.stat().st_size)
    except Exception:
        pass
    # C8: coalesce small row groups (was: write_table per flush file, each
    # ~100-row file becoming its own row group(s) — row_group_size is a
    # per-call MAXIMUM, not a coalescing target). Buffer to CHUNK rows so
    # output row groups are full; peak stays ~one chunk regardless of input
    # file count. Tables are normalized to the first schema (cast +
    # null-fill) so mixed vintages compact instead of aborting the run.
    CHUNK_ROWS = 20000
    tmp_path = dataset_path / f"part-compacted-{uuid.uuid4().hex[:8]}.parquet{temp_suffix}"
    final_path = dataset_path / f"part-compacted-{uuid.uuid4().hex[:8]}.parquet"
    # 2026-09-11: small row groups — one giant single-row-group file forces
    # the streaming export to materialize the whole row group at once
    # (PyArrow reads row-group-at-a-time; iter_batches slices do not release
    # the parent), tripping the worker RSS cap and killing uploads.
    total_rows = 0
    consumed: list = []  # only files actually read — unreadable stubs are
    # never deleted here (they go to quarantine, not oblivion, per
    # Real-Data-Only: gaps stay honest instead of vanishing in compaction).
    _writer = None
    _wschema = None
    _pending: list = []
    _pending_rows = 0

    def _norm(_t):
        # normalize to writer schema (drop extras, cast, null-fill missing)
        try:
            _cols = []
            for _f in _wschema:
                if _f.name in _t.schema.names:
                    _c = _t.column(_f.name)
                    _cols.append(_c.cast(_f.type) if not _c.type.equals(_f.type) else _c)
                else:
                    _cols.append(pa.array([None] * _t.num_rows, type=_f.type))
            return pa.table(_cols, schema=_wschema)
        except Exception:
            return None

    def _flush_pending():
        nonlocal _pending, _pending_rows, total_rows
        if not _pending:
            return
        _t = _pending[0] if len(_pending) == 1 else pa.concat_tables(_pending, promote_options="default")
        _pending = []
        _pending_rows = 0
        _writer.write_table(_t, row_group_size=CHUNK_ROWS)
        total_rows += _t.num_rows
        del _t

    try:
        from .streaming import malloc_trim as _trim
    except Exception:
        _trim = None

    def _file_batches(_p):
        """Yield ~CHUNK-row tables from one file without materializing it.

        Small flush files come back whole; legacy monsters stream in
        bounded batches so the run peak stays ~one chunk + one footer.
        """
        try:
            _pf = pq.ParquetFile(str(_p))
        except Exception:
            try:
                _t = read_table(_p)
            except Exception:
                return
            if _t is not None and _t.num_rows:
                yield _t
            return
        try:
            for _chunk in _pf.iter_batches(batch_size=CHUNK_ROWS):
                _bt = pa.Table.from_batches([_chunk])
                del _chunk
                if _bt.num_rows:
                    yield _bt
                else:
                    del _bt
        finally:
            try:
                del _pf
            except Exception:
                pass

    try:
        _n_files = 0
        for p in parts:
            _n_files += 1
            # C7 sister-fix: footer/thrift heap pins ~2MB/file otherwise.
            if _trim is not None and _n_files % 100 == 0:
                try:
                    import gc as _gc

                    _gc.collect()
                except Exception:
                    pass
                try:
                    _trim()
                except Exception:
                    pass
            try:
                _got_rows = False
                for t in _file_batches(p):
                    try:
                        if _writer is None:
                            _wschema = t.schema
                            _writer = pq.ParquetWriter(str(tmp_path), _wschema, compression="zstd")
                        if not t.schema.equals(_wschema):
                            t = _norm(t)
                            if t is None or t.num_rows == 0:
                                continue
                        _got_rows = True
                        _pending.append(t)
                        _pending_rows += t.num_rows
                        if _pending_rows >= CHUNK_ROWS:
                            _flush_pending()
                    finally:
                        try:
                            del t
                        except Exception:
                            pass
                if _got_rows:
                    consumed.append(p)
            except Exception:
                continue
        _flush_pending()
        if _writer is not None:
            try:
                _writer.close()
            except Exception:
                pass
            _writer = None
        else:
            return 0
    except Exception:
        try:
            if _writer is not None:
                _writer.close()
        except Exception:
            pass
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        return 0
    # fsync tmp before publish (same crash window as the writer flush path).
    try:
        with open(str(tmp_path), "rb") as _fh:
            try:
                import os as _os
                _os.fsync(_fh.fileno())
            except Exception:
                pass
    except Exception:
        pass
    _os_replace_safe(tmp_path, final_path)
    try:
        import os as _os2
        _dfd = _os2.open(str(dataset_path), _os2.O_DIRECTORY)
        try:
            _os2.fsync(_dfd)
        finally:
            try:
                _os2.close(_dfd)
            except Exception:
                pass
    except Exception:
        pass
    # remove only consumed inputs after successful new write — unreadable
    # stubs stay for quarantine instead of vanishing here.
    for p in consumed:
        try:
            p.unlink()
        except Exception:
            pass
    return total_rows


def compact_all(data_dir: str | Path, datasets: list[str] | None = None, temp_suffix: str = ".tmp") -> dict:
    """Compact every leaf partition under data_dir for given datasets (§10A schedule)."""
    base = Path(data_dir)
    if datasets is None:
        datasets = [
            "book_snapshots_500ms",
            "book_events",
            "trades",
            "chainlink_events",
            "collector_events",
            "resync_episodes",
            "markets_log",
        ]
    stats: dict = {}
    for ds_name in datasets:
        ds_path = base / ds_name
        if not ds_path.exists():
            continue
        # PERF: collect distinct leaf dirs in one walk (was: per-FILE
        # iteration + relative_to per file + re-listdir per leaf).
        leaf_dirs: set = set()
        try:
            for leaf in ds_path.rglob("*.parquet"):
                try:
                    leaf_dirs.add(leaf.parent)
                except Exception:
                    continue
        except Exception:
            continue
        for leaf_dir in sorted(leaf_dirs, key=str):
            try:
                key = str(leaf_dir.relative_to(base))
            except Exception:
                continue
            if key in stats:
                continue
            rows = compact_dataset(leaf_dir, temp_suffix=temp_suffix)
            if rows:
                stats[key] = rows
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Compaction job — §10A (temp + atomic rename)")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--datasets", nargs="*", default=None)
    ap.add_argument("--temp-suffix", default=".tmp")
    args = ap.parse_args()
    stats = compact_all(args.data_dir, datasets=args.datasets, temp_suffix=args.temp_suffix)
    if stats:
        for k, v in stats.items():
            print(f"compacted {k}: {v} rows")
    else:
        print("no compaction needed")


if __name__ == "__main__":
    main()
