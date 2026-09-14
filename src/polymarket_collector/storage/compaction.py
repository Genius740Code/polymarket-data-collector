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
    import re
    parts = []
    for p in dataset_path.iterdir():
        if p.is_file() and p.suffix == ".parquet" and not p.name.endswith(temp_suffix):
            # Accept the writer's naming pattern: {dataset}_{ts_ms}.parquet
            if re.match(r".+_\d+\.parquet$", p.name):
                parts.append(p)
    parts = sorted(parts)
    if len(parts) <= 1:
        return 0
    # PERF: incremental ParquetWriter append (was: hold all tables + concat
    # copy in RAM). Same rows, row_group_size=20000 preserved; del per table.
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
    try:
        for p in parts:
            try:
                t = read_table(p)
            except Exception:
                continue
            if t is None or t.num_rows == 0:
                continue
            try:
                if _writer is None:
                    _writer = pq.ParquetWriter(str(tmp_path), t.schema, compression="zstd")
                # row_group_size lives on write_table, not the constructor
                # (constructor kwarg raises TypeError on pyarrow 25 and made
                # compaction a silent no-op returning 0).
                _writer.write_table(t, row_group_size=20000)
                total_rows += t.num_rows
                consumed.append(p)
            finally:
                try:
                    del t
                except Exception:
                    pass
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
