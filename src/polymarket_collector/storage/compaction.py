"""Periodic compaction — §10A.

Merges small flushed files into larger partition files. Writes atomically
(temp + rename) so a compactor crash never corrupts settled data.
"""
from __future__ import annotations

import argparse
import uuid
from pathlib import Path

import pyarrow.parquet as pq
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
    tables = []
    for p in parts:
        try:
            tables.append(pq.read_table(str(p)))
        except Exception:
            continue
    if not tables:
        return 0
    combined = pa.concat_tables(tables, promote=True) if len(tables) > 1 else tables[0]
    tmp_path = dataset_path / f"part-compacted-{uuid.uuid4().hex[:8]}.parquet{temp_suffix}"
    final_path = dataset_path / f"part-compacted-{uuid.uuid4().hex[:8]}.parquet"
    pq.write_table(combined, str(tmp_path), compression="zstd")
    tmp_path.rename(final_path)
    # remove old parts only after successful new write
    for p in parts:
        try:
            p.unlink()
        except Exception:
            pass
    return combined.num_rows


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
        # find leaf partition dirs (those containing parquet files)
        for leaf in ds_path.rglob("*.parquet"):
            leaf_dir = leaf.parent
            key = str(leaf_dir.relative_to(base))
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
