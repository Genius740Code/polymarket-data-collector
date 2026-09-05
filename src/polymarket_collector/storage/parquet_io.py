"""Central parquet read helpers — file-only reads, no hive partition inference.

Root-cause fix (2026-09-05): `pq.read_table()` on a file inside a hive layout
(`asset=BTC/`, `date=.../`) auto-infers the partition columns via the dataset
API. Our files also carry `asset` internally, so the inferred partition column
(dictionary<string>) clashes with the in-file column (string) and EVERY dataset
read fails with `ArrowTypeError: Field asset has incompatible types`. Readers
that swallowed the error then reported 0 rows (fake "100% data loss") and the
Kaggle exporter shipped empty staging files.

Rule: all readers must go through this module — `read_table()` reads exactly
one file (no partition inference), `concat()` unifies schemas version-safely.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq


def read_table(path: str | Path) -> Optional[pa.Table]:
    """Read a single parquet FILE without hive partition inference.

    Returns None on failure — callers should treat None as a read error, not
    as "no rows", and log it loudly.
    """
    try:
        return pq.ParquetFile(str(path)).read()
    except Exception as e:
        print(f"[parquet_io] WARN failed to read {path}: {e}")
        return None


def concat(tables: List[pa.Table]) -> Optional[pa.Table]:
    """Version-safe schema-unifying concat (pyarrow >= 16 renamed promote)."""
    if not tables:
        return None
    if len(tables) == 1:
        return tables[0]
    try:
        return pa.concat_tables(tables, promote_options="default")
    except TypeError:
        return pa.concat_tables(tables, promote=True)


def read_files(
    paths: Iterable[str | Path],
    *,
    label: str = "",
    loud_on_any_error: bool = True,
) -> Optional[pa.Table]:
    """Read many parquet files and concat. Never raises.

    Read failures are printed (never silently swallowed). Returns None only
    when nothing could be read.
    """
    paths = list(paths)
    tables: List[pa.Table] = []
    errors = 0
    for p in paths:
        t = read_table(p)
        if t is None:
            errors += 1
            continue
        tables.append(t)
    if errors and (loud_on_any_error or errors == len(paths)):
        print(f"[parquet_io] {label}: {errors}/{len(paths)} files failed to read")
    return concat(tables)


def read_dataset_dir(dataset_dir: str | Path, *, label: str = "") -> Optional[pa.Table]:
    """Read every non-tmp parquet file under a dataset dir (any hive depth)."""
    base = Path(dataset_dir)
    if not base.exists():
        return None
    files = [p for p in base.rglob("*.parquet") if not p.name.endswith(".tmp")]
    if not files:
        return None
    return read_files(files, label=label or base.name)
