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


def read_table(path: str | Path, columns: Optional[List[str]] = None) -> Optional[pa.Table]:
    """Read a single parquet FILE without hive partition inference.

    Returns None on failure — callers should treat None as a read error, not
    as "no rows", and log it loudly. ``columns`` optionally projects to a
    subset (missing columns are the caller's problem — intersect first).
    """
    try:
        return pq.ParquetFile(str(path)).read(columns=columns)
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


# Read-side seal (flush-boundary partials): writers publish each flush file
# atomically via tmp+rename, so every visible .parquet file is whole — but
# the FRESHEST file covers a still-open window (later flushes will add rows
# for the same minute). Spot readers that include it see a short minute and
# live% wobbles at file edges. The seal marker is file age: a file is sealed
# only once its mtime is at least ``grace_s`` old. No writer change needed.
# Fail-closed: any stat/coerce error means treat-as-open (excluded), never
# included. All helpers are total (never raise). Timestamps go through
# validation.coerce_ts_source_ms.
SEAL_GRACE_S = 120.0


def _path_mtime_ms(path: str | Path) -> Optional[int]:
    """mtime of one file as int epoch-ms, or None when unknown. Total."""
    try:
        from ..validation import coerce_ts_source_ms
    except Exception:
        coerce_ts_source_ms = None  # type: ignore
    try:
        mtime = Path(path).stat().st_mtime
    except Exception:
        return None
    if coerce_ts_source_ms is not None:
        try:
            return coerce_ts_source_ms(mtime)
        except Exception:
            return None
    try:
        return int(float(mtime) * 1000)
    except Exception:
        return None


def _now_ms(now: Optional[float] = None) -> Optional[int]:
    """Wall-clock now as int epoch-ms (``now`` override is epoch seconds). Total."""
    try:
        from ..validation import coerce_ts_source_ms
    except Exception:
        coerce_ts_source_ms = None  # type: ignore
    try:
        import time as _time
        raw = now if now is not None else _time.time()
        if coerce_ts_source_ms is not None:
            return coerce_ts_source_ms(raw)
        return int(float(raw) * 1000)
    except Exception:
        return None


def is_sealed(path: str | Path, grace_s: float = SEAL_GRACE_S, now: Optional[float] = None) -> bool:
    """True when a hive file is sealed (age >= grace_s). Total, fail-closed.

    ``.tmp`` files, missing files, and anything unstatable return False.
    """
    try:
        p = Path(path)
    except Exception:
        return False
    try:
        if p.name.endswith(".tmp"):
            return False
    except Exception:
        return False
    try:
        grace = float(grace_s)
    except Exception:
        return False
    if grace < 0:
        return False
    mtime_ms = _path_mtime_ms(p)
    now_ms = _now_ms(now)
    if mtime_ms is None or now_ms is None:
        return False
    try:
        return (now_ms - mtime_ms) >= int(grace * 1000)
    except Exception:
        return False


def list_sealed_files(
    paths: Iterable[str | Path],
    grace_s: float = SEAL_GRACE_S,
    now: Optional[float] = None,
) -> List[Path]:
    """Filter a file list to sealed files only. Total, fail-closed.

    Input order is preserved. Dubious entries (unstatable, .tmp, bad types)
    are dropped, never included.
    """
    try:
        items = list(paths or [])
    except Exception:
        return []
    try:
        grace = float(grace_s)
    except Exception:
        return []
    sealed: List[Path] = []
    for entry in items:
        try:
            p = entry if isinstance(entry, Path) else Path(entry)
        except Exception:
            continue
        try:
            if is_sealed(p, grace, now):
                sealed.append(p)
        except Exception:
            continue
    return sealed


def list_sealed_dataset_files(
    dataset_dir: str | Path,
    *,
    grace_s: float = SEAL_GRACE_S,
    now: Optional[float] = None,
) -> List[Path]:
    """Sealed .parquet files under a dataset dir (any hive depth), oldest-first. Total."""
    try:
        base = Path(dataset_dir)
    except Exception:
        return []
    try:
        if not base.exists():
            return []
        files = [p for p in base.rglob("*.parquet")]
    except Exception:
        return []
    sealed = list_sealed_files(files, grace_s, now)
    try:
        sealed.sort(key=str)
    except Exception:
        pass
    return sealed
