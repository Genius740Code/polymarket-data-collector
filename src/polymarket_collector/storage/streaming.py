"""Streaming helpers for low-RAM staging builds (2026-09-10 OOM).

Root cause: zstd-compressed wide-float hive parquet expands ~30x into
Arrow, so concat-loading any whale dataset (snapshots 570MB, events 182MB)
needs GBs and the kernel SIGKills the box. Small hives passed by luck.

Rule enforced here: never materialize more than a small group of files at
once. Per-file tables are transformed with Arrow kernels, appended to an
incremental pq.ParquetWriter, and released. Files are processed in footer
timestamp order so each staging file stays time-ordered without a global
sort (readers sort across files anyway; see E13).
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_io import read_table


def _footer_ts_range(path: Path, ts_col: str) -> Tuple[Optional[int], Optional[int]]:
    """Min/max of ts_col from the parquet FOOTER only (no data read)."""
    try:
        md = pq.read_metadata(str(path))
        lo, hi = None, None
        for rg in range(md.num_row_groups):
            col = None
            try:
                names = md.schema.names
                if ts_col not in names:
                    break
                ci = names.index(ts_col)
                stats = md.row_group(rg).column(ci).statistics
                if stats is None or not stats.has_min_max:
                    continue
                mn, mx = stats.min, stats.max
            except Exception:
                continue
            if mn is not None:
                lo = mn if lo is None else min(lo, mn)
            if mx is not None:
                hi = mx if hi is None else max(hi, mx)
        return lo, hi
    except Exception:
        return None, None


def iter_source_files(
    data_dir: str | Path,
    dataset: str,
    asset: Optional[str] = None,
    ts_col: Optional[str] = None,
) -> List[Path]:
    """Source files for (dataset, asset), oldest-first by footer timestamp.

    Asset-partitioned datasets read only their asset= dir (partition pruning
    at the FILE level — the whole point). Unknown-ts files sort last (they
    belong to the next export, same convention as the mtime build-start rule).
    """
    from .export import PER_ASSET_DATASETS  # local import: export imports this module too

    base = Path(data_dir) / dataset
    if not base.exists():
        return []
    if asset and dataset in PER_ASSET_DATASETS:
        pats = {p.resolve() for p in base.glob(f"date=*/asset={asset.upper()}/*.parquet")}
        pats.update(p.resolve() for p in base.glob(f"date=*/asset={asset}/*.parquet"))
        files = [Path(p) for p in pats if not p.name.endswith(".tmp")]
        if not files:
            # mixed-layout fallback: filter later by column (rare, legacy)
            files = [p for p in base.rglob("*.parquet") if not p.name.endswith(".tmp")]
    else:
        files = [p for p in base.rglob("*.parquet") if not p.name.endswith(".tmp")]
    if ts_col and len(files) > 1:
        keyed = []
        for p in files:
            try:
                lo, _ = _footer_ts_range(p, ts_col)
            except Exception:
                lo = None
            keyed.append(((lo is None), lo if lo is not None else 0, str(p), p))
        keyed.sort(key=lambda k: (k[0], k[1], k[2]))
        files = [k[3] for k in keyed]
    else:
        files.sort(key=str)
    return files


class DedupState:
    """Global exact-dedup across streamed batches from NARROW key columns.

    Only the key tuples ever accumulate in RAM (small); data batches flow
    through and are released after each append.
    """

    def __init__(self, key_cols: List[str]):
        self.key_cols = key_cols
        self.seen: set = set()
        self.dupes = 0
        self.kept = 0

    def filter(self, table: pa.Table) -> pa.Table:
        """Return rows whose key is new, recording them. Arrow pre-check first."""
        if table.num_rows == 0 or not all(c in table.schema.names for c in self.key_cols):
            return table
        try:
            keys = pa.StructArray.from_arrays(
                [table.column(c) for c in self.key_cols],
                names=[f"_{i}" for i in range(len(self.key_cols))],
            )
            counts = keys.value_counts().field("counts").to_pylist()
            if counts and max(counts) == 1 and not self.seen:
                # fast path: batch internally unique + nothing seen before
                vals = keys.to_pylist()
                self.seen.update(
                    tuple(v[f"_{i}"] for i in range(len(self.key_cols))) for v in vals
                )
                self.kept += table.num_rows
                return table
        except Exception:
            pass
        # slow path: narrow key extraction only (never whole-row dicts)
        try:
            cols = [table.column(c).to_pylist() for c in self.key_cols]
        except Exception:
            return table
        keep = []
        for i in range(table.num_rows):
            k = tuple(cols[j][i] for j in range(len(self.key_cols)))
            if k not in self.seen:
                self.seen.add(k)
                keep.append(i)
        self.dupes += table.num_rows - len(keep)
        self.kept += len(keep)
        if len(keep) == table.num_rows:
            return table
        import pyarrow.compute as pc  # noqa: F401 (kept for future kernel use)

        # build mask without a python set-lookup per row for big batches
        keep_set = set(keep)
        mask = pa.array([i in keep_set for i in range(table.num_rows)], type=pa.bool_())
        return table.filter(mask)


def stream_batches(
    data_dir: str | Path,
    dataset: str,
    asset: Optional[str] = None,
    ts_col: Optional[str] = None,
    transform: Optional[Callable[[pa.Table], pa.Table]] = None,
    max_files: Optional[int] = None,
) -> Iterator[pa.Table]:
    """Yield per-file transformed tables, oldest first. Peak = one file."""
    files = iter_source_files(data_dir, dataset, asset, ts_col)
    if max_files is not None:
        files = files[:max_files]
    for p in files:
        try:
            t = read_table(p)
        except Exception:
            continue
        if t is None or t.num_rows == 0:
            continue
        if transform is not None:
            try:
                t = transform(t)
            except Exception:
                continue
            if t is None or t.num_rows == 0:
                continue
        yield t


def write_batches(
    tables: Iterator[pa.Table],
    out_path: str | Path,
    schema: Optional[pa.Schema] = None,
    compression: str = "zstd",
) -> int:
    """Append batches to one parquet file with a FIXED schema. Returns rows."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    def _writable_schema(s: pa.Schema) -> pa.Schema:
        # pq.ParquetWriter enforces nullable flags (pq.write_table, used by the
        # legacy path, does not). Staging values are identical either way;
        # readers never depend on the flags. Relax to avoid failing honest
        # sparse rows (e.g. legacy rows missing snapshot_id).
        try:
            return pa.schema([pa.field(f.name, f.type, nullable=True) for f in s])
        except Exception:
            return s

    writer: Optional[pq.ParquetWriter] = None
    rows = 0
    try:
        for t in tables:
            if t is None or t.num_rows == 0:
                continue
            if writer is None:
                wschema = _writable_schema(schema or t.schema)
                # normalize this batch to the writer schema (drop extras,
                # null-fill missing) so mixed vintages never break the write
                try:
                    cols = []
                    for f in wschema:
                        if f.name in t.schema.names:
                            c = t.column(f.name)
                            cols.append(c.cast(f.type) if not c.type.equals(f.type) else c)
                        else:
                            cols.append(pa.array([None] * t.num_rows, type=f.type))
                    t = pa.table(cols, schema=wschema)
                except Exception:
                    wschema = t.schema
                writer = pq.ParquetWriter(str(tmp_path), wschema, compression=compression)
            else:
                try:
                    cols = []
                    for f in writer.schema:
                        if f.name in t.schema.names:
                            c = t.column(f.name)
                            cols.append(c.cast(f.type) if not c.type.equals(f.type) else c)
                        else:
                            cols.append(pa.array([None] * t.num_rows, type=f.type))
                    t = pa.table(cols, schema=writer.schema)
                except Exception:
                    continue
            writer.write_table(t)
            rows += t.num_rows
            del t
        if writer is None:
            # no rows: still write an (empty, schema-correct) file when asked
            if schema is not None:
                pq.write_table(pa.table({f.name: [] for f in schema}, schema=schema), str(tmp_path), compression=compression)
            else:
                return 0
        else:
            writer.close()
        import os as _os

        _os.replace(str(tmp_path), str(out_path))
        return rows
    finally:
        try:
            if writer is not None:
                writer.close()
        except Exception:
            pass
        try:
            if tmp_path.exists() and not out_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
