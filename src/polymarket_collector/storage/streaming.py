"""Streaming helpers for low-RAM staging builds (2026-09-10 OOM).

Root cause: zstd-compressed wide-float hive parquet expands ~30x into
Arrow, so concat-loading any whale dataset (snapshots 570MB, events 182MB)
needs GBs and the kernel SIGKills the box. Small hives passed by luck.

Rule enforced here: never materialize more than a small group of files at
once. Per-file tables are transformed with Arrow kernels, appended to an
incremental pq.ParquetWriter, and released. Files are processed in
date/mtime order so each staging file stays time-ordered without a global
sort (readers sort across files anyway; see E13).
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_io import read_table


def _order_key(p: Path):
    """Cheap time-approx ordering without reading file footers.

    2026-09-11: footer-timestamp ordering (pq.read_metadata().schema per
    file) leaks ~250KB/call in pyarrow 25 (FileMetaData.schema retention)
    and pinned ~1GB RSS per export pass, killing every upload. Date
    partition + mtime approximates write order well enough: exact cross-file
    order is NOT required anywhere (snapshot dedup is key-exact, batches are
    per-batch sorted, the summary OHLC fold tracks min/max ts itself, and
    readers sort per E13).
    """
    date = ""
    try:
        for part in p.parts:
            if part.startswith("date="):
                date = part[5:]
                break
    except Exception:
        pass
    try:
        mt = p.stat().st_mtime_ns
    except OSError:
        mt = 0
    try:
        s = str(p)
    except Exception:
        s = ""
    return (date, mt, s)


def iter_source_files(
    data_dir: str | Path,
    dataset: str,
    asset: Optional[str] = None,
    ts_col: Optional[str] = None,
) -> List[Path]:
    """Source files for (dataset, asset), oldest-first by date partition + mtime.

    Asset-partitioned datasets read only their asset= dir (partition pruning
    at the FILE level — the whole point). Ordering is approximate (exact
    cross-file order is not required: dedup is key-exact, batches are
    per-batch sorted, readers sort per E13).
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
    # ts_col is accepted for API compat but intentionally IGNORED (see
    # _order_key): footer-timestamp ordering leaked ~1GB/pass.
    files.sort(key=_order_key)
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
    batch_rows: int = 20000,
    stats: Optional[Dict] = None,
    cutoff_ts: Optional[float] = None,
) -> Iterator[pa.Table]:
    """Yield row-group batches (bounded RAM), oldest file first.

    2026-09-11 OOM: per-FILE tables still explode (100MB staging/hive files
    expand ~30x). Batches cap the transient at batch_rows regardless of
    file size. Order across batches follows date/mtime order (approx time).

    stats (optional dict): filled with files_ok / files_failed /
    failed_bytes / rows_read so the export-coverage manifest can fail closed
    on unreadable inputs without re-reading the hive. cutoff_ts: skip files
    newer than the export build start (they belong to the next cycle).
    """
    import pyarrow.parquet as _pq

    files = iter_source_files(data_dir, dataset, asset, ts_col)
    if cutoff_ts is not None:
        kept = []
        for p in files:
            try:
                if p.stat().st_mtime > cutoff_ts:
                    continue
            except OSError:
                continue
            kept.append(p)
        files = kept
    if max_files is not None:
        files = files[:max_files]
    if stats is not None:
        stats["files_ok"] = 0
        stats["files_failed"] = 0
        stats["failed_bytes"] = 0
        stats["rows_read"] = 0
    for p in files:
        try:
            _pf = _pq.ParquetFile(str(p))
        except Exception:
            if stats is not None:
                stats["files_failed"] += 1
                try:
                    stats["failed_bytes"] += p.stat().st_size
                except OSError:
                    pass
            continue
        try:
            read_any = False
            yielded_any = False
            transform_errored = False
            for chunk in _pf.iter_batches(batch_size=batch_rows):
                t = pa.Table.from_batches([chunk])
                if t.num_rows == 0:
                    continue
                read_any = True
                if stats is not None:
                    stats["rows_read"] += t.num_rows
                if transform is not None:
                    try:
                        t = transform(t)
                    except Exception:
                        transform_errored = True
                        continue
                    if t is None or t.num_rows == 0:
                        # filtered out (e.g. other-lane series_id) — the file
                        # itself read fine, so it is NOT a read failure.
                        continue
                yielded_any = True
                yield t
            if stats is not None:
                # Failed only when batches errored AND nothing usable came
                # out. Clean reads (fully lane-filtered, schema-empty) are ok.
                if transform_errored and not yielded_any:
                    stats["files_failed"] += 1
                    try:
                        stats["failed_bytes"] += p.stat().st_size
                    except OSError:
                        pass
                else:
                    stats["files_ok"] += 1
            _ = read_any  # documents the loop ran; kept for clarity
        except Exception:
            if stats is not None:
                stats["files_failed"] += 1
                try:
                    stats["failed_bytes"] += p.stat().st_size
                except OSError:
                    pass
            continue


def write_batches(
    tables: Iterator[pa.Table],
    out_path: str | Path,
    schema: Optional[pa.Schema] = None,
    compression: str = "zstd",
    row_group_rows: int = 20000,
) -> int:
    """Append batches to one parquet file with a FIXED schema. Returns rows.

    Small input batches are coalesced to ~row_group_rows before each write
    so the output has sane row-group counts AND the writer never buffers an
    unbounded row group: 2026-09-11 — ParquetWriter's default 1M-row row
    group buffered the whole staging file until close (encode ~1GB transient
    at close on the BTC lane, tripping the worker RSS cap every cycle).
    Peak transient here stays ~one row group regardless of total rows.
    """
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
    pending: List[pa.Table] = []
    pending_rows = 0

    def _flush_pending() -> None:
        nonlocal pending, pending_rows, writer, rows
        if not pending:
            return
        t = pending[0] if len(pending) == 1 else pa.concat_tables(pending, promote_options="default")
        pending = []
        pending_rows = 0
        writer.write_table(t, row_group_size=row_group_rows)
        rows += t.num_rows
        del t

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
            pending.append(t)
            pending_rows += t.num_rows
            if pending_rows >= row_group_rows:
                _flush_pending()
            del t
        _flush_pending()
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
