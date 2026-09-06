"""Canonical clean view for research — §9B.

book_snapshots_clean = SELECT * FROM book_snapshots_500ms WHERE book_state='live'
  AND condition_id NOT IN (markets with resolution_outcome in disputed)
  — unknown is NOT excluded (active markets are unknown until resolved; excluding would make clean 0% during live)

This is the default read path for backtests; querying book_snapshots_500ms
directly includes stale/resyncing intentionally.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq


def _os_replace_safe(src, dst):
    """Atomic tmp->final rename that works on Windows (os.replace overwrites; Path.rename raises WinError 183 if dst exists)."""
    import os as _os
    _os.replace(str(src), str(dst))


from .parquet_io import read_table, concat as _concat_tables
import pyarrow.compute as pc


def build_clean_view(
    data_dir: str | Path,
    l2_levels: int = 10,
    opt_in_disputed: bool = False,
) -> int:
    """Build/refresh book_snapshots_clean.

    Reads book_snapshots_500ms and markets_latest, filters per §9B, writes
    partitioned parquet under book_snapshots_clean/date=.../asset=... atomically.
    Returns number of rows written.

    Idempotent: rewrites whole clean view from source tables.
    """
    base = Path(data_dir)
    src_root = base / "book_snapshots_500ms"
    latest_path = base / "markets_latest" / "markets_latest.parquet"
    dst_root = base / "book_snapshots_clean"

    if not src_root.exists():
        return 0

    # load disputed condition_ids from markets_latest — §9B clean view excludes only disputed
    # (unknown is normal for active markets and must NOT be excluded, otherwise clean is 0% during live collection)
    excluded: set[str] = set()
    if latest_path.exists() and not opt_in_disputed:
        try:
            tbl = read_table(latest_path)
            for row in tbl.to_pylist():
                if row.get("resolution_outcome") in ("disputed",):
                    cid = row.get("condition_id")
                    if cid:
                        excluded.add(cid)
        except Exception:
            pass

    # collect source tables per partition (date/asset) to preserve partitioning
    # We need to walk src partitions and filter per partition so output mirrors input partitions
    written = 0
    for date_dir in src_root.glob("date=*"):
        date_str = date_dir.name  # e.g. date=2025-01-01
        for asset_dir in date_dir.glob("asset=*"):
            asset = asset_dir.name.split("=", 1)[1] if "=" in asset_dir.name else asset_dir.name
            tables: List[pa.Table] = []
            for part in asset_dir.glob("*.parquet"):
                try:
                    tables.append(read_table(part))
                except Exception:
                    continue
            if not tables:
                continue
            combined = _concat_tables(tables) if len(tables) > 1 else tables[0]
            # filter: book_state == 'live'
            try:
                mask = pc.equal(combined.column("book_state"), pa.scalar("live"))
                filtered = combined.filter(mask)
            except Exception:
                filtered = combined

            # filter: condition_id not in excluded
            if excluded and "condition_id" in filtered.schema.names:
                try:
                    # build mask where condition_id not in excluded
                    # pyarrow doesn't have is_in with set directly efficiently for small set; use python filter
                    pylist = filtered.to_pylist()
                    kept = [r for r in pylist if r.get("condition_id") not in excluded]
                    if not kept:
                        continue
                    filtered = pa.Table.from_pylist(kept, schema=filtered.schema)
                except Exception:
                    pass

            if filtered.num_rows == 0:
                continue

            out_dir = dst_root / date_str / f"asset={asset}"
            out_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = out_dir / f"part-{asset}-{date_str.replace('date=','')}.parquet.tmp"
            final_path = out_dir / f"part-{asset}-{date_str.replace('date=','')}.parquet"
            # atomic write (§10A)
            pq.write_table(filtered, str(tmp_path), compression="zstd")
            _os_replace_safe(tmp_path, final_path)
            written += filtered.num_rows

    return written


def load_clean(data_dir: str | Path, asset: Optional[str] = None, date: Optional[str] = None) -> Optional[pa.Table]:
    """Load clean view rows for research (default path per §9B)."""
    base = Path(data_dir) / "book_snapshots_clean"
    if not base.exists():
        return None
    # file-only reads via parquet_io (hive dataset API clashes with in-file asset column)
    try:
        files = [p for p in base.rglob("*.parquet") if not p.name.endswith(".tmp")]
        if asset:
            files = [p for p in files if f"asset={asset.upper()}" in str(p) or f"asset={asset}" in str(p)]
        if date:
            files = [p for p in files if f"date={date}" in str(p)]
        tbl = read_files(files, label="clean_view load")
        return tbl
    except Exception:
        # fallback: read all parts
        tables = []
        pattern = base.rglob("*.parquet")
        for p in pattern:
            try:
                tables.append(read_table(p))
            except Exception:
                continue
        if not tables:
            return None
        return _concat_tables(tables) if len(tables) > 1 else tables[0]
