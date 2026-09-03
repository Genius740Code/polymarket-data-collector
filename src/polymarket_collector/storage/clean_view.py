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
import pyarrow.compute as pc


def build_clean_view(
    data_dir: str | Path,
    l2_levels: int = 20,
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
            tbl = pq.read_table(str(latest_path))
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
                    tables.append(pq.read_table(str(part)))
                except Exception:
                    continue
            if not tables:
                continue
            combined = pa.concat_tables(tables, promote=True) if len(tables) > 1 else tables[0]
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
            tmp_path.rename(final_path)
            written += filtered.num_rows

    return written


def load_clean(data_dir: str | Path, asset: Optional[str] = None, date: Optional[str] = None) -> Optional[pa.Table]:
    """Load clean view rows for research (default path per §9B)."""
    base = Path(data_dir) / "book_snapshots_clean"
    if not base.exists():
        return None
    # use dataset API for convenience
    try:
        import pyarrow.dataset as ds
        dataset = ds.dataset(str(base), format="parquet", partitioning="hive")
        # filter via scanner if asset/date provided
        filt = None
        if asset:
            filt = (ds.field("asset") == asset)
        if date:
            date_f = (ds.field("date") == date)
            filt = date_f if filt is None else filt & date_f
        table = dataset.to_table(filter=filt) if filt is not None else dataset.to_table()
        return table
    except Exception:
        # fallback: read all parts
        tables = []
        pattern = base.rglob("*.parquet")
        for p in pattern:
            try:
                tables.append(pq.read_table(str(p)))
            except Exception:
                continue
        if not tables:
            return None
        return pa.concat_tables(tables, promote=True) if len(tables) > 1 else tables[0]
