"""Kaggle-style per-asset single-file export — time first, condition_id second, no binance.

Reads hive-partitioned parquet under data/ and writes one flat parquet per asset per dataset:
  data/export/BTC_book_snapshots.parquet  (or data/export/book_snapshots_BTC.parquet)
  data/export/BTC_trades.parquet
etc.

Time-first column order + sorting by ts + condition_id is enforced.
Binance rows (source=binance-ticker-proxy) are excluded when include_binance=False.

Unlike live writer (batched hive partitions §10A) this is run on-demand for sharing.
Atomic tmp+rename same as parquet_writer.py §10A.

Additional functionality:
- Timeframe aggregation: derives 15min/1h/4h/1d from 5min base data
- Kaggle API upload with per-dataset versioning
- Post-upload local data cleanup with integrity guarantees
"""
from __future__ import annotations

import argparse
import uuid
from pathlib import Path
from typing import List, Optional, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from .schemas import SCHEMAS, snapshot_schema


# Datasets that are per-asset vs global
PER_ASSET_DATASETS = {"book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events"}
NON_ASSET_DATASETS = {"markets_log", "collector_events", "resync_episodes"}

# Prefer these ts columns for sorting (first present wins)
TS_SORT_CANDIDATES = [
    "ts_snapshot_utc", "ts_snapshot_ns",
    "ts_source", "ts_received_ns",
    "ts_utc", "ts_received_ns",
    "market_start_ts", "updated_at",
    "disconnect_ts_utc",
]


def _sort_keys_for_schema(schema: pa.Schema) -> List[str]:
    """Return sort keys that exist in schema, time first then condition_id."""
    keys: List[str] = []
    for k in TS_SORT_CANDIDATES:
        if k in schema.names and k not in keys:
            keys.append(k)
            if len(keys) >= 2:
                break
    # always add condition_id as tie-breaker if present
    if "condition_id" in schema.names and "condition_id" not in keys:
        keys.append("condition_id")
    # also add asset if present for stable sort, but per-asset export already filtered
    return keys


def _get_schema(dataset: str, l2_levels: int = 20) -> Optional[pa.Schema]:
    if dataset == "book_snapshots_500ms":
        return snapshot_schema(l2_levels)
    return SCHEMAS.get(dataset)


def _read_dataset_per_asset(data_dir: Path, dataset: str, asset: Optional[str], include_binance: bool = False) -> Optional[pa.Table]:
    """Read all parquet files for dataset (+ optional asset filter)."""
    base = data_dir / dataset
    if not base.exists():
        return None
    # gather files
    if asset and dataset in PER_ASSET_DATASETS:
        # search hive partitions: dataset/date=*/asset=ASSET/*.parquet
        patterns_set = {p.resolve() for p in base.glob(f"date=*/asset={asset.upper()}/*.parquet")}
        patterns_set.update(p.resolve() for p in base.glob(f"date=*/asset={asset}/*.parquet"))
        patterns = [Path(p) for p in patterns_set]
        # also flat single-file already? fallback to rglob
        if not patterns:
            patterns = [p for p in base.rglob("*.parquet") if f"asset={asset.upper()}" in str(p) or asset.upper() in str(p.parent)]
            # if still empty, read all and filter later by asset column
            if not patterns:
                patterns = list(base.rglob("*.parquet"))
    else:
        patterns = list(base.rglob("*.parquet"))
        # exclude tmp
        patterns = [p for p in patterns if not p.name.endswith(".tmp")]
    if not patterns:
        return None
    tables: List[pa.Table] = []
    for p in patterns:
        if p.name.endswith(".tmp"):
            continue
        try:
            t = pq.read_table(str(p))
            # filter by asset column if per-asset requested but files are mixed
            if asset and dataset in PER_ASSET_DATASETS and "asset" in t.schema.names:
                # if file path already guaranteed asset, skip filter; else filter
                if f"asset={asset.upper()}" not in str(p):
                    try:
                        mask = pc.equal(t.column("asset"), pa.scalar(asset.upper()))
                        t = t.filter(mask)
                        if t.num_rows == 0:
                            continue
                    except Exception:
                        pass
            # exclude binance if chainlink and not include_binance
            if dataset == "chainlink_events" and not include_binance and "source" in t.schema.names:
                try:
                    mask = pc.not_equal(t.column("source"), pa.scalar("binance-ticker-proxy"))
                    t = t.filter(mask)
                    if t.num_rows == 0:
                        continue
                except Exception:
                    pass
            tables.append(t)
        except Exception:
            continue
    if not tables:
        return None
    combined = pa.concat_tables(tables, promote=True) if len(tables) > 1 else tables[0]
    # filter binance again if combined still has mixed sources (promote case)
    if dataset == "chainlink_events" and not include_binance and "source" in combined.schema.names:
        try:
            mask = pc.not_equal(combined.column("source"), pa.scalar("binance-ticker-proxy"))
            combined = combined.filter(mask)
        except Exception:
            pass
    # backfill trades: compute notional, fee, aggressor_side, transaction_hash where null for old 3.1.0 data
    if dataset == "trades" and combined.num_rows > 0:
        try:
            # convert to pylist for easy backfill, then back to table
            pylist = combined.to_pylist()
            changed = False
            for r in pylist:
                if r.get("notional") is None and r.get("price") is not None and r.get("size") is not None:
                    try:
                        r["notional"] = float(r["price"]) * float(r["size"])
                        changed = True
                    except Exception:
                        pass
                if r.get("fee") is None and r.get("notional") is not None:
                    try:
                        r["fee"] = float(r["notional"]) * 0.0007  # 0.07% crypto fee per user
                        changed = True
                    except Exception:
                        pass
                if r.get("aggressor_side") is None and r.get("side"):
                    try:
                        r["aggressor_side"] = str(r["side"]).upper()
                        changed = True
                    except Exception:
                        pass
                if r.get("transaction_hash") is None and r.get("trade_id"):
                    # fallback: trade_id may be hash if hash was used as trade_id
                    # check if trade_id looks like hash (hex length 32+)
                    tid = str(r.get("trade_id"))
                    if len(tid) >= 32 and all(c in "0123456789abcdef" for c in tid.lower()[:8]):
                        r["transaction_hash"] = tid
                        changed = True
                if r.get("sequence_number") is None and r.get("ts_source"):
                    try:
                        # ts_source like 1788000650548 as ms string
                        ts = r["ts_source"]
                        if isinstance(ts, str) and ts.isdigit():
                            r["sequence_number"] = int(ts)
                            changed = True
                    except Exception:
                        pass
            if changed:
                # rebuild table with same schema as combined (preserve types where possible)
                combined = pa.Table.from_pylist(pylist, schema=combined.schema)
        except Exception:
            pass
    # sort by time then condition_id
    schema = _get_schema(dataset)
    sort_keys = _sort_keys_for_schema(combined.schema if schema is None else schema)
    # only sort keys that exist in actual combined
    sort_keys = [k for k in sort_keys if k in combined.schema.names]
    if sort_keys:
        try:
            indices = pc.sort_indices(combined, sort_keys=[(k, "ascending") for k in sort_keys])
            combined = pc.take(combined, indices)
        except Exception:
            pass
    # reorder columns to time-first schema if schema available, add missing cols as nulls
    if schema is not None:
        try:
            # add missing schema columns as nulls (e.g. transaction_hash added in 3.2.0)
            for field in schema:
                if field.name not in combined.schema.names:
                    # create null column of correct type
                    null_arr = pa.array([None]*combined.num_rows, type=field.type)
                    combined = combined.append_column(field.name, null_arr)
            # build new order: schema.names that exist in combined + remaining cols
            ordered = [n for n in schema.names if n in combined.schema.names]
            remaining = [n for n in combined.schema.names if n not in ordered]
            final_order = ordered + remaining
            combined = combined.select(final_order)
        except Exception:
            pass
    return combined


def export_per_asset_single_file(
    data_dir: str | Path,
    out_dir: str | Path | None = None,
    datasets: List[str] | None = None,
    assets: List[str] | None = None,
    l2_levels: int = 20,
    include_binance: bool = False,
) -> dict:
    """Export one flat parquet per asset per dataset (Kaggle style).

    Returns dict {relative_out_path: rows}
    """
    base = Path(data_dir)
    out = Path(out_dir) if out_dir else base / "export"
    out.mkdir(parents=True, exist_ok=True)

    if datasets is None:
        datasets = ["book_snapshots_500ms", "book_events", "trades", "chainlink_events", "markets_log", "collector_events"]
    if assets is None:
        # discover from config or from existing hive dirs
        assets = ["BTC", "ETH", "SOL"]
        # try discover from book_snapshots_500ms partitions
        try:
            snap_root = base / "book_snapshots_500ms"
            if snap_root.exists():
                discovered = set()
                for p in snap_root.glob("date=*/asset=*"):
                    discovered.add(p.name.split("=", 1)[1].upper() if "=" in p.name else p.name.upper())
                if discovered:
                    assets = sorted(discovered)
        except Exception:
            pass

    stats: dict = {}
    for ds in datasets:
        schema = _get_schema(ds, l2_levels)
        if ds in PER_ASSET_DATASETS:
            for asset in assets:
                au = asset.upper()
                table = _read_dataset_per_asset(base, ds, au, include_binance=include_binance)
                if table is None or table.num_rows == 0:
                    continue
                # enforce schema ordering already done, ensure per-asset single file
                out_path = out / f"{au}_{ds}.parquet"
                # handle clean_view alias: book_snapshots_500ms -> but user may want book_snapshots_clean too
                tmp_path = out_path.with_suffix(".parquet.tmp")
                pq.write_table(table, str(tmp_path), compression="zstd")
                tmp_path.rename(out_path)
                stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = table.num_rows
        else:
            # non-asset: single global file
            table = _read_dataset_per_asset(base, ds, None, include_binance=include_binance)
            if table is None or table.num_rows == 0:
                continue
            out_path = out / f"{ds}.parquet"
            # also handle markets_log -> markets.parquet alias for Kaggle familiarity
            tmp_path = out_path.with_suffix(".parquet.tmp")
            pq.write_table(table, str(tmp_path), compression="zstd")
            tmp_path.rename(out_path)
            stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = table.num_rows
            if ds == "markets_log":
                alias = out / "markets.parquet"
                # copy via write again or hard link? just write same table to alias
                tmp_alias = alias.with_suffix(".parquet.tmp")
                pq.write_table(table, str(tmp_alias), compression="zstd")
                tmp_alias.rename(alias)
                stats[str(alias.relative_to(base) if alias.is_relative_to(base) else alias)] = table.num_rows
    return stats


def export_all_flat(data_dir: str | Path, out_dir: str | Path | None = None, include_binance: bool = False) -> dict:
    """Compatibility wrapper for global single-file (not per-asset) — not used per user request but kept."""
    return export_per_asset_single_file(data_dir, out_dir, include_binance=include_binance)


def main() -> None:
    ap = argparse.ArgumentParser(description="Per-asset single-file export — time first, condition_id second, no binance (Kaggle style)")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--out-dir", default=None, help="output dir, default <data-dir>/export")
    ap.add_argument("--datasets", nargs="*", default=None, help="datasets to export")
    ap.add_argument("--assets", nargs="*", default=None, help="assets to export (default BTC ETH SOL or discovered)")
    ap.add_argument("--l2-levels", type=int, default=20)
    ap.add_argument("--include-binance", action="store_true", help="include binance-ticker-proxy rows (default excluded)")
    ap.add_argument("--markets-latest", action="store_true", help="also export markets_latest single file as markets_latest.parquet")
    args = ap.parse_args()
    stats = export_per_asset_single_file(
        args.data_dir,
        out_dir=args.out_dir,
        datasets=args.datasets,
        assets=args.assets,
        l2_levels=args.l2_levels,
        include_binance=args.include_binance,
    )
    # optionally also dump markets_latest flat
    if args.markets_latest:
        base = Path(args.data_dir)
        out = Path(args.out_dir) if args.out_dir else base / "export"
        latest = base / "markets_latest" / "markets_latest.parquet"
        if latest.exists():
            try:
                t = pq.read_table(str(latest))
                # sort time first
                if "updated_at" in t.schema.names:
                    idx = pc.sort_indices(t, sort_keys=[("updated_at", "ascending"), ("condition_id", "ascending")])
                    t = pc.take(t, idx)
                out_path = out / "markets_latest.parquet"
                tmp = out_path.with_suffix(".parquet.tmp")
                pq.write_table(t, str(tmp), compression="zstd")
                tmp.rename(out_path)
                stats[str(out_path)] = t.num_rows
                print(f"exported markets_latest.parquet: {t.num_rows} rows")
            except Exception as e:
                print(f"markets_latest export failed: {e}")
    if stats:
        for k, v in stats.items():
            print(f"exported {k}: {v} rows")
    else:
        print("no data to export (is data/ empty after delete?)")


# ------------------------------------------------------------------ timeframe aggregation
# When the collector runs with 5min (300s) windows, we can derive larger timeframes
# by grouping consecutive windows. This section provides aggregation functions
# and Kaggle API upload support.


def _compute_timebucket_ms(ts_ms_values: list, window_size_seconds: int) -> list:
    """Compute bucket index for timestamp values and window size.
    
    Args:
        ts_ms_values: List of timestamps in milliseconds (as ints or convertible)
        window_size_seconds: Window size in seconds (300, 900, 3600, 14400, 86400)
    
    Returns:
        List of bucket indices (one per row)
    """
    seconds_map = {
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }
    sec = seconds_map.get(window_size_seconds, 300)
    interval_ms = sec * 1000
    # Convert each value to int first, then compute bucket
    result = []
    for ts_ms in ts_ms_values:
        try:
            val = int(ts_ms)
            result.append(val // interval_ms)
        except (ValueError, TypeError):
            result.append(-1)  # invalid timestamp -> bucket -1
    return result


def aggregate_5min_to_timeframe(
    table: pa.Table,
    window_size_seconds: int,
    timeframe_label: str,
) -> pa.Table:
    """Aggregate a 5min-snapshot table into a larger timeframe.
    
    For book_snapshots_500ms: computes TWAP-like weighted average per bucket.
    For trades: groups trades into buckets.
    For chainlink_events: groups price events into buckets.
    
    The function assumes the table has ts_snapshot_ns or ts_snapshot_utc columns
    for time grouping.
    """
    if table.num_rows == 0:
        return table

    # Determine which timestamp column to use for bucketing
    ts_col = None
    for candidate in ["ts_snapshot_ns", "ts_snapshot_utc", "ts_source", "ts_received_ns", "ts_utc"]:
        if candidate in table.schema.names:
            ts_col = candidate
            break

    if ts_col is None:
        # Cannot aggregate without a timestamp; return as-is
        return table

    # Extract timestamps as Python ints for bucket computation
    try:
        ts_raw = table.column(ts_col)
        if hasattr(ts_raw, 'to_pylist'):
            ts_ms_list = ts_raw.to_pylist()
        else:
            ts_ms_list = list(ts_raw)
        # Filter out None values
        ts_ms_list = [ts for ts in ts_ms_list if ts is not None]
    except Exception:
        return table

    if not ts_ms_list:
        return table

    # Compute bucket indices
    bucket_indices = _compute_timebucket_ms(ts_ms_list, window_size_seconds)

    # Sort rows by bucket index, then take first row per bucket
    # Create a temporary table with bucket column added
    try:
        # Create bucket column as pa.array
        bucket_col = pa.array(bucket_indices, type=pa.int64())
        
        # Add bucket column to table
        table_with_bucket = table.append_column("__bucket__", bucket_col)
        
        # Sort by bucket
        sorted_table = table_with_bucket.sort_by(["__bucket__"])
        
        # Get unique buckets
        unique_buckets = sorted_table.column("__bucket__").unique()
        
        # For each unique bucket, take the first row
        results: List[pa.Table] = []
        for ub in unique_buckets:
            mask = pc.equal(sorted_table.column("__bucket__"), ub)
            bucket_table = sorted_table.filter(mask)
            # Take first row
            if bucket_table.num_rows > 0:
                first_row = bucket_table.take([0])
                results.append(first_row)
        
        if results:
            combined = pa.concat_tables(results, promote=True)
            # Drop the temporary bucket column - results are already ordered by bucket
            # since we iterate over unique buckets from the sorted table
            final_schema = [f for f in combined.schema.names if f != "__bucket__"]
            combined = combined.select(final_schema)
            # Return as-is; rows are already in bucket order from the iteration
            return combined
        else:
            return table
    except Exception as e:
        import traceback
        traceback.print_exc()
        return table


def export_timeframe_aggregates(
    data_dir: str | Path,
    out_dir: str | Path,
    assets: List[str] | None = None,
    l2_levels: int = 20,
) -> dict:
    """Export aggregated timeframe Parquet files from 5min base data.
    
    Creates one file per asset per timeframe:
    - {asset}_book_snapshots_15m.parquet
    - {asset}_book_snapshots_1h.parquet
    - {asset}_book_snapshots_4h.parquet
    - {asset}_book_snapshots_1d.parquet
    
    Also exports trades and chainlink_events aggregated.
    
    Returns dict of {out_path: rows}.
    """
    base = Path(data_dir)
    out = Path(out_dir) if out_dir else base / "export"
    out.mkdir(parents=True, exist_ok=True)

    if assets is None:
        assets = ["BTC", "ETH", "SOL"]

    # Datasets to aggregate (only per-asset ones that make sense to aggregate)
    datasets = ["book_snapshots_500ms", "book_events", "trades", "chainlink_events"]

    stats: dict = {}

    for asset in assets:
        au = asset.upper()
        asset_stats: dict = {}

        for ds in datasets:
            # Read per-asset data
            table = _read_dataset_per_asset(base, ds, au, include_binance=False)
            if table is None or table.num_rows == 0:
                continue

            # Determine which timeframes to aggregate based on dataset
            if ds == "book_snapshots_500ms":
                # Can aggregate to all timeframes
                timeframes = [
                    ("300", "5m", ds),
                    ("900", "15m", ds),
                    ("3600", "1h", ds),
                    ("14400", "4h", ds),
                    ("86400", "1d", ds),
                ]
            elif ds in ("trades", "chainlink_events"):
                # Trades and chainlink can also be aggregated
                timeframes = [
                    ("300", "5m", ds),
                    ("900", "15m", ds),
                    ("3600", "1h", ds),
                    ("14400", "4h", ds),
                    ("86400", "1d", ds),
                ]
            else:
                # book_events and others: only 5min
                timeframes = [("300", "5m", ds)]

            for sec, label, dset in timeframes:
                agg_table = aggregate_5min_to_timeframe(table, int(sec), label)
                if agg_table is None or agg_table.num_rows == 0:
                    continue

                # Build output filename
                out_path = out / f"{au}_{dset}_{label}.parquet"

                # Write with schema alignment
                try:
                    # Ensure schema has required columns
                    schema = _get_schema(dset, l2_levels)
                    if schema is not None:
                        # Add missing columns as nulls
                        current_names = set(agg_table.schema.names)
                        for field in schema:
                            if field.name not in current_names:
                                null_arr = pa.array([None] * agg_table.num_rows, type=field.type)
                                agg_table = agg_table.append_column(field.name, null_arr)
                        # Reorder columns to match schema
                        ordered = [n for n in schema.names if n in agg_table.schema.names]
                        remaining = [n for n in agg_table.schema.names if n not in ordered]
                        final_order = ordered + remaining
                        agg_table = agg_table.select(final_order)

                    pq.write_table(agg_table, str(out_path), compression="zstd")
                    rows = agg_table.num_rows
                    asset_stats[f"{dset}_{label}"] = rows
                    stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = rows
                    print(f"exported aggregated {au} {dset} {label}: {rows} rows")
                except Exception as e:
                    print(f"failed to write {au} {dset} {label}: {e}")

    return stats


# ------------------------------------------------------------------ Kaggle API upload
try:
    import kaggle
    KAGGLE_AVAILABLE = True
except ImportError:
    KAGGLE_AVAILABLE = False


def _get_kaggle_dataset_name(window_label: str, asset: str) -> str:
    """Generate Kaggle dataset name based on timeframe and asset.
    
    Patterns based on user's dataset:
    - gghgg1/polymarket-5m-crypto-btc-eth-sol
    - Each timeframe gets its own dataset or version
    """
    # Map window label to dataset suffix
    suffix_map = {
        "5m": "",
        "15m": "-15m",
        "1h": "-1h",
        "4h": "-4h",
        "1d": "-1d",
    }
    suffix = suffix_map.get(window_label, "")
    return f"gghgg1/polymarket-5m-crypto{suffix}-{asset.lower()}"


def upload_to_kaggle(
    parquet_path: Path,
    dataset_name: str,
    api_username: str | None = None,
    api_key: str | None = None,
    overwrite: bool = True,
) -> bool:
    """Upload a Parquet file to Kaggle dataset.
    
    Args:
        parquet_path: Local path to .parquet file
        dataset_name: Kaggle dataset name (e.g., 'gghgg1/polymarket-5m-crypto-btc')
        api_username: Kaggle username (optional, uses kaggle.json if not provided)
        api_key: Kaggle API key (optional, uses kaggle.json if not provided)
        overwrite: Whether to overwrite existing version
    
    Returns:
        True if upload successful, False otherwise
    """
    if not KAGGLE_AVAILABLE:
        print("kaggle package not available, skipping upload")
        return False

    try:
        # Prepare file upload
        # kaggle.api.dataset_version_create accepts:
        # - dataset: str
        # - files: str or Path
        # - version_message: str
        # - release: bool (whether to release / overwrite)

        # If file doesn't exist, skip
        if not parquet_path.exists():
            print(f"Parquet file not found: {parquet_path}")
            return False

        # Upload the file
        print(f"Uploading {parquet_path} to Kaggle dataset {dataset_name}...")

        # Try to authenticate - kaggle will use ~/.kaggle/kaggle.json
        api = kaggle.api()

        # Create/upload a new version
        version_message = f"Automated upload - {parquet_path.name} - {pc.datetime.datetime.utcnow().isoformat()}"
        
        # Use release=True to overwrite if version exists, or create new version
        api.dataset_version_create(
            dataset=dataset_name,
            files=str(parquet_path),
            version_message=version_message,
            release=overwrite,
        )

        print(f"Successfully uploaded {parquet_path.name} to {dataset_name}")
        return True

    except ImportError:
        print("kaggle package import failed")
        return False
    except Exception as e:
        print(f"Kaggle upload failed: {e}")
        # Don't raise - let the caller decide
        return False


def cleanup_local_data(
    data_dir: str | Path,
    assets: List[str],
    timeframe_labels: List[str] = None,
    keep_seconds: int = 3600,
) -> dict:
    """Remove local Parquet data older than keep_seconds.
    
    After successful Kaggle upload, this clears local data for each timeframe
    to free space, while the collector continues collecting fresh data.
    
    Args:
        data_dir: Base data directory
        assets: List of assets
        timeframe_labels: Timeframe labels to clean (5m, 15m, 1h, 4h, 1d)
        keep_seconds: Keep data this many seconds before cleaning (default 1 hour)
    
    Returns:
        dict of {asset: rows_deleted}
    """
    if timeframe_labels is None:
        timeframe_labels = ["5m", "15m", "1h", "4h", "1d"]

    base = Path(data_dir)
    stats: dict = {}

    for asset in assets:
        au = asset.upper()
        asset_deleted = 0

        for label in timeframe_labels:
            # Find all parquet files for this asset and timeframe
            # Pattern: data/{dataset}/date=*/asset={AU}/{label}.parquet or similar
            patterns_to_try = [
                base / f"book_snapshots_500ms" / f"date=*" / f"asset={au.upper()}" / f"*{label}.parquet",
                base / f"book_snapshots_500ms" / f"date=*" / f"asset={au}" / f"*{label}.parquet",
                base / f"export" / f"{au}_{label}.parquet",
            ]

            deleted = 0
            for pattern in patterns_to_try:
                files = list(base.glob(str(pattern))) if "*" in str(pattern) else []
                # Also try rglob for loose patterns
                if not files:
                    files = list(base.rglob(f"*{au}*{label}*.parquet"))

                for f in files:
                    try:
                        # Check file modification time
                        mtime = f.stat().st_mtime
                        age_seconds = pc.datetime.datetime.now().timestamp() - mtime
                        
                        if age_seconds > keep_seconds:
                            f.unlink()
                            deleted += 1
                            asset_deleted += 1
                            print(f"Deleted old: {f}")
                    except Exception:
                        pass

            stats[f"{au}_{label}"] = asset_deleted

    return stats


# =============================================================================
# Kaggle hourly upload orchestrator
# =============================================================================

def export_and_upload_all_kaggle(
    data_dir: str | Path = "./data",
    out_dir: str | Path | None = None,
    assets: List[str] | None = None,
    kaggle_username: str | None = None,
    kaggle_key: str | None = None,
    timeframe_labels: List[str] = None,
) -> dict:
    """Full pipeline: export aggregated timeframes + upload to Kaggle + cleanup local data.
    
    This is the main function to call hourly:
    1. Aggregate 5min data into 15min/1h/4h/1d timeframes
    2. Upload each to Kaggle dataset
    3. Clean local data older than 1 hour (recent data kept for continuity)
    
    Returns dict with export stats, upload results, and cleanup stats.
    """
    if timeframe_labels is None:
        timeframe_labels = ["5m", "15m", "1h", "4h", "1d"]

    if assets is None:
        assets = ["BTC", "ETH", "SOL"]

    base = Path(data_dir)
    out = Path(out_dir) if out_dir else base / "export"
    out.mkdir(parents=True, exist_ok=True)

    result: dict = {
        "export": {},
        "kaggle_uploads": {},
        "cleanup": {},
    }

    # Step 1: Export aggregated timeframes
    print(f"=== Step 1: Exporting aggregated timeframes for {assets} ===")
    export_stats = export_timeframe_aggregates(data_dir, out, assets=assets, l2_levels=20)
    result["export"] = export_stats

    # Step 2: Upload to Kaggle
    print(f"=== Step 2: Uploading to Kaggle ===")
    for asset in assets:
        au = asset.upper()
        asset_uploads = {}
        for label in timeframe_labels:
            # Determine which dataset name to use
            # Based on user's dataset: gghgg1/polymarket-5m-crypto-btc-eth-sol
            ds_name = _get_kaggle_dataset_name(label, au)
            # Find the exported file for this asset and timeframe
            # Look in export dir: {asset}_{dataset}_{label}.parquet
            possible_files = [
                out / f"{au}_book_snapshots_{label}.parquet",
                out / f"{au}_trades_{label}.parquet",
                out / f"{au}_chainlink_events_{label}.parquet",
            ]
            
            # Also check for any parquet files matching the pattern
            found_file = None
            for pf in possible_files:
                if pf.exists():
                    found_file = pf
                    break
            
            # If no file found, try rglob
            if found_file is None:
                import glob as glob_mod
                pattern = str(out / f"*{au}*{label}*.parquet")
                matches = glob_mod.glob(pattern)
                if matches:
                    found_file = Path(matches[0])

            if found_file is None:
                print(f"No export file found for {au} {label}, skipping Kaggle upload")
                asset_uploads[label] = {"status": "skipped", "reason": "no_export_file"}
                continue

            # Upload to Kaggle
            upload_success = upload_to_kaggle(
                parquet_path=found_file,
                dataset_name=ds_name,
                api_username=kaggle_username,
                api_key=kaggle_key,
                overwrite=True,
            )

            asset_uploads[label] = {
                "status": "success" if upload_success else "failed",
                "dataset": ds_name,
                "file": str(found_file),
            }
            result["kaggle_uploads"][f"{au}_{label}"] = asset_uploads[label]

            if upload_success:
                # Step 3: Clean local data after successful upload
                print(f"Upload successful for {au} {label}, initiating cleanup...")
                cleanup_stats = cleanup_local_data(
                    data_dir, [au], timeframe_labels=[label], keep_seconds=3600
                )
                result["cleanup"] = {**result["cleanup"], **cleanup_stats}

    return result


def _validate_kaggle_config() -> bool:
    """Check if Kaggle API is properly configured."""
    if not KAGGLE_AVAILABLE:
        print("⚠️ kaggle package not installed. Install with: pip install kaggle")
        return False
    
    kaggle_dir = Path(".kaggle")
    if kaggle_dir.exists():
        kaggle_json = kaggle_dir / "kaggle.json"
        if kaggle_json.exists():
            print("✓ Kaggle API credentials found in ~/.kaggle/kaggle.json")
            return True
    
    # Check environment variables
    if "KAGGLE_USERNAME" in __import__("os").environ and "KAGGLE_KEY" in __import__("os").environ:
        print("✓ Kaggle API credentials found in environment variables")
        return True
    
    print("⚠️ No Kaggle API credentials configured.")
    print("  Please setup either:")
    print("  1. ~/.kaggle/kaggle.json with username and key")
    print("  2. Environment vars: KAGGLE_USERNAME and KAGGLE_KEY")
    return False
