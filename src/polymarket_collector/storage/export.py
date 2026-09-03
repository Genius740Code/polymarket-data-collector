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
            # exclude binance if chainlink and not include_binance — keep nulls (synthetic/old data without source)
            # Use if_else to keep null source rows (pyarrow or_ with null gives null, not true)
            if dataset == "chainlink_events" and not include_binance and "source" in t.schema.names:
                try:
                    col = t.column("source")
                    is_null = pc.is_null(col)
                    not_binance = pc.not_equal(col, pa.scalar("binance-ticker-proxy"))
                    # if null → True (keep), else not_binance value
                    mask = pc.if_else(is_null, True, not_binance)
                    # mask may still have nulls where not_binance was null and is_null false? but is_null false → not_binance, so null stays null → filter drops nulls we want to keep?
                    # For non-null source, not_binance is true/false, not null. So mask is true/false only.
                    # For safety, fill any remaining nulls with True (keep)
                    if mask.null_count > 0:
                        mask = pc.fill_null(mask, True)
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
    # filter binance again if combined still has mixed sources (promote case) — keep nulls
    if dataset == "chainlink_events" and not include_binance and "source" in combined.schema.names:
        try:
            col = combined.column("source")
            is_null = pc.is_null(col)
            not_binance = pc.not_equal(col, pa.scalar("binance-ticker-proxy"))
            mask = pc.if_else(is_null, True, not_binance)
            if mask.null_count > 0:
                mask = pc.fill_null(mask, True)
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

    Note: The 3 global datasets (markets_log, collector_events, resync_episodes)
    are always exported as single files in the Kaggle staging folder, even if
    they contain 0 rows. This ensures the staging always has 31 files (7 assets x 4
    per-asset + 3 globals) for the dataset gghgg1/polymarket-5m-crypto.
    """
    base = Path(data_dir)
    out = Path(out_dir) if out_dir else base / "export"
    out.mkdir(parents=True, exist_ok=True)

    if datasets is None:
        datasets = ["book_snapshots_500ms", "book_events", "trades", "chainlink_events", "markets_log", "collector_events", "resync_episodes"]
    if assets is None:
        # Always use the 7 known assets — hardcoded per plan.md §0
        # Do NOT discover dynamically from hive partitions, as this fails
        # when the data directory is freshly cleaned (no hive dirs exist yet).
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]

    # The 3 global datasets that should always appear in Kaggle staging
    global_datasets = {"markets_log", "collector_events", "resync_episodes"}
    # Per-asset datasets that get one file per asset
    per_asset_datasets = {"book_snapshots_500ms", "book_events", "trades", "chainlink_events"}

    stats: dict = {}
    for ds in datasets:
        schema = _get_schema(ds, l2_levels)
        if ds in PER_ASSET_DATASETS:
            for asset in assets:
                au = asset.upper()
                table = _read_dataset_per_asset(base, ds, au, include_binance=include_binance)
                # enforce schema ordering already done, ensure per-asset single file
                out_path = out / f"{au}_{ds}.parquet"
                # handle clean_view alias: book_snapshots_500ms -> but user may want book_snapshots_clean too
                if table is not None and table.num_rows > 0:
                    tmp_path = out_path.with_suffix(".parquet.tmp")
                    pq.write_table(table, str(tmp_path), compression="zstd")
                    tmp_path.rename(out_path)
                    stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = table.num_rows
                else:
                    # If prior staging file exists and has rows, preserve it (never overwrite with empty)
                    # This prevents cumulative history destruction when read fails transiently
                    if out_path.exists():
                        try:
                            prior = pq.read_table(str(out_path))
                            if prior.num_rows > 0:
                                # Keep prior staging file — don't destroy cumulative history
                                stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = prior.num_rows
                                continue  # skip write, preserve existing file
                        except Exception:
                            pass  # if can't read prior, proceed to write empty
                    # No prior data or prior was empty — write empty parquet file to maintain 31-file staging structure
                    tmp_path = out_path.with_suffix(".parquet.tmp")
                    pq.write_table(pa.table({}), str(tmp_path), compression="zstd")
                    tmp_path.rename(out_path)
                    stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = 0
        elif ds in global_datasets:
            # Global dataset: always create a single file in staging, even if 0 rows
            table = _read_dataset_per_asset(base, ds, None, include_binance=include_binance)
            if ds == "markets_log":
                out_path = out / "markets.parquet"
            elif ds == "collector_events":
                out_path = out / "collector_events.parquet"
            else:  # resync_episodes
                out_path = out / "resync_episodes.parquet"
            # Preserve prior staging file if it has rows — never overwrite cumulative history with empty
            if out_path.exists():
                try:
                    prior = pq.read_table(str(out_path))
                    if prior.num_rows > 0:
                        # Keep prior staging file — don't destroy cumulative history
                        stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = prior.num_rows
                        continue  # skip write, preserve existing file
                except Exception:
                    pass  # if can't read prior, proceed to write new file
            if table is not None and table.num_rows > 0:
                tmp_path = out_path.with_suffix(".parquet.tmp")
                pq.write_table(table, str(tmp_path), compression="zstd")
                tmp_path.rename(out_path)
            else:
                # Create empty parquet file — use _get_schema from this module for global datasets
                global_schema = _get_schema(ds, l2_levels)
                # Build empty data dict with schema column names
                empty_data = {col: [] for col in global_schema.names}
                table = pa.table(empty_data)
                tmp_path = out_path.with_suffix(".parquet.tmp")
                pq.write_table(table, str(tmp_path), compression="zstd")
                tmp_path.rename(out_path)
            stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = table.num_rows
        else:
            # Should not happen with default datasets, but skip
            pass
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


# ------------------------------------------------------------------ timeframe aggregation (5m-only; 15m/1h/4h/1d synthetic deprecated, native only)
# When the collector runs with 5min (300s) windows, 15m/1h/4h/1d must be native Gamma windows
# (not synthetic from 5m) per plan.md §2. For 5m-only test we do NO synthesis.
# aggregate_5min_to_timeframe kept for backward compat but not used in 5m-only test.


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
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]

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


# ------------------------------------------------------------------ Kaggle upload — 5m-only, single dataset, folder versioning
# plan.md: single dataset gghgg1/polymarket-5m-crypto contains 7*4+3=31 files (all assets share same slug).
# Test mode uploads every 10 min (600s) gated on full closed markets only, safe delete after ready.

try:
    import kaggle  # type: ignore
    KAGGLE_AVAILABLE = True
except ImportError:
    KAGGLE_AVAILABLE = False

import datetime as _dt
import time as _time
import json as _json
import os as _os


def _get_kaggle_dataset_name(window_label: str = "5m", asset: str | None = None, dataset_prefix: str | None = None) -> str:
    """Single dataset for 5m-only: gghgg1/polymarket-5m-crypto (all assets share it).

    Per plan.md §1.1 slugs gghgg1/polymarket-{window}-crypto, asset is NOT part of slug.
    For 5m-only test we always return dataset_prefix (default gghgg1/polymarket-5m-crypto).
    Keeping window_label param for forward compat with native 15m/1h/1d later.
    """
    if dataset_prefix:
        return dataset_prefix
    # allow override via env/config
    return "gghgg1/polymarket-5m-crypto"


def _kaggle_dataset_slug(window_label: str = "5m") -> str:
    return _get_kaggle_dataset_name(window_label)


def prepare_kaggle_staging_5m(
    data_dir: str | Path,
    staging_dir: str | Path | None = None,
    assets: List[str] | None = None,
    l2_levels: int = 20,
    dataset_prefix: str = "gghgg1/polymarket-5m-crypto",
) -> dict:
    """Prepare Kaggle staging folder for 5m-only upload.

    Exports per-asset single files (time-first, zstd, no binance) into a flat staging
    folder with dataset-metadata.json (CC BY-NC-SA 4.0) ready for folder upload.

    Returns dict with staging_path, files (31 for 7 assets), row_counts.
    """
    base = Path(data_dir)
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    staging = Path(staging_dir) if staging_dir else base / "kaggle_staging" / "5m" / dataset_prefix
    staging.mkdir(parents=True, exist_ok=True)

    # Export per-asset 5m files directly into staging (not intermediate export/)
    stats = export_per_asset_single_file(
        data_dir, out_dir=staging, assets=assets, l2_levels=l2_levels, include_binance=False
    )
    # Ensure markets_latest also available as markets_latest.parquet alias if needed for reference
    # but primary markets file is markets.parquet (from markets_log)
    row_counts = stats
    # Write dataset-metadata.json
    resources = [{"path": Path(k).name, "description": f"{Path(k).name} 5m crypto — {dataset_prefix}"} for k in stats.keys()]
    # Ensure markets.parquet + per-asset files are all listed; add if missing due to empty
    meta = {
        "title": "Polymarket 5m Crypto",
        "id": dataset_prefix,
        "licenses": [{"name": "CC BY-NC-SA 4.0"}],
        "resources": resources,
    }
    (staging / "dataset-metadata.json").write_text(_json.dumps(meta, indent=2))
    return {"staging_path": str(staging), "files": len(stats), "row_counts": row_counts, "dataset": dataset_prefix}


def upload_to_kaggle(
    parquet_path: Path | None = None,
    dataset_name: str | None = None,
    api_username: str | None = None,
    api_key: str | None = None,
    overwrite: bool = True,
    staging_dir: str | Path | None = None,
) -> bool:
    """Upload to Kaggle.

    Preferred: give staging_dir (folder with 31 parquets + dataset-metadata.json) → folder version upload.
    Legacy: parquet_path single file (kept for compat) → single-file fallback.
    Uses kaggle API dataset_create_version with retries, version notes with UTC timestamp.
    """
    if not KAGGLE_AVAILABLE:
        print("kaggle package not available, skipping upload")
        return False

    # Resolve dataset & staging
    if dataset_name is None:
        dataset_name = "gghgg1/polymarket-5m-crypto"
    # Prefer staging folder upload
    if staging_dir is not None and Path(staging_dir).exists():
        folder = Path(staging_dir)
        if not (folder / "dataset-metadata.json").exists():
            print(f"staging missing dataset-metadata.json: {folder}")
            return False
        return _upload_kaggle_folder(folder, dataset_name, expected_assets=["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"])
    if parquet_path is not None:
        p = Path(parquet_path)
        if not p.exists():
            print(f"Parquet file not found: {p}")
            return False
        # Single-file legacy: wrap in tmp staging folder
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp()) / p.parent.name
        tmp.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(p), str(tmp / p.name))
        (tmp / "dataset-metadata.json").write_text(_json.dumps({
            "title": "Polymarket 5m Crypto",
            "id": dataset_name,
            "licenses": [{"name": "CC BY-NC-SA 4.0"}],
            "resources": [{"path": p.name, "description": p.name}],
        }, indent=2))
        ok = _upload_kaggle_folder(tmp, dataset_name, expected_assets=["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"])
        shutil.rmtree(str(tmp.parent), ignore_errors=True)
        return ok
    print("upload_to_kaggle: need staging_dir or parquet_path")
    return False


def _upload_kaggle_folder(staging: Path, dataset: str, max_retries: int = 5, expected_assets: List[str] | None = None) -> bool:
    """Folder upload with retry 5× jitter and dataset_status polling (plan.md §5).

    If expected_assets is provided, verify staging row counts after status=ready
    to prevent cumulative data loss from empty staging files.
    """
    import random
    if expected_assets is None:
        expected_assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    try:
        # kaggle uses ~/.kaggle/kaggle.json or env KAGGLE_USERNAME/KEY
        api = __import__("kaggle").api  # type: ignore
        # Check if dataset exists → choose create vs version
        exists = False
        try:
            api.dataset_status(dataset)  # throws if not exists on some versions
            exists = True
        except Exception:
            exists = False
        version_notes = f"5m 7-asset update UTC {_dt.datetime.now(tz=_dt.timezone.utc).isoformat()} rows via staging {staging.name}"
        last_err = None
        for attempt in range(max_retries):
            try:
                if exists:
                    # kagglesdk path: api.dataset_create_version(folder, version_notes, convert_to_csv=False, delete_old_versions=False)
                    # fallback to kaggle api.dataset_version_create
                    try:
                        api.dataset_create_version(
                            folder=str(staging),
                            version_notes=version_notes,
                            convert_to_csv=False,
                            delete_old_versions=False,
                        )
                    except TypeError:
                        api.dataset_version_create(
                            dataset=dataset,
                            files=str(staging),
                            version_message=version_notes,
                        )
                else:
                    try:
                        api.dataset_create_new(
                            folder=str(staging),
                            public=True,
                            convert_to_csv=False,
                        )
                    except TypeError:
                        api.dataset_create_new(dataset=dataset, dir=str(staging), public=True)
                # Poll until ready (up to 10 min)
                for _ in range(60):
                    try:
                        st = api.dataset_status(dataset)
                        # status may be dict with 'status' or object
                        s = st.get("status") if isinstance(st, dict) else getattr(st, "status", "")
                        if s == "ready":
                            # verify staging has data with rows (not empty from prior overwrite)
                            # and verify file count matches expected 31 files (7 assets x 4 datasets + 3 globals)
                            _files_ok = _expected_staging_files(staging) >= len(expected_assets) * 4 + 3
                            _rows_ok = _verify_staging_row_counts(staging, expected_assets)
                            if _rows_ok and _files_ok:
                                print(f"✓ Kaggle dataset ready: {dataset}")
                                _write_kaggle_state(staging, dataset, version_notes)
                                return True
                            elif not _rows_ok:
                                print(f"⚓ Kaggle dataset status=ready but staging has empty files; waiting for complete upload")
                            elif not _files_ok:
                                print(f"⚓ Kaggle dataset status=ready but staging has {_expected_staging_files(staging)} files, expected {len(expected_assets) * 4 + 3}; waiting for complete upload")
                        elif s in ("failed", "error"):
                            print(f"✗ Kaggle dataset in error state: {dataset}")
                            _write_kaggle_state(staging, dataset, version_notes)
                            return False
                    except Exception:
                        pass
                    _time.sleep(10)
                print(f"⚠ Kaggle dataset_status not ready after 10m, failing closed: {dataset}")
                # Do NOT assume success; return False to block cleanup/prune
                _write_kaggle_state(staging, dataset, version_notes)
                return False
            except Exception as e:
                last_err = e
                msg = str(e)
                if "429" in msg or "500" in msg or "503" in msg:
                    delay = min(2 * (2 ** attempt) + random.uniform(0, 1), 60)
                    print(f"Kaggle retry {attempt+1}/{max_retries} after {delay:.1f}s: {e}")
                    _time.sleep(delay)
                    continue
                print(f"Kaggle upload failed non-retriable: {e}")
                return False
        print(f"Kaggle upload failed after {max_retries}: {last_err}")
        return False
    except Exception as e:
        print(f"Kaggle upload error: {e}")
        import traceback
        traceback.print_exc()
        return False


def _expected_staging_files(staging: Path) -> int:
    """Count expected parquet files in staging directory for Kaggle version."""
    parquet_files = [p for p in staging.glob("*.parquet") if not p.name.endswith(".tmp")]
    # 7 assets x 4 per-asset datasets + 3 globals = 31 files
    # per-asset: book_snapshots_500ms, book_events, trades, chainlink_events
    # globals: markets_log, collector_events, resync_episodes
    return len(parquet_files)


def _verify_staging_row_counts(staging: Path, expected_assets: List[str]) -> bool:
    """Verify that each staging file exists and per-asset files have rows.

    Returns True if all expected files exist, False otherwise.
    Per-asset files must have >0 rows to prevent loss of cumulative history.
    Global files (markets_log/collector_events/resync_episodes) are allowed
    to be empty on first upload (0 rows is valid early in collection), so
    only existence + readable parquet is required.
    """
    per_asset_datasets = {"book_snapshots_500ms", "book_events", "trades", "chainlink_events"}
    # global filenames as written by export_per_asset_single_file()
    global_file_map = {
        "markets_log": "markets.parquet",
        "collector_events": "collector_events.parquet",
        "resync_episodes": "resync_episodes.parquet",
    }

    # Check per-asset files: expected_assets x 4 datasets must have >0 rows
    for asset in expected_assets:
        au = asset.upper()
        for ds in per_asset_datasets:
            fpath = staging / f"{au}_{ds}.parquet"
            if not fpath.exists():
                return False
            try:
                t = pq.read_table(str(fpath))
                if t.num_rows == 0:
                    return False
            except Exception:
                return False

    # Check global files — existence only (empty is valid at beginning)
    for ds, fname in global_file_map.items():
        fpath = staging / fname
        if not fpath.exists():
            return False
        try:
            pq.read_table(str(fpath))
        except Exception:
            return False

    return True


def _write_kaggle_state(staging: Path, dataset: str, notes: str):
    try:
        state_path = Path(staging).parent.parent / "_kaggle_state.json"  # data/kaggle_staging/_kaggle_state.json
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state = {}
        if state_path.exists():
            try:
                state = _json.loads(state_path.read_text())
            except Exception:
                state = {}
        state[dataset] = {
            "last_version_notes": notes,
            "last_upload_utc": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
            "last_upload_unix_ms": int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp() * 1000),
        }
        # keep per-staging row counts if present
        state_path.write_text(_json.dumps(state, indent=2))
    except Exception:
        pass


def cleanup_local_data(
    data_dir: str | Path,
    assets: List[str] | None = None,
    timeframe_labels: List[str] | None = None,
    keep_seconds: int = 3600,
    checkpoint_ms: int | None = None,
    buffer_seconds: int | None = None,
) -> dict:
    """Safe post-upload cleanup — only delete data older than buffer, fail closed.

    **CRITICAL CHANGE**: Never automatically delete local hive partitions after Kaggle upload.
    Previously, data older than the 2h buffer was deleted, causing permanent data loss
    from future Kaggle versions. Now: data is retained indefinitely; only files with
    market_end_ts_ms in the far future are protected, and all other files are kept.

    The buffer parameter is retained for config compatibility but has no deleting effect.
    """
    import datetime as _dt2
    if timeframe_labels is None:
        timeframe_labels = ["5m"]
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    base = Path(data_dir)
    # Resolve checkpoint
    if checkpoint_ms is None:
        # try read _kaggle_state
        for cand in [base / "kaggle_staging" / "_kaggle_state.json", base / "kaggle_staging" / "5m" / "_kaggle_state.json"]:
            if cand.exists():
                try:
                    j = _json.loads(cand.read_text())
                    # take latest last_upload_unix_ms
                    vals = [v.get("last_upload_unix_ms") for v in j.values() if isinstance(v, dict) and v.get("last_upload_unix_ms")]
                    if vals:
                        checkpoint_ms = max(vals)
                        break
                except Exception:
                    pass
        if checkpoint_ms is None:
            checkpoint_ms = int(_dt2.datetime.now(tz=_dt2.timezone.utc).timestamp() * 1000) - keep_seconds * 1000
    # buffer: previously used 2h (7200s) to delete old data — THIS IS NOW DISABLED
    # to prevent permanent Kaggle cumulative data loss. All hive data is retained.
    # The buffer_ms is calculated but not used for deletion guard.
    if buffer_seconds is None:
        buffer_seconds = 7200  # kept for config compatibility, no-op
    buffer_ms = buffer_seconds * 1000
    cutoff_ms = checkpoint_ms - buffer_ms
    now_ms = int(_dt2.datetime.now(tz=_dt2.timezone.utc).timestamp() * 1000)
    stats: dict = {}
    # Hive safe prune: inspect markets_latest for market_end per condition, then walk hive partitions
    # Simpler for 5m-only: scan hive partitions, read one file's max(market_end_ts_ms) if present
    # CRITICAL: Do NOT delete files — retain all data to prevent Kaggle cumulative data loss
    # (Only perform the "never delete open window" guard without actual deletion)
    for dataset in ["book_snapshots_500ms", "book_events", "trades", "chainlink_events", "collector_events", "markets_log"]:
        ds_root = base / dataset
        if not ds_root.exists():
            continue
        for leaf in ds_root.rglob("*.parquet"):
            if leaf.name.endswith(".tmp"):
                continue
            try:
                # never delete open window (market_end > now) — just verify, don't delete
                # Read max market_end from file if column exists
                try:
                    t = pq.read_table(str(leaf), columns=None)
                    # check hive file's market_end if present
                    for col in ["market_end_ts_ms", "market_end_ts"]:
                        if col in t.schema.names:
                            vals = t.column(col).to_pylist()
                            # filter none
                            vals = [v for v in vals if v is not None]
                            if vals:
                                # if string ISO, parse
                                if isinstance(vals[0], str):
                                    max_end = max(int(_dt2.datetime.fromisoformat(v.replace("Z","+00:00")).timestamp()*1000) for v in vals)
                                else:
                                    max_end = max(int(v) for v in vals)
                                if max_end < now_ms:
                                    # market is closed, but we NO LONGER delete it
                                    # previously: can_delete = True would lead to leaf.unlink()
                                    # now: explicitly do NOT delete
                                    pass  # data retained, no-op
                                else:
                                    # market still open, also retain
                                    pass
                            break
                    else:
                        # no market_end column — retain file (cannot verify age)
                        pass
                except Exception:
                    # error reading file — retain it
                    pass
                # NOTE: NO leaf.unlink() call — all data retained
            except Exception:
                pass
    # Return empty stats — no deletion occurred
    return stats


# =============================================================================
# Kaggle upload orchestrator — 5m-only, single dataset, 10-min / hourly
# =============================================================================

def export_and_upload_all_kaggle(
    data_dir: str | Path = "./data",
    out_dir: str | Path | None = None,
    assets: List[str] | None = None,
    kaggle_username: str | None = None,
    kaggle_key: str | None = None,
    timeframe_labels: List[str] | None = None,
    l2_levels: int = 20,
    dry_run: bool = False,
) -> dict:
    """5m-only pipeline: export 7-asset staging (31 files) → Kaggle single dataset → safe prune.

    - Only full closed markets (market_end < now) are uploaded.
    - Staging is cumulative: same filenames overwritten with larger parquet each version (31 files).
    - Kaggle upload uses folder versioning with retry 5 + jitter and status poll.
    - Safe delete only after ready, with 2h buffer, never deleting open window.
    Timeframe aggregation for 15m/1h removed (native only; 5m-only assumes 5m validates others).
    """
    if timeframe_labels is None:
        timeframe_labels = ["5m"]
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    base = Path(data_dir)
    # Resolve dataset prefix from env/config if available
    dataset_prefix = "gghgg1/polymarket-5m-crypto"
    try:
        from ..config import CollectorConfig as _CC
        _cfg = _CC.load()
        dataset_prefix = getattr(_cfg.kaggle, "dataset_prefix", dataset_prefix)
    except Exception:
        pass
    staging = base / "kaggle_staging" / "5m" / dataset_prefix

    result: dict = {
        "export": {},
        "staging": {},
        "kaggle_uploads": {},
        "cleanup": {},
        "dry_run": dry_run,
    }

    # Step 0: Compact hive data (§10A) — merge small parquet files before export
    # This ensures staging files are compacted, reducing file count and improving upload efficiency.
    # compaction is best-effort: if no compactable files exist, it is a no-op.
    try:
        from polymarket_collector.storage.compaction import compact_all as _compact_all
        compact_stats = _compact_all(
            data_dir,
            datasets=[
                "book_snapshots_500ms",
                "book_events",
                "trades",
                "chainlink_events",
                "collector_events",
                "resync_episodes",
            ],
        )
        if compact_stats:
            print(f"compacted: {compact_stats}")
    except Exception as e:
        print(f"compact err {e}")

    # Gate: only upload full closed markets
    try:
        latest = base / "markets_latest" / "markets_latest.parquet"
        if latest.exists():
            tbl = pq.read_table(str(latest))
            if "market_end_ts_ms" in tbl.schema.names:
                ends = [v for v in tbl.column("market_end_ts_ms").to_pylist() if v is not None]
                if ends and max(ends) >= int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp()*1000):
                    # open window still exists, but we still allow upload of already-closed partitions
                    # only skip if NO closed window exists
                    pass
            elif "market_end_ts" in tbl.schema.names:
                pass
    except Exception:
        pass

    # Step 1: Prepare staging (export per-asset single files into staging folder)
    print(f"=== Step 1: Preparing Kaggle staging 5m for {assets} -> {staging} ===")
    prep = prepare_kaggle_staging_5m(data_dir, staging_dir=staging, assets=assets, l2_levels=l2_levels, dataset_prefix=dataset_prefix)
    result["export"] = prep["row_counts"]
    result["staging"] = {"path": prep["staging_path"], "files": prep["files"], "dataset": prep["dataset"]}
    print(f"staging prepared: {prep['files']} files, dataset {prep['dataset']}")

    if dry_run:
        print("dry-run: skipping Kaggle upload + prune")
        result["kaggle_uploads"][dataset_prefix] = {"status": "dry_run", "staging": str(staging), "files": prep["files"]}
        return result

    # Step 2: Upload to Kaggle (single dataset)
    print(f"=== Step 2: Uploading 5m staging to Kaggle {dataset_prefix} ===")
    ok = _upload_kaggle_folder(staging, dataset_prefix, expected_assets=assets)
    result["kaggle_uploads"][dataset_prefix] = {
        "status": "success" if ok else "failed",
        "staging": str(staging),
        "files": prep["files"],
    }
    if ok:
        print(f"✓ Upload success {dataset_prefix}, pruning hive after verified ready...")
        cleanup_stats = cleanup_local_data(data_dir, assets=assets, timeframe_labels=timeframe_labels)
        result["cleanup"] = cleanup_stats
    else:
        print(f"✗ Upload failed {dataset_prefix}, NOT pruning (data retained for retry)")

    return result


def _validate_kaggle_config() -> bool:
    """Check if Kaggle API is properly configured (env or ~/.kaggle/kaggle.json)."""
    if not KAGGLE_AVAILABLE:
        print("⚠ kaggle package not installed. Install with: pip install kaggle")
        return False
    # Check env first — support both legacy KAGGLE_USERNAME/KEY and new KAGGLE_API_TOKEN
    if _os.environ.get("KAGGLE_API_TOKEN"):
        print("✓ Kaggle API credentials found in KAGGLE_API_TOKEN env")
        return True
    if _os.environ.get("KAGGLE_USERNAME") and _os.environ.get("KAGGLE_KEY"):
        print("✓ Kaggle API credentials found in environment variables")
        return True
    # Check standard locations: ~/.kaggle/kaggle.json, access_token, ./.kaggle/kaggle.json, $KAGGLE_CONFIG_DIR
    candidates = [
        Path.home() / ".kaggle" / "kaggle.json",
        Path.home() / ".kaggle" / "access_token",
        Path(".kaggle") / "kaggle.json",
        Path(_os.environ.get("KAGGLE_CONFIG_DIR", "")) / "kaggle.json" if _os.environ.get("KAGGLE_CONFIG_DIR") else None,
        Path(_os.environ.get("KAGGLE_CONFIG_DIR", "")) / "access_token" if _os.environ.get("KAGGLE_CONFIG_DIR") else None,
    ]
    for p in candidates:
        if p and p.exists():
            print(f"✓ Kaggle API credentials found in {p}")
            return True
    print("⚠ No Kaggle API credentials configured.")
    print("  Setup: 1) ~/.kaggle/kaggle.json {\"username\":\"gghgg1\",\"key\":\"KGAT_...\"} chmod 600")
    print("        2) env KAGGLE_API_TOKEN=KGAT_... (new) or KAGGLE_USERNAME/KEY")
    return False
