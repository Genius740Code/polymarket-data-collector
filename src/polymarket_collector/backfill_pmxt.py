"""PMXT archive history backfill — pre-cutover windows only.

History rule (docs/NEW_COLLECTOR_PERFECT_SPEC.md section 0): nothing before
the cutover is invented. Live ticks missed by the collector stay gaps; only
windows covered by the PMXT hourly archive
(``clarkpalmer/pmxt-data-ingestion`` pattern: download hourly archive,
filter, enrich trades/resolutions/strikes) are backfilled.

Safety:

- Read-only on live state: ``data/kaggle_staging``, ``data/_wal`` and live
  hive files are never modified, moved or pruned by this module.
- Backfill rows land in the same hive dataset/partition directories (so
  :mod:`polymarket_collector.export_pmdata` picks them up with no code
  change) but in uniquely named ``backfill_pmxt-*.parquet`` files only.
  Existing files are never overwritten.
- Every backfilled row carries ``source='backfill_pmxt'`` distinctly, so
  live rows and backfilled rows are separable downstream.
- Idempotent re-runs: dedup keys per dataset are checked against live AND
  previously backfilled files before every write; a re-run writes 0 rows
  when there is nothing new.
- Offline-capable: the DuckDB filter is an optional fast path; every read
  falls back to the local pyarrow reader. Network is used only by the
  explicit archive download helper and is never required by the tests.
- Real-data-only: invalid rows (out-of-range prices, negative sizes,
  missing required ids) are dropped and counted loudly — never adjusted.
  Clocks absent from the archive stay NULL — the receive clock is never
  used as a stand-in for the event clock.

Dedup keys (stable across re-runs):

- trades: ``trade_id``, else ``(tx_hash, token_id, ts_source, price, size)``.
- book_events: ``event_id``, else
  ``(condition_id, token_id, ts_source, event_type, new_best_bid,
  new_best_ask)``.
- book_snapshots_500ms: ``snapshot_id``, else
  ``(condition_id, ts_snapshot_ns)``.
- onchain_fills: ``(tx_hash, token_id)``.
- chainlink_events: ``event_id``, else ``(asset, ts_source, price)``.
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import os as _os
import time as _time
import urllib.request as _urlreq
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .storage.parquet_io import read_table

BACKFILL_SOURCE = "backfill_pmxt"
BACKFILL_PREFIX = "backfill_pmxt-"

TICK_DATASETS = (
    "book_snapshots_500ms",
    "book_events",
    "trades",
    "chainlink_events",
    "onchain_fills",
)

# Sidecar dataset for official outcomes recovered from the archive. Kept out
# of the live markets hive on purpose: promoting these rows into
# markets_latest/markets_log is a separate operator step (see
# resolution_backfill.py). Readers that want archive outcomes join on
# condition_id.
RESOLUTIONS_SIDECAR_DATASET = "_backfill_pmxt_resolutions"


# -- clocks ------------------------------------------------------------------


def coerce_ts_source_ms(v: Any) -> Optional[int]:
    """Epoch-ms int from int/float/numeric-string clocks, else None.

    Sub-1e12 values are seconds-epoch and are scaled. Bools, empties and
    unparsable values stay NULL (never guessed). Never raises.
    """
    try:
        if v is None or isinstance(v, bool):
            return None
        f = float(v) if not isinstance(v, (int, float)) else float(v)
        if f != f:
            return None
        return int(f) if f > 1e11 else int(f * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def _coerce_ns(v: Any) -> Optional[int]:
    """ns-epoch int from a collector/backfilled clock value, else None."""
    try:
        if v is None or isinstance(v, bool):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def _utc_day_str(ms: Any) -> Optional[str]:
    """UTC YYYY-MM-DD for epoch-ms, else None. Never raises."""
    try:
        ms_i = coerce_ts_source_ms(ms)
        if ms_i is None:
            return None
        return _dt.datetime.fromtimestamp(
            ms_i / 1000, tz=_dt.timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _iso_to_ms(s: Any) -> Optional[int]:
    """Epoch-ms for an ISO8601 string, else None. Never raises."""
    try:
        if not s or not isinstance(s, str):
            return None
        dt = _dt.datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except (ValueError, OverflowError):
        return None


def _now_utc_iso() -> str:
    try:
        return _dt.datetime.now(
            tz=_dt.timezone.utc).isoformat(timespec="milliseconds")
    except (ValueError, OverflowError):
        return "1970-01-01T00:00:00.000+00:00"


# -- atomic writes ------------------------------------------------------------


def _os_replace_safe(src: Path, dst: Path) -> None:
    _os.replace(str(src), str(dst))


def _atomic_write_table(table: pa.Table, path: Path) -> bool:
    """zstd parquet write via tmp+rename. Returns False (never raises)."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.parent / (path.name + ".tmp")
        pq.write_table(table, str(tmp), compression="zstd")
        _os_replace_safe(tmp, path)
        return True
    except (OSError, ValueError, pa.ArrowException) as e:
        print(f"[backfill_pmxt] WARN write failed {path.name}: {e}")
        return False


# -- archive download ----------------------------------------------------------


def download_pmxt_hour_archive(
    hour_str: str,
    dest_dir: str | Path,
    base_url: Optional[str] = None,
    fetcher: Any = None,
) -> Optional[Path]:
    """Fetch one hourly PMXT archive file into ``dest_dir`` (idempotent).

    ``hour_str`` is UTC ``YYYY-MM-DDTHH``. When the destination file already
    exists it is returned untouched. ``base_url`` is a format string taking
    ``{hour}`` (no default is assumed — pass the published archive layout
    explicitly). ``fetcher`` is an injectable ``(url, dest_path) -> bool``
    used by offline tests. Never raises; None means "not available".
    """
    try:
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        safe = "".join(c if (c.isalnum() or c in "-_T") else "_"
                       for c in str(hour_str))
        final = dest / f"pmxt-{safe}.parquet"
        if final.exists():
            return final
        tmp = dest / (final.name + ".tmp")
        ok = False
        if fetcher is not None:
            try:
                ok = bool(fetcher(base_url or "", tmp))
            except (OSError, ValueError, TypeError) as e:
                print(f"[backfill_pmxt] WARN fetcher failed {hour_str}: {e}")
                ok = False
        elif base_url:
            try:
                url = base_url.format(hour=hour_str)
                _urlreq.urlretrieve(url, str(tmp))
                ok = True
            except (OSError, ValueError, KeyError) as e:
                print(f"[backfill_pmxt] WARN download failed {hour_str}: {e}")
                ok = False
        else:
            print("[backfill_pmxt] no base_url and no fetcher — "
                  f"archive {hour_str} unavailable (gap stays a gap)")
            return None
        if not ok or not tmp.exists():
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError as e:
                print(f"[backfill_pmxt] WARN tmp cleanup failed: {e}")
            return None
        _os_replace_safe(tmp, final)
        return final
    except (OSError, ValueError) as e:
        print(f"[backfill_pmxt] WARN download_pmxt_hour_archive failed: {e}")
        return None


# -- archive reads (DuckDB fast path, pyarrow fallback) ------------------------


def _read_archive_table(path: str | Path) -> Optional[pa.Table]:
    """Read one archive file (parquet/csv/json/jsonl) as a table, else None."""
    try:
        p = Path(path)
        suf = p.suffix.lower()
        if suf == ".parquet":
            return read_table(p)
        if suf == ".csv":
            try:
                with open(p, newline="", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
                if not rows:
                    return pa.table({})
                return pa.Table.from_pylist(rows)
            except (OSError, ValueError, pa.ArrowException) as e:
                print(f"[backfill_pmxt] WARN csv read failed {p.name}: {e}")
                return None
        if suf in (".json", ".jsonl"):
            try:
                rows: List[dict] = []
                with open(p, encoding="utf-8") as f:
                    text = f.read().strip()
                if not text:
                    return pa.table({})
                if suf == ".jsonl" or text.splitlines()[0].strip().startswith("{"):
                    for line in text.splitlines():
                        line = line.strip()
                        if line:
                            rows.append(json.loads(line))
                else:
                    obj = json.loads(text)
                    rows = obj if isinstance(obj, list) else [obj]
                if not rows:
                    return pa.table({})
                return pa.Table.from_pylist(rows)
            except (OSError, ValueError, pa.ArrowException) as e:
                print(f"[backfill_pmxt] WARN json read failed {p.name}: {e}")
                return None
        print(f"[backfill_pmxt] WARN unsupported archive suffix: {p.suffix}")
        return None
    except (OSError, ValueError) as e:
        print(f"[backfill_pmxt] WARN archive read failed {path}: {e}")
        return None


def _filter_with_duckdb(
    archive_path: str | Path,
    *,
    day_str: Optional[str],
    condition_ids: Optional[set],
    token_ids: Optional[set],
) -> Optional[List[dict]]:
    """DuckDB pushdown filter over one parquet archive, else None on any miss.

    None means "DuckDB unavailable or unusable" — the caller falls back to
    the pyarrow reader. Never raises.
    """
    try:
        if Path(archive_path).suffix.lower() != ".parquet":
            return None
        import duckdb  # type: ignore[import-not-found]

        clauses: List[str] = []
        params: List[Any] = []
        if condition_ids:
            holders = ", ".join(["?"] * len(condition_ids))
            clauses.append(f"condition_id IN ({holders})")
            params.extend(sorted(condition_ids))
        if token_ids:
            holders = ", ".join(["?"] * len(token_ids))
            clauses.append(f"token_id IN ({holders})")
            params.extend(sorted(token_ids))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_path = str(archive_path).replace("'", "''")
        rel = duckdb.query(
            f"SELECT * FROM read_parquet('{safe_path}') {where}", params or None)
        try:
            rows = rel.fetchall()
            cols = [d[0] for d in rel.description]
        finally:
            try:
                rel.close()
            except (AttributeError, ValueError):
                print("[backfill_pmxt] WARN duckdb relation close skipped")
        out = [dict(zip(cols, r, strict=False)) for r in rows]
        if day_str:
            out = [r for r in out if _row_day_fallback(r) in (day_str, None)]
        return out
    except (ImportError, ValueError, OSError,
            pa.ArrowException, AttributeError) as e:
        print(f"[backfill_pmxt] duckdb filter unavailable "
              f"({type(e).__name__}) — pyarrow fallback")
        return None


def _row_day_fallback(row: dict) -> Optional[str]:
    """Day from any clock field the archive row carries, else None."""
    try:
        for key in ("ts_source", "timestamp", "ts_ms", "ts",
                    "ts_snapshot_ms", "block_timestamp_ms"):
            day = _utc_day_str(row.get(key))
            if day:
                return day
        try:
            ns = row.get("ts_snapshot_ns")
            if ns is not None and not isinstance(ns, bool):
                day = _utc_day_str(int(ns) // 1_000_000)
                if day:
                    return day
        except (TypeError, ValueError):
            pass
        for key in ("ts_utc", "timestamp_utc", "ts_snapshot_utc", "datetime"):
            ms = _iso_to_ms(row.get(key))
            day = _utc_day_str(ms)
            if day:
                return day
        return None
    except (TypeError, ValueError):
        return None


def filter_archive(
    archive_path: str | Path,
    *,
    day_str: Optional[str] = None,
    condition_ids: Optional[set] = None,
    token_ids: Optional[set] = None,
) -> Dict[str, Any]:
    """Filter one PMXT archive file to row dicts. Pure local read.

    Tries the DuckDB pushdown filter first, then the pyarrow reader with
    identical Python-side predicates (day + condition/token allow-lists).
    Returns ``{"rows": [...], "stats": {...}}``. Never raises.
    """
    stats: Dict[str, Any] = {"files_ok": 0, "files_failed": 0,
                             "rows_read": 0, "rows_kept": 0,
                             "duckdb_used": False}
    try:
        duck_rows = _filter_with_duckdb(
            archive_path, day_str=day_str,
            condition_ids=condition_ids, token_ids=token_ids)
        if duck_rows is not None:
            stats["files_ok"] = 1
            stats["rows_read"] = len(duck_rows)
            stats["rows_kept"] = len(duck_rows)
            stats["duckdb_used"] = True
            return {"rows": duck_rows, "stats": stats}
        table = _read_archive_table(archive_path)
        if table is None:
            stats["files_failed"] = 1
            return {"rows": [], "stats": stats}
        stats["files_ok"] = 1
        rows = table.to_pylist()
        stats["rows_read"] = len(rows)
        kept: List[dict] = []
        for r in rows:
            try:
                if condition_ids and r.get("condition_id") not in condition_ids:
                    if r.get("token_id") not in (token_ids or set()):
                        continue
                elif token_ids and r.get("token_id") not in token_ids \
                        and r.get("condition_id") not in (condition_ids or set()):
                    continue
                if day_str and _row_day_fallback(r) not in (day_str, None):
                    continue
                kept.append(r)
            except (TypeError, ValueError):
                print("[backfill_pmxt] WARN skipping unreadable archive row")
                continue
        stats["rows_kept"] = len(kept)
        return {"rows": kept, "stats": stats}
    except (OSError, ValueError, pa.ArrowException) as e:
        print(f"[backfill_pmxt] WARN filter_archive failed: {e}")
        stats["files_failed"] = 1
        return {"rows": [], "stats": stats}


# -- markets maps (read-only) ---------------------------------------------------


def load_markets_maps(data_dir: str | Path) -> Tuple[Dict[str, dict], Dict[str, str]]:
    """condition_id -> market info, token_id -> condition_id (read-only).

    Authoritative source is markets_latest; falls back to the markets_log
    hive (last row per condition wins). A token naming two markets maps to
    neither (ambiguous, honest NULL). Never raises.
    """
    cid_info: Dict[str, dict] = {}
    token_to_cid: Dict[str, str] = {}
    try:
        base = Path(data_dir)
        rows: List[dict] = []
        latest = base / "markets_latest" / "markets_latest.parquet"
        if latest.exists():
            try:
                t = read_table(latest)
                if t is not None and t.num_rows:
                    rows = t.to_pylist()
            except (OSError, ValueError, pa.ArrowException) as e:
                print(f"[backfill_pmxt] WARN markets_latest unreadable: {e}")
        if not rows:
            log_root = base / "markets_log"
            if log_root.exists():
                for p in sorted(log_root.rglob("*.parquet")):
                    if p.name.endswith(".tmp"):
                        continue
                    try:
                        t = read_table(p)
                        if t is not None and t.num_rows:
                            rows.extend(t.to_pylist())
                    except (OSError, ValueError, pa.ArrowException):
                        print(f"[backfill_pmxt] WARN markets_log file skipped: {p.name}")
                        continue
        by_cid: Dict[str, dict] = {}
        for r in rows:
            try:
                cid = r.get("condition_id")
                if cid:
                    by_cid[str(cid)] = r
            except (TypeError, ValueError):
                continue
        for cid, r in by_cid.items():
            try:
                cid_info[cid] = {
                    "slug": r.get("slug"),
                    "series_id": r.get("series_id"),
                    "asset": r.get("asset"),
                    "window_index": r.get("window_index"),
                    "window_size_seconds": r.get("window_size_seconds"),
                    "up_token_id": r.get("up_token_id"),
                    "down_token_id": r.get("down_token_id"),
                    "market_start_ts_ms": coerce_ts_source_ms(
                        r.get("market_start_ts_ms")),
                    "market_end_ts_ms": coerce_ts_source_ms(
                        r.get("market_end_ts_ms")),
                }
                for tok in (r.get("up_token_id"), r.get("down_token_id")):
                    if not tok:
                        continue
                    tok_s = str(tok)
                    if tok_s in token_to_cid and token_to_cid[tok_s] != cid:
                        del token_to_cid[tok_s]
                    else:
                        token_to_cid.setdefault(tok_s, cid)
            except (TypeError, ValueError):
                continue
        return cid_info, token_to_cid
    except (OSError, ValueError) as e:
        print(f"[backfill_pmxt] WARN load_markets_maps failed: {e}")
        return {}, {}


# -- live coverage + windows needing backfill -----------------------------------


def _list_partition_files(base: Path, dataset: str,
                          day_str: Optional[str],
                          asset_upper: Optional[str]) -> List[Path]:
    """Hive files for (dataset, day, asset); partition-pruned, else scan.

    When ``day_str`` and ``asset_upper`` are both given the query is scoped:
    the writer partitions every row under ``date={day}/asset={ASSET}`` (live
    and backfilled alike), so an absent partition means zero in-scope rows
    and an empty list is returned directly. Falling back to an unscoped
    full-hive scan here would open every file in the dataset (thousands)
    just to conclude the same thing and times out the CLI — unscoped
    callers pass day/asset as None explicitly.
    """
    try:
        root = base / dataset
        if not root.exists():
            return []
        if day_str and asset_upper:
            pats = set(root.glob(
                f"date={day_str}/asset={asset_upper}/*.parquet"))
            pats.update(p for p in root.glob(
                f"date={day_str}/asset={asset_upper.lower()}/*.parquet"))
            return sorted(
                (p for p in pats if not p.name.endswith(".tmp")), key=str)
        files = [p for p in root.rglob("*.parquet")
                 if not p.name.endswith(".tmp")]
        return sorted(files, key=str)
    except (OSError, ValueError) as e:
        print(f"[backfill_pmxt] WARN partition list failed: {e}")
        return []


def _read_key_columns(path: Path, columns: List[str]) -> Optional[pa.Table]:
    """Projected read for dedup keys; full read fallback. None on failure."""
    try:
        try:
            return pq.read_table(str(path), columns=columns)
        except (OSError, ValueError, pa.ArrowException):
            t = read_table(path)
            if t is None:
                return None
            keep = [c for c in columns if c in t.schema.names]
            return t.select(keep) if keep else None
    except (OSError, ValueError, pa.ArrowException) as e:
        print(f"[backfill_pmxt] WARN key read failed {path.name}: {e}")
        return None


def _accumulate_coverage_table(
    table: pa.Table,
    is_backfill_file: bool,
    cov: Dict[str, Dict[str, int]],
) -> None:
    """Fold one key-projected table into coverage counts (vectorized).

    Counting rules are identical to the row loop in
    :func:`live_condition_coverage`: rows without ``condition_id`` are
    skipped; rows in a backfill-prefixed file (or carrying
    ``source='backfill_pmxt'``) count as backfilled, all others as live.
    Aggregation runs in pyarrow compute over distinct values, so per-file
    cost is O(distinct conditions) Python steps instead of O(rows).
    Raises on unexpected schemas — the caller falls back to the row loop.
    """
    cid = table.column("condition_id").combine_chunks()
    valid = pc.and_(pc.is_valid(cid), pc.not_equal(cid, ""))
    if is_backfill_file:
        counts = pc.value_counts(pc.filter(cid, valid))
        for v in counts.to_pylist():
            entry = cov.setdefault(
                str(v["values"]), {"live": 0, "backfilled": 0})
            entry["backfilled"] += v["counts"]
        return
    if "source" not in table.schema.names:
        counts = pc.value_counts(pc.filter(cid, valid))
        for v in counts.to_pylist():
            entry = cov.setdefault(
                str(v["values"]), {"live": 0, "backfilled": 0})
            entry["live"] += v["counts"]
        return
    src = table.column("source").combine_chunks()
    is_bf = pc.fill_null(pc.equal(src, BACKFILL_SOURCE), False)
    for mask, key in ((pc.invert(is_bf), "live"), (is_bf, "backfilled")):
        counts = pc.value_counts(pc.filter(cid, pc.and_(valid, mask)))
        for v in counts.to_pylist():
            entry = cov.setdefault(
                str(v["values"]), {"live": 0, "backfilled": 0})
            entry[key] += v["counts"]


def live_condition_coverage(
    data_dir: str | Path,
    day_str: Optional[str] = None,
    asset: Optional[str] = None,
) -> Dict[str, Dict[str, int]]:
    """Per-condition row counts split into live vs backfilled. Read-only.

    Returns ``{condition_id: {"live": n, "backfilled": m}}``. A row counts
    as backfilled when ``source == 'backfill_pmxt'`` or its file name
    starts with the backfill prefix. Never raises.
    """
    cov: Dict[str, Dict[str, int]] = {}
    try:
        base = Path(data_dir)
        asset_upper = str(asset).upper() if asset else None
        for dataset in TICK_DATASETS:
            for p in _list_partition_files(base, dataset, day_str, asset_upper):
                try:
                    cols = ["condition_id", "source"]
                    t = _read_key_columns(p, cols)
                    if t is None or t.num_rows == 0:
                        continue
                    is_backfill_file = p.name.startswith(BACKFILL_PREFIX)
                    try:
                        _accumulate_coverage_table(t, is_backfill_file, cov)
                    except (OSError, ValueError, pa.ArrowException,
                            TypeError, AttributeError, KeyError):
                        pylist = t.to_pylist()
                        for r in pylist:
                            try:
                                cid = r.get("condition_id")
                                if not cid:
                                    continue
                                entry = cov.setdefault(
                                    str(cid), {"live": 0, "backfilled": 0})
                                if is_backfill_file or r.get("source") == BACKFILL_SOURCE:
                                    entry["backfilled"] += 1
                                else:
                                    entry["live"] += 1
                            except (TypeError, ValueError):
                                continue
                        del pylist
                    del t
                except (OSError, ValueError, pa.ArrowException) as e:
                    print(f"[backfill_pmxt] WARN coverage file skipped {p.name}: {e}")
                    continue
        return cov
    except (OSError, ValueError) as e:
        print(f"[backfill_pmxt] WARN live_condition_coverage failed: {e}")
        return {}


def needs_backfill(
    data_dir: str | Path,
    day_str: Optional[str] = None,
    asset: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> Dict[str, Any]:
    """Windows with zero live rows (pre-cutover gaps worth backfilling).

    A market qualifies when its window day matches ``day_str`` (when given),
    its asset/lane match (when given), and no live (non-backfilled) tick
    row exists for its condition_id. Markets already carrying live rows are
    never listed — missed live ticks inside covered windows stay gaps.
    Returns ``{"windows": [...], "stats": {...}}``. Read-only, never raises.
    """
    try:
        cid_info, _ = load_markets_maps(data_dir)
        cov = live_condition_coverage(data_dir, day_str, asset)
        asset_upper = str(asset).upper() if asset else None
        want_lane = f"{asset_upper}-{timeframe}" if (
            asset_upper and timeframe) else None
        windows: List[dict] = []
        stats: Dict[str, Any] = {"markets_seen": 0, "need": 0,
                                 "covered_live": 0, "skipped_lane": 0}
        for cid, info in cid_info.items():
            try:
                stats["markets_seen"] += 1
                if asset_upper and str(info.get("asset") or "").upper() != asset_upper:
                    continue
                if want_lane and info.get("series_id") not in (want_lane, None):
                    stats["skipped_lane"] += 1
                    continue
                start_ms = info.get("market_start_ts_ms")
                end_ms = info.get("market_end_ts_ms")
                win_day = _utc_day_str(start_ms)
                if win_day is None and end_ms is not None:
                    win_day = _utc_day_str(int(end_ms) - 1)
                if day_str and win_day != day_str:
                    continue
                entry = cov.get(cid, {"live": 0, "backfilled": 0})
                if entry.get("live"):
                    stats["covered_live"] += 1
                    continue
                stats["need"] += 1
                windows.append({
                    "condition_id": cid,
                    "slug": info.get("slug"),
                    "asset": info.get("asset"),
                    "series_id": info.get("series_id"),
                    "window_index": info.get("window_index"),
                    "day": win_day,
                    "live_rows": entry.get("live", 0),
                    "backfilled_rows": entry.get("backfilled", 0),
                })
            except (TypeError, ValueError):
                continue
        windows.sort(key=lambda w: (str(w.get("day") or ""),
                                    str(w.get("asset") or ""),
                                    str(w.get("condition_id"))))
        return {"windows": windows, "stats": stats}
    except (OSError, ValueError) as e:
        print(f"[backfill_pmxt] WARN needs_backfill failed: {e}")
        return {"windows": [], "stats": {"error": str(e)[:200]}}


# -- validation (real-data-only: drop + count, never adjust) ---------------------


def _valid_price(v: Any) -> Optional[float]:
    """Price in [0, 1]; None/NaN/out-of-range -> None (drop, never clamp)."""
    try:
        if v is None:
            return None
        f = float(v)
        if f != f or not (0.0 <= f <= 1.0):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _valid_size(v: Any) -> Optional[float]:
    """Size >= 0; None/NaN/negative -> None (drop, never clamp)."""
    try:
        if v is None:
            return None
        f = float(v)
        if f != f or f < 0:
            return None
        return f
    except (TypeError, ValueError):
        return None


def _outcome_for_token(token_id: Any, info: Optional[dict]) -> Optional[str]:
    """up/down for a market token, else None (never guessed)."""
    try:
        if token_id is None or not info:
            return None
        tok = str(token_id)
        if tok and tok == str(info.get("up_token_id") or ""):
            return "up"
        if tok and tok == str(info.get("down_token_id") or ""):
            return "down"
        return None
    except (TypeError, ValueError):
        return None


# -- enrich ---------------------------------------------------------------------


def enrich_trades(
    rows: List[dict],
    cid_info: Dict[str, dict],
    token_to_cid: Dict[str, str],
) -> Tuple[List[dict], Dict[str, Any]]:
    """Project archive rows onto hive trades shape. Pure, never raises.

    Resolves condition_id via the token map, outcome via the market's token
    pair, market columns via markets_latest. Every kept row carries
    ``source='backfill_pmxt'``, ``ts_received_ns=None`` (never received
    live) and ``ts_backfilled_ns`` from the event clock when known.
    Dropped rows are counted by reason — never adjusted into shape.
    """
    kept: List[dict] = []
    stats: Dict[str, Any] = {"in": len(rows), "kept": 0, "dropped_no_market": 0,
                             "dropped_bad_price": 0, "dropped_bad_size": 0,
                             "dropped_no_id": 0}
    try:
        for i, r in enumerate(rows):
            try:
                tok = r.get("token_id")
                tok_s = str(tok) if tok is not None else None
                cid = r.get("condition_id")
                cid_s = str(cid) if cid else None
                if not cid_s and tok_s and tok_s in token_to_cid:
                    cid_s = token_to_cid[tok_s]
                info = cid_info.get(cid_s) if cid_s else None
                if not cid_s or info is None:
                    stats["dropped_no_market"] += 1
                    continue
                price = _valid_price(r.get("price"))
                if price is None:
                    stats["dropped_bad_price"] += 1
                    continue
                size = _valid_size(r.get("size"))
                if size is None:
                    stats["dropped_bad_size"] += 1
                    continue
                ts_ms = coerce_ts_source_ms(
                    r.get("ts_source", r.get("timestamp", r.get("ts_ms"))))
                trade_id = r.get("trade_id") or r.get("id")
                if not trade_id:
                    txh = str(r.get("transaction_hash")
                              or r.get("tx_hash") or "").lower()
                    seed = txh or f"{cid_s}-{tok_s}-{ts_ms}-{price}-{size}"
                    trade_id = f"pmxt-{seed}-{i}"
                    if txh:
                        trade_id = f"pmxt-{txh}-{tok_s}-{ts_ms}"
                side = r.get("side")
                side = side.strip().lower() if isinstance(side, str) and side.strip() else None
                if side not in ("buy", "sell", None):
                    side = None
                kept.append({
                    "ts_source": ts_ms,
                    "ts_received_ns": None,
                    "source": BACKFILL_SOURCE,
                    "ts_backfilled_ns": (ts_ms * 1_000_000
                                         if ts_ms is not None else None),
                    "condition_id": cid_s,
                    "series_id": info.get("series_id"),
                    "window_index": info.get("window_index"),
                    "asset": info.get("asset"),
                    "trade_id": str(trade_id),
                    "transaction_hash": (str(r.get("transaction_hash")
                                             or r.get("tx_hash") or "").lower()
                                         or None),
                    "token_id": tok_s,
                    "outcome": (r.get("outcome")
                                if r.get("outcome") in ("up", "down")
                                else _outcome_for_token(tok_s, info)),
                    "price": price,
                    "size": size,
                    "fee": _valid_size(r.get("fee")),
                    "side": side,
                    "maker_wallet": r.get("maker_wallet") or r.get("maker"),
                    "taker_wallet": r.get("taker_wallet") or r.get("taker"),
                    "wallet": (r.get("taker_wallet") or r.get("taker")
                               or r.get("maker_wallet") or r.get("maker")
                               or r.get("wallet")),
                })
            except (TypeError, ValueError) as e:
                print(f"[backfill_pmxt] WARN enrich trade row skipped: {e}")
                stats["dropped_no_id"] += 1
                continue
        stats["kept"] = len(kept)
        return kept, stats
    except (TypeError, ValueError) as e:
        print(f"[backfill_pmxt] WARN enrich_trades failed: {e}")
        return kept, stats


def enrich_book_events(
    rows: List[dict],
    cid_info: Dict[str, dict],
    token_to_cid: Dict[str, str],
) -> Tuple[List[dict], Dict[str, Any]]:
    """Project archive rows onto hive book_events shape. Pure, never raises."""
    kept: List[dict] = []
    stats: Dict[str, Any] = {"in": len(rows), "kept": 0,
                             "dropped_no_market": 0, "dropped_no_id": 0}
    try:
        for i, r in enumerate(rows):
            try:
                tok = r.get("token_id")
                tok_s = str(tok) if tok is not None else None
                cid = r.get("condition_id")
                cid_s = str(cid) if cid else None
                if not cid_s and tok_s and tok_s in token_to_cid:
                    cid_s = token_to_cid[tok_s]
                info = cid_info.get(cid_s) if cid_s else None
                if not cid_s or info is None:
                    stats["dropped_no_market"] += 1
                    continue
                event_id = r.get("event_id") or r.get("id")
                if not event_id:
                    ts = coerce_ts_source_ms(
                        r.get("ts_source", r.get("timestamp")))
                    event_id = (f"pmxt-{cid_s}-{tok_s}-{ts}-"
                                f"{r.get('event_type', 'book')}-{i}")
                outcome = r.get("outcome")
                if not (isinstance(outcome, str) and outcome.strip().lower()
                        in ("up", "down")):
                    outcome = _outcome_for_token(tok_s, info)
                kept.append({
                    "ts_source": coerce_ts_source_ms(
                        r.get("ts_source", r.get("timestamp"))),
                    # NULL when never received live (never a stand-in clock).
                    "ts_received_ns": _coerce_ns(r.get("ts_received_ns")),
                    "source": BACKFILL_SOURCE,
                    "condition_id": cid_s,
                    "series_id": info.get("series_id"),
                    "window_index": info.get("window_index"),
                    "asset": info.get("asset"),
                    "event_id": str(event_id),
                    "token_id": tok_s,
                    "outcome": outcome,
                    "event_type": str(r.get("event_type") or "book"),
                    "side": r.get("side"),
                    "old_best_bid": _valid_price(r.get("old_best_bid")),
                    "new_best_bid": _valid_price(r.get("new_best_bid")),
                    "old_best_ask": _valid_price(r.get("old_best_ask")),
                    "new_best_ask": _valid_price(r.get("new_best_ask")),
                    "old_bid_size": _valid_size(r.get("old_bid_size")),
                    "new_bid_size": _valid_size(r.get("new_bid_size")),
                    "old_ask_size": _valid_size(r.get("old_ask_size")),
                    "new_ask_size": _valid_size(r.get("new_ask_size")),
                })
            except (TypeError, ValueError) as e:
                print(f"[backfill_pmxt] WARN enrich book_event skipped: {e}")
                stats["dropped_no_id"] += 1
                continue
        stats["kept"] = len(kept)
        return kept, stats
    except (TypeError, ValueError) as e:
        print(f"[backfill_pmxt] WARN enrich_book_events failed: {e}")
        return kept, stats


def enrich_snapshots(
    rows: List[dict],
    cid_info: Dict[str, dict],
    token_to_cid: Dict[str, str],
) -> Tuple[List[dict], Dict[str, Any]]:
    """Project archive rows onto hive snapshot shape. Pure, never raises.

    Only YES/NO top-of-book and L2 level columns present in the archive row
    are carried; absent sides stay absent (never 0-filled).
    """
    kept: List[dict] = []
    stats: Dict[str, Any] = {"in": len(rows), "kept": 0,
                             "dropped_no_market": 0, "dropped_no_clock": 0}
    try:
        for i, r in enumerate(rows):
            try:
                cid = r.get("condition_id")
                cid_s = str(cid) if cid else None
                tok = r.get("token_id")
                if not cid_s and tok is not None \
                        and str(tok) in token_to_cid:
                    cid_s = token_to_cid[str(tok)]
                info = cid_info.get(cid_s) if cid_s else None
                if not cid_s or info is None:
                    stats["dropped_no_market"] += 1
                    continue
                ns = _coerce_ns(r.get("ts_snapshot_ns"))
                if ns is None:
                    ms = coerce_ts_source_ms(r.get("ts_source",
                                                  r.get("timestamp",
                                                        r.get("ts_snapshot_ms"))))
                    if ms is None:
                        ms = _iso_to_ms(r.get("ts_snapshot_utc"))
                    ns = ms * 1_000_000 if ms is not None else None
                if ns is None:
                    stats["dropped_no_clock"] += 1
                    continue
                out: Dict[str, Any] = {
                    "ts_snapshot_ns": ns,
                    "condition_id": cid_s,
                    "series_id": info.get("series_id"),
                    "window_index": info.get("window_index"),
                    "asset": info.get("asset"),
                    "snapshot_id": str(r.get("snapshot_id")
                                       or f"pmxt-{cid_s}-{ns}-{i}"),
                    "source": BACKFILL_SOURCE,
                    "book_state": r.get("book_state") or "live",
                }
                for key in ("up_token_id", "down_token_id"):
                    out[key] = info.get(key)
                for col, val in r.items():
                    try:
                        if col.startswith(("up_", "down_")) and (
                                col.endswith("_price", "_size")
                                or col in ("up_bid", "up_ask", "down_bid",
                                           "down_ask", "up_bid_size",
                                           "up_ask_size", "down_bid_size",
                                           "down_ask_size")):
                            out[col] = _valid_price(val) if col.endswith(
                                ("_price", "_bid", "_ask")) else _valid_size(val)
                    except (TypeError, ValueError):
                        continue
                kept.append(out)
            except (TypeError, ValueError) as e:
                print(f"[backfill_pmxt] WARN enrich snapshot skipped: {e}")
                continue
        stats["kept"] = len(kept)
        return kept, stats
    except (TypeError, ValueError) as e:
        print(f"[backfill_pmxt] WARN enrich_snapshots failed: {e}")
        return kept, stats


def enrich_resolutions(
    resolution_records: List[dict],
    cid_info: Dict[str, dict],
) -> Tuple[List[dict], Dict[str, Any]]:
    """Official outcomes from the archive into sidecar rows. Pure.

    A record counts when it names a known condition_id and carries an
    ``up``/``down`` outcome (winner flag or explicit outcome). Output rows
    carry ``settlement_source='polymarket_official'`` plus the
    ``source='backfill_pmxt'`` marker and are written to the sidecar
    dataset — never into the live markets hive. Never raises.
    """
    kept: List[dict] = []
    stats: Dict[str, Any] = {"in": len(resolution_records), "kept": 0,
                             "dropped_unknown_market": 0,
                             "dropped_no_outcome": 0}
    try:
        for rec in resolution_records:
            try:
                cid = rec.get("condition_id")
                cid_s = str(cid) if cid else None
                info = cid_info.get(cid_s) if cid_s else None
                if not cid_s or info is None:
                    stats["dropped_unknown_market"] += 1
                    continue
                outcome = rec.get("resolution_outcome") or rec.get("outcome")
                if isinstance(outcome, str):
                    outcome = outcome.strip().lower()
                if outcome not in ("up", "down"):
                    stats["dropped_no_outcome"] += 1
                    continue
                kept.append({
                    "condition_id": cid_s,
                    "asset": info.get("asset"),
                    "slug": info.get("slug"),
                    "series_id": info.get("series_id"),
                    "window_index": info.get("window_index"),
                    "resolution_outcome": outcome,
                    "settlement_price": _valid_price(
                        rec.get("settlement_price", rec.get("price"))),
                    "settlement_source": "polymarket_official",
                    "source": BACKFILL_SOURCE,
                    "resolved_at_utc": _now_utc_iso(),
                })
            except (TypeError, ValueError) as e:
                print(f"[backfill_pmxt] WARN enrich resolution skipped: {e}")
                continue
        stats["kept"] = len(kept)
        return kept, stats
    except (TypeError, ValueError) as e:
        print(f"[backfill_pmxt] WARN enrich_resolutions failed: {e}")
        return kept, stats


def enrich_strikes(
    rows: List[dict],
    strikes: List[dict],
) -> Tuple[List[dict], Dict[str, Any]]:
    """Attach underlying strike prices to tick rows. Pure, never raises.

    ``strikes`` are ``{asset, window_start_ts_ms, window_end_ts_ms,
    strike_price}`` records (Chainlink boundary prices from the archive).
    Rows whose event clock falls inside a strike window gain the
    ``strike_price`` column; every other row keeps ``strike_price=None``
    (honest NULL — never carried forward across windows).
    """
    stats: Dict[str, Any] = {"in": len(rows), "attached": 0,
                             "strikes": len(strikes)}
    try:
        windows: List[tuple] = []
        for s in strikes:
            try:
                start = coerce_ts_source_ms(s.get("window_start_ts_ms"))
                end = coerce_ts_source_ms(s.get("window_end_ts_ms"))
                px = s.get("strike_price", s.get("price"))
                px_f = float(px) if px is not None else None
                if px_f is not None and px_f == px_f and start is not None \
                        and end is not None:
                    windows.append((str(s.get("asset") or "").upper(),
                                    start, end, px_f))
            except (TypeError, ValueError):
                continue
        for r in rows:
            try:
                r.setdefault("strike_price", None)
                if r.get("strike_price") is not None:
                    continue
                ts = coerce_ts_source_ms(r.get("ts_source"))
                if ts is None and r.get("ts_snapshot_ns") is not None:
                    try:
                        ts = int(r["ts_snapshot_ns"]) // 1_000_000
                    except (TypeError, ValueError):
                        ts = None
                if ts is None:
                    continue
                asset = str(r.get("asset") or "").upper()
                for w_asset, start, end, px in windows:
                    if asset == w_asset and start <= ts < end:
                        r["strike_price"] = px
                        stats["attached"] += 1
                        break
            except (TypeError, ValueError):
                continue
        return rows, stats
    except (TypeError, ValueError) as e:
        print(f"[backfill_pmxt] WARN enrich_strikes failed: {e}")
        return rows, stats


# -- dedup ----------------------------------------------------------------------


def dedup_key(row: dict, dataset: str) -> Optional[tuple]:
    """Stable dedup key for one enriched row, else None. Never raises."""
    try:
        if dataset == "trades":
            tid = row.get("trade_id")
            if tid:
                return ("trade_id", str(tid))
            tx = str(row.get("transaction_hash") or "").lower()
            tok = row.get("token_id")
            ts = row.get("ts_source")
            price = row.get("price")
            size = row.get("size")
            if not (tx or tok is not None or ts is not None
                    or price is not None or size is not None):
                return None  # nothing identifying: undedupable, drop loudly
            return ("trade", tx, str(tok or ""), str(ts),
                    str(price), str(size))
        if dataset == "book_events":
            eid = row.get("event_id")
            if eid:
                return ("event_id", str(eid))
            return ("book_ev", str(row.get("condition_id")),
                    str(row.get("token_id") or ""), str(row.get("ts_source")),
                    str(row.get("event_type")),
                    str(row.get("new_best_bid")), str(row.get("new_best_ask")))
        if dataset == "book_snapshots_500ms":
            sid = row.get("snapshot_id")
            if sid:
                return ("snapshot_id", str(sid))
            return ("snap", str(row.get("condition_id")),
                    str(row.get("ts_snapshot_ns")))
        if dataset == "onchain_fills":
            return ("fill", str(row.get("tx_hash") or "").lower(),
                    str(row.get("token_id") or ""))
        if dataset == "chainlink_events":
            eid = row.get("event_id")
            if eid:
                return ("event_id", str(eid))
            return ("cl", str(row.get("asset") or "").upper(),
                    str(row.get("ts_source")), str(row.get("price")))
        txh = row.get("tx_hash") or row.get("transaction_hash")
        cid = row.get("condition_id")
        if txh or cid:
            return ("gen", str(txh or "").lower(), str(cid or ""),
                    str(row.get("ts_source")))
        return None
    except (TypeError, ValueError):
        return None


def existing_keys_for_partition(
    base: Path,
    dataset: str,
    day_str: str,
    asset_upper: str,
) -> set:
    """Dedup keys already stored (live + prior backfills). Never raises."""
    keys: set = set()
    try:
        for p in _list_partition_files(base, dataset, day_str, asset_upper):
            try:
                t = read_table(p)
                if t is None or t.num_rows == 0:
                    continue
                for r in t.to_pylist():
                    try:
                        k = dedup_key(r, dataset)
                        if k is not None:
                            keys.add(k)
                    except (TypeError, ValueError):
                        continue
                del t
            except (OSError, ValueError, pa.ArrowException) as e:
                print(f"[backfill_pmxt] WARN key scan skipped {p.name}: {e}")
                continue
        return keys
    except (OSError, ValueError) as e:
        print(f"[backfill_pmxt] WARN existing_keys scan failed: {e}")
        return keys


def dedup_new_rows(
    rows: List[dict],
    dataset: str,
    existing_keys: set,
) -> Tuple[List[dict], Dict[str, Any]]:
    """Drop rows duplicating stored keys or each other. Pure, never raises."""
    stats: Dict[str, Any] = {"in": len(rows), "kept": 0,
                             "dropped_dup_stored": 0,
                             "dropped_dup_batch": 0, "dropped_no_key": 0}
    try:
        seen = set(existing_keys or set())
        kept: List[dict] = []
        for r in rows:
            try:
                k = dedup_key(r, dataset)
                if k is None:
                    stats["dropped_no_key"] += 1
                    continue
                if k in seen:
                    if k in (existing_keys or set()):
                        stats["dropped_dup_stored"] += 1
                    else:
                        stats["dropped_dup_batch"] += 1
                    continue
                seen.add(k)
                kept.append(r)
            except (TypeError, ValueError):
                continue
        stats["kept"] = len(kept)
        return kept, stats
    except (TypeError, ValueError) as e:
        print(f"[backfill_pmxt] WARN dedup_new_rows failed: {e}")
        return [], stats


# -- writes (backfilled paths only) ----------------------------------------------


def write_backfilled_rows(
    data_dir: str | Path,
    dataset: str,
    rows: List[dict],
    day_str: str,
    asset: str,
) -> Dict[str, Any]:
    """Store enriched rows in a new ``backfill_pmxt-`` file. Never raises.

    The partition directory is shared with live data (so exports read the
    rows) but the file name is unique per call — existing files are never
    touched. Stored + batch dedup runs before the write, so re-runs are
    idempotent. Empty input writes no file.
    """
    stats: Dict[str, Any] = {"in": len(rows), "wrote": 0,
                             "files": [], "skipped_empty": False}
    try:
        if not rows:
            stats["skipped_empty"] = True
            return stats
        base = Path(data_dir)
        asset_upper = str(asset).upper()
        part = base / dataset / f"date={day_str}" / f"asset={asset_upper}"
        stored = existing_keys_for_partition(base, dataset, day_str, asset_upper)
        kept, dedup_stats = dedup_new_rows(rows, dataset, stored)
        stats["dedup"] = dedup_stats
        if not kept:
            stats["skipped_empty"] = True
            return stats
        stamp = _dt.datetime.now(tz=_dt.timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ")
        name = (f"{BACKFILL_PREFIX}{dataset}-{stamp}-"
                f"{_os.getpid()}.parquet")
        path = part / name
        table = pa.Table.from_pylist(kept)
        if not _atomic_write_table(table, path):
            stats["write_failed"] = True
            return stats
        try:
            check = read_table(path)
            if check is None or check.num_rows != table.num_rows:
                print(f"[backfill_pmxt] WARN verify mismatch on {name} "
                      f"(kept, never deleted)")
            del check
        except (OSError, ValueError, pa.ArrowException) as e:
            print(f"[backfill_pmxt] WARN verify read failed {name}: {e}")
        del table
        stats["wrote"] = len(kept)
        stats["files"] = [str(path)]
        print(f"[backfill_pmxt] wrote {len(kept)}/{len(rows)} {dataset} "
              f"{day_str} {asset_upper} -> {name}")
        return stats
    except (OSError, ValueError, pa.ArrowException) as e:
        print(f"[backfill_pmxt] WARN write_backfilled_rows failed: {e}")
        stats["write_failed"] = True
        return stats


def write_resolution_sidecar(
    data_dir: str | Path,
    rows: List[dict],
    day_str: str,
) -> Dict[str, Any]:
    """Store archive outcomes in the clearly-marked sidecar dataset."""
    stats: Dict[str, Any] = {"in": len(rows), "wrote": 0, "files": []}
    try:
        if not rows:
            return stats
        base = Path(data_dir)
        part = base / RESOLUTIONS_SIDECAR_DATASET / f"date={day_str}"
        seen: set = set()
        for p in _list_partition_files(base, RESOLUTIONS_SIDECAR_DATASET,
                                       day_str, None):
            try:
                t = read_table(p)
                if t is None:
                    continue
                for r in t.to_pylist():
                    if r.get("condition_id"):
                        seen.add(str(r["condition_id"]))
                del t
            except (OSError, ValueError, pa.ArrowException):
                continue
        fresh = [r for r in rows
                 if str(r.get("condition_id") or "") not in seen]
        deduped = {str(r["condition_id"]): r for r in fresh}
        fresh = [deduped[k] for k in sorted(deduped)]
        stats["dropped_dup"] = len(rows) - len(fresh)
        if not fresh:
            return stats
        stamp = _dt.datetime.now(tz=_dt.timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ")
        path = part / f"{BACKFILL_PREFIX}resolutions-{stamp}.parquet"
        table = pa.Table.from_pylist(fresh)
        if _atomic_write_table(table, path):
            stats["wrote"] = len(fresh)
            stats["files"] = [str(path)]
        else:
            stats["write_failed"] = True
        del table
        return stats
    except (OSError, ValueError, pa.ArrowException) as e:
        print(f"[backfill_pmxt] WARN write_resolution_sidecar failed: {e}")
        stats["write_failed"] = True
        return stats


# -- row-kind split ---------------------------------------------------------------


def split_archive_rows(rows: List[dict]) -> Dict[str, List[dict]]:
    """Split archive rows into trades/events/snapshots/chainlink buckets.

    An explicit ``record_kind`` field wins; otherwise the shape decides:
    trade ids or (price + size) present -> trade; snapshot clocks ->
    snapshot; chainlink symbols -> chainlink; winner/outcome-only rows ->
    resolution; strike windows -> strike. Never raises.
    """
    buckets: Dict[str, List[dict]] = {"trades": [], "book_events": [],
                                      "snapshots": [], "chainlink": [],
                                      "resolutions": [], "strikes": []}
    try:
        for r in rows:
            try:
                kind = r.get("record_kind")
                if isinstance(kind, str) and kind in buckets:
                    buckets[kind].append(r)
                    continue
                if r.get("winner") is not None and r.get("price") is None \
                        and r.get("size") is None and "condition_id" in r:
                    buckets["resolutions"].append(r)
                    continue
                if "strike_price" in r and "price" not in r \
                        and "window_start_ts_ms" in r:
                    buckets["strikes"].append(r)
                    continue
                if r.get("ts_snapshot_ns") is not None \
                        or r.get("ts_snapshot_utc") is not None \
                        or (r.get("up_bid") is not None
                            and r.get("price") is None):
                    buckets["snapshots"].append(r)
                    continue
                if r.get("symbol") is not None and r.get("price") is not None \
                        and r.get("condition_id") is None:
                    buckets["chainlink"].append(r)
                    continue
                if r.get("price") is not None and (
                        r.get("size") is not None
                        or r.get("trade_id") is not None
                        or r.get("transaction_hash") is not None
                        or r.get("tx_hash") is not None):
                    buckets["trades"].append(r)
                    continue
                buckets["book_events"].append(r)
            except (TypeError, ValueError):
                continue
        return buckets
    except (TypeError, ValueError) as e:
        print(f"[backfill_pmxt] WARN split_archive_rows failed: {e}")
        return buckets


# -- orchestrator -------------------------------------------------------------------


def _market_window_day(info: Optional[dict]) -> Optional[str]:
    """UTC day of a market window start, else None. Never raises."""
    try:
        if not info:
            return None
        day = _utc_day_str(info.get("market_start_ts_ms"))
        if day is not None:
            return day
        end_ms = info.get("market_end_ts_ms")
        if end_ms is not None:
            return _utc_day_str(int(end_ms) - 1)
        return None
    except (TypeError, ValueError):
        return None


def _keep_market_day(rows: List[dict], cid_info: Dict[str, dict],
                     day: str) -> Tuple[List[dict], int]:
    """Drop rows whose resolved market window falls on another day.

    Clock-less archive rows pass the archive filter for any day; this is
    the honest placement guard — a row is written to the ``day`` partition
    only when its market window agrees (or carries no clocks at all).
    Returns (kept, dropped). Never raises.
    """
    kept: List[dict] = []
    dropped = 0
    try:
        for r in rows:
            try:
                info = cid_info.get(str(r.get("condition_id") or ""))
                win_day = _market_window_day(info)
                if win_day is not None and win_day != day:
                    dropped += 1
                    continue
                kept.append(r)
            except (TypeError, ValueError):
                continue
        return kept, dropped
    except (TypeError, ValueError) as e:
        print(f"[backfill_pmxt] WARN market-day guard failed: {e}")
        return rows, 0


def run_backfill(
    data_dir: str | Path,
    archive_paths: List[str | Path],
    *,
    day: Optional[str] = None,
    asset: Optional[str] = None,
    timeframe: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Filter archives, enrich, dedup and write backfilled rows.

    ``dry_run`` lists windows needing backfill and returns without writing.
    ``day``/``asset``/``timeframe`` scope both the needs listing and the
    archive filter. Returns a stats dict. Never raises.
    """
    summary: Dict[str, Any] = {"dry_run": dry_run, "writes": {},
                               "needs": {}, "filter": {}}
    try:
        if day:
            try:
                _dt.date.fromisoformat(day)
            except ValueError:
                summary["error"] = f"day must be YYYY-MM-DD, got {day!r}"
                return summary
        needs = needs_backfill(data_dir, day, asset, timeframe)
        summary["needs"] = needs
        if dry_run:
            for w in needs.get("windows", []):
                print(f"[backfill_pmxt] needs backfill: "
                      f"{w.get('asset')} {w.get('series_id')} "
                      f"{w.get('slug') or w.get('condition_id')} "
                      f"day={w.get('day')}")
            print(f"[backfill_pmxt] dry-run: {needs['stats'].get('need', 0)} "
                  f"windows need backfill, 0 files written")
            return summary
        cid_info, token_to_cid = load_markets_maps(data_dir)
        need_cids = {str(w.get("condition_id"))
                     for w in needs.get("windows", [])
                     if w.get("condition_id")}
        token_scope = {t for t, c in token_to_cid.items() if c in need_cids}
        all_rows: List[dict] = []
        for ap in archive_paths or []:
            try:
                res = filter_archive(
                    ap, day_str=day,
                    condition_ids=need_cids or None,
                    token_ids=token_scope or None)
                all_rows.extend(res.get("rows", []))
                summary["filter"][str(ap)] = res.get("stats", {})
            except (OSError, ValueError) as e:
                print(f"[backfill_pmxt] WARN archive skipped {ap}: {e}")
                summary["filter"][str(ap)] = {"error": str(e)[:200]}
                continue
        buckets = split_archive_rows(all_rows)
        if asset:
            au = str(asset).upper()
            for key in ("trades", "book_events", "snapshots"):
                buckets[key] = [r for r in buckets[key]
                                if str(r.get("asset") or "").upper()
                                in ("", au) or r.get("condition_id") in need_cids
                                or (r.get("token_id") is not None
                                    and str(r["token_id"]) in token_scope)]
        trades, st_tr = enrich_trades(buckets["trades"], cid_info, token_to_cid)
        events, st_ev = enrich_book_events(buckets["book_events"], cid_info,
                                           token_to_cid)
        snaps, st_sn = enrich_snapshots(buckets["snapshots"], cid_info,
                                        token_to_cid)
        reso_rows, st_re = enrich_resolutions(buckets["resolutions"], cid_info)
        day_of = day or _utc_day_str(_time.time() * 1000)
        if day:
            trades, n = _keep_market_day(trades, cid_info, day)
            st_tr["dropped_other_day"] = n
            events, n = _keep_market_day(events, cid_info, day)
            st_ev["dropped_other_day"] = n
            snaps, n = _keep_market_day(snaps, cid_info, day)
            st_sn["dropped_other_day"] = n
            reso_rows, n = _keep_market_day(reso_rows, cid_info, day)
            st_re["dropped_other_day"] = n
        trades, st_strikes = enrich_strikes(trades, buckets["strikes"])
        summary["enrich"] = {"trades": st_tr, "book_events": st_ev,
                             "snapshots": st_sn, "resolutions": st_re,
                             "strikes": st_strikes}
        asset_of = asset or "BTC"
        writes: Dict[str, Any] = {}
        writes["trades"] = write_backfilled_rows(
            data_dir, "trades", trades, str(day_of), str(asset_of))
        writes["book_events"] = write_backfilled_rows(
            data_dir, "book_events", events, str(day_of), str(asset_of))
        writes["book_snapshots_500ms"] = write_backfilled_rows(
            data_dir, "book_snapshots_500ms", snaps, str(day_of),
            str(asset_of))
        chain_rows: List[dict] = []
        for r in buckets["chainlink"]:
            try:
                px = r.get("price")
                px_f = float(px) if px is not None else None
                if px_f is None or px_f != px_f:
                    continue
                ts_fallback = coerce_ts_source_ms(
                    r.get("ts_source", r.get("timestamp")))
                chain_rows.append({
                    "ts_source": ts_fallback,
                    # NULL when never received live (never a stand-in clock).
                    "ts_received_ns": _coerce_ns(r.get("ts_received_ns")),
                    "source": BACKFILL_SOURCE,
                    "asset": str(r.get("asset") or asset_of).upper(),
                    "event_id": str(r.get("event_id") or r.get("id")
                                    or f"pmxt-cl-{ts_fallback}-{px_f}"),
                    "symbol": r.get("symbol"),
                    "price": px_f,
                })
            except (TypeError, ValueError):
                continue
        writes["chainlink_events"] = write_backfilled_rows(
            data_dir, "chainlink_events", chain_rows, str(day_of),
            str(asset_of))
        try:
            from .onchain import onchain_rows_from_fills
            fills = [r for r in all_rows if r.get("tx_hash")
                     and (r.get("maker") or r.get("taker"))]
            oc = onchain_rows_from_fills(fills, token_to_cid)
            for r in oc:
                r["source"] = BACKFILL_SOURCE
            writes["onchain_fills"] = write_backfilled_rows(
                data_dir, "onchain_fills", oc, str(day_of), str(asset_of))
        except (ImportError, TypeError, ValueError) as e:
            print(f"[backfill_pmxt] WARN onchain enrich skipped: {e}")
            writes["onchain_fills"] = {"in": 0, "wrote": 0,
                                       "skipped": "onchain_unavailable"}
        writes["resolutions_sidecar"] = write_resolution_sidecar(
            data_dir, reso_rows, str(day_of))
        summary["writes"] = writes
        total = sum(w.get("wrote", 0) for w in writes.values()
                    if isinstance(w, dict))
        print(f"[backfill_pmxt] done: {len(all_rows)} archive rows -> "
              f"{total} backfilled rows across {len(writes)} datasets")
        return summary
    except (OSError, ValueError, pa.ArrowException) as e:
        print(f"[backfill_pmxt] WARN run_backfill failed: {e}")
        summary["error"] = str(e)[:200]
        return summary


def main(argv: Optional[List[str]] = None) -> int:
    """CLI: --data-dir --archive/--archive-dir --day --asset --dry-run."""
    ap = argparse.ArgumentParser(
        description="PMXT archive history backfill (pre-cutover windows only)")
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--archive", action="append", default=[],
                    help="local archive file (repeatable)")
    ap.add_argument("--archive-dir", default=None,
                    help="directory of pmxt-*.parquet archives")
    ap.add_argument("--hour", action="append", default=[],
                    help="UTC hour YYYY-MM-DDTHH to download (needs --base-url)")
    ap.add_argument("--base-url", default=None,
                    help="archive URL template taking {hour}")
    ap.add_argument("--day", default=None, help="UTC day YYYY-MM-DD filter")
    ap.add_argument("--asset", default=None, help="asset filter (e.g. BTC)")
    ap.add_argument("--timeframe", default=None,
                    help="lane label filter (e.g. 5m)")
    ap.add_argument("--dry-run", action="store_true",
                    help="list windows needing backfill; write nothing")
    args = ap.parse_args(argv)
    try:
        paths: List[Path] = [Path(a) for a in (args.archive or [])]
        if args.archive_dir:
            try:
                ad = Path(args.archive_dir)
                if ad.exists():
                    paths.extend(sorted(ad.glob("pmxt-*.parquet")))
                    paths.extend(sorted(ad.glob("*.parquet")))
                    seen: set = set()
                    uniq: List[Path] = []
                    for p in paths:
                        if str(p) not in seen:
                            seen.add(str(p))
                            uniq.append(p)
                    paths = uniq
            except OSError as e:
                print(f"[backfill_pmxt] WARN archive-dir scan failed: {e}")
        for hour in (args.hour or []):
            try:
                dest_default = Path(args.data_dir) / RESOLUTIONS_SIDECAR_DATASET
                dest_default = dest_default.parent / "_backfill_pmxt_archive"
                got = download_pmxt_hour_archive(
                    hour, dest_default, base_url=args.base_url)
                if got is not None:
                    paths.append(got)
            except (OSError, ValueError) as e:
                print(f"[backfill_pmxt] WARN hour {hour} skipped: {e}")
                continue
        summary = run_backfill(args.data_dir, paths, day=args.day,
                               asset=args.asset, timeframe=args.timeframe,
                               dry_run=args.dry_run)
        if summary.get("error"):
            print(f"[backfill_pmxt] FAILED: {summary['error']}")
            return 1
        return 0
    except (OSError, ValueError) as e:
        print(f"[backfill_pmxt] FAILED: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
