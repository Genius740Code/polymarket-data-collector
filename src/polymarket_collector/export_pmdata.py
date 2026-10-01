"""PMData-layout export — per-slug per-day parquet + day-ZIP + manifest.

Checkbox (5) of docs/NEW_COLLECTOR_PERFECT_SPEC.md. Additive: the hive and
the 39-file Kaggle staging (storage/export.py) are untouched.

Layout (under ``out_dir``)::

    l2/{slug}.parquet              YES-only book ticks (snapshots + book_events)
    trades/{slug}.parquet          fills for that market (all outcomes, explicit)
    onchain_fills/{slug}.parquet   OrderFilled rows for that market (when known)
    {ASSET}-{timeframe}.zip        day-ZIP with the three dirs above
    manifest.json                  row counts + sha256 per file

YES-only normalization (L2 files): book columns carry the YES (Up) token
side only — ``ask_prices``/``ask_sizes`` from the up-ask levels,
``bid_prices``/``bid_sizes`` from the up-bid levels. The NO side is the
complement (``no_bid = 1 - yes_ask``, ``no_ask = 1 - yes_bid``) and is NOT
stored separately; it is documented here so consumers derive it instead of
us doubling every row. Both sides (bid AND ask) are kept when present;
an absent side is NULL — never 0 (null-vs-zero, AGENT.md).

Timestamps: ``timestamp`` is the wire/exchange clock (``ts_source`` for
events, snapshot-grid ms for snapshots) and stays NULL when the wire
carried none — receive time is NEVER substituted. ``local_timestamp`` is
the collector clock (``ts_received_ns``; NULL for reconciled rows that
were never received live). Day placement of a clock-less row may use the
receive/backfilled clock, but the ``timestamp`` column itself stays NULL.

Real-data-only (AGENT.md/AGENTS.md): no synthesis, no interpolation.
Gaps are missing rows + manifest skip counters, never fills.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .onchain import onchain_rows_from_fills
from .storage.parquet_io import read_table
from .storage.schemas import ONCHAIN_FILLS_SCHEMA


# -- output schemas ---------------------------------------------------------

PMDATA_L2_SCHEMA = pa.schema([
    pa.field("market_slug", pa.string(), nullable=False),
    # ms-epoch wire/grid clock; NULL when the wire carried none (never filled).
    pa.field("timestamp", pa.int64(), nullable=True),
    # ns-epoch collector clock; NULL when never received live.
    pa.field("local_timestamp", pa.int64(), nullable=True),
    pa.field("event_type", pa.string(), nullable=False),
    # YES (Up) side depth, best-first; NULL side = absent side (never 0).
    pa.field("ask_prices", pa.list_(pa.float64()), nullable=True),
    pa.field("ask_sizes", pa.list_(pa.float64()), nullable=True),
    pa.field("bid_prices", pa.list_(pa.float64()), nullable=True),
    pa.field("bid_sizes", pa.list_(pa.float64()), nullable=True),
    pa.field("condition_id", pa.string(), nullable=True),
    pa.field("asset", pa.string(), nullable=True),
])

PMDATA_TRADES_SCHEMA = pa.schema([
    pa.field("market_slug", pa.string(), nullable=False),
    pa.field("timestamp", pa.int64(), nullable=True),
    pa.field("local_timestamp", pa.int64(), nullable=True),
    pa.field("event_type", pa.string(), nullable=False),  # always "trade"
    pa.field("price", pa.float64(), nullable=True),
    pa.field("size", pa.float64(), nullable=True),
    pa.field("fee", pa.float64(), nullable=True),
    pa.field("side", pa.string(), nullable=True),
    pa.field("transaction_hash", pa.string(), nullable=True),
    pa.field("trade_id", pa.string(), nullable=True),
    pa.field("outcome", pa.string(), nullable=True),
    pa.field("token_id", pa.string(), nullable=True),
    pa.field("condition_id", pa.string(), nullable=True),
    pa.field("asset", pa.string(), nullable=True),
])


# -- small helpers (export.py naming/zstd conventions) -----------------------


def _os_replace_safe(src: Path, dst: Path) -> None:
    """Atomic tmp->final rename (os.replace overwrites; Path.rename raises on Win)."""
    import os as _os

    _os.replace(str(src), str(dst))


def _coerce_ms(v: Any) -> Optional[int]:
    """Epoch-ms int from int/float/numeric-string wire clocks, else None.

    Mirrors book._parse_frame_ts_ms: sub-1e12 values are seconds-epoch and
    are scaled. Bools, empties and garbage stay NULL (never guessed).
    """
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v) if not isinstance(v, (int, float)) else float(v)
    except (TypeError, ValueError):
        return None
    try:
        if f != f:  # NaN
            return None
        ms = int(f) if f > 1e11 else int(f * 1000)
        return ms
    except (OverflowError, ValueError):
        return None


def _coerce_ns(v: Any) -> Optional[int]:
    """ns-epoch int from a collector clock value, else None."""
    if v is None or isinstance(v, bool):
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n


def _nz_float(v: Any) -> Optional[float]:
    """Null-vs-zero price/size: None/0/NaN -> None, else float."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f == 0:
        return None
    return f


def _nz_size(v: Any) -> Optional[float]:
    """Null-vs-zero size element: None/0/NaN -> None, else float."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f == 0:
        return None
    return f


def _utc_date_str(ms: Any) -> Optional[str]:
    """UTC YYYY-MM-DD for epoch-ms, else None."""
    ms_i = _coerce_ms(ms)
    if ms_i is None:
        return None
    try:
        return _dt.datetime.fromtimestamp(ms_i / 1000, tz=_dt.timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _iso_to_ms(s: Any) -> Optional[int]:
    """Epoch-ms for an ISO8601 string (snapshot_utc fallback), else None."""
    if not s or not isinstance(s, str):
        return None
    try:
        dt = _dt.datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except (ValueError, OverflowError):
        return None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _now_utc_iso() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).isoformat(timespec="milliseconds")


def _safe_slug(s: Any) -> Optional[str]:
    """Filename-safe slug (no separators); None when empty."""
    if not s:
        return None
    slug = str(s).strip().replace("/", "_").replace("\\", "_")
    return slug or None


# -- markets maps ------------------------------------------------------------


def _load_markets_maps(base: Path) -> Tuple[Dict[str, dict], Dict[str, str]]:
    """condition_id -> market info, token_id -> condition_id (first-wins).

    Authoritative source is markets_latest; falls back to the markets_log
    hive (last row per condition wins). Tokens are unique per market — a
    token naming two markets maps to neither (ambiguous, honest NULL).
    """
    rows: List[dict] = []
    latest = base / "markets_latest" / "markets_latest.parquet"
    if latest.exists():
        try:
            t = read_table(latest)
            if t is not None and t.num_rows:
                rows = t.to_pylist()
        except Exception as e:
            print(f"[export_pmdata] WARN markets_latest unreadable: {e}")
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
                except Exception:
                    continue
    by_cid: Dict[str, dict] = {}
    for r in rows:
        cid = r.get("condition_id")
        if cid:
            by_cid[str(cid)] = r  # log order: last wins
    cid_info: Dict[str, dict] = {}
    token_to_cid: Dict[str, str] = {}
    for cid, r in by_cid.items():
        cid_info[cid] = {
            "slug": r.get("slug"),
            "series_id": r.get("series_id"),
            "asset": r.get("asset"),
            "up_token_id": r.get("up_token_id"),
            "down_token_id": r.get("down_token_id"),
            "window_size_seconds": r.get("window_size_seconds"),
            "market_start_ts_ms": _coerce_ms(r.get("market_start_ts_ms")),
            "market_end_ts_ms": _coerce_ms(r.get("market_end_ts_ms")),
        }
        for tok in (r.get("up_token_id"), r.get("down_token_id")):
            if not tok:
                continue
            tok_s = str(tok)
            if tok_s in token_to_cid and token_to_cid[tok_s] != cid:
                # token reused across markets: ambiguous, map to neither.
                del token_to_cid[tok_s]
            else:
                token_to_cid.setdefault(tok_s, cid)
    return cid_info, token_to_cid


# -- hive reads --------------------------------------------------------------


def _list_hive_files(base: Path, dataset: str, asset_upper: Optional[str],
                     date_str: str) -> List[Path]:
    """Source files for (dataset, asset, day): partition-pruned, else full scan.

    Mirrors streaming.iter_source_files: prefer date=/asset= partitions, fall
    back to the whole hive (in-memory filters still apply, so mixed layouts
    stay correct). Never returns *.tmp files.
    """
    root = base / dataset
    if not root.exists():
        return []
    if asset_upper is not None:
        pats = {p for p in root.glob(f"date={date_str}/asset={asset_upper}/*.parquet")}
        pats.update(p for p in root.glob(f"date={date_str}/asset={asset_upper.lower()}/*.parquet"))
        pats = {p for p in pats if not p.name.endswith(".tmp")}
        if pats:
            return sorted(pats, key=str)
    files = [p for p in root.rglob("*.parquet") if not p.name.endswith(".tmp")]
    return sorted(files, key=str)


def _read_hive_rows(files: List[Path], stats: dict) -> List[dict]:
    """Read parquet files to row dicts. Read errors are counted, never raised."""
    rows: List[dict] = []
    for p in files:
        try:
            t = read_table(p)
            if t is None:
                raise IOError(f"unreadable {p.name}")
            stats["files_ok"] += 1
            stats["rows_read"] += t.num_rows
            if t.num_rows:
                rows.extend(t.to_pylist())
            del t
        except Exception as e:
            stats["files_failed"] += 1
            print(f"[export_pmdata] WARN failed to read {p}: {e}")
    return rows


# -- row filters --------------------------------------------------------------


def _lane_ok(row_series: Any, cid: Optional[str], want: Optional[str],
             cid_info: Dict[str, dict], stats: dict) -> bool:
    """Lane filter: series must equal the wanted {ASSET}-{tf} lane.

    NULL series resolve via markets_latest (static token->market mapping);
    residual unresolvable NULLs are skipped + counted (they would otherwise
    leak across lanes or vanish silently).
    """
    if want is None:
        return True
    series = row_series or None
    if series is None and cid and cid in cid_info:
        series = cid_info[cid].get("series_id")
    if series == want:
        return True
    stats["skipped_other_lane"] = stats.get("skipped_other_lane", 0) + 1
    return False


def _asset_ok(row_asset: Any, cid: Optional[str], asset_upper: str,
              cid_info: Dict[str, dict], stats: dict) -> bool:
    """Per-asset filter; NULL-asset rows resolve via the market map."""
    if row_asset is not None and str(row_asset).strip() != "":
        if str(row_asset).upper() == asset_upper:
            return True
        stats["skipped_other_asset"] = stats.get("skipped_other_asset", 0) + 1
        return False
    info = cid_info.get(str(cid)) if cid else None
    if info and info.get("asset") and str(info["asset"]).upper() == asset_upper:
        return True
    if info and not info.get("asset"):
        return True  # unknown market asset: keep (honest gap, placed by lane)
    stats["skipped_other_asset"] = stats.get("skipped_other_asset", 0) + 1
    return False


# -- YES-only converters -------------------------------------------------------


def _yes_depth_lists(row: dict) -> Tuple[Any, Any, Any, Any]:
    """YES (Up) depth arrays from a snapshot row, best-first.

    Levels whose price is NULL/0 are absent (skipped, never 0-filled); a
    size of NULL/0 on a present price stays a NULL element. An empty side
    returns NULL for both its arrays. Falls back to top-of-book singles
    when the row carries no L2 level columns (legacy vintages).
    """
    ask_p: List[float] = []
    ask_s: List[Any] = []
    bid_p: List[float] = []
    bid_s: List[Any] = []
    seen_levels = False
    for lvl in range(1, 21):
        for side, ps, ss in (("ask", ask_p, ask_s), ("bid", bid_p, bid_s)):
            pk = f"up_{side}_level_{lvl}_price"
            sk = f"up_{side}_level_{lvl}_size"
            if pk in row or sk in row:
                seen_levels = True
            p = _nz_float(row.get(pk))
            if p is None:
                continue
            ps.append(p)
            ss.append(_nz_size(row.get(sk)))
    if not seen_levels:
        # legacy row without L2 columns: top-of-book singles only.
        for side, ps, ss in (("ask", ask_p, ask_s), ("bid", bid_p, bid_s)):
            p = _nz_float(row.get(f"up_{side}"))
            if p is None:
                continue
            ps.append(p)
            ss.append(_nz_size(row.get(f"up_{side}_size")))
    return (
        ask_p or None, ask_s if ask_p else None,
        bid_p or None, bid_s if bid_p else None,
    )


def _snapshot_to_pmdata(row: dict, slug: str, asset_upper: str) -> Optional[dict]:
    ns = _coerce_ns(row.get("ts_snapshot_ns"))
    ts_ms = ns // 1_000_000 if ns is not None else _iso_to_ms(row.get("ts_snapshot_utc"))
    ask_p, ask_s, bid_p, bid_s = _yes_depth_lists(row)
    return {
        "market_slug": slug,
        "timestamp": ts_ms,
        "local_timestamp": ns,
        "event_type": "snapshot",
        "ask_prices": ask_p,
        "ask_sizes": ask_s,
        "bid_prices": bid_p,
        "bid_sizes": bid_s,
        "condition_id": row.get("condition_id"),
        "asset": asset_upper,
    }


def _book_event_to_pmdata(row: dict, slug: str, asset_upper: str) -> Tuple[Optional[dict], bool]:
    """Returns (pmdata_row_or_None, skipped_as_down)."""
    outcome = row.get("outcome")
    if isinstance(outcome, str) and outcome.strip().lower() == "down":
        return None, True  # YES-only: NO-side rows are not stored (1-x derivable)
    ts_ms = _coerce_ms(row.get("ts_source"))  # NULL when wire absent; never received fallback
    local_ns = _coerce_ns(row.get("ts_received_ns"))
    etype = row.get("event_type") or "book"
    ask_p = ask_s = bid_p = bid_s = None
    if outcome is None or (isinstance(outcome, str) and outcome.strip().lower() in ("up", "")):
        nb = _nz_float(row.get("new_best_bid"))
        na = _nz_float(row.get("new_best_ask"))
        if nb is not None:
            bid_p, bid_s = [nb], [_nz_size(row.get("new_bid_size"))]
        if na is not None:
            ask_p, ask_s = [na], [_nz_size(row.get("new_ask_size"))]
    elif isinstance(row.get("side"), str) and str(etype) == "bbo_snapped":
        # Exchange-authoritative BBO snap: the named side carries exchange_best.
        ex = _nz_float(row.get("exchange_best"))
        if ex is not None:
            if str(row["side"]).lower() == "ask":
                ask_p, ask_s = [ex], [None]
            elif str(row["side"]).lower() == "bid":
                bid_p, bid_s = [ex], [None]
    # else: derived/anomaly types (crossed_reverted, level_parse_failed, ...)
    # keep the event with NULL depth (honest gap, never reconstructed).
    return {
        "market_slug": slug,
        "timestamp": ts_ms,
        "local_timestamp": local_ns,
        "event_type": str(etype),
        "ask_prices": ask_p,
        "ask_sizes": ask_s,
        "bid_prices": bid_p,
        "bid_sizes": bid_s,
        "condition_id": row.get("condition_id"),
        "asset": asset_upper,
    }, False


def _trade_to_pmdata(row: dict, slug: str, asset_upper: str) -> dict:
    """Trades variant: original fill price/size (outcome preserved, no 1-x rewrite)."""
    side = row.get("side")
    if isinstance(side, str) and side:
        side = side.lower() or None
    return {
        "market_slug": slug,
        "timestamp": _coerce_ms(row.get("ts_source")),
        "local_timestamp": _coerce_ns(row.get("ts_received_ns")),
        "event_type": "trade",
        "price": row.get("price"),
        "size": row.get("size"),
        "fee": row.get("fee"),
        "side": side,
        "transaction_hash": row.get("transaction_hash"),
        "trade_id": row.get("trade_id"),
        "outcome": row.get("outcome"),
        "token_id": row.get("token_id"),
        "condition_id": row.get("condition_id"),
        "asset": asset_upper,
    }


def _sort_pmdata_rows(rows: List[dict]) -> List[dict]:
    """Time order; NULL timestamps sort last (never fabricated into order)."""
    return sorted(
        rows,
        key=lambda r: (
            r.get("timestamp") is None, r.get("timestamp") or 0,
            r.get("local_timestamp") is None, r.get("local_timestamp") or 0,
        ),
    )


# -- writers --------------------------------------------------------------------


def _write_parquet_atomic(table: pa.Table, path: Path) -> None:
    """zstd parquet write via tmp+rename (same convention as export.py)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / (path.name + ".tmp")
    pq.write_table(table, str(tmp), compression="zstd")
    _os_replace_safe(tmp, path)


# -- main entry -------------------------------------------------------------------


def export_pmdata_layout(
    data_dir: str | Path,
    out_dir: str | Path,
    asset: str,
    timeframe: str,
    date_str: str,
    extra_fills: Optional[List[dict]] = None,
) -> dict:
    """Export one asset/lane/day to the PMData per-slug layout.

    Reads hive ``book_snapshots_500ms`` / ``book_events`` / ``trades`` /
    ``chainlink_events`` (+ ``onchain_fills`` when present) and
    ``markets_latest`` under ``data_dir``; writes per-market ``{slug}.parquet``
    files, a day-ZIP and ``manifest.json`` under ``out_dir`` (all writes
    atomic tmp+rename, zstd parquet).

    ``extra_fills``: decoded OrderFilled logs (no new RPC in this task) merged
    into the onchain_fills grouping via onchain_rows_from_fills().
    Returns the manifest dict.
    """
    base = Path(data_dir)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    asset_upper = str(asset).upper()
    want = f"{asset_upper}-{timeframe}" if timeframe else None
    try:
        _dt.date.fromisoformat(date_str)
    except ValueError:
        raise ValueError(f"date must be YYYY-MM-DD, got {date_str!r}")

    try:
        day_start_ms = int(_dt.datetime.fromisoformat(f"{date_str}T00:00:00+00:00").timestamp() * 1000)
    except ValueError:
        raise ValueError(f"date must be YYYY-MM-DD, got {date_str!r}")
    day_end_ms = day_start_ms + 86_400_000

    cid_info, token_to_cid = _load_markets_maps(base)

    l2_by_slug: Dict[str, List[dict]] = {}
    trades_by_slug: Dict[str, List[dict]] = {}
    stats: Dict[str, Any] = {"skipped_no_slug": 0, "skipped_down_events": 0}
    reads: Dict[str, dict] = {}

    def _slug_for(cid: Any) -> Optional[str]:
        if not cid:
            return None
        info = cid_info.get(str(cid))
        if not info:
            return None
        return _safe_slug(info.get("slug"))

    # ---- snapshots -> L2 ----
    ds = "book_snapshots_500ms"
    st: dict = {"files_ok": 0, "files_failed": 0, "rows_read": 0, "rows_kept": 0}
    for row in _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st):
        cid = row.get("condition_id")
        if not _asset_ok(row.get("asset"), cid, asset_upper, cid_info, stats):
            continue
        if not _lane_ok(row.get("series_id"), str(cid) if cid else None, want, cid_info, stats):
            continue
        slug = _slug_for(cid)
        if slug is None:
            stats["skipped_no_slug"] += 1
            continue
        ns = _coerce_ns(row.get("ts_snapshot_ns"))
        ms = ns // 1_000_000 if ns is not None else _iso_to_ms(row.get("ts_snapshot_utc"))
        if _utc_date_str(ms) != date_str:
            stats["skipped_bad_date"] = stats.get("skipped_bad_date", 0) + 1
            continue
        pm = _snapshot_to_pmdata(row, slug, asset_upper)
        if pm is not None:
            l2_by_slug.setdefault(slug, []).append(pm)
            st["rows_kept"] += 1
    reads[ds] = st

    # ---- book_events -> L2 ----
    ds = "book_events"
    st = {"files_ok": 0, "files_failed": 0, "rows_read": 0, "rows_kept": 0}
    for row in _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st):
        cid = row.get("condition_id")
        if not _asset_ok(row.get("asset"), cid, asset_upper, cid_info, stats):
            continue
        if not _lane_ok(row.get("series_id"), str(cid) if cid else None, want, cid_info, stats):
            continue
        slug = _slug_for(cid)
        if slug is None:
            stats["skipped_no_slug"] += 1
            continue
        pm, was_down = _book_event_to_pmdata(row, slug, asset_upper)
        if was_down:
            stats["skipped_down_events"] += 1
            continue
        assert pm is not None
        place_ms = pm["timestamp"]
        if place_ms is None and pm["local_timestamp"] is not None:
            place_ms = int(pm["local_timestamp"]) // 1_000_000
        if _utc_date_str(place_ms) != date_str:
            stats["skipped_bad_date"] = stats.get("skipped_bad_date", 0) + 1
            continue
        l2_by_slug.setdefault(slug, []).append(pm)
        st["rows_kept"] += 1
    reads[ds] = st

    # ---- trades -> trades ----
    ds = "trades"
    st = {"files_ok": 0, "files_failed": 0, "rows_read": 0, "rows_kept": 0}
    for row in _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st):
        cid = row.get("condition_id")
        if not _asset_ok(row.get("asset"), cid, asset_upper, cid_info, stats):
            continue
        if not _lane_ok(row.get("series_id"), str(cid) if cid else None, want, cid_info, stats):
            continue
        slug = _slug_for(cid)
        if slug is None:
            stats["skipped_no_slug"] += 1
            continue
        pm = _trade_to_pmdata(row, slug, asset_upper)
        place_ms = pm["timestamp"]
        if place_ms is None:
            back_ns = _coerce_ns(row.get("ts_backfilled_ns"))
            if back_ns is not None:
                place_ms = back_ns // 1_000_000
            elif pm["local_timestamp"] is not None:
                place_ms = int(pm["local_timestamp"]) // 1_000_000
        if _utc_date_str(place_ms) != date_str:
            stats["skipped_bad_date"] = stats.get("skipped_bad_date", 0) + 1
            continue
        trades_by_slug.setdefault(slug, []).append(pm)
        st["rows_kept"] += 1
    reads[ds] = st

    # ---- chainlink_events: asset-level, counted only (no slug to attach to) ----
    ds = "chainlink_events"
    st = {"files_ok": 0, "files_failed": 0, "rows_read": 0, "rows_kept": 0}
    for row in _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st):
        ra = row.get("asset")
        if ra is not None and str(ra).upper() != asset_upper:
            stats["skipped_other_asset"] = stats.get("skipped_other_asset", 0) + 1
            continue
        ts_ms = _coerce_ms(row.get("ts_source"))
        place_ms = ts_ms
        if place_ms is None:
            rx_ns = _coerce_ns(row.get("ts_received_ns"))
            place_ms = rx_ns // 1_000_000 if rx_ns is not None else None
        if _utc_date_str(place_ms) != date_str:
            stats["skipped_bad_date"] = stats.get("skipped_bad_date", 0) + 1
            continue
        st["rows_kept"] += 1
    reads[ds] = st

    # ---- onchain_fills: hive rows (schema-shaped) + caller-supplied decoded fills ----
    onchain_by_slug: Dict[str, List[dict]] = {}
    ds = "onchain_fills"
    st = {"files_ok": 0, "files_failed": 0, "rows_read": 0, "rows_kept": 0}
    hive_onchain_rows: List[dict] = []
    if (base / ds).exists():
        hive_onchain_rows = _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st)
    decoded_extra = onchain_rows_from_fills(list(extra_fills or []), token_to_cid)
    for row in hive_onchain_rows:
        # Normalize through the same helper shape (passthrough for unknowns).
        decoded_extra.append({
            "tx_hash": (str(row.get("tx_hash") or "").lower() or None),
            "token_id": (str(row["token_id"]) if row.get("token_id") is not None else None),
            "condition_id": row.get("condition_id"),
            "maker": row.get("maker"),
            "taker": row.get("taker"),
            "price": row.get("price"),
            "size": row.get("size"),
            "fee": row.get("fee"),
            "side": (str(row["side"]).lower() if isinstance(row.get("side"), str) and row.get("side") else row.get("side")),
            "exchange_version": row.get("exchange_version"),
            "builder": row.get("builder"),
        })
    for r in decoded_extra:
        cid = r.get("condition_id")
        if not cid and r.get("token_id") and str(r["token_id"]) in token_to_cid:
            cid = token_to_cid[str(r["token_id"])]
            r = {**r, "condition_id": cid}
        if not cid:
            stats["skipped_no_slug"] += 1
            continue
        info = cid_info.get(str(cid))
        if info and info.get("asset") and str(info["asset"]).upper() != asset_upper:
            stats["skipped_other_asset"] = stats.get("skipped_other_asset", 0) + 1
            continue
        if want and info and info.get("series_id") and info["series_id"] != want:
            stats["skipped_other_lane"] = stats.get("skipped_other_lane", 0) + 1
            continue
        slug = _slug_for(cid)
        if slug is None:
            stats["skipped_no_slug"] += 1
            continue
        # Onchain rows carry no event clock: attribute by market-window overlap
        # with the export day (fallback: the market shipped other rows today).
        if info and (info.get("market_start_ts_ms") is not None or info.get("market_end_ts_ms") is not None):
            s_ms = info.get("market_start_ts_ms")
            e_ms = info.get("market_end_ts_ms")
            overlaps = (e_ms is None or e_ms >= day_start_ms) and (s_ms is None or s_ms < day_end_ms)
            if not overlaps:
                stats["skipped_bad_date"] = stats.get("skipped_bad_date", 0) + 1
                continue
        elif slug not in l2_by_slug and slug not in trades_by_slug:
            stats["skipped_bad_date"] = stats.get("skipped_bad_date", 0) + 1
            continue
        if not r.get("tx_hash"):
            stats["skipped_no_slug"] = stats.get("skipped_no_slug", 0) + 1
            continue
        onchain_by_slug.setdefault(slug, []).append({k: r.get(k) for k in ONCHAIN_FILLS_SCHEMA.names})
        st["rows_kept"] += 1
    st["rows_read"] += len(hive_onchain_rows) + len(extra_fills or [])
    reads[ds] = st

    # ---- write per-slug files ----
    files: Dict[str, dict] = {}

    def _commit(kind: str, slug: str, rows: List[dict], schema: pa.Schema) -> None:
        rows = _sort_pmdata_rows(rows) if kind != "onchain_fills" else sorted(
            rows, key=lambda r: (str(r.get("tx_hash") or ""), str(r.get("token_id") or "")))
        rel = f"{kind}/{slug}.parquet"
        path = out / rel
        table = pa.Table.from_pylist(rows, schema=schema)
        _write_parquet_atomic(table, path)
        files[rel] = {"rows": table.num_rows, "sha256": _sha256_file(path), "kind": kind}

    for slug in sorted(l2_by_slug):
        _commit("l2", slug, l2_by_slug[slug], PMDATA_L2_SCHEMA)
    for slug in sorted(trades_by_slug):
        _commit("trades", slug, trades_by_slug[slug], PMDATA_TRADES_SCHEMA)
    for slug in sorted(onchain_by_slug):
        _commit("onchain_fills", slug, onchain_by_slug[slug], ONCHAIN_FILLS_SCHEMA)

    # ---- day-ZIP ----
    zip_name = f"{asset_upper}-{timeframe}.zip" if timeframe else f"{asset_upper}.zip"
    zip_path = out / zip_name
    zip_info: Optional[dict] = None
    if files:
        tmp_zip = out / (zip_name + ".tmp")
        members = sorted(files)
        with zipfile.ZipFile(str(tmp_zip), "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for rel in members:
                zf.write(str(out / rel), arcname=rel)
        _os_replace_safe(tmp_zip, zip_path)
        zip_info = {
            "path": zip_name,
            "sha256": _sha256_file(zip_path),
            "members": len(members),
        }

    manifest = {
        "asset": asset_upper,
        "timeframe": timeframe,
        "date": date_str,
        "generated_at_utc": _now_utc_iso(),
        "layout": "per-slug per-day ({slug}.parquet) + day-ZIP",
        "notes": {
            "yes_only": "L2 book columns carry the YES (Up) token side only; "
            "NO side is the complement (no_bid = 1 - yes_ask, "
            "no_ask = 1 - yes_bid) and is not stored.",
            "down_events": "book_events with outcome=down are excluded from L2 "
            "files (counted as skipped_down_events).",
            "trades": "trades files carry original fill price/size with explicit "
            "outcome (no 1-x rewrite).",
            "timestamps": "timestamp = wire clock (ts_source / snapshot grid ms), "
            "NULL when the wire carried none — never receive-time fallback; "
            "local_timestamp = collector clock (ts_received_ns), NULL when never "
            "received live. Day placement of clock-less rows may use the "
            "receive/backfilled clock; the timestamp column stays NULL.",
            "nulls": "NULL means absent (empty side, unknown wallet/hash, "
            "missing clock) — never 0-guessed.",
            "chainlink": "chainlink_events are asset-level (no slug) and stay in "
            "the hive; counted in reads for audit, not emitted per-slug.",
            "onchain": "onchain_fills rows carry no event clock; day attribution "
            "is by market-window overlap (fallback: market shipped other rows "
            "that day). price/size/fee/builder stay NULL until the amount-word "
            "decode lands.",
        },
        "reads": reads,
        "skipped": stats,
        "files": files,
        "totals": {
            "slugs": len({*(l2_by_slug), *(trades_by_slug), *(onchain_by_slug)}),
            "l2_rows": sum(len(v) for v in l2_by_slug.values()),
            "trades_rows": sum(len(v) for v in trades_by_slug.values()),
            "onchain_rows": sum(len(v) for v in onchain_by_slug.values()),
        },
        "zip": zip_info,
    }
    man_tmp = out / "manifest.json.tmp"
    man_path = out / "manifest.json"
    man_tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    _os_replace_safe(man_tmp, man_path)
    print(f"[export_pmdata] {asset_upper} {timeframe} {date_str}: "
          f"{manifest['totals']['slugs']} slugs, "
          f"{manifest['totals']['l2_rows']} l2 / {manifest['totals']['trades_rows']} trades / "
          f"{manifest['totals']['onchain_rows']} onchain rows -> {out}")
    return manifest


def main(argv: Optional[List[str]] = None) -> int:
    """CLI: --data-dir --out-dir --asset --timeframe --date."""
    ap = argparse.ArgumentParser(description="PMData per-slug per-day export + day-ZIP + manifest")
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--asset", required=True)
    ap.add_argument("--timeframe", required=True, help="lane label, e.g. 5m/15m/1h")
    ap.add_argument("--date", required=True, help="UTC day YYYY-MM-DD")
    args = ap.parse_args(argv)
    export_pmdata_layout(args.data_dir, args.out_dir, args.asset, args.timeframe, args.date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
