"""PMData-layout export — per-slug per-day parquet + day-ZIP + manifest.

Checkbox (5) of docs/NEW_COLLECTOR_PERFECT_SPEC.md. Additive: the hive and
the 39-file Kaggle staging (storage/export.py) are untouched.

Layout (under ``out_dir``)::

    l2/{slug}.parquet              YES-only book ticks (snapshots + book_events + l2_raw frames)
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
from typing import Any, Collection, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .jsonfast import loads as _frame_loads
from .onchain import collapse_onchain_unanimity, onchain_rows_from_fills
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
    # Delivering WS leg (single/A/B) verbatim from the l2_raw row; NULL when
    # the hive row carries none (snapshots/events predate conn tagging and
    # compacted l2_raw vintages were written untagged — never fabricated).
    pa.field("source_conn", pa.string(), nullable=True),
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

# Column projections: the ONLY columns the converters/filters below read.
# Reading the full 800+ column snapshot rows materializes gigabytes and OOMs
# small boxes (verified: full 143k-row read dies, projected reads in ~1s).
# Projection is result-identical by construction: absent columns behave
# exactly like legacy rows that never carried them (`in`-checks / .get()).
_SNAPSHOT_BASE_COLS = (
    "condition_id", "series_id", "asset",
    "ts_snapshot_ns", "ts_snapshot_utc",
    "up_ask", "up_ask_size", "up_bid", "up_bid_size",
)
_SNAPSHOT_LEVEL_COLS = tuple(
    f"up_{side}_level_{lvl}_{kind}"
    for lvl in range(1, 21)
    for side in ("ask", "bid")
    for kind in ("price", "size")
)
SNAPSHOT_NEED_COLS = frozenset(_SNAPSHOT_BASE_COLS + _SNAPSHOT_LEVEL_COLS)
EVENTS_NEED_COLS = frozenset((
    "condition_id", "series_id", "asset", "outcome", "event_type",
    "ts_source", "ts_received_ns",
    "new_best_bid", "new_best_ask", "new_bid_size", "new_ask_size",
    "side", "exchange_best",
))
TRADES_NEED_COLS = frozenset((
    "condition_id", "series_id", "asset", "outcome",
    "token_id", "trade_id", "transaction_hash",
    "price", "size", "fee", "side",
    "ts_source", "ts_received_ns", "ts_backfilled_ns",
))
CHAINLINK_NEED_COLS = frozenset(("asset", "ts_source", "ts_received_ns"))
ONCHAIN_NEED_COLS = frozenset(ONCHAIN_FILLS_SCHEMA.names)
# l2_raw rows carry no series_id (lane resolves via the market map) and no
# outcome column (YES-only resolves from the verbatim frame: per-token side
# for book/best_bid_ask/last_trade_price/tick_size_change, YES-leg entry
# for price_change, market-level for market_resolved/unknown).
L2_RAW_NEED_COLS = frozenset((
    "asset", "condition_id", "token_id", "event_type",
    "ts_source", "ts_received_ns", "frame_json", "source_conn",
))
DATASET_NEED_COLS = {
    "book_snapshots_500ms": SNAPSHOT_NEED_COLS,
    "book_events": EVENTS_NEED_COLS,
    "trades": TRADES_NEED_COLS,
    "chainlink_events": CHAINLINK_NEED_COLS,
    "onchain_fills": ONCHAIN_NEED_COLS,
    "l2_raw": L2_RAW_NEED_COLS,
}


def _list_hive_files(base: Path, dataset: str, asset_upper: Optional[str],
                     date_str: str) -> List[Path]:
    """Source files for (dataset, asset, day): partition-pruned, else full scan.

    Mirrors streaming.iter_source_files: prefer date=/asset= partitions, fall
    back to the whole hive (in-memory filters still apply, so mixed layouts
    stay correct) — except files under a mismatching ``date=YYYY-MM-DD/``
    partition, which writers fill by row day (the same clock chain the day
    filters use) and therefore cannot hold in-day rows. Flat files and
    ``date=unknown`` partitions stay readable (legacy layouts, audit). Never
    returns *.tmp files.
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
    want_part = f"date={date_str}"
    files = []
    for p in root.rglob("*.parquet"):
        if p.name.endswith(".tmp"):
            continue
        try:
            parts = p.relative_to(root).parts
        except ValueError:
            continue
        if any(pt.startswith("date=") and len(pt) == len("date=2026-10-04")
               and pt != want_part for pt in parts):
            continue
        files.append(p)
    return sorted(files, key=str)


def _read_hive_rows(files: List[Path], stats: dict,
                   columns: Optional[frozenset] = None) -> List[dict]:
    """Read parquet files to row dicts. Read errors are counted, never raised.

    ``columns`` projects to the needed subset (intersected per file, so
    legacy vintages missing some level columns read fine); None reads all.
    """
    rows: List[dict] = []
    for p in files:
        try:
            if columns is not None:
                try:
                    pf = pq.ParquetFile(str(p))
                    # NB: .schema is the thrift ParquetSchema (nested lists
                    # surface as repeated `element`); .schema_arrow carries
                    # the real top-level field names.
                    arrow_schema = getattr(pf, "schema_arrow", None) or pf.schema
                    have = set(arrow_schema.names)
                    want = [c for c in columns if c in have]
                    t = read_table(p, columns=want or None)
                except Exception:
                    t = read_table(p)
            else:
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


def _l2_frame_token(frame: Any, row: dict) -> Optional[str]:
    """Per-token side of a WS frame, else the row token (honest NULL when neither).

    Key order mirrors storage.l2_raw.extract_token_id (collect-time), so the
    export resolves the same side the collector stored.
    """
    if isinstance(frame, dict):
        for k in ("token_id", "asset_id", "token", "tokenId", "asset"):
            try:
                v = frame.get(k)
            except Exception:
                continue
            if v:
                s = str(v).strip()
                if s:
                    return s
    try:
        tok = row.get("token_id")
    except Exception:
        return None
    return str(tok).strip() if tok else None


def _l2_book_frame_depth(frame: dict) -> Tuple[Any, Any, Any, Any]:
    """Full-depth YES-side arrays from a book frame, best-first.

    Wire order is worst-first (bids ascending, asks descending), so both
    sides are sorted best-first by definition (bids desc, asks asc) —
    ordering only, never synthesis. Levels with NULL/0 price are absent
    (dropped); NULL/0 size on a present price stays a NULL element.
    Malformed input yields NULL sides (honest gap, never raised).
    """
    try:
        bids = frame.get("bids")
        asks = frame.get("asks")
    except Exception:
        return None, None, None, None

    def _side(levels: Any, reverse: bool) -> Tuple[Any, Any]:
        try:
            items = list(levels) if isinstance(levels, (list, tuple)) else []
        except Exception:
            return None, None
        pts: List[Tuple[float, Any]] = []
        for lv in items:
            try:
                lv_d = lv if isinstance(lv, dict) else {}
                p = _nz_float(lv_d.get("price"))
            except Exception:
                continue
            if p is None:
                continue
            try:
                s = _nz_size(lv_d.get("size"))
            except Exception:
                s = None
            pts.append((p, s))
        pts.sort(key=lambda t: t[0], reverse=reverse)
        if not pts:
            return None, None
        return [p for p, _ in pts], [s for _, s in pts]

    bid_p, bid_s = _side(bids, True)
    ask_p, ask_s = _side(asks, False)
    return ask_p, ask_s, bid_p, bid_s


def _l2_price_change_depth(frame: dict, up_token: Optional[str]) -> Tuple[Any, Any, Any, Any]:
    """BBO pair from a price_change frame's YES-leg entry; sizes unknown (NULL).

    The frame carries both legs; the YES (Up) entry's best_bid/best_ask is
    projected, the NO leg is the 1-x complement (never stored). Best sizes
    are absent on the wire, so sizes stay NULL elements (never 0-guessed).
    No YES entry (or unidentifiable up token) yields NULL sides, kept.
    """
    try:
        pcs = frame.get("price_changes")
    except Exception:
        return None, None, None, None
    if not isinstance(pcs, list) or up_token is None:
        return None, None, None, None
    for entry in pcs:
        if not isinstance(entry, dict):
            continue
        aid = None
        for k in ("asset_id", "token_id", "asset"):
            try:
                v = entry.get(k)
            except Exception:
                continue
            if v:
                aid = str(v).strip()
                break
        if aid != up_token:
            continue
        try:
            nb = _nz_float(entry.get("best_bid"))
            na = _nz_float(entry.get("best_ask"))
        except Exception:
            return None, None, None, None
        bid = ([nb], [None]) if nb is not None else (None, None)
        ask = ([na], [None]) if na is not None else (None, None)
        return ask[0], ask[1], bid[0], bid[1]
    return None, None, None, None


def _l2_raw_frame_to_pmdata(
    frame: Any,
    row: dict,
    slug: str,
    asset_upper: str,
    up_token: Optional[str],
    down_token: Optional[str],
    resolved_cid: Optional[str],
) -> Tuple[Optional[dict], bool]:
    """Project one verbatim l2_raw WS frame onto PMDATA_L2_SCHEMA. Never raises.

    Returns (pmdata_row_or_None, skipped_as_down). YES-only exactly as for
    book_events: per-token frames on the NO (down) token are excluded and
    counted as skipped_down_events (same counter — the NO side is 1-x
    derivable); price_change frames always project their YES-leg entry, so
    they are never a down-skip. Market-level frames (market_resolved: no
    token side at collect) are always kept. Depth-absent types keep the
    event with NULL depth — the event row is the coverage signal and is
    never dropped: last_trade_price (its single price lives in the trades
    dataset; no quote to project), tick_size_change / market_resolved /
    unknown future types (no quote on the wire). Clocks and source_conn come
    from the hive row columns (already coerced at collect); NULL stays NULL.
    """
    try:
        etype = None
        if isinstance(frame, dict):
            for k in ("event_type", "type", "eventType", "event"):
                try:
                    v = frame.get(k)
                except Exception:
                    continue
                if isinstance(v, str) and v.strip():
                    etype = v.strip().lower()
                    break
        if not etype:
            etype = str(row.get("event_type") or "unknown").strip().lower() or "unknown"
    except Exception:
        etype = "unknown"
    try:
        token = _l2_frame_token(frame, row)
        is_down = down_token is not None and token is not None and token == down_token
    except Exception:
        is_down = False
    frame_d = frame if isinstance(frame, dict) else {}
    try:
        if etype == "price_change":
            ask_p, ask_s, bid_p, bid_s = _l2_price_change_depth(frame_d, up_token)
        elif etype == "book":
            if is_down:
                return None, True
            ask_p, ask_s, bid_p, bid_s = _l2_book_frame_depth(frame_d)
        elif etype == "best_bid_ask":
            if is_down:
                return None, True
            try:
                nb = _nz_float(frame_d.get("best_bid"))
                na = _nz_float(frame_d.get("best_ask"))
            except Exception:
                nb = na = None
            bid_p, bid_s = ([nb], [None]) if nb is not None else (None, None)
            ask_p, ask_s = ([na], [None]) if na is not None else (None, None)
        elif etype == "market_resolved":
            # Market-level resolution (token_id NULL at collect): no side to
            # exclude, always kept with NULL depth (no quote on the wire).
            ask_p = ask_s = bid_p = bid_s = None
        elif etype in ("last_trade_price", "tick_size_change"):
            if is_down:
                return None, True
            ask_p = ask_s = bid_p = bid_s = None
        else:
            # Unknown future types: fail-open (kept, NULL depth) unless
            # clearly a NO-side per-token frame.
            if is_down:
                return None, True
            ask_p = ask_s = bid_p = bid_s = None
    except Exception:
        ask_p = ask_s = bid_p = bid_s = None
    try:
        cid = row.get("condition_id") or resolved_cid
    except Exception:
        cid = resolved_cid
    try:
        conn = row.get("source_conn")
        conn = str(conn) if conn else None
    except Exception:
        conn = None
    return {
        "market_slug": slug,
        "timestamp": _coerce_ms(row.get("ts_source")),
        "local_timestamp": _coerce_ns(row.get("ts_received_ns")),
        "event_type": etype,
        "ask_prices": ask_p,
        "ask_sizes": ask_s,
        "bid_prices": bid_p,
        "bid_sizes": bid_s,
        "condition_id": cid,
        "asset": asset_upper,
        "source_conn": conn,
    }, False


def _l2_raw_convert_file(
    path_str: str,
    ctx: Dict[str, Any],
) -> Tuple[List[dict], Dict[str, int]]:
    """Convert one l2_raw file to PMDATA L2 rows, row group by row group.

    Never raises (read errors are counted, never raised). Single sequential
    pass per file over projected columns only: full-file reads of multi-GB
    compacted l2_raw files OOM small boxes, so _read_hive_rows is never used
    here. Scoped runs pre-filter each group with a vectorized (C++)
    condition_id/token_id membership mask — mask-excluded rows are exactly
    the not-wanted rows (scope sets derive from the same market map as
    resolution) and are counted as skipped_other_slug without paying
    frame_json decode or a per-row loop. frame_json is parsed only for rows
    surviving slug/lane/day filters. Down-skips share skipped_down_events
    with book_events (same YES-only rule). Memory stays one row group
    regardless of file size.
    """
    rows_out: List[dict] = []
    loc: Dict[str, int] = {
        "rows_read": 0, "rows_kept": 0, "group_errors": 0,
        "skipped_no_slug": 0, "skipped_down_events": 0,
        "skipped_other_slug": 0, "skipped_other_lane": 0,
        "skipped_other_asset": 0, "skipped_bad_date": 0,
        "l2_raw_bad_frame": 0,
    }
    cid_info: Dict[str, dict] = ctx["cid_info"]
    token_to_cid: Dict[str, str] = ctx["token_to_cid"]
    only_slugs = ctx["only_slugs"]
    want = ctx["want"]
    asset_upper = ctx["asset_upper"]
    day_start_ms = ctx["day_start_ms"]
    day_end_ms = ctx["day_end_ms"]
    read_cols: List[str] = ctx["read_cols"]
    scope_cids = ctx["scope_cids"]
    scope_toks = ctx["scope_toks"]

    def _slug_for_local(cid: Any) -> Optional[str]:
        if not cid:
            return None
        info = cid_info.get(str(cid))
        if not info:
            return None
        return _safe_slug(info.get("slug"))

    try:
        pf = pq.ParquetFile(path_str)
        ng = pf.metadata.num_row_groups
    except Exception as e:
        print(f"[export_pmdata] WARN failed to open {path_str}: {e}")
        return rows_out, loc
    for gi in range(ng):
        try:
            table = pf.read_row_group(gi, columns=read_cols)
        except Exception as e:
            print(f"[export_pmdata] WARN failed to read group {gi} of {path_str}: {e}")
            loc["group_errors"] += 1
            continue
        n = table.num_rows
        if n == 0:
            continue
        loc["rows_read"] += n
        try:
            cols = table.to_pydict()
        except Exception as e:
            print(f"[export_pmdata] WARN failed to decode group {gi} of {path_str}: {e}")
            loc["group_errors"] += 1
            continue
        cands: List[int] = list(range(n))
        if scope_cids is not None and scope_toks is not None:
            try:
                mask = pc.or_(
                    pc.is_in(table.column("condition_id"), value_set=scope_cids),
                    pc.is_in(table.column("token_id"), value_set=scope_toks))
                cands = pc.indices_nonzero(pc.fill_null(mask, False)).to_pylist()
            except Exception:
                cands = list(range(n))
            if not cands:
                # No row in this group can resolve to a wanted slug (scope
                # sets derive from the same market map as resolution).
                loc["skipped_other_slug"] += n
                del table, cols
                continue
            # Mask-excluded rows in a matching group are exactly the
            # not-wanted rows (same map argument as above, per row).
            loc["skipped_other_slug"] += n - len(cands)
        cids = cols.get("condition_id", [None] * n)
        toks = cols.get("token_id", [None] * n)
        assets = cols.get("asset", [None] * n)
        tss = cols.get("ts_source", [None] * n)
        rxs = cols.get("ts_received_ns", [None] * n)
        etypes = cols.get("event_type", [None] * n)
        conns = cols.get("source_conn", [None] * n)
        frames = None
        for i in cands:
            cid = str(cids[i]) if cids[i] else None
            tok = str(toks[i]) if toks[i] else None
            # Token ids are unique per market (map enforces): a known token
            # disambiguates even when the row condition_id is absent (l2_raw
            # frames keyed by market/token interchangeably at collect).
            rcid: Optional[str] = None
            if cid and cid in cid_info:
                rcid = cid
            elif tok and tok in token_to_cid:
                rcid = token_to_cid[tok]
            if not _asset_ok(assets[i], rcid, asset_upper, cid_info, loc):
                continue
            if not _lane_ok(None, rcid, want, cid_info, loc):
                continue
            slug = _slug_for_local(rcid)
            if slug is None:
                loc["skipped_no_slug"] += 1
                continue
            if only_slugs is not None and slug not in only_slugs:
                loc["skipped_other_slug"] += 1
                continue
            ts_ms = _coerce_ms(tss[i])
            place_ms = ts_ms
            if place_ms is None:
                rx_ns = _coerce_ns(rxs[i])
                place_ms = rx_ns // 1_000_000 if rx_ns is not None else None
            if place_ms is None or not (day_start_ms <= place_ms < day_end_ms):
                loc["skipped_bad_date"] += 1
                continue
            if frames is None:
                try:
                    frames = table.column("frame_json").to_pylist()
                except Exception as e:
                    print(f"[export_pmdata] WARN no frame_json in group {gi} "
                          f"of {path_str}: {e}")
                    loc["group_errors"] += 1
                    frames = []
                    break
            fj = frames[i] if i < len(frames) else None
            try:
                frame = _frame_loads(fj) if fj else None
                if frame is not None and not isinstance(frame, dict):
                    frame = None
            except Exception:
                frame = None
            if frame is None:
                # Unparseable/empty frame: event preserved via row columns
                # with NULL depth (coverage over precision, never dropped).
                loc["l2_raw_bad_frame"] += 1
            info = cid_info.get(rcid, {})
            up = info.get("up_token_id")
            down = info.get("down_token_id")
            narrow_row = {
                "condition_id": cids[i],
                "token_id": toks[i],
                "event_type": etypes[i],
                "ts_source": tss[i],
                "ts_received_ns": rxs[i],
                "source_conn": conns[i],
            }
            pm, was_down = _l2_raw_frame_to_pmdata(
                frame, narrow_row, slug, asset_upper,
                str(up) if up else None, str(down) if down else None, rcid)
            if was_down:
                loc["skipped_down_events"] += 1
                continue
            assert pm is not None
            rows_out.append(pm)
            loc["rows_kept"] += 1
        del table, cols, frames
    return rows_out, loc


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
    only_slugs: Optional[Collection[str]] = None,
) -> dict:
    """Export one asset/lane/day to the PMData per-slug layout.

    Reads hive ``book_snapshots_500ms`` / ``book_events`` / ``l2_raw`` /
    ``trades`` / ``chainlink_events`` (+ ``onchain_fills`` when present) and
    ``markets_latest`` under ``data_dir``; writes per-market ``{slug}.parquet``
    files, a day-ZIP and ``manifest.json`` under ``out_dir`` (all writes
    atomic tmp+rename, zstd parquet).

    ``extra_fills``: decoded OrderFilled logs (no new RPC in this task) merged
    into the onchain_fills grouping via onchain_rows_from_fills().
    ``only_slugs``: bounded single-slice export — rows resolving to any other
    slug are skipped + counted (``skipped_other_slug``). Per-slug parquet for
    the wanted slugs is byte-identical to the unscoped run; only the manifest
    totals cover the scoped slice. Reads stay bounded: partition-pruned
    ``date={day}/asset={asset}`` files plus the single ``markets_latest``
    file, column-projected to the converter inputs (see DATASET_NEED_COLS).
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

    def _slug_wanted(slug: str) -> bool:
        if only_slugs is None or slug in only_slugs:
            return True
        stats["skipped_other_slug"] = stats.get("skipped_other_slug", 0) + 1
        return False

    # ---- snapshots -> L2 ----
    ds = "book_snapshots_500ms"
    st: dict = {"files_ok": 0, "files_failed": 0, "rows_read": 0, "rows_kept": 0}
    for row in _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st,
                               columns=DATASET_NEED_COLS[ds]):
        cid = row.get("condition_id")
        if not _asset_ok(row.get("asset"), cid, asset_upper, cid_info, stats):
            continue
        if not _lane_ok(row.get("series_id"), str(cid) if cid else None, want, cid_info, stats):
            continue
        slug = _slug_for(cid)
        if slug is None:
            stats["skipped_no_slug"] += 1
            continue
        if not _slug_wanted(slug):
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
    for row in _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st,
                               columns=DATASET_NEED_COLS[ds]):
        cid = row.get("condition_id")
        if not _asset_ok(row.get("asset"), cid, asset_upper, cid_info, stats):
            continue
        if not _lane_ok(row.get("series_id"), str(cid) if cid else None, want, cid_info, stats):
            continue
        slug = _slug_for(cid)
        if slug is None:
            stats["skipped_no_slug"] += 1
            continue
        if not _slug_wanted(slug):
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

    # ---- l2_raw -> L2 (verbatim WS frames, YES-only projection) ----
    # Row-group streaming (single sequential pass per file, projected
    # columns only): full-file reads of the multi-GB compacted l2_raw files
    # OOM small boxes, so _read_hive_rows is never used here. Scoped runs
    # pre-filter each group vectorized on condition_id/token_id, so only
    # in-scope rows pay the per-row loop and only surviving candidates pay
    # frame_json parsing. Results are time-sorted at write, so group order
    # is irrelevant. Unscoped runs convert every resolvable in-day row
    # (slow on full days — only_slugs is the expected path for l2_raw).
    ds = "l2_raw"
    st = {"files_ok": 0, "files_failed": 0, "rows_read": 0, "rows_kept": 0,
          "groups_failed": 0}
    scope_cids = scope_toks = None
    if only_slugs is not None:
        _cids: List[str] = []
        _toks: List[str] = []
        for _cid, _info in cid_info.items():
            if _safe_slug(_info.get("slug")) in only_slugs:
                _cids.append(str(_cid))
                for _t in (_info.get("up_token_id"), _info.get("down_token_id")):
                    if _t:
                        _toks.append(str(_t))
        scope_cids = pa.array(_cids)
        scope_toks = pa.array(_toks)
    for path in _list_hive_files(base, ds, asset_upper, date_str):
        try:
            probe = pq.ParquetFile(str(path))
            have = set(probe.schema_arrow.names)
            del probe
        except Exception as e:
            st["files_failed"] += 1
            print(f"[export_pmdata] WARN failed to read {path}: {e}")
            continue
        if "frame_json" not in have:
            st["files_failed"] += 1
            print(f"[export_pmdata] WARN {path} has no frame_json column, skipped")
            continue
        st["files_ok"] += 1
        ctx = {
            "cid_info": cid_info,
            "token_to_cid": token_to_cid,
            "only_slugs": (frozenset(only_slugs) if only_slugs is not None else None),
            "want": want,
            "asset_upper": asset_upper,
            "day_start_ms": day_start_ms,
            "day_end_ms": day_end_ms,
            "read_cols": [c for c in sorted(L2_RAW_NEED_COLS) if c in have],
            "scope_cids": scope_cids,
            "scope_toks": scope_toks,
        }
        grows, gloc = _l2_raw_convert_file(str(path), ctx)
        for pm in grows:
            l2_by_slug.setdefault(pm["market_slug"], []).append(pm)
        st["rows_read"] += gloc.pop("rows_read")
        st["rows_kept"] += gloc.pop("rows_kept")
        st["groups_failed"] += gloc.pop("group_errors")
        for k, v in gloc.items():
            if k.startswith("skipped_") or k == "l2_raw_bad_frame":
                stats[k] = stats.get(k, 0) + v
    reads[ds] = st

    # ---- trades -> trades ----
    ds = "trades"
    st = {"files_ok": 0, "files_failed": 0, "rows_read": 0, "rows_kept": 0}
    for row in _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st,
                               columns=DATASET_NEED_COLS[ds]):
        cid = row.get("condition_id")
        if not _asset_ok(row.get("asset"), cid, asset_upper, cid_info, stats):
            continue
        if not _lane_ok(row.get("series_id"), str(cid) if cid else None, want, cid_info, stats):
            continue
        slug = _slug_for(cid)
        if slug is None:
            stats["skipped_no_slug"] += 1
            continue
        if not _slug_wanted(slug):
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
    for row in _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st,
                               columns=DATASET_NEED_COLS[ds]):
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
        hive_onchain_rows = _read_hive_rows(_list_hive_files(base, ds, asset_upper, date_str), st,
                                            columns=DATASET_NEED_COLS[ds])
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
    # First-class unanimity-join (spec §4): one row per (tx_hash, token_id);
    # multi-maker / multi-taker / multi-side fills collapse to NULL, never guessed.
    united = collapse_onchain_unanimity(decoded_extra)
    for r in united:
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
        if not _slug_wanted(slug):
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
            "l2_raw": "l2_raw WS frames project verbatim onto L2 rows: book "
            "frames carry full depth re-sorted best-first (wire is "
            "worst-first); price_change frames project their YES-leg "
            "best_bid/best_ask (sizes unknown, NULL); per-token frames on "
            "the down token are excluded like down book_events (same "
            "skipped_down_events counter); last_trade_price / "
            "tick_size_change / market_resolved / unknown keep the event "
            "with NULL depth (no quote on the wire; trade prices live in "
            "the trades files). source_conn is preserved verbatim (NULL "
            "when the hive row carries none — never fabricated).",
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
