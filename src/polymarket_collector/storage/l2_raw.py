"""l2_raw — verbatim WS frame log (perfect-collector checkbox 1, spec §4).

Every market-channel WS frame is stored verbatim (no threshold, no
synthesis): the full frame lands in ``frame_json`` and the row only adds
join/clock columns. ``tick_size_change`` + ``market_resolved`` frames live
here (today: 0 handlers, ``book.py:649`` gap).

Real-data-only: gaps are stale rows + events, never fills. ``frame_json``
is a canonical re-serialization of the received frame —
``json.loads(frame_json)`` round-trips to the input dict exactly; no fields
are added, dropped, or interpolated.
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from ..validation import coerce_ts_source_ms

L2_RAW_SCHEMA = pa.schema([
    pa.field("ts_source", pa.int64(), nullable=True),  # epoch ms, NULL when the wire carries none
    pa.field("ts_received_ns", pa.int64(), nullable=False),
    pa.field("asset", pa.string(), nullable=False),
    pa.field("condition_id", pa.string(), nullable=True),
    pa.field("token_id", pa.string(), nullable=True),
    pa.field("event_type", pa.string(), nullable=False),
    pa.field("frame_json", pa.string(), nullable=False),  # verbatim frame, canonical JSON
    pa.field("source_conn", pa.string(), nullable=True),  # A/B tag for dedup audit
])

# Frames routed here by the collector hook (spec §4). Unknown types still
# pass through as event_type verbatim — never dropped.
L2_RAW_EVENT_TYPES = (
    "book",
    "price_change",
    "last_trade_price",
    "tick_size_change",
    "market_resolved",
)

_TOKEN_KEYS = ("token_id", "asset_id", "token", "tokenId", "asset")
_COND_KEYS = ("condition_id", "conditionId", "condition", "market")
_TYPE_KEYS = ("event_type", "type", "eventType", "event")
_TS_KEYS = ("timestamp", "ts", "ts_source")


def normalize_event_type(msg: Dict[str, Any]) -> str:
    """Canonical event_type for the row; ``frame_json`` keeps the verbatim wire value."""
    for k in _TYPE_KEYS:
        try:
            v = msg.get(k)
        except Exception:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip().lower()
    return "unknown"


def extract_token_id(msg: Dict[str, Any]) -> Optional[str]:
    """Best-effort token id from a frame; None when the wire carries none (honest gap)."""
    try:
        for k in _TOKEN_KEYS:
            v = msg.get(k)
            if v:
                s = str(v).strip()
                if s:
                    return s
        pcs = msg.get("price_changes")
        if isinstance(pcs, list):
            for pc in pcs:
                if not isinstance(pc, dict):
                    continue
                for k in ("asset_id", "token_id", "asset"):
                    v = pc.get(k)
                    if v:
                        s = str(v).strip()
                        if s:
                            return s
    except Exception:
        pass
    return None


def extract_condition_id(msg: Dict[str, Any]) -> Optional[str]:
    """Best-effort condition_id from a frame; None when absent (never fabricated)."""
    try:
        for k in _COND_KEYS:
            v = msg.get(k)
            if v:
                s = str(v).strip()
                if s:
                    return s
    except Exception:
        pass
    return None


def build_l2_raw_row(
    msg: Dict[str, Any],
    *,
    asset: str,
    condition_id: Optional[str] = None,
    token_id: Optional[str] = None,
    event_type: Optional[str] = None,
    source_conn: Optional[str] = None,
    ts_received_ns: Optional[int] = None,
) -> Dict[str, Any]:
    """Build one l2_raw row from a received WS frame (pure, no I/O).

    ``frame_json`` is a canonical re-serialization of ``msg`` — the input
    dict is never mutated and ``json.loads`` of the output equals it. Join
    columns fall back to wire-derived values; unknown stays None (never
    synthesized). Raises TypeError on a non-dict frame.
    """
    if not isinstance(msg, dict):
        raise TypeError(f"l2_raw frame must be a dict, got {type(msg).__name__}")
    try:
        frame_json = json.dumps(msg, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        frame_json = json.dumps({"_unserializable": str(msg)[:4000]}, separators=(",", ":"))
    ts_raw = None
    for k in _TS_KEYS:
        try:
            ts_raw = msg.get(k)
        except Exception:
            continue
        if ts_raw is not None and ts_raw != "":
            break
    try:
        au = str(asset).upper() if asset else "UNKNOWN"
    except Exception:
        au = "UNKNOWN"
    if not au:
        au = "UNKNOWN"
    return {
        "ts_source": coerce_ts_source_ms(ts_raw),
        "ts_received_ns": int(ts_received_ns) if ts_received_ns is not None else time.time_ns(),
        "asset": au,
        "condition_id": condition_id if condition_id else extract_condition_id(msg),
        "token_id": token_id if token_id else extract_token_id(msg),
        "event_type": str(event_type).lower() if event_type else normalize_event_type(msg),
        "frame_json": frame_json,
        "source_conn": str(source_conn) if source_conn else None,
    }


def dedup_key_for_row(row: Dict[str, Any]) -> Optional[Tuple]:
    """Dedup key for an l2_raw row: redelivery across conns A/B shares the
    frame but gets a fresh ``ts_received_ns``/``source_conn``, so the key
    covers (token, exchange-time, type, frame-hash) only. None when there is
    no frame to hash (never false-dupe).
    """
    try:
        fj = row.get("frame_json")
    except Exception:
        return None
    if not fj:
        return None
    try:
        h = hashlib.sha1(fj.encode("utf-8") if isinstance(fj, str) else bytes(fj)).hexdigest()
    except Exception:
        return None
    try:
        tok = row.get("token_id")
        et = row.get("event_type")
        ts = row.get("ts_source")
        if ts is not None:
            return ("l2_raw", tok, ts, et, h)
        return ("l2_raw", tok, et, h)
    except Exception:
        return None


def append_row(writer: Any, msg: Dict[str, Any], *, asset: str, **kwargs: Any) -> bool:
    """Build the row and append via ``ParquetWriter`` (WAL-before-buffer,
    tmp+rename publish). Returns writer.append()'s bool (False = backpressure
    retry, never silent loss).
    """
    row = build_l2_raw_row(msg, asset=asset, **kwargs)
    return bool(writer.append("l2_raw", row, asset=row["asset"]))


def rows_to_hive_path(data_dir: str, date_str: str, asset: str) -> str:
    """Hive leaf for l2_raw: ``data/l2_raw/date=YYYY-MM-DD/asset=XXX/``."""
    from pathlib import Path

    return str(Path(data_dir) / "l2_raw" / f"date={date_str}" / f"asset={str(asset).upper()}")


def event_types_present(rows: List[Dict[str, Any]]) -> List[str]:
    """Distinct event_type values in a row batch (coverage helper for gates)."""
    seen: List[str] = []
    for r in rows:
        try:
            et = r.get("event_type")
        except Exception:
            continue
        if et and et not in seen:
            seen.append(et)
    return seen
