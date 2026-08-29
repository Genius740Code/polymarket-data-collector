"""Sanity-bounds validation — §3A.

Distinct from drift/crossed-book detection: catches malformed values
(price outside [0,1], size < 0) before they touch the in-RAM book.
Cheap, independent of whether sequence numbers / REST L2 are available (§18).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


PRICE_MIN = 0.0
PRICE_MAX = 1.0


@dataclass(frozen=True)
class ValidationError:
    field: str
    value: Any
    reason: str
    message_context: Optional[dict] = None


def validate_price(field: str, value: Any, ctx: Optional[dict] = None) -> Optional[ValidationError]:
    """Validate a single price field is in [0,1] inclusive per §3A."""
    if value is None:
        return None  # null means absent book side — allowed per §3 null-vs-zero
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ValidationError(field=field, value=value, reason="not numeric", message_context=ctx)
    if not (PRICE_MIN <= v <= PRICE_MAX):
        return ValidationError(
            field=field, value=value,
            reason=f"price {v} outside [{PRICE_MIN},{PRICE_MAX}]",
            message_context=ctx,
        )
    return None


def validate_size(field: str, value: Any, ctx: Optional[dict] = None) -> Optional[ValidationError]:
    """Validate a single size field is >=0 per §3A."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ValidationError(field=field, value=value, reason="not numeric", message_context=ctx)
    if v < 0:
        return ValidationError(field=field, value=value, reason=f"size {v} < 0", message_context=ctx)
    return None


def validate_snapshot_fields(
    snapshot: dict,
    ctx: Optional[dict] = None,
) -> List[ValidationError]:
    """Validate all price/size fields in a snapshot dict (wide flat-column layout §3).

    Checks top-of-book, each L2 level price/size, depth aggregates (size-like),
    and optional trade price/size if present. Returns list of errors (empty → valid).
    """
    errors: List[ValidationError] = []
    # top-of-book prices
    for side in ("up", "down"):
        for kind in ("bid", "ask"):
            field = f"{side}_{kind}"
            err = validate_price(field, snapshot.get(field), ctx)
            if err:
                errors.append(err)
            err = validate_size(f"{side}_{kind}_size", snapshot.get(f"{side}_{kind}_size"), ctx)
            if err:
                errors.append(err)
    # L2 levels
    for outcome in ("up", "down"):
        for side in ("bid", "ask"):
            for lvl in range(1, 21):
                p_field = f"{outcome}_{side}_level_{lvl}_price"
                s_field = f"{outcome}_{side}_level_{lvl}_size"
                if p_field in snapshot:
                    err = validate_price(p_field, snapshot[p_field], ctx)
                    if err:
                        errors.append(err)
                if s_field in snapshot:
                    err = validate_size(s_field, snapshot[s_field], ctx)
                    if err:
                        errors.append(err)
    # depth aggregates (sizes)
    for outcome in ("up", "down"):
        for side in ("bid", "ask"):
            for thc in (1, 5, 10):
                f = f"{outcome}_{side}_depth_{thc}c"
                if f in snapshot:
                    err = validate_size(f, snapshot[f], ctx)
                    if err:
                        errors.append(err)
    # trade-like fields if snapshot carries them
    if "price" in snapshot:
        err = validate_price("price", snapshot["price"], ctx)
        if err:
            errors.append(err)
    if "size" in snapshot:
        err = validate_size("size", snapshot["size"], ctx)
        if err:
            errors.append(err)
    return errors


def validate_ws_message(msg: dict) -> List[ValidationError]:
    """Validate a raw WS delta/message dict before applying to book (§3A).

    Expected keys vary by message type (price_change, book, last_trade_price).
    We scan known price/size keys.
    """
    price_keys = {"price", "best_bid", "best_ask", "bid", "ask"}
    size_keys = {"size", "bid_size", "ask_size", "amount"}
    # also L2 level arrays if present as lists
    errors: List[ValidationError] = []
    for k, v in msg.items():
        lk = k.lower()
        if lk in price_keys or lk.endswith("_price"):
            err = validate_price(k, v, ctx=msg)
            if err:
                errors.append(err)
        elif lk in size_keys or lk.endswith("_size") or lk.endswith("_amount"):
            err = validate_size(k, v, ctx=msg)
            if err:
                errors.append(err)
        elif lk in ("bids", "asks") and isinstance(v, list):
            for i, level in enumerate(v):
                if isinstance(level, (list, tuple)) and len(level) >= 2:
                    e1 = validate_price(f"{k}[{i}].price", level[0], ctx=msg)
                    if e1:
                        errors.append(e1)
                    e2 = validate_size(f"{k}[{i}].size", level[1], ctx=msg)
                    if e2:
                        errors.append(e2)
                elif isinstance(level, dict):
                    if "price" in level:
                        e1 = validate_price(f"{k}[{i}].price", level["price"], ctx=msg)
                        if e1:
                            errors.append(e1)
                    if "size" in level:
                        e2 = validate_size(f"{k}[{i}].size", level["size"], ctx=msg)
                        if e2:
                            errors.append(e2)
    return errors
