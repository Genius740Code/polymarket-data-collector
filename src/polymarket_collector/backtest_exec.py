"""Tick-replay execution simulator — backtest gate only, never the collector.

Marketlens pattern: replay ``l2_raw`` / snapshot ticks in timestamp order and
simulate limit-order fills at resting prices with a CLOB queue-position
approximation (size-ahead), an order-latency delay and configurable fees.
Settlement comes from ``markets_latest`` official rows only.

Real-data-only (AGENT.md/AGENTS.md): pure function of the inputs — no
network, no synthetic price generation. NULL means gap: a tick with a
missing side can never fill that side, a missing mid yields NULL slippage /
equity (never interpolated or carried forward).

Tick rows (dict or DataFrame row) accept either scalar top-of-book fields
or depth lists; first list element is the touch::

    timestamp | ts | ts_source          epoch-ms event clock (None = gap)
    bid_price | best_bid | up_bid | bid_prices[0]
    bid_size  | best_bid_size | up_bid_size | bid_sizes[0]
    ask_price | best_ask | up_ask | ask_prices[0]
    ask_size  | best_ask_size | up_ask_size | ask_sizes[0]

Order rows (dict or DataFrame row)::

    order_id      unique label (auto-generated when absent)
    timestamp     signal time, epoch-ms (effective = signal + latency_ms)
    side          buy | sell (case-insensitive)
    price         limit price, must be within [0, 1]
    size          shares, must be > 0
    condition_id  optional, used only for settlement lookup
    outcome       optional token side (up | down), used for settlement mapping

Fill model (per order, CLOB semantics):

- The order becomes active at the first tick with
  ``tick_ts >= signal_ts + latency_ms`` (default 100ms). Earlier ticks are
  invisible to it — no look-ahead.
- Buy fills against the ask, sell against the bid, always at resting
  prices: a marketable order (buy with ``ask <= limit``) lifts the touch
  immediately as taker (``fill_price = ask``); a resting order fills at its
  own limit price.
- Queue approximation: when the touch first reaches the resting limit
  (``ask == limit``), the whole resting size is ahead of us
  (``size_ahead = ask_size`` — we joined the back). Later ticks at the same
  price fill us as the queue ahead depletes
  (``fill = max(0, size_ahead - size_now)``); newcomers join behind us and
  never increase ``size_ahead``. A trade-through (buy with ``ask < limit``)
  clears the level and fills the remainder at the limit.
- Ticks with a NULL relevant side/size are gaps: no fill, no state change.
- Fees: ``fee = fill_price * qty * fee_bps / 1e4`` (``fee_bps=0`` default —
  5m markets report ``fee_rate_bps="0"``, DATA_CARD E7).
- Settlement: only rows whose source is official
  (``polymarket_official`` / legacy ``on_chain_confirmed``) settle.
  ``inferred_nearest`` and unknown sources are ignored — the position stays
  open and its PnL stays NULL (never inferred). Binary mapping: winning
  side settles 1.0, losing side 0.0, tie 0.5. Buys are longs, sells shorts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

OFFICIAL_SETTLEMENT_SOURCES = frozenset({"polymarket_official", "on_chain_confirmed"})

DEFAULT_LATENCY_MS = 100
DEFAULT_FEE_BPS = 0.0

# Float tolerance for "touch equals limit" comparisons.
_PX_TOL = 1e-9

FILLS_COLUMNS = [
    "order_id", "fill_ts", "fill_price", "fill_qty", "fee",
    "side", "limit_price", "slippage_bps", "taker",
]

TRADES_COLUMNS = [
    "order_id", "side", "limit_price", "size", "filled_qty",
    "fill_rate", "avg_fill_price", "total_fee", "status",
    "settled", "pnl",
]

EQUITY_COLUMNS = ["timestamp", "position", "cash", "mid", "equity"]


@dataclass
class BacktestResult:
    """Output of :func:`run_backtest`."""

    fills: pd.DataFrame
    equity: pd.DataFrame
    trades: pd.DataFrame
    metrics: Dict[str, Any] = field(default_factory=dict)


def _first_present(row: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _head_of(v: Any) -> Any:
    if isinstance(v, (list, tuple)):
        return v[0] if v else None
    return v


def _valid_price(v: Any) -> Optional[float]:
    """Float in [0, 1], else None (gap — never clamped)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or not (0.0 <= f <= 1.0):
        return None
    return f


def _valid_size(v: Any) -> Optional[float]:
    """Non-negative float, else None. Zero size is unknown depth (gap)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f < 0.0:
        return None
    if f == 0.0:
        return None
    return f


def _coerce_ts_ms(v: Any) -> Optional[int]:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:
        return None
    try:
        return int(f) if f > 1e11 else int(f * 1000)
    except (OverflowError, ValueError):
        return None


def _norm_tick(row: Dict[str, Any]) -> Dict[str, Any]:
    ts = _coerce_ts_ms(_first_present(row, ["timestamp", "ts", "ts_source"]))
    bid = _valid_price(_head_of(_first_present(
        row, ["bid_price", "best_bid", "up_bid", "bid_prices", "bids"])))
    ask = _valid_price(_head_of(_first_present(
        row, ["ask_price", "best_ask", "up_ask", "ask_prices", "asks"])))
    bid_sz = _valid_size(_head_of(_first_present(
        row, ["bid_size", "best_bid_size", "up_bid_size", "bid_sizes"])))
    ask_sz = _valid_size(_head_of(_first_present(
        row, ["ask_size", "best_ask_size", "up_ask_size", "ask_sizes"])))
    return {"timestamp": ts, "bid_price": bid, "bid_size": bid_sz,
            "ask_price": ask, "ask_size": ask_sz}


def _tick_mid(tick: Dict[str, Any]) -> Optional[float]:
    b, a = tick["bid_price"], tick["ask_price"]
    if b is None or a is None:
        return None
    return (b + a) / 2.0


def _norm_side(v: Any) -> str:
    s = str(v).strip().lower() if v is not None else ""
    if s in ("buy", "b", "bid", "yes", "up"):
        return "buy"
    if s in ("sell", "s", "ask", "no", "down"):
        return "sell"
    raise ValueError(f"order side must be buy/sell, got {v!r}")


def _norm_outcome(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("up", "yes", "true", "1"):
        return "up"
    if s in ("down", "no", "false", "0"):
        return "down"
    if s in ("tie", "draw"):
        return "tie"
    return None


def _rows_of(frame: Any) -> List[Dict[str, Any]]:
    if frame is None:
        return []
    if isinstance(frame, pd.DataFrame):
        return [dict(r) for r in frame.to_dict(orient="records")]
    if isinstance(frame, (list, tuple)):
        out = []
        for r in frame:
            if isinstance(r, dict):
                out.append(dict(r))
            else:
                raise TypeError(f"tick/order rows must be dicts, got {type(r).__name__}")
        return out
    raise TypeError(f"ticks/orders must be a DataFrame or list of dicts, got {type(frame).__name__}")


def normalize_settlement(settlement: Any) -> Dict[str, Dict[str, Any]]:
    """Normalize settlement input to ``{condition_id: {outcome, price, source}}``.

    Accepts None, a ``{condition_id: {...}}`` dict, a list of
    ``markets_latest``-shaped row dicts, or a DataFrame with
    ``condition_id`` / ``settlement_source`` /
    ``resolution_outcome`` (or ``winner``) / ``settlement_price`` columns.
    Only official sources survive — everything else maps to unsettled
    (the caller treats a missing entry as "no official settlement").
    """
    if settlement is None:
        return {}
    if isinstance(settlement, pd.DataFrame):
        rows = [dict(r) for r in settlement.to_dict(orient="records")]
    elif isinstance(settlement, dict):
        rows = []
        for cid, info in settlement.items():
            if isinstance(info, dict):
                rows.append({"condition_id": cid, **info})
            else:
                rows.append({"condition_id": cid, "resolution_outcome": info})
    elif isinstance(settlement, (list, tuple)):
        rows = [dict(r) for r in settlement]
    else:
        raise TypeError(f"settlement must be None/dict/DataFrame/rows, got {type(settlement).__name__}")
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        cid = r.get("condition_id")
        if not cid:
            continue
        source = r.get("settlement_source") or r.get("source")
        if source not in OFFICIAL_SETTLEMENT_SOURCES:
            continue
        outcome = _norm_outcome(r.get("resolution_outcome", r.get("winner")))
        price = r.get("settlement_price")
        try:
            price = float(price) if price is not None else None
        except (TypeError, ValueError):
            price = None
        out[str(cid)] = {"outcome": outcome, "price": price, "source": source}
    return out


def _settle_price_for(order: Dict[str, Any], official: Dict[str, Dict[str, Any]]) -> Optional[float]:
    """Official settlement price for one order's token, else None (unsettled)."""
    cid = order.get("condition_id")
    if not cid or str(cid) not in official:
        return None
    info = official[str(cid)]
    outcome = _norm_outcome(order.get("outcome", order.get("token_outcome")))
    winner = info.get("outcome")
    if outcome is None:
        # No side labelled: only an explicit official price settles.
        return info.get("price")
    if winner == "tie":
        return 0.5
    if winner is None or winner == "unknown":
        return info.get("price")
    if outcome == winner:
        return 1.0
    return 0.0


def run_backtest(
    ticks: Any,
    orders: Any,
    *,
    latency_ms: int = DEFAULT_LATENCY_MS,
    fee_bps: float = DEFAULT_FEE_BPS,
    settlement: Any = None,
) -> BacktestResult:
    """Replay ticks and simulate limit-order fills.

    Returns :class:`BacktestResult` with ``fills`` (one row per partial
    fill), ``equity`` (per-tick position/cash/mark-to-mid, NULL mid on
    gaps), ``trades`` (one row per order) and ``metrics`` (``fill_rate``,
    ``slippage_bps``, ``pnl`` and counters).
    """
    if latency_ms is None or float(latency_ms) < 0:
        raise ValueError(f"latency_ms must be >= 0, got {latency_ms!r}")
    if fee_bps is None or float(fee_bps) < 0:
        raise ValueError(f"fee_bps must be >= 0, got {fee_bps!r}")
    latency_ms = int(latency_ms)
    fee_bps = float(fee_bps)

    raw_ticks = [_norm_tick(r) for r in _rows_of(ticks)]
    ticks_null_ts = sum(1 for t in raw_ticks if t["timestamp"] is None)
    ordered = sorted(
        [t for t in raw_ticks if t["timestamp"] is not None],
        key=lambda t: t["timestamp"],
    )

    raw_orders = _rows_of(orders)
    norm_orders: List[Dict[str, Any]] = []
    for i, r in enumerate(raw_orders):
        side = _norm_side(r.get("side"))
        try:
            limit = float(r.get("price", r.get("limit_price")))
        except (TypeError, ValueError):
            raise ValueError(f"order {i}: limit price must be numeric, got {r.get('price')!r}")
        if limit != limit or not (0.0 <= limit <= 1.0):
            raise ValueError(f"order {i}: limit price must be within [0, 1], got {limit!r}")
        try:
            size = float(r.get("size", r.get("qty")))
        except (TypeError, ValueError):
            raise ValueError(f"order {i}: size must be numeric, got {r.get('size')!r}")
        if size != size or size <= 0:
            raise ValueError(f"order {i}: size must be > 0, got {size!r}")
        ts = _coerce_ts_ms(_first_present(r, ["timestamp", "ts", "ts_source"]))
        if ts is None:
            raise ValueError(f"order {i}: timestamp is required (epoch-ms)")
        norm_orders.append({
            "order_id": str(r.get("order_id", r.get("id", f"order-{i}"))),
            "timestamp": ts,
            "side": side,
            "price": limit,
            "size": size,
            "condition_id": r.get("condition_id"),
            "outcome": r.get("outcome", r.get("token_outcome")),
        })

    official = normalize_settlement(settlement)

    fills: List[Dict[str, Any]] = []
    trade_rows: List[Dict[str, Any]] = []
    # (tick_index, qty_delta, cash_delta, fill_ref) applied in tick order.
    ledger: Dict[int, List[tuple]] = {}

    for order in norm_orders:
        oid = order["order_id"]
        side = order["side"]
        limit = order["price"]
        remaining = order["size"]
        effective = order["timestamp"] + latency_ms
        start = next((k for k, t in enumerate(ordered) if t["timestamp"] >= effective), None)
        if start is None:
            trade_rows.append({
                "order_id": oid, "side": side, "limit_price": limit, "size": order["size"],
                "filled_qty": 0.0, "fill_rate": 0.0, "avg_fill_price": None,
                "total_fee": 0.0, "status": "expired_no_tick",
                "settled": False, "pnl": None,
            })
            continue
        mid_effective = _tick_mid(ordered[start])
        size_ahead = 0.0
        touched = False
        order_fills: List[Dict[str, Any]] = []

        def _fill(tick_idx: int, qty: float, price: float, taker: bool) -> None:
            fee = price * qty * fee_bps / 1e4
            tick_ts = ordered[tick_idx]["timestamp"]
            slip: Optional[float] = None
            if mid_effective is not None and mid_effective != 0:
                if side == "buy":
                    slip = (price - mid_effective) / mid_effective * 1e4
                else:
                    slip = (mid_effective - price) / mid_effective * 1e4
            fills.append({
                "order_id": oid, "fill_ts": tick_ts, "fill_price": price,
                "fill_qty": qty, "fee": fee, "side": side,
                "limit_price": limit, "slippage_bps": slip, "taker": taker,
            })
            order_fills.append(fills[-1])
            qty_delta = qty if side == "buy" else -qty
            cash_delta = -(price * qty + fee) if side == "buy" else (price * qty - fee)
            ledger.setdefault(tick_idx, []).append((qty_delta, cash_delta))

        for k in range(start, len(ordered)):
            if remaining <= 0:
                break
            tick = ordered[k]
            first_tick = (k == start)
            if side == "buy":
                px, sz = tick["ask_price"], tick["ask_size"]
            else:
                px, sz = tick["bid_price"], tick["bid_size"]
            if px is None or sz is None:
                continue  # gap on our side: no fill, no state change.
            if side == "buy":
                marketable = px <= limit + _PX_TOL
                through = px < limit - _PX_TOL
            else:
                marketable = px >= limit - _PX_TOL
                through = px > limit + _PX_TOL
            if first_tick and marketable:
                # Taker: lift the resting touch up to its size.
                qty = min(remaining, sz)
                if qty > 0:
                    _fill(k, qty, px, True)
                    remaining -= qty
                touched = True
                size_ahead = max(0.0, sz - qty)
                continue
            if through:
                # Price traded through our level: queue cleared, fill at limit.
                _fill(k, remaining, limit, False)
                remaining = 0.0
                break
            if marketable:
                # Touching our limit: first touch queues behind the full
                # resting size; later ticks fill us as the queue ahead
                # depletes (growth joins behind us and is ignored).
                if not touched:
                    touched = True
                    size_ahead = sz
                    continue
                fill_qty = min(remaining, max(0.0, size_ahead - sz))
                if fill_qty > 0:
                    _fill(k, fill_qty, limit, False)
                    remaining -= fill_qty
                    size_ahead = sz
                else:
                    size_ahead = min(size_ahead, sz)
                continue
            # Touch moved away from our limit: any queue we tracked is
            # gone; the next touch re-queues behind the fresh size.
            touched = False
            size_ahead = 0.0
        filled = order["size"] - remaining
        if order_fills:
            avg_px = sum(f["fill_price"] * f["fill_qty"] for f in order_fills) / filled
            total_fee = sum(f["fee"] for f in order_fills)
        else:
            avg_px = None
            total_fee = 0.0
        if filled <= 0:
            status = "unfilled"
        elif remaining > 0:
            status = "partial"
        else:
            status = "filled"
        sp = _settle_price_for(order, official)
        settled = sp is not None and filled > 0
        pnl: Optional[float] = None
        if settled:
            assert avg_px is not None
            if side == "buy":
                pnl = (sp - avg_px) * filled - total_fee
            else:
                pnl = (avg_px - sp) * filled - total_fee
        trade_rows.append({
            "order_id": oid, "side": side, "limit_price": limit, "size": order["size"],
            "filled_qty": filled,
            "fill_rate": filled / order["size"] if order["size"] else 0.0,
            "avg_fill_price": avg_px, "total_fee": total_fee, "status": status,
            "settled": settled, "pnl": pnl,
        })

    # --- equity curve (mark-to-mid; NULL mid/equity on gaps) ---
    position = 0.0
    cash = 0.0
    equity_rows: List[Dict[str, Any]] = []
    for k, tick in enumerate(ordered):
        for qty_delta, cash_delta in ledger.get(k, []):
            position += qty_delta
            cash += cash_delta
        mid = _tick_mid(tick)
        equity_rows.append({
            "timestamp": tick["timestamp"],
            "position": position,
            "cash": cash,
            "mid": mid,
            "equity": (cash + position * mid) if mid is not None else None,
        })

    fills_df = pd.DataFrame(fills, columns=FILLS_COLUMNS)
    equity_df = pd.DataFrame(equity_rows, columns=EQUITY_COLUMNS)
    trades_df = pd.DataFrame(trade_rows, columns=TRADES_COLUMNS)

    total_ordered = sum(o["size"] for o in norm_orders)
    total_filled = sum(f["fill_qty"] for f in fills)
    slips = [f["slippage_bps"] for f in fills if f["slippage_bps"] is not None]
    settled_pnls = [t["pnl"] for t in trade_rows if t["settled"] and t["pnl"] is not None]
    metrics: Dict[str, Any] = {
        "n_orders": len(norm_orders),
        "n_ticks": len(ordered),
        "ticks_null_ts_skipped": ticks_null_ts,
        "n_fills": len(fills),
        "total_ordered": total_ordered,
        "total_filled": total_filled,
        "fill_rate": (total_filled / total_ordered) if total_ordered else None,
        "slippage_bps": (sum(slips) / len(slips)) if slips else None,
        "total_fees": sum(f["fee"] for f in fills),
        "n_settled": sum(1 for t in trade_rows if t["settled"]),
        "n_unsettled": sum(1 for t in trade_rows if not t["settled"]),
        "pnl": sum(settled_pnls) if settled_pnls else None,
        "latency_ms": latency_ms,
        "fee_bps": fee_bps,
    }
    return BacktestResult(fills=fills_df, equity=equity_df, trades=trades_df, metrics=metrics)


__all__ = [
    "BacktestResult",
    "DEFAULT_FEE_BPS",
    "DEFAULT_LATENCY_MS",
    "OFFICIAL_SETTLEMENT_SOURCES",
    "normalize_settlement",
    "run_backtest",
]
