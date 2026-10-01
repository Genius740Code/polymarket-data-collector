"""Direct Chainlink Data Streams source — opt-in stub (RTDS stays primary).

AGENT.md real-data-only: NULL=absent, never fabricate. The Polymarket RTDS
socket (``wss://ws-live-data.polymarket.com``, see ``chainlink.py``) stays the
primary chainlink source. This module is a pure-logic stub for a direct
Chainlink Data Streams websocket: subscribe payloads, message parsing into
the shared :class:`chainlink.ChainlinkEvent` shape, and an unreachable
fallback note. The websocket transport is *injected* (``connect`` callable) —
this module performs no network I/O, so unit tests run offline.

Accepted message families (``type``/``channel``/``stream`` field):
- ``streams`` — benchmark price reports (``reportId`` when present else NULL).
- ``streams_twap30s`` / ``streams_twap60s`` — trailing-TWAP passthrough rows
  (``reportId``/``roundId`` when present else NULL).

Subscribed symbols: BTCUSD/ETHUSD/SOLUSD/XRPUSD/DOGEUSD/BNBUSD/HYPEUSD.
"""
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, Optional

from ..chainlink import ChainlinkEvent
from ..validation import coerce_ts_source_ms


#: Symbols subscribed on the direct streams socket.
DIRECT_SYMBOLS = ("BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "DOGEUSD", "BNBUSD", "HYPEUSD")

#: Asset <-> feed-symbol map (both directions derived from one table).
ASSET_TO_SYMBOL: Dict[str, str] = {
    "BTC": "BTCUSD",
    "ETH": "ETHUSD",
    "SOL": "SOLUSD",
    "XRP": "XRPUSD",
    "DOGE": "DOGEUSD",
    "BNB": "BNBUSD",
    "HYPE": "HYPEUSD",
}
SYMBOL_TO_ASSET: Dict[str, str] = {v: k for k, v in ASSET_TO_SYMBOL.items()}

#: Message families accepted by parse_direct_message (lowercased compare).
STREAM_TYPES = ("streams", "streams_twap30s", "streams_twap60s")

SOURCE_PREFIX = "chainlink-direct"


def symbol_for_asset(asset: Any) -> Optional[str]:
    """Feed symbol for an asset (``BTC`` -> ``BTCUSD``), None when unknown."""
    try:
        if asset is None:
            return None
        return ASSET_TO_SYMBOL.get(str(asset).strip().upper())
    except Exception:
        return None


def asset_for_symbol(symbol: Any) -> Optional[str]:
    """Asset for a feed symbol (``BTCUSD`` -> ``BTC``), None when unknown.

    Unknown feeds map to NULL — never guessed (real-data-only).
    """
    try:
        if symbol is None:
            return None
        return SYMBOL_TO_ASSET.get(str(symbol).strip().upper())
    except Exception:
        return None


def build_subscribe_message(symbols=None) -> Dict[str, Any]:
    """Pure subscribe payload for the direct streams socket.

    Returns a plain dict (no I/O); the injected transport sends it.
    """
    try:
        subs = [str(s).strip().upper() for s in (symbols if symbols is not None else DIRECT_SYMBOLS)]
        subs = [s for s in subs if s]
    except Exception:
        subs = list(DIRECT_SYMBOLS)
    if not subs:
        subs = list(DIRECT_SYMBOLS)
    # De-dupe, preserve order.
    seen = set()
    ordered = []
    for s in subs:
        if s not in seen:
            seen.add(s)
            ordered.append(s)
    return {
        "type": "subscribe",
        "symbols": ordered,
        "streams": list(STREAM_TYPES),
        "source": SOURCE_PREFIX,
    }


def _coerce_price(value: Any) -> Optional[float]:
    """Float price or None (bool/NaN/inf/unparseable stay NULL, never guessed)."""
    try:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, str) and not value.strip():
            return None
        p = float(value)
    except Exception:
        return None
    try:
        if p != p or p in (float("inf"), float("-inf")):
            return None
    except Exception:
        return None
    return p


def _coerce_report_id(value: Any) -> Optional[str]:
    """reportId/roundId as string when present, else NULL."""
    try:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, str):
            s = value.strip()
            return s if s else None
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
    except Exception:
        return None
    return None


def parse_direct_message(msg: Any, schema_version: str = "3.4.0") -> Optional[ChainlinkEvent]:
    """Parse one direct-streams message into a ChainlinkEvent, or None.

    None means "no usable row" (wrong family, unknown feed, non-dict) — the
    caller drops it and counts it; a dropped frame is an honest gap, never a
    fabricated tick. Present-but-bad price/ts/report fields become NULL on an
    otherwise valid row (NULL=absent per AGENT.md).
    """
    if not isinstance(msg, dict):
        return None
    try:
        raw_stream = msg.get("type", msg.get("channel", msg.get("stream")))
        stream = str(raw_stream).strip().lower() if raw_stream is not None else ""
    except Exception:
        return None
    if stream not in STREAM_TYPES:
        return None
    raw_symbol = msg.get("symbol", msg.get("feed", msg.get("feedID", msg.get("feedId"))))
    asset = asset_for_symbol(raw_symbol)
    if asset is None:
        return None
    try:
        symbol = str(raw_symbol).strip().upper()
    except Exception:
        symbol = asset
    price = None
    for key in ("benchmarkPrice", "benchmark_price", "price", "twapPrice", "twap_price", "twap", "value"):
        try:
            if msg.get(key) is not None:
                price = _coerce_price(msg.get(key))
                break
        except Exception:
            continue
    report_id = None
    for key in ("reportId", "report_id", "reportIdHex", "roundId", "round_id"):
        try:
            raw = msg.get(key)
        except Exception:
            continue
        if raw is None or raw == "":
            continue
        report_id = _coerce_report_id(raw)
        if report_id is not None:
            break
    # Source timestamp: first PRESENT time field wins (coerced, may be NULL);
    # never shop across clocks when the primary field is garbage.
    ts_source: Optional[int] = None
    for key in ("timestamp", "ts_source", "reportTimestamp", "report_timestamp",
                "observationsTimestamp", "observations_timestamp",
                "validFromTimestamp", "valid_from_timestamp"):
        try:
            raw_ts = msg.get(key)
        except Exception:
            continue
        if raw_ts is None or raw_ts == "":
            continue
        ts_source = coerce_ts_source_ms(raw_ts)
        break
    source = SOURCE_PREFIX if stream == "streams" else f"{SOURCE_PREFIX}-{stream.replace('streams_', '')}"
    return ChainlinkEvent(
        event_id=str(uuid.uuid4()),
        schema_version=schema_version,
        asset=asset,
        symbol=symbol,
        source=source,
        price=price,
        report_id=report_id,
        ts_source=ts_source,
        ts_received_ns=time.time_ns(),
    )


def unreachable_note(reason: str, detail: str = "") -> Dict[str, Any]:
    """Gap-evidence payload when the direct socket is unreachable.

    RTDS stays the primary chainlink source — this note explains the absence
    of direct rows (collector_events), it never substitutes data (AGENT.md:
    unavailable feed = gap + event, zero fabricated rows).
    """
    try:
        rs = str(reason).strip() if reason is not None else ""
    except Exception:
        rs = ""
    try:
        dt = str(detail).strip() if detail else ""
    except Exception:
        dt = ""
    return {
        "event_type": "chainlink_direct_unreachable",
        "reason": rs or "unreachable",
        "detail": dt or None,
        "fallback": "rtds_primary",
    }


class ChainlinkDirectClient:
    """Injected-transport direct-streams client stub (no network here).

    ``connect`` is an optional async callable the host passes in (e.g.
    ``await connect(symbols, subscribe_payload, on_message)``); when None,
    only the pure-logic methods work and :meth:`run_forever` raises instead
    of touching the network. Counters (received/parsed/dropped) are
    upstream-cadence telemetry; drops are honest gaps.
    """

    def __init__(self, symbols=None, connect=None, on_event=None,
                 schema_version: str = "3.4.0") -> None:
        try:
            self.symbols = list(symbols) if symbols is not None else list(DIRECT_SYMBOLS)
        except Exception:
            self.symbols = list(DIRECT_SYMBOLS)
        self.connect = connect
        self.on_event = on_event
        self.schema_version = schema_version
        self.received = 0
        self.parsed = 0
        self.dropped = 0

    def subscribe_payload(self) -> Dict[str, Any]:
        """Subscribe payload for the injected transport to send."""
        return build_subscribe_message(self.symbols)

    def handle_message(self, msg: Any) -> Optional[ChainlinkEvent]:
        """Parse one inbound message; None = dropped (counted, honest gap)."""
        try:
            self.received += 1
        except Exception:
            pass
        try:
            ev = parse_direct_message(msg, schema_version=self.schema_version)
        except Exception:
            ev = None
        try:
            if ev is None:
                self.dropped += 1
            else:
                self.parsed += 1
        except Exception:
            pass
        return ev

    def note_unreachable(self, reason: str, detail: str = "") -> Dict[str, Any]:
        """Build (+ optionally emit) the unreachable fallback note."""
        note = unreachable_note(reason, detail=detail)
        if self.on_event is not None:
            try:
                self.on_event("chainlink_direct_unreachable", note)
            except Exception:
                pass
        return note

    async def run_forever(self) -> None:
        """Serve the direct socket via the injected ``connect`` transport.

        Raises when no transport was injected — callers keep RTDS primary.
        """
        if self.connect is None:
            raise RuntimeError("chainlink-direct: no connect transport injected; RTDS stays primary")
        await self.connect(self.symbols, self.subscribe_payload(), self.handle_message)
