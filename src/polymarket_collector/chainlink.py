"""Chainlink events & settlement ground truth — §6, §6A.

- Every Chainlink price/data-stream event stored at native frequency (§6)
- Settlement: exact report used for resolution (report_id, price, tx) — §6A
  Fallback to inferred_nearest if on-chain not available, flagged via settlement_source.
- Stuck resolution alerting if outcome still unknown/disputed > max wait.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .enums import SettlementSource


@dataclass
class ChainlinkEvent:
    event_id: str
    schema_version: str
    asset: str
    symbol: Optional[str]
    source: Optional[str]
    price: Optional[float]
    twap: Optional[float]
    twap_window_seconds: Optional[int]
    report_id: Optional[str]
    round_id: Optional[str]
    sequence_number: Optional[int]
    ts_source: Optional[str]
    ts_received_ns: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts_source": self.ts_source,
            "ts_received_ns": self.ts_received_ns,
            "asset": self.asset,
            "event_id": self.event_id,
            "symbol": self.symbol,
            "source": self.source,
            "price": self.price,
            "twap": self.twap,
            "twap_window_seconds": self.twap_window_seconds,
            "report_id": self.report_id,
            "round_id": self.round_id,
            "sequence_number": self.sequence_number,
        }


def chainlink_event_from_ws(msg: Dict[str, Any], asset: str, schema_version: str = "3.0.0") -> ChainlinkEvent:
    """Parse a raw Chainlink Data Streams WS message (§6). Preserve both timestamps, full precision."""
    # ts_source from message, ts_received_ns now
    ts_source = msg.get("timestamp") or msg.get("ts_source") or msg.get("reportTimestamp")
    report_id = msg.get("report_id") or msg.get("reportId") or msg.get("reportIdHex")
    round_id = str(msg.get("round_id") or msg.get("roundId") or "") or None
    # If round_id still null but report_id exists, mirror report_id for joinability (Data Streams legacy compat)
    if round_id is None and report_id:
        round_id = str(report_id)
    seq = msg.get("sequence_number") or msg.get("sequence")
    if seq is not None:
        try:
            seq = int(str(seq).strip()) if str(seq).strip().lstrip("-").isdigit() else int(float(str(seq)))
        except Exception:
            seq = None
    return ChainlinkEvent(
        event_id=str(uuid.uuid4()),
        schema_version=schema_version,
        asset=asset.upper(),
        symbol=msg.get("symbol") or asset.upper(),
        source=msg.get("source") or "chainlink",
        price=msg.get("price"),
        twap=msg.get("twap"),
        twap_window_seconds=msg.get("twap_window_seconds"),
        report_id=report_id,
        round_id=round_id,
        sequence_number=seq,
        ts_source=str(ts_source) if ts_source else None,
        ts_received_ns=time.time_ns(),
    )


@dataclass
class SettlementRecord:
    condition_id: str
    settlement_report_id: Optional[str]
    settlement_price: Optional[float]
    settlement_ts_utc: Optional[str]
    settlement_tx_hash: Optional[str]
    resolution_confirmed_at: Optional[str]
    settlement_source: str  # on_chain_confirmed | inferred_nearest

    def to_dict(self) -> Dict[str, Any]:
        return {
            "settlement_report_id": self.settlement_report_id,
            "settlement_price": self.settlement_price,
            "settlement_ts_utc": self.settlement_ts_utc,
            "settlement_tx_hash": self.settlement_tx_hash,
            "resolution_confirmed_at": self.resolution_confirmed_at,
            "settlement_source": self.settlement_source,
        }


async def fetch_settlement(
    condition_id: str,
    market_end_ts_ms: int,
    chainlink_store,  # writer or in-memory list for nearest lookup
    on_chain_fetcher=None,  # async (condition_id) -> SettlementRecord | None
) -> SettlementRecord:
    """Fetch settlement ground truth (§6A).

    Prefer on-chain report/tx; fallback to nearest chainlink_events row with
    explicit inferred_nearest flag.
    """
    import datetime

    # try on-chain first
    if on_chain_fetcher:
        try:
            result = await on_chain_fetcher(condition_id)
            if result and result.settlement_report_id:
                return result
        except Exception:
            pass

    # fallback: nearest chainlink_events row
    nearest_price = None
    nearest_ts = None
    nearest_report = None
    if chainlink_store:
        # chainlink_store may be a list or a queryable; handle list case
        candidates = chainlink_store if isinstance(chainlink_store, list) else []
        # find closest ts_source to market_end_ts
        best_delta = None
        best = None
        for ev in candidates:
            ts_str = ev.get("ts_source") if isinstance(ev, dict) else getattr(ev, "ts_source", None)
            if not ts_str:
                continue
            try:
                dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
            except Exception:
                continue
            delta = abs(ts_ms - market_end_ts_ms)
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best = ev
        if best:
            if isinstance(best, dict):
                nearest_price = best.get("price")
                nearest_report = best.get("report_id")
                nearest_ts = best.get("ts_source")
            else:
                nearest_price = getattr(best, "price", None)
                nearest_report = getattr(best, "report_id", None)
                nearest_ts = getattr(best, "ts_source", None)

    now_iso = datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    return SettlementRecord(
        condition_id=condition_id,
        settlement_report_id=nearest_report,
        settlement_price=nearest_price,
        settlement_ts_utc=nearest_ts,
        settlement_tx_hash=None,
        resolution_confirmed_at=now_iso,
        settlement_source=SettlementSource.inferred_nearest.value,
    )
