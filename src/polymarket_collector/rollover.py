"""Market rollover — §1 continuous coverage across windows.

Every 5-min window is a new market (new condition_id). Collector must discover
and subscribe to NEXT window ~30s before current ends, holding two active book
states per asset during overlap.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional


@dataclass
class MarketInfo:
    condition_id: str
    market_id: str
    asset: str
    up_token_id: str
    down_token_id: str
    market_start_ts_ms: int
    market_end_ts_ms: int
    window_index: int
    series_id: str
    status: str = "active"
    question: Optional[str] = None
    tick_size: Optional[float] = None

    @property
    def market_end_ts(self) -> int:
        return self.market_end_ts_ms


@dataclass
class RolloverState:
    """Per-asset rollover state (§1)."""
    asset: str
    current: Optional[MarketInfo] = None
    next: Optional[MarketInfo] = None
    is_rollover_window: bool = False
    last_discovery_attempt_ms: Optional[int] = None
    rollover_miss_logged: bool = False
    rollover_started_emitted: bool = False  # throttle spam — emit once per window
    initial_discovery_attempts: int = 0  # for initial current=None phase

    def needs_rollover_lookahead(self, now_ms: int, lead_ms: int) -> bool:
        if not self.current:
            return True
        return (self.current.market_end_ts_ms - now_ms) <= lead_ms and self.next is None

    def should_promote(self, now_ms: int) -> bool:
        if not self.current or not self.next:
            return False
        return now_ms >= self.current.market_end_ts_ms


class MarketDiscovery:
    """Polls Polymarket Gamma for the next Up/Down market — rate-limited per §1 #7.

    Uses deterministic slug ``{btc,eth,sol}-updown-{window}-{unix_ts}`` via
    ``https://gamma-api.polymarket.com/markets?slug=...`` which bypasses the
    indexing delay of generic search (see Market-Finder repo).  Falls back to
    the configured rest_market_url only if Gamma is unreachable.

    ``window_size_seconds`` determines the market width (300=5min, 900=15min,
    3600=1h, 14400=4h, 86400=1d). The slug suffix is derived from this.
    """

    GAMMA_BASE = "https://gamma-api.polymarket.com"

    def __init__(
        self,
        rest_market_url: str,
        poll_interval_s: float = 2.0,
        backoff_max_s: float = 8.0,
        on_event=None,
        window_size_seconds: int = 300,
    ):
        self.rest_market_url = rest_market_url
        self.poll_interval = poll_interval_s
        self.backoff_max = backoff_max_s
        self.on_event = on_event
        self._backoff_s = poll_interval_s
        self.window_size_seconds = window_size_seconds
        self.window_multiplier = window_size_seconds // 300

    def _slug_for(self, asset: str, ts_seconds: int) -> str:
        # Determine the window label from window_size_seconds
        ws = self.window_size_seconds
        if ws >= 86400:
            window_label = "1d"
        elif ws >= 14400:
            window_label = "4h"
        elif ws >= 3600:
            window_label = "1h"
        elif ws >= 900:
            window_label = "15m"
        else:
            window_label = "5m"
        # asset prefix is lower-case, e.g. btc-updown-5m-1787994000
        return f"{asset.lower()}-updown-{window_label}-{ts_seconds}"

    def _ts_for_after(self, after_ts_ms: int) -> int:
        # floor to configurable window boundary
        # e.g. 5min (300s), 15min (900s), 1h (3600s), 4h (14400s), 1d (86400s)
        return (after_ts_ms // 1000) // self.window_size_seconds * self.window_size_seconds

    async def fetch_next_market(self, asset: str, after_ts_ms: int) -> Optional[MarketInfo]:
        """Query Gamma for the next market after after_ts_ms."""
        import httpx
        import json

        asset = asset.upper()
        ts = self._ts_for_after(after_ts_ms)
        slug = self._slug_for(asset, ts)
        gamma_url = f"{self.GAMMA_BASE}/markets"
        params = {"slug": slug}

        # Try Gamma first — slug is deterministic but may be indexed ~30-60s late.
        # If the market for ts is already ended (endDate <= now), the current window
        # is actually ts+300 (next window). Try ts and ts+300.
        import datetime as _dt
        candidates = [ts, ts + 300]
        # For "next after" we prefer ts (exact), but for initial discovery where ts is floor(now)
        # and that window is already ended, we fallback to ts+300.
        for cand_ts in candidates:
            cand_slug = self._slug_for(asset, cand_ts)
            cand_params = {"slug": cand_slug}
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(gamma_url, params=cand_params)
                    if resp.status_code == 429:
                        if self.on_event:
                            self.on_event("rate_limited", {"asset": asset, "status": 429, "url": gamma_url})
                        self._backoff_s = min(self._backoff_s * 2, self.backoff_max)
                        return None
                    resp.raise_for_status()
                    data = resp.json()
                    if isinstance(data, list) and not data:
                        continue
                    # check if market is active and not closed
                    m0 = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
                    if isinstance(m0, dict):
                        # verify not already ended
                        end_iso = m0.get("endDate")
                        try:
                            if end_iso:
                                dt_end = _dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                                end_ms = int(dt_end.timestamp()*1000)
                                now_ms_check = int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp()*1000)
                                # For initial discovery, if market already ended, try next candidate
                                if end_ms <= now_ms_check and cand_ts == ts:
                                    continue
                        except Exception:
                            pass
                        self._backoff_s = self.poll_interval
                        return self._parse_gamma_market(asset, m0, cand_ts)
            except Exception as e:
                continue
        # No market found for either candidate — let poll retry / fallback

        # Legacy fallback: use configured rest_market_url with old param shape (for tests / mock injectors)
        if self.rest_market_url and self.rest_market_url != gamma_url:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(self.rest_market_url, params={"asset": asset, "after": after_ts_ms})
                    if resp.status_code == 429:
                        if self.on_event:
                            self.on_event("rate_limited", {"asset": asset, "status": 429})
                        self._backoff_s = min(self._backoff_s * 2, self.backoff_max)
                        return None
                    resp.raise_for_status()
                    data = resp.json()
                    self._backoff_s = self.poll_interval
                    return self._parse_market_response(asset, data, after_ts_ms)
            except Exception as e:
                if self.on_event:
                    self.on_event("subscription_failed", {"asset": asset, "error": str(e)})
                return None
        return None

    def _parse_gamma_market(self, asset: str, data: dict, ts_seconds: int) -> Optional[MarketInfo]:
        import json
        import datetime

        condition_id = data.get("conditionId") or data.get("condition_id") or data.get("id")
        if not condition_id:
            return None
        # clobTokenIds is JSON-encoded string
        raw_tokens = data.get("clobTokenIds") or data.get("clob_token_ids") or "[]"
        try:
            tokens = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
        except Exception:
            tokens = []
        outcomes_raw = data.get("outcomes") or "[]"
        try:
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        except Exception:
            outcomes = ["Up", "Down"]

        up_token = down_token = None
        if isinstance(tokens, list) and len(tokens) >= 2 and isinstance(outcomes, list) and len(outcomes) >= 2:
            for idx, outcome in enumerate(outcomes):
                tid = tokens[idx] if idx < len(tokens) else None
                if not tid:
                    continue
                if str(outcome).lower() == "up":
                    up_token = tid
                elif str(outcome).lower() == "down":
                    down_token = tid
            # fallback if outcomes not as expected
            if not up_token:
                up_token = tokens[0]
            if not down_token:
                down_token = tokens[1] if len(tokens) > 1 else None
        elif isinstance(tokens, list) and len(tokens) >= 1:
            up_token = tokens[0]
            down_token = tokens[1] if len(tokens) > 1 else None

        if not up_token or not down_token:
            # incomplete market (maybe not yet fully created)
            return None

        # timestamps: endDate is ISO, start = end - 5min if missing
        end_iso = data.get("endDate") or data.get("end_date")
        start_iso = data.get("startDate") or data.get("start_date")
        try:
            if end_iso:
                dt_end = datetime.datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                end_ms = int(dt_end.timestamp() * 1000)
            else:
                end_ms = (ts_seconds + 300) * 1000
            if start_iso:
                dt_start = datetime.datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
                start_ms = int(dt_start.timestamp() * 1000)
            else:
                start_ms = ts_seconds * 1000
        except Exception:
            start_ms = ts_seconds * 1000
            end_ms = (ts_seconds + 300) * 1000

        # window_index deterministic from ts
        window_index = ts_seconds // 300  # stable across runs

        tick_raw = data.get("orderPriceMinTickSize") or data.get("order_price_min_tick_size") or 0.01
        try:
            tick_size = float(tick_raw)
        except Exception:
            tick_size = 0.01

        return MarketInfo(
            condition_id=str(condition_id),
            market_id=str(data.get("id") or condition_id),
            asset=asset.upper(),
            up_token_id=str(up_token),
            down_token_id=str(down_token),
            market_start_ts_ms=start_ms,
            market_end_ts_ms=end_ms,
            window_index=int(window_index),
            series_id=f"{asset.upper()}-{self.window_size_seconds}s",
            status="active" if data.get("active") else "active",
            question=data.get("question"),
            tick_size=tick_size,
        )

    def _parse_market_response(self, asset: str, data: dict, after_ts_ms: int) -> Optional[MarketInfo]:
        # Placeholder parser for injected fakes / legacy; real Polymarket response shape varies.
        # Accepts either single market dict or list
        if isinstance(data, list) and data:
            data = data[0]
        if not isinstance(data, dict):
            return None
        # Try to extract required fields; if missing, return None (discovered late vs no market — §1 #6)
        condition_id = data.get("condition_id") or data.get("conditionId") or data.get("id")
        if not condition_id:
            return None
        # tokens
        tokens = data.get("tokens") or data.get("token_ids") or []
        up_token = down_token = None
        if isinstance(tokens, list) and len(tokens) >= 2:
            # heuristic: first is UP, second DOWN or by outcome field
            for t in tokens:
                if isinstance(t, dict):
                    outcome = (t.get("outcome") or t.get("side") or "").lower()
                    tid = t.get("token_id") or t.get("tokenId") or t.get("id")
                    if outcome == "up":
                        up_token = tid
                    elif outcome == "down":
                        down_token = tid
                elif isinstance(t, str) and not up_token:
                    up_token = t
                elif isinstance(t, str) and not down_token:
                    down_token = t
            if not up_token and len(tokens) >= 1:
                up_token = tokens[0].get("token_id") if isinstance(tokens[0], dict) else tokens[0]
            if not down_token and len(tokens) >= 2:
                down_token = tokens[1].get("token_id") if isinstance(tokens[1], dict) else tokens[1]
        up_token = up_token or data.get("up_token_id") or f"{condition_id}-UP"
        down_token = down_token or data.get("down_token_id") or f"{condition_id}-DOWN"

        # timestamps (ms)
        start_ms = data.get("market_start_ts_ms") or data.get("start_time") or after_ts_ms
        end_ms = data.get("market_end_ts_ms") or data.get("end_time") or (after_ts_ms + 5 * 60 * 1000)
        # normalize to int ms
        try:
            start_ms = int(start_ms) if start_ms else after_ts_ms
            end_ms = int(end_ms) if end_ms else start_ms + 300_000
            # if values look like seconds ( < 1e12 ), convert
            if start_ms < 1e12:
                start_ms *= 1000
            if end_ms < 1e12:
                end_ms *= 1000
        except (TypeError, ValueError):
            start_ms = after_ts_ms
            end_ms = after_ts_ms + 300_000

        return MarketInfo(
            condition_id=str(condition_id),
            market_id=str(data.get("market_id") or data.get("marketId") or condition_id),
            asset=asset.upper(),
            up_token_id=str(up_token),
            down_token_id=str(down_token),
            market_start_ts_ms=start_ms,
            market_end_ts_ms=end_ms,
            window_index=int(data.get("window_index", 0)),
            series_id=data.get("series_id", f"{asset.upper()}-5MIN"),
            status=data.get("status", "active"),
            question=data.get("question"),
            tick_size=data.get("tick_size"),
        )


class RolloverManager:
    """Coordinates per-asset dual-tracking overlap (§1)."""

    def __init__(self, config, discovery: Optional[MarketDiscovery] = None, on_event=None):
        self.config = config
        self.discovery = discovery or MarketDiscovery(
            rest_market_url=config.ws.rest_market_url,
            poll_interval_s=config.discovery_poll_interval_seconds,
            backoff_max_s=config.discovery_backoff_max_seconds,
            on_event=on_event,
            window_size_seconds=config.test_mode.window_size_seconds,
        )
        self.on_event = on_event
        self.states: Dict[str, RolloverState] = {a.upper(): RolloverState(asset=a.upper()) for a in config.assets}
        self.lead_ms = config.rollover_lead_seconds * 1000
        self.max_gap_ms = config.max_coverage_gap_seconds * 1000

    async def check_and_roll(self, asset: str, subscribe_fn: Callable, now_ms: Optional[int] = None) -> Optional[str]:
        """Check if asset needs lookahead discovery and/or promotion.

        subscribe_fn: async callable(market: MarketInfo) → subscribe to feeds.
        Returns event type string if an event was emitted, else None.
        """
        asset = asset.upper()
        state = self.states[asset]
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)

        # Promotion: current ended, next becomes current
        if state.should_promote(now_ms):
            prev_cid = state.current.condition_id if state.current else None
            state.current = state.next
            state.next = None
            state.is_rollover_window = False
            state.rollover_miss_logged = False
            state.rollover_started_emitted = False
            if self.on_event:
                self.on_event("rollover_completed", {"asset": asset, "prev_condition_id": prev_cid, "new_condition_id": state.current.condition_id if state.current else None})
            return "rollover_completed"

        # Lookahead: need to discover next?
        if state.needs_rollover_lookahead(now_ms, self.lead_ms):
            # distinguish initial discovery (no current) from true rollover window
            is_initial = state.current is None
            if not is_initial:
                state.is_rollover_window = True
            after = state.current.market_end_ts_ms if state.current else now_ms
            # rate-limited polling — don't tight-loop (§1 #7)
            if state.last_discovery_attempt_ms and (now_ms - state.last_discovery_attempt_ms) < int(self.discovery._backoff_s * 1000):
                return None
            state.last_discovery_attempt_ms = now_ms
            # throttle rollover_started — emit once per window, not every poll (fixes 65k spam)
            # initial discovery phase does NOT emit rollover_started (would spam until first market found)
            should_emit_rollover = (not is_initial) and (not state.rollover_started_emitted)
            if should_emit_rollover:
                if self.on_event:
                    self.on_event("rollover_started", {"asset": asset, "after_ts_ms": after})
                state.rollover_started_emitted = True
            elif is_initial:
                state.initial_discovery_attempts += 1
                # throttle initial discovery logging: only first attempt or every 10th to avoid spam
                if state.initial_discovery_attempts == 1 and self.on_event:
                    # lightweight trace, not the heavy rollover_started event
                    self.on_event("rollover_started", {"asset": asset, "after_ts_ms": after, "initial": True})
            next_market = await self.discovery.fetch_next_market(asset, after)
            if next_market:
                # reset emission flags on success
                state.rollover_started_emitted = False  # allow next window to emit again
                state.initial_discovery_attempts = 0
                # for initial discovery, set current directly; for rollover, set next
                if state.current is None:
                    state.current = next_market
                    state.is_rollover_window = False
                    try:
                        await subscribe_fn(next_market)
                    except Exception as e:
                        if self.on_event:
                            self.on_event("subscription_failed", {"asset": asset, "condition_id": next_market.condition_id, "error": str(e)})
                    if self.on_event:
                        self.on_event("market_added", {"asset": asset, "condition_id": next_market.condition_id})
                    return "market_added"
                else:
                    state.next = next_market
                    # subscribe immediately (§1 step 2)
                    try:
                        await subscribe_fn(next_market)
                    except Exception as e:
                        if self.on_event:
                            self.on_event("subscription_failed", {"asset": asset, "condition_id": next_market.condition_id, "error": str(e)})
                    if self.on_event:
                        self.on_event("market_added", {"asset": asset, "condition_id": next_market.condition_id})
                    return "rollover_started" if should_emit_rollover else "market_added"
            else:
                # Distinguish discovered late vs no market existed (§1 #6)
                # If we're past market_end_ts + max_gap and still no next, it's a coverage_gap
                if state.current and now_ms > state.current.market_end_ts_ms + self.max_gap_ms:
                    if not state.rollover_miss_logged:
                        if self.on_event:
                            self.on_event("coverage_gap", {"asset": asset, "after": state.current.market_end_ts_ms, "now": now_ms})
                        state.rollover_miss_logged = True
                        return "coverage_gap"
                elif state.current and now_ms > state.current.market_end_ts_ms:
                    if not state.rollover_miss_logged:
                        if self.on_event:
                            self.on_event("rollover_miss", {"asset": asset, "after": state.current.market_end_ts_ms})
                        state.rollover_miss_logged = True
                        return "rollover_miss"
        else:
            # not in rollover window
            if state.current and (state.current.market_end_ts_ms - now_ms) > self.lead_ms:
                state.is_rollover_window = False
        return None

    def active_markets(self, asset: str) -> List[MarketInfo]:
        """Return list of currently active markets for asset (1 or 2 during overlap)."""
        state = self.states[asset.upper()]
        res: List[MarketInfo] = []
        if state.current:
            res.append(state.current)
        if state.next:
            res.append(state.next)
        return res

    def set_current(self, asset: str, market: MarketInfo) -> None:
        self.states[asset.upper()].current = market
