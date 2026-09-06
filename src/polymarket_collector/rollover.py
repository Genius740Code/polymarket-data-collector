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


def _window_label_for(window_size_seconds: int) -> str:
    """Map window_size_seconds -> label per plan.md §1.1."""
    if window_size_seconds >= 86400:
        return "1d"
    elif window_size_seconds >= 14400:
        return "4h"
    elif window_size_seconds >= 3600:
        return "1h"
    elif window_size_seconds >= 900:
        return "15m"
    else:
        return "5m"


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
    # §3.1 enrichment — slug/volume/liquidity/window_label per plan.md
    slug: Optional[str] = None
    window_label: Optional[str] = None
    window_size_seconds: Optional[int] = None
    reported_volume: Optional[float] = None
    reported_liquidity: Optional[float] = None

    def _derived_resolution_outcome(self) -> str:
        """Derive resolution vs unknown — until settlement fetch, keep unknown (no fabrication)."""
        # Gamma active/closed flag alone is not settlement; keep unknown until settlement_source populated
        return "unknown"

    def _derived_status(self) -> str:
        """Map Gamma active field to status: active / closed / resolved when settlement known."""
        # self.status is always "active" at discovery; lifecycle transitions happen via settlement
        # Keep "active" until market_end_ts_ms passed, then "closed" (resolved still requires settlement)
        import time as _t
        try:
            now_ms = int(_t.time() * 1000)
            if now_ms >= self.market_end_ts_ms:
                # market ended but not yet resolved via settlement fetch
                return "closed"
            return self.status or "active"
        except Exception:
            return self.status or "active"

    @property
    def market_end_ts(self) -> int:
        return self.market_end_ts_ms

    def to_markets_row(self, updated_at: Optional[str] = None) -> dict:
        """Convert to markets_log row dict per enriched MARKETS_SCHEMA (§3)."""
        import datetime as _dt
        now_iso = updated_at or _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
        def _ms_to_iso(ms: int) -> str:
            try:
                return _dt.datetime.fromtimestamp(ms/1000, tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                return now_iso
        return {
            "updated_at": now_iso,
            "recorded_at": now_iso,
            "market_start_ts": _ms_to_iso(self.market_start_ts_ms),
            "market_end_ts": _ms_to_iso(self.market_end_ts_ms),
            "market_start_ts_ms": int(self.market_start_ts_ms),
            "market_end_ts_ms": int(self.market_end_ts_ms),
            "condition_id": self.condition_id,
            "market_id": self.market_id,
            "slug": self.slug,
            "series_id": self.series_id,
            "window_index": int(self.window_index),
            "window_label": self.window_label or _window_label_for(self.window_size_seconds or 300),
            "window_size_seconds": int(self.window_size_seconds or 300),
            "asset": self.asset,
            "up_token_id": self.up_token_id,
            "down_token_id": self.down_token_id,
            "status": self._derived_status(),
            "resolution_outcome": self._derived_resolution_outcome(),
            "question": self.question,
            "tick_size": self.tick_size,
            "reported_volume": self.reported_volume,
            "reported_liquidity": self.reported_liquidity,
        }


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
        if not self.current:
            return False
        # Allow promotion even if next is None so we don't stall on missing market (gap)
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
        liquidity_filter=None,
    ):
        self.rest_market_url = rest_market_url
        self.poll_interval = poll_interval_s
        self.backoff_max = backoff_max_s
        self.on_event = on_event
        self._backoff_s = poll_interval_s
        self.window_size_seconds = window_size_seconds
        self.window_multiplier = window_size_seconds // 300
        self.liquidity_filter = liquidity_filter  # LiquidityFilterConfig or None

    def _slug_for(self, asset: str, ts_seconds: int) -> str:
        window_label = _window_label_for(self.window_size_seconds)
        # asset prefix is lower-case, e.g. btc-updown-5m-1787994000
        return f"{asset.lower()}-updown-{window_label}-{ts_seconds}"

    def _ts_for_after(self, after_ts_ms: int) -> int:
        # floor to configurable window boundary
        # e.g. 5min (300s), 15min (900s), 1h (3600s), 4h (14400s), 1d (86400s)
        return (after_ts_ms // 1000) // self.window_size_seconds * self.window_size_seconds

    async def fetch_next_market(self, asset: str, after_ts_ms: int, strict_adjacent: bool = False) -> Optional[MarketInfo]:
        """Query Gamma for the next market after after_ts_ms.

        strict_adjacent: during rollover lookahead only accept the ADJACENT next
        window. Far-future windows are sometimes indexed on Gamma BEFORE the
        adjacent one; adopting them as "next" skips a whole 5-minute window
        (the 16:10 UTC gap on 2026-09-05). Genuine missing windows stay handled
        by the rollover_miss/coverage_gap paths.
        """
        import httpx
        import json

        asset = asset.upper()
        ts = self._ts_for_after(after_ts_ms)
        slug = self._slug_for(asset, ts)
        gamma_url = f"{self.GAMMA_BASE}/markets"
        params = {"slug": slug}

        # Try Gamma first — slug is deterministic but may be indexed ~30-60s late.
        import datetime as _dt
        # Skip-ahead candidates only for initial discovery (no current market);
        # lookahead discovery must adopt the adjacent window or wait for it.
        if strict_adjacent:
            candidates = [ts]
        else:
            candidates = [ts, ts + self.window_size_seconds, ts + 2 * self.window_size_seconds]
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
                    m0 = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
                    if isinstance(m0, dict):
                        end_iso = m0.get("endDate")
                        try:
                            if end_iso:
                                dt_end = _dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                                end_ms = int(dt_end.timestamp()*1000)
                                now_ms_check = int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp()*1000)
                                # Only skip if this is the first candidate and market already ended long ago (>window)
                                if end_ms + self.window_size_seconds*1000 <= now_ms_check and cand_ts == ts:
                                    continue
                        except Exception:
                            pass
                        self._backoff_s = self.poll_interval
                        parsed = self._parse_gamma_market(asset, m0, cand_ts)
                        # liquidity filtering — no RPC, uses reported_liquidity/reported_volume only
                        if parsed is not None and not self._passes_liquidity_filter(parsed):
                            # try next candidate window instead of returning low-liq market
                            continue
                        if parsed is not None:
                            return parsed
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
                    parsed2 = self._parse_market_response(asset, data, after_ts_ms)
                    if parsed2 is not None and not self._passes_liquidity_filter(parsed2):
                        return None
                    return parsed2
            except Exception as e:
                if self.on_event:
                    self.on_event("subscription_failed", {"asset": asset, "error": repr(e), "phase": "discovery_poll"})
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

        # timestamps: endDate is ISO, start = end - window if missing (§3 hard-code fix)
        end_iso = data.get("endDate") or data.get("end_date")
        start_iso = data.get("startDate") or data.get("start_date")
        ws = self.window_size_seconds
        try:
            if end_iso:
                dt_end = datetime.datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                end_ms = int(dt_end.timestamp() * 1000)
            else:
                end_ms = (ts_seconds + ws) * 1000
            if start_iso:
                dt_start = datetime.datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
                start_ms = int(dt_start.timestamp() * 1000)
            else:
                start_ms = ts_seconds * 1000
            # HYPE bugfix: Gamma sometimes returns startDate ~24h off for low-liquidity markets (e.g. HYPE 2026-09-03 21:58 vs expected 21:50)
            # If start/end diverge from slug window by >2 windows, force deterministic window per slug (AGENT.md honest gaps not fabricated times)
            expected_start = ts_seconds * 1000
            expected_end = (ts_seconds + ws) * 1000
            if abs(start_ms - expected_start) > ws * 2000 or abs(end_ms - expected_end) > ws * 2000:
                start_ms = expected_start
                end_ms = expected_end
            # also fix if start >= end or duration != window
            if start_ms >= end_ms or (end_ms - start_ms) != ws * 1000:
                # prefer end_ms if it matches expected, otherwise use slug
                if abs(end_ms - expected_end) < 5000:
                    start_ms = end_ms - ws * 1000
                elif abs(start_ms - expected_start) < 5000:
                    end_ms = start_ms + ws * 1000
                else:
                    start_ms = expected_start
                    end_ms = expected_end
        except Exception:
            start_ms = ts_seconds * 1000
            end_ms = (ts_seconds + ws) * 1000

        # window_index deterministic from ts (§3 fix: use window, not 300)
        window_index = ts_seconds // ws  # stable across runs

        tick_raw = data.get("orderPriceMinTickSize") or data.get("order_price_min_tick_size") or 0.01
        try:
            tick_size = float(tick_raw)
        except Exception:
            tick_size = 0.01

        # §3.1 slug / §3.3 volume/liquidity population (robust fallback, fixes 86% null misread)
        slug_val = data.get("slug") or data.get("marketSlug") or data.get("market_slug") or self._slug_for(asset, ts_seconds)
        # volume / liquidity may be string/number under many keys; Gamma v2 uses volume/liquidity as strings
        reported_volume = (
            data.get("volumeNum") if data.get("volumeNum") is not None else
            data.get("volume_num") if data.get("volume_num") is not None else
            data.get("volume")
        )
        reported_liquidity = (
            data.get("liquidityNum") if data.get("liquidityNum") is not None else
            data.get("liquidity_num") if data.get("liquidity_num") is not None else
            data.get("liquidity")
        )
        # also try nested inside 'market' dict if present
        if reported_volume is None and isinstance(data.get("market"), dict):
            reported_volume = data["market"].get("volumeNum") or data["market"].get("volume")
        if reported_liquidity is None and isinstance(data.get("market"), dict):
            reported_liquidity = data["market"].get("liquidityNum") or data["market"].get("liquidity")
        # coerce to float where possible; empty string -> None (upcoming market has 0 volume, not error)
        def _to_float(v):
            if v is None or v == "" or (isinstance(v, str) and v.strip().lower() in ("none","null")):
                return None
            try:
                # handle comma string "1,234.56"
                if isinstance(v, str):
                    v = v.replace(",", "")
                return float(v)
            except Exception:
                return None
        reported_volume = _to_float(reported_volume)
        reported_liquidity = _to_float(reported_liquidity)

        window_label = _window_label_for(ws)

        return MarketInfo(
            condition_id=str(condition_id),
            market_id=str(data.get("id") or condition_id),
            asset=asset.upper(),
            up_token_id=str(up_token),
            down_token_id=str(down_token),
            market_start_ts_ms=start_ms,
            market_end_ts_ms=end_ms,
            window_index=int(window_index),
            series_id=f"{asset.upper()}-{window_label}",
            status="active" if data.get("active") else "active",
            question=data.get("question"),
            tick_size=tick_size,
            slug=str(slug_val) if slug_val else None,
            window_label=window_label,
            window_size_seconds=ws,
            reported_volume=reported_volume,
            reported_liquidity=reported_liquidity,
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

        # timestamps (ms) — §3 fix: use window not hardcoded 300s
        ws = self.window_size_seconds
        start_ms = data.get("market_start_ts_ms") or data.get("start_time") or after_ts_ms
        end_ms = data.get("market_end_ts_ms") or data.get("end_time") or (after_ts_ms + ws * 1000)
        # normalize to int ms
        try:
            start_ms = int(start_ms) if start_ms else after_ts_ms
            end_ms = int(end_ms) if end_ms else start_ms + ws * 1000
            # if values look like seconds ( < 1e12 ), convert
            if start_ms < 1e12:
                start_ms *= 1000
            if end_ms < 1e12:
                end_ms *= 1000
        except (TypeError, ValueError):
            start_ms = after_ts_ms
            end_ms = after_ts_ms + ws * 1000

        # window_index: prefer provided else compute from ts
        if "window_index" in data and data.get("window_index") is not None:
            try:
                window_index = int(data.get("window_index"))
            except Exception:
                window_index = (start_ms // 1000) // ws
        else:
            window_index = (start_ms // 1000) // ws

        window_label = _window_label_for(ws)
        slug_val = data.get("slug") or data.get("marketSlug") or None
        if not slug_val:
            try:
                ts_secs = int(start_ms // 1000) // ws * ws
                slug_val = self._slug_for(asset, ts_secs)
            except Exception:
                slug_val = None

        return MarketInfo(
            condition_id=str(condition_id),
            market_id=str(data.get("market_id") or data.get("marketId") or condition_id),
            asset=asset.upper(),
            up_token_id=str(up_token),
            down_token_id=str(down_token),
            market_start_ts_ms=start_ms,
            market_end_ts_ms=end_ms,
            window_index=int(window_index),
            series_id=data.get("series_id", f"{asset.upper()}-{window_label}"),
            status=data.get("status", "active"),
            question=data.get("question"),
            tick_size=data.get("tick_size"),
            slug=str(slug_val) if slug_val else None,
            window_label=window_label,
            window_size_seconds=ws,
            reported_volume=data.get("reported_volume") or data.get("volumeNum") or data.get("volume"),
            reported_liquidity=data.get("reported_liquidity") or data.get("liquidityNum") or data.get("liquidity"),
        )

    def _passes_liquidity_filter(self, market: MarketInfo) -> bool:
        """Liquidity filtering — no RPC, uses reported_volume/liquidity only.
        Returns True if market passes filter or filtering disabled.
        Emits low_liquidity event if filtered out.
        """
        lf = self.liquidity_filter
        if lf is None or not getattr(lf, "enabled", False):
            return True
        min_liq = getattr(lf, "min_liquidity", 0) or 0
        min_vol = getattr(lf, "min_volume", 0) or 0
        # treat None as 0 for filtering (unknown liquidity = fail when threshold >0)
        liq = market.reported_liquidity if market.reported_liquidity is not None else 0
        vol = market.reported_volume if market.reported_volume is not None else 0
        try:
            liq_f = float(liq)
        except Exception:
            liq_f = 0
        try:
            vol_f = float(vol)
        except Exception:
            vol_f = 0
        if min_liq and liq_f < float(min_liq):
            if self.on_event:
                try:
                    self.on_event("low_liquidity", {
                        "asset": market.asset,
                        "condition_id": market.condition_id,
                        "slug": market.slug,
                        "reported_liquidity": liq_f,
                        "required": float(min_liq),
                        "reason": f"liquidity {liq_f} < {min_liq}",
                    })
                except Exception:
                    pass
            return False
        if min_vol and vol_f < float(min_vol):
            if self.on_event:
                try:
                    self.on_event("low_liquidity", {
                        "asset": market.asset,
                        "condition_id": market.condition_id,
                        "slug": market.slug,
                        "reported_volume": vol_f,
                        "required": float(min_vol),
                        "reason": f"volume {vol_f} < {min_vol}",
                    })
                except Exception:
                    pass
            return False
        return True


class RolloverManager:
    """Coordinates per-asset dual-tracking overlap (§1)."""

    def __init__(self, config, discovery: Optional[MarketDiscovery] = None, on_event=None):
        self.config = config
        # §3.5 fix: use config.window_size_seconds not test_mode.window_size_seconds
        # fallback to test_mode only if window_size_seconds missing (backward compat)
        ws = getattr(config, "window_size_seconds", None)
        if ws is None:
            ws = getattr(getattr(config, "test_mode", None), "window_size_seconds", 300)
        # liquidity filter config (no RPC)
        lf = getattr(config, "liquidity_filter", None)
        self.discovery = discovery or MarketDiscovery(
            rest_market_url=config.ws.rest_market_url,
            poll_interval_s=config.discovery_poll_interval_seconds,
            backoff_max_s=config.discovery_backoff_max_seconds,
            on_event=on_event,
            window_size_seconds=ws,
            liquidity_filter=lf,
        )
        # if external discovery passed, inject liquidity_filter if missing
        if discovery is not None and getattr(discovery, "liquidity_filter", None) is None and lf is not None:
            discovery.liquidity_filter = lf
        self.on_event = on_event
        self.states: Dict[str, RolloverState] = {a.upper(): RolloverState(asset=a.upper()) for a in config.assets}
        self.lead_ms = config.rollover_lead_seconds * 1000
        self.max_gap_ms = config.max_coverage_gap_seconds * 1000

    def _synthetic_market(self, asset: str, after_ts_ms: int) -> MarketInfo:
        """REMOVED: synthetic markets permanently disabled - never generate fake markets."""
        raise RuntimeError("synthetic markets disabled - _synthetic_market should never be called")

    async def check_and_roll(self, asset: str, subscribe_fn: Callable, now_ms: Optional[int] = None) -> Optional[str]:
        """Check if asset needs lookahead discovery and/or promotion.

        subscribe_fn: async callable(market: MarketInfo) → subscribe to feeds.
        Returns event type string if an event was emitted, else None.
        """
        asset = asset.upper()
        state = self.states[asset]
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)

        # Promotion: current ended, next becomes current (even if next is None -> gap)
        if state.should_promote(now_ms):
            prev_cid = state.current.condition_id if state.current else None
            was_next_none = state.next is None
            state.current = state.next
            state.next = None
            state.is_rollover_window = False
            state.rollover_miss_logged = False
            state.rollover_started_emitted = False
            if self.on_event:
                if was_next_none and prev_cid:
                    self.on_event("coverage_gap", {"asset": asset, "prev_condition_id": prev_cid, "now": now_ms})
                self.on_event("rollover_completed", {"asset": asset, "prev_condition_id": prev_cid, "new_condition_id": state.current.condition_id if state.current else None})
            # No synthetic fallback - if next is None, stay gapped and emit coverage_gap
            return "coverage_gap" if was_next_none else "rollover_completed"

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
            next_market = await self.discovery.fetch_next_market(asset, after, strict_adjacent=not is_initial)
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
                            self.on_event("subscription_failed", {"asset": asset, "condition_id": next_market.condition_id, "error": repr(e)})
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
                            self.on_event("subscription_failed", {"asset": asset, "condition_id": next_market.condition_id, "error": repr(e)})
                    if self.on_event:
                        self.on_event("market_added", {"asset": asset, "condition_id": next_market.condition_id})
                    return "rollover_started" if should_emit_rollover else "market_added"
            else:
                # No synthetic fallback - distinguish discovered late vs no market existed (§1 #6)
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
