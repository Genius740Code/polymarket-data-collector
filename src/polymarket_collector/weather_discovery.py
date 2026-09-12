"""Weather market discovery — Gamma event-search based (NOT deterministic slug).

Crypto discovery (rollover.py) derives ``{asset}-updown-{label}-{ts}`` slugs on a
fixed time grid. Weather markets are city-day bracket events
(``highest/lowest-temperature-in-{city}-on-{date}``), each holding ~11 Yes/No
bracket markets with a shared endDate. There is no time grid to derive, so this
module searches Gamma (public-search + /events/{id} expansion) and maps every
bracket to a MarketInfo with Yes->up / No->down token mapping. Schemas, WS,
book engine and writers are reused unchanged.

Real-data-only: incomplete markets (missing tokens) are skipped, never
fabricated; failures emit throttled discovery_poll events; empty results are
honest (caller logs coverage_gap, never synthetic fill).
"""
from __future__ import annotations

import datetime
import json
import time
from typing import Dict, List, Optional

from .rollover import MarketInfo, clean_market_id


def city_slug(asset: str) -> str:
    """Config asset (e.g. HONG-KONG) -> Gamma ticker city part (hong-kong)."""
    return asset.strip().lower().replace("_", "-").replace(" ", "-")


def city_pretty(asset: str) -> str:
    """Config asset -> search text (hong-kong -> 'hong kong')."""
    return city_slug(asset).replace("-", " ")


def _to_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    if isinstance(v, str):
        if v.strip().lower() in ("none", "null"):
            return None
        v = v.replace(",", "")
    try:
        return float(v)
    except Exception:
        return None


def _parse_bracket_market(
    asset: str,
    series_id: str,
    data: dict,
    mode: str,
) -> Optional[MarketInfo]:
    """Map one Gamma bracket market (Yes/No) to MarketInfo (Yes->up, No->down)."""
    condition_id = data.get("conditionId") or data.get("condition_id")
    if not condition_id:
        return None
    raw_tokens = data.get("clobTokenIds") or data.get("clob_token_ids") or "[]"
    try:
        tokens = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
    except Exception:
        tokens = []
    outcomes_raw = data.get("outcomes") or "[]"
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
    except Exception:
        outcomes = ["Yes", "No"]
    up_token = down_token = None
    if isinstance(tokens, list) and isinstance(outcomes, list):
        for idx, outcome in enumerate(outcomes):
            if idx >= len(tokens) or not tokens[idx]:
                continue
            if str(outcome).strip().lower() == "yes":
                up_token = tokens[idx]
            elif str(outcome).strip().lower() == "no":
                down_token = tokens[idx]
        if not up_token and len(tokens) >= 1:
            up_token = tokens[0]
        if not down_token and len(tokens) >= 2:
            down_token = tokens[1]
    if not up_token or not down_token:
        return None  # incomplete bracket — skip honestly, never fabricate

    end_iso = data.get("endDate") or data.get("end_date")
    start_iso = data.get("startDate") or data.get("start_date")
    try:
        if end_iso:
            end_ms = int(datetime.datetime.fromisoformat(
                str(end_iso).replace("Z", "+00:00")).timestamp() * 1000)
        else:
            return None  # no settlement time -> cannot track lifecycle
        if start_iso:
            start_ms = int(datetime.datetime.fromisoformat(
                str(start_iso).replace("Z", "+00:00")).timestamp() * 1000)
        else:
            start_ms = end_ms - 86400 * 1000
        if start_ms >= end_ms:
            start_ms = end_ms - 86400 * 1000
    except Exception:
        return None

    window_index = int(end_ms // 86400000)  # stable per city-day event
    slug_val = data.get("slug") or data.get("marketSlug")
    reported_volume = _to_float(
        data.get("volumeNum", data.get("volume_num", data.get("volume"))))
    reported_liquidity = _to_float(
        data.get("liquidityNum", data.get("liquidity_num", data.get("liquidity"))))
    tick_raw = data.get("orderPriceMinTickSize") or data.get("order_price_min_tick_size") or 0.01
    try:
        tick_size = float(tick_raw)
    except Exception:
        tick_size = 0.01
    return MarketInfo(
        condition_id=str(condition_id),
        market_id=clean_market_id(data.get("id")),
        asset=asset.upper(),
        up_token_id=str(up_token),
        down_token_id=str(down_token),
        market_start_ts_ms=start_ms,
        market_end_ts_ms=end_ms,
        window_index=window_index,
        series_id=series_id,
        status="active",
        question=data.get("question"),
        tick_size=tick_size,
        slug=str(slug_val) if slug_val else None,
        window_label="1d",
        window_size_seconds=86400,
        reported_volume=reported_volume,
        reported_liquidity=reported_liquidity,
    )


class WeatherDiscovery:
    """Gamma event-search discovery for one mode (high|low).

    mode=high tracks tickers ``highest-temperature-in-{city}-on-*``,
    mode=low tracks ``lowest-temperature-in-{city}-on-*``.
    """

    GAMMA_BASE = "https://gamma-api.polymarket.com"
    # Only events ending in this window are tradeable now (today + next 2 days,
    # plus 12h grace for just-ended events whose books still print late trades).
    LOOKBACK_MS = 12 * 3600 * 1000
    LOOKAHEAD_MS = 72 * 3600 * 1000
    MAX_EVENTS_PER_CITY = 3  # nearest 3 by endDate — bounds Gamma load

    def __init__(self, mode: str, on_event=None, liquidity_filter=None,
                 poll_interval_s: float = 60.0):
        mode = str(mode).strip().lower()
        if mode not in ("high", "low"):
            raise ValueError(f"WeatherDiscovery mode must be high|low, got {mode!r}")
        self.mode = mode
        self.prefix = "highest" if mode == "high" else "lowest"
        self.series_id = f"WEATHER-{mode.upper()}-1D"
        self.on_event = on_event
        self.liquidity_filter = liquidity_filter
        self.poll_interval_s = poll_interval_s
        self._last_fail_event_ms: Dict[str, int] = {}

    def _note_failure(self, asset: str, detail: dict) -> None:
        if not self.on_event:
            return
        try:
            now_ms = int(time.time() * 1000)
            if now_ms - self._last_fail_event_ms.get(asset.upper(), 0) < 60_000:
                return
            self._last_fail_event_ms[asset.upper()] = now_ms
            payload = {"asset": asset.upper(), "mode": self.mode}
            payload.update(detail)
            self.on_event("discovery_poll", payload)
        except Exception:
            pass

    def _passes_liquidity_filter(self, market: MarketInfo) -> bool:
        lf = self.liquidity_filter
        if lf is None or not getattr(lf, "enabled", False):
            return True
        try:
            min_liq = float(getattr(lf, "min_liquidity", 0) or 0)
            min_vol = float(getattr(lf, "min_volume", 0) or 0)
        except Exception:
            return True
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
        if min_liq and liq_f < min_liq:
            if self.on_event:
                try:
                    self.on_event("low_liquidity", {
                        "asset": market.asset, "condition_id": market.condition_id,
                        "slug": market.slug, "reported_liquidity": liq_f,
                        "required": min_liq,
                        "reason": f"liquidity {liq_f} < {min_liq}",
                    })
                except Exception:
                    pass
            return False
        if min_vol and vol_f < min_vol:
            if self.on_event:
                try:
                    self.on_event("low_liquidity", {
                        "asset": market.asset, "condition_id": market.condition_id,
                        "slug": market.slug, "reported_volume": vol_f,
                        "required": min_vol,
                        "reason": f"volume {vol_f} < {min_vol}",
                    })
                except Exception:
                    pass
            return False
        return True

    async def discover_city_markets(self, asset: str) -> List[MarketInfo]:
        """Full active bracket set for one city. Never raises — [] is honest."""
        import httpx

        asset = asset.upper()
        slug = city_slug(asset)
        want_prefix = f"{self.prefix}-temperature-in-{slug}-on-"
        now_ms = int(time.time() * 1000)
        found: List[MarketInfo] = []
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{self.GAMMA_BASE}/public-search",
                    params={"q": f"{self.prefix} temperature in {city_pretty(asset)}",
                            "limit": 10},
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                resp.raise_for_status()
                body = resp.json()
                events = body.get("events") if isinstance(body, dict) else None
                if not isinstance(events, list):
                    self._note_failure(asset, {"phase": "search", "detail": "no events list"})
                    return []
                cands = []
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    ticker = str(ev.get("ticker") or ev.get("slug") or "")
                    if not ticker.startswith(want_prefix):
                        continue
                    try:
                        end_iso = ev.get("endDate")
                        end_ms = int(datetime.datetime.fromisoformat(
                            str(end_iso).replace("Z", "+00:00")).timestamp() * 1000)
                    except Exception:
                        continue
                    if end_ms < now_ms - self.LOOKBACK_MS or end_ms > now_ms + self.LOOKAHEAD_MS:
                        continue
                    cands.append((end_ms, ev.get("id"), ticker))
                cands.sort(key=lambda t: t[0])
                for end_ms, eid, ticker in cands[: self.MAX_EVENTS_PER_CITY]:
                    try:
                        er = await client.get(
                            f"{self.GAMMA_BASE}/events/{eid}",
                            headers={"User-Agent": "Mozilla/5.0"},
                        )
                        er.raise_for_status()
                        ej = er.json()
                        brackets = ej.get("markets") if isinstance(ej, dict) else None
                        if not isinstance(brackets, list):
                            continue
                        for b in brackets:
                            if not isinstance(b, dict):
                                continue
                            m = _parse_bracket_market(asset, self.series_id, b, self.mode)
                            if m is None:
                                continue
                            if self._passes_liquidity_filter(m):
                                found.append(m)
                    except Exception as e:
                        self._note_failure(asset, {"phase": "expand", "ticker": ticker,
                                                   "error": repr(e)})
                        continue
        except Exception as e:
            self._note_failure(asset, {"phase": "search", "error": repr(e)})
            return []
        return found
