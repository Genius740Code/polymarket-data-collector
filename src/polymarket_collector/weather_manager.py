"""Weather event-set manager — drop-in replacement for RolloverManager.

Crypto rollover tracks ONE current/next market per (asset, timeframe) on a
deterministic time grid. A weather city-day holds ~11 concurrent Yes/No
bracket markets with no grid, so this manager tracks a SET of active brackets
per city and re-polls Gamma on a slow cadence (default 60s per city).

Implements the exact RolloverManager surface Collector uses
(enabled_lane_labels / lane_ws / active_markets / check_and_roll_all /
state_for / state_for_market / set_enabled_lanes / primary_tf / discovery)
so collector.py needs zero changes — cli_weather.py just assigns
``collector.rollover = WeatherManager(...)`` after construction.

Cursor compat: states expose RolloverState with .current = nearest-ending
active bracket (representative for the 1-row-per-lane cursor). Restart
re-discovers the full bracket set from Gamma and recreates books as stale —
honest, logged, never fabricated.
"""
from __future__ import annotations

import time
from collections.abc import Callable
from typing import Dict, List, Optional

from .rollover import MarketInfo, RolloverState
from .weather_discovery import WeatherDiscovery


class WeatherManager:
    """Per-city bracket-set tracker for one mode (high|low), single 1d lane."""

    def __init__(self, config, discovery: Optional[WeatherDiscovery] = None,
                 on_event=None):
        self.config = config
        self.on_event = on_event
        self.discovery = discovery or WeatherDiscovery(
            mode=_infer_mode(config),
            on_event=on_event,
            liquidity_filter=getattr(config, "liquidity_filter", None),
            poll_interval_s=float(getattr(config, "discovery_poll_interval_seconds", 60)),
        )
        self.primary_tf = "1d"
        self.lane_ws: Dict[str, int] = {"1d": 86400}
        self.lane_poll_s: Dict[str, float] = {
            "1d": float(getattr(config, "discovery_poll_interval_seconds", 60))}
        self.lane_lead_ms: Dict[str, int] = {"1d": 0}
        self.states: Dict[tuple, RolloverState] = {
            (a.upper(), "1d"): RolloverState(asset=a.upper(), tf="1d")
            for a in config.assets
        }
        self.enabled_lanes: Optional[set] = {"1d"}
        # condition_id -> MarketInfo, all brackets ever seen this process
        self._known: Dict[str, MarketInfo] = {}
        self._last_poll_ms: Dict[str, int] = {}
        self._empty_polls: Dict[str, int] = {}

    # -- lane helpers (RolloverManager-compatible) ---------------------------
    def enabled_lane_labels(self) -> List[str]:
        if self.enabled_lanes is None:
            return ["1d"]
        return [tf for tf in ("1d",) if tf in self.enabled_lanes]

    def set_enabled_lanes(self, labels) -> None:
        self.enabled_lanes = {str(t).lower() for t in labels}

    def state_for(self, asset: str, tf: str) -> Optional[RolloverState]:
        return self.states.get((asset.upper(), str(tf).lower()))

    def state_for_market(self, market: MarketInfo) -> Optional[RolloverState]:
        try:
            ws = int(getattr(market, "window_size_seconds", 0) or 86400)
        except Exception:
            ws = 86400
        tf = "1d" if ws >= 86400 else "1d"
        return self.states.get((market.asset.upper(), tf))

    def active_markets(self, asset: str) -> List[MarketInfo]:
        au = asset.upper()
        now_ms = int(time.time() * 1000)
        # Keep recently-ended brackets around for 1h so late trades/closes
        # still resolve against a known market instead of an orphan token.
        return [m for m in self._known.values()
                if m.asset.upper() == au
                and now_ms < m.market_end_ts_ms + 3600 * 1000]

    # -- discovery -----------------------------------------------------------
    async def check_and_roll_all(self, asset: str, subscribe_fn: Callable,
                                 now_ms: Optional[int] = None) -> Optional[str]:
        asset = asset.upper()
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        poll_s = self.lane_poll_s["1d"]
        last = self._last_poll_ms.get(asset, 0)
        if now_ms - last < int(poll_s * 1000):
            return None
        self._last_poll_ms[asset] = now_ms
        try:
            brackets = await self.discovery.discover_city_markets(asset)
        except Exception:
            return None
        if not brackets:
            self._empty_polls[asset] = self._empty_polls.get(asset, 0) + 1
            if self.on_event and self._empty_polls[asset] in (1, 5, 15):
                try:
                    self.on_event("discovery_poll", {
                        "asset": asset, "mode": self.discovery.mode,
                        "empty_polls": self._empty_polls[asset],
                        "detail": "no active brackets in window",
                    })
                except Exception:
                    pass
            return None
        self._empty_polls[asset] = 0
        added = 0
        for m in brackets:
            if m.condition_id in self._known:
                continue
            self._known[m.condition_id] = m
            try:
                await subscribe_fn(m)
            except Exception as e:
                if self.on_event:
                    try:
                        self.on_event("subscription_failed", {
                            "asset": asset, "condition_id": m.condition_id,
                            "error": repr(e)})
                    except Exception:
                        pass
                continue
            added += 1
            if self.on_event:
                try:
                    self.on_event("market_added", {
                        "asset": asset, "condition_id": m.condition_id,
                        "slug": m.slug, "mode": self.discovery.mode})
                except Exception:
                    pass
        # representative cursor state = nearest-ending live bracket
        try:
            live = [m for m in self._known.values()
                    if m.asset.upper() == asset and m.market_end_ts_ms > now_ms]
            live.sort(key=lambda m: m.market_end_ts_ms)
            st = self.states.get((asset, "1d"))
            if st is not None:
                st.current = live[0] if live else st.current
                st.next = None
                st.is_rollover_window = False
        except Exception:
            pass
        return "market_added" if added else None


def _infer_mode(config) -> str:
    """High|low from the weather config's Kaggle prefix or series_ids."""
    try:
        prefix = str(getattr(getattr(config, "kaggle", None),
                             "dataset_prefix", "") or "").lower()
        if "low" in prefix:
            return "low"
        if "high" in prefix:
            return "high"
    except Exception:
        pass
    try:
        for v in (getattr(config, "series_ids", {}) or {}).values():
            s = str(v).lower()
            if "low" in s:
                return "low"
            if "high" in s:
                return "high"
    except Exception:
        pass
    raise ValueError("cannot infer weather mode (high|low) from config — "
                     "kaggle.dataset_prefix must contain 'high' or 'low'")
