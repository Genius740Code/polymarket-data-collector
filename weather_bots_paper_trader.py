"""Three independent Polymarket weather-market paper-trading strategies,
sharing one data layer, one execution/slippage model, and one CSV trade log.

Strategies
----------
1. MultiStationConsensusBot
   Pulls METAR observations from N nearby stations for a market. Only
   trades when every station agrees on which side of the strike
   temperature conditions currently point to. Disagreement = skip.

2. LateWindowGapBot
   Late in the observation window, compares the current observed temp
   to the strike, and to how much the temp could plausibly still move
   given the time remaining (a configurable max F/hour drift rate).
   If the gap is too large to close in the time left, takes the
   favorite side.

3. ForecastMispricingBot
   Pulls the NWS hourly gridpoint forecast, converts the point forecast
   into a rough probability of exceeding the strike using a normal
   error model (configurable forecast sigma), and trades when that
   model probability diverges meaningfully from Polymarket's price.

Data sources (all public, no API key required):
  - METAR observations : aviationweather.gov Data API
  - NWS gridpoint forecast : api.weather.gov  (needs a User-Agent header)
  - Polymarket market price / order book : clob.polymarket.com

THIS IS A PAPER TRADER. No real orders are ever submitted. Fills are
simulated by walking the *live* order book for realistic slippage, plus
a configurable taker fee. Verify Polymarket's current fee schedule
yourself before assuming fee_bps=0 is accurate long-term.

Fill in the MARKETS list below with your real token IDs / stations /
coordinates / strikes before running.
"""

from __future__ import annotations

import csv
import math
import os
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Dict, Tuple

import requests

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

NWS_USER_AGENT = "weather-bots-paper-trader (contact: you@example.com)"  # NWS requires a real UA
TRADE_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weather_bot_trades.csv")
POLL_INTERVAL_SECONDS = 60

# Execution assumptions — tune these to what you actually observe.
FEE_BPS = 0.0                 # Polymarket taker fee in bps of notional. Verify current schedule.
FLOOR_SLIPPAGE_BPS = 15.0     # extra slippage/latency haircut applied on top of book-walk price
PAPER_STARTING_BANKROLL = 1000.0
PER_TRADE_NOTIONAL_USD = 25.0

# Strategy-specific knobs
CONSENSUS_MIN_STATIONS_AGREE = 3      # all N stations must agree (set == len(station_ids))
LATE_WINDOW_TRIGGER_MINUTES = 90      # only evaluate gap-bot inside this many minutes of window close
LATE_WINDOW_MAX_DRIFT_F_PER_HR = 3.0  # assumed max plausible temp swing, °F/hour
FORECAST_SIGMA_F = 3.0                # assumed 1-sigma NWS forecast error, °F
FORECAST_EDGE_THRESHOLD = 0.08        # min |model_prob - market_price| to trade


@dataclass
class MarketConfig:
    name: str                 # your label, e.g. "NYC_2026-09-05_high_gt_82"
    token_id: str              # Polymarket CLOB token id for the side you'd BUY if bullish on strike
    station_ids: List[str]     # METAR ICAO codes for consensus bot, e.g. ["KNYC", "KJFK", "KLGA"]
    lat: float                 # for NWS forecast lookup
    lon: float
    strike_f: float            # temperature threshold the market resolves on
    direction: str              # "above" -> token pays out if final temp > strike_f, else "below"
    window_close_utc: datetime  # when the observation window for this market closes

    def resolves_true_if_temp(self, temp_f: float) -> bool:
        return temp_f > self.strike_f if self.direction == "above" else temp_f < self.strike_f


# EDIT THIS with your real markets before running.
MARKETS: List[MarketConfig] = [
    MarketConfig(
        name="EXAMPLE_NYC_high_gt_82F",
        token_id="REPLACE_WITH_REAL_TOKEN_ID",
        station_ids=["KNYC", "KJFK", "KLGA"],
        lat=40.7128,
        lon=-74.0060,
        strike_f=82.0,
        direction="above",
        window_close_utc=datetime(2026, 9, 5, 23, 59, tzinfo=timezone.utc),
    ),
]

PLACEHOLDER_TOKENS = {"", "REPLACE_WITH_REAL_TOKEN_ID", "REPLACE_ME"}


def _is_configured(market: MarketConfig) -> bool:
    return market.token_id not in PLACEHOLDER_TOKENS


# --------------------------------------------------------------------------
# Data clients
# --------------------------------------------------------------------------

class METARClient:
    BASE = "https://aviationweather.gov/api/data/metar"

    def fetch_temps_f(self, station_ids: List[str]) -> Dict[str, Optional[float]]:
        """Returns latest observed temp in F per station ID. Missing/failed -> None."""
        params = {"ids": ",".join(station_ids), "format": "json", "hours": 2}
        out: Dict[str, Optional[float]] = {s: None for s in station_ids}
        try:
            r = requests.get(self.BASE, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
        except Exception:
            traceback.print_exc()
            return out
        # response is a list of obs, most recent first per station typically
        for obs in data:
            sid = obs.get("icaoId") or obs.get("station_id") or obs.get("id")
            temp_c = obs.get("temp")
            if sid in out and out[sid] is None and temp_c is not None:
                out[sid] = temp_c * 9.0 / 5.0 + 32.0
        return out


class NWSClient:
    HEADERS = {"User-Agent": NWS_USER_AGENT}

    def hourly_forecast_temps_f(self, lat: float, lon: float, hours_ahead: int = 12) -> List[Tuple[datetime, float]]:
        """Returns list of (start_time_utc, temp_f) for the next `hours_ahead` forecast periods."""
        try:
            points_url = f"https://api.weather.gov/points/{lat},{lon}"
            points = requests.get(points_url, headers=self.HEADERS, timeout=10).json()
            hourly_url = points["properties"]["forecastHourly"]
            fc = requests.get(hourly_url, headers=self.HEADERS, timeout=10).json()
            periods = fc["properties"]["periods"][:hours_ahead]
        except Exception:
            traceback.print_exc()
            return []
        out = []
        for p in periods:
            try:
                t = datetime.fromisoformat(p["startTime"])
                temp = float(p["temperature"])
                # FIX: NWS may return degC depending on unit config — normalize to F.
                unit = str(p.get("temperatureUnit", "F")).upper()
                if unit.startswith("C"):
                    temp = temp * 9.0 / 5.0 + 32.0
                out.append((t, temp))
            except Exception:
                continue
        return out


class PolymarketClient:
    BASE = "https://clob.polymarket.com"

    def get_price(self, token_id: str, side: str = "buy") -> Optional[float]:
        try:
            r = requests.get(f"{self.BASE}/price", params={"token_id": token_id, "side": side}, timeout=10)
            r.raise_for_status()
            return float(r.json()["price"])
        except Exception:
            traceback.print_exc()
            return None

    def get_book(self, token_id: str) -> Optional[dict]:
        try:
            r = requests.get(f"{self.BASE}/book", params={"token_id": token_id}, timeout=10)
            r.raise_for_status()
            return r.json()  # {"bids": [...], "asks": [...]} entries may be dicts or [price, size] pairs
        except Exception:
            traceback.print_exc()
            return None


# --------------------------------------------------------------------------
# Execution model: walk the real book, add a slippage floor + fee
# --------------------------------------------------------------------------

@dataclass
class FillResult:
    avg_price: float
    filled_shares: float
    fee_usd: float
    slippage_usd: float
    best_ask: float


def _normalize_levels(levels) -> List[Tuple[float, float]]:
    """FIX: CLOB /book returns [{"price": "0.5", "size": "100"}] dicts, while the
    WSS feed and older docs use [[price, size]] pairs. Accept both."""
    norm: List[Tuple[float, float]] = []
    for lvl in levels or []:
        try:
            if isinstance(lvl, dict):
                norm.append((float(lvl["price"]), float(lvl["size"])))
            else:
                p, s = lvl
                norm.append((float(p), float(s)))
        except Exception:
            continue
    return norm


class PaperExecutionEngine:
    def __init__(self, fee_bps: float = FEE_BPS, floor_slippage_bps: float = FLOOR_SLIPPAGE_BPS):
        self.fee_bps = fee_bps
        self.floor_slippage_bps = floor_slippage_bps

    def simulate_buy(self, book: dict, notional_usd: float) -> Optional[FillResult]:
        asks = sorted(_normalize_levels(book.get("asks", [])), key=lambda x: x[0])
        if not asks:
            return None
        remaining = notional_usd
        cost = 0.0
        shares = 0.0
        for price, size in asks:
            level_notional = price * size
            take_notional = min(remaining, level_notional)
            if take_notional <= 0:
                continue
            shares += take_notional / price
            cost += take_notional
            remaining -= take_notional
            if remaining <= 1e-9:
                break
        if shares == 0:
            return None
        raw_avg_price = cost / shares
        avg_price = raw_avg_price * (1 + self.floor_slippage_bps / 10000.0)
        fee_usd = cost * (self.fee_bps / 10000.0)
        best_ask = asks[0][0]
        slippage_usd = (avg_price - best_ask) * shares
        return FillResult(avg_price=avg_price, filled_shares=shares, fee_usd=fee_usd,
                           slippage_usd=slippage_usd, best_ask=best_ask)


# --------------------------------------------------------------------------
# Trade logging
# --------------------------------------------------------------------------

class TradeLogger:
    FIELDS = [
        "timestamp_utc", "bot", "market", "side", "filled_shares", "notional_usd",
        "avg_fill_price", "fee_usd", "slippage_usd", "reason", "strike_f",
        "model_edge", "bankroll_after",
    ]

    def __init__(self, path: str = TRADE_LOG_PATH):
        self.path = path
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(self.FIELDS)

    def log(self, **kwargs) -> None:
        row = [kwargs.get(k, "") for k in self.FIELDS]
        with open(self.path, "a", newline="") as f:
            csv.writer(f).writerow(row)


# --------------------------------------------------------------------------
# Shared paper account (one bankroll per bot instance)
# --------------------------------------------------------------------------

class PaperAccount:
    def __init__(self, starting_bankroll: float = PAPER_STARTING_BANKROLL):
        self.bankroll = starting_bankroll

    def debit(self, amount: float) -> None:
        self.bankroll -= amount


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# --------------------------------------------------------------------------
# Bot 1: Multi-station consensus
# --------------------------------------------------------------------------

class MultiStationConsensusBot:
    name = "multi_station_consensus"

    def __init__(self, metar: METARClient, poly: PolymarketClient, engine: PaperExecutionEngine,
                 logger: TradeLogger, account: PaperAccount):
        self.metar = metar
        self.poly = poly
        self.engine = engine
        self.logger = logger
        self.account = account
        self._traded_today = set()  # market names already traded, reset externally per window

    def evaluate(self, market: MarketConfig) -> None:
        if market.name in self._traded_today:
            return

        temps = self.metar.fetch_temps_f(market.station_ids)
        valid = {s: t for s, t in temps.items() if t is not None}
        if len(valid) < len(market.station_ids):
            return  # missing data for at least one station -> skip, don't guess

        sides = [market.resolves_true_if_temp(t) for t in valid.values()]
        if len(set(sides)) != 1:
            return  # stations disagree -> skip

        consensus_side = sides[0]  # True -> favors "direction" outcome
        book = self.poly.get_book(market.token_id)
        if book is None:
            return

        if consensus_side:
            self._execute(market, book, reason=f"all {len(valid)} stations agree: "
                                                 f"{market.direction} {market.strike_f}F")
        # If consensus is against the token's resolution side, this simple version just
        # skips rather than shorting — extend with a "sell"/opposite-token flow if you
        # want the short side too.

    def _execute(self, market: MarketConfig, book: dict, reason: str) -> None:
        fill = self.engine.simulate_buy(book, PER_TRADE_NOTIONAL_USD)
        if fill is None:
            return
        total_cost = fill.avg_price * fill.filled_shares + fill.fee_usd
        self.account.debit(total_cost)
        self._traded_today.add(market.name)
        self.logger.log(
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            bot=self.name, market=market.name, side="buy",
            filled_shares=round(fill.filled_shares, 4),
            notional_usd=round(total_cost, 2),
            avg_fill_price=round(fill.avg_price, 4),
            fee_usd=round(fill.fee_usd, 4),
            slippage_usd=round(fill.slippage_usd, 4),
            reason=reason, strike_f=market.strike_f, model_edge="",
            bankroll_after=round(self.account.bankroll, 2),
        )


# --------------------------------------------------------------------------
# Bot 2: Late-window observed-vs-needed gap
# --------------------------------------------------------------------------

class LateWindowGapBot:
    name = "late_window_gap"

    def __init__(self, metar: METARClient, poly: PolymarketClient, engine: PaperExecutionEngine,
                 logger: TradeLogger, account: PaperAccount):
        self.metar = metar
        self.poly = poly
        self.engine = engine
        self.logger = logger
        self.account = account
        self._traded_today = set()

    def evaluate(self, market: MarketConfig) -> None:
        if market.name in self._traded_today:
            return

        now = datetime.now(timezone.utc)
        minutes_left = (market.window_close_utc - now).total_seconds() / 60.0
        if minutes_left <= 0 or minutes_left > LATE_WINDOW_TRIGGER_MINUTES:
            return  # only act late in the window

        primary_station = market.station_ids[0]
        temps = self.metar.fetch_temps_f([primary_station])
        temp = temps.get(primary_station)
        if temp is None:
            return

        gap_f = abs(temp - market.strike_f)
        max_possible_move = LATE_WINDOW_MAX_DRIFT_F_PER_HR * (minutes_left / 60.0)
        if gap_f <= max_possible_move:
            return  # still plausible for the temp to cross the strike -> no edge

        current_side_holds = market.resolves_true_if_temp(temp)
        if not current_side_holds:
            return  # current reading favors the side we're NOT set up to buy

        book = self.poly.get_book(market.token_id)
        if book is None:
            return

        fill = self.engine.simulate_buy(book, PER_TRADE_NOTIONAL_USD)
        if fill is None:
            return
        total_cost = fill.avg_price * fill.filled_shares + fill.fee_usd
        self.account.debit(total_cost)
        self._traded_today.add(market.name)
        self.logger.log(
            timestamp_utc=now.isoformat(), bot=self.name, market=market.name, side="buy",
            filled_shares=round(fill.filled_shares, 4), notional_usd=round(total_cost, 2),
            avg_fill_price=round(fill.avg_price, 4), fee_usd=round(fill.fee_usd, 4),
            slippage_usd=round(fill.slippage_usd, 4),
            reason=f"gap={gap_f:.1f}F > max_possible_move={max_possible_move:.1f}F "
                   f"with {minutes_left:.0f}m left",
            strike_f=market.strike_f, model_edge="", bankroll_after=round(self.account.bankroll, 2),
        )


# --------------------------------------------------------------------------
# Bot 3: NWS forecast vs Polymarket price mispricing
# --------------------------------------------------------------------------

class ForecastMispricingBot:
    name = "forecast_mispricing"

    def __init__(self, nws: NWSClient, poly: PolymarketClient, engine: PaperExecutionEngine,
                 logger: TradeLogger, account: PaperAccount):
        self.nws = nws
        self.poly = poly
        self.engine = engine
        self.logger = logger
        self.account = account
        self._traded_today = set()

    def evaluate(self, market: MarketConfig) -> None:
        if market.name in self._traded_today:
            return

        periods = self.nws.hourly_forecast_temps_f(market.lat, market.lon)
        target = self._closest_period_to_window_close(periods, market.window_close_utc)
        if target is None:
            return
        _, forecast_temp = target

        # model probability that the market's "direction" condition is met, assuming
        # forecast error is ~Normal(0, FORECAST_SIGMA_F) around the point forecast
        z = (forecast_temp - market.strike_f) / FORECAST_SIGMA_F
        prob_above = normal_cdf(z)
        model_prob = prob_above if market.direction == "above" else (1 - prob_above)

        market_price = self.poly.get_price(market.token_id, side="buy")
        if market_price is None:
            return

        edge = model_prob - market_price
        if edge < FORECAST_EDGE_THRESHOLD:
            return  # not enough edge to buy (this simple version only takes the long side)

        book = self.poly.get_book(market.token_id)
        if book is None:
            return
        fill = self.engine.simulate_buy(book, PER_TRADE_NOTIONAL_USD)
        if fill is None:
            return
        total_cost = fill.avg_price * fill.filled_shares + fill.fee_usd
        self.account.debit(total_cost)
        self._traded_today.add(market.name)
        self.logger.log(
            timestamp_utc=datetime.now(timezone.utc).isoformat(), bot=self.name, market=market.name,
            side="buy", filled_shares=round(fill.filled_shares, 4), notional_usd=round(total_cost, 2),
            avg_fill_price=round(fill.avg_price, 4), fee_usd=round(fill.fee_usd, 4),
            slippage_usd=round(fill.slippage_usd, 4),
            reason=f"model_prob={model_prob:.3f} vs market_price={market_price:.3f}, "
                   f"forecast={forecast_temp:.1f}F",
            strike_f=market.strike_f, model_edge=round(edge, 4),
            bankroll_after=round(self.account.bankroll, 2),
        )

    @staticmethod
    def _closest_period_to_window_close(periods: List[Tuple[datetime, float]],
                                          window_close_utc: datetime) -> Optional[Tuple[datetime, float]]:
        if not periods:
            return None
        return min(periods, key=lambda p: abs((p[0] - window_close_utc).total_seconds()))


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run() -> None:
    metar = METARClient()
    nws = NWSClient()
    poly = PolymarketClient()
    engine = PaperExecutionEngine()
    logger = TradeLogger()

    consensus_account = PaperAccount()
    gap_account = PaperAccount()
    forecast_account = PaperAccount()

    consensus_bot = MultiStationConsensusBot(metar, poly, engine, logger, consensus_account)
    gap_bot = LateWindowGapBot(metar, poly, engine, logger, gap_account)
    forecast_bot = ForecastMispricingBot(nws, poly, engine, logger, forecast_account)

    active = [m for m in MARKETS if _is_configured(m)]
    skipped = len(MARKETS) - len(active)
    if skipped:
        print(f"WARNING: {skipped} market(s) still have placeholder token_ids — "
              f"fill in MARKETS with real token IDs to trade. Idling.")
    print(f"Paper trading {len(active)}/{len(MARKETS)} market(s) -> {TRADE_LOG_PATH} "
          f"(poll every {POLL_INTERVAL_SECONDS}s). Ctrl+C to stop.")

    while True:
        now = datetime.now(timezone.utc)
        for market in active:
            if now > market.window_close_utc:
                continue  # window already resolved, nothing to do
            for bot in (consensus_bot, gap_bot, forecast_bot):
                try:
                    bot.evaluate(market)
                except Exception:
                    traceback.print_exc()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
