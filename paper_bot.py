"""Paper trader for Polymarket 5-min BTC Up/Down — implements THESIS.md.

Realism model (no real money, no keys):
  - Fills walk the LIVE CLOB order book level by level => real slippage, not synthetic.
  - Taker fee 0.07 * p * (1-p) per share (Polymarket crypto rate; makers pay 0).
  - Configurable execution delay between decision and fill; fill re-reads the book
    at fill time, so the price can move against you during the delay.
  - Settlement uses the Chainlink stream (the actual resolution basis): close vs
    the recorded strike. Strike is never estimated — if we missed the window open,
    the window is skipped.

Run:  python paper_bot.py --minutes 60
"""
import argparse
import asyncio
import csv
import json
import math
import os
import sys
import time
import urllib.request
from collections import deque

try:
    import websockets  # noqa: F401  (required for live streams; checked at startup)
    _WEBSOCKETS_OK = True
except ImportError:  # pragma: no cover - surfaced clearly at runtime
    _WEBSOCKETS_OK = False

# ---------------- config (mirrors THESIS.md) ----------------
BANKROLL0 = 1_000.0
FEE_RATE = 0.07          # taker fee, crypto markets: fee = shares * 0.07 * p * (1-p)
EXEC_DELAY_S = 0.75      # decision -> fill delay
EDGE_MIN = 0.04          # model p - fee-inclusive cost
SPREAD_MAX = 0.02
L_MIN = 5.0              # points from strike (no coin flips)
Z_MAX = 2.2              # no near-locks
PRICE_MAX = 0.85         # never buy late favorites
AGE_MIN_S, AGE_MAX_S = 30, 240     # entry window within the 300s market
T_EXIT_S = 270           # time stop at T-30s
TP_PRICE = 0.90          # sell into strength
EDGE_DECAY_EXIT = 0.01   # exit if live edge collapses
Z_FLIP_EXIT = 0.5        # invalidation: z flips sign by this much vs entry
KELLY_FRAC = 0.25
WINDOW_RISK_CAP = 0.02   # max cost per window as fraction of bankroll
DAILY_STOP = -0.06
BASIS_KILL = 15.0        # |binance - chainlink| points
STALE_S = 5.0            # chainlink tick max age
SIGMA_FLOOR = 3.0
Z_SHRINK = 0.85          # fat-tail haircut
DIR = os.path.dirname(os.path.abspath(__file__))

WSS_URL = "wss://ws-live-data.polymarket.com"
CLOB_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BOOK_FALLBACK_S = 5.0    # if no book event for this long, poll REST as fallback
SUB = {"action": "subscribe", "subscriptions": [
    {"topic": "crypto_prices_chainlink", "type": "*",
     "filters": json.dumps({"symbol": "btc/usd"})}]}
UA = {"User-Agent": "btc5m-paper/0.1", "Content-Type": "application/json"}

def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)

def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

async def get_json(url, timeout=8):
    def _get():
        req = urllib.request.Request(url, headers=UA)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    return await asyncio.get_event_loop().run_in_executor(None, _get)

# ---------------- chainlink stream ----------------
class ChainlinkStream:
    def __init__(self):
        self.ticks = deque(maxlen=4000)   # (ts_ms, price)
        self.last_msg = 0.0

    @property
    def latest(self):
        return self.ticks[-1] if self.ticks else None

    def price(self):
        t = self.latest
        return (t[1], t[0]) if t else (None, None)

    def tick_at_or_after(self, ts_ms):
        for ts, v in self.ticks:
            if ts >= ts_ms:
                return v
        return None

    def strike_at(self, win_ms):
        """Strike = first tick at/after open, but only if we were watching before
        the open. Otherwise we'd mislabel a join-time tick as the strike."""
        if not any(ts < win_ms for ts, _ in self.ticks):
            return None
        return self.tick_at_or_after(win_ms)

    def close_before(self, ts_ms):
        prior = [v for ts, v in self.ticks if ts < ts_ms]
        return prior[-1] if prior else None

    def realized_range(self, seconds):
        if not self.ticks:
            return None
        cut = (self.latest[0] - seconds * 1000)
        vals = [v for ts, v in self.ticks if ts >= cut]
        return max(vals) - min(vals) if len(vals) > 5 else None

    def value_ago(self, seconds):
        """Price N seconds ago (nearest tick); None if history doesn't reach back."""
        if not self.ticks:
            return None
        target = self.latest[0] - seconds * 1000
        prior = [v for ts, v in self.ticks if ts <= target]
        return prior[-1] if prior else None

    async def run(self):
        if not _WEBSOCKETS_OK:
            log("!! websockets package missing — run `.venv/bin/pip install websockets`")
            return
        while True:
            try:
                async with websockets.connect(WSS_URL) as ws:
                    await ws.send(json.dumps(SUB))
                    log("chainlink stream connected")
                    last_ping = time.monotonic()
                    while True:
                        if time.monotonic() - last_ping > 4:
                            await ws.send("PING"); last_ping = time.monotonic()
                        msg = await asyncio.wait_for(ws.recv(), timeout=25)
                        self.last_msg = time.time()
                        if not msg or msg in ("PING", "PONG"):
                            continue
                        try:
                            evt = json.loads(msg)
                        except ValueError:
                            continue
                        p = evt.get("payload") or {}
                        if isinstance(p, str):
                            try: p = json.loads(p)
                            except ValueError: continue
                        for t in (p.get("data") if isinstance(p, dict) else None) or []:
                            if "value" in t:
                                self.ticks.append((t["timestamp"], float(t["value"])))
            except Exception as e:
                log(f"chainlink stream error: {e!r} — reconnecting in 2s")
                await asyncio.sleep(2)

# ---------------- market / book ----------------
class MarketFeed:
    def __init__(self):
        self.win = None            # window start epoch s
        self.tokens = None         # (up_token, down_token)
        self.old_tokens = None
        self.books = {"Up": None, "Down": None}   # {"asks": [(px,size)...], "bids": [...], "ts": epoch}
        self.book_errors = 0
        self.ws = None
        self.last_book_msg = 0.0

    def fresh(self):
        return (time.time() - self.last_book_msg) < BOOK_FALLBACK_S

    def best(self, side, field):
        b = self.books[side]
        if not b or not b[field]:
            return None
        px, sz = b[field][0]
        return float(px), float(sz)

    async def run(self, stream):
        del stream  # window loading is slug-based; stream not needed here (kept for API compat)
        while True:
            now = int(time.time())
            win = now // 300 * 300
            if win != self.win:
                await self._load_window(win)
            # REST only as fallback when the WSS book feed is quiet/stale
            if not self.fresh():
                await self._poll_books()
            await asyncio.sleep(1)

    async def run_wss(self):
        """Live CLOB book stream: full snapshots ('book') + deltas ('price_change')."""
        if not _WEBSOCKETS_OK:
            log("!! websockets package missing — book stream disabled, REST fallback only")
            return
        while True:
            try:
                async with websockets.connect(CLOB_WSS) as ws:
                    self.ws = ws
                    log("clob book stream connected")
                    if self.tokens:
                        await ws.send(json.dumps({"assets_ids": list(self.tokens), "type": "market"}))
                        self.old_tokens = self.tokens
                    last_ping = time.monotonic()
                    while True:
                        if time.monotonic() - last_ping > 9:
                            await ws.send("PING"); last_ping = time.monotonic()
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        if not msg or msg == "PONG":
                            continue
                        self.last_book_msg = time.time()
                        try:
                            data = json.loads(msg)
                        except ValueError:
                            continue
                        for ev in (data if isinstance(data, list) else [data]):
                            self._apply_event(ev)
            except Exception as e:
                self.ws = None
                log(f"clob book stream error: {e!r} — reconnecting in 2s")
                await asyncio.sleep(2)

    def _apply_event(self, ev):
        if not isinstance(ev, dict) or not self.tokens:
            return
        aid = ev.get("asset_id") or ""
        side = "Up" if aid == self.tokens[0] else ("Down" if aid == self.tokens[1] else None)
        if not side:
            return
        et = ev.get("event_type")
        if et == "book":
            asks = sorted((float(x["price"]), float(x["size"])) for x in ev.get("asks", []) if float(x["size"]) > 0)
            bids = sorted(((float(x["price"]), float(x["size"])) for x in ev.get("bids", []) if float(x["size"]) > 0), key=lambda x: -x[0])
            self.books[side] = {"asks": asks, "bids": bids, "ts": time.time()}
        elif et == "price_change":
            b = self.books[side]
            if not b:
                return
            for ch in ev.get("changes", []):
                px, sz = float(ch["price"]), float(ch["size"])
                is_bid = ch.get("side") == "BUY"
                levels = b["bids"] if is_bid else b["asks"]
                levels = [(p, s) for p, s in levels if abs(p - px) > 1e-9]
                if sz > 0:
                    levels.append((px, sz))
                levels.sort(key=lambda x: -x[0] if is_bid else x[0])
                if is_bid:
                    b["bids"] = levels
                else:
                    b["asks"] = levels
            b["ts"] = time.time()

    async def _load_window(self, win):
        self.win, self.tokens = win, None
        slug = f"btc-updown-5m-{win}"
        for attempt in range(4):
            try:
                ms = await get_json(f"https://gamma-api.polymarket.com/markets?slug={slug}")
                if isinstance(ms, list) and ms:
                    m = ms[0]
                    toks = json.loads(m.get("clobTokenIds") or "[]")
                    outs = json.loads(m.get("outcomes") or "[]")
                    if len(toks) >= 2 and outs[:2] == ["Up", "Down"]:
                        self.tokens = (toks[0], toks[1])
                        if self.ws:
                            try:
                                if self.old_tokens:
                                    await self.ws.send(json.dumps(
                                        {"assets_ids": list(self.old_tokens), "operation": "unsubscribe"}))
                                await self.ws.send(json.dumps(
                                    {"assets_ids": list(self.tokens), "operation": "subscribe"}))
                            except Exception:
                                pass  # reconnect loop will re-subscribe with current tokens
                        self.old_tokens = self.tokens
                        log(f"window {win} market loaded (slug {slug})")
                        return
            except Exception as e:
                log(f"gamma fetch {slug} attempt {attempt+1} failed: {e!r}")
            await asyncio.sleep(2)
        log(f"window {win}: market {slug} not found — skipping window")

    async def _poll_books(self):
        if not self.tokens:
            return
        for side, tok in (("Up", self.tokens[0]), ("Down", self.tokens[1])):
            try:
                b = await get_json(f"https://clob.polymarket.com/book?token_id={tok}", timeout=5)
                asks = sorted([(float(x["price"]), float(x["size"])) for x in b.get("asks", [])], key=lambda x: x[0])
                bids = sorted([(float(x["price"]), float(x["size"])) for x in b.get("bids", [])], key=lambda x: -x[0])
                self.books[side] = {"asks": asks, "bids": bids, "ts": time.time()}
                self.book_errors = 0
            except Exception:
                self.book_errors += 1

# ---------------- paper broker ----------------
class PaperBroker:
    def __init__(self, trades_csv, windows_csv):
        self.cash = BANKROLL0
        self.day = time.strftime("%Y-%m-%d")
        self.day_start_equity = BANKROLL0
        self.realized_day = 0.0
        self.window_realized = 0.0   # realized pnl in current window (for windows.csv)
        self.trades_csv, self.windows_csv = trades_csv, windows_csv
        for path, head in ((trades_csv, ["ts", "window", "action", "side", "shares", "vwap", "fee", "cash", "reason"]),
                           (windows_csv, ["window", "strike", "close", "resolved", "traded", "pnl", "equity"])):
            fresh = not os.path.exists(path)
            if fresh:
                with open(path, "a", newline="") as f:
                    csv.writer(f).writerow(head)

    def equity(self, pos, mark_px):
        return self.cash + (pos["shares"] * mark_px if pos else 0.0)

    def halted(self, pos, mark_px):
        if self.day != time.strftime("%Y-%m-%d"):
            self.day = time.strftime("%Y-%m-%d")
            self.day_start_equity = self.equity(pos, mark_px)
            self.realized_day = 0.0
            log(f"new UTC day — daily stop reset at {self.day_start_equity:.2f}")
        return self.realized_day <= DAILY_STOP * self.day_start_equity

    def walk(self, levels, shares, is_buy):
        """Walk book levels; returns (filled_shares, vwap) or (0, 0) if book empty."""
        del is_buy  # both sides fill at quoted levels; direction encoded by caller
        filled, cost = 0.0, 0.0
        for px, sz in levels:
            if filled >= shares:
                break
            take = min(sz, shares - filled)
            filled += take
            cost += take * px
        return (filled, cost / filled) if filled > 0 else (0, 0)

    def execute(self, action, side, shares, levels, window, reason):
        filled, vwap = self.walk(levels, shares, action == "BUY")
        if filled <= 0:
            log(f"  !! {action} {side}: book empty — no fill")
            return None
        fee = filled * FEE_RATE * vwap * (1.0 - vwap)
        gross = filled * vwap
        if action == "BUY":
            self.cash -= gross + fee
        else:
            self.cash += gross - fee
        with open(self.trades_csv, "a", newline="") as f:
            csv.writer(f).writerow([int(time.time()), window, action, side,
                                    round(filled, 2), round(vwap, 4), round(fee, 4),
                                    round(self.cash, 2), reason])
        log(f"  FILL {action} {side}: {filled:.1f} @ {vwap:.3f}  fee ${fee:.2f}  cash {self.cash:.2f}  ({reason})")
        return filled, vwap, fee

    def settle(self, pos, window, strike, close, won):
        px = 1.0 if won else 0.0
        pnl = pos["shares"] * px - pos["cost_total"]
        self.cash += pos["shares"] * px
        self.realized_day += pnl
        self.window_realized += pnl
        eq = self.cash
        with open(self.windows_csv, "a", newline="") as f:
            csv.writer(f).writerow([window, strike, close, "Up" if close > strike else "Down",
                                    1, round(pnl, 2), round(eq, 2)])
        log(f"  SETTLE {'WON' if won else 'LOST'}: {pos['shares']:.1f} sh  pnl {pnl:+.2f}  cash {self.cash:.2f}")
        return pnl

    def log_skipped(self, window, strike, close):
        with open(self.windows_csv, "a", newline="") as f:
            csv.writer(f).writerow([window, strike or "", close or "",
                                    "" if strike and close else "",
                                    1 if self.window_realized else 0,
                                    round(self.window_realized, 2), round(self.cash, 2)])
        self.window_realized = 0.0

# ---------------- strategy ----------------
class Strategy:
    def __init__(self, stream, market, broker, binance):
        self.stream, self.market, self.broker, self.binance = stream, market, broker, binance
        self.strike = None
        self.strike_win = None
        self.pos = None
        self.entry_z = None
        self.pending = False
        self.traded_this_window = False
        self.atr15 = None

    def sigma_1m(self):
        parts = []
        if self.atr15:
            parts.append(self.atr15)
        rr = self.stream.realized_range(180)
        if rr is not None:
            parts.append(rr)
        if not parts:
            return None
        return max(SIGMA_FLOOR, sum(parts) / len(parts))

    def p_up(self, c, sigma_rem):
        z = Z_SHRINK * (c - self.strike) / sigma_rem
        return norm_cdf(z), z

    def fee_pts(self, p):
        return FEE_RATE * p * (1 - p)

    async def run(self):
        asyncio.create_task(self._poll_klines())
        heartbeat = 0.0
        while True:
            await asyncio.sleep(1.0)
            now = int(time.time())
            win = now // 300 * 300
            age, remain = now - win, win + 300 - now

            c, c_ts = self.stream.price()
            if c is None:
                continue
            now = max(now, c_ts // 1000)          # feed time is authoritative
            age, remain = now - win, win + 300 - now

            # window rollover: settle + new strike
            if self.strike_win != win:
                if self.pos and self.strike_win:
                    close = self.stream.close_before((self.strike_win + 300) * 1000)
                    if close is None:
                        log("  !! no close tick — settling at last known price")
                        close = c
                    won = (close > self.strike) if self.pos["side"] == "Up" else (close < self.strike)
                    self.broker.settle(self.pos, self.strike_win, self.strike, close, won)
                    self.pos = None
                elif self.strike_win:
                    close = self.stream.close_before((self.strike_win + 300) * 1000)
                    self.broker.log_skipped(self.strike_win, self.strike, close)
                self.strike = self.stream.strike_at(win * 1000)
                self.strike_win = win
                self.traded_this_window = False
                self.entry_z = None
                log(f"=== window {win} | " + (f"strike {self.strike:.2f}" if self.strike
                    else "strike pending...") + f" | age {age}s")
            if self.strike is None and self.strike_win == win and age < AGE_MIN_S:
                self.strike = self.stream.strike_at(win * 1000)
                if self.strike:
                    log(f"  strike recorded late: {self.strike:.2f}")

            if time.time() - heartbeat > 15:
                heartbeat = time.time()
                sig = self.sigma_1m()
                if self.strike and sig:
                    sig_rem = sig * math.sqrt(max(remain, 20) / 60.0)
                    p, z = self.p_up(c, sig_rem)
                    bu, bd = self.market.best("Up", "asks"), self.market.best("Down", "asks")
                    log(f"    c={c:.2f} L={c-self.strike:+.2f} sig_rem={sig_rem:.1f} p_up={p:.3f} "
                        f"| Up {bu[0] if bu else '-'} Down {bd[0] if bd else '-'} "
                        f"| pos={self.pos['side'] + '@' + format(self.pos['vwap'], '.2f') if self.pos else 'flat'} "
                        f"cash={self.broker.cash:.2f}")

            await self._manage(now, win, age, remain, c)

    async def _poll_klines(self):
        while True:
            try:
                kl = await get_json("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=15")
                self.atr15 = sum(float(k[2]) - float(k[3]) for k in kl) / len(kl)
            except Exception:
                pass
            await asyncio.sleep(60)

    def kill_reason(self, c, now):
        t = self.stream.latest
        if not t or (now * 1000 - t[0]) / 1000 > STALE_S:
            return "chainlink stale"
        b = self.binance.price()
        if b and abs(b - c) > BASIS_KILL:
            return f"basis {b - c:+.1f}"
        if time.time() - self.market.last_book_msg > 8:
            return "book stale"
        if self.market.book_errors >= 5:
            return "book errors"
        if self.broker.halted(self.pos, 0.5):
            return "daily stop"
        return None

    async def _manage(self, now, win, age, remain, c):
        if not self.strike or not self.market.tokens:
            return
        sig = self.sigma_1m()
        if not sig:
            return
        sig_rem = sig * math.sqrt(max(remain, 20) / 60.0)
        p, z = self.p_up(c, sig_rem)
        mark = self.market.best(self.pos["side"] if self.pos else "Up", "bids")
        mark_px = mark[0] if mark else (p if not self.pos else 0.5)

        kill = self.kill_reason(c, now)
        if kill and not self.pos:
            if int(now) % 30 == 0:
                log(f"    kills active: {kill}")
            return

        # ---- exits ----
        if self.pos:
            side = self.pos["side"]
            bid = self.market.best(side, "bids")
            if not bid:
                return
            if self.entry_z is not None and z * self.entry_z < 0 and abs(z - self.entry_z) > Z_FLIP_EXIT:
                await self._exit("invalidation z-flip", p)
            elif bid[0] >= TP_PRICE:
                await self._exit(f"take profit {bid[0]:.2f}", p)
            elif age >= T_EXIT_S and bid[0] < 0.92:
                await self._exit("time stop T-30s", p)
            elif age >= T_EXIT_S:
                pass  # near-lock: hold to resolution
            elif self._live_edge(p, side) < EDGE_DECAY_EXIT:
                await self._exit("edge decayed", p)
            return

        # ---- entry ----
        if self.pending or self.traded_this_window or kill:
            return
        if not (AGE_MIN_S <= age <= AGE_MAX_S):
            return
        L = abs(c - self.strike)
        if L < L_MIN or L > Z_MAX * sig_rem:
            return
        cands = []
        # momentum guard: never buy against a fresh, expanding impulse
        # (lesson from first live paper trade: Down@0.27 into an uptrend)
        # FIX: actually exclude the blocked side (old code re-included it).
        blocked = None
        c_60 = self.stream.value_ago(60)
        if c_60 is not None:
            moved = (c - self.strike) - (c_60 - self.strike)
            if L * moved > 0 and abs(moved) >= L_MIN:
                blocked = "Down" if (c - self.strike) > 0 else "Up"
                log(f"    momentum guard: blocking {blocked} (L {c-self.strike:+.1f}, "
                    f"moved {moved:+.1f}/60s)")
        cands_side_ok = ("Up",) if blocked == "Down" else (("Down",) if blocked == "Up" else ("Up", "Down"))
        for side, p_side in (("Up", p), ("Down", 1 - p)):
            if side not in cands_side_ok:
                continue
            ask = self.market.best(side, "asks")
            if not ask:
                continue
            px, sz = ask
            bid = self.market.best(side, "bids")
            if bid:
                spread = px - bid[0]
                if spread > SPREAD_MAX:
                    continue
            if px > PRICE_MAX:
                continue
            cost = px + self.fee_pts(px)
            cands.append((p_side - cost, side, px, sz, cost, p_side))
        if not cands:
            return
        edge, side, px, sz, cost, p_side = max(cands)
        if edge < EDGE_MIN:
            return
        # 1/4 Kelly on fee-inclusive cost, capped at WINDOW_RISK_CAP of bankroll
        f = max(0.0, (p_side - cost) / (1.0 - cost))
        stake = self.broker.cash * KELLY_FRAC * f
        stake = min(stake, self.broker.cash * WINDOW_RISK_CAP)
        shares = stake / cost
        log(f"  SIGNAL {side}: p={p_side:.3f} ask={px:.2f} fee={self.fee_pts(px):.3f} "
            f"edge={edge*100:+.1f}pts L={c-self.strike:+.1f} sig_rem={sig_rem:.1f} — "
            f"buying {shares:.0f} in {EXEC_DELAY_S}s")
        self.pending = True
        await asyncio.sleep(EXEC_DELAY_S)   # execution delay; book re-read inside execute
        levels = self.market.books[side]["asks"] if self.market.books[side] else []
        res = self.broker.execute("BUY", side, shares, levels, win,
                                  f"edge {edge*100:.1f}pts p {p_side:.2f}")
        if res:
            filled, vwap, fee = res
            self.pos = {"side": side, "shares": filled, "vwap": vwap, "cost_total": filled * vwap + fee}
            self.entry_z = (Z_SHRINK * (c - self.strike) / sig_rem)
            self.traded_this_window = True
        self.pending = False

    def _live_edge(self, p, side):
        bid = self.market.best(side, "bids")
        if not bid:
            return -1.0
        p_side = p if side == "Up" else 1 - p
        return p_side - (bid[0] - self.fee_pts(bid[0]))

    async def _exit(self, reason, p):
        del p  # exit at live bid; model p only used by caller for logging context
        side = self.pos["side"]
        levels = self.market.books[side]["bids"] if self.market.books[side] else []
        await asyncio.sleep(EXEC_DELAY_S)
        res = self.broker.execute("SELL", side, self.pos["shares"], levels,
                                  self.strike_win, reason)
        if res:
            filled, vwap, fee = res
            realized = filled * vwap - fee - self.pos["cost_total"]
            self.broker.window_realized += realized
            self.broker.realized_day += realized
            self.pos = None
            self.entry_z = None

# ---------------- binance spot (basis guard) ----------------
class BinanceSpot:
    def __init__(self):
        self.px = None
    def price(self):
        return self.px
    async def run(self):
        while True:
            try:
                d = await get_json("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", timeout=5)
                self.px = float(d["price"])
            except Exception:
                pass
            await asyncio.sleep(5)

async def main(minutes):
    if not _WEBSOCKETS_OK:
        log("FATAL: `websockets` not installed — run `.venv/bin/pip install websockets` first")
        sys.exit(2)
    trades_csv = os.path.join(DIR, "trades.csv")
    windows_csv = os.path.join(DIR, "windows.csv")
    broker = PaperBroker(trades_csv, windows_csv)
    stream, market, binance = ChainlinkStream(), MarketFeed(), BinanceSpot()
    strat = Strategy(stream, market, broker, binance)
    log(f"paper bot start — bankroll {BANKROLL0:.0f} fee {FEE_RATE} delay {EXEC_DELAY_S}s — writing {trades_csv}")
    tasks = [stream.run(), market.run(stream), market.run_wss(), binance.run(), strat.run()]
    try:
        if minutes:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=minutes * 60)
        else:
            await asyncio.gather(*tasks)
    except (asyncio.TimeoutError, KeyboardInterrupt):
        pass
    # graceful shutdown: flatten any open position into the current book
    if strat.pos:
        side = strat.pos["side"]
        levels = market.books[side]["bids"] if market.books[side] else []
        broker.execute("SELL", side, strat.pos["shares"], levels,
                       strat.strike_win, "shutdown flatten")
    log(f"shutdown — final cash {broker.cash:.2f}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=None, help="stop after N minutes (default: forever)")
    args = ap.parse_args()
    try:
        asyncio.run(main(args.minutes))
    except (asyncio.TimeoutError, KeyboardInterrupt):
        pass
