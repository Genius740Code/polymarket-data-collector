"""Paper trader: BTC 5-min Up/Down momentum-continuation (impulse + skew, T-120s).

Mechanics (from the general idea):
  1. Market: Polymarket BTC 5-min Up/Down, slug btc-updown-5m-{window}.
  2. Timing: decide once per window in ENTRY_AGE_MIN..ENTRY_AGE_MAX
     (default 180..240s, i.e. ~T-120s to T-60s). One evaluation per window.
  3. Impulse: |chainlink_now - strike| >= IMPULSE_MIN (default $80,
     env IMPULSE_MIN, idea range $70-100). Strike = first Chainlink tick
     at/after window open; windows joined late are SKIPPED, never estimated.
  4. Skew: crowd agrees with the move. up_mid/down_mid from live CLOB
     mids; require impulse side mid > 0.5. Logged with full context.
  5. Entry: BUY impulse side at live asks (walk the book => real slippage),
     taker fee 0.07*p*(1-p), EXEC_DELAY_S re-read before fill.
     Sizing: risk_budget = equity * PER_TRADE_RISK (default 5%, within the
     1-15% wrapper) capped so notional <= equity * MAX_POS_FRAC (default 50%
     of allocated equity per the idea). One position per window, hold to settle.
  6. Micro-hedge: if dominant mid >= SKEW_EXTREME (default 0.95), buy
     HEDGE_DOLLARS (default $1.50) of the OPPOSITE side at asks. Tail insurance.
  7. Risk wrapper: DAILY_STOP (default -12%), MAX_TRADES_DAY (20),
     SPREAD_MAX (0.02), MIN_ASK_SIZE (5 shares), stale guards
     (chainlink 90s oracle cadence, book 8s), CONSEC_FAIL_KILL (5 fails => halt day).

Real-data-only (AGENT.md): no synthetic ticks, no estimated strikes, no
interpolated fills. Gaps are logged as skipped windows in windows csv.

Run:  python paper_momentum_impulse.py --minutes 60
PM2:  pm2 start ecosystem.bots.config.js  (entry btc5m-momentum-impulse)
"""
import argparse
import asyncio
import csv
import json
import os
import sys
import time
import urllib.request
from collections import deque

try:
    import websockets  # noqa: F401
    _WEBSOCKETS_OK = True
except ImportError:  # pragma: no cover
    _WEBSOCKETS_OK = False

# ---------------- config ----------------
BANKROLL0 = float(os.getenv("MOM_BANKROLL", "1000"))
FEE_RATE = 0.07
EXEC_DELAY_S = float(os.getenv("MOM_EXEC_DELAY_S", "0.75"))
ENTRY_AGE_MIN = int(os.getenv("MOM_ENTRY_MIN_AGE", "180"))   # T-120s
ENTRY_AGE_MAX = int(os.getenv("MOM_ENTRY_MAX_AGE", "240"))   # T-60s
IMPULSE_MIN = float(os.getenv("MOM_IMPULSE_MIN", "80"))      # $70-100 idea range
PER_TRADE_RISK = float(os.getenv("MOM_PER_TRADE_RISK", "0.05"))  # 1-15% wrapper
MAX_POS_FRAC = float(os.getenv("MOM_MAX_POS_FRAC", "0.50"))      # 50% allocated/trade
DAILY_STOP = float(os.getenv("MOM_DAILY_STOP", "-0.12"))         # -10..-15% wrapper
MAX_TRADES_DAY = int(os.getenv("MOM_MAX_TRADES_DAY", "20"))
SPREAD_MAX = 0.02
MIN_ASK_SIZE = 5.0
SKEW_EXTREME = 0.95
HEDGE_DOLLARS = float(os.getenv("MOM_HEDGE_DOLLARS", "1.50"))
PRICE_MAX = 0.95          # never chase a fully-priced favorite for the MAIN leg
STALE_CHAINLINK_S = float(os.getenv("MOM_STALE_CHAINLINK_S", "90"))
STALE_BOOK_S = 8.0
CONSEC_FAIL_KILL = 5
DIR = os.path.dirname(os.path.abspath(__file__))

WSS_URL = "wss://ws-live-data.polymarket.com"
CLOB_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BOOK_FALLBACK_S = 5.0
SUB = {"action": "subscribe", "subscriptions": [
    {"topic": "crypto_prices_chainlink", "type": "*",
     "filters": json.dumps({"symbol": "btc/usd"})}]}
UA = {"User-Agent": "btc5m-momentum-paper/0.1", "Content-Type": "application/json"}


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} [momentum] {msg}", flush=True)


# ---------------- pure decision helpers (unit-tested) ----------------
def skew_agrees(side, up_mid, down_mid):
    """Crowd-agreement check: impulse side's mid must be > 0.5.

    side: "Up" or "Down". Returns False if either mid is missing."""
    if up_mid is None or down_mid is None:
        return False
    return (up_mid > 0.5) if side == "Up" else (down_mid > 0.5)


def size_shares(cash, ask_px):
    """Shares to buy: risk_budget = cash*PER_TRADE_RISK at fee-inclusive cost,
    capped so notional <= cash*MAX_POS_FRAC. Returns 0.0 if unpriceable."""
    fee_pts = FEE_RATE * ask_px * (1.0 - ask_px)
    cost_ps = ask_px + fee_pts
    if cost_ps <= 0 or cash <= 0:
        return 0.0
    return min(cash * PER_TRADE_RISK / cost_ps, cash * MAX_POS_FRAC / cost_ps)


async def get_json(url, timeout=8):
    def _get():
        req = urllib.request.Request(url, headers=UA)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    return await asyncio.get_event_loop().run_in_executor(None, _get)


# ---------------- chainlink stream (real WS only) ----------------
class ChainlinkStream:
    def __init__(self):
        self.ticks = deque(maxlen=4000)  # (ts_ms, price)
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
        if not any(ts < win_ms for ts, _ in self.ticks):
            return None  # joined late: skip, never estimate
        return self.tick_at_or_after(win_ms)

    def close_before(self, ts_ms):
        prior = [v for ts, v in self.ticks if ts < ts_ms]
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
                            await ws.send("PING")
                            last_ping = time.monotonic()
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
                            try:
                                p = json.loads(p)
                            except ValueError:
                                continue
                        for t in (p.get("data") if isinstance(p, dict) else None) or []:
                            if "value" in t:
                                self.ticks.append((t["timestamp"], float(t["value"])))
            except Exception as e:
                log(f"chainlink stream error: {e!r} — reconnecting in 2s")
                await asyncio.sleep(2)


# ---------------- market / book (live CLOB) ----------------
class MarketFeed:
    def __init__(self):
        self.win = None
        self.tokens = None
        self.old_tokens = None
        self.books = {"Up": None, "Down": None}
        self.book_errors = 0
        self.empty_snaps = 0
        self.ws = None
        self.last_book_msg = 0.0
        self.last_rest_poll = 0.0
        self.REST_REFRESH_S = 10.0

    def fresh(self):
        return (time.time() - self.last_book_msg) < BOOK_FALLBACK_S

    def best(self, side, field):
        b = self.books[side]
        if not b or not b[field]:
            return None
        px, sz = b[field][0]
        return float(px), float(sz)

    def mid(self, side):
        b = self.books[side]
        if not b or not b["asks"] or not b["bids"]:
            return None
        return (b["asks"][0][0] + b["bids"][0][0]) / 2.0

    async def run(self, stream):
        del stream
        while True:
            now = int(time.time())
            win = now // 300 * 300
            if win != self.win:
                await self._load_window(win)
            # REST fallback when the WSS feed is quiet/stale OR when we have
            # no book at all (WSS heartbeat traffic must not mask missing books).
            # Plus a periodic full-snapshot refresh: WSS deltas alone can thin
            # the local view to empty (and bursty windows get us disconnected
            # as 'slow consumer'), so re-anchor to REST every REST_REFRESH_S.
            if (not self.fresh() or self.books["Up"] is None or self.books["Down"] is None
                    or (time.time() - self.last_rest_poll) > self.REST_REFRESH_S):
                await self._poll_books()
                self.last_rest_poll = time.time()
            await asyncio.sleep(1)

    async def run_wss(self):
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
                            await ws.send("PING")
                            last_ping = time.monotonic()
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        if not msg or msg == "PONG":
                            continue
                        try:
                            data = json.loads(msg)
                        except ValueError:
                            continue
                        for ev in (data if isinstance(data, list) else [data]):
                            # only token-matched events count toward freshness;
                            # unrelated traffic must not mask a stale/missing book
                            if self._apply_event(ev):
                                self.last_book_msg = time.time()
            except Exception as e:
                self.ws = None
                log(f"clob book stream error: {e!r} — reconnecting in 2s")
                await asyncio.sleep(2)

    def _apply_event(self, ev):
        if not isinstance(ev, dict) or not self.tokens:
            return False
        aid = ev.get("asset_id") or ""
        side = "Up" if aid == self.tokens[0] else ("Down" if aid == self.tokens[1] else None)
        if not side:
            return False
        et = ev.get("event_type")
        if et == "book":
            asks = sorted((float(x["price"]), float(x["size"])) for x in ev.get("asks", []) if float(x["size"]) > 0)
            bids = sorted(((float(x["price"]), float(x["size"])) for x in ev.get("bids", []) if float(x["size"]) > 0),
                          key=lambda x: -x[0])
            if not asks and not bids:
                # fully-empty snapshot would wipe good REST data; ignore and
                # count it (a genuinely empty book still shows via REST poll)
                self.empty_snaps += 1
                return False
            self.books[side] = {"asks": asks, "bids": bids, "ts": time.time()}
            return True
        elif et == "price_change":
            b = self.books[side]
            if not b:
                return False
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
            return True
        return False

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
                                pass
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


# ---------------- paper broker (separate csvs) ----------------
class PaperBroker:
    def __init__(self, trades_csv, windows_csv):
        self.cash = BANKROLL0
        self.day = time.strftime("%Y-%m-%d")
        self.day_start_equity = BANKROLL0
        self.realized_day = 0.0
        self.trades_today = 0
        self.consec_fails = 0
        self.window_realized = 0.0
        self.trades_csv, self.windows_csv = trades_csv, windows_csv
        for path, head in (
            (trades_csv, ["ts", "window", "action", "side", "shares", "vwap", "fee",
                          "cash", "reason", "impulse", "skew_up_mid", "skew_down_mid", "age_s"]),
            (windows_csv, ["window", "strike", "close", "resolved", "traded",
                           "pnl", "equity", "impulse", "skew_up_mid", "skew_down_mid", "reason"]),
        ):
            if not os.path.exists(path):
                with open(path, "a", newline="") as f:
                    csv.writer(f).writerow(head)

    def halted(self):
        if self.day != time.strftime("%Y-%m-%d"):
            self.day = time.strftime("%Y-%m-%d")
            self.day_start_equity = self.cash
            self.realized_day = 0.0
            self.trades_today = 0
            self.consec_fails = 0
            log(f"new UTC day — stops reset at {self.day_start_equity:.2f}")
        if self.realized_day <= DAILY_STOP * self.day_start_equity:
            return True
        if self.trades_today >= MAX_TRADES_DAY:
            return True
        if self.consec_fails >= CONSEC_FAIL_KILL:
            return True
        return False

    def walk(self, levels, shares):
        filled, cost = 0.0, 0.0
        for px, sz in levels:
            if filled >= shares:
                break
            take = min(sz, shares - filled)
            filled += take
            cost += take * px
        return (filled, cost / filled) if filled > 0 else (0, 0)

    def execute(self, action, side, shares, levels, window, reason,
                impulse="", up_mid="", down_mid="", age_s=""):
        filled, vwap = self.walk(levels, shares)
        if filled <= 0:
            self.consec_fails += 1
            log(f"  !! {action} {side}: book empty — no fill (fail {self.consec_fails})")
            return None
        self.consec_fails = 0
        fee = filled * FEE_RATE * vwap * (1.0 - vwap)
        gross = filled * vwap
        if action == "BUY":
            self.cash -= gross + fee
        else:
            self.cash += gross - fee
        if action == "BUY":
            self.trades_today += 1
        with open(self.trades_csv, "a", newline="") as f:
            csv.writer(f).writerow([int(time.time()), window, action, side,
                                    round(filled, 2), round(vwap, 4), round(fee, 4),
                                    round(self.cash, 2), reason,
                                    impulse, up_mid, down_mid, age_s])
        log(f"  FILL {action} {side}: {filled:.1f} @ {vwap:.3f} fee ${fee:.2f} "
            f"cash {self.cash:.2f} ({reason})")
        return filled, vwap, fee

    def settle_leg(self, shares, cost_total, won):
        pnl = shares * (1.0 if won else 0.0) - cost_total
        self.cash += shares * (1.0 if won else 0.0)
        self.realized_day += pnl
        self.window_realized += pnl
        return pnl

    def log_window(self, window, strike, close, resolved, traded, ctx, reason):
        with open(self.windows_csv, "a", newline="") as f:
            csv.writer(f).writerow([window, strike or "", close or "", resolved,
                                    1 if traded else 0, round(self.window_realized, 2),
                                    round(self.cash, 2),
                                    ctx.get("impulse", ""), ctx.get("up_mid", ""),
                                    ctx.get("down_mid", ""), reason])
        self.window_realized = 0.0


# ---------------- strategy: impulse + skew, T-120s ----------------
class ImpulseSkewStrategy:
    def __init__(self, stream, market, broker):
        self.stream, self.market, self.broker = stream, market, broker
        self.strike = None
        self.strike_win = None
        self.pos = []  # legs: dicts side/shares/vwap/cost_total/hedge:bool
        self.decided_win = None
        self.skip_reason = None
        self.ctx = {}

    def kill_reason(self, c, now):
        t = self.stream.latest
        if not t or (now * 1000 - t[0]) / 1000 > STALE_CHAINLINK_S:
            return "chainlink stale"
        if time.time() - self.market.last_book_msg > STALE_BOOK_S:
            return "book stale"
        if self.market.book_errors >= 5:
            return "book errors"
        if self.broker.halted():
            return "daily stop / max-trades / fail-kill"
        return None

    async def run(self):
        heartbeat = 0.0
        while True:
            await asyncio.sleep(1.0)
            now = int(time.time())
            win = now // 300 * 300
            c, c_ts = self.stream.price()
            if c is None:
                continue
            now = max(now, c_ts // 1000)
            age, remain = now - win, win + 300 - now

            if self.strike_win != win:
                await self._settle_prev(c)
                self.strike = self.stream.strike_at(win * 1000)
                self.strike_win = win
                self.decided_win = None
                self.skip_reason = None
                self.pos = []
                self.ctx = {}
                log(f"=== window {win} | " + (f"strike {self.strike:.2f}" if self.strike
                     else "strike pending (must see pre-open tick)...") + f" | age {age}s")
            if self.strike is None and age < ENTRY_AGE_MIN:
                self.strike = self.stream.strike_at(win * 1000)
                if self.strike:
                    log(f"  strike recorded late: {self.strike:.2f}")

            if time.time() - heartbeat > 15:
                heartbeat = time.time()
                up_mid = self.market.mid("Up")
                down_mid = self.market.mid("Down")
                log(f"    c={c:.2f} L={c - self.strike:+.1f} " if self.strike else f"    c={c:.2f} "
                    f"| up_mid={up_mid if up_mid is not None else '-'} "
                    f"down_mid={down_mid if down_mid is not None else '-'} "
                    f"| legs={len(self.pos)} cash={self.broker.cash:.2f} "
                    f"(book_err={self.market.book_errors} empty_snaps={self.market.empty_snaps})")

            await self._maybe_enter(now, win, age, remain, c)

    async def _settle_prev(self, c):
        if not self.strike_win:
            return
        close = self.stream.close_before((self.strike_win + 300) * 1000)
        if close is None:
            close = c
        if self.pos and self.strike is not None:
            won_side = "Up" if close > self.strike else "Down"
            for leg in self.pos:
                won = leg["side"] == won_side
                pnl = self.broker.settle_leg(leg["shares"], leg["cost_total"], won)
                log(f"  SETTLE {'WON' if won else 'LOST'} {leg['side']}"
                    f"{' (hedge)' if leg.get('hedge') else ''}: "
                    f"{leg['shares']:.1f} sh pnl {pnl:+.2f} cash {self.broker.cash:.2f}")
            self.broker.log_window(self.strike_win, self.strike, close, won_side, True,
                                   self.ctx, "held to settlement")
        else:
            # single row per window: carry the decision-time skip reason if any
            if self.skip_reason:
                reason = self.skip_reason
            else:
                reason = "no strike (joined late)" if self.strike is None else "no signal"
            if self.strike_win:
                self.broker.log_window(self.strike_win, self.strike, close, "", False,
                                       self.ctx, reason)
            self.skip_reason = None

    async def _maybe_enter(self, now, win, age, remain, c):
        if self.decided_win == win or not self.strike or not self.market.tokens:
            return
        if not (ENTRY_AGE_MIN <= age <= ENTRY_AGE_MAX):
            return
        impulse = c - self.strike
        if abs(impulse) < IMPULSE_MIN:
            return  # no real move: sit out
        side = "Up" if impulse > 0 else "Down"
        up_mid = self.market.mid("Up")
        down_mid = self.market.mid("Down")
        if up_mid is None or down_mid is None:
            return
        # skew agrees with the move?
        agrees = skew_agrees(side, up_mid, down_mid)
        self.ctx = {"impulse": round(impulse, 2), "up_mid": round(up_mid, 3),
                    "down_mid": round(down_mid, 3)}
        if not agrees:
            self.decided_win = win  # evaluated, crowd fades the move: skip by design
            self.skip_reason = "skipped: skew disagrees"
            log(f"  SKIP {side}: impulse {impulse:+.1f} but skew disagrees "
                f"(up {up_mid:.2f}/down {down_mid:.2f})")
            return

        kill = self.kill_reason(c, now)
        if kill:
            if int(now) % 30 == 0:
                log(f"    kills active: {kill}")
            return

        ask = self.market.best(side, "asks")
        bid = self.market.best(side, "bids")
        if not ask:
            return
        px, sz = ask
        if bid and (px - bid[0]) > SPREAD_MAX:
            self.decided_win = win
            self.skip_reason = "skipped: spread guard"
            return
        if sz < MIN_ASK_SIZE:
            self.decided_win = win
            self.skip_reason = "skipped: thin book"
            return
        if px > PRICE_MAX:
            self.decided_win = win
            self.skip_reason = "skipped: price>0.95 favorite"
            return

        shares = size_shares(self.broker.cash, px)
        if shares <= 0:
            return
        log(f"  SIGNAL {side}: impulse {impulse:+.1f} (min {IMPULSE_MIN:.0f}) "
            f"skew up {up_mid:.2f}/down {down_mid:.2f} agrees | ask {px:.2f} "
            f"age {age}s — buying {shares:.0f} in {EXEC_DELAY_S}s")
        self.decided_win = win  # single decision per window
        await asyncio.sleep(EXEC_DELAY_S)
        levels = self.market.books[side]["asks"] if self.market.books[side] else []
        res = self.broker.execute("BUY", side, shares, levels, win,
                                  f"impulse {impulse:+.1f} skew agrees",
                                  impulse=round(impulse, 2), up_mid=round(up_mid, 3),
                                  down_mid=round(down_mid, 3), age_s=age)
        if res:
            filled, vwap, fee = res
            self.pos.append({"side": side, "shares": filled,
                             "vwap": vwap, "cost_total": filled * vwap + fee})
            # micro-hedge on extreme consensus
            dom = up_mid if side == "Up" else down_mid
            if dom >= SKEW_EXTREME:
                await self._micro_hedge(win, side, age)

    async def _micro_hedge(self, win, main_side, age):
        opp = "Down" if main_side == "Up" else "Up"
        ask = self.market.best(opp, "asks")
        if not ask:
            return
        px, _ = ask
        shares = HEDGE_DOLLARS / (px + FEE_RATE * px * (1.0 - px))
        if shares <= 0:
            return
        await asyncio.sleep(EXEC_DELAY_S)
        levels = self.market.books[opp]["asks"] if self.market.books[opp] else []
        res = self.broker.execute("BUY", opp, shares, levels, win,
                                  f"micro-hedge ${HEDGE_DOLLARS:.2f} vs extreme skew",
                                  impulse=self.ctx.get("impulse", ""),
                                  up_mid=self.ctx.get("up_mid", ""),
                                  down_mid=self.ctx.get("down_mid", ""), age_s=age)
        if res:
            filled, vwap, fee = res
            self.pos.append({"side": opp, "shares": filled, "vwap": vwap,
                             "cost_total": filled * vwap + fee, "hedge": True})


# ---------------- main ----------------
async def main(minutes):
    if not _WEBSOCKETS_OK:
        log("FATAL: `websockets` not installed — run `.venv/bin/pip install websockets` first")
        sys.exit(2)
    broker = PaperBroker(os.path.join(DIR, "trades_momentum.csv"),
                         os.path.join(DIR, "windows_momentum.csv"))
    stream, market = ChainlinkStream(), MarketFeed()
    strat = ImpulseSkewStrategy(stream, market, broker)
    log(f"start — bankroll {BANKROLL0:.0f} impulse>={IMPULSE_MIN:.0f} "
        f"entry {ENTRY_AGE_MIN}-{ENTRY_AGE_MAX}s risk {PER_TRADE_RISK*100:.1f}% "
        f"pos-cap {MAX_POS_FRAC*100:.0f}% hedge ${HEDGE_DOLLARS:.2f}@{SKEW_EXTREME}")
    tasks = [stream.run(), market.run(stream), market.run_wss(), strat.run()]
    try:
        if minutes:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=minutes * 60)
        else:
            await asyncio.gather(*tasks)
    except (asyncio.TimeoutError, KeyboardInterrupt):
        pass
    if strat.pos:  # flatten open legs into the live book on shutdown
        for leg in strat.pos:
            side = leg["side"]
            levels = market.books[side]["bids"] if market.books[side] else []
            broker.execute("SELL", side, leg["shares"], levels,
                           strat.strike_win, "shutdown flatten")
    log(f"shutdown — final cash {broker.cash:.2f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=None)
    args = ap.parse_args()
    try:
        asyncio.run(main(args.minutes))
    except (asyncio.TimeoutError, KeyboardInterrupt):
        pass
