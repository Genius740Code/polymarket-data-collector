"""Unified BTC 5m paper-trading runner — ONE pm2 process for 3 bots.

Combines (no real orders, no keys):
  1. paper_bot.py               (base mean-reversion, THESIS.md)
  2. paper_momentum_impulse.py  (impulse+skew, T-120s)
  3. paper_harvester.py         (late-round harvester, simulation mode)

Why one process: each bot previously held its own Chainlink WS + CLOB WS +
REST pollers + full parquet scan. That was 3x connections, 3x polls, and the
harvester re-loaded the whole hive on every pm2 restart (crash-loop:
FileNotFoundError on collector *.parquet.tmp atomic-write files, 900+
restarts, ~700MB + 60-100% CPU). Here:

  * ONE shared ChainlinkStream (btc/usd) + ONE shared MarketFeed (BTC 5m CLOB
    book) + ONE shared BinanceSpot feed both live strategies via duck-typing.
    The shared classes implement the UNION of methods both strategies need
    (paper_bot needs realized_range/value_ago; momentum needs mid/book_age/
    crossed/empty_snaps).
  * Harvester runs as a scheduled background task (default every 60 min,
    staggered 60s after startup) inside the same loop, wrapped in
    try/except so a data-scan failure NEVER kills the live bots. It runs in
    a thread executor so pandas/pyarrow don't block the event loop, and it
    excludes collector *.tmp atomic-write files (the crash-loop cause).

Run:
  python paper_btc_unified.py [--minutes N] [--no-harvester]
      [--harvester-interval-min 60] [--harvester-minutes 120]
      [--harvester-variant twap]

PM2:
  pm2 start ecosystem.bots.config.cjs --only btc5m-unified
CSV outputs are unchanged (trades.csv, windows.csv, trades_momentum.csv,
windows_momentum.csv, book_anomalies_momentum.csv, trades_harvester_sim.csv,
windows_harvester_sim.csv) so history is preserved.
"""

import argparse
import asyncio
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

import paper_bot as base_mod
import paper_harvester as harv_mod
import paper_momentum_impulse as mom_mod

DIR = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "btc5m-unified-paper/0.1", "Content-Type": "application/json"}
WSS_URL = "wss://ws-live-data.polymarket.com"
CLOB_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SUB = {"action": "subscribe", "subscriptions": [
    {"topic": "crypto_prices_chainlink", "type": "*",
     "filters": json.dumps({"symbol": "btc/usd"})}]}


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} [unified] {msg}", flush=True)


async def get_json(url, timeout=8):
    def _get():
        req = urllib.request.Request(url, headers=UA)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    return await asyncio.get_event_loop().run_in_executor(None, _get)


# ---------------- shared feeds (union of both bots' needs) ----------------
class SharedChainlinkStream:
    """Single btc/usd tick store serving both strategies (duck-typed)."""

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

    # paper_bot-only helpers:
    def realized_range(self, seconds):
        if not self.ticks:
            return None
        cut = self.latest[0] - seconds * 1000
        vals = [v for ts, v in self.ticks if ts >= cut]
        return max(vals) - min(vals) if len(vals) > 5 else None

    def value_ago(self, seconds):
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
                    log("chainlink stream connected (shared)")
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


class SharedMarketFeed:
    """Single BTC-5m CLOB book serving both strategies (momentum superset)."""

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
        self.BOOK_FALLBACK_S = 5.0

    def fresh(self):
        return (time.time() - self.last_book_msg) < self.BOOK_FALLBACK_S

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

    def book_age(self, side):
        b = self.books[side]
        if not b or "ts" not in b:
            return None
        return time.time() - b["ts"]

    def crossed(self, side):
        b = self.books[side]
        if not b or not b["asks"] or not b["bids"]:
            return False
        return b["bids"][0][0] - b["asks"][0][0] > 1e-9

    async def run(self, stream):
        del stream
        while True:
            now = int(time.time())
            win = now // 300 * 300
            if win != self.win:
                await self._load_window(win)
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
                    log("clob book stream connected (shared)")
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


class SharedBinanceSpot:
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


# ---------------- harvester scheduler ----------------
def _patch_harvester_tmp_exclusion():
    """Exclude collector atomic-write *.tmp files from harvester scans.

    The collector writes via tmp+rename (ParquetWriter); a live scan that
    includes *.tmp hits FileNotFoundError mid-rename and — under pm2
    autorestart — crash-loops. Filter to real *.parquet files only.
    """
    import pyarrow.dataset as ds
    from pathlib import Path

    _orig_dataset = ds.dataset

    def _dataset_no_tmp(source, *args, **kwargs):
        try:
            p = Path(str(source))
            if p.is_dir() and kwargs.get("format") == "parquet":
                files = [str(f) for f in p.rglob("*.parquet")
                         if not f.name.endswith(".tmp") and ".tmp" not in f.name]
                if files:
                    return _orig_dataset(files, *args, **kwargs)
        except Exception:
            pass
        return _orig_dataset(source, *args, **kwargs)

    ds.dataset = _dataset_no_tmp


async def harvester_loop(interval_min, sim_minutes, variant):
    """Periodic harvester simulation; failures are logged, never fatal."""
    _patch_harvester_tmp_exclusion()
    log(f"harvester scheduler: every {interval_min}m, sim {sim_minutes}m, variant {variant}")
    await asyncio.sleep(60)  # let live bots + collector stabilize first
    while True:
        try:
            log(f"harvester run start (variant={variant}, minutes={sim_minutes})")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, lambda: harv_mod.run_simulation(minutes=sim_minutes, variant=variant))
            log("harvester run done")
        except Exception as e:
            log(f"harvester run failed (non-fatal): {e!r}")
        await asyncio.sleep(interval_min * 60)


async def main(minutes=None, harvester=True, harvester_interval_min=60,
               harvester_minutes=120, harvester_variant="twap"):
    if not _WEBSOCKETS_OK:
        log("FATAL: `websockets` not installed — run `.venv/bin/pip install websockets` first")
        sys.exit(2)

    stream = SharedChainlinkStream()
    market = SharedMarketFeed()
    binance = SharedBinanceSpot()

    base_broker = base_mod.PaperBroker(
        os.path.join(DIR, "trades.csv"), os.path.join(DIR, "windows.csv"))
    base_strat = base_mod.Strategy(stream, market, base_broker, binance)

    mom_broker = mom_mod.PaperBroker(
        os.path.join(DIR, "trades_momentum.csv"), os.path.join(DIR, "windows_momentum.csv"))
    mom_strat = mom_mod.ImpulseSkewStrategy(stream, market, mom_broker)

    log(f"unified start — base bankroll {base_mod.BANKROLL0:.0f} + "
        f"momentum bankroll {mom_mod.BANKROLL0:.0f} (shared feeds) "
        f"harvester={'on' if harvester else 'off'} — 1 process")
    tasks = [
        stream.run(),
        market.run(stream),
        market.run_wss(),
        binance.run(),
        base_mod.Strategy.run(base_strat),
        mom_mod.ImpulseSkewStrategy.run(mom_strat),
    ]
    # paper_bot Strategy also needs its kline poller (ATR); run it explicitly
    async def _base_klines():
        await base_strat._poll_klines()
    # NOTE: base Strategy.run() already spawns _poll_klines itself, so no
    # extra task needed — kept here as documentation of the shared design.
    del _base_klines

    if harvester:
        tasks.append(harvester_loop(harvester_interval_min, harvester_minutes, harvester_variant))

    try:
        if minutes:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=minutes * 60)
        else:
            await asyncio.gather(*tasks)
    except (asyncio.TimeoutError, KeyboardInterrupt):
        pass

    # graceful shutdown: flatten open positions into the live book
    if getattr(base_strat, "pos", None):
        side = base_strat.pos["side"]
        levels = market.books[side]["bids"] if market.books[side] else []
        base_broker.execute("SELL", side, base_strat.pos["shares"], levels,
                            base_strat.strike_win, "shutdown flatten")
    if getattr(mom_strat, "pos", None):
        for leg in list(mom_strat.pos):
            side = leg["side"]
            levels = market.books[side]["bids"] if market.books[side] else []
            mom_broker.execute("SELL", side, leg["shares"], levels,
                               mom_strat.strike_win, "shutdown flatten")
    log(f"shutdown — base cash {base_broker.cash:.2f} | momentum cash {mom_broker.cash:.2f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Unified BTC 5m paper bots (1 process)")
    ap.add_argument("--minutes", type=float, default=None, help="stop after N minutes (default: forever)")
    ap.add_argument("--no-harvester", action="store_true", help="disable harvester scheduler")
    ap.add_argument("--harvester-interval-min", type=float, default=60)
    ap.add_argument("--harvester-minutes", type=int, default=120)
    ap.add_argument("--harvester-variant", choices=["point", "twap", "dual"], default="twap")
    args = ap.parse_args()
    try:
        asyncio.run(main(args.minutes, not args.no_harvester,
                         args.harvester_interval_min, args.harvester_minutes,
                         args.harvester_variant))
    except (asyncio.TimeoutError, KeyboardInterrupt):
        pass
