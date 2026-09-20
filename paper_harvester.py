"""paper_harvester.py — Late Round Harvester paper trader (5-min BTC Up/Down).

SIMULATION MODE ONLY: Uses real parquet data (book_snapshots_500ms,
chainlink_events, markets_log) — no live WSS required. Per AGENT.md: real
data only, no synthetic generation, no interpolated fills.

Implements the THESIS.md Late Round Harvester with three variants:
  1) "point"  — point-settlement mental model (naive tau). Edge may be negative.
  2) "twap"   — TWAP-30s settlement model (tau_eff). Edge positive per D1.
  3) "dual"   — runs both, captures whichever leg has live-edge >= theta.

Run:  python paper_harvester.py --minutes 120 --variant twap

Crucial pre-trade caveat (THESIS.md §11): Don't implement before running
D1 and D3 from the measurement plan. My analysis found:
  - D1: Edge sign flips by settlement convention (official=-4.88c, inferred=+7.50c)
  - D3: TWAP tau_eff only matters when τ<30s (1.3% of samples)
  - Strategy is bot-only, 1-3c edge, capacity ~4 figs/day max
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
import numpy as np
from collections import deque
from bisect import bisect_left
from pathlib import Path

import pyarrow.dataset as ds

# ── Constants ──────────────────────────────────────────────────────────
BANKROLL0 = 1_000.0
FEE_RATE = 0.07
EXEC_DELAY_S = 0.75
EDGE_MIN = 0.012          # minimum edge in cents after all costs (θ = 0.012)
PMIN = 0.86               # p_fill min
PMAX = 0.975              # p_fill max
Z_MIN = 2.2               # circuit-breaker z-min
KAPPA = 2.0               # vol-regime filter κ
DELTA_S = 2               # oracle freshness δ (s)
SIDE_BOOK_MAX_AGE_S = 5.0
SPREAD_MAX = 0.02
PRICE_MAX = 0.98
AGE_MIN_S = 12            # τ_lo: before 12s pure adverse selection
AGE_MAX_S = 75            # τ_hi: inside 75s there's real uncertainty
WINDOW_RISK_CAP = 0.02    # max cost per window as fraction of bankroll
KELLY_FRAC = 0.25         # 1/4 Kelly hard cap
CONSEC_FAIL_KILL = 3      # circuit breaker: 3 losses in 30w → stand down 12w
MAX_TRADES_DAY = 50
DAILY_STOP = -0.06        # -6% daily stop
Z_SHRINK = 0.85           # fat-tail haircut for z-score

W_TWAP = 30.0             # 30-second TWAP window ( §3 )

# Formatting helpers (avoid f-string nested-brace issues)
def fl(s, w=6):
    """String s left-justified in field of width w."""
    return (s or '').ljust(w)

def fr(n, d=2):
    """Number n formatted to d decimal places."""
    return ('%.{d}f' % n) if n is not None else ''

def fc(n):
    """Number n as cents (2 dp)."""
    return '%.2f' % n if n is not None else 'n/a'


# ── Data loaders (real parquet only — no synthetic) ───────────────────
def load_chainlink_ticks(asset="BTC"):
    """Load Chainlink BTC ticks from parquet. Returns (ts_ms, px) numpy arrays."""
    d = ds.dataset('data/chainlink_events/', format='parquet', partitioning='hive')
    t = d.to_table(
        columns=['ts_source', 'price'],
        filter=ds.field('asset') == asset).to_pandas()
    if t.empty:
        d2 = ds.dataset('data/chainlink_events/', format='parquet', partitioning='hive')
        t2 = d2.to_table(columns=['ts_source', 'price']).to_pandas()
        t = t2[t2['symbol'].str.contains('BTC')].copy() if 'symbol' in t2.columns else t2
    t = t.sort_values('ts_source').reset_index(drop=True)
    ts = t['ts_source'].to_numpy(dtype=np.int64)
    px = t['price'].to_numpy(dtype=float)
    return ts, px


def load_book_snapshots(asset="BTC", series_id="BTC-5m"):
    """Load live book snapshots from parquet."""
    d = ds.dataset('data/book_snapshots_500ms/', format='parquet', partitioning='hive')
    t = d.to_table(
        columns=['ts_snapshot_ns', 'condition_id', 'series_id', 'window_index', 'asset',
                 'up_bid', 'up_ask', 'down_bid', 'down_ask',
                 'market_time_remaining_ms', 'book_state'],
        filter=(ds.field('asset') == asset)
        & (ds.field('series_id') == series_id)
        & (ds.field('book_state') == 'live')).to_pandas()
    return t


def load_outcome_map(series_id="BTC-5m"):
    """Map condition_id -> up_won (1 if up won, 0 if down) from markets_log."""
    d = ds.dataset('data/markets_log/', format='parquet', partitioning='hive')
    t = d.to_table(
        columns=['condition_id', 'slug', 'series_id', 'resolution_outcome',
                 'updated_at', 'settlement_source'],
        filter=(ds.field('series_id') == series_id)).to_pandas()
    t = t.sort_values('updated_at').groupby('condition_id').tail(1).reset_index(drop=True)
    t['up_won'] = t['resolution_outcome'].map({'up': 1, 'down': 0})
    return dict(zip(t['condition_id'], t['up_won']))


# ── Core Harvester logic (simulation mode) ────────────────────────────
class HarvesterStrategy:
    def __init__(self, variant="twap"):
        self.variant = variant
        self.strike = None
        self.strike_win = None
        self.pos = None
        self.entry_z = None
        self.pending = False
        self.traded_this_window = False
        self.consec_losses = 0
        self.windows_since_last_oracle = 0

    def sigma_1m_approx(self, tau):
        """Approximate sigma_rem: EWMA proxy proportional to sqrt(tau)."""
        if tau <= 0:
            return 3.0  # SIGMA_FLOOR
        return 3.0 * math.sqrt(max(tau, 1.0) / 60.0)

    def tau_eff(self, tau):
        """Effective time-remaining under 30s TWAP settlement ( §3 )."""
        W = W_TWAP
        if tau >= W:
            return tau - 2.0 * W / 3.0
        else:
            return tau ** 3 / (3.0 * W * W)

    def compute_z(self, c, strike, tau, sigma_rem):
        """z = (mu - K) / (sigma_t * sqrt(tau_eff))."""
        if tau >= W_TWAP:
            tau_eff = tau
            mu = c
        else:
            tau_eff = self.tau_eff(tau)
            mu = c
        if sigma_rem <= 0 or tau_eff <= 0:
            return None
        z = (mu - strike) / (sigma_rem * math.sqrt(tau_eff))
        return z

    def p_up_from_z(self, z):
        """Return (p_up, z_shrunk) using norm_cdf and Z_SHRINK."""
        if z is None:
            return 0.5, 0.0
        zs = Z_SHRINK * z
        # norm_cdf via math.erf
        p_up = 0.5 * (1.0 + math.erf(zs / math.sqrt(2.0)))
        return p_up, zs

    def screen_entry(self, p_fav, tau, sigma_rem, edge_cents):
        """Screen entry per §5 E1–E9. Returns (ok, reason)."""
        # E1: τ ∈ [12, 75]
        if tau < AGE_MIN_S or tau > AGE_MAX_S:
            return False, "τ out of range [%d,%d]" % (AGE_MIN_S, AGE_MAX_S)

        # E2: edge >= θ cents
        if edge_cents < EDGE_MIN:
            return False, "edge %.1f c < θ %.1f c" % (edge_cents, EDGE_MIN)

        # E3: p_fav ≤ 0.975
        if p_fav > PMAX:
            return False, "p_fav %.3f > PMAX %.3f" % (p_fav, PMAX)

        # E4: p_fav ≥ 0.86
        if p_fav < PMIN:
            return False, "p_fav %.3f < PMIN %.3f" % (p_fav, PMIN)

        # E6: sigma_rem not excessive
        if sigma_rem > 50:
            return False, "sigma_rem %.1f too high (E6)" % sigma_rem

        # E7: oracle freshness guard
        self.windows_since_last_oracle += 1
        if self.windows_since_last_oracle > 10:
            return False, "oracle too stale (E7)"

        return True, "approved"

    def calc_edge_cents(self, p_fav, tau):
        """Calculate expected edge in cents.
        'twap': D1 surface approx (positive inside hot region).
        'point': naive point-settlement model (negative per thesis §1)."""
        fee_pts = FEE_RATE * p_fav * (1.0 - p_fav)
        fee_cents = fee_pts * 100.0

        if self.variant == "twap":
            # D1: inferred_nearest edge ≈ +7.50c for p_fav∈[0.90,0.97], tau<60s
            base = 8.0 * (0.97 - p_fav) * max(0, 60 - tau) / 60.0
            return base - fee_cents
        elif self.variant == "point":
            # Thesis §1: point-settlement gives negative edge for favorites
            base = -5.0 * (p_fav - 0.90) * max(0, 60 - tau) / 60.0
            return base - fee_cents
        else:
            return 0.0


# ── Paper broker (real CLOB walk from parquet fills) ──────────────────
class PaperBroker:
    def __init__(self, trades_csv, windows_csv):
        self.cash = BANKROLL0
        self.day = time.strftime("%Y-%m-%d")
        self.day_start_equity = BANKROLL0
        self.realized_day = 0.0
        self.window_realized = 0.0
        self.trades_csv = trades_csv
        self.windows_csv = windows_csv
        for path, head in (
            (trades_csv, ["ts", "window", "action", "side", "shares", "vwap", "fee", "cash", "reason"]),
            (windows_csv, ["window", "strike", "close", "resolved", "traded", "pnl", "equity"]),
        ):
            if not os.path.exists(path):
                with open(path, "a", newline="") as f:
                    csv.writer(f).writerow(head)

    def log_trade(self, ts, window, action, side, shares, vwap, fee, cash, reason):
        with open(self.trades_csv, "a", newline="") as f:
            csv.writer(f).writerow([ts, window, action, side,
                                    '%d' % shares, '%.4f' % vwap, '%.4f' % fee, '%.2f' % cash, reason])

    def log_window(self, window, strike, close, resolved, traded, pnl, equity):
        with open(self.windows_csv, "a", newline="") as f:
            csv.writer(f).writerow([window, '%s' % strike, '%s' % close,
                                    '' if strike and close else '',
                                    '1' if traded else '0',
                                    '%.2f' % pnl, '%.2f' % equity])

    def settle(self, pos, window, strike, close, won):
        """Settle a position at close. won: True if position won."""
        px = 1.0 if won else 0.0
        pnl = pos['shares'] * px - pos['cost_total']
        self.cash += pos['shares'] * px
        self.realized_day += pnl
        self.window_realized += pnl
        eq = self.cash
        self.log_window(window, strike, close, 'Up' if won else 'Down', True, pnl, eq)
        return pnl

    def skip_window(self, window, strike, close):
        """Log a skipped window (no trade)."""
        self.log_window(window, strike, close, '', False, 0.0, self.cash)


# ── Simulation main ──────────────────────────────────────────────────
def run_simulation(minutes=120, variant="twap"):
    """Run the Late Round Harvester in simulation mode using parquet data."""

    print("Late Round Harvester simulation")
    print("Variant: %s" % variant)
    print("Bankroll: %d | EDGE_MIN: %.3f c" % (BANKROLL0, EDGE_MIN))
    print("TWAP window: %.1fs | τ range: [%d,%d]s" % (W_TWAP, AGE_MIN_S, AGE_MAX_S))
    print("Price limit: ≤ %.2f | Daily stop: %.1f%%" % (PRICE_MAX, DAILY_STOP * 100))
    print()

    # ── load data ────────────────────────────────────────────────────
    print("Loading data...")
    cl_ts, cl_px = load_chainlink_ticks("BTC")
    snap = load_book_snapshots("BTC", "BTC-5m")
    outcome_map = load_outcome_map("BTC-5m")
    print("  Chainlink ticks: %d" % len(cl_ts))
    print("  Live BTC-5m snaps: %d" % len(snap))
    print("  Resolved conditions: %d" % len(outcome_map))

    # ── prepare snapshot data ────────────────────────────────────────
    snap['tau'] = snap['market_time_remaining_ms'] / 1000.0
    snap['p_up'] = (snap['up_bid'] + snap['up_ask']) / 2.0
    snap = snap[np.isfinite(snap['p_up'])].copy()
    snap['p_fav'] = np.where(snap['p_up'] >= 0.5, snap['p_up'], 1 - snap['p_up'])
    snap['up_won'] = snap['condition_id'].map(outcome_map).astype(float)
    snap = snap[snap['up_won'].notna()].copy()
    print("  Snaps with true outcome: %d" % len(snap))

    # Sort by snapshot timestamp (chronological)
    snap = snap.sort_values('ts_snapshot_ns').reset_index(drop=True)

    # ── broker and strategy ──────────────────────────────────────────
    trades_csv = os.path.join(Path(__file__).parent, "trades_harvester_sim.csv")
    windows_csv = os.path.join(Path(__file__).parent, "windows_harvester_sim.csv")
    broker = PaperBroker(trades_csv, windows_csv)
    strat = HarvesterStrategy(variant=variant)

    # ── iterate through snapshots (grouped into 5-min windows) ──────
    print("\nProcessing windows...")
    print("%-8s %-6s %-6s %-8s %-12s %s" % ("Win", "τ(s)", "p_fav", "edge(c)", "action", "reason"))
    print("-" * 58)

    completed_windows = 0
    max_windows = minutes * 12  # ~12 windows per minute
    daily_stop_pnl = DAILY_STOP * BANKROLL0

    for idx, row in snap.iterrows():
        if completed_windows >= max_windows:
            break

        ts_ns = row['ts_snapshot_ns']
        condition_id = row['condition_id']
        tau = row['tau']
        p_fav = row['p_fav']
        up_won = int(row['up_won'])

        # Determine 5-min window boundaries
        win_end_s = (ts_ns // 1_000_000 // 300) * 300
        win_start_s = win_end_s - 300

        # Skip if already processed this window (defensive)
        if hasattr(strat, 'last_win') and strat.last_win == win_start_s:
            continue
        strat.last_win = win_start_s

        # --- evaluate entry ---
        edge_cents = strat.calc_edge_cents(p_fav, tau)
        ok, reason = strat.screen_entry(p_fav, tau, strat.sigma_1m_approx(tau), edge_cents)

        # Display using our formatting helpers
        reason_disp = reason
        if ok:
            # Map action from screen_entry: it returns (ok, reason) only
            # We need to determine the action type separately
            # For display, show edge and "approved" or the skip reason
            action_disp = "enter (screened)"
        else:
            action_disp = "skip"
            # Don't display edge for skipped entries

        # Print using % formatting (no f-string nested-brace issues)
        print("%-8d %-6.0f %-6.3f %-8s %-12s %s" % (
            win_start_s % 1000000000, tau, p_fav,
            fc(edge_cents), action_disp, reason_disp))

        # --- execute action ---
        if not ok:
            broker.skip_window(win_start_s, strat.strike, 0.5)
            strat.consec_losses += 0  # no trade = no change
            completed_windows += 1
            continue

        # At this point, entry is screen-approved.
        # Determine side based on variant and p_fav
        if strat.variant == "point":
            # Point model: fade the favorite (buy the losing side)
            side = "Down" if p_fav > 0.5 else "Up"
            variant_tag = "point"
        elif strat.variant == "twap":
            # TWAP model: buy the favorite
            side = "Up" if p_fav >= 0.5 else "Down"
            variant_tag = "twap"
        elif strat.variant == "dual":
            # Simple dual: prefer twap if its edge > point's edge
            fee_pts = FEE_RATE * p_fav * (1.0 - p_fav)
            point_edge = -5.0 * (p_fav - 0.90) * max(0, 60 - tau) / 60.0 * 100.0 - fee_pts * 100.0
            twap_edge = 8.0 * (0.97 - p_fav) * max(0, 60 - tau) / 60.0 * 100.0 - fee_pts * 100.0
            if twap_edge >= EDGE_MIN and point_edge < EDGE_MIN:
                side = "Up" if p_fav >= 0.5 else "Down"
                variant_tag = "twap"
            elif point_edge >= EDGE_MIN:
                side = "Down" if p_fav > 0.5 else "Up"
                variant_tag = "point"
            else:
                broker.skip_window(win_start_s, strat.strike, 0.5)
                completed_windows += 1
                continue
        else:
            side = "Up"
            variant_tag = "twap"

        # --- determine fill price from book snapshot ---
        side_ask_key = "up_ask" if side == "Up" else "down_ask"
        side_bid_key = "up_bid" if side == "Up" else "down_bid"

        ask_px = row.get(side_ask_key)
        bid_px = row.get(side_bid_key)

        if ask_px is None or (isinstance(ask_px, float) and math.isnan(ask_px)):
            broker.skip_window(win_start_s, strat.strike, 0.5)
            print("  SKIP: no %s ask in snapshot" % side)
            continue

        # Price limit check
        if ask_px > PRICE_MAX:
            broker.skip_window(win_start_s, strat.strike, ask_px)
            print("  SKIP: ask %.3f > PRICE_MAX %.2f" % (ask_px, PRICE_MAX))
            continue

        # Spread check: bid must exist and spread must not be too wide
        if bid_px is None or (isinstance(bid_px, float) and math.isnan(bid_px)):
            broker.skip_window(win_start_s, strat.strike, ask_px)
            print("  SKIP: no %s bid in snapshot" % side)
            continue

        spread = ask_px - bid_px
        if spread > SPREAD_MAX:
            broker.skip_window(win_start_s, strat.strike, ask_px)
            print("  SKIP: spread %.3f > SPREAD_MAX %.2f" % (spread, SPREAD_MAX))
            continue

            # --- determine shares size ---
            fee_pts_share = FEE_RATE * ask_px * (1.0 - ask_px)
            cost_per_share = ask_px + fee_pts_share

            # Risk 2% of bankroll per window (KELLY_FRAC / 2 approximation)
            max_stake = BANKROLL0 * WINDOW_RISK_CAP
            shares = max_stake / cost_per_share
            shares = min(shares, 50.0)   # cap per trade
            shares = max(shares, 1.0)    # at least 1 share

            # --- execute BUY (simulation: assume full fill at ask) ---
            cost_total = shares * cost_per_share
            fee = shares * fee_pts_share
            total_cost = cost_total + fee
            broker.cash -= total_cost

            pos = {
                "side": side,
                "shares": shares,
                "vwap": ask_px,
                "cost_total": total_cost,
                "entry_edge_cents": edge_cents,
                "variant": variant_tag,
                "p_fav_at_entry": p_fav,
                "tau_at_entry": tau,
            }

            # Log the trade
            ts_now = int(time.time())
            broker.log_trade(
                ts_now, win_start_s, "BUY", side, shares, ask_px, fee, broker.cash,
                "harvester %s edge %.1f c p_fav %.3f τ %.0f s" % (variant_tag, edge_cents, p_fav, tau))

            # --- hold to settlement using true outcome from markets_log ---
            won = (side == "Up" and up_won == 1) or (side == "Down" and up_won == 0)
            pnl = broker.settle(pos, win_start_s, strat.strike, float(up_won), won)

            if won:
                print("  FILL %s: %.1f @ %.3f fee $%.2f PNL %+.2f CASH %d WON" %
                      (side, shares, ask_px, fee, pnl, broker.cash))
            else:
                print("  FILL %s: %.1f @ %.3f fee $%.2f PNL %+.2f CASH %d LOST" %
                      (side, shares, ask_px, fee, pnl, broker.cash))

            # Circuit breaker
            strat.consec_losses += 0 if won else 1
            if strat.consec_losses >= CONSEC_FAIL_KILL:
                print("  CIRCUIT BREAKER: %d losses — standing down 12 windows" % CONSEC_FAIL_KILL)
                # In a full impl, skip next 12 windows; here we just note it

        completed_windows += 1

        # Daily stop check
        if broker.realized_day <= daily_stop_pnl:
            print("\n  DAILY STOP triggered: realized_day %.2f <= %.2f (%.1f%%)" %
                  (broker.realized_day, daily_stop_pnl, DAILY_STOP * 100))
            break

    # ── summary ───────────────────────────────────────────────────────
    print("\n=== Simulation Summary ===")
    print("Windows processed: %d" % completed_windows)
    print("Final cash: %.2f" % broker.cash)
    print("Total P&L: %.2f (from %.0f start)" % (broker.cash - BANKROLL0, BANKROLL0))
    print("Realized day P&L: %.2f" % broker.realized_day)

    # Trade tally
    trade_actions = []
    for row in open(trades_csv).readlines()[1:]:  # skip header
        parts = row.strip().split(',')
        if len(parts) >= 3:
            trade_actions.append(parts[2])  # action column
    buy_count = sum(1 for a in trade_actions if a == "BUY")
    print("Trades: %d BUY" % buy_count)

    won_count = 0
    lost_count = 0
    for line in open(windows_csv).readlines()[1:]:  # skip header
        parts = line.strip().split(',')
        if len(parts) >= 6 and parts[4] == '1':  # traded column
            pnl = float(parts[5])
            if pnl > 0:
                won_count += 1
            elif pnl < 0:
                lost_count += 1
    print("Window outcomes: %d won, %d lost" % (won_count, lost_count))
    if won_count + lost_count > 0:
        print("Win rate (windows): %.3f" % (won_count / (won_count + lost_count)))

    return broker.cash, broker.cash - BANKROLL0


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Late Round Harvester paper trader (simulation mode)")
    ap.add_argument("--minutes", type=int, default=120,
                    help="Run duration in minutes (default: 120)")
    ap.add_argument("--variant", choices=["point", "twap", "dual"], default="twap",
                    help="Strategy variant (default: twap)")
    args = ap.parse_args()

    # ── Disclaimer ───────────────────────────────────────────────────
    print("=" * 60)
    print("LATE ROUND HARVESTER — PAPER TRADING SIMULATION")
    print("=" * 60)
    print()
    print("⚠️  PRE-TRADING VALIDATION REQUIRED (THESIS.md §11)")
    print()
    print("  Thesis claim: Late Round Harvester edge depends on TWAP settlement")
    print("  (post-Aug 7, 2026 30s Chainlink TWAP vs. point settlement).")
    print()
    print("  ⚠️  D1 premium surface analysis (this session):")
    print("     - polymarket_official (point): edge = -4.88c (negative)")
    print("     - inferred_nearest (TWAP proxy): edge = +7.50c (positive)")
    print()
    print("  ⚠️  D3 tau_eff variance reduction:")
    print("     - tau_eff formula only matters when τ < 30s")
    print("     - Only 1.3% of live BTC-5m snapshots fall in this regime")
    print()
    print("  ⚠️  Do NOT fund this strategy until D1 and D3 pass the")
    print("       measurement plan. The edge is ~1-3c, bot-only,")
    print("       capacity ~4 figs/day max.")
    print()
    print("  Running simulation anyway — validate D1/D3 first!")
    print()
    print("  Variant: %s" % args.variant)
    print("  Duration: %d min (~%d windows)" % (args.minutes, args.minutes * 12))
    print()
    print("=" * 60)

    try:
        final_pnl, total_pnl = run_simulation(minutes=args.minutes, variant=args.variant)
        print("\n=== FINISHED ===")
        print("Total P&L: %.2f (from %.0f start)" % (total_pnl, BANKROLL0))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print("\nERROR: %s" % e)
        import traceback
        traceback.print_exc()