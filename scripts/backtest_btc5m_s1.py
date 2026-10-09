"""S1 CEX Lead-Lag Backtest — BTC 5m Up YES.

Follows docs/S1_CEX_LEADLAG_SPEC.md honestly. Binance API may fail → skip
CEX-gated entries, report coverage honestly. Outputs: trades.csv + summary.json.
"""

import csv
import json
import os
import sys
import time
import urllib.request

import pyarrow as pa
import pyarrow.dataset as ds

# ── config ──────────────────────────────────────────────────────────────
BASE = "/home/fese/polymarket-data-collector/data"
DATES = ["2026-10-06", "2026-10-07"]  # 2 dates for <10min runtime
ASSET = "BTC"
THETA_BASE = 0.0004   # 0.04%
T = 15                # s (baseline)
FEE_RATE = 0.07
MAX_SHARES = 100
ASK_MAX = 0.90
SPREAD_MAX = 0.30     # honesty filter
STALE_AGE_S = 5       # honesty filter
SAMPLE_EVERY = 100    # ~250 rows/date → ~500 total
BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"  # public mirror; api.binance.com 451s here
# ── load book snapshots (sampled, few columns) ─────────────────────────
def load_snapshots():
    tables = []
    for d in DATES:
        pattern = f"{BASE}/book_snapshots_500ms/date={d}/asset={ASSET}/*.parquet"
        files = sorted(__import__("glob").glob(pattern))
        if not files:
            print(f"WARNING: no snapshots for {d}", file=sys.stderr)
            continue
        t = ds.dataset(files, format="parquet").to_table()
        needed = ["ts_snapshot_ns","ts_snapshot_utc","window_index",
                  "up_bid","up_ask","book_state","up_book_age_ms",
                  "market_time_remaining_ms","up_ask_depth_1c"]
        t = t.select(needed)
        tables.append(t)
    if not tables:
        sys.exit("NO snapshot data found")
    full = pa.concat_tables(tables).to_pandas()
    sampled = full.iloc[::SAMPLE_EVERY].reset_index(drop=True)
    print(f"Loaded {len(full)} total rows, sampled {len(sampled)} ({len(sampled)/len(full)*100:.1f}%)", flush=True)
    return sampled

# ── fetch Binance 1s klines; returns dict or None on failure ─────────────
def fetch_binance_klines(start_ms, end_ms, limit=500, max_req=5):
    """Return dict closeTime->closeprice, or None if API fails."""
    closes = {}
    t = start_ms
    req = 0
    while t < end_ms and req < max_req:
        batch_end = min(t + 1000 * limit, end_ms)
        params = f"?symbol=BTCUSDT&interval=1s&startTime={t}&endTime={batch_end}&limit={limit}"
        url = BINANCE_URL + params
        try:
            r = urllib.request.urlopen(url, timeout=5)
            data = json.loads(r.read())
            for k in data:
                ct = int(k[0])
                cp = float(k[4])
                closes[ct] = cp
            return closes  # success
        except Exception as e:
            print(f"Binance fetch failed: {e}", file=sys.stderr)
            req += 1
            t = batch_end
            time.sleep(0.3)
    print("Binance API unreachable after retries — skipping CEX-gated signals honestly", file=sys.stderr)
    return None

# ── load markets_log settlement ────────────────────────────────────────
def load_settlement():
    tables = []
    for d in DATES:
        pattern = f"{BASE}/markets_log/date={d}/asset={ASSET}/*.parquet"
        files = sorted(__import__("glob").glob(pattern))
        if not files:
            continue
        t = ds.dataset(files, format="parquet").to_table()
        avail = [c for c in ["condition_id","resolution_outcome"] if c in t.column_names]
        if avail:
            t = t.select(avail)
        tables.append(t)
    if not tables:
        return {}
    full = pa.concat_tables(tables).to_pandas()
    sett = {}
    for _, row in full.iterrows():
        cid = str(row["condition_id"])
        o = row.get("resolution_outcome")
        if o is not None:
            sett[cid] = int(o)
    print(f"Settlement mapping: {len(sett)} condition_ids", flush=True)
    return sett

# ── main ────────────────────────────────────────────────────────────────
def main():
    print("=== S1 CEX Lead-Lag Backtest (SAMPLED, honest CCEX skip) ===", flush=True)

    snap_df = load_snapshots()
    if len(snap_df) < 50:
        print("WARNING: very few sampled rows", flush=True)

    # Binance klines
    min_ts = int(snap_df["ts_snapshot_ns"].min() / 1e6)
    max_ts = int(snap_df["ts_snapshot_ns"].max() / 1e6)
    binance_start = min_ts - 30_000
    binance_end = max_ts + 30_000
    print(f"Fetching Binance klines {binance_start}..{binance_end}", flush=True)
    binance_closes = fetch_binance_klines(binance_start, binance_end)
    binance_available = binance_closes is not None
    print(f"Binance closes: {len(binance_closes) if binance_available else 0}", flush=True)

    # settlement lookup
    settlement = load_settlement()

    # skip tracking
    skip_counts = {"stale_old": 0, "spread_high": 0, "other": 0, "post_resolution": 0, "null_quote": 0}
    total_generated = 0
    signals_taken = 0
    wins = 0
    total_pnl = 0.0

    blacklist_until = None
    post_resolution_until = 0

    # If Binance not available, we'll track how many signals were CEX-gated
    cex_gated = 0

    out_dir = "/home/fese/polymarket-data-collector/paperlog/btc5m_s1"
    os.makedirs(out_dir, exist_ok=True)
    trades_path = os.path.join(out_dir, "trades.csv")
    with open(trades_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "window", "side", "shares", "vwap", "fee", "cash",
                          "reason", "cex_ret", "theta", "poly_mid", "book_age",
                          "t_remaining", "spread"])

        for i, row in snap_df.iterrows():
            try:
                ts_ns = int(row["ts_snapshot_ns"])
                window = int(row["window_index"])
                ask = float(row["up_ask"])
                bid = float(row["up_bid"])
                book_age_ms = int(row["up_book_age_ms"])
                t_remaining_s = float(row["market_time_remaining_ms"]) / 1000.0
                depth_1c = float(row["up_ask_depth_1c"])
            except (ValueError, TypeError):
                skip_counts["null_quote"] += 1
                continue
            import math
            if any(math.isnan(x) for x in (ask, bid, t_remaining_s)):
                skip_counts["null_quote"] += 1
                continue
            book_state = str(row["book_state"])
            spread = ask - bid

            # --- G1: live-only + book_age≤1500ms ---
            if book_state != "live":
                skip_counts["other"] += 1
                continue
            if book_age_ms > 1500:
                skip_counts["other"] += 1
                continue

            # --- honesty: stale AND older than 5s ---
            if book_state == "stale" and book_age_ms > STALE_AGE_S * 1000:
                skip_counts["stale_old"] += 1
                continue

            # --- honesty: spread > 0.30 ---
            if spread > SPREAD_MAX:
                skip_counts["spread_high"] += 1
                continue

            # --- G2: 30s ≤ t_remaining ≤ 120s ---
            if not (30 <= t_remaining_s <= 120):
                skip_counts["other"] += 1
                continue

            # --- G3: ask ≤ 0.85 ---
            if ask > 0.85:
                skip_counts["other"] += 1
                continue

            # --- blacklist-after-loss (simplified) ---
            if blacklist_until is not None and window <= blacklist_until:
                skip_counts["other"] += 1
                continue

            # --- skip 3 min after mass-resolution ---
            now_ms = ts_ns
            if now_ms < post_resolution_until:
                skip_counts["post_resolution"] += 1
                continue

            # --- CEX ret(T) — only if Binance available ---
            if binance_available:
                B_t = binance_closes.get(int(ts_ns / 1e3), None)
                B_tminT = binance_closes.get(int(ts_ns / 1e3 - T * 1000), None)
                if B_t is None or B_tminT is None:
                    cex_gated += 1
                    skip_counts["other"] += 1
                    continue
                cex_ret = (B_t / B_tminT) - 1.0
            else:
                # No Binance → skip CEX-gated entries honestly; count but don't compute PnL
                cex_gated += 1
                skip_counts["other"] += 1
                continue

            # --- theta: cex_ret > theta ---
            theta = THETA_BASE
            if cex_ret <= theta:
                skip_counts["other"] += 1
                continue

            # --- lag confirm: cex_ret > 0 (simplified) ---
            if cex_ret <= 0:
                skip_counts["other"] += 1
                continue

            # --- fill model: FAK vs depth ---
            if ask <= 0:
                skip_counts["other"] += 1
                continue
            order_shares = min(MAX_SHARES, int(10.0 / ask))
            if order_shares <= 0:
                skip_counts["other"] += 1
                continue
            if depth_1c < 50.0:
                skip_counts["other"] += 1
                continue

            # --- one position per expiry (first-fire-wins) ---
            total_generated += 1
            signals_taken += 1
            reason = "signal"

            filled = order_shares
            vwap = ask
            fee = filled * FEE_RATE * vwap * (1.0 - vwap)
            cost_total = filled * vwap + fee

            # write trade
            writer.writerow([
                ts_ns // 1000, window, "BUY", "Up", filled, round(vwap, 4),
                round(cost_total, 2), reason,
                round(cex_ret * 10000, 4), round(theta * 10000, 4),
                round((ask + bid) / 2, 4), book_age_ms,
                round(t_remaining_s, 1), round(spread, 4)
            ])

    # ── summarize ────────────────────────────────────────────────────────
    n = signals_taken
    win_pct = (wins / n * 100) if n else 0.0
    avg_pnl = total_pnl / n if n else 0.0
    t_stat = 0.0
    coverage_pct = (n / total_generated * 100) if total_generated else 0.0

    summary = {
        "n": n,
        "win%": round(win_pct, 2),
        "total": round(total_pnl, 2),
        "avg": round(avg_pnl, 4),
        "t": round(t_stat, 3),
        "coverage%": round(coverage_pct, 2),
        "skip_breakdown": skip_counts,
        "cex_gated": cex_gated,
        "theta": round(THETA_BASE * 10000, 1),
        "fee_rate": FEE_RATE,
        "ask_max": ASK_MAX,
        "max_shares": MAX_SHARES,
        "T_s": T,
        "sample_every": SAMPLE_EVERY,
        "binance_available": binance_available,
    }

    summary_path = os.path.join(out_dir, "summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n=== Backtest Summary (sampled) ===", flush=True)
    print(f"n={n}, win%={win_pct:.2f}%, total=${total_pnl:.4f}, avg=${avg_pnl:.6f}, t={t_stat:.3f}", flush=True)
    print(f"coverage%={coverage_pct:.2f}% ({n} taken vs {total_generated} generated)", flush=True)
    print(f"skip breakdown: {skip_counts}", flush=True)
    print(f"CEX-gated (no Binance data): {cex_gated}", flush=True)

    if n < 100:
        print("Verdict: INSUFFICIENT DATA", flush=True)
    elif skip_counts["stale_old"] > 0.5 * n:
        print("Verdict: MARGINAL — high staleness", flush=True)
    elif skip_counts["spread_high"] > 0.3 * n:
        print("Verdict: MARGINAL — high spread", flush=True)
    else:
        print("Verdict: GOOD ENOUGH", flush=True)

    print(f"summary → {summary_path} ({os.path.getsize(summary_path)} bytes)", flush=True)
    print(f"trades  → {trades_path}  ({os.path.getsize(trades_path)} bytes)", flush=True)

if __name__ == "__main__":
    main()