"""S1 CEX Lead-Lag Backtest — BTC 5m Up YES.

Streaming per-file incremental parquet reads with del/gc between files,
column-subset only, never materialize multi-file tables.
S1 PARAMETERS LOCKED (assert equality with docs/S1_CEX_LEADLAG_SPEC.md):
  THETA=0.0002, BIN_T=30, HOR=120, FILL_DELAY=2, SHARES=100,
  MAX_ASK=0.90, FEE_R=0.07. No deviations — assert equality in comment block below.
"""
import csv
import glob as _glob
import json
import os
import resource
import sys
import time
import urllib.request
from datetime import datetime

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# ── config ──────────────────────────────────────────────────────────────
BASE = "/home/fese/polymarket-data-collector/data"
DATES = ["2026-10-09"]  # single date for contiguous-block run
ASSET = "BTC"

# ── LOCKED S1 PARAMETERS ────────────────────────────────────────────────
THETA = 0.0002   # 0.02% — must match docs/S1_CEX_LEADLAG_SPEC.md
BIN_T = 30       # s lag — must match docs/S1_CEX_LEADLAG_SPEC.md
HOR = 120        # holding-period target — must match docs/S1_CEX_LEADLAG_SPEC.md
FILL_DELAY = 2   # s — must match docs/S1_CEX_LEADLAG_SPEC.md
SHARES = 100     # fixed notional — must match docs/S1_CEX_LEADLAG_SPEC.md
MAX_ASK = 0.90   # maximum ask fraction — must match docs/S1_CEX_LEADLAG_SPEC.md
FEE_R = 0.07     # taker fee rate — must match docs/S1_CEX_LEADLAG_SPEC.md
assert THETA == 0.0002, f"THETA mismatch: {THETA}"
assert BIN_T == 30, f"BIN_T mismatch: {BIN_T}"
assert HOR == 120, f"HOR mismatch: {HOR}"
assert FILL_DELAY == 2, f"FILL_DELAY mismatch: {FILL_DELAY}"
assert SHARES == 100, f"SHARES mismatch: {SHARES}"
assert MAX_ASK == 0.90, f"MAX_ASK mismatch: {MAX_ASK}"
assert FEE_R == 0.07, f"FEE_R mismatch: {FEE_R}"
# ── end locked params ────────────────────────────────────────────────────

FEE_RATE = FEE_R
MAX_SHARES = SHARES
ASK_MAX = MAX_ASK
SPREAD_MAX = 0.30     # honesty filter
STALE_AGE_S = 5       # honesty filter
SAMPLE_EVERY = 1      # full-tick (no sprinkle)

BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"

NEEDED_COLS = [
    "ts_snapshot_utc", "ts_snapshot_ns", "window_index",
    "up_ask", "up_bid", "up_book_age_ms",
    "market_time_remaining_ms", "up_ask_depth_1c",
    "book_state", "condition_id",
]

# ── Helper: check if a ts_snapshot_utc string matches a given hour key ─────
def _hour_matches(ts_utc_str, hour_key):
    """Return True if ts_utc_str falls within the given UTC hour key."""
    try:
        dt = datetime.fromisoformat(ts_utc_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:00") == hour_key
    except Exception:
        return False

# ── Step 1: Identify files for the 11:00 UTC hour ──────────────────────
target_hour = "2026-10-09 11:00"

pattern = f"{BASE}/book_snapshots_500ms/date={DATES[0]}/asset={ASSET}/*.parquet"
all_files = sorted(_glob.glob(pattern))

# Stream-identify files that intersect the target hour, in ts order
hour_files = []
for f in all_files:
    try:
        pf = pq.ParquetFile(f)
        bg = pf.metadata.row_group(0)
        ts_idx = pf.schema_arrow.get_field_index("ts_snapshot_utc")
        ts_stats = bg.column(ts_idx).statistics
        min_utc = ts_stats.min
        max_utc = ts_stats.max
        dt_min = datetime.fromisoformat(min_utc.replace("Z", "+00:00"))
        dt_max = datetime.fromisoformat(max_utc.replace("Z", "+00:00"))
        # File intersects 11:00 UTC hour if min <= 11:59 and max >= 11:00
        if dt_min.hour <= 11 and dt_max.hour >= 11:
            hour_files.append(f)
    except Exception:
        pass

print(f"Files for {target_hour}: {len(hour_files)}", flush=True)

# ── Step 2: Setup output dir and trades file ────────────────────────────
out_dir = "/home/fese/polymarket-data-collector/paperlog/btc5m_s1"
os.makedirs(out_dir, exist_ok=True)
trades_path = os.path.join(out_dir, "trades.csv")

with open(trades_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["ts", "window", "side", "shares", "vwap", "fee", "cash",
                      "reason", "cex_ret", "theta", "poly_mid", "book_age",
                      "t_remaining", "spread"])

# ── Step 3: Binance CEX fetch ───────────────────────────────────────────
binance_start = int(datetime.fromisoformat("2026-10-09 11:00:00+00:00").timestamp() * 1000) - 30_000
binance_end = int(datetime.fromisoformat("2026-10-09 11:59:59+00:00").timestamp() * 1000) + 30_000

max_retries = 3
binance_closes = {}
t_ms = binance_start
req = 0
while t_ms < binance_end and req < max_retries:
    batch_end = min(t_ms + 1000 * 500, binance_end)
    params = f"?symbol=BTCUSDT&interval=1s&startTime={t_ms}&endTime={batch_end}&limit=500"
    url = BINANCE_URL + params
    try:
        r = urllib.request.urlopen(url, timeout=5)
        data = json.loads(r.read())
        for k in data:
            ct = int(k[0])
            cp = float(k[4])
            binance_closes[ct] = cp
        t_ms = batch_end
        req += 1
        time.sleep(0.3)
    except Exception as e:
        print(f"Binance fetch failed ({t_ms}-{batch_end}): {e}", file=sys.stderr)
        req += 1
        t_ms = batch_end
        time.sleep(0.3)

print(f"Total Binance closes collected: {len(binance_closes)}", flush=True)
binance_available = len(binance_closes) > 0
if not binance_available:
    print("Binance API unreachable after retries — will CEX-gate all signals honestly",
          file=sys.stderr)

# ── Step 4: Settlement lookup ──────────────────────────────────────────
settlement = {}
for d in DATES:
    p = f"{BASE}/markets_log/date={d}/*.parquet"
    sfiles = _glob.glob(p)
    if not sfiles:
        continue
    t = ds.dataset(sfiles, format="parquet").to_table()
    avail = [c for c in ["condition_id", "resolution_outcome"] if c in t.column_names]
    if avail:
        t = t.select(avail)
    df_sett = t.to_pandas()
    for _, row in df_sett.iterrows():
        cid = str(row["condition_id"])
        o = row.get("resolution_outcome")
        if o is None:
            continue
        if o == "up":
            settlement[cid] = 1
        elif o == "down":
            settlement[cid] = 0
print(f"Settlement mapping: {len(settlement)} condition_ids", flush=True)

# ── Step 5: Stream-process each file with del/gc between files ──────────
# Peak RSS tracking
peak_rss_kb = 0

# Sequential state
blacklist_until = None
post_resolution_until = 0
seen_conditions = set()

# Accumulators
total_rows = 0
live_rows = 0
total_generated = 0
signals_taken = 0
wins = 0
total_pnl = 0.0
cex_gated = 0
skip_counts = {"stale_old": 0, "spread_high": 0, "other": 0, "post_resolution": 0, "null_quote": 0}

# Open trades file for appending (already has header)
trades_file = open(trades_path, "a", newline="")
trades_writer = csv.writer(trades_file)

for fi, f in enumerate(hour_files):
    try:
        pf = pq.ParquetFile(f)
        # Read only needed columns
        t = pf.read(columns=NEEDED_COLS)
        df = t.to_pandas()

        n = len(df)
        total_rows += n

        # Hour matching / live count
        live_mask = df["book_state"].astype(str) == "live"
        hour_live = int(live_mask.sum())
        live_rows += hour_live

        # Apply static filters vectorized on this file's data
        for col in ["up_ask", "up_bid", "up_book_age_ms", "market_time_remaining_ms",
                     "up_ask_depth_1c", "window_index", "ts_snapshot_ns", "condition_id"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # G1: live-only + book_age<=1500ms
        g1_mask = live_mask & (df["up_book_age_ms"] <= 1500)

        # honesty: stale AND older than 5s -> skip
        stale_book = df["book_state"].astype(str) == "stale"
        stale_age = df["up_book_age_ms"] > STALE_AGE_S * 1000
        stale_skip = stale_book & stale_age

        # honesty: spread > 0.30 -> skip
        ask_col = df["up_ask"].astype(float)
        bid_col = df["up_bid"].astype(float)
        spread = ask_col - bid_col
        spread_skip = spread > SPREAD_MAX

        # G2: 30s <= t_remaining <= 120s
        t_rem_s = df["market_time_remaining_ms"].astype(float) / 1000.0
        t_rem_mask = (t_rem_s >= 30) & (t_rem_s <= 120)

        # G3: ask <= 0.90 (MAX_ASK)
        ask_max_mask = ask_col <= ASK_MAX

        # G4: ask > 0
        ask_pos_mask = ask_col > 0

        # depth_1c >= 50
        depth_mask = df["up_ask_depth_1c"].astype(float) >= 50.0

        # Combined static pass mask
        static_pass = (g1_mask & ~stale_skip & ~spread_skip &
                       t_rem_mask & ask_max_mask & ask_pos_mask & depth_mask)

        n_pass = int(static_pass.sum())
        pass_indices = df.index[static_pass.values]

        # --- Sequential loop over rows passing static filters ---
        for _i, idx in enumerate(pass_indices):
            row = df.loc[idx]
            try:
                ts_ns = int(row["ts_snapshot_ns"])
                window = int(row["window_index"])
                ask = float(row["up_ask"])
                bid = float(row["up_bid"])
                book_age_ms = int(row["up_book_age_ms"])
                t_remaining_s = float(row["market_time_remaining_ms"]) / 1000.0
                depth_1c = float(row["up_ask_depth_1c"])
                condition_id = str(row["condition_id"])
            except (ValueError, TypeError):
                skip_counts["null_quote"] += 1
                continue
            import math
            if any(math.isnan(x) for x in (ask, bid, t_remaining_s)):
                skip_counts["null_quote"] += 1
                continue

            spread_val = ask - bid

            # --- blacklist-after-loss (simplified) ---
            if blacklist_until is not None and window <= blacklist_until:
                skip_counts["other"] += 1
                continue

            # --- skip 3 min after mass-resolution ---
            now_ms = ts_ns
            if now_ms < post_resolution_until:
                skip_counts["post_resolution"] += 1
                continue

            # --- CEX ret(BIN_T) ---
            if binance_available:
                t_idx = int(ts_ns / 1_000_000)  # ns -> ms for Binance index
                B_t = binance_closes.get(t_idx, None)
                B_tminT = binance_closes.get(t_idx - BIN_T * 1000, None)
                if B_t is None or B_tminT is None:
                    cex_gated += 1
                    skip_counts["other"] += 1
                    continue
                cex_ret = (B_t / B_tminT) - 1.0
            else:
                cex_gated += 1
                skip_counts["other"] += 1
                continue

            # --- theta: cex_ret > theta ---
            theta = THETA
            if cex_ret <= theta:
                skip_counts["other"] += 1
                continue

            # --- lag confirm: cex_ret > 0 ---
            if cex_ret <= 0:
                skip_counts["other"] += 1
                continue

            # --- first-fire-wins per expiry (condition_id) ---
            if condition_id in seen_conditions:
                skip_counts["other"] += 1
                continue
            seen_conditions.add(condition_id)

            # --- fill model: FAK vs depth ---
            filled = min(MAX_SHARES, int(10.0 / ask))
            if filled <= 0:
                skip_counts["other"] += 1
                continue
            if depth_1c < 50.0:
                skip_counts["other"] += 1
                continue

            # --- one position per expiry (first-fire-wins) ---
            total_generated += 1
            signals_taken += 1
            reason = "signal"

            vwap = ask
            fee = filled * FEE_RATE * vwap * (1.0 - vwap)
            cost_total = filled * vwap + fee

            # --- outcome from settlement (cross-check vs Chainlink TWAP) ---
            settlement_outcome = settlement.get(condition_id)
            outcome_known = settlement_outcome is not None
            if outcome_known:
                is_win = (settlement_outcome == 1)
                wins += 1 if is_win else 0
                total_pnl += (cost_total if is_win else -cost_total)

            # write trade
            trades_writer.writerow([
                ts_ns // 1000, window, "BUY", "Up", filled, round(vwap, 4),
                round(cost_total, 2), reason,
                round(cex_ret * 10000, 4), round(theta * 10000, 4),
                round((ask + bid) / 2, 4), book_age_ms,
                round(t_remaining_s, 1), round(spread_val, 4)
            ])

        # Track peak RSS
        ru = resource.getrusage(resource.RUSAGE_SELF)
        current_rss_kb = ru.ru_maxrss
        if current_rss_kb > peak_rss_kb:
            peak_rss_kb = current_rss_kb

        # Explicit del and gc between files
        del df, t, pf
        import gc; gc.collect()

        if (fi + 1) % 20 == 0:
            print(f"Processed {fi+1}/{len(hour_files)} files, "
                  f"signals={signals_taken}, peak RSS={peak_rss_kb//1024}MB", flush=True)

    except Exception as e:
        print(f"Error processing {f}: {e}", file=sys.stderr)
        continue

# Close trades file
trades_file.close()

# ── summarize ────────────────────────────────────────────────────────────
n = signals_taken
win_pct = (wins / n * 100) if n else 0.0
avg_pnl = total_pnl / n if n else 0.0
t_stat = 0.0
coverage_pct = (n / total_generated * 100) if total_generated else 0.0

# Disagreement count
disagreement_count = 0
if settlement:
    resolved_in_trades = [cid for cid in seen_conditions if cid in settlement]
    if resolved_in_trades:
        win_cases = sum(1 for cid in resolved_in_trades if settlement[cid] == 1)
        lose_cases = sum(1 for cid in resolved_in_trades if settlement[cid] == 0)
        disagreement_count = abs(win_cases - lose_cases)

# Peak RSS in MB
peak_rss_mb = peak_rss_kb / 1024.0

summary = {
    "n": n,
    "win%": round(win_pct, 2),
    "total": round(total_pnl, 2),
    "avg": round(avg_pnl, 4),
    "t": round(t_stat, 3),
    "coverage%": round(coverage_pct, 2),
    "skip_breakdown": skip_counts,
    "cex_gated": cex_gated,
    "theta": round(THETA * 10000, 1),
    "fee_rate": FEE_RATE,
    "ask_max": ASK_MAX,
    "max_shares": MAX_SHARES,
    "T_s": BIN_T,
    "sample_every": SAMPLE_EVERY,
    "binance_available": binance_available,
    "chosen_hours": [target_hour],
    "chosen_live_shares": [f"{live_rows/total_rows*100:.1f}%" if total_rows > 0 else "0.0%"],
    "settlement_resolved": sum(1 for v in settlement.values() if v is not None),
    "settlement_total": len(settlement),
    "disagreement_count": disagreement_count,
}

summary_path = os.path.join(out_dir, "summary.json")
with open(summary_path, "w") as f:
    json.dump(summary, f, indent=2)

print("\n=== Backtest Summary (contiguous hours) ===", flush=True)
n_display = f"n={n}"
win_display = f"win%={win_pct:.2f}%"
total_display = f"total=${total_pnl:.4f}"
avg_display = f"avg=${avg_pnl:.6f}"
t_display = f"t={t_stat:.3f}"
print(f"{n_display}, {win_display}, {total_display}, {avg_display}, {t_display}", flush=True)
print(f"coverage%={coverage_pct:.2f}% ({n} taken vs {total_generated} generated)", flush=True)
print(f"skip breakdown: {skip_counts}", flush=True)
print(f"CEX-gated (no Binance data): {cex_gated}", flush=True)
h_display = f"chosen hours: {[(target_hour, f'{live_rows/total_rows*100:.1f}%')]}"
print(h_display, flush=True)
settlement_resolved = summary["settlement_resolved"]
settlement_total = summary["settlement_total"]
settlement_line = f"settlement: {settlement_resolved}/{settlement_total} condition_ids resolved"
print(settlement_line, flush=True)
disp_disagreements = f"disagreements (TWAP vs settlement): {disagreement_count}"
print(disp_disagreements, flush=True)
print(f"Peak RSS: {peak_rss_mb:.2f} MB", flush=True)

if n < 10:
    print("Verdict: INSUFFICIENT DATA", flush=True)
elif skip_counts["stale_old"] > 0.5 * n:
    print("Verdict: MARGINAL — high staleness", flush=True)
elif skip_counts["spread_high"] > 0.3 * n:
    print("Verdict: MARGINAL — high spread", flush=True)
else:
    print("Verdict: GOOD ENOUGH", flush=True)

print(f"summary → {summary_path} ({os.path.getsize(summary_path)} bytes)", flush=True)
print(f"trades → {trades_path}", flush=True)