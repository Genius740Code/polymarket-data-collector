"""S1 CEX Lead-Lag Backtest — BTC 5m Up YES. POST-FIX RERUN 2026-10-10/11.

Streaming footer-bounded per-(file,row-group) parquet reads with del/gc between
units, column-subset only; the only in-memory table is the union of the three
chosen hours' masked rows (~45k rows x 10 cols, a few MB), globally ts-sorted
once so first-fire-wins fires in true time order (the 2026-10-10 compacted
partition's row groups are not ts-ordered).

Chosen hours = top-3 FULL UTC hours by live-row share from a footer-bounded
live-share scan of the two post-fix BTC partitions:
  2026-10-10 06:00 (61.21%), 2026-10-10 12:00 (68.61%), 2026-10-11 05:00 (73.95%)

Adaptations vs the 2026-10-09 run, all reporting/date-scope level:
  - DATES/hour selection moved to the post-fix partitions (above).
  - Binance 1s closes fetched for every chosen hour in full batches with
    per-batch retries (the old 3-request global cap could not cover one hour).
  - condition_id kept as its real hex string (it was previously coerced to NaN,
    collapsing every expiry into one key, capping coverage at 1/generated and
    making the settlement join impossible).
  - settlement read per file with column subsets (dataset() cast fails on the
    2026-10-10 partition's null-typed reported_volume column).
  - real Chainlink cross-check of settlement outcomes (end>start => Up) with a
    disagreement count, and a real t-stat over per-trade PnL.
Trading gates, fills, fees, PnL accounting and the coverage invariant are
unchanged from the run that scored 0.17% on 2026-10-09 11:00.

S1 PARAMETERS LOCKED (assert equality with docs/S1_CEX_LEADLAG_SPEC.md):
  THETA=0.0002, BIN_T=30, HOR=120, FILL_DELAY=2, SHARES=100,
  MAX_ASK=0.90, FEE_R=0.07. No deviations — assert equality in comment block below.
"""
import bisect
import csv
import gc
import glob as _glob
import json
import math
import os
import resource
import sys
import time
import urllib.request
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq

# ── config ──────────────────────────────────────────────────────────────
BASE = "/home/fese/polymarket-data-collector/data"
DATES = ["2026-10-10", "2026-10-11"]  # post-fix partitions
ASSET = "BTC"
# Top-3 full UTC hours by live-row share (footer-bounded scan of both dates).
CHOSEN_HOURS = ["2026-10-10 06:00", "2026-10-10 12:00", "2026-10-11 05:00"]
SCAN_LIVE_SHARES = {"2026-10-10 06:00": 61.21, "2026-10-10 12:00": 68.61,
                    "2026-10-11 05:00": 73.95}

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

HOUR_NS = 3_600_000_000_000

# ── Step 1: hour windows + footer-bounded unit discovery ────────────────
hour_bounds = []
for key in CHOSEN_HOURS:
    dt = datetime.strptime(key, "%Y-%m-%d %H:00").replace(tzinfo=timezone.utc)
    start_ns = int(dt.timestamp()) * 1_000_000_000
    hour_bounds.append((key, start_ns, start_ns + HOUR_NS))

units = []  # (file, row_group_idx, rg_min_ts)
for d in DATES:
    pattern = f"{BASE}/book_snapshots_500ms/date={d}/asset={ASSET}/*.parquet"
    for f in sorted(_glob.glob(pattern)):
        try:
            pf = pq.ParquetFile(f)
            md = pf.metadata
            ts_idx = pf.schema_arrow.get_field_index("ts_snapshot_ns")
            for rg in range(md.num_row_groups):
                try:
                    st = md.row_group(rg).column(ts_idx).statistics
                    lo, hi = int(st.min), int(st.max)
                except Exception:
                    continue
                if any(lo < e and hi >= s for _, s, e in hour_bounds):
                    units.append((f, rg, lo))
            pf.close()
        except Exception as e:
            print(f"Footer scan failed for {f}: {e}", file=sys.stderr)
units.sort(key=lambda u: (u[2], u[0]))
print(f"Units (file,row-group) intersecting chosen hours: {len(units)}", flush=True)

# ── Step 2: Output dir and trades file ──────────────────────────────────
out_dir = "/home/fese/polymarket-data-collector/paperlog/btc5m_s1"
os.makedirs(out_dir, exist_ok=True)
trades_path = os.path.join(out_dir, "trades.csv")
with open(trades_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["ts", "window", "side", "shares", "vwap", "fee", "cash",
                     "reason", "cex_ret", "theta", "poly_mid", "book_age",
                     "t_remaining", "spread"])

# ── Step 3: Binance CEX fetch (full coverage per chosen hour) ───────────
binance_closes = {}
max_retries = 3
for key, s_ns, e_ns in hour_bounds:
    binance_start = s_ns // 1_000_000 - 30_000
    binance_end = e_ns // 1_000_000 + 30_000
    t_ms = binance_start
    while t_ms < binance_end:
        batch_end = min(t_ms + 1000 * 500, binance_end)
        params = f"?symbol=BTCUSDT&interval=1s&startTime={t_ms}&endTime={batch_end}&limit=500"
        got = False
        for attempt in range(1, max_retries + 1):
            try:
                r = urllib.request.urlopen(BINANCE_URL + params, timeout=5)
                data = json.loads(r.read())
                for k in data:
                    binance_closes[int(k[0])] = float(k[4])
                got = True
                break
            except Exception as e:
                print(f"Binance batch {key} failed (attempt {attempt}): {e}",
                      file=sys.stderr)
                time.sleep(0.5)
        if not got:
            print(f"Binance batch abandoned: {key} {t_ms}-{batch_end}",
                  file=sys.stderr)
        t_ms = batch_end
        time.sleep(0.25)

print(f"Total Binance closes collected: {len(binance_closes)}", flush=True)
binance_available = len(binance_closes) > 0
if not binance_available:
    print("Binance API unreachable after retries — will CEX-gate all signals honestly",
          file=sys.stderr)

# ── Step 4: Settlement lookup + window bounds (per-file column reads) ────
settlement = {}
win_bounds = {}
for d in DATES:
    for f in sorted(_glob.glob(f"{BASE}/markets_log/date={d}/*.parquet")):
        try:
            pf = pq.ParquetFile(f)
            names = pf.schema_arrow.names
            cols = [c for c in ["condition_id", "resolution_outcome",
                                "market_start_ts_ms", "market_end_ts_ms"]
                    if c in names]
            df_sett = pf.read(columns=cols).to_pandas()
        except Exception:
            continue
        for _, row in df_sett.iterrows():
            cid = str(row["condition_id"])
            if "market_start_ts_ms" in cols and "market_end_ts_ms" in cols:
                try:
                    win_bounds[cid] = (int(row["market_start_ts_ms"]),
                                       int(row["market_end_ts_ms"]))
                except (ValueError, TypeError):
                    pass
            o = row.get("resolution_outcome")
            if o is None:
                continue
            if o == "up":
                settlement[cid] = 1
            elif o == "down":
                settlement[cid] = 0
print(f"Settlement mapping: {len(settlement)} condition_ids", flush=True)

# ── Step 4b: Chainlink RTDS ticks for the cross-check ────────────────────
cl_price = {}
for d in DATES:
    for f in sorted(_glob.glob(f"{BASE}/chainlink_events/date={d}/asset={ASSET}/*.parquet")):
        try:
            df_cl = pq.ParquetFile(f).read(columns=["ts_source", "price"]).to_pandas()
        except Exception:
            continue
        for ts, px in zip(df_cl["ts_source"].values, df_cl["price"].values):
            cl_price[int(ts)] = float(px)
cl_ts = sorted(cl_price)
CL_TOL_MS = 30_000


def _cl_nearest(ms):
    i = bisect.bisect_left(cl_ts, ms)
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(cl_ts):
            if best is None or abs(cl_ts[j] - ms) < abs(cl_ts[best] - ms):
                best = j
    if best is None or abs(cl_ts[best] - ms) > CL_TOL_MS:
        return None
    return cl_price[cl_ts[best]]


print(f"Chainlink ticks loaded: {len(cl_ts)}", flush=True)

# ── Step 5: Stream units, hour-mask rows, then one ts-sorted pass ───────
peak_rss_kb = 0
sel_frames = []
for ui, (f, rg, _lo) in enumerate(units):
    try:
        pf = pq.ParquetFile(f)
        df = pf.read_row_group(rg, columns=NEEDED_COLS).to_pandas()
        pf.close()
        ts_ns = df["ts_snapshot_ns"].astype("int64")
        hour_mask = False
        for _key, s, e in hour_bounds:
            hour_mask = hour_mask | ((ts_ns >= s) & (ts_ns < e))
        if bool(hour_mask.any()):
            sel_frames.append(df.loc[hour_mask.values].copy())
        del df
        gc.collect()
        ru = resource.getrusage(resource.RUSAGE_SELF)
        peak_rss_kb = max(peak_rss_kb, ru.ru_maxrss)
        if (ui + 1) % 20 == 0:
            print(f"Scanned {ui+1}/{len(units)} units, masked_rows="
                  f"{sum(len(x) for x in sel_frames)}, "
                  f"peak RSS={peak_rss_kb//1024}MB", flush=True)
    except Exception as e:
        print(f"Error processing {f} rg{rg}: {e}", file=sys.stderr)
        continue

sel = pd.concat(sel_frames, ignore_index=True)
del sel_frames
gc.collect()
sel.sort_values("ts_snapshot_ns", kind="mergesort", inplace=True)
sel.reset_index(drop=True, inplace=True)
ru = resource.getrusage(resource.RUSAGE_SELF)
peak_rss_kb = max(peak_rss_kb, ru.ru_maxrss)

# Per-hour row / live accounting (all rows inside the chosen hours)
hour_id = (sel["ts_snapshot_ns"].astype("int64") // HOUR_NS).astype("int64")
hour_rows = {}
hour_live = {}
for key, s, _e in hour_bounds:
    m = hour_id == (s // HOUR_NS)
    hour_rows[key] = int(m.sum())
    hour_live[key] = int((sel.loc[m.values, "book_state"].astype(str) == "live").sum())

# Sequential state
blacklist_until = None
post_resolution_until = 0
seen_conditions = set()
traded_cids = []

# Accumulators
total_rows = int(len(sel))
live_rows = sum(hour_live.values())
generated = 0          # rows entering the sequential loop (after static filters)
signals_taken = 0
wins = 0
total_pnl = 0.0
cex_gated = 0
pnl_list = []
skip_counts = {"stale_old": 0, "spread_high": 0, "other": 0, "post_resolution": 0, "null_quote": 0}
hour_taken = {key: 0 for key, _s, _e in hour_bounds}

# Static filters, vectorized (same predicates as the original run)
live_mask = sel["book_state"].astype(str) == "live"
g1_mask = live_mask & (sel["up_book_age_ms"] <= 1500)
stale_book = sel["book_state"].astype(str) == "stale"
stale_age = sel["up_book_age_ms"] > STALE_AGE_S * 1000
stale_skip = stale_book & stale_age
ask_col = sel["up_ask"].astype(float)
bid_col = sel["up_bid"].astype(float)
spread = ask_col - bid_col
spread_skip = spread > SPREAD_MAX
t_rem_s = sel["market_time_remaining_ms"].astype(float) / 1000.0
t_rem_mask = (t_rem_s >= 30) & (t_rem_s <= 120)
ask_max_mask = ask_col <= ASK_MAX
ask_pos_mask = ask_col > 0
depth_mask = sel["up_ask_depth_1c"].astype(float) >= 50.0
static_pass = (g1_mask & ~stale_skip & ~spread_skip &
               t_rem_mask & ask_max_mask & ask_pos_mask & depth_mask)

generated = int(static_pass.sum())
pass_indices = sel.index[static_pass.values]

trades_file = open(trades_path, "a", newline="")
trades_writer = csv.writer(trades_file)

for idx in pass_indices:
    row = sel.loc[idx]
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
        # Round to nearest second since Binance 1s closes are at integer-second boundaries
        t_idx_rounded = (t_idx // 1000) * 1000
        B_t = binance_closes.get(t_idx_rounded, None)
        B_tminT = binance_closes.get(t_idx_rounded - BIN_T * 1000, None)
        if B_t is None or B_tminT is None:
            cex_gated += 1
            continue
        cex_ret = (B_t / B_tminT) - 1.0
    else:
        cex_gated += 1
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
    signals_taken += 1
    traded_cids.append(condition_id)
    h = ts_ns // HOUR_NS
    for key, s, _e in hour_bounds:
        if h == s // HOUR_NS:
            hour_taken[key] += 1
    reason = "signal"

    vwap = ask
    fee = filled * FEE_RATE * vwap * (1.0 - vwap)
    cost_total = filled * vwap + fee

    # --- outcome from settlement (cross-check vs Chainlink below) ---
    settlement_outcome = settlement.get(condition_id)
    outcome_known = settlement_outcome is not None
    if outcome_known:
        is_win = (settlement_outcome == 1)
        wins += 1 if is_win else 0
        trade_pnl = cost_total if is_win else -cost_total
        total_pnl += trade_pnl
        pnl_list.append(trade_pnl)

    # write trade
    trades_writer.writerow([
        ts_ns // 1000, window, "BUY", "Up", filled, round(vwap, 4),
        round(cost_total, 2), reason,
        round(cex_ret * 10000, 4), round(theta * 10000, 4),
        round((ask + bid) / 2, 4), book_age_ms,
        round(t_remaining_s, 1), round(spread_val, 4)
    ])

trades_file.close()

# ── summarize ────────────────────────────────────────────────────────────
n = signals_taken
win_pct = (wins / n * 100) if n else 0.0
avg_pnl = total_pnl / n if n else 0.0
t_stat = 0.0
if len(pnl_list) >= 2:
    mean_p = sum(pnl_list) / len(pnl_list)
    var_p = sum((p - mean_p) ** 2 for p in pnl_list) / (len(pnl_list) - 1)
    if var_p > 0:
        t_stat = mean_p / ((var_p ** 0.5) / (len(pnl_list) ** 0.5))
coverage_pct = (n / generated * 100) if generated else 0.0

# invariant: taken + skips + gated must equal generated exactly
inv_total = signals_taken + sum(skip_counts.values()) + cex_gated
assert inv_total == generated, (
    f"INVARIANT MISMATCH: taken={signals_taken}, "
    f"skips={sum(skip_counts.values())}, gated={cex_gated}, "
    f"generated={generated}")

# Chainlink cross-check of settlement for every taken expiry
disagreement_count = 0
chainlink_cross_checked = 0
chainlink_unchecked = 0
for cid in traded_cids:
    so = settlement.get(cid)
    wb = win_bounds.get(cid)
    if so is None or wb is None:
        chainlink_unchecked += 1
        continue
    p_start = _cl_nearest(wb[0])
    p_end = _cl_nearest(wb[1])
    if p_start is None or p_end is None:
        chainlink_unchecked += 1
        continue
    chainlink_cross_checked += 1
    cl_outcome = 1 if p_end > p_start else 0
    if cl_outcome != so:
        disagreement_count += 1

peak_rss_mb = peak_rss_kb / 1024.0

summary = {
    "n": n,
    "win%": round(win_pct, 2),
    "total": round(total_pnl, 2),
    "avg": round(avg_pnl, 4),
    "t": round(t_stat, 3),
    "coverage%": round(coverage_pct, 2),
    "generated": generated,
    "skip_breakdown": skip_counts,
    "cex_gated": cex_gated,
    "theta": round(THETA * 10000, 1),
    "fee_rate": FEE_RATE,
    "ask_max": ASK_MAX,
    "max_shares": MAX_SHARES,
    "T_s": BIN_T,
    "sample_every": SAMPLE_EVERY,
    "binance_available": binance_available,
    "binance_closes": len(binance_closes),
    "pnl_accounting": "unchanged baseline signed-cost (win:+cost_total, loss:-cost_total)",
    "dates": DATES,
    "chosen_hours": CHOSEN_HOURS,
    "chosen_live_shares": {k: round(hour_live[k] / hour_rows[k] * 100, 2)
                           if hour_rows[k] else 0.0 for k in CHOSEN_HOURS},
    "scan_live_shares": SCAN_LIVE_SHARES,
    "hour_rows": hour_rows,
    "hour_taken": hour_taken,
    "settlement_resolved": sum(1 for v in settlement.values() if v is not None),
    "settlement_total": len(settlement),
    "trades_settled": sum(1 for c in traded_cids if settlement.get(c) is not None),
    "chainlink_cross_checked": chainlink_cross_checked,
    "chainlink_unchecked": chainlink_unchecked,
    "disagreement_count": disagreement_count,
    "peak_rss_mb": round(peak_rss_mb, 2),
}

summary_path = os.path.join(out_dir, "summary.json")
with open(summary_path, "w") as f:
    json.dump(summary, f, indent=2)

print("\n=== Backtest Summary (post-fix hours) ===", flush=True)
print(f"n={n}, win%={win_pct:.2f}%, total=${total_pnl:.4f}, "
      f"avg=${avg_pnl:.6f}, t={t_stat:.3f}", flush=True)
print(f"coverage%={coverage_pct:.2f}% ({n} taken vs {generated} generated)", flush=True)
print(f"skip breakdown: {skip_counts}", flush=True)
print(f"CEX-gated (no Binance data): {cex_gated}", flush=True)
for key in CHOSEN_HOURS:
    share = hour_live[key] / hour_rows[key] * 100 if hour_rows[key] else 0.0
    print(f"hour {key}: rows={hour_rows[key]}, live={share:.2f}%, taken={hour_taken[key]}",
          flush=True)
settlement_line = (f"settlement: {summary['settlement_resolved']}/{summary['settlement_total']} "
                   f"condition_ids resolved; trades settled: {summary['trades_settled']}/{n}")
print(settlement_line, flush=True)
print(f"chainlink cross-check: checked={chainlink_cross_checked}, "
      f"unchecked={chainlink_unchecked}, disagreements={disagreement_count}", flush=True)
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
