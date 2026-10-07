# S1 CEX Lead-Lag Backtest Spec — Binance Spot → Polymarket BTC 5m Up YES

Repo: polymarket-data-collector · branch `perfect/pmdata-parity` @ `84836b7` · SPEC ONLY (no code, no pulls, no run).
Thesis (external input, cf. STRATEGY.md:333/:10 — not in this repo, contents not verified here): Binance spot
leads Poly BTC 5m Up YES by 1–3 s. ASSUMPTIONS (unverified): 56–58% win @ 0.50–0.55 entry, 5–15 trades/day,
taker cost ≈ shares·0.07·p·(1−p) (~3.5¢ round-trip @ 0.50). Real data only (AGENTS.md): observed depth fills,
no interpolation, gaps stay gaps.

## 0. Feasibility gate FIRST (before any PnL)
Backtest read path (repo standard): `book_snapshots_clean` (= `book_state='live'` ONLY) + both-sides-quoted
(`up_bid/ask, down_bid/ask` all NOT NULL — clean is live+thin, ~17.8% null-BBO per audit E3) +
`up/down_book_age_ms <= 1500` (post-E6 fix; pre-2026-09-09 rows carry dead `0` — exclude or flag vintage) +
late-window (min 3–4 one-sided books: 0% min 0–2, ~11% min 3, ~83% min 4) + live-only (~15% current live share).
This STARVES the sample. Gate G0: count tradeable windows/ticks after ALL gates §6 BEFORE computing PnL.
If tradeable windows < 100 or quoted-live ticks < 20k → verdict = INSUFFICIENT DATA (not a strategy verdict).
Report: raw windows → settled-official → gate-passed, with attrition at each step.

## 1. Objective
Test: does `cex_ret(T) > theta` predict Up-YES resolution edge after fees/fills? Output: PASS/FAIL per §9,
plus sensitivity grid §8 answering user's three opens (theta-vs-T, Up/Down symmetry, fill-vs-depth).

## 2. Data pulls
### 2a. Binance (lead source; fresh pull at run time, not stored in repo)
- Endpoint: `GET https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s&startTime=<ms>&endTime=<ms>&limit=1000`
  (pin in run log: exact URL, params, response `openTime/close`, weight used). Spot klines, 1 s interval.
- Rate limits (document actuals at pull): klines weight 1–10/req by limit; ≤6000 wt/min IP. Paginate 1000/request,
  sleep on 429/418, store raw JSON + pull manifest (pull_ts, weight, retry count). One pull per backtest span.
- Fallback/cross-check ONLY: our `chainlink_events` RTDS BTC (~1 s median, `price`, `ts_source` ms, `ts_received_ns`;
  dedup on `(asset,event_id)` — E9 dup burst known; NULL `report_id` expected). Use for gap cross-check and
  settlement-boundary sanity, never as the lead signal when Binance present. Record Binance-gap seconds separately.
### 2b. Ours (read-only hive paths; columns from `storage/schemas.py`)
- `data/book_snapshots_clean/date=*/asset=BTC/*.parquet` (842 cols, 10 L2 levels): `ts_snapshot_ns` (500 ms grid,
  `%500M==0`), `condition_id`, `up_bid/ask(+_size)`, `down_bid/ask(+_size)`, `up/down_{bid,ask}_level_{1..10}_{price,size}`,
  `up/down_{bid,ask}_depth_{1,5,10}c` (3.3.0+: full 100-level window sums — NOT comparable pre-3.3.0 top-10 sums),
  `book_state`, `up/down_book_age_ms`, `market_time_remaining_ms`, `up/down_book_hash`, `underlying_price/ts/age_ms`.
- `data/book_events/` (`event_id`, `ts_source` ms, `ts_received_ns`, `token_id/outcome` NULL=honest gap, `side`,
  `old/new_best_*`): signal-refresh evidence only, not fills.
- `data/trades/` (`trade_id`, `price/size/notional`, `fee`, `fee_is_estimated`, `side/aggressor_side` lowercased,
  `quote_book_state`, quote-context BBO): microstructure context only — backtest fills are synthetic FAK vs snapshot
  depth (§4), never other traders' prints.
- `markets_log`/`markets_latest` (`condition_id`, `market_start/end_ts_ms` (end−start=300000), `window_index`,
  `up/down_token_id`, `status`, `resolution_outcome`, `settlement_price`, `settlement_source`,
  `settlement_ts_utc`, `resolution_confirmed_at`, `tick_size`, `minimum_order_size`): expiry/strike/settlement joins.
- `collector_events`/`resync_episodes`: attribution for every excluded window (T4 closure).

## 3. Clock alignment (all UTC)
Ms epoch canonical. Poly grid: `ts_snapshot_ns` 500 ms. Binance: 1 s klines keyed by `openTime`; decision tick `t`
may use only info with event time ≤ `t` (kline fully closed, i.e. `closeTime ≤ t`; snapshot `ts ≤ t`). Chainlink
`ts_source` likewise ≤ `t`. Max skew budget: Binance→Poly join tolerance 1500 ms; `underlying_age_ms` default
tolerance 2000 ms (stored cols). Any signal tick with no closed kline or `book_age>1500` ms = no-trade (counted
as gated-out, §6). Sort Poly events by `ts_received_ns` (api-reconciled rows: `ts_backfilled_ns`); Chainlink joins
by `ts_source`.

## 4. Signal definition (per decision tick `t`, Up-YES side; mirror for Down in symmetry leg §8)
- `cex_ret(T) = (B(t)/B(t−T)) − 1`, `B` = Binance close; grid T ∈ {10,15,20} s (baseline 15 s).
- `poly_mid(t) = (up_ask(t)+up_bid(t))/2` (both quoted required); confirm: `poly_mid(t) − poly_mid(t−T) < k·cex_ret(T)`,
  `k=0.05` (lag confirmation — Poly hasn't moved).
- FIRE long Up-YES iff `cex_ret(T) > theta` AND lag-confirm true; theta grid {0.02%,0.04%,0.06%} (baseline 0.04%).
  One evaluation per snapshot tick (2 Hz); first-fire-wins per expiry (§6).
- Direction filter: BTC only; venue = Binance spot BTCUSDT (per thesis; ETH/SOL lanes out of scope v1).

## 5. Fill model — FAK vs observed depth, no lookahead
Fill tick = signal tick `t`; inputs restricted to snapshot row at `t` ONLY. Buy Up-YES at `ask_t`:
`fill_shares = min(order_shares, ask_size_t + Σ level sizes within +1¢ of ask_t)` from stored L2
(`up_ask_level_{1..10}`); remainder unfilled → signal counts as MISSED (fill_rate denominator), never resized or
chased. No market-impact beyond depth walk (record slippage if walk >1 level). Order size v1: fixed $10 notional
(`shares = 10/ask_t`); respect `minimum_order_size` (skip if shares < min → gated-out). Partial fills credited
pro-rata; fees pro-rata. Never interpolate missing levels/ticks; NULL side = unfillable.

## 6. Gates / filters (all must pass at `t`; else gated-out, counted by reason)
G0 feasibility (§0). G1 live-only + both-sides-quoted + `book_age≤1500ms`. G2 `30s ≤ t_remaining ≤ 120s`
(`market_time_remaining_ms`; ASSUMPTION t≥25 s floor folded in — use 30 s). G3 `ask_t ≤ 0.85`. G4 spread
`(ask−bid) ≤ 0.04`. G5 depth: `up_ask_depth_1c ≥ $50` (sensitivity §8 varies $25/$50/$100). G6 one position per
expiry (first-fire-wins; ignore later ticks). G7 blacklist-after-loss: skip next N=1 expiry after a losing trade
(report with/without). G8 volatility filter: skip expiry if `|cex_ret(60s)| > 0.30%` at first-fire (chop guard;
sensitivity on/off). G9 expiry settled-official only (§7).

## 7. Fees (dual regime — load-bearing) + settlement join
Fees: 5 m markets report `fee_rate_bps=0`; stored `fee=0.0` is REAL with `fee_is_estimated=NULL`
(DATA_CARD.md:25; audit E7). NEVER apply 0.07 fallback silently. Report TWO PnL columns per trade:
(A) as-stored (0 fees); (B) sensitivity `cost = shares·0.0007·p·(1−p)`-style taker drag (user assumption, state as
ASSUMPTION). Net expectancy/trade and PASS thresholds evaluated on BOTH; divergence stated explicitly (if fee=0
the hurdle collapses). Settlement: tradeable expiries = `status=resolved` AND `settlement_source='polymarket_official'`
(from CLOB `tokens[].winner`, via `resolution_backfill` cron; note: PERFECT_DATA_SPEC.md still names
`on_chain_confirmed` — code reality is `polymarket_official`; match code). `inferred_nearest` rows EXCLUDED from
PnL (5 m null ~2.7% per audit:66); report official-coverage separately. Payout: Up win → $1.00/share else $0.
Hold to resolution (no exit leg v1).

## 8. Metrics + sensitivity (answer the three opens with rows, not prose)
Metrics per (theta,T) cell, both fee regimes: n_trades, win_rate, gross/net expectancy per trade ($ and ¢/share),
Sharpe (per-trade), trades/day, fill_rate (= filled/fires), gated-out breakdown by G-reason, MFE/MAE + adverse-
excursion distribution (the 30–40% snap-back fear MUST be measured: excursion of `poly_mid` against position from
`t` to expiry, in ¢; report P(excursion>10¢), median time-to-snap). Sensitivity grid: theta {0.02,0.04,0.06}% × T
{10,15,20}s (=9 cells, baseline 0.04/15s); symmetry: mirror Down-YES leg on same grid (report Up−Down delta);
fill-vs-depth: depth floor {$25,$50,$100} × order size {$5,$10,$20} fill_rate table. Each open → one table; no
narrative verdict without its table.

## 9. PASS / FAIL + NO-GO
PASS (all, fee regime B unless noted): net expectancy/trade > +$0.30 @ $10 size; win_rate > 53% with n≥100 filled;
fill_rate ≥ 50%; trades/day ≥ 3 sustained; MAE distribution bounded (P(excursion>15¢) < 20%); Down-mirror not
sign-reversed (rules out BTC-drift artifact). FAIL: any PASS line missed on baseline cell after full grid.
NO-GO (invalidate, regardless of PnL): G0 insufficient; official-settlement coverage < 90% of fires;
book_age-gated-out > 50% (staleness regime); fill_rate < 30% at any depth floor (untradeable); Binance gaps > 5%
of span; any interpolated/carried-forward price detected in inputs. Verdict vocabulary: PASS / FAIL / INSUFFICIENT
DATA only.

## 10. Variants skeleton (v1 baseline only; v2+ one-liners, no detail)
v1 = this spec. v2: dynamic theta = rolling Binance vol quantile. v3: maker/limit entry at bid+tick w/ queue model.
v4: early-exit at t_remaining<10s if `poly_mid` adverse >8¢. v5: cross-asset (ETH) same template. v6: Chainlink-only
signal when Binance gapped.
