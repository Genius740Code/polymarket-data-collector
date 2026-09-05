# Perfect Data Specification — 99.9% Accuracy Contract for 5-Minute Market Data

**Scope:** all Parquet tables written by `polymarket_collector` for 5-minute Up/Down markets
(assets per `config/collector.yaml`), covering `book_snapshots_500ms`, `book_snapshots_clean`,
`markets_log` / `markets_latest`, `book_events`, `trades`, `chainlink_events`,
`resync_episodes`, `collector_events`.

**Purpose:** define, in one place, what a *perfect* dataset looks like — no unexplained nulls,
no missing intervals, no duplicate/corrupt rows, no internal contradictions — and the exact
thresholds a dataset must meet to be accepted as 99.9%-quality. Column names are the real ones
from `src/polymarket_collector/storage/schemas.py`.

**Companion documents:** `TEST_PLAN_99pct.md` (how to test), `data_quality_report.md` (past runs),
`completeness.py` (day-level measurement), `validation.py` (row-level sanity bounds).

---

## 1. The four quality tiers

Not everything can be held to the same bar. Every rule below belongs to exactly one tier:

| Tier | Name | Bar | Meaning |
|---|---|---|---|
| **T1** | Hard invariants | **100.000%** — zero violations tolerated | Schema conformance, uniqueness, referential integrity, ordering, format. One violation = dataset rejected. These are fully under collector control. |
| **T2** | Completeness | **≥ 99.9%** | Every 500ms tick, every existing market, every configured asset, every settled market, every rollover captured. |
| **T3** | Bounded quality | **≤ 0.1%** affected | Stale rows, crossed books, empty book sides, exchange-side latency, missing chainlink ticks. Things the exchange can degrade but we must bound and record. |
| **T4** | Explained exceptions | **100% attributed** | Anything that *does* go wrong (disconnect, resync, rollover miss, sequence gap) must appear in `resync_episodes` / `collector_events`. Unexplained is the only unacceptable state. |

> **The golden rule (T4):** silence is the only real failure. A gap that is recorded with a
> `resync_id` and a `resync_episodes` row is acceptable; the same gap with no episode row is a
> T1 violation of attribution.

---

## 2. Global invariants (T1 — every table)

1. **Schema conformance** — every row matches its schema in `schemas.py`: no missing columns,
   no extra columns, correct types, `non-nullable` schema fields never null. No `NaN`/`inf`
   anywhere (Parquet float NaN ≠ null — a NaN is a glitch, not an empty value).
2. **Uniqueness**
   - `book_snapshots_500ms`: `(condition_id, ts_snapshot_ns)` unique; `snapshot_id` unique.
   - `book_events`: `event_id` unique; `trades`: `trade_id` unique; `chainlink_events`: `event_id` unique.
   - `markets_log`: one final state per `(condition_id, updated_at)`; `markets_latest` has exactly
     one row per `condition_id`.
3. **Referential integrity** — every `condition_id` appearing in any table exists in `markets_log`;
   `up_token_id`/`down_token_id`/`token_id` values match the market's pair; `market_id`, `series_id`,
   `asset`, `window_index` are identical across all rows of the same window. `resync_id` on a
   snapshot must exist in `resync_episodes`.
4. **Window math is exact** — for every row: `market_end_ts_ms − market_start_ts_ms = 300000`;
   `window_index = market_start_ts_ms // 1000 // 300`; slug = `{asset_lower}-updown-5m-{start_unix}`;
   `window_label = "5m"`; `window_size_seconds = 300`.
5. **Timestamps**
   - `ts_snapshot_ns % 500_000_000 == 0` (exact 500ms grid, `book.py:snapshot_bucket_ms`).
   - `ts_snapshot_utc` is the ISO rendering of `ts_snapshot_ns` — never contradictory.
   - `ts_received_ns ≥` source timestamp (exchange time) on every row where both exist; a negative
     latency is a clock glitch.
   - Monotonic ordering within each `(condition_id)` by snapshot time; within each token by
     `sequence_number`.
6. **Partition integrity** — a row's hive partition `date=YYYY-MM-DD` equals the UTC date of its
   timestamp; `asset=…` partition equals the row's `asset` column. No row in the wrong partition.
7. **Enum conformance** — `status` ∈ {pending, active, closed, resolved}; `resolution_outcome` ∈
   {up, down, tie, voided, disputed, unknown}; `book_state` ∈ {live, stale, resyncing};
   `settlement_source` ∈ {on_chain_confirmed, inferred_nearest}; `event_type` values ∈ `enums.py`.
8. **Format conformance** — `condition_id`, `settlement_report_id`, `settlement_tx_hash`,
   `transaction_hash` are `0x` + 64 hex; wallets are `0x` + 40 hex; token IDs are decimal-integer
   strings; ISO timestamps are UTC (`Z` suffix).

---

## 3. `book_snapshots_500ms` and `book_snapshots_clean` — the core contract

### 3.1 Volume (T2)

- **Per existing market:** exactly **600** grid ticks (300s × 2/s), spanning
  `market_start_ts_ms` (inclusive) to `market_end_ts_ms − 500` (inclusive).
- **Per asset per UTC day:** **172,800** ticks total across its markets (288 windows/day × 600),
  matching `completeness.py` `expected_snapshots`.
- **Missing ticks:** ≤ **0.1%** (≤ 173/day/asset; ≤ 1 tick per market on average). Every missing
  tick must be covered by a `resync_episodes` window or an explicit `collector_events` gap record —
  unexplained missing ticks are a T1 attribution violation, not merely a T2 shortfall.
- **Clean view:** `book_snapshots_clean` = exactly the rows with `book_state='live'`; row-for-row
  derived, no independent edits.

### 3.2 Null policy (the part that must never surprise you)

| Column class | Perfect-data rule | Bar |
|---|---|---|
| Identity + time + state (`ts_snapshot_*`, `condition_id`, `market_id`, `series_id`, `window_index`, `asset`, `snapshot_id`, token ids, `market_time_remaining_ms`, `is_rollover_window`, `book_state`, `book_crossed`) | **Never null** | T1 (schema non-nullable) |
| Top-of-book `up_bid/up_ask/down_bid/down_ask` (+ `_size`) | Null **only** when that book side is genuinely empty at that instant | ≤ **0.1%** of clean rows have any BBO field null |
| L2 level columns `*_level_N_price/size` | `null` only as **tail padding** beyond real depth; never a hole between populated levels | structural — holes are T1 violations |
| Depth aggregates `*_depth_1c/5c/10c` | Null **iff** that side's best price is null; otherwise a finite number (may be 0.0 when a best exists but no level sits within the window) | T1 consistency with BBO |
| `resync_id` | Null on every `book_state='live'` row; non-null exactly on `stale`/`resyncing` rows belonging to an episode | T1 |

### 3.3 Value and consistency rules

- **Bounds (T1):** all prices ∈ [0, 1]; all sizes ≥ 0 (`validation.py`); tick-aligned to the
  market's `tick_size` (from `markets_log`; typically 0.01).
- **Book sanity (T1):** `up_bid ≤ up_ask`, `down_bid ≤ down_ask` on every row.
- **Complementarity (T3 ≤ 0.1%):** `|up_ask + down_bid − 1| ≤ 2 ticks` and
  `|up_bid + down_ask − 1| ≤ 2 ticks` on clean rows (two tokens of the same binary market).
- **No crossed books:** `book_crossed = true` on ≤ **0.01%** of raw rows, and **0** rows in the
  clean view.
- **Staleness (T3):** `book_state='stale'` on ≤ 0.1% of raw rows; `up_book_age_ms`/`down_book_age_ms`
  ≤ 1500 on ≥ 99.9% of clean rows.
- **Remaining time is derived, not free (T1):** `market_time_remaining_ms` must equal
  `market_end_ts_ms − ts_snapshot_ns/1e6` exactly, and ∈ [0, 300000].
- **Rollover flag is derived (T1):** `is_rollover_window = true` iff
  `market_time_remaining_ms ≤ 30000` (rollover lead) and the next window's market was discovered —
  expect ~60 true ticks per market.
- **Depth recomputability (T1):** every `*_depth_Nc` must equal the value recomputed from the
  stored L2 levels via `book.depth_within` (cumulative size within N¢ of **that side's own best**),
  tolerance 1e-9. A mismatch means the snapshot and its levels disagree — a glitch by definition.
- **Duplicates:** 0 (see §2).

---

## 4. `markets_log` / `markets_latest`

- **Coverage (T2 ≥ 99.9%):** for every 5-min window in which Polymarket actually listed a market
  for a configured asset, exactly one market row reaching terminal state `resolved` (or `closed`
  if resolution genuinely never lands — then a `resolution_stuck` event must exist). Windows
  Polymarket did not list must be evidenced by a `coverage_gap` event — absence of both is a fail.
- **Lifecycle (T1):** the event-sourced log contains the transition chain
  `pending → active → closed → resolved` with non-decreasing `updated_at`; `markets_latest`
  holds exactly the final state per `condition_id`.
- **Resolution fields (T1 once `status='resolved'`):** `resolution_outcome`, `settlement_price`,
  `settlement_ts_utc`, `settlement_report_id`, `settlement_source`, `resolution_confirmed_at` all
  **non-null**. `settlement_source='on_chain_confirmed'` on ≥ 99.9% of markets
  (`inferred_nearest` only when on-chain is unreachable, and flagged).
- **Resolution latency (T3):** `resolution_confirmed_at − market_end_ts ≤ 120s`
  (`max_resolution_wait_seconds`).
- **Outcome consistency (T1):** `resolution_outcome` agrees with the Chainlink record:
  end price > open price → `up`; < → `down`; equal → `tie` (per the market's `resolution_rule`).
- **Economics (T3):** `reported_volume ≥ 0`, `reported_liquidity ≥ 0`; `fee_information`,
  `minimum_order_size`, `minimum_notional` non-null for ≥ 99.9% of markets.

---

## 5. `book_events`

- **Sequence continuity (T2):** per token, `sequence_number` strictly increasing with
  **≤ 0.1%** gaps; each gap must have fired a `sequence_gap` event and (if data was lost) a
  resync episode. Unexplained gaps = T1 attribution violation.
- **Change-chain consistency (T1):** for consecutive events on the same token,
  `new_best_*` of event *N* equals `old_best_*` of event *N+1* (unless a full `book` snapshot or
  resync intervened). A silent jump is a dropped event.
- **Bounds (T1):** prices ∈ [0,1] tick-aligned, sizes ≥ 0 (malformed messages are rejected at
  ingestion and never reach this table).
- **Latency (T3):** `ts_received_ns − ts_source` median ≤ 100ms, p99 ≤ 500ms.

## 6. `trades`

- **Bounds (T1):** `price` ∈ [0,1] tick-aligned; `size > 0` (a zero-size trade is a glitch);
  `notional` present and equal to `price × size` within 1e-9 (or explicitly null everywhere —
  never sometimes-derived).
- **Fee accounting (T1):** exactly one of: exchange-reported fee with `fee_is_estimated = false`,
  or 0.07% of notional with `fee_is_estimated = true`. No fee row without its flag.
- **Inside-the-book (T3 ≤ 0.1%):** trade price lies within the prevailing BBO of that token
  ±1 tick (trades execute at resting limit prices).
- **Ordering/dedup (T1):** `trade_id` unique; `ts_source` non-decreasing per market;
  `aggressor_side` ∈ {buy, sell} when present.

## 7. `chainlink_events`

- **Cadence (T2 ≥ 99.9% of expected window):** per asset, median inter-event gap ≤ 2s and no gap
  > 10s without a `collector_events` record. This table is the settlement oracle — holes here
  directly corrupt resolution.
- **Bounds (T1):** `price > 0`; `twap` within ±1% of `price` when both present;
  `twap_window_seconds = 300` for window TWAPs.
- **Settlement linkage (T1):** for every resolved market there exists an event with
  `|ts_source − market_end_ts| ≤ 1s`, and for `on_chain_confirmed` settlements
  `settlement_price` equals that event's `price` **exactly**.

## 8. `resync_episodes` and `collector_events`

- **Attribution closure (T1):** every documented gap (§3.1) maps to an episode with
  `disconnect_ts_utc < reconnect_ts_utc ≤ resync_rest_fetch_ts_utc ≤ resync_completed_ts_utc`,
  non-null `resync_id`, `disconnect_reason` ∈ `enums.py`, `resync_attempt_count ≥ 1`.
- **Downtime budget (T3):** `sum(gap_duration_ms) ≤ 0.1%` of the day (≤ 86.4s/day/asset).
- **Rollover health (T2 ≥ 99.9%):** `rollover_completed` per market per asset;
  `rollover_miss` and `coverage_gap` counts ≤ 1 per 1000 windows.
- **No junk:** `event_type` ∈ `enums.py`; `details` is valid JSON.

---

## 9. Glitch catalog (each one is auto-detectable and must be 0 or within its T3 budget)

| # | Glitch | Detection | Tier |
|---|---|---|---|
| G1 | Missing 500ms tick | gap in `(condition_id, ts_snapshot_ns)` grid | T2/T4 |
| G2 | Unexplained gap | missing tick with no resync/gap record | **T1 fail** |
| G3 | Null BBO on a liquid book | BBO null with `book_state='live'` | T3 |
| G4 | Price out of [0,1], size < 0, NaN/inf | `validation.py` bounds | T1 |
| G5 | Crossed / locked book | `up_bid > up_ask` etc. | T1 clean / T3 raw |
| G6 | Depth ≠ recomputed-from-L2 | recompute via `depth_within` | T1 |
| G7 | Duplicate snapshot/event/trade | uniqueness keys (§2) | T1 |
| G8 | Sequence gap | monotonicity break per token | T2/T4 |
| G9 | `ts_received_ns < ts_source` | negative latency | T1 |
| G10 | Off-grid timestamp | `ts_snapshot_ns % 500_000_000 ≠ 0` | T1 |
| G11 | Wrong partition | row date ≠ UTC date of timestamp | T1 |
| G12 | Orphan foreign key | condition/token/resync_id not in parent table | T1 |
| G13 | `market_time_remaining_ms` inconsistency | ≠ end − ts | T1 |
| G14 | Complementarity break | \|up_ask + down_bid − 1\| > 2 ticks | T3 |
| G15 | Settlement mismatch | no chainlink event at end, or price ≠ settlement_price | T1 |
| G16 | Stuck resolution | `closed` with no `resolved` after 120s | T3 |
| G17 | Empty window (no snapshots) | market row exists, 0 snapshot rows | T2 fail |
| G18 | Silent drop under backpressure | rows missing after `backpressure` event | **T1 fail** |

---

## 10. Acceptance scorecard — the 99.9% gate

A dataset is **accepted** when every row below passes. Any T1 row at < 100%, or any T2/T3 row
below target, fails the gate.

| # | Metric | Formula | Target | Tier |
|---|---|---|---|---|
| 1 | Snapshot completeness (per asset/day) | `clean_ticks / expected_ticks` (172,800) | ≥ 99.9% | T2 |
| 2 | Snapshot completeness (per market) | `clean_ticks / 600` | ≥ 99.9% | T2 |
| 3 | Unexplained missing ticks | missing ticks with no episode/gap record | **0** | T1 |
| 4 | BBO null rate (clean rows) | rows with any BBO null / clean rows | ≤ 0.1% | T3 |
| 5 | Stale rate (raw) | `book_state='stale'` rows / raw rows | ≤ 0.1% | T3 |
| 6 | Crossed rows (raw) | `book_crossed=true` / raw rows | ≤ 0.01% | T3 |
| 7 | Sequence gaps unexplained | gaps w/o `sequence_gap` event | **0** | T1 |
| 8 | Duplicates (all tables) | unique-key violations | **0** | T1 |
| 9 | Orphan foreign keys | §2 rule 3 violations | **0** | T1 |
| 10 | Depth mismatches | recompute deltas > 1e-9 | **0** | T1 |
| 11 | Grid violations | off-grid / off-partition rows | **0** | T1 |
| 12 | Market coverage | listed windows with a terminal market row | ≥ 99.9% | T2 |
| 13 | Settlement completeness | resolved markets with full settlement fields | ≥ 99.9% (target 100%) | T2 |
| 14 | On-chain settlement share | `settlement_source='on_chain_confirmed'` / resolved | ≥ 99.9% | T2 |
| 15 | Chainlink end-price availability | resolved markets with end event ≤ 1s | ≥ 99.9% | T2 |
| 16 | Rollover success | `rollover_completed` / windows | ≥ 99.9% | T2 |
| 17 | Disconnected downtime | `sum(gap_duration_ms)` / day | ≤ 0.1% | T3 |
| 18 | Null rate on schema-required cols | non-nullable schema fields null | **0** | T1 |

**Scoring honesty:** tiers 1/2/3 exist because ~all T3 degradation originates on the exchange
side (a thin book with empty asks is real market state, not collector error). The collector's
job is: never create T1 violations, capture ≥ 99.9% of what existed, bound the rest at ≤ 0.1%,
and record 100% of exceptions.

---

## 11. How to verify (works directly on the Parquet output)

Use DuckDB against the hive layout, e.g.:

```sql
-- M1: per-market completeness (G1)
SELECT condition_id, asset,
       count(*) AS ticks,
       600 - count(*) AS missing,
       (date_diff('millisecond', min(ts_snapshot_utc)::TIMESTAMP,
                  max(ts_snapshot_utc)::TIMESTAMP)/500 + 1) AS grid_span
FROM read_parquet('data/book_snapshots_clean/date=*/asset=BTC/*.parquet')
GROUP BY condition_id, asset
HAVING ticks < 600 OR grid_span <> 600;

-- G2/G3/G5: unexplained nulls and crossed books in the clean view
SELECT count(*) FILTER (WHERE up_bid IS NULL OR up_ask IS NULL
                         OR down_bid IS NULL OR down_ask IS NULL) AS bbo_nulls,
       count(*) FILTER (WHERE up_bid > up_ask OR down_bid > down_ask) AS crossed,
       count(*) AS total
FROM read_parquet('data/book_snapshots_clean/date=*/asset=*/*.parquet');

-- G7: duplicate snapshots
SELECT condition_id, ts_snapshot_ns, count(*)
FROM read_parquet('data/book_snapshots_500ms/date=*/asset=*/*.parquet')
GROUP BY 1, 2 HAVING count(*) > 1;

-- G15: every resolved market has a chainlink end-tick within 1s
SELECT m.condition_id, m.settlement_price, c.price AS chainlink_end,
       abs(datediff('millisecond', c.ts_source::TIMESTAMP, m.market_end_ts::TIMESTAMP)) AS lag_ms
FROM read_parquet('data/markets_latest/markets_latest.parquet') m
JOIN read_parquet('data/chainlink_events/date=*/*.parquet') c
  ON c.asset = m.asset
 AND abs(datediff('millisecond', c.ts_source::TIMESTAMP, m.market_end_ts::TIMESTAMP)) <= 1000
WHERE m.status = 'resolved';
```

Expected daily totals to sanity-check against: 288 windows/asset/day, 600 ticks/market,
172,800 ticks/asset/day, ~1,209,600 ticks/day across all 7 configured assets, ~60
`is_rollover_window=true` ticks per market.

---

## 12. Relationship to existing code

- Tier bounds extend `TEST_PLAN_99pct.md` (99% → 99.9%) and reuse its scenarios.
- `completeness.py::compute_daily_completeness` supplies metric #1 at day granularity; per-market
  metric #2 (600 ticks) is stricter and should be added there.
- `validation.py` already rejects G4-class values at ingestion; this spec requires that rejection
  counter (`book_anomaly` / `sanity_violations`) stays ≤ 0.1% of messages.
- `verify_gate.py` must pass before any run that claims this spec.
- Every failed check in this document must emit the matching `CollectorEventType`
  (`snapshot_gap`, `sequence_gap`, `book_anomaly`, `resolution_stuck`, `write_failed`,
  `backpressure`, `clock_issue`, …) — that is what makes Tier 4 closure checkable.
