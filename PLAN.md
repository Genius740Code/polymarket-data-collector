# PLAN.md — BTC / ETH / SOL Data Storage (v3 — field fixes + pre-build gating)

## Scope

This file defines the data our program will store for **BTC, ETH, and SOL 5-minute up/down markets** on Polymarket.

The asset list is **configurable, not hardcoded** — adding a fourth asset later should require a config change, not a schema change.

The goal is to preserve enough information for strategy research, realistic backtesting, latency research, and future bot development — without gaps, without ambiguity about what a stored value means, and without unnecessarily storing every raw message — and to keep running **24/7 without silently losing data** when the network, the exchange, or our own process misbehaves.

**v2 changes (from v1):** added §1A (WebSocket disconnect/resync), §1B (crash/restart recovery), §6A (settlement ground truth), §9A (mutable-state handling under Parquet), §10A (write batching/backpressure), §17A (watchdog/alerting), plus dedup, sanity-check, rate-limit, and clock-threshold fixes.

**v3 changes (this revision):** closes the field-level ambiguities and process gaps found in review before coding starts:
- §3: depth-aggregate units now precisely defined; null-vs-zero rule extended to top-of-book and depth fields, not just L2 levels.
- §3A (NEW): explicit price/size sanity-bounds validation, separate from drift/crossed-book anomaly detection.
- §9B (NEW): canonical "clean" query view definition, so gap-exclusion isn't left to each backtest script's discretion.
- §1B: concurrency spec added for the local durable cursor store (WAL mode / per-asset file decision).
- §13: backfill note — clarifies what the raw archive does and doesn't recover, and where a secondary source could fill genuine outage windows.
- §19 (NEW): testing / chaos-injection plan required before an unattended live run.
- §18: reframed as a **gate**, not a checklist to verify alongside coding — §1A should not be implemented until these are answered against real payloads.

Nothing from v1/v2 was removed; items are added, tightened, or made precise.

---

## 0. Assets

```
assets: [BTC, ETH, SOL]
```

Each asset is collected identically. Everywhere below, "per asset" means this pipeline runs independently and in parallel for BTC, ETH, and SOL.

**Before enabling an asset:** confirm a live 5-minute up/down market actually exists for it and has meaningful liquidity (volume, order book depth, spread). Don't assume — check directly against Polymarket's market list/API.

---

## 1. Market Rollover — Continuous Coverage Across Windows

Every 5-minute window is a brand-new market (new `condition_id`, new `up_token_id`/`down_token_id`). If the collector only discovers and subscribes to the next window's market *after* the current one resolves, the first snapshots of the new market are lost.

### Rollover procedure

1. **Track `market_end_ts`** for the currently active market of each asset.
2. **~30 seconds before `market_end_ts`** (configurable, `rollover_lead_seconds: 30`), the collector:
   - Queries Polymarket for the **next** market for that asset.
   - Subscribes to its order-book and trade feeds *immediately*, in addition to the still-active current market.
   - Begins writing snapshots/events/trades for the new market as soon as data arrives.
3. **Dual-tracking overlap window**: for the ~30s leading up to rollover, and briefly after, the collector holds **two active book states per asset** (current + next), each snapshotted independently at the normal 500ms cadence, tagged with its own `condition_id`.
4. **After `market_end_ts` passes** and the current market resolves, drop it from the active set and promote "next" to "current." Repeat the lookahead for the following market.
5. If the next market cannot be discovered in time, log a `rollover_miss` collector event.
6. **Distinguish "discovered late" from "no market existed."** If, after the previous window's `market_end_ts`, no next market is subscribed within a configurable `max_coverage_gap_seconds` (e.g. 5s), emit a `coverage_gap` event (see §8) — separate from `rollover_miss` — so research code can tell "we were slow" apart from "the venue genuinely had no market for this asset during this interval."
7. **Rate-limited discovery polling.** The lookahead query is polled on a backoff schedule (e.g. every 2s starting at `rollover_lead_seconds`, capped), not tight-looped, to avoid hitting API rate limits across 3 assets simultaneously. Log `rate_limited` (§8) if the API returns a 429/backoff response, and widen the poll interval automatically.

### `series_id` / `window_index`

```
series_id           # stable identifier for the asset's continuous market series, e.g. "BTC-5MIN"
window_index         # monotonically increasing integer, one per 5-min window since collection began
```

Lets you reconstruct "BTC's full price/book history" as one continuous series without manually joining thousands of individual markets on timestamp ranges.

---

## 1A. WebSocket Disconnection & Resync Handling (core fix)

**The failure mode:** most order-book feeds are *incremental* (deltas on top of a book you built earlier). If the WebSocket drops and reconnects, and the collector just resumes applying deltas to its old in-RAM book, that book is now silently wrong — every snapshot written after that point looks normal but doesn't reflect the real market. This is the single most dangerous way to "lose data" because it doesn't look like loss; it looks like clean, complete, wrong data.

**Do not begin implementing this section until §18 is answered against live payloads.** The design below depends on assumptions (a usable sequence/cursor field; a full-L2 REST endpoint) that are not yet confirmed. Coding this against the wrong assumption means building the wrong primary mechanism, not just tuning a parameter.

### Rule: never resume a book from a stale state after a disconnect

On **every** disconnect (network drop, exchange-initiated close, heartbeat timeout, process pause/GC stall long enough to miss heartbeats), the collector must:

1. **Mark the book "dirty" immediately.** As soon as the socket closes, flag the affected `(asset, condition_id)` book(s) as `stale = true`. Any snapshot taken while `stale = true` is tagged `book_state = "stale"` in `book_snapshots_500ms` rather than silently written as if valid — or, preferably, snapshot writing is *paused* for that book until resync completes, and the gap is logged instead of filled with bad data.
2. **Reconnect with backoff.** Exponential backoff with jitter (e.g. 0.5s, 1s, 2s, 4s... capped at ~30s), so a broad outage doesn't hammer the API. Log every attempt.
3. **On successful reconnect, do a full resync before trusting deltas:**
   - Fetch a fresh **full order-book snapshot via REST** (not the WS delta stream) for every currently-active `(asset, condition_id)` pair. Confirm the CLOB REST API actually exposes a full L2 book endpoint per token (not just a best-bid/ask/spread summary endpoint) before relying on this step — verify against the live API, not assumed (§18).
   - Replace the in-RAM book wholesale with this snapshot. Discard the old, possibly-stale book entirely — do not try to "patch" it.
   - **Buffer, don't just filter, WS messages received during the REST fetch.** From the moment the WS reconnects, start buffering every incoming delta/`book` message in order. Once the REST snapshot arrives: apply it as the new book baseline, then replay the buffered messages in order, discarding only those provably older than or equal to the snapshot's own cursor (if available) and applying the rest. Without this buffer-and-replay step, any delta arriving in the gap between "REST fetch started" and "REST snapshot applied" is silently lost.
   - Note the **sequence number** (or equivalent cursor) the REST snapshot corresponds to, if the API exposes one, so buffered deltas older than that cursor can be safely dropped instead of double-applied.
4. **Clear the `stale` flag** only once the REST resync has completed successfully **and** the buffered-message replay (above) has finished.
5. **Never silently drop the gap.** Whether it's 200ms or 20 seconds, the exact disconnect window is recorded (see below) so downstream code always knows precisely which intervals have `book_state = "stale"` or are simply missing, rather than inferring it after the fact from timestamp math.

### Fields added to support this

`book_snapshots_500ms` gains:
```
book_state            # "live" | "stale" | "resyncing"  — resyncing = REST snapshot in flight, not yet applied
resync_id             # nullable; groups all snapshots affected by the same disconnect/resync episode
```

`collector_events` (§8) gains explicit event types for this: `ws_disconnected`, `ws_reconnect_attempt`, `ws_reconnected`, `resync_started`, `resync_completed`, `resync_failed`.

A new small reference dataset, **`resync_episodes`**, records each disconnect/resync cycle so gaps are queryable without scanning every snapshot:
```
resync_id
asset
condition_id                # or list, if multiple markets affected
disconnect_ts_utc
disconnect_reason           # network_error | heartbeat_timeout | exchange_close | process_pause | unknown
reconnect_ts_utc
resync_rest_fetch_ts_utc
resync_completed_ts_utc
gap_duration_ms             # reconnect_ts_utc - disconnect_ts_utc
snapshots_missed_estimate   # gap_duration_ms / 500, informational
resync_attempt_count        # how many resync attempts this episode took before success/give-up, see retry policy below
```

### Resync retry policy

The reconnect backoff (step 2 above) governs getting the WebSocket back; it does **not** cover what happens if the REST resync fetch itself keeps failing (endpoint down, rate-limited, 5xx) after the socket is already back up. Without an explicit policy here, a book can sit `stale` indefinitely with nothing writing, and no one would notice except by querying.

- Retry the REST resync fetch on its own exponential backoff (independent of the WS reconnect backoff), e.g. 1s, 2s, 4s, 8s... capped at ~20s.
- Log `resync_failed` (§8) on every failed attempt, incrementing `resync_attempt_count`.
- If resync has not succeeded within a configurable `max_resync_duration_seconds` (e.g. 60s), escalate: this crosses from "routine transient failure" to "page the operator" territory — treat it the same as the `ws_disconnected > X seconds` alert in §17A rather than continuing to retry silently forever.
- The book stays `stale` and unsnapshotted for the entire duration; this is intentional (never write unverified state), but it must be *loud*, not just logged.

### Sequence number gap detection (applies even without a full disconnect — where the API supports it)

Where the exchange feed exposes a `sequence_number` per token, the collector tracks the last-seen sequence number per `token_id` and:
- If a newly received message's sequence number is not exactly `last_seen + 1`, log a `sequence_gap` event (§8) with the expected vs. received values — **and treat this the same as a disconnect**: mark the book `stale`, trigger a REST resync. A sequence gap without a socket-level disconnect (e.g. a dropped message on a technically-open connection) is just as dangerous as a full disconnect and must not be ignored.
- Duplicate or out-of-order (lower than expected) sequence numbers are logged as `duplicate_event` and dropped, not reapplied.

### Full-book diff drift check (fallback / supplement when sequence numbers are unavailable or unreliable)

Because §1A's sequence-based gap detection depends on a field that may not exist on this feed (§18), add an independent, always-on drift check that doesn't depend on sequence numbers being present:

- On a regular interval (e.g. every 30–60s per active `(asset, condition_id)`), fetch a fresh REST full-book snapshot and diff it against the in-RAM book built from applied deltas.
- If the diff exceeds a small tolerance (e.g. any level mismatch beyond expected in-flight timing noise), treat this as silent drift: mark the book `stale`, log `book_anomaly` (§8) with the diff details, and trigger the same resync procedure as a disconnect.
- This check runs regardless of whether sequence-number gap detection is available, and is the primary defense if it turns out the feed doesn't expose usable sequence numbers at all.

### Redundancy option (recommended if zero-gap is a hard requirement)

A single collector process is a single point of failure — a disconnect-and-resync still produces a real gap of however long reconnect+resync takes (typically sub-second to a few seconds, but not zero). If truly zero-gap coverage matters more than infra cost:
- Run **two independent collector instances** (ideally on different network paths/regions) subscribing to the same feeds.
- Both write to the same dataset with an idempotent write key of `(asset, condition_id, token_id, sequence_number)` for events/trades and `(asset, condition_id, ts_snapshot_bucket)` for snapshots, so duplicates from the redundant collector are naturally deduplicated at merge/compaction time (§9A), not double-counted. `ts_snapshot_bucket` must be computed against a shared wall-clock grid (e.g. `floor(unix_ms / 500) * 500`, aligned to UTC epoch boundaries) rather than each collector's own independent 500ms timer — otherwise two collectors' snapshot timestamps won't land in the same bucket even when capturing "the same" 500ms tick, and dedup silently fails to dedup.
- This turns "one collector's WS drop" into "usually covered by the other collector's still-open connection," at the cost of running and reconciling two pipelines.
- If this is overkill for now, at minimum keep it as a documented future option — the schema above (idempotent keys, `resync_episodes`) is designed to support adding a second collector later without a schema change.

---

## 1B. Crash / Restart Recovery

A WS resync fixes *in-flight* disconnects, but doesn't help if the collector **process itself** crashes or is redeployed. Two things must survive a process restart:

1. **Persisted cursor state**, written to a small local durable store (SQLite file or equivalent — not Parquet) on a short interval (e.g. every 5–10s) and on clean shutdown:
   ```
   asset
   current_window_index
   current_condition_id
   next_condition_id            # if already discovered pre-rollover
   last_sequence_number_per_token
   last_snapshot_written_ts
   ```
2. **On startup**, the collector reads this state and:
   - If a `current_condition_id` is still active (its `market_end_ts` hasn't passed), it does **not** treat this as a fresh start — it resubscribes to that market, forces a full REST resync (same procedure as §1A) rather than assuming its last in-RAM state is valid (there is no in-RAM state after a restart), and logs a `collector_restarted` event with the estimated downtime gap.
   - If the persisted `current_condition_id`'s market has already ended while the collector was down, it logs a `coverage_gap` (§8) for the full downtime window, then proceeds fresh via the normal discovery/rollover path.
3. Startup always logs `collector_started` (fresh) or `collector_restarted` (recovered state found) with the gap duration, so downtime is queryable the same way WS-disconnect gaps are.

### Concurrency (NEW)

If BTC/ETH/SOL run as **separate processes**, use **one SQLite file per asset** for the persisted cursor store — avoids any lock contention between processes and keeps a crash in one asset's process from touching another's state file.

If they instead run as **threads/tasks within one process**, a single shared SQLite file is fine, but it must be opened in **WAL mode** so the periodic 5–10s writes from each asset's task don't serialize behind each other or block reads used for monitoring/debugging.

Either way, writes to this store should be small, synchronous, and not share a code path with the much larger Parquet write pipeline (§10A) — this store exists specifically to survive a crash that the batched-flush buffer would not.

---

## 2. Assets and Markets — Metadata

For every market (every 5-minute window, every asset), store:

```
schema_version
series_id
window_index
condition_id
market_id
asset

up_token_id
down_token_id

market_start_ts
market_end_ts
resolution_ts

status
resolution_outcome
```

Also store, when available:
```
question
resolution_rule
resolution_source

tick_size
minimum_order_size
minimum_notional

fee_information
reported_volume
reported_liquidity
```

See **§6A** for the settlement/resolution ground-truth fields added here.

---

## 3. book_snapshots_500ms

One order-book snapshot every **500 milliseconds** per active market, per asset.

**Clock synchronization:** all assets snapshot from the **same clock tick** (single scheduler firing every 500ms across BTC/ETH/SOL), not independent drifting loops. This scheduler tick should itself be aligned to the shared wall-clock grid described in §1A's redundancy section (UTC epoch boundaries), so a single collector's timestamps are already compatible if a second collector is added later.

### Identifiers
```
snapshot_id
schema_version
series_id
window_index
condition_id
market_id
asset
up_token_id
down_token_id
```

### Timestamps
```
ts_snapshot_utc
ts_snapshot_ns
```

### UP / DOWN best prices
```
up_bid / up_ask / up_bid_size / up_ask_size
down_bid / down_ask / down_bid_size / down_ask_size
```
**Null-vs-zero applies here too (CHANGED — was previously only stated for L2 levels below):** if a side of the book is genuinely empty (no resting orders at all), `*_bid`/`*_ask`/`*_bid_size`/`*_ask_size` are `null`, not `0`. `0` is reserved for a real observed zero-size level; an empty book is the *absence* of a level, not a zero-size one. The same distinction previously written only for L2 levels applies uniformly to top-of-book and to the depth aggregates below.

### L2 order book (10–20 levels, both sides, both outcomes)
```
up_bid_level_{1..20}_price / _size
up_ask_level_{1..20}_price / _size
down_bid_level_{1..20}_price / _size
down_ask_level_{1..20}_price / _size
```
When fewer than 20 levels exist on a side, the unfilled `_price`/`_size` fields are `null`, never `0` — see the null-vs-zero rule above, now stated once and applied consistently across §3 rather than being L2-specific.

**Note on column layout:** this is a wide flat-column design (~160 L2 columns alone). That's fine for columnar backtest reads, but it's inflexible if the depth-level count ever changes and it multiplies the null-vs-zero bookkeeping across many columns. A nested `list<struct<price,size>>` column per side/outcome is a reasonable alternative if flexibility matters more than the current design's simplicity — not required, just worth a deliberate choice rather than a default.

### Depth aggregates (units CLARIFIED — NEW)

```
{up,down}_{bid,ask}_depth_1c
{up,down}_{bid,ask}_depth_5c
{up,down}_{bid,ask}_depth_10c
```

**Definition:** for a given side (bid/ask) of a given outcome (up/down), `depth_Nc` is the **cumulative resting size, summed across all price levels within N cents of that side's own best price** (i.e. within N cents of `up_bid` for `up_bid_depth_Nc`, not within N cents of the mid price or of the opposing side). "1c/5c/10c" means one cent, five cents, ten cents, in the token's native [0,1] price units (i.e. `0.01`, `0.05`, `0.10`). Computed from the same L2 levels stored above, not from a separate feed.

If the best price itself is `null` (empty side — see above), the corresponding `depth_Nc` fields are also `null`, not `0`.

### State
```
market_time_remaining_ms
up_book_age_ms
down_book_age_ms
is_rollover_window
book_state             # "live" | "stale" | "resyncing", see §1A
resync_id              # nullable, see §1A
book_crossed           # true if best_bid >= best_ask observed (should never happen; flags upstream anomalies rather than silently storing them)
```

---

## 3A. Sanity-Bounds Validation (NEW — distinct from drift/crossed-book detection)

`book_crossed` (§3) and the full-book diff check (§1A) catch two specific anomaly shapes: crossed markets and drift between REST and WS state. Neither catches a plain **malformed or out-of-range value** on an otherwise internally-consistent message — e.g. a price field that arrives as `1.4` or `-0.02`, or a size field that arrives negative. Without an explicit bounds check, such a value would be written as a normal-looking row.

On every incoming price/size field, before it's applied to the book or written to a snapshot/event/trade row:

- **Price fields** (bid/ask/level prices, trade price) must be within `[0, 1]` inclusive, since these are binary-outcome token prices. A value outside this range is invalid, not just unusual.
- **Size fields** (bid/ask/level sizes, trade size) must be `>= 0`.

If a field fails its bounds check:
- Log `book_anomaly` (§8) with the offending field, raw value, and message context — reuse the same event type as crossed-book/drift anomalies rather than inventing a parallel one, since downstream monitoring already watches `book_anomaly`.
- Do **not** apply the malformed field to the in-RAM book. Treat the affected book as `stale` and trigger a resync (§1A) rather than guessing at a corrected value or silently dropping just that one field — a book that's had one field silently discarded is in an unknown, unverifiable state, the same reasoning as §1A's core rule.

This check is cheap and independent of whether sequence numbers or a REST L2 endpoint turn out to be available (§18) — it should be built regardless of how §1A's open questions resolve.

---

## 4. book_events

Store an event whenever something important changes (not every raw update): best bid/ask change, significant size change, significant spread change, large L2 liquidity change, significant depth change, UP+DOWN ask/bid threshold crossings, book empty/available transitions.

### Fields
```
event_id
schema_version
series_id
window_index
condition_id
market_id
asset

token_id
outcome
event_type

ts_source
ts_received_ns
sequence_number

old_best_bid / new_best_bid
old_best_ask / new_best_ask
old_bid_size / new_bid_size
old_ask_size / new_ask_size

threshold_config_id
```

**Dedup rule:** events are written with a uniqueness constraint on `(token_id, sequence_number)` where a sequence number is available (§18 — if the feed doesn't expose one, fall back to a uniqueness constraint on `(token_id, ts_received_ns, event_type, new_best_bid, new_best_ask)` as a best-effort substitute). Any redelivery (from at-least-once WS delivery, reconnect replay, or a redundant collector per §1A) that matches an already-written key is dropped at write time, not appended as a duplicate.

---

## 5. trades

```
trade_id
schema_version
series_id
window_index
condition_id
market_id
asset

token_id
outcome

price
size
notional
fee                     # per-trade fee if the API provides it; needed for accurate realized-PnL backtests, separate from market-level fee_information

side                    # only if provided
aggressor_side          # only if provided or reliably inferable

ts_source
ts_received_ns
sequence_number
```

Same dedup rule as §4: unique on `(token_id, sequence_number)` where available, else `(token_id, trade_id)`.

---

## 6. chainlink_events

Every relevant Chainlink price/data-stream event for BTC, ETH, and SOL — native frequency, not sampled to 500ms.

```
event_id
schema_version

asset
symbol
source

price
twap
twap_window_seconds

report_id               # Polymarket's 5-min markets settle via Chainlink Data Streams + Automation, which are report-based (on-demand signed reports), not classic round-indexed price feeds. Store report_id as the primary identifier; keep round_id as an optional legacy field only if a given feed actually exposes one.
round_id                 # optional/legacy, nullable

sequence_number

ts_source
ts_received_ns
```

**Rules:** full precision, no rounding, never overwrite, preserve both source and receive timestamps.

---

## 6A. Settlement Ground Truth

`chainlink_events` captures the ambient price feed, but it is **not** the same thing as "the exact value Polymarket used to resolve this specific market." Resolution happens via a Chainlink Automation-triggered on-chain settlement at `market_end_ts`, and that specific report is what determines `resolution_outcome` — it should be captured directly, not reconstructed later by nearest-timestamp-joining `chainlink_events`.

Add to the `markets` table (§2):
```
settlement_report_id        # the specific Chainlink Data Streams report used for resolution
settlement_price             # the exact price value from that report
settlement_ts_utc            # timestamp of the settlement report itself (may differ slightly from market_end_ts)
settlement_tx_hash            # on-chain transaction that executed the resolution, if available
resolution_confirmed_at       # when our collector observed/confirmed this resolution (see §9A — this can arrive after market_end_ts)
```

If the settlement report/tx cannot be independently fetched (e.g. Polymarket doesn't expose it directly), fall back to: the `chainlink_events` row with `ts_source` closest to `market_end_ts`, but explicitly flag `settlement_source = "inferred_nearest"` vs `settlement_source = "on_chain_confirmed"` so backtests can tell the difference and, if needed, exclude inferred-only resolutions from strict-accuracy studies.

**Stuck/weird resolution alerting:** if a market's `resolution_outcome` is still `unknown` or `disputed` more than a configurable `max_resolution_wait_seconds` (e.g. 120s) after `market_end_ts`, log a `resolution_stuck` collector event (§8) and route it through the same alerting path as §17A.

---

## 7. Status / Outcome Enums

### `status`
```
pending
active
closed
resolved
```

### `resolution_outcome`
```
up
down
tie
voided
disputed
unknown            # must be reconciled later, never treated as a real outcome
```

Downstream code must explicitly handle `voided`, `disputed`, and `unknown` — never assume `resolution_outcome` is always `up`/`down`.

---

## 8. collector_events (data-quality tracking)

```
event_id
ts_utc
ts_received_ns

event_type
connection_id

condition_id
market_id
token_id
asset

details
```

**Event types:**
```
connected / disconnected / reconnected
ws_disconnected / ws_reconnect_attempt / ws_reconnected        # §1A
resync_started / resync_completed / resync_failed              # §1A
subscription_started / subscription_failed
market_added / market_removed
rollover_started / rollover_miss / rollover_completed
coverage_gap                                                     # §1
rate_limited                                                     # §1
snapshot_gap / event_gap
sequence_gap / duplicate_event                                   # §1A
book_anomaly                                                     # crossed book, full-book diff drift, or sanity-bounds failure (§1A, §3, §3A)
resolution_stuck                                                 # resolution_outcome unresolved past max_resolution_wait_seconds (§6A)
write_failed
backpressure                                                     # §10A
collector_started / collector_restarted                          # §1B
clock_issue
```

**Clock issue threshold:** `clock_issue` fires when NTP-reported drift exceeds a configurable threshold, default **50ms**. Include the measured drift in `details`.

**Also track (derivable, worth summarizing periodically):**
```
missing snapshot intervals
disconnect start/end (from resync_episodes, §1A)
sequence gaps
duplicate / out-of-order events
file write failures
process downtime (from §1B state)
```

---

## 9. Schema & Config Versioning

### `schema_version`
Every row in every dataset carries `schema_version`. Bump on any column add/remove/redefine. Never silently reinterpret old columns under a new meaning.

### `event_thresholds_config`
```
threshold_config_id
effective_from_ts
effective_to_ts

spread_change_threshold
size_change_threshold_pct
depth_change_threshold_pct
crossing_threshold
```
Every `book_event` row stores the `threshold_config_id` active when it fired.

---

## 9A. Handling Mutable State Under an Immutable Storage Format

Some fields are **not known at write time and change later** — most importantly `resolution_outcome` (can sit at `unknown`/`disputed` before settling), and the §6A settlement fields. Parquet partitions are treated as append-only; naively "editing" a row already written to `markets.parquet` is unsafe with concurrent readers/writers.

**Approach: append-only state log + compacted "latest" view.**
1. `markets` becomes an **event-sourced** table: every time a market's status/outcome/settlement fields change, append a new row (`condition_id`, `updated_at`, full field snapshot) rather than mutating in place — small staging store (SQLite/Postgres) buffers these before periodic Parquet flush, same as §10A.
2. A separate, periodically-rebuilt **`markets_latest`** Parquet dataset (or view) is compacted from the log on a schedule (e.g. every few minutes) — one row per `condition_id`, taking the most recent state. This is what research code queries by default; the full log remains available for auditing "what did we know, when."
3. Applies to any other field that can legitimately change post-write (e.g. `reported_volume` if the API updates it after the fact).

---

## 9B. Canonical Clean View for Research (NEW)

§15 says research/backtest code *should* exclude incomplete or `stale`-tagged periods, but leaving that filter to each script's discretion is exactly how someone eventually forgets it and gets contaminated results. Define it once, centrally:

- A compacted view/dataset, **`book_snapshots_clean`**, built alongside `markets_latest` (§9A) on the same refresh cadence, defined as:
  ```
  SELECT * FROM book_snapshots_500ms
  WHERE book_state = 'live'
    AND condition_id NOT IN (markets currently resolution_outcome IN ('disputed','unknown') per markets_latest, unless the caller explicitly opts in)
  ```
- This is the **default read path** for backtests and strategy research. Anyone who genuinely needs `stale`/`resyncing` rows (e.g. studying resync behavior itself, or building the redundancy option in §17B) queries `book_snapshots_500ms` directly and does so deliberately, not by accident.
- Document this convention in the same place as the schema itself (e.g. a top-of-repo README pointer), so "which table do I query" isn't tribal knowledge.

---

## 10. Storage Frequency

| Data | Frequency |
|---|---|
| CLOB updates | Processed every update, in memory |
| Order-book state (top-of-book, L2, depth) | Every 500 ms |
| Important book changes | Event-driven |
| Trades | Every available trade |
| Chainlink price/data-stream events | Every event |
| Market metadata | On discovery / change (event-sourced, §9A) |
| Rollover lookahead | Starts ~30s before each market's close |
| Connection / data-quality events | Every event |
| Persisted cursor state (§1B) | Every 5–10s and on shutdown |

---

## 10A. Write Batching & Backpressure

Writing an individual Parquet file per 500ms tick per asset, forever, produces a small-files problem that degrades both write throughput and later query performance, and gives no room to absorb slow disk I/O.

1. **In-memory buffer / WAL first.** All snapshots, events, trades are appended to an in-memory buffer (with a local WAL/journal for crash safety) rather than written directly as individual Parquet files.
2. **Batched flush.** Flush to Parquet on whichever comes first: a time interval (e.g. every 60s) or a row-count threshold. This is compatible with §9A's compaction approach.
3. **Backpressure handling.** If the buffer grows past a configured limit (disk too slow, downstream stall), the collector logs `backpressure` (§8) and — critically — **never drops data to relieve pressure**; it either blocks briefly, spills to local disk as a fallback WAL, or (last resort) pages an operator. Silently dropping snapshots under load would recreate exactly the "looks fine, actually missing" failure mode from §1A.
4. **Disk space monitoring.** A low-disk-space check runs independently of the write path and fires `write_failed`-class alerting before the disk actually fills.
5. **Periodic compaction job** merges small flushed files into larger partition files on a schedule (e.g. daily), independent of the live write path. Compaction must write to a temporary file and atomically rename it into place on success, never write in-place over an existing partition file — a compactor crash mid-write is a second way to corrupt already-settled data, distinct from the live-collector failure modes §1A/§1B cover.

---

## 11. Storage Format

Apache Parquet, partitioned by date (UTC) and asset:

```
data/
  book_snapshots_500ms/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/part-000.parquet
  book_snapshots_clean/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/part-000.parquet   # NEW — compacted view, §9B
  book_events/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/part-000.parquet
  trades/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/part-000.parquet
  chainlink_events/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/part-000.parquet
  markets_log/date=YYYY-MM-DD/part-000.parquet          # append-only, §9A
  markets_latest/markets_latest.parquet                  # compacted view, §9A
  resync_episodes/date=YYYY-MM-DD/part-000.parquet       # §1A
  event_thresholds_config/config.parquet
  collector_events/date=YYYY-MM-DD/part-000.parquet
  raw_ws_archive/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/    # short retention, see §13
```

Date partitions use **UTC calendar days**, explicitly, to avoid local-timezone ambiguity around midnight. Rotate files via the §10A compaction job — do not let one file grow forever.

---

## 11A. Capacity Planning

Before assuming "24/7, won't fill the disk" holds, size it explicitly rather than by assumption:

- `book_snapshots_500ms` alone is ~172,800 rows/day per asset (2/sec × 86,400s), × 3 assets, × ~160 L2 price/size columns plus identifiers/state fields. Estimate uncompressed and Parquet-compressed row size from the actual field list above, multiply out to a daily/weekly/monthly figure, and confirm it against available disk before relying on the pipeline running unattended for weeks.
- Include `book_events`, `trades`, and `chainlink_events` in the same estimate — their volume is data-dependent (event-driven) rather than fixed-cadence, so size them from a short pilot run rather than guessing.
- Feed this into the §10A compaction schedule and the §13 raw-archive retention window: if compaction cadence or archive retention was picked arbitrarily, revisit both against the actual measured volume.
- Re-check this estimate once real data starts flowing — pilot-run numbers often differ from the back-of-envelope figures above, especially for event-driven tables.

---

## 12. Raw Data Rules

Store raw collected facts only. Do not persist calculated strategy values (spread, mid price, microprice, imbalance, arbitrage signals, etc.) — compute them later from raw data. Keeps the dataset reusable across future strategies.

---

## 13. Short-Retention Raw Websocket Archive (safety net)

Keep the **raw, unprocessed websocket messages** for a short rolling window (e.g. 24–48 hours) in a separate, cheap storage path. If a bug is later found in event-detection or snapshot logic, this lets you replay and re-derive correct data for the affected window. Rolling debug buffer, deleted after retention window.

**Scope note (NEW):** this archive only replays messages our own collector actually received — it does **not** recover the seconds where the collector was genuinely disconnected (§1A) or down (§1B); there's nothing to replay for a window nothing was captured in. If backfilling genuine outage windows matters (rather than just excluding them via `book_state`/`book_snapshots_clean`, §9B), that requires an independent secondary data source — e.g. a historical-trades/candles endpoint from Polymarket or a vendor already in use elsewhere (the Dome API referenced in the user's related data-collection work is a candidate to evaluate, not assumed to cover this). Treat gap-exclusion (§9B) as the default behavior; treat backfilling as a separate, optional project.

---

## 14. Timestamp & Clock Rules

- Preserve both `ts_source` and `ts_received_ns` wherever available. Never substitute one for the other.
- Collector VM's clock must be NTP-synchronized (`chrony` or equivalent). Log `clock_issue` if drift exceeds **50ms** (§8).
- Do not round timestamps.

---

## 15. Data Completeness

Track, per asset per day:
```
expected snapshots vs actual snapshots
missing intervals
disconnect duration                    # from resync_episodes, §1A
resync episode count and total gap time
sequence gaps
duplicate / out-of-order events
write failures
rollover misses
coverage gaps                          # §1
process downtime (crash/restart)       # §1B
resolution_stuck occurrences           # §6A
sanity-bounds violations               # NEW, §3A
```

Research and backtesting code should default to `book_snapshots_clean` (§9B) rather than filtering `book_snapshots_500ms` ad hoc.

---

## 16. Final Data Flow

```
                    BTC / ETH / SOL 5-MIN MARKETS
                              │
                 ┌────────────┴────────────┐
                 │  ~30s before close:      │
                 │  discover + subscribe    │
                 │  to NEXT market too      │
                 └────────────┬────────────┘
                              ▼
                      POLYMARKET CLOB (WebSocket)
                              │
              ┌───────────────┼────────────────┐
              │  every message: check           │
              │  sequence_number continuity ────┼──▶ gap? ─▶ mark book "stale"
              │  (where available; else rely     │           + REST resync (§1A)
              │  on periodic full-book diff)      │
              │  + sanity-bounds check (§3A) ─────┼──▶ out of range? ─▶ same as gap
              │                                  │
              │  disconnect? ────────────────────┼──▶ backoff reconnect
              │                                  │     + REST resync, buffer-and-
              │                                  │     replay in-flight deltas (§1A)
              └───────────────┬──────────────────┘
                              ▼
                     BOOK IN RAM (per asset,
                     current + next during rollover,
                     tagged live/stale/resyncing)
                              │
                 ┌────────────┴────────────┐
                 │                         │
                 ▼                         ▼
          IMPORTANT EVENTS              500 ms
                 │                         │
                 ▼                         ▼
           book_events          book_snapshots_500ms
                 │                         │
                 └────────────┬────────────┘
                              ▼
                  IN-MEMORY BUFFER / WAL (§10A)
                              │
                     batched flush (60s or N rows)
                              ▼
                  PARQUET  ──▶ periodic compaction
                  │           (write temp + atomic rename)
                  └──▶ compacted book_snapshots_clean (§9B, filters stale/anomalous)

TRADES ──every trade, deduped on sequence_number (or fallback key)──▶ trades
CHAINLINK ──every event──▶ chainlink_events
SETTLEMENT REPORT ──on resolution──▶ markets_log.settlement_* (§6A)
  └─ unresolved past timeout ──▶ resolution_stuck ──▶ watchdog/alerting (§17A)
MARKET METADATA ──discovery/change, event-sourced──▶ markets_log ──compact──▶ markets_latest
COLLECTOR HEALTH ──events──▶ collector_events ──▶ watchdog/alerting (§17A)
CURSOR STATE ──every 5-10s──▶ local durable store, per-asset SQLite or WAL-mode shared file (§1B)
RAW WS MESSAGES ──rolling 24-48h──▶ raw_ws_archive (debug/replay only, not outage backfill — §13)
```

---

## 17. Checklist — What Would Otherwise Cause Silent Bugs or Missing Data

- [x] Multi-asset support is config-driven, not hardcoded (§0)
- [x] Rollover lookahead prevents the recurring gap at every 5-min boundary (§1)
- [x] `series_id` / `window_index` lets windows be stitched into one continuous series (§1)
- [x] WebSocket disconnects trigger full REST resync, not silent delta resumption (§1A)
- [x] In-flight WS messages during REST resync are buffered and replayed, not dropped (§1A)
- [x] Sequence-number gaps are detected and treated as disconnects, with a full-book diff fallback if sequence numbers aren't available on this feed (§1A)
- [x] Resync itself has its own retry/escalation policy, independent of WS reconnect (§1A)
- [x] Crash/restart recovers cursor state and forces resync rather than assuming validity (§1B)
- [x] **Cursor-store concurrency (per-asset file vs WAL-mode shared file) is specified, not left to whoever writes the code (§1B)**
- [x] Settlement ground truth captured directly, not inferred from sampled price events (§6A)
- [x] Stuck/disputed/unknown resolutions alert after a timeout, not just sit queryable (§6A)
- [x] Mutable fields (resolution, settlement) handled via event log + compacted view, safe under Parquet (§9A)
- [x] **A canonical clean-data view (`book_snapshots_clean`) makes gap-exclusion the default, not opt-in per script (§9B)**
- [x] Writes are batched with backpressure handling — no silent drops under load (§10A)
- [x] Compaction writes are crash-safe (temp file + atomic rename) (§10A)
- [x] Storage volume sized explicitly against actual disk, not assumed (§11A)
- [x] `schema_version` on every row prevents silent column reinterpretation (§9)
- [x] `resolution_outcome` enum covers voided/disputed/unknown, not just up/down (§7)
- [x] Event thresholds are versioned and traceable per event (§9)
- [x] All assets snapshot on the same clock tick, aligned to a shared wall-clock grid (§3)
- [x] NTP sync required; drift threshold (50ms) defined and logged, not silent (§14, §8)
- [x] Short-retention raw websocket archive as a replay safety net, with its outage-backfill limits stated explicitly (§13)
- [x] Data completeness explicitly tracked, including resync/coverage/downtime/resolution-stuck/sanity-violation gaps (§15)
- [x] **Null-vs-zero applies uniformly to top-of-book, L2 levels, and depth aggregates — not just L2 (§3)**
- [x] **Depth aggregate units (`depth_1c/5c/10c`) are precisely defined, not left ambiguous (§3)**
- [x] **Price/size sanity bounds are validated independently of drift/crossed-book detection (§3A)**
- [x] Duplicate/redelivered events deduped on `(token_id, sequence_number)` or a documented fallback key (§4, §5)
- [x] Crossed-book and other upstream anomalies flagged, not silently stored (§3, §8)

## 17A. Watchdog & Alerting

The data above only helps if someone/something notices problems in near-real-time:

1. A **separate process** (not the collector itself) polls a heartbeat the collector writes every N seconds. If the heartbeat goes stale, the watchdog restarts the collector and/or alerts.
2. Alert (page/notify, not just log) on: `ws_disconnected` lasting > X seconds, `resync_failed` past `max_resync_duration_seconds` (§1A), `coverage_gap`, `rollover_miss`, `backpressure`, `write_failed`, `clock_issue`, `resolution_stuck` (§6A), `book_anomaly` from sanity-bounds violations (§3A), and watchdog-detected process death.
3. A daily summary rolls up §15's completeness metrics per asset so degraded-but-not-fully-broken collection (e.g. one asset consistently resyncing more than others) is visible before it becomes a research-corrupting problem.

## 17B. Optional: Redundant Collector for Zero-Gap Guarantee

See §1A "Redundancy option." Not required for v3, but the schema (idempotent write keys, `resync_episodes`, event-sourced `markets_log`) is designed so a second, independently-running collector instance can be added later purely as an infra change, writing into the same deduplicated tables, without another schema revision.

---

## 18. Verification Gate — Answer Before Building §1A (NEW framing)

This is not a checklist to tick off *alongside* coding — it's a **gate**. §1A's resync/dedup design is built around these assumptions, and two of them (sequence numbers, full-L2 REST) determine which of two structurally different code paths is the primary mechanism vs. the fallback. Confirm against a live payload capture first:

- [ ] Does the CLOB market-channel WS feed expose a monotonic `sequence_number` (or equivalent cursor) per token on `price_change`/`book`/`last_trade_price` messages? If not, §1A's full-book diff check becomes the primary (not fallback) drift-detection mechanism, and §4/§5's dedup key falls back to the documented substitute immediately rather than as a contingency.
- [ ] Does the CLOB REST API expose a full L2 order book per token (not just best-bid/ask/spread summary)? This is required for the resync snapshot step in §1A; if it doesn't exist, §1A needs a redesign before coding, not a workaround during coding.
- [ ] Is there a settlement/resolution report endpoint or on-chain lookup path that gives `settlement_report_id` / `settlement_tx_hash` directly (§6A), or will resolutions have to fall back to `inferred_nearest` in practice? If it's always `inferred_nearest`, say so up front rather than discovering it later.
- [ ] What rate limits actually apply to the REST endpoints used for rollover discovery (§1) and resync (§1A) — the plan assumes backoff will avoid 429s, but the actual limits should size the backoff parameters, not guesswork.

---

## 19. Testing / Chaos-Injection Plan (NEW — required before an unattended live run)

The value of §1A/§1B/§3A is entirely in how they behave under failure. That behavior should be exercised deliberately before trusting a multi-day unattended run for data you intend to backtest on:

1. **Disconnect injection.** Against a live (or recorded-and-replayed) feed, forcibly kill the WebSocket connection at random points — including mid-message and during the ~30s rollover overlap window — and confirm: the book is marked `stale` immediately, no snapshot is written (or is correctly tagged) during the gap, REST resync fires, buffered deltas replay correctly, and `resync_episodes` records an accurate `gap_duration_ms`.
2. **Sequence-gap injection.** If sequence numbers are confirmed available (§18), synthetically drop or reorder a message and confirm `sequence_gap` fires and triggers the same resync path as a full disconnect.
3. **Malformed-message injection.** Feed a price outside `[0,1]` or a negative size (§3A) and confirm it's rejected, logged as `book_anomaly`, and does not get applied to the in-RAM book.
4. **REST failure during resync.** Simulate the resync REST call returning 5xx/429/timeout repeatedly and confirm the independent resync backoff (§1A) retries correctly, `resync_attempt_count` increments, and the `max_resync_duration_seconds` escalation actually pages/alerts rather than retrying forever.
5. **Process crash/restart.** Kill the collector process outright (not a clean shutdown) at various points — mid-window, mid-rollover-overlap — and confirm §1B's startup logic correctly distinguishes "market still active, force resync" from "market ended while down, log coverage_gap."
6. **Backpressure.** Artificially slow down disk I/O or the flush path and confirm the buffer fills, `backpressure` fires, and no snapshots are silently dropped (§10A).
7. Only once each of the above has been observed to behave as designed — not just "the code runs without throwing" — should the collector be trusted for an unattended live run whose output will feed backtests.

---

## 18-verification-status

Kept as an open gate per §18 — no items here should be marked resolved until confirmed against a live payload capture, not against documentation or assumption. 
