# Data Card — Polymarket 5m Crypto (gghgg1/polymarket-5m-crypto)

Dataset: 7 assets (BTC, ETH, SOL, HYPE, BNB, XRP, DOGE) × Polymarket 5-minute Up/Down crypto markets, collected live from the CLOB market WebSocket + REST, with official resolutions and Chainlink RTDS price ground truth.

## Files (39 per version)

Per asset (`{ASSET}_*.parquet` × 5): `book_snapshots_500ms`, `book_snapshots_clean`, `book_events`, `trades`, `chainlink_events`.
Global: `markets.parquet`, `collector_events.parquet`, `resync_episodes.parquet`, and the derived `markets_summary.parquet` (one row per condition_id: resolution, underlying open/close from Chainlink ticks at the window boundaries, outcome-token mid OHLC from clean snapshots, traded volume / fill count / unique traders incl. reconciled `api-` fills, average spread).

## Known caveats (read before querying)

- **`book_events` / `trades` sort order**: sort by `ts_received_ns`. `ts_source` is best-effort — the CLOB market channel does not always carry an event timestamp, so `ts_source` is NULL on ~1–14% of `book_events` rows.
- **No sequence numbers**: the CLOB market channel sends none, so `book_events` and `trades` carry no `sequence_number` column (dropped 2026-09-05 — it was 100% NULL; the earlier populated values were a local tick counter, never a wire sequence — fully removed from `book_events` 2026-09-06). Gap detection is not possible from the wire; books are healed via full `book` snapshots on (re)subscribe and REST `/book`, integrity-checked via the exchange `hash` (A4).
- **Book integrity hashes**: snapshots carry `up_book_hash` / `down_book_hash` — the exchange's `hash` from the last accepted `book`/`price_change` frame per outcome (NULL until the first frame for that side). Stored for downstream audit; the collector also rejects a promoted snapshot whose hash doesn't reproduce (mark stale + REST heal).
- **`collector_events`**: `token_id` dropped 2026-09-06 — no emitter ever populated it (100% NULL across all runs). `connection_id` auto-fills from per-asset connection tracking; `details` is a JSON string.
- **L2 depth**: typical resting depth is ~4–8 levels per side (liquid BTC windows can show 20+); the writer keeps the top 10 (`l2_levels: 10` since 2026-09-05). Older hive/staging files keep the 20-level schema — export concats old and new files with schema promotion so trailing `*_level_11_*`…`*_level_20_*` extras are tolerated, never dropped.
- **`chainlink_events`**: collected from RTDS topic `crypto_prices_chainlink` only (the `crypto_prices` topic was a rounded duplicate for 6 of 7 assets — HYPE exists only on the chainlink topic). The RTDS payload carries no TWAP/roundId/reportId/sequence, so `report_id` is a reserved (currently NULL) join key and rolling TWAP is a downstream derived metric. `price` is the full-precision value.
- **`trades` wallets**: the CLOB stream does not carry wallets; `maker_wallet`/`taker_wallet`/`wallet` are backfilled from Polymarket's public Data-API (`takerOnly=false`, both legs) at export time and by a second enrichment pass ~15 min later (fills are indexed late by the Data-API). Unattributable fills stay NULL — never guessed. `trade_id` values prefixed `api-` are fills reconciled from the Data-API that the CLOB stream coalesced (12–18% captured on liquid BTC markets, 93% on DOGE).
- **`trades.side`** is the aggressor (taker) side; `price_change.side` on the WS is likewise the taker side.
- **Null-vs-zero**: an empty book side is NULL, never 0. Missing ingredients in `markets_summary` are NULL for the same reason (e.g. `underlying_open` is NULL when no Chainlink tick lands within tolerance of the window boundary — open 10s, close 5s).
- **One-sided books late in the window (ACCEPTED, exchange-side):** in minutes 3–4 of a 5-minute window one outcome's side-pair (`up_bid`+`down_ask`, or `up_ask`+`down_bid`) is frequently empty — 0% of ticks in min 0–2, ~11% in min 3, ~83% in min 4 (2026-09-05 runs). Makers pull quotes ahead of settlement; complementarity on the quoted pair holds exactly (0 violations in ~19k pairs) and fully-empty books never occur. NOT a collector bug — do not "fix" by filling.
- **Official settlement lags ~10 min (ACCEPTED, exchange-side):** CLOB `tokens[].winner` flags appear ~10+ min after window end, so in-run resolutions land as `inferred_nearest` and the 15-min backfill pass promotes them to `polymarket_official` (also filling `minimum_order_size`). `resolution_stuck` events on pre-collection windows are correct gap attribution, recovered by the backfill.
- **`markets_summary` OHLC**: outcome-token open/high/low/close are mid-prices `((bid+ask)/2)` from `book_snapshots_clean` (live-book snapshots only). `avg_spread_*` is the mean `(ask − bid)` over the same snapshots.
- **Resolutions**: `resolution_outcome`/`settlement_price` with `settlement_source='polymarket_official'` come from the CLOB `tokens[].winner` flag (authoritative, queryable indefinitely). Rows from `inferred_nearest` use the nearest stored Chainlink tick and are labelled as such.
- **`markets.reported_volume`** is usually NULL for fresh windows (ACCEPTED, upstream): Gamma only reports volume for markets that had trades at discovery time; 5m windows start empty. Do not treat as a collector defect — use `markets_summary.traded_volume` (derived from stored fills) instead.

## Recommended read path for research

`book_snapshots_clean` (`book_state='live'` only). Querying `book_snapshots_500ms` directly includes `stale`/`resyncing` rows intentionally kept for audit.
