# Data Card — Polymarket 5m Crypto (gghgg1/polymarket-5m-crypto)

Dataset: 7 assets (BTC, ETH, SOL, HYPE, BNB, XRP, DOGE) × Polymarket 5-minute Up/Down crypto markets, collected live from the CLOB market WebSocket + REST, with official resolutions and Chainlink RTDS price ground truth.

## Files (39 per version)

Per asset (`{ASSET}_*.parquet` × 5): `book_snapshots_500ms`, `book_snapshots_clean`, `book_events`, `trades`, `chainlink_events`.
Global: `markets.parquet`, `collector_events.parquet`, `resync_episodes.parquet`, and the derived `markets_summary.parquet` (one row per condition_id: resolution, underlying open/close from Chainlink ticks at the window boundaries, outcome-token mid OHLC from clean snapshots, traded volume / fill count / unique traders incl. reconciled `api-` fills, average spread).

## Known caveats (read before querying)

- **`book_events` / `trades` sort order**: sort by `ts_received_ns`. `ts_source` is best-effort — the CLOB market channel does not always carry an event timestamp, so `ts_source` is NULL on ~1–14% of `book_events` rows.
- **No sequence numbers**: the CLOB market channel sends none, so `book_events` and `trades` carry no `sequence_number` column (dropped 2026-09-05 — it was 100% NULL). Gap detection is not possible from the wire; books are healed via full `book` snapshots on (re)subscribe and REST `/book`.
- **L2 depth**: observed book depth is ~4–8 levels per side, so snapshot columns `*_level_9_*` through `*_level_20_*` are structurally empty (NULL). Levels 1–8 are populated. The 20-level schema is retained for forward compatibility.
- **`chainlink_events`**: collected from RTDS topic `crypto_prices_chainlink` only (the `crypto_prices` topic was a rounded duplicate for 6 of 7 assets — HYPE exists only on the chainlink topic). The RTDS payload carries no TWAP/roundId/reportId/sequence, so `report_id` is a reserved (currently NULL) join key and rolling TWAP is a downstream derived metric. `price` is the full-precision value.
- **`trades` wallets**: the CLOB stream does not carry wallets; `maker_wallet`/`taker_wallet`/`wallet` are backfilled from Polymarket's public Data-API (`takerOnly=false`, both legs) at export time and by a second enrichment pass ~15 min later (fills are indexed late by the Data-API). Unattributable fills stay NULL — never guessed. `trade_id` values prefixed `api-` are fills reconciled from the Data-API that the CLOB stream coalesced (12–18% captured on liquid BTC markets, 93% on DOGE).
- **`trades.side`** is the aggressor (taker) side; `price_change.side` on the WS is likewise the taker side.
- **Null-vs-zero**: an empty book side is NULL, never 0. Missing ingredients in `markets_summary` are NULL for the same reason (e.g. `underlying_open` is NULL when no Chainlink tick lands within 5s of the window boundary).
- **`markets_summary` OHLC**: outcome-token open/high/low/close are mid-prices `((bid+ask)/2)` from `book_snapshots_clean` (live-book snapshots only). `avg_spread_*` is the mean `(ask − bid)` over the same snapshots.
- **Resolutions**: `resolution_outcome`/`settlement_price` with `settlement_source='polymarket_official'` come from the CLOB `tokens[].winner` flag (authoritative, queryable indefinitely). Rows from `inferred_nearest` use the nearest stored Chainlink tick and are labelled as such.

## Recommended read path for research

`book_snapshots_clean` (`book_state='live'` only). Querying `book_snapshots_500ms` directly includes `stale`/`resyncing` rows intentionally kept for audit.
