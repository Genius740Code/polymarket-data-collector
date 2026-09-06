# AGENT.md — Real Data Only Policy

This file is mandatory for all contributors (human or AI agent) working on `polymarket-collector`.

## 0. Golden Rule: Never Use Synthetic / Fake Data

- **Never generate, inject, interpolate or fallback to synthetic data.** No `synthetic`, `mock`, `fake`, `simulated`, `seeded`, `interpolated` rows.
- If a feed is unavailable, the dataset must contain a **gap** (`book_state='stale'` or missing rows) and a `collector_events` / `resync_episodes` entry explaining why. Gaps are truth; fabrications are poison.
- Prior bugs that violated this (e.g. `collector.py:1177 if synthetic_mode` generating `price==up_bid` chainlink, `book_events` with `old==new`) have been removed. Do not reintroduce them even for tests unless the test explicitly asserts synthetic isolation (use `synthetic_mode` guard that is `False` by default and never enabled in prod).
- `synthetic_mode` config (`src/polymarket_collector/config.py:159`) is deprecated, always `False`. New code must not read it.

## 1. Good Data Definition

Good data is:

- **Complete:** 600 snapshots / 5-min window / asset (500ms grid `book.py:23`). `completeness_ratio >0.95` `completeness.py:37`. `book_state='live'` >98% outside resync.
- **Correct:** All prices `0<=price<=1` `validation.py:33`, sizes `>=0`, `book_crossed==false` (<1% true). `up_bid_depth_*` cumulative within N¢ of own best `book.py:28`. L2 levels sorted best-first, tail `null` not `0`.
- **Traceable:** Every row has `ts_source` + `ts_received_ns`, `condition_id`→`markets_latest` `markets_log.py:206`, token id maps to `up/down_token_id` `book.py:332`. Duplicates deduped `(asset,condition_id,bucket)` `parquet_writer.py:449`.
- **Honest gaps:** Disconnects logged `resync_episodes` `resync.py:87` with `gap_duration_ms`, `collector_events ws_disconnected/sequence_gap/coverage_gap`. Never hide gaps with interpolation.

## 2. Kaggle & Local Handling

- Kaggle dataset `gghgg1/polymarket-5m-crypto` is cumulative. Never overwrite non-empty staging with empty `export.py:352` monotonic guard. Local `data/` is source of truth; Kaggle is mirror.
- Deleting data: remove `data/book_snapshots_500ms`, `data/trades`, `data/markets_log`, `data/_wal`, `data/kaggle_staging` only via explicit `rm -rf` or `export.py:cleanup_local_data` after `dataset_status==ready`. Never `age` delete.

## 3. Chainlink / Settlement

- Chainlink events are **real WS** `wss://ws-live-data.polymarket.com` or empty. Do not derive `price==up_bid`. If WS unavailable, write 0 rows with empty file schema, not synthetic.
- Settlement `settlement_report_id/price/tx_hash` `schemas.py:46` only when `fetch_settlement` `chainlink.py:104` finds on-chain or `inferred_nearest` flagged. Currently `unknown` is correct until resolver wired.

## 4. Enforcement

- CI must fail if any writer path creates `source='synthetic'` or `report_id startswith 'synth-'` or `trade fee` fabricated without `fee_is_estimated=true`.
- `pytest` must include `tests/test_chaos.py` disconnect / sequence-gap / malformed injections `PLAN.md:19`.
- Any agent ignoring this file is violating repo policy.

