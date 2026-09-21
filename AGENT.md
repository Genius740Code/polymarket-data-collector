# AGENT.md — Real Data Only Policy

This file is mandatory for all contributors (human or AI agent) working on `polymarket-collector`.

## 0. Golden Rule: Never Use Synthetic / Fake Data

- **Never generate, inject, interpolate or fallback to synthetic data.** No `synthetic`, `mock`, `fake`, `simulated`, `seeded`, `interpolated` rows.
- If a feed is unavailable, the dataset must contain a **gap** (`book_state='stale'` or missing rows) and a `collector_events` / `resync_episodes` entry explaining why. Gaps are truth; fabrications are poison.
- Prior bugs that violated this (e.g. `collector.py:1177 if synthetic_mode` generating `price==up_bid` chainlink, `book_events` with `old==new`) have been removed. Do not reintroduce them even for tests unless the test explicitly asserts synthetic isolation (use `synthetic_mode` guard that is `False` by default and never enabled in prod).
- `synthetic_mode` config (`src/polymarket_collector/config.py:159`) is deprecated, always `False`. New code must not read it.

## 1. Good Data Definition

Good data is:

- **Complete:** 600 snapshots / 5-min window / asset (500ms grid `book.py` `snapshot_interval_ms`). `completeness_ratio >0.95` `completeness.py`. `book_state='live'` >98% outside resync.
- **Correct:** All prices `0<=price<=1` `validation.py` (`validate_price`), sizes `>=0`, `book_crossed==false` (<1% true). L2 levels sorted best-first, tail `null` not `0`.
- **Traceable:** Every row has `ts_source` + `ts_received_ns`, `condition_id`→`markets_latest` (`markets_log.py`), token id maps to `up/down_token_id` (`book.py`). Duplicates deduped `(asset,condition_id,bucket)` (`parquet_writer.py` `_dedup_key`/WAL-before-buffer in `append()`).
- **Honest gaps:** Disconnects logged `resync_episodes` (`resync.py`) with `gap_duration_ms`, `collector_events ws_disconnected/sequence_gap/coverage_gap`. Never hide gaps with interpolation.

## 2. Kaggle & Local Handling

- Kaggle dataset `gghgg1/polymarket-5m-crypto` is a ROLLING WINDOW in production
  (`config/collector.yaml` `rolling_window: true`, `local_retention_hours: 4`).
  Each lane upload contains the retention window, not full history — history
  lives in older Kaggle versions (`delete_old_versions=False`). The monotonic
  staging guard is therefore disabled in rolling mode (`export.py`
  `check_monotonic=not rolling_window`); the gap-evidence guard
  (`collector_events` / `resync_episodes` never shrink) stays enforced.
- Local `data/` is source of truth for the retention window; Kaggle is the
  cumulative mirror across versions. Never overwrite non-empty staging with
  empty (pre-upload validation gate).
- Deleting data: market-data files (`book_snapshots_500ms`,
  `book_snapshots_clean`, `book_events`, `trades`, `chainlink_events`) ONLY via
  `export.py:cleanup_local_data` after a VERIFIED upload, gated by (a) slowest-lane
  checkpoint, (b) per-dataset staging-freshness coverage proof, (c) market-end
  cutoff — unknown condition → keep. The prune MOVES to `data/_quarantine/`
  (never direct unlink; a failed move KEEPS the file).
- Gap evidence is NEVER pruned: `collector_events`, `resync_episodes`,
  `markets_log` / `markets_latest` stay local forever.
- Quarantine expiry is the ONLY age/size deletion allowed, and ONLY inside
  `data/_quarantine/` (already-uploaded or unreadable review buffer), gated on
  verified upload, bounded by `quarantine_retention_hours` /
  `quarantine_max_bytes`, via the single `quarantine.reap_quarantine`
  implementation. Every expiry batch emits a `coverage_gap`
  `collector_events` row. Never age-delete the live hive.
- Compaction (`storage/compaction.py`) merges + VERIFIES (output footer ==
  rows written == rows read) then deletes consumed inputs; any flush/close/
  verification failure aborts with inputs untouched.
- Diagnostic `raw_archive/` (`storage/raw_archive.py`) is DISABLED in prod and
  its age-prune has no callers (dormant); if ever enabled for diagnostics, the
  prune logs every batch. All other `*.unlink()` sites are staging `*.tmp` /
  worker-tmp cleanup or derived-view (`book_snapshots_clean`) rebuilds — the
  primary hive is never unlinked except via the verified-upload prune above.

## 3. Chainlink / Settlement

- Chainlink events are **real WS** `wss://ws-live-data.polymarket.com` or empty. Do not derive `price==up_bid`. If WS unavailable, write 0 rows with empty file schema, not synthetic.
- Settlement fields in `schemas.py` (`COLLECTOR_EVENTS_SCHEMA` / chainlink schemas) only when `fetch_settlement` (`chainlink.py`) finds on-chain or `inferred_nearest` flagged. Currently `unknown` is correct until resolver wired.
- Book `book_hash` (`book.py` `_well_formed_hash` / `_note_frame_hash`) is attestation-only: format-checked on capture, never fabricated, never used to overwrite book contents. Exchange hash algorithm is not published, so content-verification is documented as out of scope.

## 4. Enforcement

- CI must fail if any writer path creates `source='synthetic'` or `report_id startswith 'synth-'` or `trade fee` fabricated without `fee_is_estimated=true`.
- `pytest` must include `tests/test_chaos.py` disconnect / sequence-gap / malformed injections `PLAN.md:19`.
- Any agent ignoring this file is violating repo policy.

