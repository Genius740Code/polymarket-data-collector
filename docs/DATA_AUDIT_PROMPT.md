# DATA_AUDIT_PROMPT — find data issues, trace them to code bugs

Copy-paste this prompt into a fresh AI session to audit collection health.
It finds anomalies **in the data first**, then traces each one back to the
**code bug that caused it**. Data symptoms are evidence; code is the diagnosis.

---

**Role:** You are a data-quality auditor for the `polymarket-data-collector`
repo (real Polymarket CLOB data). Your job: find anomalies **in the data
first**, then trace each one back to the **code bug that caused it**.

**0. Mandatory policy (read first):** `AGENT.md` + `AGENTS.md` at repo root —
real-data-only. Never invent, interpolate, or delete data to hide gaps. Gaps
must be `book_state='stale'` + `resync_episodes` + `collector_events`, never
silently dropped. `data/` is gitignored and is the source of truth; Kaggle
staging under `data/kaggle_staging/` is a mirror.

**1. Environment constraints (the box is a 3.9GB VPS — violate these and you
OOM it):**

- Metadata-only scans first (`parquet.read_metadata`, file counts/mtimes,
  `ls`). Never full-hive concats.
- Single-file reads only, one small file at a time (newest file per
  partition), select minimal columns.
- Keep every tool output SMALL (counts, tails, value-counts — never full
  tables/logs).

**2. Data checks (run all, per asset in BTC/ETH/SOL/HYPE/BNB/XRP/DOGE, per
date partition):**

- `data/book_snapshots_500ms/date=*/asset=*`: row counts vs 500ms-grid
  expectation; `book_state` live/stale mix; longest 100%-stale run and its
  start time; `book_crossed` rate; price bounds 0..1; null-vs-value on
  top-of-book and `up/down_book_age_ms`; 500ms cadence continuity (row counts
  mask dead-book writes — check the flag, not the count).
- `data/trades`, `data/book_events`: partitions/files present for every asset
  that has snapshots? Time-range coverage per asset (min/max `ts_source`);
  `fee` always 0.0 vs NULL and `fee_is_estimated` tri-state consistency
  (False=reported, True=derived, NULL=N/A); `market_id` must never be a hex
  condition_id (E1); `trade_id`s starting `api-` must be deterministic across
  rebuilds.
- `data/resync_episodes`, `data/collector_events`: every stale window in
  snapshots MUST have a matching episode +
  `ws_disconnected`/`ws_reconnect_attempt`/`ws_reconnected` events. Orphan
  stale = logging bug. Check `gap_duration_ms` nulls.
- `data/chainlink_events`: coverage start/end vs snapshot span;
  dupes/out-of-order; any `report_id` starting `synth-` (policy violation).
- `data/_wal`: non-empty files = unflushed rows (fine if fresh, incident if
  old). `data/kaggle_staging/*`: any `*.tmp*` files (aborted-worker litter
  that the Kaggle folder-upload would ship as junk); `*_clean` row counts vs
  raw (a near-empty clean file means the raw feed is ~all stale).
- Liveness: `data/heartbeat.json` age, `pm2 list` status, per-asset `[ws:*]`
  recency in `logs/collector-out-*.log`, any `Traceback`/`NameError` since
  the last deploy.

**3. Trace each anomaly to code (the actual deliverable):**

- For every data issue, name the writer path
  (`src/polymarket_collector/collector.py`, `storage/export.py`,
  `storage/streaming.py`, `storage/parquet_writer.py`, `resync.py`,
  `book.py`, `storage/clean_view.py`, `storage/cursor_store.py`) and the
  exact lines + mechanism (e.g. "reconnect path references unbound variable
  → task dies on first disconnect → books stale forever, snapshots keep
  emitting, zero episodes").
- Distinguish: (a) code bug, (b) exchange-side reality honestly recorded
  (e.g. CLOB 404 "no orderbook", thin-book nulls — verify with one live REST
  probe before claiming), (c) operational (disk, OOM, mid-tick edit). Only
  (a) gets a fix proposal.
- Verify each suspected bug by reading the code (AST/scope check for
  NameErrors, gate-condition logic for flags) and by the cheapest possible
  test or log evidence — never by assumption.

**4. Output format — ranked list, severity first:**

`[critical/medium/low] one-line symptom → evidence (counts, timestamps, file
paths) → root-cause file:line + mechanism → fix sketch (no code yet) → how
to verify.` End with: issues needing immediate action vs watch items.

**5. Hard rules:** do not edit source, do not delete anything under `data/`,
do not commit/push, do not `git add -A`, do not run heavy jobs mid
Kaggle-tick (check `logs/` for active `Step N:`/upload lines first).

---

## Resolved-issue archive (do not re-report; regress-test instead)

- 2026-09-15 — Shard-loop `asset`/`resync_id` NameErrors killed WS tasks on
  first reconnect (7/7 lanes stale, zero episodes). Fixed per-city reconnect
  in `collector.py:_run_shard_loop`; guard test
  `tests/test_outage_discovery_events.py`.
- 2026-09-15 — Zero-fee reconcile produced `fee=NULL` on `api-` rows
  (rate gate required flag False; E7 rows carry NULL). Fixed via
  `_fee_rate_vote()` in `storage/export.py`; guard test in
  `tests/test_r_fixes.py`.
- 2026-09-15 — Kaggle folder-upload shipped `*.tmp*` worker litter as dataset
  files. Fixed with pre-upload janitor sweep in `_upload_kaggle_folder`.
- 2026-09-15 — `export.py` wrote hex `condition_id` into `market_id` (E1) on
  reconciled rows. Fixed to NULL.
- Known non-bugs: `book_age_ms` NULL = side never got an exchange frame (E6,
  honest); `asset` in-file column + `asset=` partition breaks naive
  Hive reads — use `storage/parquet_io.py` file-only readers; BTC churn with
  CLOB 404s is exchange-side thinness, honestly stale-flagged.
