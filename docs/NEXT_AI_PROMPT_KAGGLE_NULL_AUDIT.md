# Prompt for the next AI session — Kaggle dataset null/quality audit (paste this verbatim)

```text
Start by syncing the repo and reading the data documentation:

1. cd into the polymarket-data-collector repo and run `git pull origin master`. If the remote is unreachable, continue from the latest local commit.
2. Read `DATA_CARD.md` in full — it lists the dataset's KNOWN, EXPECTED nulls. Do not report those as defects without the expected-context noted; do NOT re-derive them. Also skim `handoff.md` for background.

Your task: download the published Kaggle dataset and run a full null-value and data-quality audit on it. Analyze ONLY what Kaggle serves — do not use local `data/` files as the analysis input (they are working copies, not the published artifact).

STEP 1 — FETCH THE DATASET
- Use the Kaggle CLI: `kaggle datasets download gghgg1/polymarket-5m-crypto --unzip -p kaggle_audit_<UTC date>` (if the CLI is not authenticated, check `~/.kaggle/kaggle.json` or KAGGLE_USERNAME/KAGGLE_KEY env vars; report clearly if credentials are missing and stop).
- Verify exactly 39 parquet files are present: per asset (BTC, ETH, SOL, HYPE, BNB, XRP, DOGE) 5 files (`book_snapshots_500ms`, `book_snapshots_clean`, `book_events`, `trades`, `chainlink_events`) plus 4 global files (`markets.parquet`, `collector_events.parquet`, `resync_episodes.parquet`, `markets_summary.parquet`). List any missing or unexpected files.

STEP 2 — PER-FILE NULL AUDIT (all 39 files)
For every parquet file, load with pandas/pyarrow and compute per column:
- row count, dtype, null count, null %, zero count, zero %, distinct count.
Flag every column into one of three buckets:
  a) CLEAN — 0% null.
  b) EXPECTED NULL — matches a caveat in DATA_CARD.md (e.g. `*_level_9_*`–`*_level_20_*` structurally empty; `ts_source` NULL on ~1–14% of `book_events`; unattributed `maker_wallet`/`taker_wallet`/`wallet` in `trades`; reserved `report_id` in `chainlink_events`; NULL-instead-of-zero empty book sides and missing `markets_summary` ingredients like `underlying_open`).
  c) UNEXPECTED NULL — anything else. These are the findings; give file, column, null %, and a plausible cause.
Also flag NULL-vs-zero violations: any column where an empty/missing value is stored as 0, 0.0, or "" instead of NULL (the dataset policy is NULL, never 0).

STEP 3 — CROSS-FILE & CONTENT CHECKS
- Key integrity: `condition_id` non-null and unique-per-row where expected in `markets.parquet` and `markets_summary.parquet`; every per-asset file's `condition_id`s should exist in `markets.parquet`.
- Duplicates: exact duplicate rows and duplicate primary keys (e.g. `snapshot_id`, `trade_id`).
- Timestamps: null/zero/negative/NaN in `ts_*` columns; monotonicity of `ts_received_ns` in `book_events` and `trades` (they must be sorted by it).
- Per-asset breakdown: report null rates per asset so a single broken asset (e.g. only HYPE missing something) is visible.
- `markets_summary` completeness: % of rows with NULL resolution (`resolution_outcome`), NULL `underlying_open`/`underlying_close`, NULL OHLC, NULL volume; and `book_state` mix in `book_snapshots_clean` (live vs stale vs resyncing).

STEP 4 — REPORT & COMMIT
- Write the full results to `kaggle_null_audit_<UTC date>.md`: per-file table of rows/cols, the three-bucket classification, top unexpected-null findings ranked by severity, per-asset null-rate summary, duplicates, timestamp issues, and a one-paragraph verdict (is the published dataset healthy?).
- Do NOT modify, re-upload, or delete anything on Kaggle. Do NOT fabricate or impute values — a missing value is a finding, not something to fix in the audit.
- `git add` the audit report and `git commit` with the verdict in the message. Do not push unless the operator asks.

Report at the end: total rows/files audited, count of unexpected-null findings (with the top 5), any duplicates or timestamp issues, the markets_summary completeness numbers, and the commit hash.
```
