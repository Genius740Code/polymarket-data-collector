# AGENTS.md — Real Data Only Policy

> Both `AGENT.md` and `AGENTS.md` are kept at repo root — different tooling checks different filenames. Full policy lives in `AGENT.md`.

This repository collects **real Polymarket CLOB** data only.

## Forbidden
- Synthetic generation (`synthetic_mode`, `synth-` report_ids, `old_best==new_best` fake book_events).
- Interpolating missing snapshots, inventing prices, fabricating wallets/hashes.
- Hiding gaps: every gap must be `book_state='stale'/'resyncing'` + `resync_episodes` + `collector_events`.

## Required
- Prices `0..1` (`validation.py` `validate_price`), null-vs-zero (`book.py`), 500ms UTC grid (`book.py`).
- Markets via Gamma slug (deterministic, `rollover.py`), dual-tracking `RolloverManager`.
- Deduplication + WAL-before-buffer (`parquet_writer.py` `append()`), compaction merge-verify-then-delete (`compaction.py`).
- All writes via `ParquetWriter` atomic tmp+rename, `markets_log` event-sourced (`markets_log.py`).
- Rolling-window Kaggle mirror (`rolling_window: true`): market-data pruned ONLY after verified upload (slowest-lane checkpoint + coverage proof + market-end cutoff, move-to-quarantine, failed move keeps file). Gap evidence (`collector_events`/`resync_episodes`/`markets_log`) never pruned. Quarantine expiry ONLY in `_quarantine/`, gated on upload, with `coverage_gap` row.

See `AGENT.md` for full policy.
