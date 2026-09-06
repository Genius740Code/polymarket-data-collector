# AGENTS.md — Real Data Only Policy

> Symlink to AGENT.md — both filenames checked by different tooling.

This repository collects **real Polymarket CLOB** data only.

## Forbidden
- Synthetic generation (`synthetic_mode`, `synth-` report_ids, `old_best==new_best` fake book_events).
- Interpolating missing snapshots, inventing prices, fabricating wallets/hashes.
- Hiding gaps: every gap must be `book_state='stale'/'resyncing'` + `resync_episodes` + `collector_events`.

## Required
- Prices `0..1` `validation.py:33`, null-vs-zero `book.py:481`, 500ms UTC grid `book.py:23`.
- Markets via Gamma slug `rollover.py:164` deterministic, dual-tracking `RolloverManager`.
- Deduplication `parquet_writer.py:428,449`, WAL-before-buffer `parquet_writer.py:199`.
- All writes via `ParquetWriter` atomic tmp+rename, `markets_log` event-sourced `markets_log.py:21`.

See `AGENT.md` for full policy.
