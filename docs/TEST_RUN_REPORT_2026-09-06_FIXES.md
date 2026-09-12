# Test Run Report — 2026-09-06 — boundary-gap & WAL fixes

Three 2×5min runs (wipe local data + delete Kaggle dataset each time), 7 assets, all exit 0, all uploads verified 39/39 remote.

| Run | Raw completeness | Clean | coverage_gap | subscription_failed | resolution_stuck | Notes |
|---|---|---|---|---|---|---|
| 19:17 baseline | 96.23% | 95.77% | 15 | 28 | 7 | pre-fix baseline |
| 19:58 after fixes | 100.0% | 99.25% | 0 | 2 | 0 | second-pass crash found (pyarrow .max) |
| 20:29 final | 99.94% | 99.71% | 0 | 0 | 0 | all fixes in; rollover_started 21 = completed 21 |

Both post-fix runs clear the TEST_PLAN_99pct.md clean gate. Log: `test_run_20260906_202940.log`.

## Changes

1. **Discovery transport (`rollover.py`)** — shared pooled `httpx.AsyncClient` (2s connect timeout) instead of a fresh client per candidate slug; transport errors abort the discovery cycle and fast-retry in ~1s instead of falling through to the legacy CLOB fallback (which burned 15–20s of the lead window and caused the 18:20/18:30 boundary gaps); `rollover_lead_seconds` 60 → 120 (`config/collector.yaml:52`). Result: next-window markets now discovered 50–105s *before* each boundary.
2. **WAL fsync batching (`storage/parquet_writer.py`)** — per-row `open()+fsync()` (10–20ms each on Windows/OneDrive, 14 rows/tick) consumed the entire 500ms snapshot tick budget (scheduler_lag p95 556ms). Now write+flush per row, one `fsync` per flush before WAL truncation. Tick time mean 194ms → 29ms, p95 556ms → 81ms, max 1206ms → 575ms. Process-crash safety unchanged; power-loss window is now one flush interval.
3. **Second-pass enrichment deferral (`storage/export.py`)** — `second_pass_enrich_trades` defers (no data-api queries) when the newest stored trade is <900s old; data-api coverage indexes late, so the inline pass was a guaranteed no-op (4755 rows needed, 0 rewritten in the baseline). Uses `pc.max` (not `ChunkedArray.max`, which does not exist in this pyarrow).
4. **Event hygiene (`enums.py`, `rollover.py`, `collector.py`)** — new `discovery_timeout` event (throttled 1/asset/10s) and `initial_discovery` event; `rollover_started` deduped per target window ts (`rollover_started_for_ts`), fixing 28-started-vs-21-completed accounting; `resolution_stuck` suppressed for pre-warm windows with no collected data.

## Known-remaining (accepted)

- scheduler_lag catch-ups continue (172 in final run) but are now 5–10ms jitter with intact grid; event-loop offload not warranted at current scale.
- 5/8400 snapshots missing (99.94%) — stop-race at run end + mid-window 150s WS recycles.
- Test-window official resolutions resolve via the 15-min cron backfill.

Tests: 108 passed (`pytest tests/`) after all changes.
