# Prompt for the next AI session — finish + RUN the multi-timeframe 24/7 runner (paste this verbatim)

Start by syncing the repo and reading context:

1. `cd` into the polymarket-data-collector repo and run `git pull origin master`.
2. Read `TEST_RUN_REPORT_2026-09-06_FIXES.md` (today's earlier fixes), `NEXT_AI_PROMPT_MULTI_TF_RUNNER.md` (this file) and skim `plan.md` §1.1/§7 and `DATA_CARD.md`.

## What is ALREADY DONE (commit by previous session — do NOT redo)

- **Single-process multi-timeframe runner**: `CollectorConfig.timeframes: ["5m","15m","4h"]` (config.py), `RolloverManager` with per-`(asset, tf)` lanes (`states[(asset, tf)]`, per-lane `MarketDiscovery` with scaled poll cadence 2s/5s/30s and lead max(120s, ws/10)) — rollover.py.
- `check_and_roll_all` drives all enabled lanes; `active_markets` returns the union; `state_for_market(m)` resolves a market's lane via `window_size_seconds`.
- Cursor store keyed `(asset, window_label)` with legacy migration (cursor_store.py); crash recovery per lane (collector.py `_recover_from_cursor`).
- Kaggle loop uploads EVERY enabled lane hourly to its own dataset (`config.kaggle.datasets`), staging per lane at `kaggle_staging/{tf}/`, lane-filtered exports (`series_id == "{ASSET}-{tf}"`), chainlink_events shared across lanes by design.
- **Real prune**: `cleanup_local_data` in rolling-window mode deletes local parquet files only when every condition_id in the file ended before `checkpoint - local_retention_hours` (48h default) AND the upload was verified; ts-based fallback for chainlink/collector_events; cumulative mode still never deletes.
- **Probe gate**: `python -m polymarket_collector.verify_gate --probe-timeframes` (probed 2026-09-06: 5m/15m/4h live on 7/7 assets → ENABLE; 1h/1d DO NOT EXIST on Gamma — keep OFF until the probe says otherwise, including alternate slug patterns already checked).
- Test harness: `python run_2x5min_test.py --timeframe 15m` and `--test-timeframe` CLI flag.
- Tests: 114 passed including new `tests/test_multi_timeframe.py`.
- `config/collector.yaml` already has `timeframes: [5m, 15m, 4h]`, `kaggle.rolling_window: true`, `local_retention_hours: 48`.

## YOUR TASKS (in order — commit after each)

### 1. pm2 ecosystem + runbook (code)
- Update `ecosystem.config.js` header comment for the multi-TF single process (it stays ONE collector process + watchdog + 15-min resolution-backfill cron; no per-TF processes). Ensure the backfill re-uploads each enabled lane: extend `resolution_backfill.main` with `--all-lanes` that loops `cfg.timeframes` calling `reupload_kaggle(timeframe=tf)` (default keeps 5m-only behavior).
- Write `RUNBOOK.md`: GCP e2-medium (4GB) recommended / e2-small minimum, 25GB disk, Ubuntu 24.04, `pm2 start ecosystem.config.js && pm2 startup && pm2 save`, kaggle.json setup, the probe gate as step 0, log locations, and the rollout order (5m first, then 15m, then 4h — each after its own soak).

### 2. Static verification (no live run yet)
- `python -m pytest tests/ -q` → must be all green.
- `python -m polymarket_collector.verify_gate --probe-timeframes` → confirm 5m/15m/4h still ENABLE (re-probe; if 1h/1d appear live, note it but keep them OFF this session).
- `python -m polymarket_collector.cli --config config/collector.yaml` is NOT run yet — first check `Collector(cfg)` constructs and `cfg.timeframe_window_sizes()` matches the yaml.

### 3. Live validation — 5m regression (gate: exit 0, completeness ≥99%, coverage_gaps 0)
- `python run_2x5min_test.py` (2×5min, wipes local data + deletes/recreates the 5m Kaggle dataset). This proves the multi-lane refactor did not regress the proven 5m path. Watch the log for `[test-mode:real] lane restricted to 5m`.
- Check `data/test_analysis.json`: `snapshot_completeness_pct >= 99`, `clean >= 99`, `collector_events_by_type` has no `coverage_gap`/`subscription_failed` rise.

### 4. Live validation — 15m lane (gate: same, 2×15min ≈ 40 min)
- `python run_2x5min_test.py --timeframe 15m` — validates the multi-lane discovery/staging/upload on the 15m dataset `gghgg1/polymarket-15m-crypto`.
- Verify per-lane staging is actually filtered: open `data/kaggle_staging/15m/<dataset>/BTC_book_snapshots_500ms.parquet` and assert every `series_id == "BTC-15m"` (same for one more asset). Cross-TF rows in a lane's staging is a P0 bug — fix the filter, do not upload.
- 4h lane: do NOT live-test interactively (2×4h = 8h+); instead start it and verify discovery/subscription only (`market_added` events within the 4h lead window), then leave it to the overnight run.

### 5. Prune dry-run on real data (SAFETY GATE before prod)
- With a few hours of local data present, call `cleanup_local_data(data_dir, timeframe_labels=["5m"], rolling_window=True, retention_hours=48, dry_run=True)` and confirm it only lists files whose markets all ended >48h ago. NOTHING may be deleted in dry-run. Only then is rolling_window prod-safe.

### 6. Production run + commit
- `pm2 start ecosystem.config.js && pm2 save` on the Linux VPS (or a background run on Windows for the overnight soak).
- After ≥1h: confirm all three lane datasets uploaded (`[kaggle:5m]`, `[kaggle:15m]`, `[kaggle:4h]` success lines) and no `[prune] WARN`.
- Write `TEST_RUN_REPORT_<UTC date>_MULTI_TF.md` with: per-lane completeness, probe output, prune dry-run output, any issues. `git add -A && git commit` with the verdict in the message. Do not push unless the operator asks.

## Hard rules (carry over from AGENT.md/handoff.md)
- Never fabricate or impute market data; a missing lane stays off.
- Never delete local data except via the verified-upload prune path (rolling_window + retention).
- If a live test fails, diagnose from `test_run_*.log` + `collector_events` — do not disable checks to make it pass.
- Do not enable 1h/1d lanes without a passing probe.
