# TEST RUN REPORT 2026-09-07 — MULTI-TF (5m regression + 15m lane + prod soak)

Date: 2026-09-07 UTC. Box: Linux VPS (3.9G RAM, 9.7G disk), `.venv` python.
Commits: `37ef52a` (task0, pulled), `810e740` (task3+fix), `cbcd138` (task4),
this report + box-fit config (below).

## Task 2 — static verification: PASS
- pytest **121/121** with `.venv/bin/python` (120 + 1 new prune regression test).
  System `python3` is shadowed by sibling `polymarket-data-collector` editable
  install (3 collection errors) — always use `.venv`. Known footgun, unchanged.
- Probe: **5m/15m/4h ENABLE 7/7, 1h/1d OFF**. `Collector(cfg)` constructs,
  21 lanes, `timeframe_window_sizes()` matches yaml.

## Task 3 — 5m regression: PASS
- `run_2x5min_test.py`: exit 0, lane restricted to 5m, 14 windows,
  **completeness 100.0% / clean 99.95%**, coverage_gaps 0, critical_null false,
  staging 39/39, `gghgg1/polymarket-5m-crypto` ready + remote-verified
  (BTC snapshots 1359 rows, all `BTC-5m`). Log: `test_run_20260907_105741.log`.
- **Found + fixed (test harness, not collector):** interim chunk upload published
  full `collector_events` (229 rows), but the test-buffer prune
  (`retention_hours=0`, mtime fallback for TS datasets) deleted event files
  before the final backfill re-upload rebuilt staging → published
  collector_events 229→2 rows, chainlink ~765→11 rows. Fix:
  `cleanup_local_data(..., skip_datasets=...)` (default None, prod unchanged) +
  test prune skips `("chainlink_events","collector_events","resync_episodes")` +
  `test_prune_skip_datasets_keeps_event_history`.

## Task 4 — 15m lane: PASS (attempt 4)
- Attempts 1–2 killed by **host reboots** (16:29, ~17:40 UTC — infra, not code).
- Attempt 3 reached 1440s/1890s, then **earlyoom SIGKILLed the collector
  (exit -9, syslog)** during the 2nd interim chunk upload: pandas export of the
  growing hive overlapping collection peaks RAM on this box. Partial evidence
  still valid (15m discovery/staging/filter/backfill all worked).
- Attempt 4 with `test_upload_interval_seconds: 3600` (no interim uploads;
  final path unchanged; yaml reverted to 600 after): **exit 0, 14 windows,
  completeness 100.0% / clean 99.92%**, all gap counters 0, staging 39/39
  remote-verified on `gghgg1/polymarket-15m-crypto`. Filter: **7/7 assets pure
  `series_id == "{ASSET}-15m"`** (3614 rows each, zero cross-TF). Published
  `collector_events.parquet` = 535 rows full history (fix validated end-to-end).
  Log: `test_run_20260907_multiTF_15m_v5.log`.
- Caveat: final backfill deferred trades second-pass enrichment (newest trade
  622s < 900s heal threshold) — 15m trades carry first-pass enrichment only.

## Task 5 — prune dry-run gate: PASS
- `cleanup_local_data(..., rolling_window=True, retention_hours=48, dry_run=True)`
  on 744M real 15m data lists nothing (all fresh); `data/` untouched.
  Old-file deletion path covered by `test_tf_filter_and_rolling_prune`.

## Task 6 — prod soak: CONDITIONAL (5m-only per rollout step 1)
- `pm2 start ecosystem.config.js && pm2 save`: collector + watchdog +
  backfill cron online (compact stopped by design).
- **21-lane full set does NOT fit this box:** RSS grows ~110MB/min to 1.1G/10min
  → 1.66G/25min (pm2 max_memory_restart loop); growth ~lane-independent,
  points at a global buffer → **P0 memory-leak hunt filed (open)**. Do NOT just
  raise the cap (earlyoom is the real ceiling on 3.9G).
- **Disk:** raw WS archive grows ~1.8GB/h (disk hit 92% in 25 min).
- Box-fit changes (committed, this box only): `raw_archive.enabled: false`
  (diagnostic-only; re-enable on ≥25GB box), `max_memory_restart` 1G→1536M,
  `timeframes: [5m]` (RUNBOOK §7 rollout step 1; 15m/4h cursors persist).
  No data purged manually; 1.6G of old archive files left in place.
- 5m-only soak ≥1h: collector online, restarts continue on ~10–15min period
  (mem growth persists at 7 lanes — same P0), recovery clean every time
  (SIGINT flush + cursor resume + WAL replay; resolutions flowing).
- Uploads: in-process hourly collector loop **starved by restarts** (timer never
  reaches 3600s — zero `[kaggle:*]` lines from collector). Backfill cron
  (`--reupload --all-lanes`, */15) carries uploads: **5m published 3×
  (21:47/22:02/22:17 UTC), 39 files, remote-verified, no `[prune] WARN`**.
  4h dataset published once by cron (20:19, 39 files) while full lanes were up.
- Prod data sanity: 175k snapshots, 7 assets, 25s fresh, 99.62% live.

## Verdict
5m path clean end-to-end (live → staging → Kaggle → backfill → cron uploads).
15m lane plumbing validated live. Prod runs 5m-only pending P0 leak fix;
15m/4h re-enable per rollout after their soaks. Box needs ≥25GB disk for
full-tilt prod with raw archive.

## Open
1. P0: collector RSS growth (~110MB/min, lane-independent) → heap profile + fix.
2. P1: in-process hourly upload starved by restarts (fix follows from P0).
3. Optional: `markets_log.flush_staging` retry hardening (from task0 session).
4. Housekeeping: purge `data/raw_ws_archive/` old files (needs operator OK);
   published 5m dataset's latest `collector_events` is the 2-row version
   (superseded; full history in earlier versions + 15m dataset).
