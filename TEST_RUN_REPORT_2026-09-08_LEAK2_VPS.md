# TEST RUN REPORT ADDENDUM 2026-09-08 — P0 leak-hunt session 2, VPS verify

## Verdict: VPS probe FLAT — 5aa2feb fixes verified on the Linux VPS

### 1. Baseline
- `git pull` → `d71a00c` (contains `5aa2feb` leak fixes + `leak_probe.py` + `tests/test_leak2_regression.py`).
- `pytest tests/` → **131 passed**, 2 warnings (backpressure-test UserWarnings, by design) in 40.83s.

### 2. VPS counter-probe (`logs/leak_probe_vps_s3.log`, `logs/rss_external_vps_s3.log`)
- Config: 7 assets, 5m-only, plain prod path. `pm2 stop polymarket-collector` first.
- Window: 2026-09-08 17:49:24 → ~18:01:53 UTC (~770s of planned 780s).
- **External RSS (ps, every 30s): 595076 KB → 595408 KB (+332 KB / ~12 min ≈ +0.03 MB/min).**
  Pre-fix prod rate was ~110 MB/min. Verdict: flat.
- **In-process RSS: 581.3 → 581.4 MB** (per-sample deltas +0.0 / +0.1).
- Suspect counters, all 6 in-process samples: `resync_episodes 0`, `resync_buffers 0`,
  `resync_buffered_msgs 0`, `books 21`, `markets 21`, `books_pending_events 0`,
  `book_seq_entries 0`, `episode_latest/persisted 0`, `coverage_gapped 0`,
  `ws_connected 7`. `chainlink_events` 10773 (seed) → 13850 at T+476s (~+600/sample,
  capped at 20000 by `_note_chainlink_event` — by design, flattens at cap).
  `closed/resolved_cids` step 7→14 on 5m rollover (normal); `heal_inflight` drains 7→1.
- **Honesty note:** the probe tail (samples ~7–8 + `[probe] stopping`/`final`) was lost —
  the launching shell was aborted and the process died during shutdown without flushing
  stdio (block-buffered to file). 6 in-process samples + 27 external RSS readings over the
  full window survive and are all flat. No traceback in log; no OOM/earlyoom kill in
  syslog (`earlyoom` avail% logs only, no kill lines); disk 93% (694M free, above 150M gate).

### 3. Prod restore
- `pm2 restart ecosystem.config.js` (FULL-FILE, so the 1536M cap applies) + `pm2 save`.
- Collector healthy: cursors recovered, Kaggle creds ok, flushes flowing (676/868 rows),
  RSS 353MB at 2m. Watchdog online.
- `polymarket-compact` came online via the full-file restart — returned to `stopped`
  (compact cron off by design) + `pm2 save`.

### 4. Next (operator call)
- Gate passed → 15m re-enable + soak ≥1h + `series_id` purity assert, then 4h.
  Caution: disk 93%, backfill cron spikes ~1.4GB; 21-lane set previously OOMed this box.
- Open items: `markets_log.flush_staging` retry hardening; backfill cron already active.
