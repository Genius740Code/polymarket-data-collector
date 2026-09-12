/**
 * PM2 ecosystem — multi-timeframe Polymarket collector (single process).
 *
 * ONE collector process drives ALL enabled timeframe lanes (5m/15m/4h per
 * config/collector.yaml `timeframes:`) — there are deliberately NO per-TF
 * processes. Each (asset, tf) lane has its own current/next market pair,
 * discovery cadence, cursor and Kaggle dataset, all inside the one asyncio
 * loop (see NEXT_AI_PROMPT_MULTI_TF_RUNNER.md).
 *
 * Three PM2 entries:
 *   1. polymarket-collector — main asyncio collector (7 assets × enabled lanes,
 *      500ms snapshots, per-lane rollover, resync, per-lane cursor store)
 *   2. polymarket-watchdog  — separate heartbeat monitor + alerting (must not share process with collector)
 *   3. polymarket-resolution-backfill (cron every 15 min) — official-outcome
 *      backfill + `--reupload --all-lanes` so EVERY enabled lane's Kaggle
 *      dataset gets a fresh version carrying resolutions.
 *
 * Optional cron: polymarket-compact — daily Parquet compaction (temp + atomic rename, §10A)
 *
 * Rollout order (each lane soaks before the next is enabled):
 *   1. timeframes: [5m]            → soak ≥24h, completeness ≥99%, gaps 0
 *   2. timeframes: [5m, 15m]       → soak ≥24h, both datasets uploading
 *   3. timeframes: [5m, 15m, 4h]   → full 24/7 runner
 * New lanes ONLY after: python -m polymarket_collector.verify_gate --probe-timeframes
 * reports ENABLE for that lane (1h/1d do NOT exist on Gamma — keep OFF).
 *
 * Usage:
 *   pm2 start ecosystem.config.js
 *   pm2 logs                    # tail all
 *   pm2 logs polymarket-collector
 *   pm2 monit                   # dashboard
 *   pm2 save                    # save process list for resurrect after reboot
 *   pm2 startup                 # (run the command it prints, then) pm2 save
 *
 * Requires:
 *   python3 -m venv .venv && .venv/bin/pip install -e .
 *   cp config/collector.example.yaml config/collector.yaml  # edit if needed
 *   ~/.kaggle/kaggle.json with API credentials (chmod 600)
 *
 * (§18 gate) Run verification before live:
 *   python -m polymarket_collector.verify_gate --probe-timeframes --config config/collector.yaml
 *   python -m pytest tests/ -q
 */

const path = require('path');
const cwd = __dirname; // /home/fese/polymarket-collector
const python = path.join(cwd, '.venv', 'bin', 'python');

module.exports = {
  apps: [
    {
      name: 'polymarket-collector',
      cwd,
      // PM2 default interpreter is node; override to run python directly.
      // Using `script: python` + `args: -m <module>` + `interpreter: 'none'` makes PM2 fork the binary as-is.
      script: python,
      args: '-m polymarket_collector.cli --config config/collector.yaml',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      // BOX-FIT 2026-09-07 (3.9GB box): 21 lanes peak ~1.1GB (startup + export
      // overlap); 1G caused restart-loop. 1.5G cap leaves ~2GB headroom before
      // earlyoom. Revisit if RSS plateaus above 1.5G (leak hunt, not cap raise).
      // 2026-09-09 28 lanes: hourly Kaggle export (4×39 files + clean_view
      // ~300k rows + wallet backfill) spiked over 1536M → pm2 SIGKILLed mid-
      // export 08:15 (10s kill_timeout too short for in-flight export).
      // 2048M fits the 3.9GB box (2G collector + 0.74G opencode + 0.52G
      // backfill transient ≈ 3.3G peak); 60s kill lets stop() flush + cursor.
      max_memory_restart: '2048M',
      restart_delay: 1000,
      exp_backoff_restart_delay: 100,
      kill_timeout: 60000,          // SIGINT → give collector time to flush + persist cursor (§1B)
      wait_ready: false,
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      out_file: path.join(cwd, 'logs', 'collector-out.log'),
      error_file: path.join(cwd, 'logs', 'collector-error.log'),
      merge_logs: false,
      env: {
        PYTHONUNBUFFERED: '1',
        // Uncomment to override config path via env:
        // POLYMARKET_COLLECTOR_CONFIG: path.join(cwd, 'config', 'collector.yaml'),
      },
    },
    {
      name: 'polymarket-watchdog',
      cwd,
      script: python,
      args: '-m polymarket_collector.watchdog.cli --config config/collector.yaml',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '300M',
      restart_delay: 1000,
      exp_backoff_restart_delay: 100,
      kill_timeout: 5000,
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      out_file: path.join(cwd, 'logs', 'watchdog-out.log'),
      error_file: path.join(cwd, 'logs', 'watchdog-error.log'),
      env: {
        PYTHONUNBUFFERED: '1',
      },
    },

    // --- optional: daily compaction (cron_restart) ---------------------------------
    // PM2 cron: restart the script on schedule even though it exits. The script itself
    // is idempotent (merges small flushed files → larger partitions atomically).
    // Disabled by default; enable by setting `cron_restart` or run manually:
    //   .venv/bin/polymarket-compact --data-dir ./data
    //
    {
      name: 'polymarket-compact',
      cwd,
      script: python,
      args: '-m polymarket_collector.storage.compaction --data-dir ./data',
      interpreter: 'none',
      exec_mode: 'fork',
      autorestart: false,
      cron_restart: '0 3 * * *',   // 03:00 UTC daily
      time: true,
      out_file: path.join(cwd, 'logs', 'compact-out.log'),
      error_file: path.join(cwd, 'logs', 'compact-error.log'),
    },

    // --- B-7: resolution backfill — every 15 minutes --------------------------------
    // Upgrades ended markets (active/closed/unknown) to the OFFICIAL outcome via
    // the CLOB tokens[].winner flag, append-only + atomic compact. Idempotent:
    // already-resolved markets are skipped, unsettled ones retry next run.
    // --reupload --all-lanes pushes a fresh Kaggle version for EVERY enabled
    // timeframe lane (5m/15m/4h datasets), not just the 5m default.
    // 2026-09-10 OOM: --skip-onchain — the :00 full run (500 receipts + full
    // trades-hive reads) still spiked to 1.9GB and died. Wallets keep healing
    // via export-time first pass; on-chain resumes after the export diet.
    // (Also: single exporter per earlier note — no --reupload here.)
    // 2026-09-11 OOM: --skip-enrich — the :00 second-pass full trades-hive
    // read co-spiked with the collector export (death 00:01). Resolutions
    // (cheap CLOB GETs) continue every 15 min; enrichment rides the export.
    {
      name: 'polymarket-resolution-backfill',
      cwd,
      script: python,
      args: '-m polymarket_collector.resolution_backfill --config config/collector.yaml --skip-onchain --skip-enrich',
      interpreter: 'none',
      exec_mode: 'fork',
      autorestart: false,
      cron_restart: '*/15 * * * *',   // every 15 minutes
      // BOX-FIT 2026-09-08: staging build (full-hive pandas read) exceeds pm2's
      // 1G default -> SIGINT restart-loop every ~30s, uploads never complete.
      // Same 1.5G reasoning as the collector entry. Numeric bytes: pm2 ignores
      // the '1536M' string form on cron apps (autorestart:false quirk).
      max_memory_restart: 1610612736,
      time: true,
      out_file: path.join(cwd, 'logs', 'resolution-backfill-out.log'),
      error_file: path.join(cwd, 'logs', 'resolution-backfill-error.log'),
    },
  ],
};
