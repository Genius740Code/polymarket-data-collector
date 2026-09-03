/**
 * PM2 ecosystem — BTC/ETH/SOL 5-min Polymarket collector (PLAN.md v3)
 *
 * Two long-running processes (separate per §17A):
 *   1. polymarket-collector — main asyncio collector (BTC/ETH/SOL, 500ms snapshots, rollover, resync, cursor store)
 *   2. polymarket-watchdog  — separate heartbeat monitor + alerting (must not share process with collector)
 *
 * Optional cron: polymarket-compact — daily Parquet compaction (temp + atomic rename, §10A)
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
 *
 * (§18 gate) Run verification before live:
 *   .venv/bin/polymarket-verify-gate --live --config config/collector.yaml
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
      max_memory_restart: '800M',
      restart_delay: 1000,
      exp_backoff_restart_delay: 100,
      kill_timeout: 10000,          // SIGINT → give collector time to flush + persist cursor (§1B)
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
  ],
};
