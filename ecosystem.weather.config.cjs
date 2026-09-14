/**
 * PM2 ecosystem — weather collectors (HIGH + LOW). SEPARATE file from
 * ecosystem.config.js so `pm2 start ecosystem.weather.config.js` only ADDS
 * apps and never touches the BTC/crypto runner.
 *
 * Apps:
 *   polymarket-weather-high — config/collector.weather.high.yaml -> ./data-weather-high
 *   polymarket-weather-low  — config/collector.weather.low.yaml  -> ./data-weather-low
 *   watchdogs for each (separate heartbeat/data dir).
 *
 * Requires cli_weather.py (phase 1 build). DO NOT point these at cli.py:
 * stock discovery looks for {btc}-updown slugs and would poll Gamma uselessly.
 *
 * Start:  pm2 start ecosystem.weather.config.js
 * Logs:   pm2 logs polymarket-weather-high / polymarket-weather-low
 * Stop:   pm2 stop ecosystem.weather.config.js  (crypto runner keeps running)
 */

const path = require('path');
const fs = require('fs');
const cwd = __dirname;
const python = path.join(cwd, '.venv', 'bin', 'python');

// Local hive lives on /dev/shm tmpfs (850 MB/s dsync) instead of the 1TB
// HDD (3.1 MB/s dsync, journal-bound): at 51-city scale the sync
// parquet/WAL/cursor fsync storms wedged the asyncio loop in D-state
// (heartbeat 5s->60s, kaggle 600s sleep never elapsed, no uploads).
// Kaggle is the durable archive (rolling_window); local is a 12h staging
// window, so tmpfs volatility (reboot wipes) is by design. Recreated here
// so `pm2 start`/`pm2 resurrect` self-heals after a reboot wiped /dev/shm.
for (const [link, target] of [
  ['data-weather-high', '/dev/shm/pw-weather-high'],
  ['data-weather-low', '/dev/shm/pw-weather-low'],
]) {
  try {
    fs.mkdirSync(target, { recursive: true });
    const linkPath = path.join(cwd, link);
    if (!fs.existsSync(linkPath)) fs.symlinkSync(target, linkPath);
  } catch (e) { console.error(`[weather] shm setup failed for ${link}: ${e.message}`); }
}

function watchdogApp(name, configFile, outLog, errLog) {
  return {
    name,
    cwd,
    script: python,
    args: `-m polymarket_collector.watchdog.cli --config ${configFile}`,
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
    out_file: path.join(cwd, 'logs', outLog),
    error_file: path.join(cwd, 'logs', errLog),
    env: { PYTHONUNBUFFERED: '1' },
  };
}
function weatherApp(name, configFile, outLog, errLog) {
  return {
    name,
    cwd,
    script: python,
    args: `-m polymarket_collector.cli_weather --config ${configFile}`,
    interpreter: 'none',
    exec_mode: 'fork',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1000M',
    restart_delay: 5000,
    exp_backoff_restart_delay: 100,
    kill_timeout: 60000,
    wait_ready: false,
    time: true,
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    out_file: path.join(cwd, 'logs', outLog),
    error_file: path.join(cwd, 'logs', errLog),
    merge_logs: false,
    // KAGGLE_API_TOKEN passed through from `pm2 start` env (never hardcoded).
    // Falls back to ~/.kaggle/kaggle.json when env is absent.
    env: {
      PYTHONUNBUFFERED: '1',
      ...(process.env.KAGGLE_API_TOKEN
        ? { KAGGLE_API_TOKEN: process.env.KAGGLE_API_TOKEN }
        : {}),
    },
  };
}
function compactApp(name, dataDir, outLog, errLog, cron) {
  return {
    name,
    cwd,
    script: python,
    args: `-m polymarket_collector.storage.compaction --data-dir ${dataDir}`,
    interpreter: 'none',
    exec_mode: 'fork',
    autorestart: false,
    cron_restart: cron,
    watch: false,
    kill_timeout: 30000,
    time: true,
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    out_file: path.join(cwd, 'logs', outLog),
    error_file: path.join(cwd, 'logs', errLog),
  };
}

module.exports = {
  apps: [
    weatherApp(
      'polymarket-weather-high',
      'config/collector.weather.high.yaml',
      'weather-high-out.log',
      'weather-high-error.log'
    ),
    weatherApp(
      'polymarket-weather-low',
      'config/collector.weather.low.yaml',
      'weather-low-out.log',
      'weather-low-error.log'
    ),
    watchdogApp(
      'polymarket-weather-high-watchdog',
      'config/collector.weather.high.yaml',
      'weather-high-watchdog-out.log',
      'weather-high-watchdog-error.log'
    ),
    watchdogApp(
      'polymarket-weather-low-watchdog',
      'config/collector.weather.low.yaml',
      'weather-low-watchdog-out.log',
      'weather-low-watchdog-error.log'
    ),
    // Daily Parquet compaction (§10A, temp + atomic rename), staggered so the
    // two data dirs never compact at the same minute.
    compactApp(
      'polymarket-weather-high-compact',
      './data-weather-high',
      'weather-high-compact-out.log',
      'weather-high-compact-error.log',
      '10 3 * * *'
    ),
    compactApp(
      'polymarket-weather-low-compact',
      './data-weather-low',
      'weather-low-compact-out.log',
      'weather-low-compact-error.log',
      '25 3 * * *'
    ),
  ],
};
