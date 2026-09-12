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
const cwd = __dirname;
const python = path.join(cwd, '.venv', 'bin', 'python');

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
    env: { PYTHONUNBUFFERED: '1' },
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
  ],
};
