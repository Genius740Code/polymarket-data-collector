/**
 * PM2 ecosystem — paper-trading bots (no real orders).
 *
 *   1. btc5m-paper-bot      — paper_bot.py (BTC 5-min Up/Down, live CLOB book walk)
 *   2. weather-paper-trader — weather_bots_paper_trader.py (METAR/NWS/Polymarket consensus bots)
 *
 * Usage:
 *   pm2 start ecosystem.bots.config.js
 *   pm2 logs btc5m-paper-bot --lines 50
 *   pm2 logs weather-paper-trader --lines 50
 */
const path = require('path');
const cwd = __dirname;
const python = path.join(cwd, '.venv', 'bin', 'python');

module.exports = {
  apps: [
    {
      name: 'btc5m-paper-bot',
      cwd,
      script: python,
      args: 'paper_bot.py',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      restart_delay: 2000,
      exp_backoff_restart_delay: 100,
      kill_timeout: 15000,
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      out_file: path.join(cwd, 'logs', 'btc-paper-out.log'),
      error_file: path.join(cwd, 'logs', 'btc-paper-error.log'),
      env: { PYTHONUNBUFFERED: '1' },
    },
    {
      name: 'weather-paper-trader',
      cwd,
      script: python,
      args: 'weather_bots_paper_trader.py',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '300M',
      restart_delay: 2000,
      exp_backoff_restart_delay: 100,
      kill_timeout: 10000,
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      out_file: path.join(cwd, 'logs', 'weather-paper-out.log'),
      error_file: path.join(cwd, 'logs', 'weather-paper-error.log'),
      env: { PYTHONUNBUFFERED: '1' },
    },
  ],
};
