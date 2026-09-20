/**
 * PM2 ecosystem — paper-trading bots (no real orders).
 *
 *   1. btc5m-unified            — paper_btc_unified.py (base + momentum live + harvester sched)
 *   2. btc5m-momentum-impulse   — paper_momentum_impulse.py (impulse+skew T-120s momentum)
 *   3. weather-paper-trader     — weather_bots_paper_trader.py (METAR/NWS/Polymarket consensus bots)
 *
 * Usage:
 *   pm2 start ecosystem.bots.config.js
 *   pm2 start ecosystem.bots.config.js --only btc5m-unified
 *   pm2 logs btc5m-unified --lines 50
 *   pm2 logs weather-paper-trader --lines 50
 *
 * NOTE: package.json has "type": "module", so this .js file MUST stay ESM
 * (import/export). For tooling that expects CommonJS, a twin
 * ecosystem.bots.config.cjs is kept in sync.
 */
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const cwd = path.dirname(__filename);
const python = path.join(cwd, '.venv', 'bin', 'python');

export default {
  apps: [
    {
      name: 'btc5m-unified',
      cwd,
      script: python,
      args: 'paper_btc_unified.py',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '600M',
      restart_delay: 2000,
      exp_backoff_restart_delay: 100,
      kill_timeout: 15000,
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      out_file: path.join(cwd, 'logs', 'btc-unified-out.log'),
      error_file: path.join(cwd, 'logs', 'btc-unified-error.log'),
      env: { PYTHONUNBUFFERED: '1' },
    },
    {
      name: 'btc5m-momentum-impulse',
      cwd,
      script: python,
      args: 'paper_momentum_impulse.py',
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
      out_file: path.join(cwd, 'logs', 'btc-momentum-out.log'),
      error_file: path.join(cwd, 'logs', 'btc-momentum-error.log'),
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
