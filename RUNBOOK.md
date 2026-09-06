# RUNBOOK — multi-timeframe 24/7 collector (single process)

One PM2-managed Python process collects **all enabled timeframe lanes**
(`timeframes: [5m, 15m, 4h]` in `config/collector.yaml`) for 7 assets
(BTC/ETH/SOL/HYPE/BNB/XRP/DOGE). No per-TF processes — ever.

## 0. Probe gate (do this FIRST, repeat before enabling any new lane)

```bash
python -m polymarket_collector.verify_gate --probe-timeframes --config config/collector.yaml
```

- Expected (2026-09-06): `5m / 15m / 4h` live on 7/7 assets → ENABLE.
- `1h / 1d` do NOT exist on Gamma (alternate slug patterns already checked) → keep OFF.
- **Never enable a lane the probe hasn't passed.** A missing lane stays off;
  we never fabricate or impute market data.

## 1. Machine

| | Recommended | Minimum |
|---|---|---|
| GCP type | **e2-medium** (2 vCPU, 4 GB) | e2-small (2 vCPU, 2 GB) |
| Disk | **25 GB** SSD (local parquet + WAL + raw WS archive) | 15 GB |
| OS | Ubuntu 24.04 LTS | Ubuntu 22.04 |
| Runtime | Python 3.11+, pm2 (via npm), `kaggle` CLI | same |

```bash
# OS deps + node/pm2
sudo apt update && sudo apt install -y python3-venv git nodejs npm
sudo npm i -g pm2
```

## 2. Install

```bash
git clone <repo-url> polymarket-collector && cd polymarket-collector
python3 -m venv .venv && .venv/bin/pip install -e .
mkdir -p logs data
cp config/collector.example.yaml config/collector.yaml  # if starting fresh
```

## 3. Kaggle credentials

```bash
mkdir -p ~/.kaggle && chmod 700 ~/.kaggle
# paste API token from https://www.kaggle.com/settings → "Create New Token"
cp ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
kaggle datasets list --mine  # sanity check auth works
```

Per-lane datasets (defaults in `CollectorConfig.kaggle.datasets`,
overridable via a `kaggle.datasets:` map in the yaml):

| lane | dataset |
|---|---|
| 5m | `gghgg1/polymarket-5m-crypto` |
| 15m | `gghgg1/polymarket-15m-crypto` |
| 4h | `gghgg1/polymarket-4h-crypto` |

The hourly Kaggle loop uploads **every enabled lane** to its own dataset from
`data/kaggle_staging/{tf}/`. `chainlink_events` is shared across lanes by design.

## 4. Static verification (before any live run)

```bash
python -m pytest tests/ -q   # must be all green
python -m polymarket_collector.verify_gate --probe-timeframes  # 5m/15m/4h ENABLE
python -c "
from polymarket_collector.config import CollectorConfig
from polymarket_collector.collector import Collector
cfg = CollectorConfig.load('config/collector.yaml')
print(cfg.timeframe_window_sizes())  # must match timeframes: in the yaml
c = Collector(cfg); print('Collector constructs OK')
"
```

## 5. Start / stop / persist

```bash
pm2 start ecosystem.config.js   # collector + watchdog + 15-min backfill cron
pm2 startup                     # run the command it prints (sudo), then:
pm2 save                        # persist process list across reboots

pm2 logs polymarket-collector   # tail collector
pm2 monit                       # dashboard
pm2 restart polymarket-collector
pm2 stop all && pm2 delete all  # full stop
```

PM2 entries: `polymarket-collector` (the single multi-TF process),
`polymarket-watchdog` (heartbeat monitor, separate process by design),
`polymarket-resolution-backfill` (cron `*/15`, runs backfill +
`--reupload --all-lanes`), `polymarket-compact` (daily 03:00 UTC, disabled by default).

## 6. Log locations

| log | path |
|---|---|
| collector stdout/stderr | `logs/collector-out.log`, `logs/collector-error.log` |
| watchdog | `logs/watchdog-out.log`, `logs/watchdog-error.log` |
| resolution backfill cron | `logs/resolution-backfill-out.log`, `logs/resolution-backfill-error.log` |
| live-test runs | `test_run_*.log` (repo root) |
| on-disk event evidence | `data/collector_events/*.parquet` (`coverage_gap`, `rollover_miss`, `market_added`, `kaggle_upload`) |

Healthy steady-state lines: `[kaggle:5m]`, `[kaggle:15m]`, `[kaggle:4h]`
hourly success lines; **no** `[prune] WARN`; no `coverage_gap` /
`subscription_failed` growth in `collector_events`.

## 7. Rollout order (each lane soaks before the next)

1. **`timeframes: [5m]`** — soak ≥24h. Gate: completeness ≥99%, `coverage_gaps` 0,
   hourly `gghgg1/polymarket-5m-crypto` uploads succeeding.
2. **`timeframes: [5m, 15m]`** — soak ≥24h. Gate: both datasets uploading;
   assert lane staging is filtered (`series_id == "BTC-15m"` in
   `data/kaggle_staging/15m/...`). Cross-TF rows in a lane's staging = P0, fix filter, do not upload.
3. **`timeframes: [5m, 15m, 4h]`** — full 24/7 runner. 4h needs no interactive
   live test (2×4h = 8h+); verify `market_added` events within its lead window,
   then leave it to the overnight soak.

To restrict a validation run to one lane:
`python run_2x5min_test.py --timeframe 15m` (uses `--test-timeframe`;
watch for `[test-mode:real] lane restricted to 15m`).

## 8. Local-disk safety (rolling-window prune)

- Prod runs with `kaggle.rolling_window: true`, `local_retention_hours: 48`.
- `cleanup_local_data` deletes a local parquet file **only if** every
  `condition_id` in it ended before `checkpoint − 48h` **AND** its upload was
  verified. `chainlink_events`/`collector_events` use a timestamp fallback.
  Cumulative mode never deletes.
- Safety gate before prod: dry-run first —
  `cleanup_local_data(..., timeframe_labels=["5m"], rolling_window=True, retention_hours=48, dry_run=True)`
  must list only files whose markets all ended >48h ago. **Nothing is deleted in dry-run.**

## 9. Incident basics

- **Coverage gaps** (`coverage_gap` in `collector_events`): check `test_run_*.log` /
  collector log around the boundary for `discovery_timeout` (transient transport,
  auto-retries in ~1s) vs `rollover_miss` (Gamma indexing late). Do not disable
  checks to make tests pass.
- **Kaggle upload failures**: fail-closed — local data is retained; the next
  hourly cycle retries. Never delete local data manually; only the verified-upload
  prune path may delete.
- **Crash recovery**: per-lane cursors `(asset, window_label)` resume each lane
  independently on restart (`_recover_from_cursor`).
- **Rollback**: set `timeframes: [5m]`, `pm2 restart polymarket-collector`.
