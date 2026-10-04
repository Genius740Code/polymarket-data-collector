# Dual-Enable Runbook

**Purpose:** Enable dual-track WsConfig mode so both WS lanes process in parallel
without data loss, with automatic rollback if the new mode destabilizes the
collector.

## 1. Config key

- **File:** `config.py`, line ~62
- **Key:** `WsConfig.dual_enabled`
- **Default:** `False`
- **To enable:** Set `WsConfig.dual_enabled = True` and restart the collector.

```python
# config.py ~ line 62
class WsConfig:
    dual_enabled = False  # <-- set to True to enable dual-track mode
    # ... rest of config
```

## 2. How to enable

1. Edit `config.py` and change `WsConfig.dual_enabled = False` to
   `WsConfig.dual_enabled = True`.
2. Restart the collector (PM2 or systemd):
   ```bash
   pm2 restart collector   # or: systemctl restart polymarket-collector
   ```
3. Verify the flag took effect by checking the collector startup banner:
   ```
   [startup] dual-track mode enabled — 2 WS lanes active
   ```

## 3. Expected first-5-min log markers

After enabling, watch the first 5 minutes of collector output:

| Marker | Meaning |
|---|---|
| `[ws:ETH] peer_covering flap` | Lane ETH is in *peer_covering* mode (first sign of dual-track flap). |
| `[ws:BTC] dual-down` | Lane BTC has dropped to dual-down (expected during transition). |
| `[resolution] ...` | Resolution events continue on both lanes; **no gaps** should appear. |
| `[kaggle] staging ... success` | Staging mirror succeeds on both lanes. |

**Normal transition sequence (first 5 min):**
1. `[ws:ETH] peer_covering flap`
2. `[ws:BTC] dual-down` (or vice versa)
3. Both lanes resolve windows without `[export] WARN` failures.
4. `[kaggle] staging ... success` for both 5m and 1h lanes.

If *neither* `peer_covering flap` nor `dual-down` appears within 5 minutes,
the flag may not have been picked up — verify the config change and restart.

## 4. Rollback

To rollback if the collector shows errors:

1. Flip the flag back: `WsConfig.dual_enabled = False` in `config.py`.
2. Restart the collector:
   ```bash
   pm2 restart collector
   ```
3. Verify the startup banner shows `dual_enabled=false` (default mode).

Rollback is immediate — the collector switches back to single-track WS mode
on the next restart. No data loss occurs because the prior single-track state
is preserved in the existing parquet files.

## 5. Soak metrics (pass/fail thresholds)

Run the soak test for 24h to validate stability:

| Metric | Baseline (single-track) | PASS threshold |
|---|---|---|
| `1006-ep/h` (events/hour, lane 1006) | 258.8 | **down ≤ 220** (≤15% reduction / stability) |
| `live%` (fraction of windows resolving live) | variable | **climbing** (monotonically increasing over 24h) |
| `[export] WARN` per hour | baseline rate | **≤ 2× baseline** (no spike) |

**Pass condition:** After 24h soak with `dual_enabled=true`:
- `1006-ep/h` is down from 258.8 baseline (i.e., ≤ 220 events/hour), **AND**
- `live%` is climbing (each 5m lane resolution shows a resolved price, no
  unresolved windows), **AND**
- export WARN rate does not double the baseline.

If any condition fails, rollback the flag and investigate WS message ordering.

### auxiliary scripts

- `scripts/measure_1006_rate.py` — streams 1006-ep/h rate for the duration of
  the soak, writing a time-series CSV.
- `scripts/watch_24h.py` — watches the collector log for `live%` progression
  and `[export] WARN` occurrences, exiting 1 if thresholds are breached.

Example 24h soak command:
```bash
# enable dual mode first
python3 -c "from polymarket_collector.config import WsConfig; WsConfig.dual_enabled = True"
pm2 restart collector

# run soak for 24h
./scripts/measure_1006_rate.py --hours 24 > soak_1006_rate.csv
./scripts/watch_24h.py --log logs/collector-out-11.log --hours 24 \
    --live-threshold 5% --warn-multiplier 2.0
```