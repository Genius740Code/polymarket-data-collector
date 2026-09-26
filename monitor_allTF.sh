#!/bin/bash
# Hourly health + data-quality monitor for the all-timeframe collector run.
# Policy: REPORT ONLY. Never deletes/moves data (collector's own disk-guard +
# verified-upload prune own all reclamation). Restarts the collector if dead.
cd /home/fese/polymarket-data-collector || exit 1
LOG=logs/monitor-allTF.log
CFG=config/collector.fast.yaml
# 2026-09-24 crash fix: match ANY collector invocation, not just $CFG.
# pm2 runs config/collector.yaml while this script starts $CFG — the old
# config-specific pattern missed the pm2 instance and launched a SECOND
# concurrent writer on the same ./data (double RSS -> OOM-kill 06:46,
# raced _kaggle_state.json -> 1h never verified -> prune fail-closed).
# Single-supervisor rule: exactly one collector process per data dir.
# NOTE: the trailing "--config" excludes polymarket_collector.watchdog.cli
# (separate watchdog process by design) — otherwise a live watchdog would
# mask a dead collector and no restart would fire (2026-09-24 fix).
PATTERN="polymarket_collector.cli --config"

check_once() {
  echo "=== $(date -u +%FT%TZ) ===" >> "$LOG"
  # 1. process alive? restart if dead (append logs, never overwrite).
  # 2026-09-24: ONLY restart when NO collector with ANY config is alive
  # (single-supervisor rule — see PATTERN). Never kill/restart a live one.
  if pgrep -f "$PATTERN" > /dev/null; then
    echo "collector: ALIVE pid=$(pgrep -f "$PATTERN" | head -n1) cmd=$(pgrep -af "$PATTERN" | head -n1)" >> "$LOG"
  else
    echo "collector: DEAD -> restarting" >> "$LOG"
    setsid nohup .venv/bin/python -m polymarket_collector.cli --config "$CFG" \
      >> logs/collector-allTF-out.log 2>> logs/collector-allTF-error.log < /dev/null &
    echo "collector: restarted pid=$!" >> "$LOG"
  fi
  # 2. heartbeat freshness
  .venv/bin/python -c "
import json,time
try:
    hb=json.load(open('data/heartbeat.json'))
    age=(time.time_ns()-hb.get('ts_ns',0))/1e9
    print(f\"heartbeat: age={age:.1f}s assets={hb.get('assets')}\")
    print('HEARTBEAT STALE' if age>60 else 'heartbeat: FRESH')
except Exception as e: print(f'heartbeat: MISSING/ERR {e}')
" >> "$LOG" 2>&1
  # 3. disk + data growth (the historical ENOSPC bug)
  echo "disk: $(df -h / | tail -n1)" >> "$LOG"
  echo "data: $(du -sh data 2>&1)" >> "$LOG"
  FREE_B=$(df --output=avail -B1 / | tail -n1)
  echo "disk_free_bytes: $FREE_B" >> "$LOG"
  [ "$FREE_B" -lt 1073741824 ] && echo "!!! LOW DISK <1GiB (collector disk-guard should be reclaiming) !!!" >> "$LOG"
  # 4. data-quality scan: per-lane balance, prices 0..1, crossed, live/stale, event mix
  .venv/bin/python -c "
from pathlib import Path
from collections import Counter
import pyarrow.parquet as pq
lane=Counter(); state=Counter(); crossed=0; total=0; bad=0
for f in Path('data/book_snapshots_500ms').rglob('*.parquet'):
    try: t=pq.read_table(f, columns=['asset','series_id','book_state','book_crossed','up_bid','up_ask','down_bid','down_ask'])
    except Exception as e: print('UNREADABLE',f,e); continue
    total+=t.num_rows; d=t.to_pydict()
    for a,s,bs in zip(d['asset'],d['series_id'],d['book_state']):
        lane[(str(a),str(s))]+=1; state[str(bs)]+=1
    crossed+=sum(1 for v in d['book_crossed'] if v)
    for col in ['up_bid','up_ask','down_bid','down_ask']:
        bad+=sum(1 for v in d[col] if v is not None and not (0<=v<=1))
print(f'snapshots: rows={total} lanes={len(lane)} min_lane={min(lane.values()) if lane else 0} max_lane={max(lane.values()) if lane else 0}')
print(f'state: {dict(state)} crossed_true={crossed} out_of_range={bad}')
ev=Counter()
for f in Path('data/collector_events').rglob('*.parquet'):
    try: t=pq.read_table(f, columns=['event_type'])
    except Exception: continue
    for v in t.column('event_type').to_pylist(): ev[str(v)]+=1
print(f'events: {dict(ev)}')
for tbl in ['trades','chainlink_events','book_events','resync_episodes']:
    n=0
    for f in Path(f'data/{tbl}').rglob('*.parquet'):
        try: n+=pq.read_table(f).num_rows
        except Exception: pass
    print(f'{tbl}: rows={n}')
" >> "$LOG" 2>&1
  echo "" >> "$LOG"
}

check_once
# 2026-09-24 crash fix: split liveness (cheap, every 60s) from the heavy
# full-scan audit (hourly). The old single hourly cadence left the 07:00
# death undetected for up to an hour (MTTR ~1h). The light check only runs
# pgrep - no parquet scans, no heartbeat reads.
light_check() {
  if pgrep -f "$PATTERN" > /dev/null; then
    return 0
  fi
  echo "=== $(date -u +%FT%TZ) light ===" >> "$LOG"
  echo "collector: DEAD -> restarting" >> "$LOG"
  setsid nohup .venv/bin/python -m polymarket_collector.cli --config "$CFG" \
    >> logs/collector-allTF-out.log 2>> logs/collector-allTF-error.log < /dev/null &
  echo "collector: restarted pid=$!" >> "$LOG"
}
n=0
while true; do
  sleep 60
  n=$((n+1))
  if [ "$n" -ge 60 ]; then
    n=0
    check_once
  else
    light_check
  fi
done
