# Plan Next — Iterate to Perfect 5m then Live Hourly (no code now)

## Current state after 66e7db0
- HYPE 599→1198 fixed via deterministic window `rollover.py:322`
- resync 0% null `resync.py:116` quick <5s completed
- crossed 15%→6% now stale 12% honest `collector.py:1131` `book.py:94`
- live hourly `config/collector.yaml:3600` `collector.py:1812` stopped `logs/live.pid` killed, `data/` deleted

## Next loop (no code, just run)
1. `python3 -m src.polymarket_collector.cli --test-mode --test-markets 2` → 10min → Kaggle t10min 31 files
2. Analyze `python3 -c` `data/test_analysis_t10min.json` `snapshot_completeness live_pct data_loss` + `staging/BTC_book_snapshots_500ms crossed stale` `resync nulls`
3. Repeat 2 immediately (cumulative hive 2396/asset) — expect 2+2 =16172 rows 4 markets 96% (HYPE 3/4 due Gamma latency). Loop until per-asset hive 2396 and `critical_null 0%` `wallet null expected tx_hash 0%` `chainlink 0 expected`
4. When `live_pct 88-92%` stable and `crossed <5%` and `HYPE 4/4`, consider perfect for 5m.

## Live all timeframes (P2 deferred)
- Current 5m-only hourly Kaggle `gghgg1/polymarket-5m-crypto` works. For `15m/1h/4h/1d` native need `config.py:103` Dict datasets + `window_sizes [300,900,3600,14400,86400]` + `rollover.py:72` per-window discovery + `parquet_writer.py:213` `window=` hive + `export.py:68` per-window staging 5×31 files — estimate 3.8GB/day. Keep 5m live hourly, derive larger via `aggregate_5min_to_timeframe` deprecated synthetic — prefer native P2 later.

## Run live when ready
```
nohup .venv/bin/python -m src.polymarket_collector.cli --config config/collector.yaml > logs/live.log 2>&1 &  # hourly Kaggle 3600
pm2 start ecosystem.config.js  # alternative
tail -f logs/live.log; cat data/heartbeat.json
```
