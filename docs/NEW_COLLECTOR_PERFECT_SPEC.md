# Perfect Crypto Collector — rewrite spec (branch `perfect/pmdata-parity`)

> Goal: best-quality crypto-market data (5m/15m/1h UpDown + full crypto discovery),
> PMData-compatible raw ticks + our audit layer, Kaggle rolling upload.
> Strategy: rewrite the ingest spine from scratch, keep the stellar code below verbatim.
> Policy: real data only (`AGENT.md`/`AGENTS.md`). Gaps are rows + events, never fills.

## 0. Scope

- Assets: `BTC ETH SOL HYPE BNB XRP DOGE` (+ `ZEC` when Gamma lists it). Crypto only.
- Lanes: `5m 15m 1h` day-one; `4h 1d` after gate (retention + first-upload proof each).
- Datasets out: `l2_raw` (new, PMData ticks), `book_snapshots_500ms`, `book_snapshots_clean`,
  `book_events`, `trades`, `onchain_fills` (first-class), `chainlink_events`,
  `chainlink_twap` (derived 30s/60s), `markets_log/markets_latest/markets_summary`,
  `resync_episodes/collector_events`.
- History rule: nothing before cutover is invented. Backfill only from PMXT archive
  (`clarkpalmer/pmxt-data-ingestion` pattern: download hourly archive → DuckDB filter →
  `enrich.py` trades/resolutions/strikes). Missed live ticks stay gaps.

## 1. Architecture (mix: Python brain, Rust edge later)

```
Gamma discovery ──▶ market registry (condition_id, slug, tokens, end_ts)
                        │
CLOB WS x2/shard ──▶ dedup ──▶ data/raw/ws_market/{book,price_change,last_trade_price,tick_size_change,market_resolved}/hour.jsonl   (jamtho landing)
                        │                                                          │
                        └──▶ RAM books (LiveBook, owen pattern) ──▶ 500ms snapshots + book_events(threshold) + trades ──▶ WAL ──▶ hive parquet
                        │
RTDS chainlink ──▶ chainlink_events ──▶ chainlink_twap (derived, NULL on gaps>10s)
                        │
Data-API + Polygon RPC ──▶ trades enrich (api- reconcile) + onchain_fills (substreams v1+v2)
                        │
export ──▶ hive (date=/asset=) + per-slug per-day parquet + day-ZIPs (PMData layout) + Kaggle staging (39-file compat) + HF mirror (SII pattern)
                        │
gates ──▶ verify_gate + PERFECT_DATA_SPEC 99.9% + PMData sample diff + marketlens-style execution backtest
```

Phase 2 (only if Python saturates): `pmxt-dev` split — Rust WS-pool holder →
Redis pub/sub → same Python writer. No logic rewrite, sockets move only.
`poly-book` extras when needed: CRC WAL, flock failover, PIT replay, workstation API.

## 2. Keep verbatim (stellar code — do not rewrite)

| Keep | Path | Why |
|---|---|---|
| OrderBookState + null-vs-zero + depth_within | `src/polymarket_collector/book.py` | 500ms grid, L2 truncate/pad, hash attestation — proven |
| ResyncManager (episodes, buffer-and-replay, supersede, 429/backoff) | `src/polymarket_collector/resync.py` | audit-closure core |
| ParquetWriter (WAL-before-buffer, dedup, tmp+rename, dead-letter, disk-guard) | `src/polymarket_collector/storage/parquet_writer.py` | zero-silent-loss |
| Schemas + SCHEMAS map | `src/polymarket_collector/storage/schemas.py` (+ `CHAINLINK_TWAP_SCHEMA`) | contracts |
| MarketsLog event-source + compact | `src/polymarket_collector/storage/markets_log.py` | exactly-one-row-per-market |
| clean_view (`live`-only) | `src/polymarket_collector/storage/clean_view.py` | backtest read path |
| compaction merge-verify-then-delete | `src/polymarket_collector/storage/compaction.py` | 100k-file killer |
| quarantine reap (bounded) | `src/polymarket_collector/storage/quarantine.py` | only bounded deleter |
| chainlink TWAP derive | `src/polymarket_collector/chainlink_twap.py` | NULL-on-gap rule |
| export staging (39 files) + markets_summary + api-/RPC enrich | `src/polymarket_collector/storage/export.py`, `onchain.py` | Kaggle compat |
| verify_gate + watchdog + resolution_backfill | `verify_gate.py`, `watchdog/`, `resolution_backfill.py` | gates |
| validation/coerce/clock | `validation.py`, `clock.py`, `enums.py` | bounds + dual-ts |

## 3. Rewrite from scratch (new spine)

1. **`storage/l2_raw.py`** — every WS frame verbatim (no threshold): `{ts_source, ts_received_ns, asset, condition_id, token_id, event_type, frame_json, source_conn}`. Full depth arrays. Conn A/B tag for dedup audit. Writer: JSONL hourly (jamtho) → Parquet hourly (compactor) → hive `l2_raw/date=/asset=/`.
2. **`ingest/ws_pool.py`** — 2x `wss://ws-subscriptions-clob.polymarket.com/ws/market` per shard, shared-subscribe (`assets_ids`), hot-add, `recycle 270s` max 280, dedup `(token,seq/ts)`. owen `LiveBook`: snapshot + absolute-size deltas + 120s-silence watchdog reconnect.
3. **`ingest/heal.py`** — batched `POST /books` heal (one round-trip, not per-token GET — kills `fetch_none`/429 storm). Ended-window precheck via registry (supersede, no 60s burn).
4. **`ingest/discovery.py`** — Gamma slug deterministic (`rollover.py` kept), dual-track overlap, `coverage_gap` on unlisted windows. Expand to full crypto series list (Telonex 1.28M snapshot optional seed).
5. **`export_pmdata.py`** — per-`slug` per-day parquet (`{slug}.parquet`: `market_slug,timestamp,local_timestamp,event_type,ask_prices,ask_sizes,bid_prices,bid_sizes,…`) + day-ZIPs (`{asset}-{5m,15m,1h}.zip`) + manifest. YES-only columns + full-depth. Additive: hive + 39-file staging untouched.
6. **`ingest/chainlink_direct.py`** — direct Chainlink streams WS alongside RTDS (`streams, streams_twap30s/60s` passthrough when reachable; else derived TWAP stands in, labelled).
7. **`backtest_exec.py`** (marketlens pattern) — tick replay with latency/queue/fees/settlement; gate-only, never collector.

## 4. Schemas (delta vs today)

- `l2_raw`: raw frame log (above). No thresholds, no synthesis. `tick_size_change` + `market_resolved` stored here (today: 0 handlers, `book.py:649` gap).
- `book_events`: keep BBO-threshold semantics, tag wire vs `bbo_snapped/crossed_reverted`-derived in `source`.
- `chainlink_events`: unchanged (RTDS raw, `report_id` NULL-known). Direct-streams rows go to `chainlink_streams` (same shape + `reportId/roundId` when present).
- `chainlink_twap`: as built (1s grid, `twap_30s/60s`, `n_ticks`, `gap_max_ms_60s`, `source='derived_chainlink_rtds'`).
- `onchain_fills`: promote to first-class (today: enrich inside trades): `{tx_hash,token_id,condition_id,maker,taker,price,size,fee,side,exchange_version,builder}` unanimity-join, multi-maker NULL.
- Snapshots: keep top-10 default + `l2_full` opt-in flag (PMData parity on demand; full = ~10x rows).

## 5. Storage & Kaggle

- Hot: `data/raw/ws_market/*/*.jsonl` (hourly rotate) → `data/<ds>/date=/asset=/*.parquet` (flush 30s/1500 rows, tmp+rename) → daily `compact_all` (verify-then-delete).
- Warm: per-slug per-day + day-ZIP under `data/pmdata/{l2,trades,onchain_fills}/YYYY/MM/DD/`.
- Upload: rolling window (`rolling_window:true`), per-lane datasets (`gghgg1/polymarket-{5m,15m,1h}-crypto`), slowest-lane checkpoint + coverage proof + market-end cutoff → move-to-`_quarantine` → `reap_quarantine` bounded. Gap evidence never pruned. HF mirror monthly (SII).
- Layout compat: existing 39-file staging keeps uploading; PMData layout is additive.

## 6. Server

- Prod: `4 vCPU / 16 GB / 500 GB SSD, Ubuntu 24.04, AWS eu-west-2 London` (same AZ as CLOB; ~1ms). 500G ≈ 60d full-tick raw + derived + headroom. Current 100G data disk (88G free) becomes staging/dev.
- Budget alt: Hetzner `CCX33` Falkenstein (~10ms). Fine for snapshots, not for tick-parity claims.
- Run: `pm2 start ecosystem.config.cjs` (collector + watchdog + compact 03:00Z + backfill */15). `pm2 save`. Logs rotate (root filled once by 386M watchdog log — cap at 100M).

## 7. Quality gates (must all pass for `perfect` tag)

1. `pytest -q` green + `verify_gate --probe-timeframes` ENABLE per lane.
2. `PERFECT_DATA_SPEC` scorecard 18/18: completeness ≥99.9%/market+day, stale ≤0.1%, crossed 0 clean, dups 0, orphans 0, depth-mismatch 0, grid/partition 0, settlement ≥99.9% official.
3. `l2_raw` vs PMData same slug/day: row-count ±2%, mid-price median diff ≤1 tick, event-type coverage (`book/price_change/last_trade_price/tick_size_change`) present.
4. Backtest gate: execution replay PnL-neutral on mirrored sample (fees/latency on).
5. Ops gate 7d soak: `resync_failed≈0`, `coverage_gap=0`, `[retention-guard]` + `[disk-guard]` silent, `df<70%`, restarts 0.

## 8. Build order (checkboxes)

- [ ] (1) `l2_raw` + JSONL landing + hourly compactor
- [ ] (2) dual WS + dedup + `tick_size_change/market_resolved` handlers
- [ ] (3) batched `POST /books` heal (delete per-token GET storm)
- [ ] (4) full-depth opt-in + YES-only view
- [ ] (5) `export_pmdata` per-slug + day-ZIP + manifest
- [ ] (6) `onchain_fills` first-class + v2 sides/fees/builder
- [ ] (7) direct Chainlink streams source (RTDS stays as fallback)
- [ ] (8) `backtest_exec` gate + PMData diff job
- [ ] (9) Kaggle multi-lane (5m→15m→1h) soak, one lane at a time
- [ ] (10) Rust edge split (only on saturation proof)

## 9. File tree (new files only)

```
src/polymarket_collector/ingest/{__init__.py,ws_pool.py,heal.py,discovery.py,chainlink_direct.py}
src/polymarket_collector/storage/l2_raw.py
src/polymarket_collector/export_pmdata.py
src/polymarket_collector/backtest_exec.py
tests/test_l2_raw.py
docs/NEW_COLLECTOR_PERFECT_SPEC.md  (this file)
```

## 10. Backtest read path (unchanged)

`book_snapshots_clean` (`live` only) + both-sides-quoted + `book_age≤1500ms` +
`polymarket_official` settlement + 600-tick markets; `l2_raw` for tick replay;
TWAP from `chainlink_twap` (NULL=gap); gaps excluded, never interpolated.
