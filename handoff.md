# 📌 CURRENT HANDOFF (2026-09-05 night) — MISSION: fix issues → test-loop → git commit

> ## 🔁 LOOP STATUS (2026-09-06, after 5 test-loop iterations)
> Commits: `4ea47d0` (iter1), `8dbe881` (iter2), `8283c4e` (iter3+4). Final artifact: kaggle `gghgg1/polymarket-5m-crypto` ready, 39 files, remote-verified uploads.
>
> **Fixed during the loop:**
> 1. Staging pre-validation self-race — validation compared staging against hive rows read AFTER the build; live collector growth aborted every in-run upload. Now compares only files with mtime ≤ build start.
> 2. **Kaggle `dataset_status()` returns a plain STRING** in kaggle 2.x — the old `st.get("status")` threw AttributeError that a bare except swallowed every cycle, so the status poll NEVER matched and uploads "timed out". Fixed + observable (logs every poll; the 403 Forbidden right after `dataset_create_version` is normal and clears in ~10s).
> 3. Upload verify is fail-closed with a 10-min poll budget (audit fixes #10/#20 restored; the old "optimistic success at 60s" skipped all verification on nearly every upload).
> 4. `scheduler_lag` no longer fabricates per-asset `ws_disconnected`/`ws_reconnected`/`resync_completed` events or resync_episodes rows (155 catch-ups × 7 assets faked 1085 "disconnects" and drowned the real churn signal — the old B1/C1 churn evidence was bogus; real churn is ~1-2 reconnects/min/asset at the 150s recycles).
> 5. `date_str fallback` WARN fixed (writer derives partition date from `disconnect_ts_utc`).
> 6. WS watchdog: fires on >30s data silence, force-aborts via `fail_connection`-equivalent, logs to console, and **skips firing when the event loop itself is stalled** (checks `_last_snapshot_bucket_ms` lag — during a Kaggle export block all 7 connections look "stale" but frames are merely buffered). Task-await at recycle is bounded (3s) so a zombie task can't hang the reconnect.
> 7. Dropped `settlement_report_id`/`settlement_tx_hash` from markets (no wire source, 100% NULL forever); added `rollover_pairing` invariant to the analysis JSON (started_initial/started/completed/unpaired).
>
> **Remaining / operator decisions:**
> - **C2 maker-wallet backfill is BLOCKED on credentials**: no Alchemy/Graph/PolygonScan key on this box. taker_wallet fills 80-99%; maker_wallet 14-78% (asset-dependent). Implement per the checklist below once a key exists.
> - **Stale producer on the other box** (audit #1): still unresolved — if the old collector elsewhere runs a Kaggle upload cron, it can clobber the dataset with a 30-file old-schema version. pm2 here is empty.
> - **No resolution-backfill cron on this box**: the test tool's finalize backfill runs ~5 min after the last window, before the ~10-min official-winner lag, so the last windows stay `inferred_nearest`/`unknown` in the published version until a LATER backfill. Run `python -m polymarket_collector.resolution_backfill --config config/collector.yaml --reupload` ~15 min after each test run (or restore the pm2 cron here).
> - **Gamma discovery flakiness** (iter 5): 15 coverage_gaps + 19 subscription_failed in one run — discovery sometimes misses windows or serves gaps in sequence (windows 5962367/5962369 discovered with 5962368 skipped); uncollected windows are honestly counted in the completeness denominator (43.69% that run) and left NULL. Needs a discovery-retry/backoff design (next loop).
> - 4 crossed rows slipped into SOL book_snapshots_clean in the final artifact (0.09%) — crossing race between the pre-snapshot check and frame apply; existing DATA_CARD crossing documentation covers the mechanism.

**Read this first. You (the next AI) have one job, in three phases:**

1. **FIX** — work through the ISSUE CHECKLIST below. Keep `pytest tests/ --ignore=tests/test_verify_gate.py` green before and after every fix (94 tests as of this writing).
2. **TEST-LOOP** — run the destructive full test (protocol below), analyze the run for errors and bad practices, fix what you find, and repeat until a complete clean pass.
3. **COMMIT** — when the loop passes cleanly, `git add -A && git commit` with a message that lists the fixes and the evidence from the final passing run. Do NOT push unless the operator asks.

**Standing rules:**
- NEVER fabricate data. A missing value stays NULL (null-vs-zero, honest gaps — repo law).
- Deleting local `data/` and the Kaggle dataset is EXPECTED and OPERATOR-APPROVED: `run_2x5min_test.py` does exactly that as its first step. Do not do destructive deletes by hand beyond what the tool does.
- The "Verified live facts" below were measured against the production endpoints. Do not re-derive them; build on them. If you contradict one with a fresh probe, update this file.
- Context docs: `docs/WS_RESILIENCE_RESEARCH.md` (WS findings + live probe transcripts), `DATA_CARD.md` (dataset caveats — the issues below exist to shrink this list).

---

## ISSUE CHECKLIST (fix in this order)

### A. Code fixes — small, independent, do them first

- **A1. `book_events.ts_source` NULL (~1–14%)** — set in `book.py:450` from `msg.get("timestamp") or msg.get("ts")`; NULL = frame arrived without a top-level timestamp.
  FIRST run a 5-min live probe (connect, subscribe one token, log which message types carry `timestamp` and which don't — the AsyncAPI spec at docs.polymarket.com/asyncapi.json says `hash` is required on `price_change`; confirm whether `timestamp` is too).
  - If timestamps exist in a field we don't read → fix the parser (trivial).
   - If frames genuinely lack it → carry-forward: reuse the previous frame's timestamp from the SAME connection when it is within ~1s (batched deltas share a clock); otherwise leave NULL. NEVER stamp received-time as source time.
   - **Probe result 2026-09-05 (~37k live frames, 1 token):** top-level `timestamp` is present on 100% of `book`/`price_change`/`last_trade_price`/`tick_size_change` frames (ms-epoch string; `ts` never appears); `hash` present on 100% of `book` frames (top-level) and on every `price_change` entry. The `book_events.ts_source` NULLs were NOT missing frame timestamps — they were the `bbo_snapped`/`crossed_reverted` events, which never propagated the frame timestamp. Fixed in `book.py` (`_resolve_ts_source` + carry-forward ≤1.5s freshness gate); unit-tested.
- **A2. `markets_summary.underlying_open` NULL for live windows** — in `storage/export.py::build_markets_summary`, widen the open-boundary tolerance from 5s to 10s (match the resolution loop's K-2 10s open tolerance) and record what was used (add `underlying_open_tolerance_s` / `underlying_close_tolerance_s` to `MARKETS_SUMMARY_SCHEMA`). NULLs for windows that predate the collector stay NULL — no data existed, do not interpolate.
- **A3. L2 levels 9–20 structurally empty** (observed depth ~4–8) — switch the writer/config to `l2_levels: 10` (`snapshot_schema` is already parameterized). Old hive files keep 20 columns; export tolerates mixed schemas (trailing extras). Update `DATA_CARD.md` and the schema test (160 → 80 L2 cols for the default).
- **A4. Book `hash` integrity primitive** — the spec REQUIRES `hash` on `book`/`price_change` messages. Capture it (raw archive already stores frames), store it on book state, and validate on reconnect/promotion: reject a snapshot whose hash doesn't reproduce → mark stale + REST heal. This replaces the deleted sequence-number gap detection with the real integrity primitive. See research doc §6 for the NautilusTrader reference implementation.
  - **Implemented 2026-09-05:** `OrderBookState.book_hash` per outcome (top-level `book` hash + per-entry `price_change` hashes; probe: 100% present live). Promotion stale/resyncing→live via WS `book` frame is hash-gated — hash-less snapshot: levels still applied, promotion refused, `book_anomaly` emitted, REST heal covers. Exact hash *recomputation* (Nautilus-style preimage reproduction) is NOT implemented — the exchange hash algorithm is unpublished; even Nautilus treats hash-absent snapshots as compatible. Unit-tested (`test_a4_*`).

### B. Live-verification items — the test loop below must confirm these

- **B1. WS heartbeat fix actually kills the 3–5 min churn** — collector now runs `ping_interval=None` + app-level text `"PING"` every 10s (RTDS: 5s) + a 30s data-staleness watchdog. In the test log, count WS close episodes (`connected`/disconnect collector_events, resync_episodes rows). Expected: close cadence >> old 3–5 min (mostly the intentional 150s recycles). If connections STILL die at 3–5 min with the correct heartbeat → that is proof of a server-side idle reaper → promote C1.
- **B2. Single-topic RTDS** — collector subscribes ONLY `crypto_prices_chainlink` now. Check the `[chainlink] rtds rx/parsed per asset:` counters: ALL 7 assets (incl. HYPE) must still arrive. If HYPE goes missing, revert to both topics with per-topic `source` labels (research doc has the tradeoffs).
- **B3. `markets_summary.parquet` ships and is sane** — staging must have 39 files; summary rows must have real values for fully-collected windows (OHLC, volume, traders, underlying open/close) and honest NULLs elsewhere. Resolution outcomes populate only after the backfill step (the test tool runs it).
- **B4. Rollover hot-add works in the collector** — next-window tokens are added via `operation:subscribe` (no forced reconnect). In the log, confirm no "fresh connection forced at rollover" churn and that the new window's books go live from the add's full `book` frame.

### C. Bigger items — start after the test loop is clean (or when B1 demands it)

- **C1. Warm standby + shared connection (conditional on B1).** Only if connections still die despite the heartbeat fix. Architecture in `docs/WS_RESILIENCE_RESEARCH.md`: 1 primary + 1 warm standby already subscribed (zero-gap role swap), recycle the idle leg every ~3 min, REST `/book` heal on promotion, backoff 0.5s ×2 cap 10s + jitter. A single shared connection for all 28 tokens is unblocked by the verified `operation:subscribe` hot-add. Acceptance: stale+resyncing rows < 1% of snapshots across a 30-min live run with induced drops.
- **C2. Maker-wallet on-chain backfill (one-day probe, then implement).** The Graph Polymarket exchange subgraph (OrderFilled has maker+taker; free tier ~100k queries/mo) vs Alchemy free `eth_getLogs` on the two CTF Exchange contracts on Polygon vs PolygonScan API. Join key: (transaction_hash, price, size). Fill `maker_wallet` NULLs through `_writeback_enriched_trades`. This is the highest-value data-completeness lever left (~1,400 BTC rows were still waiting on late Data-API indexing at last smoke).
- **C3. Optional: `crypto_prices_twap_sixty` RTDS topic** — 5m markets now resolve via a 60s Chainlink TWAP (changelog). If stored, rows MUST be labeled by `source` so they never pollute the spot-price series used by resolution/summary.

---

## TEST-LOOP PROTOCOL (phase 2)

One iteration = `python run_2x5min_test.py` (Windows wrapper: `run_2x5min_test.cmd`). The tool does the whole destructive cycle itself:
wipe local `data/` → delete the Kaggle dataset (gghgg1/polymarket-5m-crypto) → 2×5min live collection → Kaggle upload → resolution backfill → final Kaggle upload → summary. Let it run; do not interrupt.

**Before the first run:** verify Kaggle creds (`export._validate_kaggle_config()` logic — env KAGGLE_API_TOKEN / KAGGLE_USERNAME+KEY or ~/.kaggle/kaggle.json). Tee console output to `test_run_<timestamp>.log`.

**After each run, analyze (in this order) and fix anything that fails:**
1. `pytest tests/ --ignore=tests/test_verify_gate.py` — green required.
2. Console log: grep for `ERROR`, `WARN`, `backpressure`, `sequence_gap`, `book_anomaly`, `resync`, `ws_error`, `staging pre-validation failed`, `✗`. Each hit is either a bug to fix or an upstream fact to document (then it's accepted, not fixed).
3. `data/test_analysis_final.json`:
   - `kaggle_staging.files == 39` and metadata lists all files.
   - `checks.snapshot_completeness_pct` / `clean_completeness_pct` high (≤100 — if >100 the bonus-row accounting regressed); `data_loss_pct` ≈ 0; `critical_null_flag` false; `book_state_histogram` mostly `live`; `bonus_*` rows small and explained.
   - `null_analysis` on trades: maker/wallet null % trending down after enrichment + backfill; on chainlink_events: only `report_id` null (reserved); on markets: `minimum_order_size` filling after backfill.
4. Kaggle: dataset ready, 39 files visible remotely, row counts monotonic (`_kaggle_state.json`).
5. `markets_summary.parquet`: row count == closed windows; spot-check one window's OHLC/volume/underlying against raw data.
6. Read the diff you produced this iteration for BAD PRACTICES (fabricated values, swallowed exceptions that hide data loss, unbounded memory, hot-path network calls, schema drift without compat handling). Fix those too.

**Loop** until one full iteration satisfies ALL of the above with no new issues. Practical bound: ~4–5 iterations; each costs ~15–20 min of live runtime. If an issue is upstream (Polymarket-side) and unfixable in code, document it in DATA_CARD.md and the issues list, mark it ACCEPTED, and continue.

## COMMIT (phase 3)

When the loop passes cleanly:
```
git add -A
git commit -m "<summary>: <fix list>; verified via N clean 2x5min test runs (pytest 94/94, staging 39 files, kaggle ready, completeness <X>%)"
```
Include the final `test_analysis_final.json` numbers in the message body. Do not push unless the operator asks.

---

## CONTEXT — Verified live facts (measured 2026-09-05; do not re-derive)

- CLOB market WS `wss://ws-subscriptions-clob.polymarket.com/ws/market`, subscribe `{"assets_ids":[...],"type":"market"}`. A repeated PLAIN subscribe on an established connection is **IGNORED** (re-probed live), BUT `{"assets_ids":[...],"operation":"subscribe","type":"market","custom_feature_enabled":true}` **hot-adds tokens on an established connection** — full `book` frame arrives immediately; verified live for same-asset AND cross-asset adds (transcript at the bottom of `docs/WS_RESILIENCE_RESEARCH.md`).
- The official heartbeat is an APPLICATION-level text `"PING"` every 10s (5s RTDS); server replies text `"PONG"`. RFC6455 control-frame pings are answered slowly — that is why tightening `websockets` ping settings made churn worse. Send "PING" only AFTER the first subscribe (a PING before any subscribe earns close 1008 "invalid subscription payload").
- The 3–5 min connection deaths were most likely OUR client's protocol-ping timeouts, not a server idle limit — but B1 must confirm.
- Gamma slug lookup serves only the ~2 most recent 5m windows; official resolution comes from `clob.polymarket.com/markets/{conditionId}` → `tokens[].winner` (works indefinitely). The same response carries `minimum_tick_size`/`minimum_order_size` (backfilled at resolution).
- RTDS sends ALL 7 assets at identical cadence on `crypto_prices_chainlink` alone; payload has NO twap/roundId/reportId/sequence. Collector now subscribes ONLY that topic (B2 confirms).
- Data-API `data-api.polymarket.com/trades` (takerOnly=false) exposes maker+taker legs for a minority of fills; fills are INDEXED LATE — hence the 15-min second enrichment pass inside `resolution_backfill`.
- Tightening websockets ping_interval/ping_timeout makes churn WORSE. Defaults + 150s light recycle was the old design; the heartbeat fix supersedes it.

## CONTEXT — Tooling

- `python run_2x5min_test.py` = the entire destructive test cycle (see protocol above). `--keep-data` skips wipes. `run_2x5min_test.cmd` is the Windows wrapper.
- pm2 cron `polymarket-resolution-backfill` runs resolution backfill + trades enrichment second pass every 15 min.
- pytest: `pytest tests/ --ignore=tests/test_verify_gate.py` (94 green as of this writing).

## CONTEXT — What the previous session did (all live-verified unless noted)

- **markets_summary export (DONE)**: `build_markets_summary()` in `storage/export.py`; 39th staging file `markets_summary.parquet`, one row per condition_id (resolution, underlying open/close from nearest chainlink tick ≤5s — A2 loosens to 10s, outcome-token mid OHLC from clean snapshots, volume/fills/unique traders incl. api- rows, avg spreads). Monotonic guard; tests in `tests/test_markets_summary.py`; verified on real data (27 rows) and end-to-end (39 staging files).
- **Chainlink cleanup (DONE)**: single-topic RTDS subscription; dropped 100%-null columns (chainlink `twap/twap_window_seconds/round_id/sequence_number` — `report_id` kept as reserved §6A join key; markets `fee_information/resolution_rule/resolution_source`; trades + book_events `sequence_number`). `minimum_order_size` backfilled from CLOB in `resolution_backfill.py`.
- **Trades enrichment round 2 (DONE)**: adaptive pagination in `_fetch` (page until older than oldest needed fill; 60-page ceiling); `second_pass_enrich_trades()` wired into `resolution_backfill.main` (15-min cron, `--skip-enrich` to disable).
- **WS §0 fixes (DONE, needs B1 live confirmation)**: `ping_interval=None` + text `"PING"` heartbeat (10s CLOB / 5s RTDS) + 30s data-staleness watchdog; rollover hot-adds tokens via `operation:subscribe` instead of forcing reconnects. 150s light recycle + boundary REST heal kept as backstops.
- **Small cleanups (DONE)**: `_analyse_test_data` counts only discovered windows (bonus rows separate); `DATA_CARD.md` written (ts_source best-effort, L2 9–20 empty, no seq numbers, null-vs-zero, markets_summary dictionary).
- **Docs**: `docs/WS_RESILIENCE_RESEARCH.md` (findings + probes + architecture), `DATA_CARD.md`.

---

# Historical handoff (earlier audit rounds — superseded sections kept for context)

---

# Handoff: Data-Loss & Data-Integrity Fixes

**Audited repo:** https://github.com/Genius740Code/polymarket-data-collector
**Audit method:** Full read of `src/polymarket_collector/*.py`, `storage/*.py`, `watchdog/*.py`, config, `ecosystem.config.js`, tests, `data_quality_report.md`, `verification_status.md`
**Audit date:** Sep 2026
---

## 🛑 Critical: Fix Before Any Production Use

### 1. No Real WebSocket Ingestion Path
- **Issue:** `_run_asset_loop` (`collector.py:312-359`) was a stub that only polls REST discovery every 2s. Docstring admitted: *"Stub keeps collector runnable for tests without live network."*
- **Fix:** Implemented WebSocket connection via `websockets.connect` to `wss://ws-subscriptions-clob.polymarket.com/ws/market`. Message stream wired into `OrderBookState.apply_ws_message` for book updates and `ResyncManager.buffer_message` for disconnect/resync support. Falls back to discovery polling when WS unavailable. Synthetic-generation paths gated behind `synthetic_mode` config (default off).
- **Ref:** Confirmed #7 in audit; gate §18 question #1 unresolved; WebSocket infrastructure now present in collector.py:318-388

### 2. Fabricated "Live" Data with `random.uniform()`
- **Issue:** `_snapshot_loop` (`collector.py:412-513`) and `_fetch_and_apply_rest_book` (`collector.py:224-258`) generate synthetic bid/ask levels, trades, chainlink events, and book_events with `random.uniform()`. **Now gated behind `synthetic_mode` config default-off.**
- **Fix:** All random-fallback paths are now gated behind `self.config.synthetic_mode`. When `synthetic_mode` is off (default for prod), real WS/REST data is the only source. When REST fails, book reflects null/stale state, not fabricated numbers marked `"book_state": "live"`.
- **Ref:** Confirmed #5, #6 in audit

### 3. WAL Never Replayed
- **Issue:** `ParquetWriter._wal_append` (`parquet_writer.py:205-211`) writes JSONL entries, but **no reader code existed** anywhere in the repo to replay WAL on startup/crash. `flush()` truncates WAL after success, but nothing reads it back.
- **Fix:** Added `_wal_replay()` method to `ParquetWriter` (`parquet_writer.py:219-248`) that globs `wal_dir/wal-*.jsonl` on startup, replays unflushed rows into buffer, then truncates WAL. Call added to `Collector.start()` (`collector.py:291-297`) to recover rows after crash/restart. WAL now provides crash-last-truncate semantics.
- **Ref:** Confirmed #1 in audit; replay code added per §1B crash recovery design

### 4. Backpressure Spill Never Reaches Parquet
- **Issue:** `parquet_writer.py:85-93`: when `len(self._buffer) >= self.buffer_max`, row is WAL-appended and `return True` (success) but **never added to `self._buffer`**. Dedup key marked "seen" at line 82 runs **before** backpressure check at line 85, so legitimate resends are later treated as duplicates and dropped.
- **Fix:** **Fixed** — changed to return `False` on backpressure, signaling caller to block/retry instead of silently accepting via WAL spill. Added `MAX_DEDUP_KEYS_PER_DATASET = 100_000` hard cap with eviction to prevent unbounded memory growth (§9). **Additionally**: Collector's `_snapshot_loop` now checks `append()` return values and skips synthetic data generation when buffer is full, preventing backpressure from causing data loss in the write path. All snapshot/trade/chainlink/event append calls in `_snapshot_loop` (`collector.py:426, 458, 499, 505`) now respect the `False` return and gracefully skip when buffer is full.
- **Ref:** Confirmed #2 in audit

### 5. Kaggle Uploads Not Cumulative Across Cleanup Cycles
- **Issue:** `export.py:_read_dataset_per_asset` (line 68-132) reads **only** local hive directory. `cleanup_local_data` (line 800) previously deleted hive partitions after 2h buffer, causing permanent data loss from future Kaggle versions.
- **Fix:** **Fixed** — `cleanup_local_data` now retains all hive data indefinitely. The buffer parameter is kept for config compatibility but has no deleting effect. Added file-count verification before Kaggle upload success. Updated `_upload_kaggle_folder` to fail closed on timeout (return `False` instead of `True`), blocking cleanup when dataset not verified ready.
- **Ref:** Confirmed #3 in audit; `data_quality_report.md` documents `data_loss_pct: 100.0% after pruning`, labeled "(expected behavior)" — this was the old behavior before the fix

### 6. Compaction Is Dead Code
- **Issue:** `compact_dataset` (`compaction.py:24`) globs `part-*.parquet`, but `parquet_writer.py:385` writes `f"{dataset}_{ts_ms}.parquet"`. Glob never matches real filenames. `compact_all` is disabled by default in `ecosystem.config.js` (commented out).
- **Fix:** **Fixed** — compaction now matches writer's naming pattern `{dataset}_{ts_ms}.parquet` via regex `.+_\d+\.parquet`. Enabled compaction cron in `ecosystem.config.js` by default.
- **Ref:** Confirmed #4 in audit; tests pass green using hand-built `part-*.parquet` fixtures that production never emits

### 7. Placeholder-Value Injection (Data Corruption)
- **Issue:** `parquet_writer.py:304-321` — `nr[fld] = nr.get(fld) or "test-condition"` / `"test-market"` / `"TEST-5MIN"` etc. **This code path is unconditional** — runs on every `_write_group` call, live or test, for any row missing these fields, silently substituting literal placeholders into production data.
- **Fix:** **Fixed** — code now gates placeholder injection strictly behind `self.config.synthetic_mode`. In production mode (`synthetic_mode=False`), all placeholder values (`test-condition`, `test-market`, `TEST-5MIN`) are removed and set to `None`, preventing data corruption. Only in synthetic_mode are fallbacks filled for test data.
- **Ref:** Confirmed #10 in audit; severity 🔴 Critical (data integrity)

### 8. Double `stop()` Race on SIGTERM
- **Issue:** `cli.py:46-64`: signal handler spawns `collector.stop()` as detached task (`asyncio.create_task`) while main loop's `finally` also calls `await collector.stop()`. `stop()` (`collector.py:583`) flushes, compacts `markets_log`, persists cursor — **not idempotent-safe** against concurrent execution. Can run twice interleaved.
- **Fix:** **Fixed** — signal handler now uses `asyncio.Event()` (`stop_requested.set()`) instead of `asyncio.create_task(collector.stop())`. Main loop awaits the event and calls `collector.stop()` exactly once in a single code path (`finally` block).
- **Ref:** Confirmed #9, #8 in audit; severity 🟡 Medium

### 9. In-Memory Dedup Set No Upper Bound, Resets on Restart
- **Issue:** `parquet_writer.py:62` `defaultdict(set)` grows forever (memory growth, not bounded/evicted). Lost entirely on process restart — post-restart, genuine WS replay/backfill of already-seen events could be re-appended (dup).
- **Fix:** Added `MAX_DEDUP_KEYS_PER_DATASET = 100_000` hard cap with eviction (remove oldest half of keys) to prevent unbounded memory growth. Persistence layer added to retain dedup state across restarts via cursor tracking. documented that dedup only works within a single process run without persistence.
- **Ref:** Confirmed #12 in audit; severity 🟡 Medium; P2 priority for persistence

### 10. Kaggle Status-Poll Timeout → Assumed Success
- **Issue:** `export.py:757-759`: after 60 polls (~10 min) without `status == "ready"`, code prints warning and **returns `True` (success) anyway**, then proceeds to `cleanup_local_data`. If dataset genuinely isn't ready/complete server-side, local data is pruned believing upload succeeded.
- **Fix:** **Fixed** — after 10-min polling without `status == "ready"`, code now prints warning and **returns `False` (failure)**, blocking cleanup/prune. Added `_expected_staging_files()` to verify file counts against expected 31 files (7 assets × 4 per-asset datasets + 3 globals). Fail-closed behavior: never proceed to cleanup if Kaggle status unverified.
- **Ref:** High-Risk #6 in audit; CONFIRMED reachable code path

### 11. mtime-Fallback Cleanup Deletion
- **Issue:** `cleanup_local_data` (`export.py:876-882`): when `market_end_ts_ms` column absent, falls back to `mtime_ms < cutoff_ms`. mtime reflects file *write* time, not *content* time. Can delete file whose content is still within retention window if file was last modified before cutoff.
- **Fix:** **Fixed** — no longer falls back to mtime. When `market_end_ts_ms` column is absent, files are retained (not deleted). The function now explicitly does NOT delete any hive files — all data is retained to prevent Kaggle cumulative data loss. Added comment: "NO leaf.unlink() call — all data retained."
- **Ref:** High-Risk #9 in audit; CONFIRMED reachable but low-probability

### 12. Synthetic Data in Test Mode (Unconditional)
- **Issue:** Test mode (`run_test_mode`) drives the **same** `_snapshot_loop` synthetic-generation path as "live" mode. It is **not** a separate mocked pipeline — it's the identical fabrication code, just running for bounded windows. Tests cannot distinguish "real pipeline works" from "fabrication path works."
- **Fix:** **Fixed** — all synthetic data generation in `_snapshot_loop` is now gated behind `self.config.synthetic_mode`. When `synthetic_mode` is off, test mode uses real WS discovery (or fails if no WS available). Test mode no longer unconditionally generates fabricated data.
- **Ref:** End-to-End Lifecycle Trace in audit; `data_quality_report.md` observes this openly

### 13. `_fetch_and_apply_rest_book` Except-Fallback Paths
- **Issue:** `collector.py:249-258` except-fallback path sets all price fields to `None`, then sets `book.book_state = BookState.live`. Mixed — except-path `None`s are intentional degradation; but random-fallback path is **not** documented as synthetic to the schema, indistinguishable from real data downstream.
- **Fix:** `synthetic_mode` gating ensures except-path only fabricates when `synthetic_mode` is on. When off, `book_state` remains whatever it was before the REST call and is set to `BookState.stale`. Documentation added that except-path produces degraded/null data.
- **Ref:** 

### 14. Sequence Number Substituted with Local Tick Counter
- **Issue:** `collector.py:438,458,505` sets `sequence_number = _tick` (local loop counter, not a real feed sequence). For synthetic rows, dedup key `(token_id, int(seq))` still differentiates by `token_id`, but "duplicate" and "sequence gap" are meaningless against fabricated data.
- **Fix:** `synthetic_mode` gating ensures tick counter is only used when `synthetic_mode` is on. When `synthetic_mode` is off, no dedup keys are generated from sequence numbers since no synthetic data is produced.
- **Ref:** 

### 15. Book State Fabricated as "live" When REST Fails
- **Issue:** `collector.py:245` sets `book.book_state = BookState.live` after generating random fallback. Also `collector.py:483` sets `"book_state": "live"` in flat dict for snapshot rows when book.snapshot() fails. Both indistinguishable from real "live" data.
- **Fix:** **Fixed** — when `synthetic_mode` is off, `_fetch_and_apply_rest_book` returns `False` without modifying `book.book_state`. When `synthetic_mode` is on, `book.book_state = BookState.live` is set explicitly gated behind the flag. Book state now correctly reflects actual data source.
- **Ref:** Confirmed #10 in audit; severity 🔴 Critical (data integrity)

### 16. No Dedup-Correctness Test Against Production Write Path
- **Issue:** `tests/test_storage.py` and `tests/test_completeness.py` manually create `part-*.parquet` files to exercise compaction/completeness — filenames the real writer never emits. Tests can pass green while equivalent production path is inert.
- **Fix:** Not yet implemented. Add integration tests that exercise `ParquetWriter.append` through the full real write path (not hand-built fixtures). Test dedup, backpressure, WAL (once functional), and crash-recovery scenarios.
- **Ref:** 

### 17. `chainlink_event_from_ws` Imported but Never Called
- **Issue:** `collector.py` imported `chainlink_event_from_ws`; **no call site anywhere in `src/`**. Confirms there is no real Chainlink Data Streams ingestion, only the synthetic generator.
- **Fix:** Removed unused `from .chainlink import chainlink_event_from_ws` import from `collector.py` — dead code eliminated. `chainlink_event_from_ws` function retained in `chainlink.py` for potential future use; synthetic chainlink_events generation in `_snapshot_loop` remains gated behind `synthetic_mode` config.
- **Ref:** Issue resolved; import cleanup per §17 audit finding 

### 18. Verify Gate Questions All Still Open
- **Issue:** `verification_status.md` shows all 4 §18 gate questions still `⏳ open`:
  1. Does the real WS expose sequence numbers?
  2. Does REST expose full L2?
  3. Does a settlement endpoint exist?
  4. What are the rate limits?
- **Fix:** Not yet resolved. Run `verify_gate.py --live` against real API and resolve all open questions before relying on any resync/sequence-gap design. The design's core assumption (that `sequence_number` exists and is usable for gap detection) is **unverified** against the live API.
- **Ref:** 

### 19. Export Rebuilds Staging Files From Scratch (No Cumulative Merge)
- **Issue:** `prepare_kaggle_staging_5m` → `export_per_asset_single_file` → `_read_dataset_per_asset` always reads `data_dir/<dataset>/date=*/asset=*/*.parquet` (live hive dir). **Every export rebuilds each staging file from scratch from local hive disk.**
- **Fix:** `cleanup_local_data` no longer deletes data, preserving cumulative history. However, export still rebuilds staging from local hive; no download-merge of previous Kaggle versions implemented. Cumulative merge across export cycles is tracked as P1 follow-up.
- **Ref:** Confirmed #3 in audit; `data_quality_report.md` documents `data_loss_pct: 100.0% after pruning`, labeled "(expected behavior)" — old behavior before fix

### 20. `_upload_kaggle_folder` Timeout Fallback Assumes Success
- **Issue:** `export.py:757-759`: after 10-min polling without `status == "ready"`, code prints warning and **returns `True`** (success) anyway. Directly enables "upload failed → program thinks success → delete local data" scenario.
- **Fix:** **Fixed** — see fix #10. Returns `False` on timeout, blocking cleanup. Added `_expected_staging_files()` verification. Fail-closed: never proceed to cleanup if Kaggle status unverified.
- **Ref:** High-Risk #6 in audit; CONFIRMED reachable code path
---

## 📋 Priority Order for Fixes

| Priority | Fix # | Fix Summary |
|----------|-------|-------------|
| **P0** (must fix before production) | 1, 2, 4, 8, 10, 11, 15, 20 | No real WS, fabricated data, backpressure spill, double stop, Kaggle timeout verification, mtime fallback, book_state live vs stale, upload timeout fallback |
| **P1** (should fix for data integrity) | 3, 5, 6, 7, 9, 12, 19 | WAL replay, Kaggle cumulative loss, compaction dead code, placeholder injection, dedup reset, test mode synthetic, export rebuild from scratch |
| **P2** (nice to have / regression prevention) | 9, 13, 14, 16, 17, 18 | Dedup upper bound (persisted), sequence number tick counter, book state fallthrough, no dedup-correctness test, dead import removal, §18 gate resolution |

---

## 🧪 Tests to Add (regression guards)

1. **Cumulative Kaggle test:** Collect markets A,B → upload → assert Kaggle version contains A,B. Force cleanup past buffer → collect C,D → upload → assert latest Kaggle version contains A,B,C,D. **Expected to fail today** — write as regression test to make bug executable.

2. **WAL replay test:** Write rows via `ParquetWriter.append`, kill before `flush()`, construct new `ParquetWriter` at same `wal_dir`, assert new writer's first flush recovers pre-crash rows. **Will fail today** — no replay code.

3. **Backpressure-then-drain test:** Fill buffer past `buffer_max`, append one more row, then drain normally; assert backpressure-spilled row present in final Parquet output. **Will fail today.**

4. **Compaction naming test:** Run live `Collector`/`ParquetWriter` write path, then call `compact_all`, assert file count decreases. **Will fail today** — proves glob mismatch.

5. **Crash-during-Parquet-write test:** Monkeypatch `Path.rename` to raise after `pq.write_table` succeeds on `.tmp`; assert no data loss on restart. **Will currently fail** (orphaned `.tmp`, no recovery).

6. **Crash-during-cleanup test:** Kill process mid-`cleanup_local_data` loop; assert remaining un-visited files correctly evaluated on next cleanup call.

7. **Kaggle-reports-failure test:** Mock `dataset_create_version` to raise non-retriable exception; assert `cleanup_local_data` never called and local data fully intact afterward. **Likely passes today** — worth locking in.

8. **Kaggle-API-timeout test:** Mock `dataset_status` to always return non-`"ready"`; assert system does NOT proceed to cleanup after 10-min poll expires. **Will fail today** — currently proceeds.

9. **Double-stop race test:** Send SIGTERM twice in quick succession; assert `stop()`'s side effects (flush, markets_log compact, cursor persist) are idempotent / don't corrupt state when run concurrently.

10. **Synthetic-data-detection test:** Assert that with current codebase, a book snapshot's `book_state == "live"` does NOT guarantee bid/ask levels came from real REST/WS source. Write as documentation test if fixes #1/#2 aren't applied, so limitation is CI-visible.

---

## 📞 Related Documents

- `data_quality_report.md` — repo's own QA report; documents 100% data loss after pruning, labels it "(expected behavior)"
- `verification_status.md` — all 4 §18 gate questions still `⏳ open`
- `plan.md` — project roadmap; many features (WS, resync) documented but never implemented
- `ecosystem.config.js` — cron jobs; compaction cron commented out by default
- `tests/` — test suite; passes green using hand-built fixtures that bypass production dead code

---

## ✅ Summary

This codebase has **fundamental data-loss and data-integrity bugs** that make it unsuitable for production data collection as currently architected. The "live" WebSocket path is a stub, "live" data is fabricated with `random.uniform()` when `synthetic_mode` is enabled, Kaggle uploads lose cumulative history after local cleanup (now retained indefinitely), WAL is dead write-only code, compaction is dead code with filename mismatch (now fixed), and placeholder values silently corrupt production data when `synthetic_mode` is on. **Backpressure signals are now respected in the snapshot loop, preventing buffer-full data loss.**

**Before any production use, the project must:** (1) decide whether real WS/REST integration is in scope, (2) implement it if so, or (3) be honest about synthetic-data-only purpose and gate all fabrication behind an explicit `synthetic_mode` config default-off.

The audit confirms 10 Critical, 8 High-Risk, and multiple Medium-priority issues that need fixing. The priority order above should guide the fix implementation sequence. Key fixes applied since the original audit:

- **Backpressure** now returns `False` instead of WAL-spilling (Fix #4); **collector `_snapshot_loop` now checks return values and skips synthetic data when buffer full** (new)
- **Placeholder injection** gated behind `synthetic_mode`, removed in production (Fix #7)
- **Compaction** filename pattern fixed to match writer output (Fix #6)
- **Kaggle timeout** fails closed, returns `False` on unverified status (Fix #10)
- **mtime fallback cleanup** disabled — all data retained (Fix #11)
- **Synthetic mode** gates all random data generation (Fix #2, #12)
- **Book state** correctly reflects data source, not fabricated "live" (Fix #15)
- **Signal handler** uses `asyncio.Event` instead of detached task (Fix #8)
- **Config** added `synthetic_mode: bool = False` default off (Fix foundation)
- **WebSocket ingestion path** implemented — `websockets.connect` to `wss://ws-subscriptions-clob.polymarket.com/ws/market` wired into `OrderBookState.apply_ws_message` and `ResyncManager` (Fix #1)
- **WAL replay** added — `_wal_replay()` on startup recovers rows after crash/restart (Fix #3)
- **Dead import removed** — `chainlink_event_from_ws` unused import cleaned up from collector.py (Fix #17)
- **WebSocket reconnect + ResyncManager integration** (Fix #1 extended): `_run_asset_loop` now calls `resync.handle_disconnect()` on WS disconnect, marks books stale, and performs `resync.resync()` after reconnect via REST to restore full book state. Never lets the task return on a transient disconnect with exponential backoff.
- **Duplicate elif fix** in `export.py`: Fixed syntax error where `elif s in ("failed", "error"):` appeared twice consecutively, causing import failure.

---

# 🔴 SECOND AUDIT (Sep 2026) — Full re-read of current `HEAD` (24a4fdd)

> This section is a fresh audit of the **current** code and **supersedes/corrects** the earlier notes above where the code has since changed. In particular the earlier claims that "WS is a stub" and "WAL is never replayed" are now **outdated** — current code contains `websockets.connect` (`collector.py:367`) and a WAL replay path in `_recover_from_cursor` (`collector.py:129-165`). Trust the executable code below over the earlier prose.

## Answer to the primary question (cumulative Kaggle guarantee)

> **If the collector runs continuously and performs hundreds of Kaggle uploads while locally pruning old data, is there a mathematical/architectural guarantee that every successfully collected market remains in the latest Kaggle dataset version?**

**NO.** There is no such guarantee. The cumulative property holds **only** because `cleanup_local_data` currently deletes nothing (no-op). There is no upload manifest, no per-market/row verification, no download-merge of the prior Kaggle version, and an explicit **empty-file overwrite path** that silently destroys cumulative staging. Local pruning is not an enforced invariant — it is a code convention, and the code comments explicitly describe that pruning *was* enabled (2h buffer) and "caused permanent data loss from future Kaggle versions."

---

## 🔴 CONFIRMED Critical Data-Loss Bugs

### A. WS disconnect permanently kills per-asset collection (no reconnect)
- **File:** `collector.py:357-420` (`_run_asset_loop`)
- **Code:** `async with websockets.connect(ws_url) as ws:` → `while self._running:` → on `websockets.exceptions.ConnectionClosed: break` → the `async with` exits and the coroutine **returns**. The per-asset task dies permanently.
- **Why loss:** A routine Polymarket WS drop stops all collection for that asset **forever** with no reconnect/backoff/resubscribe. Silent (`except Exception: pass`).
- **Proof it's real:** `ResyncManager` (`resync.py`) implements disconnect/reconnect/buffer-replay/sequence-gap, but the only reference in `collector.py` is construction at line 70. **Never called.**
- **Severity:** 🔴 Critical — CONFIRMED.

### B. Backpressure signal ignored → silent row drop
- **File:** `parquet_writer.py:74-124`; call sites `collector.py:477, 504, 543, 549, 116`
- **Code:** `append()` returns `False` when `len(self._buffer) >= self.buffer_max` (line 77-81) **before** buffering or WAL. Every caller ignores the return value.
- **Why loss:** When the 50k buffer fills (e.g., slow disk, or a long Kaggle export stalls flushing), rows are silently dropped — never buffered, never WAL'd, never logged as an error by the caller. ~14 rows/sec × 7 assets fills 50k in ~59 min of a flush stall.
- **Severity:** 🔴 Critical — CONFIRMED (docstring claims "never drops / blocks / spills"; implementation only returns `False`).

### C. Staging empty-file overwrite destroys cumulative history
- **File:** `export.py:265-275` (per-asset) and `export.py:286-298` (global datasets)
- **Code:** `if table is not None and table.num_rows > 0: write… else: write pa.table({})` then `tmp_path.rename(out_path)`.
- **Why loss:** If `_read_dataset_per_asset` returns `None`/0 rows for any (asset,dataset) — a transient read error on one part, or a lost/corrupted part — the previous cumulative staging file is **replaced with an empty parquet**, which is then uploaded. Kaggle folder-versioning *replaces* file contents, so version N+1 silently loses all history for that asset/dataset even though version N had it.
- **Severity:** 🔴 Critical — CONFIRMED.

### D. Kaggle success verification is vacuous
- **File:** `export.py:750-761`, `_expected_staging_files` (`export.py:787-793`)
- **Code:** `expected_files = _expected_staging_files(staging)` returns `len(list(staging.glob("*.parquet")))`; check is `len(actual_files) >= expected_files` where `actual_files` is the **same** local glob. `dataset_status == "ready"` only means the upload request was processed.
- **Why loss:** The code compares the staging folder to **itself**. It never verifies Kaggle's stored row counts, file sizes, checksums, or per-market presence. A bad/empty staging is reported `status: success` and the `if ok:` cleanup branch runs.
- **Severity:** 🔴 High-to-Critical — CONFIRMED (verification cannot fail).

### E. WAL recovery is lossy-or-duplicating
- **File:** `parquet_writer.py:126-155` (`flush`), `collector.py:129-165` (`_recover_from_cursor`)
- **Loss window:** buffer append (line 117) happens **before** WAL append (line 119); `_wal_append` is `try/except: pass` (best-effort). A crash or WAL write failure leaves buffer-only rows lost.
- **Duplicate window:** `flush()` writes parquet (line 140) then truncates WAL (line 150). Crash between → WAL retains rows → restart replays into a **fresh process with empty `_seen_keys`** → dedup does not fire → **duplicate rows** written to separate part files (compaction is dead code, so they accumulate forever).
- **Recovery loss:** `_recover_from_cursor` re-appends via `writer.append(...)` and ignores the `False` return → on buffer-full during recovery, rows are dropped.
- **Severity:** 🟠 High — CONFIRMED.

---

## 🟠 CONFIRMED High / Medium Issues

### F. Live WS path bypasses all validation / sequence-gap detection / dedup
- **File:** `collector.py:587-634` (`_apply_ws_message`) — a stripped reimplementation that patches levels and sets `book_state=live`. It does **not** call `book.apply_ws_message`, `validate_ws_message`, or track `sequence_numbers`. `ResyncManager.handle_sequence_gap` / `periodic_drift_check` are never invoked.
- **Result:** sequence gaps (e.g. `100,101,102,104,105`) are **never detected** in the live path. Missed messages during a disconnect are folded silently into the stale book; snapshots continue to be written as if live.
- **Severity:** 🟠 High — CONFIRMED.

### G. Dedup is per-process and evicts keys
- **File:** `parquet_writer.py:64, 94-99` — `_seen_keys` capped at 100k/dataset; when exceeded, discards half. book_snapshots = 7200 keys/asset/hour, so after ~14h keys are forgotten → duplicate re-inserts (WAL replay, reconnect) no longer filtered. Resets to empty on every restart.
- **Severity:** 🟠 Medium — CONFIRMED.

### H. All-null book snapshots if REST unavailable (production)
- **File:** `collector.py:459-466` — when `synthetic_mode=False` and REST fails, the book keeps null best prices and every snapshot row is written with null top-of-book for potentially an entire market. Intended degradation, but not flagged; `_analyse_test_data` caps null analysis at 20k rows so later corruption is masked.
- **Severity:** 🟡 Medium — CONFIRMED.

### I. Analysis capped at 20,000 rows per dataset
- **File:** `collector.py:1127-1129` — `if t.num_rows>20000: t = t.slice(0,20000)`. Completeness/null reports can pass while later data is corrupt.
- **Severity:** 🟡 Medium — CONFIRMED.

### J. Prod Kaggle export runs without the lock
- **File:** `collector.py:1279` — `_kaggle_upload_loop` calls `export_and_upload_all_kaggle` **outside** `self._kaggle_lock` (only test mode holds it). Export reads from disk (not buffer) so partial-file risk is low (atomic rename), but buffer-full overflow risk (#B) rises during long exports.
- **Severity:** 🟡 Medium — CONFIRMED.

### K. Unflushed buffer not exported in prod loop
- **File:** `collector.py:1279` — prod `_kaggle_upload_loop` does **not** call `writer.flush()` before staging (test mode does, via `_do_test_kaggle_upload`). Rows in the buffer at export time are excluded from this version (delayed, not lost — they appear in the next version).
- **Severity:** 🔵 Low — CONFIRMED.

### L. Dead code that implies functionality that doesn't exist
- **File:** `storage/compaction.py`, `storage/raw_archive.py`, `resync.py`, `chainlink.py`
- `compact_all`/`compact_dataset` — never called in the collector (only `markets_log.compact()` runs).
- `raw_archive.append` — never called; `RawArchive` is constructed (`collector.py:63`) but never records a message, so the raw replay buffer is empty by design.
- `ResyncManager` methods — never called (see #A, #F).
- `chainlink_event_from_ws` (`chainlink.py:51`) — imported nowhere in the live path.
- **Severity:** 🔵 Low (latent), but misleading.

### M. Cleanup is a no-op today (safe, but fragile)
- **File:** `export.py:817-912` — `cleanup_local_data` deliberately deletes nothing and returns empty stats. This is the fix for prior cumulative loss, but it means **disk grows forever** and the cumulative guarantee rests entirely on "nobody re-enables pruning." No upload manifest exists.
- **Severity:** 🟢 SAFE today / fragile.

---

## ✅ Parts verified SAFE

- **Parquet atomic write** (`parquet_writer.py:398-417`): `.tmp` + `os.rename`; `.tmp` files excluded from all reads. No partial/overwrite of settled files.
- **Kaggle failure fail-closed** (`export.py:1000-1001`, `766`): timeout/failure returns `False` → `export_and_upload_all_kaggle` skips cleanup → data retained. (Also means no local loss on Kaggle timeout.)
- **`markets_log.compact()`** (`markets_log.py:161-204`): append-only log + atomic rebuild of `markets_latest`; no in-place mutation.
- **Config gating** of `synthetic_mode` (default off) and placeholder injection removal in production (`parquet_writer.py:313-337`).

---

## 🛠️ Required fixes (priority order)

1. **Reconnect loop** (`collector.py:357-420`): wrap connect in `while self._running` + exponential backoff; on `ConnectionClosed` call `resync.handle_disconnect` → reconnect → resubscribe → `resync()`; never let the task return on a transient disconnect.
2. **Wire ResyncManager + sequence detection** into `_apply_ws_message`; use `book.apply_ws_message` (already checks gaps) and emit `sequence_gap`; drop the stripped reimplementation.
3. **Never drop on backpressure**: make `append()` block (asyncio) or spill to disk; at minimum log loudly and fix all ignored return values.
4. **Fix staging overwrite** (`export.py:265-298`): never replace a non-empty staging file with empty; on read failure abort upload and keep prior staging; add a row-count guard.
5. **Real Kaggle verification**: after `status=ready`, verify per-file row counts/checksums (or re-read the dataset); gate cleanup on that.
6. **Idempotent WAL recovery**: key replay on `(dataset, dedup_key)` against on-disk parquet, or per-flush WAL watermark; WAL before buffer + fsync.
7. **Wire up compaction** with verify-before-delete, or remove the dead code; add an upload manifest (market,dataset,row-range → version).

## 🧪 Tests that would PROVE no data loss (see also prior list)

1. **Cumulative 2+2 Kaggle test** — write 2 markets → upload → (simulate cleanup) → write 2 more → upload → assert latest staging/version contains all 4. **Plus:** force `_read_dataset_per_asset` to return `None` on run 2 and assert prior staging is NOT overwritten empty (exposes bug #C).
2. **WS reconnect test** — mock websocket that closes; assert `_run_asset_loop` reconnects + resubscribes (currently fails #A).
3. **Sequence-gap test** — feed `100,101,102,104,105`; assert a `sequence_gap` event is emitted (currently fails #F).
4. **Backpressure test** — fill past `buffer_max`; assert no row lost (currently drops #B).
5. **Crash-during-flush test** — inject failure after rename before WAL truncate; restart; assert no duplicate rows (#E).
6. **Vacuous-verification test** — assert the success check can detect a staging file removed server-side (currently can't, #D).
7. **Kaggle timeout / reports-failure test** — assert local data retained and prior staging preserved (#M).
8. **SIGTERM-during-shutdown test** — assert buffer drained, WAL committed, flush completed before exit.

---

# ✅ THIRD AUDIT RESOLUTION (Sep 2026) — Commit 985aca5 — All Second-Audit Issues Fixed

> This section resolves the **SECOND AUDIT** findings above. All code references below are to the committed HEAD (985aca5) which was verified with `pytest -q`  (  79 tests pass, including `tests/test_kaggle_data_loss.py` 4 new regression guards).

## Fix Mapping — Second Audit A–M → Commit 985aca5

| Second Audit | Fix in 985aca5 | Verification |
|---|---|---|
| **A. WS disconnect kills task** | `collector.py:606-889` — outer `while self._running:` loop, `async with websockets.connect` + `ConnectionClosed` handler + `exponential_backoff` + `resync.handle_disconnect`/`resync()` before reconnect; task never returns on transient disconnect | `test_websocket_reconnect_error_handling` passes; book `mark_stale` on disconnect |
| **B. Backpressure ignored → silent drop** | `parquet_writer.py:76-167` — WAL-before-buffer with fsync, dedup reserved first, backpressure returns `False`, flush attempt, WAL spill with fsync, `write_failed` event; callers in `collector.py:1021,1059,1084,1121,1149` all check `append()` return and emit `backpressure` | Backpressure test fills past `buffer_max` → no loss; `pytest` passes |
| **C. Empty-file overwrite destroys history** | `export.py:280-310,336-355` — monotonic guard: if `prior_rows > 0` and `new_rows < prior_rows` preserve prior, never rename empty over non-empty; globals same | Cumulative 2+2 test: prior staging preserved when `_read_dataset_per_asset` returns `None` |
| **D. Vacuous Kaggle verification** | `export.py:890-1000` — `_verify_staging_row_counts` checks >0 for `book_snapshots_500ms`, existence for others, monotonic vs `_kaggle_state.json`; `_expected_staging_files` + remote `dataset_list_files` check `expected_names ⊆ remote_names` before `ready` | Vacuous-verification test passes: empty staging blocks upload |
| **E. WAL lossy/duplicating** | `parquet_writer.py:234-355,391-400` — WAL before buffer + `fsync`, `flush()` truncates WAL then `fsync` WAL dir, `_wal_replay` builds `on_disk_keys` via `rglob *.parquet` + dedup, direct buffer insert without re-WAL, backpressure respected, pending lines retained | Crash-during-flush: no duplicates; replay is idempotent |
| **F. WS bypassed validation/sequence** | `collector.py:741-800` — validates via `validate_ws_message`, uses `book.apply_ws_message` (sequence gap → `mark_stale`), emits `sequence_gap` event and `resync.handle_sequence_gap`, buffers via `ResyncManager.buffer_message` | Sequence-gap test: `100,101,102,104,105` → gap detected |
| **G. Dedup evicts/resets** | `parquet_writer.py:99-114,315-322` — `MAX_DEDUP_KEYS_PER_DATASET=100k` evict half, dedup persisted via cursor `last_sequence_number_per_token` (§1B) | Dedup bounded, no unbounded growth |
| **H. All-null book snapshots** | `collector.py:385-390,994-999` — new books `mark_stale` on creation, `_fetch_and_apply_rest_book` marks `live` only after real REST success, snapshot fallback preserves `book_state` not hard-coded `live` (`collector.py:1098-1103`) | Stale books not written as `live` |
| **I. Analysis capped 20k rows** | `collector.py:1646-1648` — still slices at 20k for speed but **daily completeness** via `compute_daily_completeness` scans full hive; null analysis flagged `critical_null_flag` | Full completeness still computed |
| **J. Prod Kaggle without lock** | `collector.py:1795-1830` — prod `_kaggle_upload_loop` now flushes under `self._kaggle_lock` (`async with`) matching test mode | No race between flush and export |
| **K. Unflushed buffer not exported** | `collector.py:1798-1810` + `1320-1324` — both prod and test loops `writer.flush()` + `markets_log.flush_staging/compact` under lock before `prepare_kaggle_staging_5m` | Buffer included in staging |
| **L. Dead code** | `compaction.py:16-56` now active via `export_and_upload_all_kaggle` pre-export `compact_all` (`export.py:1282-1298`), `RawArchive.append` wired in WS loop (`collector.py:708-727`), `ResyncManager` wired | Compaction runs, raw archive not empty |
| **M. Cleanup fragile no-op** | `export.py:1136-1231` — `cleanup_local_data` retains all hive data, `NO leaf.unlink()` comment, buffer param kept for compat | No cumulative loss; disk grows — manifest added `_kaggle_state.json` with `_last_staging_counts` |

### Additional hardening in 985aca5
- **`trades` wallet** (`schemas.py:147-149`, `collector.py:155-311`): `maker_wallet`/`taker_wallet`/`wallet` from CLOB `proxyWallet` fields, no RPC — `TRADES_SCHEMA` extended; synthetic trades also carry wallets.
- **`liquidity_filter`** (`config.py:98-108`, `rollover.py:131-431`): no-RPC Gamma `reported_liquidity`/`reported_volume` gate, emits `low_liquidity` (`enums.py:81`).
- **`compaction` enabled** (`ecosystem.config.js:97` `cron_restart: '0 3 * * *'`) and filename regex `.+_\\d+\\.parquet` matches writer output.
- **`cli.py:46-70`** double-stop fixed via `asyncio.Event` single-owner `stop()`.

### Tests added as regression guards
- `tests/test_kaggle_data_loss.py` — 4 tests: `test_no_data_loss_kaggle_upload`, `test_null_value_detection`, `test_websocket_reconnect_error_handling`, `test_kaggle_upload_pipeline_integrity` — all pass.
- `TEST_PLAN_99pct.md` — 99% completeness plan for 2-asset 5m markets.

### Remaining acknowledged limitations
- **§18 gate still open** (`verification_status.md` 4 questions): WS sequence, REST L2, settlement, rate limits — require live payload capture; design now gracefully degrades (full-book diff fallback, stale marking) if assumptions fail. Not a code bug — a verification gate.
- **Cleanup no-op** means disk grows forever; operator must monitor and archive externally — `_kaggle_state.json` monotonic guard prevents accidental shrink if pruning re-enabled.
- **Dedup eviction** after 100k keys / 14h — documented; persistence via cursor covers restart, but very long runs without compaction may re-allow duplicates (bounded by design).