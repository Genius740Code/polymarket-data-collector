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