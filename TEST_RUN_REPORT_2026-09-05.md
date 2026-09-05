# Test Run Report — 2×5min Live Markets + Kaggle Upload (2026-09-05)

**Command:** `python -m src.polymarket_collector.cli --config config/collector.yaml --test-mode --test-markets 2`
**Scope:** 7 assets (BTC, ETH, SOL, HYPE, BNB, XRP, DOGE) × 2 five-minute windows, one 10-min Kaggle chunk upload, final analysis. Goal: find out whether data survives WS reconnects, market discovery/rollover, and the Kaggle upload path.
**Verdict in one line:** **the 500ms collection engine is solid (window 2 captured 100% raw on every asset it found), but the export→Kaggle→analysis chain is broken end-to-end — the upload failed, staging would have shipped empty files, and the collector's own completeness report says "0% / 100% data loss" while the data sits healthy on disk.**

---

## 1. Run timeline (UTC)

| Time | Event |
|---|---|
| 12:00:21 | Process start, dependencies fixed (`pydantic_settings` missing → `pip install -e ".[dev,kaggle]"`) |
| 12:00:21 | Aligned to next 5m boundary; waited 279s for clean start; cursor recovery: "no cursor for BTC…", "replayed 1 rows from WAL" |
| 12:05:00 | Window 1 (5962033) starts. First snapshots at **12:05:05.5** — ~5.5s cold-start subscription lag |
| 12:05–12:09 | All 7 assets discovered via Gamma slug; 13 `market_added` events total (6 assets also pre-discovered window 2; **HYPE window 2 never found**) |
| 12:10:00 | Rollover → window 2 (5962034) starts **at exactly 12:10:00.0 with 600/600 ticks** on 6 assets — the warm rollover path works perfectly |
| 12:12:31–33 | `ws_disconnected` on 6 assets (HYPE excluded) — see issue I-6 |
| 12:15:03 | t10min chunk upload: staging prepared, **upload failed after 5 retries** ("BTC_book_snapshots_500ms.parquet does not exist") → "NOT pruning (data retained for retry)" |
| 12:16:30 | 690s timeout reached (HYPE never completed window 2, so `all_done` never fired) |
| 12:16:33 | Shutdown; `ws_reconnected` recorded at the same second (see I-6); final analysis written |

## 2. Ground-truth data yield (verified by direct per-file parquet reads, bypassing the partitioning bug)

| Dataset | Real rows on disk | Expected | Notes |
|---|---|---|---|
| book_snapshots_500ms | **7,711** | 7,800 (13 windows × 600) | Window 1: 587/600 per asset (cold start); window 2: **600/600** on all 6 discovered assets |
| book_snapshots_clean | 6,793 live rows | — | Per-market clean completeness only **76–90%** (stale ticking, see I-4) |
| trades | **1,283** (BTC 768, ETH 191, SOL 118, BNB 91, XRP 81, DOGE 34, HYPE 0) | n/a | All prices in [0,1], all sizes ≥ 0 |
| book_events | **0** | thousands | Nothing in the code ever appends to this dataset (I-3) |
| chainlink_events | **0** | ~thousands | No chainlink client is ever started (I-2) |
| markets_log / latest | 13 markets, all stuck `status=active` | lifecycle to resolved | `resolution_outcome=unknown`, no settlement fields (I-2) |
| resync_episodes | 12 rows (6 assets × 2) | — | Internally inconsistent (I-6) |
| collector_events | 458 | — | 245 with unusable null details (I-5) |

**Integrity checks that PASSED (raw snapshot table):** 0 off-grid timestamps (`ts_snapshot_ns % 500M == 0` on all 7,711), 0 duplicate `(condition_id, ts)`, 0 duplicates in trades, 0 depth↔BBO null mismatches, 0 prices outside [0,1], 0 negative sizes, correct hive partitioning by date/asset. **The core snapshot/trade writer is healthy.**

## 3. Issues found

### CRITICAL

**I-1. Export→Kaggle pipeline is broken by a hive-partitioning schema clash — zero data reached Kaggle.**
Every dataset write embeds the `asset` column *inside* the parquet file **and** in the hive path (`asset=BTC`). When the exporter reads with the pyarrow dataset API (`export.py:_read_dataset_per_asset`), pyarrow infers the partition column as `dictionary<string>` while the in-file column is `string` → `ArrowTypeError: Field asset has incompatible types: string vs dictionary` on **every file**. Consequences observed live:
- All 35 trades part files unreadable → staging `*_trades.parquet` written with **0 rows** (1,283 real trades dropped from export).
- `*_book_snapshots_500ms.parquet` staging files **never created at all** → uploader aborted after 5 retries → **no upload happened**.
- Staging actually held 25 files (21 per-asset ×3 + 3 globals + metadata), not the logged "31 files"; every per-asset file had 0 rows.
The only thing that prevented shipping empty data to Kaggle was the pre-upload file-existence check; the `_verify_staging_row_counts` guard was never reached. Unit tests reproduce this: `tests/test_kaggle_data_loss.py` fails with the same read errors.

**I-2. Settlement chain is dead: chainlink client never starts; markets never resolve.**
There is no code path in `collector.py` that starts the chainlink WS task (the only chainlink references are config + analysis fields; a comment says "chainlink via real WS (separate loop) if available" — it never becomes available). Result: 0 chainlink_events, and **all 13 markets remained `status=active` with `resolution_outcome=unknown`** even 90s after their windows ended; 49 `resolution_stuck` events fired. The active→closed→resolved lifecycle and settlement ground truth (§6A) cannot work in this build.

**I-3. `book_events` is written by nothing.**
`writer.append(...)` is called only for `resync_episodes`, `trades`, and `book_snapshots_500ms`. Despite 7,711 snapshots over live books, book_events has 0 rows and the dataset directory is never created. The comment "threshold-driven from apply_ws_message" describes an event path that was never wired to the writer.

**I-4. The collector's own completeness analysis reports fiction: "0 rows, 0% completeness, 100% data loss".**
`_analyse_test_data` / `compute_daily_completeness` read the hive dirs with the same dataset API as I-1, swallow every read error with `except: continue`, and count 0. `data/test_analysis.json` therefore claims `actual_book_snapshots: 0, loss: 100%` while 7,711 snapshots sit on disk. Also `expected_book_snapshots: 8400` counts HYPE window 2, which never existed. **Monitoring numbers cannot be trusted until I-1's root cause is fixed.** (Same root cause makes 4 tests in `test_completeness.py` fail.)

### MAJOR

**I-5. HYPE window 2 (12:10–12:15) silently missing — no attribution.**
6 of 7 assets pre-discovered window 2 during window 1; HYPE never did and collected **0 of 600 ticks**. No `coverage_gap` was emitted in real time (the single coverage_gap event fired at 12:16:18 during shutdown, with `asset=NULL` and null details). Whether the HYPE market didn't exist on Polymarket or Gamma never indexed it is **unverifiable afterwards** — Gamma's `?slug=` query returns `[]` for closed windows (verified: even `btc-updown-5m-1788610200`, which definitely ran, returns `[]`). A missing market must be distinguishable from a missing record; today it is neither logged nor later checkable.

**I-6. WS disconnect/resync telemetry contradicts the data.**
At 12:12:31–33 all assets except HYPE logged `ws_disconnected` (reason `ws_connection_close`). The corresponding `ws_reconnected` events and `resync_episodes.reconnect_ts_utc` are all stamped **12:16:33 — the moment the process shut down** — implying a 240s outage with `resync_attempt_count=0`. But snapshot data kept flowing as `live` through 12:14:59 with only scattered stale ticks (BTC: 57 stale in window 2). So either the episode tracks a connection that wasn't carrying market data, or book-state/stale marking is inconsistent with connection state. Either way: `gap_duration_ms≈240,000`, `attempts=0`, and reconnect-at-shutdown make the resync episode table unusable for loss accounting — and it double-writes each episode (2 rows per asset: one at disconnect, one at "reconnect") which inflates episode counts.

**I-7. Crossed-book anomaly at 3.2% of snapshots — and it's suspicious.**
245/7,711 ticks have `book_crossed=true`, and in **every one of them BOTH books are crossed simultaneously** (`up_bid>up_ask` AND `down_bid>down_ask`) — a state real binary-market order books essentially never sit in, suggesting a book-update/race bug rather than market reality (count also exactly equals the 245 `book_anomaly` events; ETH 63, BNB/DOGE 57, XRP 36, BTC 26, SOL 6, HYPE 0). Budget in `PERFECT_DATA_SPEC.md` is ≤0.01%.

**I-8. Clean view is gutted by scattered single-tick staleness.**
918/7,711 rows (11.9%) are `book_state='stale'`, scattered 1–2 ticks at a time (e.g. BTC stale rows span 12:05:05→12:13:00 across both windows; some with `book_age_ms=0`). Because `book_snapshots_clean` keeps only live rows, per-market clean completeness drops to **76.2–90.5%** (ETH w1: 457/600). The raw capture is 97.8–100%, but the research-facing view loses ~15%.

### MINOR

- **I-9. `_collector_event` discards event payloads:** it stores `details.get("details")` into the details column, so all 245 `book_anomaly` and 41 `backpressure` rows have `details=NULL`. Un-actionable telemetry (we only identified the anomaly meaning by matching counts against `book_crossed`).
- **I-10. `connected` is emitted as a 10-second heartbeat** (`_tick % 20` in the snapshot loop, 75 events), not on connection state changes — masks real reconnects and inflates event volume.
- **I-11. Failed upload counts as a completed chunk:** `kaggle_uploads.append()` runs even on error, so the run-end logic saw "1/1 chunks done, last 15s ago" and skipped the final upload. In a 2-market run that means no retry at all (data was retained only because pruning is gated on upload success — that gate works).
- **I-12. Kaggle upload path uses a stale/relative path convention** (`data\kaggle_staging\…` retried with backoff 2.5s→32.6s) — the retried filename doesn't match the staged file set (I-1), and the failure surfaced only as "does not exist" with no diff of expected-vs-actual staging files.
- **I-13. Export log mislabels assets** ("35 files failed for trades asset=HYPE" while listing BNB files) and emits a pyarrow `FutureWarning` (promote).
- **I-14. Test-mode progress line shows empty window sets** while collection is running (it displays only *completed* windows) — cosmetically alarming; also the run can only end by timeout when any asset has a discovery miss (HYPE), since `all_done` requires all assets.
- **I-15. Environment (Windows):** project deps and `kaggle` weren't installed (fixed via `pip install -e ".[dev,kaggle]"`); Python stdout needed `-u` to make logs visible when redirected.

## 4. Answers to the questions the run was meant to answer

- **Does rollover lose data?** No — the warm rollover is the best part of the pipeline: window 2 opened at exactly 12:10:00.0 and captured **600/600 ticks on all six discovered assets**. Only cold start (process boot mid-gap) loses ~5.5s / 11 ticks.
- **Does a WS drop lose data?** Not in this run: even through the 12:12:31 disconnect episode, window-2 capture stayed 600/600 raw; losses appeared only as ~12% stale-scattered ticks (I-6/I-8). But resync telemetry can't prove no-loss, so this needs a clean re-test after I-6 is fixed.
- **Does discovery keep up?** Yes for 13/14 windows, pre-discovering the next market ~30s ahead; no for HYPE window 2 — silently (I-5).
- **Does the Kaggle upload work?** **No** — it has never delivered data in this run (I-1), and its own analysis layer can't even measure what it holds (I-4). Data survived only because the prune is correctly gated on upload success.

## 5. Scorecard vs PERFECT_DATA_SPEC.md (raw table)

| Metric | This run | Target | Pass |
|---|---|---|---|
| Grid alignment / duplicates / depth consistency | 0 violations | 0 | ✅ |
| Snapshot completeness (per warm market) | 100% | ≥99.9% | ✅ |
| Snapshot completeness (cold-start market) | 97.8% | ≥99.9% | ❌ |
| Market coverage (windows found) | 13/14 = 92.9% | ≥99.9% | ❌ + unattributed (I-5) |
| Clean-view completeness | 76–90% | ≥99.9% | ❌ (I-8) |
| Stale rate | 11.9% | ≤0.1% | ❌ |
| Crossed rate | 3.2% | ≤0.01% | ❌ (I-7) |
| BBO null rate | 0.9% | ≤0.1% | ❌ |
| Settlement completeness | 0/13 | ≥99.9% | ❌ (I-2) |
| book_events / chainlink_events produced | 0 | >0 | ❌ (I-2/I-3) |
| Kaggle delivery | failed | success | ❌ (I-1) |
| Unexplained exceptions | HYPE w2 + cold-start gap | 0 | ❌ (T4 fail) |

## 6. Test suite status

- `tests/test_chaos.py` + `tests/test_resync.py`: **12/12 passed** (disconnect, sequence gaps, malformed messages, backpressure, crash recovery at unit level).
- `tests/test_completeness.py`: **4 failed** — all because `compute_daily_completeness` hits the I-1 read bug and counts 0.
- `tests/test_kaggle_data_loss.py`: **3 failed** — same read bug inside the export path (visible as `[export] ERROR all files failed for trades` in test output).
- `tests/test_storage.py`: passed (including backpressure overflow signaling).

## 7. Recommended fix order

1. **Fix the parquet/hive clash once, centrally** (I-1, I-4): either stop storing `asset` (and `date`) *inside* the files and rely on hive paths, or read with an explicit schema/partitioning (`partitioning="hive"` + schema override, or read per-file with `pq.ParquetFile` and cast). Then delete the `except: continue` swallows in `completeness.py`/`_analyse_test_data` so read failures are loud. This one change unblocks: Kaggle export, completeness metrics, 7 failing tests, and makes the run's data actually usable.
2. **Start the chainlink WS client** and drive the active→closed→resolved lifecycle (I-2) — without it, settlement ground truth and market status can never be right.
3. **Wire book_events emission to the writer** (I-3) or delete the dataset from the contract.
4. **Attribute missing markets**: emit `coverage_gap` with asset+window the moment a window opens with no market (I-5), and stop counting failed uploads as completed chunks (I-11).
5. **Fix resync episode bookkeeping** (I-6): single row per episode updated in place, real reconnect timestamps, honest attempt counts; reconcile with book_state.
6. **Serialize full `details`** into collector_events (I-9) and demote the 10s "connected" heartbeat to a distinct event type (I-10).
7. Investigate the both-books-crossed state (I-7) and the stale-marking threshold that scatters 1-tick staleness through the clean view (I-8).

## 8. Artifacts

- Run log: `test_run_2mkt_final.log` (stdout), pytest output: `pytest_results.txt`
- Analysis files (untrustworthy until I-4 fixed): `data/test_analysis.json`, `data/test_analysis_t10min.json`, `data/test_analysis_final.json`
- Staging (verified empty per-asset): `data/kaggle_staging/5m/gghgg1/polymarket-5m-crypto/`
- Raw hive data retained for retry/upload: `data/book_snapshots_500ms/`, `data/trades/`, `data/collector_events/`, `data/markets_log/`, `data/resync_episodes/`, `data/raw_ws_archive/` (80MB)
- Kaggle dataset `gghgg1/polymarket-5m-crypto` was **not** modified by this run (upload failed before any API write).
