# plan.md — Kaggle Auto-Upload: 5m-only (4 markets) + 7 Assets, 10-min Kaggle, Null/Data-Loss Analysis

> Companion to `PLAN.md` v4 (`src/polymarket_collector/config.py:111` `CollectorConfig`, `PLAN.md:1` now 7-asset `BTC/ETH/SOL/HYPE/BNB/XRP/DOGE`). `PLAN.md` remains source for §0–§19. **Original plan was 5 native TF datasets (5m/15m/1h/4h/1d) hourly; scoped down 2026-08-30 to 5m-only (1d too long for test, assume 5m validates others), 4 markets ×5m =20 min, 7 assets, 10-min Kaggle uploads, deep null/data-loss analysis.**

Status: `P1 DONE (20b9f0d)` · `5m-only DONE 2026-08-30 (78 tests pass)` · **5-window native (P2) DEFERRED** (see §6). Last updated 2026-08-30.

**Scope change 2026-08-30 — user decision:** only 4×5m (not 1d), 5m-only assumption, all 7 Polymarket assets now (not 3). Implemented: `config.py:90` `num_markets=4`, `assets=[BTC,ETH,SOL,HYPE,BNB,XRP,DOGE]` `config.py:116`, `kaggle.test_upload_interval_seconds=600` `config.py:103`, `collector.py:322` `run_test_mode 4 + 10-min uploads + deep _analyse_test_data (null %/book_state/gaps/data_loss)`, `storage/export.py:548` fixed `pc.datetime` bug + single dataset `gghgg1/polymarket-5m-crypto` folder upload + safe prune `market_end<checkpoint-2h` (not mtime), `enums.py:41` `kaggle_upload_*`, `collector.yaml` 7 assets, `tests/test_config.py` updated. 5-window hive `window=` partitioning (original P2) deferred to future when 15m/1h/4h/1d native needed — synthetic `aggregate_5min_to_timeframe` kept but deprecated for 5m-only.

---

## 0. Goals and Non-Goals (2026-08-30 scoped to 5m-only)

**Goals (current build — 5m-only, 4 markets, 7 assets):**
- **Single public Kaggle dataset** `gghgg1/polymarket-5m-crypto` (not 5 datasets) containing **31 files** for 7 assets (`7×4 per-asset +3 globals`: `*book_snapshots.parquet`, `*trades.parquet`, `*book_events.parquet`, `*chainlink_events.parquet` + `markets.parquet`, `collector_events.parquet`, `resync_episodes.parquet`). Each updated **every 10 min during test** (`600s` `config.kaggle.test_upload_interval_seconds`) and **hourly in prod** (`3600s`), cumulative same filenames rows appended.
- Automatic 5m: `flush` + `MarketsLog.compact()` + `build_clean_view()` → `export_per_asset_single_file` time-first sorted `TS_SORT_CANDIDATES:38` zstd → staging `data/kaggle_staging/5m/gghgg1/polymarket-5m-crypto/` + `dataset-metadata.json` `CC BY-NC-SA 4.0` → `dataset_create_new` (first) or `dataset_create_version` (folder) retry 5 jitter → poll `dataset_status` ready → checkpoint `_kaggle_state.json` → safe prune `market_end<checkpoint-2h` never open window.
- **Test mode:** 4×5m =20 min wall-clock for 7 assets (BTC/ETH/SOL/HYPE/BNB/XRP/DOGE) `TestModeConfig.num_markets=4`, every 10 min staging+upload (2 uploads in 20 min, gated closed markets only), after final `data/test_analysis.json` deep audit: per-column null % vs zero `schemas.py:69` null-vs-zero, `book_state` live/stale histogram, `collector_events` gaps, `clean_completeness` vs `snapshot_completeness`, data_loss % `completeness.py:42`, kaggle staging 31-file check.
- Enrich `markets` with `slug`, `volume`, `liquidity`, `recorded_at` (alias `updated_at`) `schemas.py:16` → `markets.parquet` matches JSON.
- Reuse **one Chainlink feed per ticker shared** (not per window).

**Deferred (original 5-window, now non-goal for test):** 15m/1h/4h/1d native datasets (assume 5m validates others per user), synthetic `aggregate_5min_to_timeframe` deprecated kept but not used. Backfill pre-history separate.

**Non-goals:** 10+ tickers live 24/7 before 7-asset 5m pilot proves ≥95% completeness `completeness.py:42` + `watchdog` no `rate_limited/backpressure` rise.

---

## 1. Dataset Design: 5 Slugs × 15 Files

### 1.1 Slugs (confirmed)

```
gghgg1/polymarket-5m-crypto
gghgg1/polymarket-15m-crypto
gghgg1/polymarket-1h-crypto
gghgg1/polymarket-4h-crypto
gghgg1/polymarket-1d-crypto
```

Template: `gghgg1/polymarket-{window}-crypto` (`window` = `5m` etc). `src/polymarket_collector/config.py:103` `KaggleConfig` currently single `dataset_prefix: str` (`:107`) — change to `Dict[str,str]` `datasets: { "5m": "gghgg1/polymarket-5m-crypto", … }` + `title`/`license_name` per window. Also `window_sizes_seconds: List[int] = [300,900,3600,14400,86400]` (today `window_size_seconds:int=300` scalar `config.py:120`, `rollover.py:72` single `MarketDiscovery`, `collector.py:43` single `RolloverManager` — all scalar, must become per-window maps; see §4). For `PLAN.md` v4 7-asset universe the same 5 slugs are kept; adding assets does not create new slugs, only adds `*_book_snapshots.parquet` files inside each dataset (see §1.2).

### 1.2 15 Files Per Dataset (same names, rows grow)

Per-asset (`PER_ASSET_DATASETS` `src/polymarket_collector/storage/export.py:34`):

```
BTC_book_snapshots.parquet   # clean (§9B) via storage/clean_view.py:19 WHERE book_state='live'
ETH_book_snapshots.parquet
SOL_book_snapshots.parquet   # schema storage/schemas.py:56 snapshot_schema(l2_levels=20) 182 cols, nullable per null-vs-zero §3
BTC_trades.parquet           # schemas.py:128 TRADES_SCHEMA price/size/notional/fee
ETH_trades.parquet
SOL_trades.parquet
BTC_book_events.parquet      # schemas.py:103 BOOK_EVENTS_SCHEMA
ETH_book_events.parquet
SOL_book_events.parquet
BTC_chainlink_events.parquet # schemas.py:150 CHAINLINK_SCHEMA price/twap/report_id — IDENTICAL across TFs, copied to each staging
ETH_chainlink_events.parquet
SOL_chainlink_events.parquet
```

Global (`NON_ASSET_DATASETS:35`):

```
markets.parquet              # enriched markets_log alias export.py:266 → enriched MARKETS_SCHEMA:16 (see §3)
collector_events.parquet     # schemas.py:166 COLLECTOR_EVENTS_SCHEMA
resync_episodes.parquet      # schemas.py:180 RESYNC_EPISODES_SCHEMA §1A
```

→ 15 files. Hourly version **overwrites same filenames** with larger parquet (cumulative). Staging is flat `data/kaggle_staging/{window}/{slug}/` + `dataset-metadata.json` (`kaggle_api_extended.py:5509` requires `dataset-metadata.json` + `licenses`).

Adding `HYPE/BNB/XRP/DOGE` per `PLAN.md:31` `assets: [BTC,ETH,SOL,HYPE,BNB,XRP,DOGE]` would make it `7 assets×4 +3 = 31` files/dataset. Keep 3 assets until (§8) proven. If you add `DOGE` alone, each of the 5 datasets becomes 19 files; full v4 7-asset becomes 31 files — gate each add on §8 capacity.

### 1.3 Where Is Up/Down Price?

Already stored, not missing:

*   `book_snapshots`: `up_bid/up_ask/up_bid_size/up_ask_size/down_bid/down_ask/...` (`schemas.py:69-76`) top-of-book per token in `[0,1]` (`validation.py` §3A bounds), full L2 `up_bid_level_{1..20}_price/size` (`:79`) and `depth_1c/5c/10c` (`:85` defined §3 as cumulative size within N cents of that side’s best). This **is** the Up/Down token order-book price.
*   `trades`: `price/size/notional/fee/aggressor_side` (`schemas.py:128`) per `token_id/outcome`.
*   `chainlink_events`: `price/twap/twap_window_seconds` per `asset` (`:150`) — reused.

Snapshot/trades together give mid/spread/microprice (compute per `PLAN.md §12:597`, not stored).

---

## 2. Polymarket Rate Limits and What Adding More Assets Costs

### 2.1 Limits As Coded (no official `X-RateLimit` published; `verification_status.md:12` item 4 still ⏳ — must measure `Retry-After` via `verify_gate.py:178-189` live)

*   **Gamma discovery** `rollover.py:124` `GAMMA_BASE=https://gamma-api.polymarket.com/markets?slug={btc,eth,sol}-updown-{window}-{ts}` (`:102,117`). Poll `discovery_poll_interval_seconds:2` (`config.yaml:13`, `config.py:125`) capped `backoff_max_s:8` (`:14`, `rollover.py:74`), guard `rollover.py:381` `if now-last < backoff*1000: skip`. On `429` emit `rate_limited` (`:133`, `enums.py:63`) + `_backoff_s*=2` (`:136`), reset on success (`:157`). Normal only `30s` before `market_end_ts` (`rollover_lead_seconds:30`, `config.py:123`, `PLAN.md:45`) dual-track holds `current+next` (`rollover.py:446` `active_markets()->[current,next]`, test `tests/test_rollover.py:54` expects 2).
*   **CLOB REST book (resync):** `config.py:28` `rest_book_url=https://clob.polymarket.com/book`, `config.py:33-34` `resync_rest_backoff_initial_ms:1000→max 20000` + jitter `resync.py:58-63` exponential, `max_resync_duration_seconds:60` then escalate (`PLAN.md:115`, `resync.py:213`).
*   **WS:** `wss://ws-subscriptions-clob.polymarket.com/ws/market` (`config.py:27`) + Chainlink `wss://ws-live-data.polymarket.com` (`config.py:48`). Reconnect `500ms→30s` jitter (`config.py:30-32`).

### 2.2 Cost Per Ticker / Per Window

*   **WS:** 2 tokens/market ×1–2 markets (overlap 30s) = peak `4 tokens/asset/window`. Today `3 assets×1 window=12 subs`. Native `3×5 windows=60 subs` peak. `+1 asset (DOGE) ×5 windows = +20 subs →80`, `7×5=140 subs`. WS has no coded cap, but more subs → more `sequence_gap`/`resync_failed` (`resync.py:208`) REST fetches.
*   **Gamma:** `N assets × M windows` polls at burst. `3×5=15 polls/2s=7.5 req/s` near top of `429` threshold; `7×5=35 polls/2s=17.5 req/s` will `429` → `backoff 2→4→8s` → near `max_coverage_gap_seconds:5` (`config.py:124`) → `coverage_gap` (`rollover.py:428`) vs `rollover_miss` (`:437`). **Fix:** per-window `MarketDiscovery` with isolated `_backoff_s`, raise poll to `5s` for `1h/4h/1d` windows, or add `discovery_poll_interval_per_window` config (`config.py:109`).
*   **Chainlink:** **Not multiplied per window** — one feed per ticker shared across all TFs. Adding `DOGE` adds one Chainlink subscription only, negligible. Each Kaggle staging reuses same `*_chainlink_events.parquet` (copy/symlink, not re-export).
*   **Storage/CPU:** `capacity.py:15` `172800 snaps/day/asset` @500ms, `storage/schemas.py:56` 182 cols `l2_levels:20`, `config.py:99-100` `2500 B×0.25` → `~108 MB/day/asset/window`. `3×1 window ≈324 MB/day`, `3×5 windows ≈1.6 GB/day`, `7×5 windows ≈3.8 GB/day` (see §8). `parquet_writer.py:72` `buffer_max_rows:50000`/`flush_interval:60s` (`config.py:56`) may need raise; `ecosystem.config.js:44` `max_memory_restart:800M` tight for 5-window. `PLAN.md:53` `rollover_lead_seconds` poll across assets deliberately not tight-looped.

### 2.3 Recommendation on More Tickers

You can reuse Chainlink per ticker across TFs, but **do not multiply live tickers ×5 windows blindly**. Keep `BTC/ETH/SOL` native 5-window for now; add new asset as **single window (`5m`)** initially via `assets: [BTC,ETH,SOL,DOGE]` (`config.py:116` config-driven, `README.md:70`) — cost `+1×1` not `+1×5`. Promote to `15m/1h` only if Gamma `doge-updown-15m-{ts}` exists with liquidity (`PLAN.md:32` says confirm live market). Gate each add on `completeness.py:42` `completeness_ratio>0.95` and `watchdog.py:83` `rate_limited/backpressure` not rising; check `pm2/capacity` `df -h` / `free -m`. For broad backtest, batch backfill Gamma history separate from live 24/7. Full 7-asset ×5-window live only after 3-asset pilot proves §8 capacity.

---

## 3. Schema Enrichment: Slug, Volume, Liquidity, RecordedAt, MarketStart/End, n_ticks

> **P1 DONE** — `20b9f0d` `feat: P1 schema enrichment`. `MARKETS_SCHEMA:16` now has `slug/window_label/window_size_seconds/recorded_at` + ms aliases; `MarketInfo:30` enriched; `_parse_gamma_market:226` + `_parse_market_response:337` populate volume/liquidity; 4× `300` hard-codes fixed; `RolloverManager:431` uses `config.window_size_seconds`. Tests `77 passed`.

### 3.1 Slug

`MARKETS_SCHEMA:16` now `pa.field("slug", pa.string(), nullable=True)` `schemas.py:26`. Populated from `data.get("slug") or data.get("marketSlug") or self._slug_for(asset,ts_seconds)` in `rollover.py:297`. `data/kaggle_staging/.../markets.parquet` contains `slug:"btc-updown-5m-1774390200"`. Also `window_label` + `window_size_seconds:int` for native partition.

### 3.2 MarketStart / MarketEnd / RecordedAt — Keep All Three

*   `market_start_ts` + `market_end_ts` (`schemas.py:19-20`, ISO) from `_parse_gamma_market:273` `startDate/endDate` (fallback `ts*1000` `:282` / `(ts+window)*1000` `:277`, hard-coded `300` fixed to `window`). Essential for `market_time_remaining_ms` (`book.py:84`), series stitching `PLAN.md:58` `series_id/window_index`, and `RolloverManager:48` `lead_ms`/`should_promote:55`. Also `market_start_ts_ms/market_end_ts_ms` int ms epoch alias for your `market_start:1774390200000` JSON (export casts ISO→ms).
*   `recorded_at` = existing `updated_at` (`schemas.py:18`, `markets_log.py:34` event-sourced Append + `compact()` `max(updated_at)`:180 §9A). `storage/export.py:42` TS sort includes `updated_at`. Alias `recorded_at` in export so JSON `recorded_at:1774390500464` matches. `markets_log.py:36` + `parquet_writer.py:283` auto-fill `recorded_at = updated_at`.

### 3.3 Volume / Liquidity / Outcome / n_ticks

*   `reported_volume`/`reported_liquidity` (`schemas.py:43-44`) schema exists and **now populated** — `_parse_gamma_market:296` reads `volumeNum/volume/liquidityNum/liquidity` with float coercion. Previously always `null` (sample `data/markets_latest/markets_latest.parquet:24` all `null`); now `volume:10 liquidity:11298` preserved.
*   `resolution_outcome` (`schemas.py:35` `unknown/up/down/tie/voided/disputed` `enums.py:24`) is your `outcome:"Up"`. Stays `unknown` until settlement (`schemas.py:45-51` `settlement_*` §6A, `chainlink.py:83` `SettlementRecord`) — wire `chainlink.py:104` `fetch_settlement()` to resolver or mark `inferred_nearest` (`chainlink.py:159`).
*   `n_ticks` (`n_ticks:300` for `5m` @500ms `1774390200→1774390500`) **not stored**; it is `actual_snapshots` per `condition_id` in `book_snapshots_clean` vs `expected_snapshots` `capacity.py:15` `172800/day`. Derive at query via `completeness.py:34` `DailyCompleteness` or cache `expected_snapshots=window/500*1000` as optional col; don’t store denormalized.

### 3.4 Other Fields — Anything Else Missing?

`MarketInfo:16` → `markets_log` mapping now via `to_markets_row:54` covers `§2 when-available` `resolution_rule/resolution_source/minimumOrderSize/minimumNotional/fee_information` (`schemas.py:30-42` nullable, kept for Kaggle). Trades/books/chainlink wide schemas (`schemas.py:56,103,128,150`) already complete per `PLAN.md:214-391`; no `mid/spread` stored (§12 compute later). Settlement fields (`settlement_report_id/price/tx_hash` `:45-51`) need wiring — P3.

### 3.5 P1 Fix List — Completed

- [x] Add `slug, window_label, window_size_seconds, recorded_at` to `MARKETS_SCHEMA:16`.
- [x] Populate `reported_volume/liquidity` from Gamma, fix 4× `300` hard-codes (`rollover.py:124,232,244,261` → `window_size_seconds`), standardize `series_id` to label (`5m` not `300s`), use `config.window_size_seconds` not `test_mode.window_size_seconds` (`rollover.py:435`).
- [x] Backward-compat: `parquet_writer.py:283` promotion + `storage/export.py:74` glob handles old files without `slug` (null).

Remaining P1 polish (optional, non-blocking): backfill old `markets_latest.parquet` rows with `null` slug via `markets_log.compact()` re-read; no migration needed as fields are nullable.

---

## 4. Storage Partitioning for 5 Native Windows

Today 2-level hive `data/{dataset}/date=YYYY-MM-DD/asset=BTC/*.parquet` (`parquet_writer.py:213`, `PLAN.md §11`) — no `window`. Native 5 windows would **intermix** `condition_id` in same leaf (query must filter `series_id` post-scan).

**Decision: hive `window=`** `data/book_snapshots_500ms/window=5m/date=.../asset=...` (add `window_label` column, predicate pushdown, `clean_view.py:33`/`compaction.py:17` walk extra level, `cursor_store.py:41` per-`(asset,window)` pathfix). Alternative `data/book_snapshots_5m/...` dataset-per-window is simpler for Kaggle export but duplicates compaction/clean logic ×5 — rejected.

**P2 changes required:**

* `config.py:103` `KaggleConfig.datasets: Dict[str,str]` + `CollectorConfig.window_sizes_seconds: List[int] = [300,900,3600,14400,86400]` and `window_labels: List[str]`.
* `rollover.py:112` → `Dict[str, MarketDiscovery]` per window, isolated `_backoff_s`; `RolloverManager:428` → `Dict[asset, Dict[window, RolloverState]]`; `collector.py:43` → `Dict[window, RolloverManager]`.
* `parquet_writer.py:213` `_write_group` → `data/{dataset}/window={label}/date=.../asset=...` + `window` level in `NON_ASSET` datasets as column (not partition) for `markets_log`.
* `clean_view.py:19` + `compaction.py:17` walk `window=` level; `cursor_store.py:41` per-`(asset,window)` pathfix (e.g. `data/cursor_state/{asset}_{window}.db` or `shared_wal` table keyed by `(asset,window)`).
* `export.py:68` `_read_dataset_per_asset` add `window=` filter; remove `export.py:362` synthetic `aggregate_5min_to_timeframe` (P2 deletes it — native only).
* `capacity.py:17` + `config.py:99` estimates ×5 windows (see §8).

Migration: old `date=`-only files remain readable (no `window` column → treat as `window=5m` fallback). New writes go to `window=5m/...`. Back-compat `clean_view` unions both.

---

## 5. Kaggle Pipeline (Hourly, Append-to-Same-15-Files, Safe Delete, Max Retries)

```
cron 0 * * * *  polymarket-kaggle-uploader (PM2, ecosystem.config.js:30 cron_restart "0 * * * *",:98 pattern)
  1. Gate: skip if no window_end < now (only full closed markets, not ongoing 500ms buffer). Check via markets_latest.parquet max(market_end_ts_ms) per window.
  2. Flush ParquetWriter:flush():120 + MarketsLog.compact() + clean_view.build_clean_view():19
  3. For window in [5m,15m,1h,4h,1d]:
     export_per_asset_single_file:206 time-first sorted (TS_SORT_CANDIDATES:38), zstd, atomic tmp → data/kaggle_staging/{window}/{slug}/ 15 files; chainlink files COPIED across windows (not re-exported, single feed per §2.1)
     write dataset-metadata.json {"title":"Polymarket {window} Crypto — BTC/ETH/SOL", "id":slug, "licenses":[{"name":"CC BY-NC-SA 4.0"}], "resources":[{"path":...}]}
  4. Auth: KAGGLE_API_TOKEN env else ~/.kaggle/kaggle.json chmod600 (kagglesdk/kaggle_http_client.py:34); gghgg1 credentials via env only — rotate after paste, never commit. Use `kaggle.json` `{"username":"gghgg1","key":"KGAT_..."}` or `KAGGLE_USERNAME`/`KAGGLE_KEY` env.
  5. For each slug: dataset_status(slug) ? dataset_create_version(folder, version_notes="hourly UTC {now} +{rows}", convert_to_csv=False, delete_old_versions=False) : dataset_create_new(folder, public=True) with retry 5 (2s×2 jitter→60s) on 429/500, emit collector_events:kaggle_upload_* (enums.py: add kaggle_upload_started/success/failed)
  6. Poll dataset_status until ready (timeout 10m); on success update data/kaggle_staging/_kaggle_state.json {slug:{last_version, last_upload_utc, row_counts, md5}} + kaggle_upload_success; on failure after 5 → kaggle_upload_failed + watchdog alert watchdog.py:83 (add kaggle_upload_failed to alert_on config.py:83 watchdog.alert_on)
  7. After ready: prune hive only `date < checkpoint_ts-2h` and `max(market_end) < checkpoint_ts`, keep 2h buffer for re-export; never delete current open window; data/kaggle_staging kept (source for next concat if needed); NEVER blind age delete (fix storage/export.py:640 pc.datetime bug → datetime, cleanup_local_data:644 must check market_end not mtime)
```

`license` recommendation: **`CC BY-NC-SA 4.0`** (Attribution, NonCommercial, ShareAlike) — prevents selling/profit and forces derivatives also NC; Kaggle `valid_dataset_license_names:982` group `cc` accepts any CC name (`kaggle_api_extended.py:5070` `CC0-1.0` / `CC BY-SA 4.0` examples); fallback `other` if strict.

P2 deletes `export.py:326-833` broken synthetic timeframe + `pc.datetime` / `glob_mod` / per-asset dataset naming (`_get_kaggle_dataset_name:574` `polymarket-5m-crypto-btc-eth-sol` is wrong; correct is `src/polymarket_collector/config.py:107` `gghgg1/polymarket-{window}-crypto` per-window slug). Correct upload uses `kagglesdk` `dataset_create_version` folder upload, not single-file `dataset_version_create`.

---

## 6. Implementation Phases (2026-08-30 actuals for 5m-only)

| Phase | Scope | Status | Key files |
|-------|-------|--------|-----------|
| **P1 — Schema+Gamma** | `schemas.py:16` `slug/window_label/window_size_seconds/recorded_at` + ms aliases, fix 4× `300` `rollover.py:124,232,244,261`, populate `volume/liquidity` `rollover.py:297` | **DONE** `20b9f0d` 78 tests pass (updated 2026-08-30 for 7 assets) | `schemas.py:16`, `rollover.py:15`, `storage/markets_log.py:34`, `storage/parquet_writer.py:283` |
| **P2 — Native multi-window (5-window hive `window=` )** | `window=` partitioning `parquet_writer.py:213` + per-window `Discovery/Rollover` | **DEFERRED** — not needed for 5m-only assumption. Original required `config.py:103` Dict datasets + `window_sizes_seconds` list; deferred until 15m/1h/4h/1d native needed. Synthetic `aggregate_5min_to_timeframe` kept but deprecated. | `config.py`, `rollover.py`, `collector.py` |
| **P3 — Kaggle uploader 5m-only (DONE)** | Single dataset `gghgg1/polymarket-5m-crypto` folder upload 31 files (7 assets), `prepare_kaggle_staging_5m` + `_upload_kaggle_folder` retry 5 jitter + status poll → checkpoint `_kaggle_state.json`, `export_and_upload_all_kaggle` 5m-only, `pc.datetime` bug fixed, safe prune `market_end<checkpoint-2h`, `dataset-metadata.json` `CC BY-NC-SA 4.0`, `enums.py:41` `kaggle_upload_*` | **DONE 2026-08-30** `storage/export.py:548` rebuilt, `collector.py:360` 10-min loop + `collector.py:322` `run_test_mode` 4 + 10-min uploads | `storage/export.py:548`, `collector.py:322,360`, `enums.py:41`, `config.py:103` |
| **P4 — PM2 + safe delete (5m-only)** | `collector.py:360` `_kaggle_upload_loop` interval `600s` test / `3600s` prod, safe prune `cleanup_local_data` `market_end` not `mtime`, 2h buffer never open window. `watchdog.alert_on` includes `kaggle_upload_failed` `config.yaml:88`. PM2 `ecosystem.config.js` hourly cron for prod (test uses in-process loop). | **DONE 5m-only** — 5-window cron `polymarket-kaggle-uploader` deferred | `collector.py:360`, `storage/export.py:640`, `config/collector.yaml:88` |
| **P5 — Tests & dry-run (5m-only)** | `tests/test_config.py` updated to 7 assets/4 markets/600s, `tests/test_chaos.py` fix `collector.books` recovery stale, `78 passed`, `prepare_kaggle_staging_5m` 31-file staging dry-run `23→31` files (depends on datasets present), `export_and_upload_all_kaggle dry_run` + `collector._analyse_test_data` null/data-loss `test_analysis.json` | **DONE 2026-08-30** — 5-window 75-file test deferred | `tests/test_config.py`, `tests/test_chaos.py`, `storage/export.py` |
| **P6 — Live 5m-only** | `polymarket-collector --test-mode --test-markets 4` for 7 assets 20 min, 2×10-min uploads, `data/test_analysis.json` with `snapshot_completeness_pct/clean_completeness_pct/data_loss_pct/critical_null_pct/book_state_histogram/kaggle_staging`, manual `verify_gate` for Gamma `btc-updown-5m` only (1h/1d gate deferred). | **READY for live** — first `dataset_create_new` gghgg1/polymarket-5m-crypto then `create_version` every 10 min test / hourly prod. 5-window gate deferred | `cli.py`, `verify_gate.py`, `collector.py:322` |

Order for 5m-only: P1 → P3 → P4 → P5 → P6 (P2 deferred). Full 5-window P2 before future P3-5-window.

---

## 7. Open Confirms Needed

1.  Exact 5 slugs: `gghgg1/polymarket-5m-crypto` … `-1d-crypto` above — **confirmed** (use as `KaggleConfig.datasets` keys). Create as public on first P6 run.
2.  License: `CC BY-NC-SA 4.0` (recommend, strongest NC) vs `CC BY-NC 4.0` — **decision: `CC BY-NC-SA 4.0`** (share-alike prevents proprietary derivatives; fallback `other` if Kaggle rejects). `dataset-metadata.json: licenses: [{"name":"CC BY-NC-SA 4.0"}]`.
3.  Snapshots public file: `clean` (§9B `book_state='live'`) vs raw `500ms` (`stale` included) — **decision: `clean`** (`book_snapshots_clean`) as `BTC_book_snapshots.parquet`; raw `500ms` available on request via `book_state` column. Kaggle description must state this.
4.  Add `DOGE` (or `HYPE/BNB/XRP`) now (as single-window `5m`) or stay 3 assets until (§8) pilot proves capacity — **decision: stay 3 assets until P2 5-window at 95% completeness** (`completeness.py:42` `completeness_ratio>0.95` for 7 days, `watchdog.py:83` no `rate_limited/backpressure` rise, `df -h`/`free -m` ok). Then add `DOGE` as single-window, promote per §2.3. Full 7-asset ×5-window only after DOGE pilot.

---

## 8. Capacity & Scale

Measured after P1 pilot (update with live numbers; below is analytic estimate for sizing):

* `book_snapshots_500ms`: `capacity.py:15` `172800 snaps/day/asset` @500ms, `schemas.py:56` `182 cols` `l2_levels:20`, `config.py:99-100` `2500 B×0.25` → `~108 MB/day/asset/window`.
* Other: `book_events` ~5k/d, `trades` ~10k/d, `chainlink` ~86k/d per asset — extra `~32 MB/day/asset` compressed.
* Totals: `3×1 window ≈324 MB/day + 96 MB ≈420 MB/day`, `3×5 windows ≈1.6 GB/day`, `7×1 window ≈980 MB/day`, `7×5 windows ≈3.8 GB/day`, `+ retain 2h buffer` before prune.
* `parquet_writer.py:57` `buffer_max_rows:50000`/`flush_interval:60s` (`config.py:56`) — raise to `100000` for 5-window; `ecosystem.config.js:44` `max_memory_restart:800M` → `1.5G` for 5-window main collector, `300M` watchdog unchanged.
* Disk: require `≥50 GB` free for 30-day 3×5-window run; monitor `writer.check_disk_space:151` `disk_space_min_bytes:1 GiB` → raise to `5 GiB` for 5-window.
* Gate every ticker add on `completeness.py:42` `>0.95` and `watchdog.py:83` `rate_limited/backpressure` flat; `polymarket-capacity --assets 3 --l2-levels 20` vs `df -h` / `free -m`.

---

## 9. Testing & Verification

* **Unit:** `tests/test_rollover.py:54` dual-tracking, `test_book.py` top-of-book null-vs-zero, `test_validation.py` `[0,1]` bounds, `test_schemas.py` MARKETS_SCHEMA new fields nullable, `test_storage.py` time-first sort `TS_SORT_CANDIDATES:38`.
* **Chaos (§19):** `tests/test_chaos.py` disconnect injection, sequence-gap, malformed price outside `[0,1]`, REST 429/timeout retry, process crash/restart `cursor_store.py:41` WAL/per-asset, backpressure `parquet_writer.py:85`. Must pass before P6 live.
* **Kaggle dry-run (P5):** `polymarket-kaggle-upload --dry-run --verbose` with mocked `kaggle.api.dataset_create_version` — asserts `data/kaggle_staging/{window}/{slug}/15 files`, `dataset-metadata.json` license `CC BY-NC-SA 4.0`, `resources` 15 entries, `row_counts` match `data/export` vs hive + `clean_view` filtering, no network. Also `75 files (5×15)` vs legacy `44-file` export.
* **Live gate (§18):** `polymarket-verify-gate --live` checks Gamma slug exists per window with `volume/liquidity >0` for `1h/4h/1d` before enabling native; `verify_gate.py:178-189` measures `Retry-After` to size `discovery_backoff_max_seconds:8` and `resync_rest_backoff_max_ms:20000`.

---

## 10. Deployment Runbook

1. **P1 verify:** `pytest -q` `77 passed`, `polymarket-verify-gate --config config/collector.yaml` (WS seq, REST L2, settlement, rate-limit).
2. **P2 deploy:** update `config/collector.yaml` `window_sizes_seconds: [300,900,3600,14400,86400]`, `pm2 restart polymarket-collector --update-env`, tail `pm2 logs` for `market_added` per `asset×window`.
3. **Kaggle auth:** `mkdir -p ~/.kaggle && chmod 700 ~/.kaggle && printf '{"username":"gghgg1","key":"KGAT_..."}' > ~/.kaggle/kaggle.json && chmod 600 ~/.kaggle/kaggle.json` or env `KAGGLE_USERNAME`/`KAGGLE_KEY` via `ecosystem.config.js:54` `env:` — **rotate `KGAT_c1ed93041c5ad3847aadbfa8b3b4bf34` after paste (already exposed in prior draft)**.
4. **First upload (P6):** `polymarket-kaggle-upload --dry-run` → verify `data/kaggle_staging/5m/gghgg1/polymarket-5m-crypto/dataset-metadata.json` + 15 parquets, then `polymarket-kaggle-upload --window 5m` (single window smoke, creates `dataset_create_new` public). Confirm Kaggle UI shows public dataset with 15 files, `CC BY-NC-SA 4.0`.
5. **Hourly:** `pm2 start ecosystem.config.js` enables `polymarket-kaggle-uploader` `cron_restart "0 * * * *"`; monitor `data/kaggle_staging/_kaggle_state.json` checkpoint and `watchdog` `kaggle_upload_failed` alerts. First week run with `--window 5m` only; promote to 5 windows after §18 gate confirms `1h/4h/1d` Gamma liquidity.
6. **Prune:** only via `prune_hive_after_verified` after `dataset_status==ready` (see §5.7); never `cron rm` on age. Keep `data/kaggle_staging` as source for next concat.

---

## 11. Risks & Mitigations

* **Rate-limit `429`:** `3×5=7.5 req/s` borderline; `7×5=17.5 req/s` will hit. Mitigate: per-window `MarketDiscovery` isolated backoff, `5s` poll for `1h/4h/1d`, `discovery_poll_interval_per_window` config, `rollover_lead_seconds:30` not tight-looped (`rollover.py:480` guard).
* **Disk full:** `1.6–3.8 GB/day` → monitor `parquet_writer.py:151` `check_disk_space` + `watchdog.py:83` `write_failed`; prune only after Kaggle `ready`; `compaction.py:16` daily merge; `raw_archive.py` 36h retention.
* **WS drift / sequence gap:** `resync.py:58` backoff + `book.py:84` `book_state` tagging + `clean_view.py:69` filter; redundant collector future option (§1A) without schema change (idempotent key `parquet_writer.py:193` `(asset,condition_id,bucket)`).
* **Synthetic resample leak:** `export.py:362` `aggregate_5min_to_timeframe` violates Non-goals — delete in P2; native `window=` hive is only path.
* **Blind delete:** `export.py:644` `cleanup_local_data` uses `mtime` not `market_end` and has `pc.datetime` bug — replace with checkpointed prune `market_end < checkpoint_ts-2h` in `kaggle_upload.py`.
* **Kaggle secret leak:** previous draft pasted `KGAT_...` — rotate key, use env/`~/.kaggle/kaggle.json` only, `.gitignore` already excludes `*.json`? Add `kaggle.json` to `.gitignore:19`.

---

## 12. Completion Definition (2026-08-30 for 5m-only)

This plan is **done (5m-only)** when:

- [x] P1 schema enrichment merged (`20b9f0d`, 78 tests pass 2026-08-30)
- [x] P3 5m-only Kaggle staging `prepare_kaggle_staging_5m` 31 files (7 assets) + folder upload retry 5 jitter + `dataset-metadata.json` `CC BY-NC-SA 4.0` `enums.py:41` `kaggle_upload_*` `78 passed`
- [x] P4 Kaggle loop `collector.py:360` 10-min test / hourly prod + safe prune `market_end<checkpoint-2h` `config.yaml:88` `kaggle_upload_failed` alert
- [x] P5 tests 7 assets / 4 markets / 10-min + dry-run staging `export_and_upload_all_kaggle dry_run` + `collector._analyse_test_data` 20-min → `data/test_analysis.json` with null/data-loss `78 passed`
- [x] P6 live ready: `polymarket-collector --test-mode --test-markets 4` for 7 assets 5m 20 min → 2×10-min uploads to `gghgg1/polymarket-5m-crypto` + `test_analysis.json` completeness/null/book_state/kaggle_staging audit
- [ ] **DEFERRED 5-window** P2 native `window=` hive + `75-file` 5 datasets at >95% for 7 days — not needed per 5m-validates-others assumption, kept for future.

Handoff after §12: `plan.md` above reflects 5m-only; full 5-window would need `README.md:1` 5-window update, `PLAN.md §11` storage add `window=`, `verification_status.md:1` close item 4 with Retry-After. `is plan.md finished?` **Yes for 5m-only (78 tests pass); No for original 5-window (P2 deferred).**

---

## Appendix A — Code Map (P1 done → P2–P6 todo)

* `src/polymarket_collector/config.py:103` `KaggleConfig` dict + `window_sizes_seconds` list — **P2**
* `src/polymarket_collector/rollover.py:15` `_window_label_for` + `MarketInfo:30` enrichment + per-window discovery — **P1 done, per-window map P2**
* `src/polymarket_collector/storage/schemas.py:16` `MARKETS_SCHEMA` 15→22 fields — **P1 done**
* `src/polymarket_collector/storage/markets_log.py:34` `recorded_at` alias + `compact():159` `max(updated_at)` — **P1 done**
* `src/polymarket_collector/storage/parquet_writer.py:213` `window=` partitioning + ms promotion — **P2**
* `src/polymarket_collector/storage/clean_view.py:19` `book_state='live'` + `window=` walk — **P2**
* `src/polymarket_collector/storage/compaction.py:17` `window=` walk + atomic rename — **P2**
* `src/polymarket_collector/storage/export.py:34` 15-file staging + `TS_SORT_CANDIDATES:38` sorted + zstd + atomic tmp — **fix P2/P3, delete :362 aggregation**
* `src/polymarket_collector/storage/kaggle_upload.py` **NEW P3** — `prepare_staging/write_metadata/upload_with_retry/poll/checkpoint/prune`
* `src/polymarket_collector/collector.py:43` `RolloverManager` per-window + `chainlink` reuse — **P2**
* `ecosystem.config.js:30` PM2 apps + `polymarket-kaggle-uploader` cron — **P4**
* `pyproject.toml:51` `polymarket-kaggle-upload` entry — **P3**
* `src/polymarket_collector/enums.py:41` `kaggle_upload_*` event types + `watchdog.py:83` alert — **P3**

## Appendix B — `dataset-metadata.json` (one per window staging folder)

```json
{
  "title": "Polymarket 5m Crypto — BTC/ETH/SOL (Native Order Book, Trades, Chainlink)",
  "id": "gghgg1/polymarket-5m-crypto",
  "licenses": [{"name": "CC BY-NC-SA 4.0"}],
  "resources": [
    {"path": "BTC_book_snapshots.parquet", "description": "clean L2 snapshots 500ms, book_state=live"},
    {"path": "markets.parquet", "description": "enriched markets with slug/volume/liquidity/recorded_at"}
  ]
}
```

Requires `title`, `id`, `licenses`, `resources` (`kaggle_api_extended.py:5509`).

## Appendix C — Safe Prune Algorithm (replaces `export.py:644` `cleanup_local_data`)

```python
# only after Kaggle dataset_status == "ready" for that window
checkpoint = _kaggle_state[slug]["last_upload_utc_ms"]
# hive prune: date < checkpoint-2h AND max(market_end_ts_ms per date partition) < checkpoint
# keep 2h buffer for re-export; never delete current open window (market_end > now)
# data/kaggle_staging kept (source for next concat if Kaggle version fails)
```

Never `age_seconds > keep_seconds` on `mtime` alone.

---

*This `plan.md` is the build checklist; P1 precedes P2, P2 before P3. After you confirm §7, build proceeds P2→P6 in order.*
