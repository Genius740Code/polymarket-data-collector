# plan.md — Kaggle Auto-Upload: 5 Native TF Datasets (5m/15m/1h/4h/1d), Slug/Volume/Liquidity/RecordedAt, Rate Limits, Scale

> Companion to `PLAN.md` v3 (which remains the source for §0–§19 collection semantics). This file is the **build plan for full Kaggle integration** that automatically exports and versions 5 public Kaggle datasets hourly, reuses Chainlink across TFs, keeps 15 files per dataset appended to same names, and only deletes local hive after Kaggle confirms. It also addresses `slug / volume / liquidity / recorded_at / market_start / market_end / n_ticks` and whether we can add more tickers.

---

## 0. Goals and Non-Goals

**Goals:**
- 5 public Kaggle datasets, one per native Polymarket window: `5m`, `15m`, `1h`, `4h`, `1d`. Each updated **hourly** (`cron 0 * * * *` UTC) with a new dataset version whose 15 parquet files are **cumulative** (same filenames, rows appended — not `part-*.parquet` per hour).
- Automatic: export from hive → staging with `dataset-metadata.json` (`CC BY-NC-SA 4.0`) → `dataset_create_new` (first time) or `dataset_create_version` (hourly) → poll `dataset_status` → checkpoint → prune verified hive source only.
- Enrich `markets` with `slug`, `volume`, `liquidity`, `recorded_at` (alias of `updated_at`) so Kaggle `markets.parquet` matches your JSON `{"condition_id","slug","market_start","market_end","recorded_at","token_up","token_down","volume","liquidity","outcome","n_ticks"}`.
- Reuse **one Chainlink price feed per ticker across all TFs** (do not multiply Chainlink WS/load per window).
- Never lose/miss data: only upload **full closed markets** (`market_end < now`), max 5 retries with jitter, watchdog alert on failure, delete local only after `ready`.

**Non-goals:** Backfilling pre-history via Gamma batch (separate job), resampled synthetic `1h` from `5m` books (native books only), adding 10+ tickers live 24/7 before proving 3-asset 5-window at 95% completeness.

---

## 1. Dataset Design: 5 Slugs × 15 Files

### 1.1 Slugs (proposed, confirm)

```
gghgg1/polymarket-5m-crypto
gghgg1/polymarket-15m-crypto
gghgg1/polymarket-1h-crypto
gghgg1/polymarket-4h-crypto
gghgg1/polymarket-1d-crypto
```

Template: `gghgg1/polymarket-{window}-crypto` (`window` = `5m` etc). `src/polymarket_collector/config.py:103` `KaggleConfig` currently single `dataset_prefix: str` (`:107`) — change to `Dict[str,str]` `datasets: { "5m": "gghgg1/polymarket-5m-crypto", … }` + `title`/`license_name` per window. Also `window_sizes_seconds: List[int] = [300,900,3600,14400,86400]` (today `window_size_seconds:int=300` scalar `config.py:120`, `rollover.py:72` single `MarketDiscovery`, `collector.py:43` single `RolloverManager` — all scalar, must become per-window maps; see §4).

### 1.2 15 Files Per Dataset (same names, rows grow)

Per-asset (`PER_ASSET_DATASETS` `src/polymarket_collector/storage/export.py:34`):

```
BTC_book_snapshots.parquet   # clean (§9B) via storage/clean_view.py:19 WHERE book_state='live'
ETH_book_snapshots.parquet
SOL_book_snapshots.parquet   # schema storage/schemas.py:49 snapshot_schema(l2_levels=20) 182 cols, nullable per null-vs-zero §3
BTC_trades.parquet           # schemas.py:121 TRADES_SCHEMA price/size/notional/fee
ETH_trades.parquet
SOL_trades.parquet
BTC_book_events.parquet      # schemas.py:96 BOOK_EVENTS_SCHEMA
ETH_book_events.parquet
SOL_book_events.parquet
BTC_chainlink_events.parquet # schemas.py:143 CHAINLINK_SCHEMA price/twap/report_id — IDENTICAL across TFs, copied to each staging
ETH_chainlink_events.parquet
SOL_chainlink_events.parquet
```

Global (`NON_ASSET_DATASETS:35`):

```
markets.parquet              # enriched markets_log alias export.py:266 → enriched MARKETS_SCHEMA:15 (see §3)
collector_events.parquet     # schemas.py:159 COLLECTOR_EVENTS_SCHEMA
resync_episodes.parquet      # schemas.py:173 RESYNC_EPISODES_SCHEMA §1A
```

→ 15 files. Hourly version **overwrites same filenames** with larger parquet (cumulative). Staging is flat `data/kaggle_staging/{window}/{slug}/` + `dataset-metadata.json` (`kaggle_api_extended.py:5509` requires `dataset-metadata.json` + `licenses`).

Adding `DOGE` etc. would make it `4 assets×4 +3 = 19` files — keep 3 assets until (§8) proven. If you add `DOGE`, each of the 5 datasets becomes 19 files.

### 1.3 Where Is Up/Down Price?

Already stored, not missing:

*   `book_snapshots`: `up_bid/up_ask/up_bid_size/up_ask_size/down_bid/down_ask/...` (`schemas.py:62-69`) top-of-book per token in `[0,1]` (`validation.py` §3A bounds), full L2 `up_bid_level_{1..20}_price/size` (`:75`) and `depth_1c/5c/10c` (`:81` defined §3 as cumulative size within N cents of that side’s best). This **is** the Up/Down token order-book price.
*   `trades`: `price/size/notional/fee/aggressor_side` (`schemas.py:121`) per `token_id/outcome`.
*   `chainlink_events`: `price/twap/twap_window_seconds` per `asset` (`:143`) — reused.

Snapshot/trades together give mid/spread/microprice (compute per `PLAN.md §12:597`, not stored).

---

## 2. Polymarket Rate Limits and What Adding More Assets Costs

### 2.1 Limits As Coded (no official `X-RateLimit` published; `verification_status.md:12` item 4 still ⏳ — must measure `Retry-After` via `verify_gate.py:178-189` live)

*   **Gamma discovery** `rollover.py:59-117` `GAMMA_BASE=https://gamma-api.polymarket.com/markets?slug={btc,eth,sol}-updown-{window}-{ts}` (`:102,117`). Poll `discovery_poll_interval_seconds:2` (`config.yaml:13`, `config.py:125`) capped `backoff_max_s:8` (`:14`, `rollover.py:74`), guard `rollover.py:381` `if now-last < backoff*1000: skip`. On `429` emit `rate_limited` (`:133`, `enums.py:63`) + `_backoff_s*=2` (`:136`), reset on success (`:157`). Normal only `30s` before `market_end_ts` (`rollover_lead_seconds:30`, `config.py:123`, `PLAN.md:45`) dual-track holds `current+next` (`rollover.py:446` `active_markets()->[current,next]`, test `tests/test_rollover.py:54` expects 2).
*   **CLOB REST book (resync):** `config.py:28` `rest_book_url=https://clob.polymarket.com/book`, `config.py:33-34` `resync_rest_backoff_initial_ms:1000→max 20000` + jitter `resync.py:58-63` exponential, `max_resync_duration_seconds:60` then escalate (`PLAN.md:115`, `resync.py:213`).
*   **WS:** `wss://ws-subscriptions-clob.polymarket.com/ws/market` (`config.py:27`) + Chainlink `wss://ws-live-data.polymarket.com` (`config.py:48`). Reconnect `500ms→30s` jitter (`config.py:30-32`).

### 2.2 Cost Per Ticker / Per Window

*   **WS:** 2 tokens/market ×1–2 markets (overlap 30s) = peak `4 tokens/asset/window`. Today `3 assets×1 window=12 subs`. Native `3×5 windows=60 subs` peak. `+1 asset (DOGE) ×5 windows = +20 subs →80`. WS has no coded cap, but more subs → more `sequence_gap`/`resync_failed` (`resync.py:208`) REST fetches.
*   **Gamma:** `N assets × M windows` polls at burst. `3×5=15 polls/2s=7.5 req/s` near top of `429` threshold; `4×5=10 req/s` likely `429` → `backoff 2→4→8s` → near `max_coverage_gap_seconds:5` (`config.py:124`) → `coverage_gap` (`rollover.py:428`) vs `rollover_miss` (`:437`). **Fix:** per-window `MarketDiscovery` with isolated `_backoff_s`, raise poll to `5s` for `1h/4h/1d` windows, or add `discovery_poll_interval_per_window` config.
*   **Chainlink:** **Not multiplied per window** — one feed per ticker shared across all TFs. Adding `DOGE` adds one Chainlink subscription only, negligible. Each Kaggle staging reuses same `*_chainlink_events.parquet` (copy/symlink).
*   **Storage/CPU:** `capacity.py:15` `172800 snaps/day/asset` @500ms, `storage/schemas.py:49` 182 cols `l2_levels:20`, `config.py:99-100` `2500 B×0.25` → `~108 MB/day/asset/window`. `3×1 window ≈324 MB/day`, `3×5 windows ≈1.6 GB/day`, `+DOGE×5 ≈+540 MB/day`. `parquet_writer.py:85` `buffer_max_rows:50000`/`flush_interval:60s` (`config.py:56`) may need raise; `ecosystem.config.js:45` `max_memory_restart:800M` tight. `PLAN.md:53` `rollover_lead_seconds` poll across assets deliberately not tight-looped.

### 2.3 Recommendation on More Tickers

You can reuse Chainlink per ticker across TFs, but **do not multiply live tickers ×5 windows blindly**. Keep `BTC/ETH/SOL` native 5-window for now; add new asset as **single window (`5m`)** initially via `assets: [BTC,ETH,SOL,DOGE]` (`config.py:116` config-driven, `README.md:70`) — cost `+1×1` not `+1×5`. Promote to `15m/1h` only if Gamma `doge-updown-15m-{ts}` exists with liquidity (`PLAN.md:32` says confirm live market). Gate each add on `completeness.py:42` `completeness_ratio>0.95` and `watchdog.py:83` `rate_limited/backpressure` not rising; check `pm2/capacity` `df -h` / `free -m`. For broad backtest, batch backfill Gamma history separate from live 24/7.

---

## 3. Schema Enrichment: Slug, Volume, Liquidity, RecordedAt, MarketStart/End, n_ticks

### 3.1 Slug (Add)

Today `MARKETS_SCHEMA:15` has no `slug` col; `_parse_gamma_market:183` reads `conditionId/clobTokenIds/outcomes/endDate` (`:187-246`) but discards `slug` (`:88` `_slug_for` generates it for the request but never stores). Add `pa.field("slug", pa.string(), nullable=True)` to `schemas.py:15`, populate from `data.get("slug") or data.get("marketSlug") or self._slug_for(asset,ts_seconds)` in `rollover.py:252`. `data/kaggle_staging/.../markets.parquet` then contains `slug:"btc-updown-5m-1774390200"`. Also add `window_label` + `window_size_seconds:int` for native partition.

### 3.2 MarketStart / MarketEnd / RecordedAt — Keep All Three

*   `market_start_ts` + `market_end_ts` (`schemas.py:17-18`, ISO) from `_parse_gamma_market:226-227` `startDate/endDate` (fallback `ts*1000` `:238` / `(ts+window)*1000` `:233`, hard-coded `300` today — fix to `window`). Essential for `market_time_remaining_ms` (`book.py:84`), series stitching `PLAN.md:58` `series_id/window_index`, and `RolloverManager:48` `lead_ms`/`should_promote:55`. Also expose `market_start_ts_ms/market_end_ts_ms` int ms epoch alias for your `market_start:1774390200000` JSON (export casts ISO→ms).
*   `recorded_at` = existing `updated_at` (`schemas.py:16`, `markets_log.py:34` event-sourced Append + `compact()` `max(updated_at)`:149 §9A). `storage/export.py:42` TS sort already includes `updated_at`. Alias `recorded_at` in export so JSON `recorded_at:1774390500464` matches.

### 3.3 Volume / Liquidity / Outcome / n_ticks

*   `reported_volume`/`reported_liquidity` (`schemas.py:36-37`) schema exists but **always `null`** — `_parse_gamma_market:183` never reads `volume`/`volumeNum`/`liquidityNum` (`:246` only reads `orderPriceMinTickSize`). Sample `data/markets_latest/markets_latest.parquet:24` all `null`, your `volume:10 liquidity:11298` would be lost. Fix: `rollover.py:252` populate `reported_volume = data.get("volumeNum") or data.get("volume")`, `reported_liquidity` similarly, and ensure `parquet_writer.py:250` casts.
*   `resolution_outcome` (`schemas.py:28` `unknown/up/down/tie/voided/disputed` `enums.py:24`) is your `outcome:"Up"`. Current stays `unknown` until settlement (`schemas.py:39-44` `settlement_*` §6A, `chainlink.py:83` `SettlementRecord`) — wire `chainlink.py:104` `fetch_settlement()` to resolver or mark `inferred_nearest` (`chainlink.py:159`).
*   `n_ticks` (`n_ticks:300` for `5m` @500ms `1774390200→1774390500`) **not stored**; it is `actual_snapshots` per `condition_id` in `book_snapshots_clean` vs `expected_snapshots` `capacity.py:15` `172800/day`. Derive at query via `completeness.py:34` `DailyCompleteness` or cache `expected_snapshots=window/500*1000` as optional col; don’t store denormalized.

### 3.4 Other Fields — Anything Else Missing?

`MarketInfo:16` → `markets_log` mapping today drops `§2 when-available` `resolution_rule/resolution_source/minimumOrderSize/minimumNotional/fee_information` (`schemas.py:30-35` nullable but never set) and aliases `market_id` (`rollover.py:254` loses `conditionId` vs `id` distinction). Keep as nullable for Kaggle. Trades/books/chainlink wide schemas (`schemas.py:49,96,121,143`) already complete per `PLAN.md:214-391`; no `mid/spread` stored (§12 compute later). Settlement fields (`settlement_report_id/price/tx_hash` `:39-44`) need wiring.

### 3.5 Missing Before Kaggle v1 — Fix List

*   Add `slug, window_label, window_size_seconds, recorded_at` to `MARKETS_SCHEMA:15`.
*   Populate `reported_volume/liquidity` from Gamma, fix 4× `300` hard-codes (`rollover.py:124,232,244,261`), standardize `series_id` to label (`5m` not `300s`), use `config.window_sizes` not `test_mode.window_size_seconds` (`rollover.py:338`).
*   Backward-compat: `parquet_writer.py` promotion + `storage/export.py:74` glob handles old files without `slug` (null).

---

## 4. Storage Partitioning for 5 Native Windows

Today 2-level hive `data/{dataset}/date=YYYY-MM-DD/asset=BTC/*.parquet` (`parquet_writer.py:213`, `plan.md §11`) — no `window`. Native 5 windows would **intermix** `condition_id` in same leaf (query must filter `series_id` post-scan).

Two options: **hive `window=`** `data/book_snapshots_500ms/window=5m/date=.../asset=...` (add `window_label` column, predicate pushdown, `clean_view.py:33`/`compaction.py:17` walk extra level, `cursor_store.py:41` per-`(asset,window)` pathfix); or `data/book_snapshots_5m/...` dataset-per-window (simpler for Kaggle export). Pick hive `window=` for scan efficiency.

---

## 5. Kaggle Pipeline (Hourly, Append-to-Same-15-Files, Safe Delete, Max Retries)

```
cron 0 * * * *  polymarket-kaggle-uploader (PM2, ecosystem.config.js:30 cron_restart "0 * * * *",:98 pattern)
  1. Gate: skip if no window_end < now (only full closed markets, not ongoing 500ms buffer)
  2. Flush ParquetWriter:flush():120 + MarketsLog.compact() + clean_view.build_clean_view():19
  3. For window in [5m,15m,1h,4h,1d]:
     export_per_asset_single_file:206 time-first sorted (TS_SORT_CANDIDATES:38), zstd, atomic tmp → data/kaggle_staging/{window}/{slug}/ 15 files; chainlink files COPIED across windows (not re-exported)
     write dataset-metadata.json {"title":"Polymarket {window} Crypto — BTC/ETH/SOL", "id":slug, "licenses":[{"name":"CC BY-NC-SA 4.0"}], "resources":[{"path":...}]}
  4. Auth: KAGGLE_API_TOKEN env else ~/.kaggle/kaggle.json chmod600 (kagglesdk/kaggle_http_client.py:34); use gghgg1/KGAT_c1ed93041c5ad3847aadbfa8b3b4bf34 — rotate secret after paste
  5. For each slug: dataset_status(slug) ? dataset_create_version(folder, version_notes="hourly UTC {now} +{rows}", convert_to_csv=False, delete_old_versions=False) : dataset_create_new(folder, public=True) with retry 5 (2s×2 jitter→60s) on 429/500, emit collector_events:kaggle_upload_* (enums.py)
  6. Poll dataset_status until ready (timeout 10m); on success update data/kaggle_staging/_kaggle_state.json {slug:{last_version, last_upload_utc, row_counts, md5}} + kaggle_upload_success; on failure after 5 → kaggle_upload_failed + watchdog alert watchdog.py:83 (add kaggle_upload_failed to alert_on config.py:83)
  7. After ready: prune hive only `date < checkpoint_ts-2h` and `max(market_end) < checkpoint_ts`, keep 2h buffer for re-export; never delete current open window; data/kaggle_staging kept (source for next concat if needed); NEVER blind age delete (fix storage/export.py:640 pc.datetime bug → datetime)
```

`license` recommendation: **`CC BY-NC-SA 4.0`** (Attribution, NonCommercial, ShareAlike) — prevents selling/profit and forces derivatives also NC; Kaggle `valid_dataset_license_names:982` group `cc` accepts any CC name (`kaggle_api_extended.py:5070` `CC0-1.0` / `CC BY-SA 4.0` examples); fallback `other` if strict.

---

## 6. Implementation Phases

**P1 — Schema+Gamma (no Kaggle yet):** `schemas.py:15` add `slug/window_label/window_size_seconds/recorded_at`, fix `rollover.py:88,104,124,232,244,261,334` `300` hard-codes + `reported_volume/liquidity` population, migrate aliases.
**P2 — Native multi-window:** `config.py:103,120` `window_sizes`, per-window `Rollover/Discovery/Cursor`, `parquet_writer.py:213` hive `window=`, `clean_view/compaction` walk, `collector.py:43` multi-rollover, tests `test_rollover:130` update.
**P3 — Kaggle uploader:** new `storage/kaggle_upload.py` (prepare_staging/write_metadata/upload_with_retry/poll/checkpoint), `cli_kaggle.py` `polymarket-kaggle-upload --window-sizes --dry-run`, `KaggleConfig` dict, `pyproject.toml:51` entry, `enums.py` `kaggle_*`.
**P4 — PM2 + safe delete:** `ecosystem.config.js:30` app `polymarket-kaggle-uploader` cron, `prune_hive_after_verified` (only full windows), max retries, watchdog.
**P5 — Tests & dry-run:** `tests/test_kaggle_upload` mocked `kaggle.api`, `polymarket-kaggle-upload --dry-run --verbose` → 75 files (5×15) rows vs `data/export` 44-file legacy, no API call.
**P6 — Live:** `pm2 start` + `pm2 logs`, first `dataset_create_new` public, then hourly `create_version`; `plan.md` §18-style gate: confirm Gamma `btc-updown-1h-{ts}` actually exists with liquidity before native `1h/4h/1d` live.

---

## 7. Open Confirms Needed

1.  Exact 5 slugs: `gghgg1/polymarket-5m-crypto` … `-1d-crypto` above?
2.  License: `CC BY-NC-SA 4.0` (recommend, strongest NC) vs `CC BY-NC 4.0`?
3.  Snapshots public file: `clean` (§9B `book_state='live'`) vs raw `500ms` (`stale` included)?
4.  Add `DOGE` now (as single-window `5m`) or stay 3 assets until (§8) pilot `df -h`/`free -m` proves capacity?

This `plan.md` is the build checklist; after you confirm, build proceeds P1→P6 in order (P1 must precede P2, P2 before P3).
