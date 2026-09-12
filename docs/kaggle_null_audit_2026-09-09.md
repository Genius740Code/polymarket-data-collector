# Kaggle Null Audit — Polymarket Collector (all TFs) 2026-09-09

**Scope:** `kaggle_staging/{5m,15m,1h,4h}/gghgg1/polymarket-*-crypto` — **156 parquet files** (39/file ×4 TF, `1d` not staged: `markets_latest` has 0 `1d` markets, `config.py:157` `timeframes=[5m,15m,1h,4h]` `1d OFF` per `verify_gate --probe-timeframes`). Audit input is **published staging** (not hive `data/`), `pyarrow` per-file `rows/dtype/null/null%/zero/zero%/distinct`, `T4` attribution checks. Kaggle CLI not authenticated on this box (`kaggle` not in `PATH`, `~/.kaggle/kaggle.json` map object empty) — used local `kaggle_staging` which is the exact upload source (`export.py:206` staging `→` `dataset_create_version`).

**Total rows staging:** `5m 2,808,684` | `15m 2,007,630` | `1h 995,039` | `4h 258,676` = **6,070,029** (the user’s “6,070,029” matches staging sum; hive is larger: `book_snapshots_500ms` 1.61M + `trades` 297k + `chainlink` 180k across dates).

**Verdict:** **NOT HEALTHY — 7 T1/T2 gates fail, 4 T3 budgets exceeded.** Dataset is *honest* (`T4` 100% gaps in `resync_episodes`/`collector_events`, `L2` holes 0%, grid 0 violations, prices `0..1` 0% out-of-bounds, FK 0 orphans) but **not 99.9%** (`PERFECT_DATA_SPEC.md:212`). The pasted “HEALTHY 619 unexpected →0” report is **false** — it misclassifies tail padding and ignores `T3` budgets.

---

## 1. Dataset summary (real counts)

| TF | Files | Rows | Distinct `condition_id` (snap) | Expected ticks | Low `<0.95` | Staging `markets` | `markets_summary` |
|---|---|---|---|---|---|---|---|
| **5m** | 39 | 2,808,684 | 721 (raw `421,801` snaps) | 600 | **63/721=8.7%** | 735 rows | 259 rows, `underlying_open` 5.4% null `settlement_price` 2.7% |
| **15m** | 39 | 2,007,630 | 245 (421,732) | 1,800 | **35/245=14.3%** | 252 | 98 rows, `underlying_open` 14.3% |
| **1h** | 39 | 995,039 | 63 (362,109) | 7,200 | **21/63=33.3%** | 63 | 21 rows, `underlying_open` 33% `settlement` 61.9% |
| **4h** | 39 | 258,676 | 14 (89,527) | 28,800 | **14/14=100%** | 14 | 14 rows, `underlying_open` 50% |

*Low = `cnt < expected*0.95`. `4h` 100% is collection-span artifact: `global ts 2026-09-08T21:18:31Z–2026-09-09T14:55:08Z` (17.5h) < one `4h` window `28,800` ticks holds; worst `2,993/28,800=10%`. `1h` similarly truncated (worst `15/7,200=0.2%` at start).*

**Examples that match `GOOD` spec:**

| File | Column | Null % | Verdict |
|---|---|---|---|
| `BTC_book_snapshots_500ms` `5m` | `up_bid_level_10_price` | 16.98% (10,231/60,258) | **GOOD – EXPECTED** tail `lvl9-10` `NULL` (depth 4-8, `DATA_CARD.md:16`, holes `0` `book.py:616`) |
| `BTC_book_events` | `ts_source` | 0% (0/156,805 `5m`; 0% `15m/1h` – probe `book.py:244` `timestamp` 100% present) | **GOOD – EXPECTED 1-14%** (`DATA_CARD.md:12`) |
| `BTC_trades` | `maker_wallet` | 69.7% `5m` `64.8%` `15m` | **GOOD – EXPECTED** unattributed stays `NULL` (`collector.py:266`, `export.py:172`) |
| `BTC_chainlink_events` | `report_id` | 100% (24,746/24,746) | **GOOD – EXPECTED** reserved `NULL` (RTDS no `reportId` `chainlink.py:45`) |
| `markets_summary` `5m` | `underlying_open` | 5.4% (14/259) | **GOOD – EXPECTED** no tick `≤10s` (`export.py:747` `UNDERLYING_OPEN_TOL_MS=10_000`) |

---

## 2. Full issue catalog — `Error | Why | How to fix | Source`

> Per-column bucket: `CLEAN 0%` / `EXPECTED NULL` (allow-list) / `UNEXPECTED NULL` (flag). `NULL-vs-zero` = `0/0.0/""` where `NULL` required (`AGENT.md:17`, `book.py:481`, `schemas.py:71`).

| # | File(s) | Column | Null % / Zero % | Verdict | Why there is error | How to fix (code) |
|---|---|---|---|---|---|---|
| **E1** | `*_book_snapshots_500ms` (all TF, `BTC` `5m` `60258` rows) | `market_id` | `distinct 112` `hex` `9/103` `5m BTC` (hive `217/1099` `19.7%` distinct, `data/book_snapshots_500ms:1`) | **HIGH T1 `market_id` corruption** | `condition_id` (`0x`+66 hex) written into `market_id` (numeric e.g. `4349753`) – `rollover.py:592` `market_id=str(data.get("id") or condition_id)` fallback when Gamma `id` missing; `collector.py:172` `market_id=state.current_condition_id` in `CursorStore` recovery (`_recover_from_cursor`). Joins/dedup on `market_id` silently drop ticks; correlated `5m` `26/77` low (33.8% of lows `hex` vs `5.7%` overall). `markets_latest` has 0 hex (ground truth). | Keep `market_id` from Gamma `id` only; if missing `raise`/`NULL` not `condition_id`; migrate staged files (`export.py:_read_dataset_per_asset` already handles) + backfill `data/book_snapshots_500ms:1`. |
| **E2** | `*_trades.parquet` (`BTC_5m` `150/187,319=0.08%`, total `162/253k` `0.06%`; `15m/1h/4h` `0`) | `window_index` | `0` sentinel, `null 0%` | **HIGH `NULL-vs-zero`** | `collector.py:453` `window_index: market.window_index if market else 0` when token not in `self.books`/`rollover` (race at rollover). Valid range `124,231–5,963,219`; `0` breaks `JOIN markets.parquet` (`schemas.py:143` `nullable=False` forces sentinel). | `TRADES_SCHEMA:143` → `nullable=True`, fallback `None` (`AGENT.md:19` honest gap), `export.py` filter `where window_index is not null`. |
| **E3** | `*_book_snapshots_clean.parquet` (`5m` `74,943/420,177=17.8%` rows any BBO null; per-side `8.4%` `up_bid`, `8.1%` `up_ask`; `15m` 6.8%, `1h` 1.4%, `4h` 1.7%) | `up_bid/up_ask/down_bid/down_ask` (+`_size`) | `8-9%` per side | **MED-HIGH `T3 ≤0.1%` fail** (`PERFECT_DATA_SPEC.md:89` clean `≤0.1%` any BBO null) | `DATA_CARD.md:21` “one-sided books min 3-4 ACCEPTED ~11% min3 ~83% min4” – live exchange thin, but clean view includes it (live-only `clean_view.py:1`). `R` measured `0→2` min `0%` `3→11%` `4→83%` matches. Publisher claims `live` yet carries empty side; backtest that assumes `mid=(bid+ask)/2` gets `NULL` 1/6 ticks. | Either document `clean` as `live+thin` and raise `T3` to `~20%` with `min` breakdown, or split `book_snapshots_clean_quoted` (`where up_bid not null`) for `OHLC`. |
| **E4** | `*_book_snapshots_500ms` all TF | `up_bid/down_bid` `0.04%` zeros (27/60,257 `BTC_5m`), `up_bid_level_1_price` `0.005%` (3) | **LOW `NULL-vs-zero`** | Empty side coded as `0.0` not `NULL` (`book.py:693` `if up_bid is None: up_bid_size=None` misses `price==0` case; `validation.py:33` `0 ∈ [0,1]` passes). | `book.py:693-711` → `if not up_bid` (falsy `0`) → `None`; add `validate_snapshot_fields` `0==empty→NULL` check. |
| **E5** | `*_trades.parquet` all TF | `side`/`aggressor_side` | `0%` null, **distinct 4** `{BUY,SELL,buy,sell}` | **LOW casing** (`enums.py:24` lowercase) | `collector.py:405` `.upper()` vs `export.py:427` `api-` rows `.lower()` (`_backfill_trade_wallets` reconciliation). `pandas groupby` case-sensitive breaks. | Normalize `export.py:427` → `.lower()` everywhere or `schemas.py:155` `enum` check. |
| **E6** | `*_book_snapshots_500ms` raw `590/60,257=0.98%` `stale` `67` `resyncing` `5m` BTC; `15m` `650` `14.8%` null `up_book_age` | `up_book_age_ms`/`down_book_age_ms` | distinct non-null `{0}` only, null `4.9%` `5m` `14.8%` `15m` `67%` `1h` | **LOW-MED dead field** | `book.py:219` `None` → `book.py:464/466` set `0` on `book` frame, never aged (`OrderBookState` has no `on_tick` increment). All non-null `0.0` (`distinct=1`). Staleness not measured. | Increment `_up_book_age_ms += now-last_update` at `snapshot()` or drop column (`schemas.py:94`). |
| **E7** | `*_trades.parquet` all TF | `fee` | `0%` null, **distinct 1 `{0.0}`** `100%` zero, `fee_is_estimated` `22.3%` True `5m` (`41,822/187k` BTC) `68%` `4h` but `fee` still `0` | **LOW-MED inconsistent** | `collector.py:428` `fee_rate_bps="0"` → `0.0` `False`; `export.py:369` derives `fee_rate=0` → `0.0` `True` for `api-` rows – flag flips value not (`PERFECT_DATA_SPEC.md:157`). Polymarket 5m fees 0, so correct value but flag meaningless. | If `fee_rate==0` keep `fee NULL` or document `0-fee` market; otherwise `fee = notional*0.0007` when `fee_rate` missing. |
| **E8** | `*_book_snapshots_*` all TF | `*_level_1..8_price/size` | `8-25%` null (`5m` `up_bid_level_8 24.9%`, `1h` 0.6%) | **FALSE POSITIVE if flagged per-column** – actually **EXPECTED tail** (depth 4-8) | Hive `L2` holes `0/60k` per-row suffix check (`book.py:616` sorted `best-first`, tail `None`) – `per-column` null reflects shallow books + `E3` empty side ( `level_1` null `≈` BBO null). The user’s “619 unexpected” misclassifies tail as bug. | Keep `EXPECTED` for `lvl≤8` when `first_null` suffix (hole `0%` already). Only flag hole (`non-null` after `null`). |
| **E9** | `*_chainlink_events.parquet` (`5m` `56/131,702=0.042%`, `15m/1h` same 8/`24,746` per `BTC`, `4h` 0) | `event_id` dup | `8` dup per asset burst `2026-09-09T11:20:19-26Z` 7s | **MED `G7` dup 0 fail** | `parquet_writer.py:550` `_dedup_key(chainlink)=(report_id,)` `report_id` `100% NULL` → dedup never fires. | `parquet_writer.py:550` → `(asset,event_id)` or `(asset,ts_received_ns,price)`. |
| **E10** | `*_book_events.parquet` | `old_best_bid/new_best_*` | `10.8%` `5m` `3.5%` `15m` `2.5%` `1h` | **EXPECTED** (initial empty book) – the audit’s `619` list would flag these; they are honest first-event `None` (`book.py:540` `pre_bbo` `None`). | Re-classify as `EXPECTED`; only flag if `old==new` and non-null (no price move). |
| **E11** | `markets_summary.parquet` (`5m` `14/259=5.4%` `underlying_open` null, `15m` 14.3%, `1h` 33%, `4h` 50%; `settlement_price` `2.7→61%`) | `underlying_open/close`, `settlement_*` | see left | **EXPECTED** per `DATA_CARD.md:20` open `10s`/close `5s` tolerance (`export.py:747`) – longer windows more gaps due to short span, not collector bug. | Document `NULL` = no tick; no fix. |
| **E12** | `collector_events.parquet` `12,155` rows | `market_id` `100%` null, `condition_id` `81%` null | **EXPECTED** asset-level events carry no `market_id` (`storage/markets_log.py:128` nullable). | – |
| **E13** | `trades` + `book_events` `ts_received_ns` | `backward 16/156,805=0.01%` `5m` `9/68k` `15m` | **LOW** file-order artifact when `read_parquet` without `ORDER BY` (`parquet_writer.py:694` sorts per-group, not global). Global sorted `0` backward, `ts_received ≥ ts_source` `0` negative. | Query with `ORDER BY ts_received_ns`. |

> **Counts:** the user’s table `617/577/678/286 unexpected + 0 zero` is **instrumentation bug**: it counted every `lvl1-8` tail as unexpected and ignored `NULL-vs-zero` zeros (`27` etc). True `UNEXPECTED` after allow-list + hole check is **E1-E7,E9 = 7** families (not 2,160).

---

## 3. Thesis impact — why each error matters for research

| Thesis claim / analysis | Affected issue(s) | Impact if ignored | Severity | Mitigation for thesis |
|---|---|---|---|---|
| **Mid-price / spread / microprice backtest** (use `book_snapshots_clean` OHLC `mid=(bid+ask)/2` `DATA_CARD.md:23`) | **E3** BBO `17.8%` null, **E4** `0` sentinel, **E8** shallow depth | `mid`/`spread` undefined 1/6 ticks → look-ahead survival bias if dropped; spread `avg 0.01-0.02` understated | **HIGH** | Filter `where up_bid not null and down_bid not null` → use `markets_summary` `snapshot_count` to weight; report `thin-book` attrition in methods. |
| **Order-book depth / liquidity (H1: depth predicts resolution)** | **E6** `book_age` dead, **E8** tail `NULL`, **E1** `market_id` join loss | Depth `1c/5c/10c` `9-16%` null when side empty (correct) but `book_age` gives no staleness signal → cannot control for stale books; `market_id` hex loses 19% ticks for `up_depth` regressions | **HIGH** | Use `book_state` not `book_age`; fix `E1` before thesis joins; recompute depth via `depth_within` (`book.py:28`) to verify. |
| **Trader clustering / wallet graph (RQ: informed traders)** | **Trades wallet** `maker 60-93%` null (`5m` 69.7% `4h` 92.9%) | `unique_traders` in `markets_summary` `10-14%` null loss; network under-counts | **MED** | Query enriched `kaggle_staging` (enriched `export.py:172` + `second_pass` `third_pass_onchain`) – freshness `1h` 90% null heals after `15min` cron; document unattributed `NULL` = honest. |
| **Trade execution quality / aggressor classification** | **E5** side `4` values, **E2** `window_index 0` | `groupby side` splits `BUY` vs `buy` → `χ²` halves; `window_index 0` joins to wrong market (all `0` maps to genesis) | **MED** | `lower(side)` + `where window_index!=0` in SQL. |
| **P&L with fees (strategy profitability)** | **E7** `fee 0` + flag mix | `fee_is_estimated` `22% True` but `fee 0` → realized P&L overstates `0.07%` if reader applies `0.07%` fallback (`PERFECT_DATA_SPEC.md:157`) | **MED** | State Polymarket 5m fees 0 (cite CLOB `fee_rate_bps=0`), set `fee=NULL` when `fee_rate 0` to avoid confusion. |
| **Settlement correctness (up vs down label)** | **E9** chainlink dup, **E11** `underlying_open 33-50%` null | Duplicate chainlink ticks at `11:20:19Z` cause double-count if `count(*)`; missing `underlying_open` `1/3` `1h` windows → `resolution_outcome` `unknown` 61.9% null, breaks `up/down` lift | **HIGH** | Dedup `chainlink` on `(event_id)` pre-join; use `markets.settlement_source` (`inferred_nearest` vs `on_chain_confirmed`) and note `10s` tolerance. |
| **Market coverage / survivorship bias** | **E1** hex drop, **E2** completeness `8.7%→100%` low, `coverage_gap` `14` rows (8h gap `2026-09-08T22:29→06:38Z` `plan.md:320` `YAML` 4-space parse bug) | Backtest universe missing 1/12 `5m` windows (`63/721`) and all `4h` windows (100% incomplete) → survivorship bias; `4h` not thesis-ready | **HIGH** | Filter `date=2026-09-08` partial; for `4h` wait `≥7d` span (`capacity.py:15` `28,800*4h`); cite `resync_episodes` gap attribution. |
| **Reproducibility / data lineage** | **1d missing**, `4h` `100%` incomplete, `E9` dup | Thesis claiming `5 TF` robustness fails – `1d` has 0 rows, `4h` no closed window; reviewer cannot reproduce `1d` results | **HIGH** | Scope thesis to `5m/15m/1h` until `1d` lane enabled (`config.py:157`); archive `git commit` hash + `kaggle_staging/_kaggle_state.json` version. |
| **Causal ordering / latency (H2: latency → spread)** | `ts_received_ns` backward `0.01%` + `scheduler_lag` `1` row per run | Event-study with `ts_source` vs `ts_received` mis-ordered 16 rows shifts `10ms` `p99` | **LOW** | `ORDER BY ts_received_ns` (`PERFECT_DATA_SPEC.md:57`); latency `ts_received - ts_source` median `≤100ms` already in spec. |

> **Thesis-ready subset today:** `5m` clean after fixes `E1,E2,E5,E9` + `where up_bid not null` (≈ `350k/421k` `83%` ticks), `15m` similar, `1h` with `33%` low (use only `≥6,840` tick windows: `42/63`), `4h` **exclude** from thesis until `2026-09-16` (7d). Always cite `DATA_CARD.md:12-26` caveats and `AGENT.md:19` honest gaps.

---

## 4. Cross-file checks

* **Duplicates:** `book_snapshots_(clean)` `0`, `book_events` `0`, `trades` `0` (key `snapshot_id`/`event_id`/`trade_id`) ✓; `chainlink` `56` (`5m/15m/1h` 8 per `BTC` burst) **T1 fail** (`parquet_writer.py:550`).
* **Timestamps:** `ts_snapshot_ns %500M==0` `0/1.29M` ✓; `ts_received_ns ≥ ts_source` `0` negative ✓; monotonic per `condition_id` `16` (`5m` BTC) in file order but `0` when `ORDER BY` – writer sorts per-group (`parquet_writer.py:694`).
* **Referential:** `condition_id` in `markets.parquet` `735/252/63/14` = `book_snapshots_*` `721/245/63/14` – surplus `14` `markets` are pending `coverage_gap` windows, **0 orphans**.
* **Markets summary:** `resolution_outcome` `unknown` where `settlement_price` null (`5m` 2.7%, `1h` 61.9% – short span, settlement ~10min lag `DATA_CARD.md:22` `inferred_nearest` → `polymarket_official` after `resolution_backfill` cron `*/15`).
* **Book state:** `BTC_5m raw` `live 59,601 / stale 590 / resyncing 67` `0.98%` stale (`T3 ≤0.1%` slightly over but attributable `resync_episodes:80` rows, `gap 8h` honest); `clean` `100%` `live` `book_crossed 0` ✓.

---

## 5. Fix checklist — to get “best data possible”

| Pri | Error | File:line | One-line fix |
|---|---|---|---|
| **P0** | E1 `market_id` hex | `rollover.py:592` `collector.py:172` `book.py:195` | Only `market_id=data["id"]` else `None`; migration: rewrite `data/kaggle_staging/**/BTC_book_snapshots*.parquet` `where market_id like '0x%'`; backfill hive `book_snapshots_500ms` via `markets_latest`. |
| **P0** | E9 chainlink dup | `storage/parquet_writer.py:550` | `if dataset=="chainlink_events": return (row["asset"],row["event_id"])` |
| **P0** | E2 `window_index 0` | `storage/schemas.py:143` `collector.py:453` | `nullable=True` + `window_index=None` when `market is None`; `export.py` drop `0` rows. |
| **P1** | E3 clean BBO 17% | `storage/clean_view.py:1` | Document or add `book_snapshots_clean_quoted` view `where up_bid not null`; raise `PERFECT_DATA_SPEC.md:89` `T3` to `20%` for `5m` with min-breakdown. |
| **P1** | E5 side casing | `storage/export.py:427` | `lower(side)` everywhere, add `CHECK side in ('buy','sell')`. |
| **P1** | E7 fee dead | `collector.py:428` `storage/export.py:369` | If `fee_rate==0` store `NULL` (or keep `0` but force `fee_is_estimated=NULL`); doc `0-fee` market. |
| **P1** | E6 `book_age` | `book.py:219` `storage/schemas.py:94` | Either age-tick increment `now-last_update` at `snapshot()` or remove columns. |
| **P2** | E4 zero sentinel | `book.py:693` | `if not price` → `None` before flat dict. |
| **P2** | `4h/1h` completeness | `config` | Wait span `≥7d` for thesis; no code. |

All fixes keep `AGENT.md:0` **real-data-only** (gaps stay `stale`/`resyncing` + `resync_episodes`, never interpolate).

---

## 6. How this report corrects the pasted “HEALTHY” report

* The pasted report counted **2,160** unexpected nulls as **0** by treating every `lvl1-8` `8-25%` null as `EXPECTED 7-20` – misses holes vs tail distinction (holes `0%` is good, but `BBO 8%` clean is `T3` fail). It also reported **0** `NULL-vs-zero` but missed `27` `0.0` sentinels (`book.py:481`). It averaged `5m`+`15m`+`1h`+`4h` rows into one `6M` total, hiding `4h` `100%` low and `1h` `33%` low. **This report uses staging exact counts** (`2.80M`/`2.00M`/`0.99M`/`0.25M`) and `TF`-stratified thresholds (`600`/`1,800`/`7,200`/`28,800`). `1d` is correctly reported **missing**, not “failed to download”.

**Commit:** `git add kaggle_null_audit_2026-09-09.md data_quality_report.md && git commit -m "audit: kaggle null 4TF 7 T1/T2 fails, 6 fixes + thesis impact (book_age/fee/side/market_id/chainlink dup)"`

