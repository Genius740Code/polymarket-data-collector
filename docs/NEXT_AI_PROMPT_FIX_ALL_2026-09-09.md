# Prompt for the next AI session — FIX ALL KAGGLE AUDIT ISSUES + SEQUENTIAL TF TESTS (paste verbatim)

```text
Start by syncing the repo and reading the mission:

1. cd into the polymarket-data-collector repo and run `git pull origin master` (fast-forward; if local changes exist stash first; if remote unreachable continue from latest local `27ad1e7`).
2. Read `handoff.md` from the top, then `DATA_CARD.md` (known EXPECTED nulls), then `kaggle_null_audit_2026-09-09.md` in full (156 files, 6.07M rows, 13-issue catalog with file:line), then `PERFECT_DATA_SPEC.md` §2-§11 (tiers T1 100%, T2 ≥99.9%, T3 ≤0.1%) and `AGENT.md` (real-data-only, gaps stay NULL). Do NOT re-derive verified live facts in handoff § Verified live facts (2026-09-05).

You have ONE job — fix to "best data possible", then prove it with sequential live tests. Work in order, keep `pytest` green, never fabricate data (missing stays NULL per AGENT.md:0).

===============================================================================
PHASE 1 — FIX ALL ISSUES (in priority order, commit per group)
===============================================================================
Keep `pytest tests/ --ignore=tests/test_verify_gate.py` green before and after every fix (currently 131 passed on master). Fix ONLY via code — no manual `data/` deletes.

P0 — T1 data-corruption (must fix first, blocks joins):
- E1 market_id hex corruption (HIGH): `book_snapshots_500ms` has 217 distinct hex `market_id == condition_id` (19.7%, `data/book_snapshots_500ms:1`, hive 1.61M rows) vs `markets_latest` 0 hex. Root `rollover.py:592` `market_id=str(data.get("id") or condition_id)` and `collector.py:172` `market_id=state.current_condition_id` in `CursorStore` recovery. Why: Gamma `id` missing → fallback to `condition_id` loses numeric market id (e.g. 4349753), downstream dedup/join on `market_id` drops ticks (correlated 33.8% of 5m lows have hex vs 5.7% overall). Fix: only `market_id=data["id"]` else `None` (schema already `nullable=False` for snapshots per `schemas.py:64` → change to allow `None` + write `NULL`, or raise and skip market); add migration to rewrite staged `kaggle_staging/**/ *book_snapshots*.parquet` where `market_id like '0x%'` → join to `markets_latest` to recover numeric `id`; add test that `MarketInfo.market_id` never hex.
- E9 chainlink dup (MED): `chainlink_events` 56 dups/131,702=0.042% (`5m/15m/1h` 8 per BTC burst 2026-09-09T11:20:19-26Z 7s, `4h` 0) vs `G7` 0 target. Root `storage/parquet_writer.py:550` `_dedup_key(chainlink)=(report_id,)` but `report_id 100% NULL` (`chainlink.py:45` reserved). Fix: `if dataset=="chainlink_events": return (row["asset"], row["event_id"])` (or `(asset,ts_received_ns,price)`), add `test_chainlink_dedup_null_report_id`.
- E2 window_index 0 sentinel (HIGH): `trades` 162/253k=0.06% `window_index=0` (`BTC 5m` 150/187,319) never in snapshots range 124,231-5,963,219. Root `collector.py:453` `window_index: market.window_index if market else 0` and `storage/schemas.py:143` `TRADES_SCHEMA window_index nullable=False` forces sentinel. Fix: `TRADES_SCHEMA:143` → `nullable=True`, fallback `None`, `export.py` filter `where window_index is not null`; audit for join break joins to `markets.parquet`.

P1 — T3 budgets + publish regressions:
- E3 clean BBO 17.8% null (MED-HIGH): `book_snapshots_clean` `74,943/420,177` rows any BBO null (`5m`; per-side 8-9% `BTC 5m` up_bid 8.42%, `15m` 6.8%, `1h` 1.4%) vs `PERFECT_DATA_SPEC.md:89` clean `≤0.1%`. Root live thin books min3 11% min4 83% (`DATA_CARD.md:21` ACCEPTED but clean view includes it). Fix: either document `clean` as `live+thin` and raise T3 to ~20% with min-breakdown, or add `book_snapshots_clean_quoted` view `where up_bid not null` for thesis OHLC; update `clean_view.py:1` and `DATA_CARD.md`.
- E5 side casing (LOW): `trades` distinct 4 `{BUY,SELL,buy,sell}` (`BTC 5m` 187k) vs `enums.py:24` lowercase. Root `collector.py:405` `.upper()` vs `storage/export.py:427` `api-` rows `.lower()`. Fix: normalize to `lower()` everywhere, add `CHECK side in ('buy','sell')`.
- E6 book_age dead (LOW-MED): `up/down_book_age_ms` distinct non-null `{0}` only, null 4.9% `5m` 67% `1h` (`book.py:219` `None` → `464/466` set `0` on `book` frame, never aged). Fix: increment `now - last_update` at `book.py:snapshot()` or drop columns `schemas.py:94`.
- E7 fee dead (LOW-MED): `trades.fee` distinct 1 `{0.0}` 100% zero, `fee_is_estimated True 22.3%` (`41,822` `5m` BTC) but value still 0 (CLOB `fee_rate_bps="0"` `collector.py:428` → `0.0` `False`; `export.py:369` derives `0.0` `True`). Fix: if `fee_rate==0` store `NULL` (or keep `0` but `fee_is_estimated=NULL`) and doc 0-fee market per `DATA_CARD.md`.

P2 — low severity:
- E4 zero sentinel `0.04%` (27/60,257 `down_bid` + 3 `up_bid_level_1_price`): empty side `0.0` not `NULL` (`book.py:693` `if up_bid is None: up_bid_size=None` misses `0`). Fix: `if not price` → `None` before `to_flat_dict` (`book.py:149`).
- Thesis completeness gaps: `5m` 63/721=8.7% low `<570/600` (worst 177), `15m` 14.3%, `1h` 33% (worst 15), `4h` 100% (span 17.5h <28,800). Not code bug — collection span 2026-09-08T21:18-14:55Z < one `4h` window. Fix: scope thesis to `5m/15m` until ≥7d span; doc `coverage_gap` 8h `2026-09-08T22:29→06:38Z` YAML indent bug `plan.md:320` as honest gap.

After each group: `pytest`, then `git add -A && git commit -m "fix(<scope>): <E# list> ..."` (do not push).

===============================================================================
PHASE 2 — 5m 2×5min KAGGLE TEST LOOP (destructive, operator-approved)
===============================================================================
This is the 5m-only pilot that must pass before any multi-TF.
- Verify Kaggle creds: `~/.kaggle/kaggle.json` or `KAGGLE_USERNAME/KAGGLE_KEY` env (`.venv/bin/python -c "from src.polymarket_collector.storage.export import _validate_kaggle_config; print(_validate_kaggle_config())"`).
- Run: `python run_2x5min_test.py 2>&1 | tee test_run_5m_<timestamp>.log`  (wipes local `data/` + deletes `gghgg1/polymarket-5m-crypto` as first step — approved, ~15-20 min, 2 windows ×7 assets).
- Analyze in order, fix and re-run until clean (bound 4-5 iterations):
  1. `pytest tests/ --ignore=tests/test_verify_gate.py` → green.
  2. `grep -i "ERROR\|WARN\|backpressure\|sequence_gap\|book_anomaly\|ws_error\|resync\|staging pre-validation failed\|✗" test_run_5m_*.log` → each hit is bug or upstream ACCEPTED (doc in `DATA_CARD.md`).
  3. `cat data/test_analysis_final.json` → `kaggle_staging.files==39`, `snapshot_completeness_pct` ≈100% (≤100), `clean_completeness` ≈100%, `data_loss_pct≈0`, `critical_null_flag false`, `book_state_histogram` mostly `live`, `rtds rx/parsed` ALL 7 assets incl. HYPE, `bonus_*` small.
  4. `ls data/kaggle_staging/5m/gghgg1/polymarket-5m-crypto/*.parquet | wc -l` → 39, `cat data/kaggle_staging/5m/.../dataset-metadata.json` license `CC BY-NC-SA 4.0`.
  5. Spot `kaggle_staging/5m/.../markets_summary.parquet` row count == closed windows.
  6. Run audit: `.venv/bin/python /tmp/audit_kaggle.py 2>&1 | tee audit_5m.log` (or inline `pyarrow` check: `book_snapshots 0% holes`, `BBO ≤20%` if thin, `chainlink dup 0`, `trades window_index 0 count 0`, `side 2 distinct`, `fee` doc). Fail on any new `UNEXPECTED NULL` beyond `kaggle_null_audit_2026-09-09.md:39` allow-list.
- Gate to proceed: ONE full iteration with pytest green + 39 files + Kaggle dataset `ready` (remote `kaggle datasets list --mine` shows `gghgg1/polymarket-5m-crypto` updated within 30 min) + completeness ≥99% on observed windows (or ≥95% with `coverage_gap` attribution per `PERFECT_DATA_SPEC.md:218`).

===============================================================================
PHASE 3 — SEQUENTIAL TF ROLLOUT (only if Phase 2 passed)
===============================================================================
Do not run all TFs at once. Do in order, each needs its own 2× window test + audit. Each test wipes `data/` — this is expected; staging per TF is `kaggle_staging/{tf}/`.

1. **15m** (30 min wall): `python run_2x5min_test.py --timeframe 15m 2>&1 | tee test_run_15m_<ts>.log` (~40 min, 2×15m). Gate: exit 0, `kaggle_staging/15m/.../BTC_book_snapshots_500ms.parquet` `series_id=="BTC-15m"` purity 100% (3614 rows each, zero cross-TF), `collector_events` full history (not 2-row truncation via `skip_datasets` fix), `completeness 100%` on 2 windows, `chainlink dup 0` after E9 fix.
2. **1h + 1h audit** (2h wall): `python run_2x5min_test.py --timeframe 1h 2>&1 | tee test_run_1h_<ts>.log` (≈2.5h, 2×1h). Gate: as above + run ` .venv/bin/python -c "import pyarrow.parquet as pq; t=pq.read_table('data/kaggle_staging/1h/gghgg1/polymarket-1h-crypto/markets_summary.parquet'); print(t.num_rows, [c.null_count for c in [t.column(c) for c in ['underlying_open','settlement_price']]])"` → `underlying_open` null ≤33% (expected 10s tolerance), `settlement_price` null ≤62% (short span, backfill heals via `resolution_backfill --reupload --all-lanes`). Also run the 1h per-column audit from `kaggle_null_audit_2026-09-09.md:114` (BBO 1.4% etc.) and compare to `5m` thresholds (expect lower BBO null on `1h`).
3. **4h (optional, soak)** (8h wall): only if `1h` passed and box has `df -h / >150M` and `uptime` stable. `python run_2x5min_test.py --timeframe 4h 2>&1 | tee test_run_4h_<ts>.log` (≈8.5h, covers one `4h` boundary). Gate: `book_snapshots` `28,800` ticks/window (`4h`) — expect `100%` low until span ≥7d, so thesis must still exclude `4h` (`thesis impact table`). For prod, enable `4h` lane via `config/collector.yaml` `timeframes: [5m,15m,1h,4h]` only after `4h` test shows `live>98%`, `dup 0`, `grid 0`.

Between TFs: keep collector `pm2` on `5m` prod (`pm2 start ecosystem.config.js --only polymarket-collector` with `FULL-FILE` restart so `1536M` cap applies). Do not enable `15m/1h/4h` lanes in prod until their test passed.

===============================================================================
PHASE 4 — COMMIT
===============================================================================
When 5m passes cleanly (or 5m+15m+1h if you did rollout):
```
git add -A
git commit -m "fix(data): E1 market_id hex, E2 window_index null, E5 side lower, E6/E7 fee/age, E9 chainlink dedup; verified via 2×5m (5m 100%/39 files) + 15m (100%/39) + 1h (audit BBO 1.4% underling 33%) — pytest <N>/131, staging 39/TF, kaggle ready"
```
Include `test_run_5m_*` tail `data/test_analysis_final.json` numbers and `kaggle datasets list` timestamp in commit body. Do not push unless operator asks.

Report at end: which Es fixed, iterations per TF (5m: N, 15m: N, 1h: N), final `test_analysis_final.json` per TF, `pytest` count, `kaggle_staging` files per TF, and new commit hash. Also note `1d` lane remains `OFF` (0 markets, out of scope per `handoff.md`).

===============================================================================
CONTEXT DOCS — read before fixing:
- `handoff.md` (LOOP STATUS, B1 heartbeat 10s, B2 single RTDS topic, B4 hot-add)
- `kaggle_null_audit_2026-09-09.md` (full 156-file per-column null/zero/distinct, §2 catalog E1-E13 with file:line)
- `DATA_CARD.md` (known EXPECTED nulls: L2 tail 9-10, ts_source 1-14%, wallets, report_id 100%, underlying 10s/5s, one-sided books min3-4)
- `PERFECT_DATA_SPEC.md:212` gate, `AGENT.md:0` real-data-only, `docs/WS_RESILIENCE_RESEARCH.md` (heartbeat + hot-add transcript)
- Tooling: `run_2x5min_test.py` wipes `data/`+Kaggle dataset as first step (approved); `--timeframe` flag added for multi-TF; `resolution_backfill --reupload --all-lanes` + `second_pass_enrich_trades` (15-min cron, 900s heal threshold) + `third_pass_onchain_wallets` (CTF OrderFilled)
```
