# Prompt for the next AI session — self-looping fix-and-verify cycle (paste this verbatim)

```text
You are running a CONTINUOUS FIX LOOP on the polymarket-data-collector repo. Your job is to cycle
[test → analyze → fix] repeatedly until the pipeline produces a fully clean run, fixing every error
you find along the way. Do not stop to ask for permission between iterations — the loop is
operator-approved end to end.

STARTUP (do this first, every session):
1. cd into the polymarket-data-collector repo and run `git pull origin master`. If the remote is
   unreachable, continue from the latest local commit (baseline for this prompt: 6dffb06).
2. Read `DATA_CARD.md` in full — it lists KNOWN, EXPECTED nulls and ACCEPTED upstream behaviors.
   Never report those as defects and never "fix" them by fabricating data. Skim `handoff.md`
   (authoritative mission brief + verified live facts — do not re-derive those facts) and
   `../kaggle_audit_2026-09-06/AUDIT_REPORT.md` (the audit whose issues are already fixed).
3. Baseline check: `python -m pytest tests/ --ignore=tests/test_verify_gate.py -q` must be green
   (105 tests as of 6dffb06). If it is not, fix the failing tests BEFORE starting the loop.

THE LOOP (repeat; one iteration ≈ 20–25 min):

  STEP 1 — RUN THE TEST CYCLE
  `python run_2x5min_test.py 2>&1 | tee test_run_<UTC timestamp>.log`
  The tool intentionally wipes local data/ and DELETES + recreates the Kaggle dataset
  (gghgg1/polymarket-5m-crypto), collects 2×5min live, uploads, backfills resolutions, uploads the
  final version. ~15–20 min. Let it run to completion; never interrupt it.

  STEP 2 — ANALYZE (in this order; every hit is a bug to fix or an upstream fact to document):
  a) pytest still green? If not, fix and restart the iteration.
  b) Console log: grep for `ERROR`, `WARN`, `backpressure`, `sequence_gap`, `book_anomaly`,
     `ws_error`, `staging pre-validation failed`, `✗`, `Traceback`. Dedup repeated noise (e.g. the
     known `date_str fallback` WARN on resync_episodes is cosmetic — consider fixing it once by
     passing date_str at emission).
  c) `data/test_analysis_final.json`: staging files == 39; snapshot/clean completeness ≤100 and
     trending ≥96; data_loss_pct ≈ 0; critical_null_flag false; book_state histogram mostly `live`;
     bonus rows small and explained; RTDS counters show ALL 7 assets (incl. HYPE).
  d) Fresh-download audit of what Kaggle ACTUALLY serves (do not trust local staging):
     `kaggle datasets download gghgg1/polymarket-5m-crypto --unzip -p kaggle_audit_<date>_<iter>` and
     verify: exactly 39 files; no `sequence_number` in book_events; no `token_id` in collector_events;
     `up_book_hash`/`down_book_hash` present and populated in snapshots; trades carry
     `maker_wallet`/`taker_wallet`/`wallet` with taker fill >50%; resolutions backfilled
     (only the still-active window may be `unknown`); `markets_summary` rows present with sane OHLC;
     no crossed books in book_snapshots_clean; timestamps monotonic; chainlink cadence ~1s.
  e) WS health: count `ws_disconnected` vs run duration vs assets. As of 6dffb06 a run showed 1099
     disconnect/reconnect/resync episodes at 96% completeness. If churn stays at ~10+/min/asset
     across two runs despite the heartbeat fix, treat that as evidence of a server-side idle reaper
     → promote architecture C1 (warm standby + shared connection, design in
     docs/WS_RESILIENCE_RESEARCH.md) as the fix for the next iteration.

  STEP 3 — FIX
  Fix every issue found (code only — NEVER fabricate data; a missing value stays NULL, honest gaps
  are repo law). Keep pytest green before and after every fix. If an issue is upstream
  (Polymarket-side) and unfixable in code, document it in DATA_CARD.md + handoff.md as ACCEPTED with
  evidence, and continue.

  STEP 4 — COMMIT (every iteration, clean or not)
  `git add -A && git commit -m "loop iter<N>: <fixes>; evidence: pytest <n>/<n>, staging <n>/39, completeness <x>%, kaggle <status>"`.
  Do NOT push unless the operator asks.

KNOWN OPEN ITEMS (work these into the loop when nothing newer is broken — highest value first):
  - C2 maker-wallet backfill: taker_wallet fills ~95% but maker_wallet only ~13% (Data-API indexes
    both legs for a minority of fills). Fix per handoff C2: Polymarket exchange subgraph (OrderFilled
    has maker+taker) or Alchemy eth_getLogs on the two CTF Exchange contracts on Polygon; join key
    (transaction_hash, price, size); fill through the existing `_writeback_enriched_trades` path.
  - `underlying_open` NULL for some summary rows (7/27 in the 6dffb06-era run): check whether the
    10s open tolerance (A2) is actually applied in export.py; pre-collection windows must stay NULL.
  - `settlement_tx_hash` / `settlement_report_id` are permanently NULL with no wire source — either
    find a real source or drop the columns (schema change → bump DATA_CARD + schema tests).
  - collector_events `rollover_started` vs `rollover_completed` asymmetry: add a pairing invariant
    to the analysis (every started pairs with completed or coverage_gap) and investigate leftovers.

EXIT CRITERIA — stop looping only when ONE iteration satisfies ALL of:
  - pytest fully green; staging 39/39; final Kaggle version status ready;
  - fresh-download audit passes every check in STEP 2d with zero FAILs;
  - completeness ≥96% (≤100), data_loss_pct ≈ 0, critical_null_flag false, book_state mostly live;
  - no NEW unexplained ERROR/WARN hits in the log;
  - then run ONE more confirmation iteration with zero fixes needed. Two clean consecutive
    iterations = done. Practical bound: ~5 iterations total; if you hit the bound, commit what you
    have, summarize remaining issues, and stop.

REPORT AT THE END: iterations run, issues fixed per iteration (with commit hashes), ACCEPTED
upstream items, final analysis numbers, the fresh-download audit verdict, and anything that needs
an operator decision (e.g. the stale producer on the other box that can overwrite the Kaggle
dataset with an old-schema version — pm2 on this machine is empty; the old collector is NOT here).
```
