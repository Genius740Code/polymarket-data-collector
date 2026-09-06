# CONTINUOUS FIX LOOP — SESSION 2 (2026-09-06, after session 1 = 5 iterations)

You are running a CONTINUOUS FIX LOOP on the polymarket-data-collector repo. Your job is to cycle
[test → analyze → fix] repeatedly until the pipeline produces a fully clean run. Do not stop to ask
for permission between iterations — the loop is operator-approved end to end.

STARTUP (do this first, every session):
1. cd into the polymarket-data-collector repo and run `git pull origin master`. If the remote is
   unreachable, continue from the latest local commit (baseline for this prompt: 9dd0836).
2. Read `DATA_CARD.md` in full — it lists KNOWN, EXPECTED nulls and ACCEPTED upstream behaviors.
   Never report those as defects and never "fix" them by fabricating data. Also read the
   "🔁 LOOP STATUS (2026-09-06)" section at the top of `handoff.md` — it lists everything session 1
   already fixed and what is still open. Do not re-derive facts recorded there.
3. Baseline check: `python -m pytest tests/ --ignore=tests/test_verify_gate.py -q` must be green
   (105 tests as of 9dd0836). If it is not, fix the failing tests BEFORE starting the loop.

CONTEXT FROM SESSION 1 (already fixed — verify, don't redo):
- Kaggle `dataset_status()` returns a plain STRING; the status poll handles it, is observable
  (logs every poll), and is fail-closed with a 10-min budget. The 403 Forbidden on poll 0 right
  after `dataset_create_version` is NORMAL and clears in ~10s — do not "fix" it.
- Staging pre-validation compares only hive files with mtime ≤ staging-build start (self-race fixed).
- `scheduler_lag` no longer fakes ws_disconnected/resync events; `resync_episodes` = real episodes only.
- WS watchdog fires on >30s data silence, skips firing when the event loop itself is stalled
  (`_last_snapshot_bucket_ms` lag >15s = loop stall, not socket death), logs `[ws:ASSET]` lines.
- settlement_report_id / settlement_tx_hash are DROPPED from markets (no wire source). Do not add back.
- `rollover_pairing` invariant exists in the analysis JSON checks.

THE LOOP (repeat; one iteration ≈ 25–40 min):
  STEP 1 — RUN THE TEST CYCLE
  `python run_2x5min_test.py 2>&1 | tee test_run_<UTC timestamp>.log`
  The tool wipes local data/ and DELETES + recreates the Kaggle dataset (gghgg1/polymarket-5m-crypto),
  collects 2×5min live, uploads (verified), backfills resolutions, uploads the final version.
  ~25–40 min. Let it run to completion; never interrupt it.
  STEP 2 — ANALYZE (in this order; every hit is a bug to fix or an upstream fact to document):
  a) pytest still green? If not, fix and restart the iteration.
  b) Console log: grep for `ERROR`, `WARN`, `backpressure`, `sequence_gap`, `book_anomaly`,
     `ws_error`, `staging pre-validation failed`, `✗`, `Traceback`, `[ws:` (watchdog/recycle lines).
     Dedup repeated noise. The `date_str fallback` WARN is FIXED as of 9dd0836 — if it reappears,
     something regressed.
  c) `data/test_analysis_final.json`: staging files == 39; snapshot/clean completeness ≤100 and
     trending ≥96; data_loss_pct ≈ 0; critical_null_flag false; book_state histogram mostly `live`;
     `rollover_pairing.unpaired_started` == 0; RTDS counters show ALL 7 assets (incl. HYPE).
     NOTE: the completeness denominator counts ALL discovered windows — coverage-gapped windows that
     were never collected tank the percentage honestly. Judge by coverage_gaps + ws_disconnected
     counts, not completeness alone.
  d) Fresh-download audit of what Kaggle ACTUALLY serves:
     `kaggle datasets download gghgg1/polymarket-5m-crypto --unzip -p kaggle_audit_<date>_<iter>`
     Verify: exactly 39 files; no `sequence_number` in book_events; no `token_id` in collector_events;
     no `settlement_tx_hash`/`settlement_report_id` in markets; `up_book_hash`/`down_book_hash`
     present; trades carry wallets with taker fill >50%; resolutions backfilled (only the last ~15-min
     window may be `unknown`); `markets_summary` rows sane OHLC; no crossed books in
     book_snapshots_clean (a handful of rows = known crossing race, investigate only if >0.5%);
     timestamps monotonic; chainlink cadence ~1s.
  e) WS health: real `ws_disconnected` count vs run duration (fake ones are gone — every count is
     real now). ~1 reconnect/min/asset across 150s recycles is EXPECTED. `[ws:X] data-staleness`
     lines mean the watchdog saved you — check what the socket was doing (raw archive
     `data/raw_ws_archive/date=*/asset=*/*.jsonl` has every frame).
  f) Discovery health (NEW top item — see below): count `coverage_gap`, `subscription_failed`,
     `market_added`, and check window indices are consecutive per asset. Gaps in window sequence =
     Gamma discovery misses.
  STEP 3 — FIX
  Fix every issue found (code only — NEVER fabricate data; a missing value stays NULL, honest gaps
  are repo law). Keep pytest green before and after every fix. If an issue is upstream
  (Polymarket-side) and unfixable in code, document it in DATA_CARD.md + handoff.md as ACCEPTED with
  evidence, and continue.
  STEP 4 — COMMIT (every iteration, clean or not)
  `git add -A && git commit -m "loop2 iter<N>: <fixes>; evidence: pytest <n>/<n>, staging <n>/39, completeness <x>%, kaggle <status>"`.
  Do NOT push unless the operator asks.

KNOWN OPEN ITEMS (highest value first):
  1. **Gamma discovery retry/backoff (top code item).** Session 1's iter-5 run: 15 coverage_gaps +
     19 subscription_failed, out-of-sequence windows (5962367 → 5962369, 5962368 skipped) per asset.
     Gamma slug lookup serves only the ~2 most recent 5m windows (verified fact in handoff.md); when a
     poll fails or a window is skipped, the asset collects nothing for that window and it counts
     against completeness forever. Design: aggressive retry on discovery failure (backoff ≤5s), and a
     mid-window recovery probe — if an active asset has no market ~10s into a window, re-poll Gamma
     with both slug patterns (`{asset}-updown-5m-{ts}` and whatever the API actually returns; log the
     raw response on failure). Also investigate whether `subscription_failed` events (now carrying
     repr(e) and phase=discovery_poll) correlate with the skipped windows.
  2. **Resolution backfill timing**: the test tool's finalize backfill runs ~5 min after the last
     window, but official winners lag ~10 min — so the final Kaggle version always ships the last
     windows unresolved. Fix in code: make `run_post_test_finalize()` wait until either all markets
     are resolved or ~12 min elapse, THEN run the backfill + final re-upload. (Operator-side stopgap:
     run `python -m polymarket_collector.resolution_backfill --config config/collector.yaml
     --reupload` ~15 min after each test run — there is no pm2 cron on this box.)
  3. **C2 maker-wallet on-chain backfill — UNBLOCKED, key is in env.** `.env` (gitignored) holds
     `ALCHEMY_POLYGON_URL=https://polygon-mainnet.g.alchemy.com/v2/...` — verified live 2026-09-06
     (`eth_blockNumber` + `eth_getLogs` both work). NEVER commit the key or hardcode it in src; load
     it from the env var only (python-dotenv or os.environ; `.env` is already in .gitignore).
     Implementation: `eth_getLogs` on the Polymarket CTF Exchange contracts on Polygon — candidates
     CTF Exchange `0x4bFb41d5B3570DeFd05fa795c73e2dE17C7073D0`, Neg Risk CTF Exchange
     `0xC5d563A36AE78145C45a50134d48A1215220f80A` (verify addresses + the OrderFilled event topic0
     from the official Polymarket docs/ABI before coding; the Neg Risk Adapter
     `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` confirmed emitting logs on this key).
     **Free-tier constraint (verified live): `eth_getLogs` accepts max a 10-block range per call**
     (~20s of Polygon blocks) — page in ≤10-block chunks backwards from the fill's block timestamp,
     and mind the free-tier CU budget (10-block pages of a busy exchange add up; probe a page first).
     OrderFilled exposes maker+taker; join key to existing rows: (transaction_hash, price, size);
     fill maker_wallet NULLs through the existing `_writeback_enriched_trades` path (NULLs only,
     atomic per file, idempotent). Current fills: taker 80–99%, maker 14–78% (asset-dependent).
     Start with a one-market probe before wiring it into second_pass_enrich_trades.
  4. **Crossed rows leaking into book_snapshots_clean** (4 rows SOL, 0.09%, in the final artifact):
     race between the pre-snapshot `is_crossed()` check and frames applied after it. Cheap fix:
     clean_view build (storage/clean_view.py) could exclude rows where bid >= ask, or the snapshot
     path could re-check `is_crossed()` right before the row is emitted.
  5. **`data_loss_pct` conflation** (optional, analysis quality): the metric divides clean-view rows
     (live-only) by expected — stale rows are honest audit data, not loss. Consider reporting
     `missing_snapshots_pct` (grid gaps) separately from `stale_pct` in the analysis JSON.
  6. **Stale producer on the other box (OPERATOR ACTION, not code)**: if the old collector on the
     other machine still runs a Kaggle upload cron, it can clobber the dataset with a 30-file
     old-schema version. pm2 on this machine is empty; the old collector is NOT here. Nag the
     operator every session until confirmed dead. If the dataset ever serves 30 files / old schema
     again, this is why — republish from local staging and add the schema-fingerprint guard to
     `_upload_kaggle_folder` if not already present.

EXIT CRITERIA — stop looping only when ONE iteration satisfies ALL of:
  - pytest fully green; staging 39/39; final Kaggle version status ready + remote-verified;
  - fresh-download audit passes every check in STEP 2d with zero FAILs;
  - completeness ≥96% (≤100), coverage_gaps == 0, data_loss_pct ≈ 0, critical_null_flag false,
    book_state mostly live, rollover_pairing.unpaired_started == 0;
  - no NEW unexplained ERROR/WARN hits in the log;
  - then run ONE more confirmation iteration with zero fixes needed. Two clean consecutive
    iterations = done. Practical bound: ~5 iterations total; if you hit the bound, commit what you
    have, update the LOOP STATUS section in handoff.md, summarize remaining issues, and stop.
REPORT AT THE END: iterations run, issues fixed per iteration (with commit hashes), ACCEPTED
upstream items, final analysis numbers, the fresh-download audit verdict, and anything that needs
an operator decision (stale producer on the other box; C2 API key; pm2 cron restoration).
