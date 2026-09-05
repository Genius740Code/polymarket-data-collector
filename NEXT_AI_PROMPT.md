# Prompt for the next AI session (paste this verbatim)

```text
Start by syncing the repo and reading the mission:

1. cd into the polymarket-data-collector repo and run `git pull origin master` (fast-forward to the latest commit; if there are local changes, stash them first and re-apply after pull; if the remote is unreachable, continue from the latest local commit `87abbda`).
2. Read `handoff.md` from the top — it is the authoritative mission brief. Also read `docs/WS_RESILIENCE_RESEARCH.md` and `DATA_CARD.md` when the checklist references them. Do NOT re-derive the "Verified live facts" listed there; they were measured against the production endpoints.

Then execute the three phases described in handoff.md, in order:

PHASE 1 — FIX: Work through the ISSUE CHECKLIST (section A first: ts_source probe + fix, markets_summary 10s open tolerance, l2_levels 20→10, book hash capture/validation; then verify section B items are code-ready). Keep `pytest tests/ --ignore=tests/test_verify_gate.py` green before and after every fix. Never fabricate data — a missing value stays NULL.

PHASE 2 — TEST-LOOP: Run `python run_2x5min_test.py` (tee output to test_run_<timestamp>.log). The tool intentionally wipes local data/ and deletes the Kaggle dataset (gghgg1/polymarket-5m-crypto) as its first step — that is operator-approved, let it run (~15–20 min). Then analyze per the TEST-LOOP PROTOCOL in handoff.md: pytest green; grep the log for ERROR/WARN/backpressure/sequence_gap/book_anomaly/ws_error/resync hits; check data/test_analysis_final.json (kaggle_staging.files == 39, completeness ≤100% and ≈100%, data_loss_pct ≈ 0, critical_null_flag false, book_state mostly live, rtds counters show ALL 7 assets incl. HYPE); verify the Kaggle dataset is ready with 39 files; spot-check markets_summary.parquet. Fix every issue you find (code fixes only — never mask a gap with fabricated data), then run the test again. Loop until one full iteration passes everything cleanly (bound: ~4–5 iterations). If an issue is upstream/Polymarket-side and unfixable in code, document it in DATA_CARD.md and handoff.md as ACCEPTED and continue.

PHASE 3 — COMMIT: When the loop passes cleanly, `git add -A && git commit` with a message listing the fixes and the final run's evidence (pytest count, staging files, completeness %, kaggle status). Do not push unless the operator asks.

Report at the end: which checklist items were fixed, how many test-loop iterations were needed, the final analysis numbers, and the commit hash.
```
