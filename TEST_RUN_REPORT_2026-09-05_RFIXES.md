# R-1…R-5 Fix Round — Report & Live Verification (2026-09-05, runs R1/R2)

**Scope:** the five "known remaining issues" from `TEST_RUN_REPORT_2026-09-05_POSTFIX.md` §3.
All five are fixed, unit-tested (88/88 green) and verified against two live 2×5min
runs with Kaggle upload. Local data + the Kaggle dataset were wiped before the runs.

## Fixes

| Issue | Fix | Where |
|---|---|---|
| **R-1** cold-start +5s on the first window | Two-part fix. (a) **Pre-warm**: test mode now starts the collector *before* the 5m boundary (WS connect + Gamma discovery + subscriptions run during the align wait); a snapshot gate (`_snapshot_start_ms`) keeps snapshot rows gated to the boundary so the pre-boundary tail is not snapshot-ed. Run timing (`start_ts`), the Kaggle chunk schedule, completed-window counting, coverage-gap attribution and the analysis denominator all measure from the boundary. (b) **Root cause found live**: the CLOB **ignores subscription updates on an established connection** — lookahead-subscribed next-window tokens never received a single message (raw WS archive proof: zero `book` frames for those tokens until the next reconnect), so books sat stale until the boundary REST heal (+7.5s every window). `_ensure_ws_subscription` now drops the connection when new tokens are added on a non-fresh socket; the reconnect resubscribes everything and gets full books immediately — 60s *before* the boundary thanks to the lookahead. | `collector.py` |
| **R-2** wallet backfill coverage | The Data-API fetch now paginates deeply with an oldest-needed early-break (the old fixed 3–4 page cap truncated liquid markets — BTC's 36% null tail was largely *unfetched*, not unknown), and pools wallets **per leg** (takerOnly=false exposes a SELL leg = maker wallet and a BUY leg = taker wallet per fill). Attribution is deliberately strict: a leg pool names a wallet only when every API row at that (tx,price,size) key agrees — otherwise NULL is kept (never guessed). Reconciled `api-` rows inherit the maker leg when it was exposed. | `storage/export.py` |
| **R-3** api- rows fee/outcome unknown | `outcome` now comes from the Data-API's own authoritative label (Up/Down) instead of hardcoded "unknown" (also backfilled onto streamed rows via fill-key match). `fee` is derived for api- rows from the fee rate the exchange itself reported on that market's streamed rows (uniform per market, 0 on current 5m markets) and flagged `fee_is_estimated=true`; NULL when the market's streamed rows disagree. | `storage/export.py` |
| **R-4** Windows asyncio teardown noise | `cli.py` installs the Windows selector event loop policy (websockets ≥12 supports both loops; the Proactor socket close is what printed `ConnectionResetError [WinError 10054]` after all work completed) plus a belt-and-braces guard around `asyncio.run`. | `cli.py` |
| **R-5** missed-bucket "backpressure" noise | Scheduler catch-up now emits its own `scheduler_lag` event type; `backpressure` keeps meaning "the writer refused a row" (and no longer trips the watchdog alert on mere catch-up). | `enums.py`, `collector.py` |

## Regression tests

`tests/test_r_fixes.py` — 4 tests: pre-warm gate blocks then releases snapshots;
wallet backfill from both legs + reconciled-row outcome/fee (with honesty cases:
no-wallet fill stays NULL); Windows policy installs; missed buckets emit
`scheduler_lag` and never `backpressure`. Full suite: **88 passed, 0 failed**.

## Live verification — run R1 (19:26 UTC, pre-warm only)

- 14/14 windows (7 assets × 2), Kaggle 31/31 files, `status: ready`.
- Snapshot capture **100.17%** (8414/8400) — the cold window is gone at the tick level.
- R-5 visible in telemetry: `scheduler_lag: 105`, `backpressure: 0`.
- Books turned live only at **+7.5s/+8.5s** into each window → exposed the CLOB
  token-add behavior → led to fix R-1(b).

## Live verification — run R2 (20:03 UTC, pre-warm + reconnect fix)

- 14/14 windows, **99.92%** snapshot capture (8393/8400), Kaggle 31/31 files re-verified.
- **First live tick +0ms on every asset in both windows** (was +5.0s tick / +7.5s live
  in the cold window before). Remaining non-live rows are mid-window WS flaps
  (network-initiated, resynced with 0.1–2.4s gaps, 0 dropped rows) — honestly labeled.
- Teardown log clean: zero `10054` / `ConnectionResetError` / traceback lines (R-4).
- `scheduler_lag: 160, backpressure: 0` (R-5).
- `resolution_stuck: 7` = the pre-boundary tail markets (joined mid-window → no open
  price reference → honestly unresolvable), expected behavior of the pre-warm.

## Trades enrichment — final staged state (5,326 rows, 7 assets)

| Metric | Result |
|---|---|
| `wallet` null | **5.0%** overall (every asset 2.5–7.7%) — was 36% null on BTC; remaining NULLs are fills the Data-API itself exposes no wallet for (kept NULL, never fabricated). Staging self-heals: the API indexes fills asynchronously, so each re-export re-enriches. |
| `fee` null | **0%** (streamed rows: exchange-reported; api- rows: derived from the market's reported rate, `fee_is_estimated=true`) |
| `outcome` unknown | **0%** (api- rows carry the API's own Up/Down label) |
| `maker_wallet` null | 74.1% — the Data-API exposes both fill legs for only a minority of fills; a complete maker-side source would need on-chain RPC, which this collector deliberately avoids. Honest NULL. |

## One-command runner

`python run_2x5min_test.py` (or `run_2x5min_test.cmd`) = wipe local data → delete the
Kaggle dataset → 2×5min live test with Kaggle upload → ground-truth summary.
`--keep-data` skips the wipe.

## Remaining known limitations (honest)

- Maker wallets need on-chain RPC (deliberately avoided) — 74% NULL stays.
- The Data-API indexes fills asynchronously; an export run immediately after a window
  closes enriches less (self-healed by the next export/version).
- The pre-warm tail window's markets are collected (trades/book_events/chainlink) but
  cannot resolve (no open-price reference) → one `resolution_stuck` per market, honest.
