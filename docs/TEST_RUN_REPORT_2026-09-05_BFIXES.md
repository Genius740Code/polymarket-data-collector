# B-Round Fix Report — Backtest-Quality Round (2026-09-05, run v13 + finalize)

**Scope:** everything flagged in the backtest-quality audit + the user's asks
(#1 last tick, #3 disconnects, #4 HYPE chainlink, #5 hive wallet nulls, #7
unresolved → resolved every 15 min). Local data (157 MB) and the Kaggle dataset
were deleted first; the run then rebuilt everything and re-uploaded.

## Fixes shipped

| Fix | What changed | Where |
|---|---|---|
| **B-1 last tick** | Test mode now stops 1.5s after the last window completes so the final 500ms bucket lands (the 19:14:59.5 gap cannot recur). In 24/7 mode the process never stops on a boundary anyway. | `collector.py` |
| **B-3 disconnects** | Two changes: (a) the CLOB kills connections server-side at ~5min age, so the collector now **proactively recycles each socket at 150s on our schedule** — but *lightly*: no disconnect episode, no REST resync, no backoff; books relive from the fresh connection's full book and are honestly labeled stale for the ~1s swap via the existing `ws_connected` downgrade. (b) The 0–2s reconnect stagger is skipped when other assets are still connected (single-socket flap → no dead air). A first attempt that also tightened `ping_interval/ping_timeout` was **reverted** — the server answers pings slowly and a tight timeout kills healthy sockets from our side (measured: it tripled the churn and starved the RTDS feed). | `collector.py` |
| **B-8 event-loop freeze** | The export+upload ran synchronously inside the async loop and froze the 500ms scheduler for minutes (this run's mid-run chunk exposed it: the loop could not even reach its own timeout check). Export now runs via `asyncio.to_thread` in the test chunk path and the prod Kaggle loop. | `collector.py` |
| **B-4 HYPE chainlink probe** | The RTDS connection now archives raw frames and counts received/parsed per asset. Result (definitive, from the raw archive): **upstream sends all 7 assets at the identical cadence** (663 frames/asset/14 min — HYPE included) and we parsed everything we received. The old "HYPE has half the ticks" observation is a **dataset-shape artifact**: we subscribe to two RTDS topics, and for BTC/ETH/SOL/BNB/XRP/DOGE *both* topics carry the symbol (two price series per tick: a rounded feed ~79746.0 and the full-precision Chainlink report ~79749.05478…), while HYPE arrives only on the Chainlink topic. Nothing was ever missing for HYPE — the other six were double-stored. Chainlink rows this run: 8,795. Follow-up (documented, not yet done): subscribe only `crypto_prices_chainlink` or label the two series distinctly in `source`. | `collector.py` |
| **B-5 hive enrichment write-back** | After the export-time enrichment, enriched values are written **back into the hive trades partitions** — fill NULLs only (outcome's `"unknown"` sentinel counts as fillable), atomic per part file, idempotent, api- rows never written back. `data/trades/` now carries wallets (45% filled on first pass; self-heals further each export as the Data-API indexes more fills) instead of 100% NULL. | `storage/export.py` |
| **B-6 clean view on Kaggle** | `book_snapshots_clean` (live-only rows) now ships as a 5th per-asset file. Staging is **38 files** (7 assets × 5 + 3 globals); all validation counters updated. | `storage/export.py`, `collector.py` |
| **B-7 resolution backfill** | New `polymarket_collector.resolution_backfill` module: every 15 min (pm2 cron entry added; also run by the one-command runner) it scans markets past their end that aren't resolved and fetches the **official outcome** from the CLOB (`tokens[].winner` — Gamma drops 5m markets from slug lookup within minutes, CLOB keeps them forever), then appends resolved rows (`settlement_source=polymarket_official`) via the append-only markets log + atomic compact. `--reupload` pushes the final Kaggle version. | `resolution_backfill.py`, `ecosystem.config.js`, `run_2x5min_test.py` |

## Run results (v13, boundary 20:45 UTC, 2×5min, 7 assets)

| Check | Result |
|---|---|
| Windows | **14/14** (7 assets × 2) |
| Snapshot capture | **8457 / 8400 = 100.68%** |
| book_state | live 7,919 (93.6%) / stale 347 / resyncing 191 — **non-live 6.4%** (was 12–32%) |
| Grid / duplicates / crossed | 0 / 0 / 0 |
| Chainlink | **8,795 rows** (RTDS alive; B-4 counters + raw archive in place) |
| Resolutions | **21 resolved** — 14 `inferred_nearest` (in-run) + **7 `polymarket_official`** (backfill, incl. the pre-warm tail window that used to stay stuck forever); 6 `active` = next-window stubs |
| Kaggle | **38/38 files, status ready** — shipped twice: mid-run chunk (fail-closed, see below) + final version |
| pytest | **91 passed, 0 failed** (3 new B-round regression tests) |

## Incidents during the round (found by the run, fixed, re-verified)

1. **Chainlink died 115× (0 rows)** — the new RTDS counters used a plain dict; the first message raised `KeyError: 'XRP'` and the loop crash-reconnected forever. Fixed with a `defaultdict`; verified alive next run. Regression lesson recorded in the code comment.
2. **Ping-tuning regression** — `ping_timeout=10` killed healthy sockets from our side (server answers pings slowly): 158 connects, 32% non-live rows, RTDS starved. Reverted to defaults; the 150s light recycle owns connection age instead.
3. **Mid-run chunk fail-closed** — with the export now threaded, collection continued during the export and the staging-vs-hive row-count validation detected growth (`SOL_book_snapshots_500ms: staging 1200 < hive 1228`) and **aborted the upload, keeping all data** — exactly the fail-closed behavior I-12 was built for. The final upload then carried everything (38 files, success).

## Remaining honest limitations

- Two long server-side outages this run (BTC: 68.4s and 68.4s gaps, straddling window boundaries) produced the 106-bucket gap in BTC w5962137 and the 6.4% non-live rows. Recovery is honest and complete; eliminating the residual ~1–2s relive window entirely needs the hot-standby connection (phase 2, needs a CLOB connection-limit check).
- Chainlink stores two price series per tick for 6 of 7 assets (dual-topic subscription) — backtests should dedupe or filter by price precision; HYPE carries only the Chainlink series. Follow-up: single-topic subscription.
- `maker_wallet` stays ~74% NULL (Data-API exposes both fill legs for a minority of fills; a complete maker-side source needs on-chain RPC, deliberately avoided).
- Hive trades wallets fill progressively (45% on first pass here) — each export re-enriches as the Data-API indexes more fills; Kaggle staging is the fully-enriched surface.
- `book_events.ts_source` is null 3.6–13.9% (those WS frames carry no timestamp) — use `ts_received_ns` for those rows.

## Artifacts

- Run log: `test_run_20260905_213919.log` · Analysis: `data/test_analysis_final.json`
- Kaggle: `gghgg1/polymarket-5m-crypto` — 38 files, status ready, resolutions + enriched trades included
- One command for the whole cycle: `python run_2x5min_test.py` (wipe → delete Kaggle → collect → upload → resolve → final upload); `--keep-data` skips the wipe
- pm2 cron: `polymarket-resolution-backfill` every 15 min
