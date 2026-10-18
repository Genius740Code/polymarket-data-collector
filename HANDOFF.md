# HANDOFF — autonomous data-quality loop on polymarket-data-collector

Branch `perfect/pmdata-parity` · box `/home/fese/polymarket-data-collector` · updated 2026-10-18 ~11:50 UTC

---

## 1. Mission

Live Polymarket CLOB collector → 99% backtestable data: every field present/correct,
every gap explicit and narrated, nothing fabricated. Work continuously; each cycle
measures real numbers, fixes the single largest gap, gates through tests before push.

Real-Data-Only policy (AGENT.md/AGENTS.md) is inviolable:
- never fill/interpolate/synthesize gaps; gaps = `book_state='stale'` + `resync_episodes`
  + `collector_events` evidence rows, evidence never pruned
- all writes via ParquetWriter (atomic tmp+rename); retention → `_quarantine` first,
  only after verified upload
- Verification gate before every commit: adjacent pytest green, `ruff check --select F,E9`
  (no NEW hits), forbidden grep over diff = 0 (`synthetic|mock|fake|interpolat`),
  FAIL-on-old proof (new test must fail on pre-fix source), then commit → pull --rebase → push
- pm2-managed processes; opencode budget ≤2 concurrent subagents (models
  `nvidia/z-ai/glm-5.3`, flash fallback)

## 2. What changed in this pass (all pushed on `perfect/pmdata-parity`)

| Commit | Fix | Verified outcome |
|---|---|---|
| `a072785` | **backlog #1 `stale_no_episode` CLOSED.** Measured first (2026-10-18): orphan rids joining no `resync_episodes` parent = **50 unique rids / ~2.9k of 2.49M rows (0.28%) over 10-17+10-18** — airborne, not the ~700–750 spans of 10-17. Root cause of the residue: books marked stale off the WS-message path (cursor recovery, quiet books whose next real frame is the heal) never reached the per-message `_ensure_episode_for_stale_book` hook, so every 2Hz snapshot row of the spell carried a permanently orphaned uuid. Fix: `_ensure_stale_row_parent` runs the SAME find-or-create at row-emit time (evidence-only: no relabel, no gap fill, row content untouched; idempotent — one dict lookup per row after the first mint; minting goes through `handle_disconnect` whose RAM insert precedes persist, so a failed write cannot mint a storm) | deployed (pkill-INT restart 11:39, WAL replayed 761 rows, restart#194): **20 fresh `stale_no_episode` parents minted in the first 3 min** across BTC/DOGE/ETH/HYPE/SOL/XRP. Gates: ruff `--select F,E9` 20 before == 20 after; forbidden grep 0; FAIL-on-old 5/5; suite = exactly the 9 pre-existing backlog failures, none new; tests 5/5 pass |
| `3da1dcc` | bounded incremental unreadable-quarantine precheck (`quarantine_unreadable_bounded()`, watermark + 120s mtime lookback, 90s budget, no advance on exhaustion) wired into export Step 0c | live: `scanned=8633 … seconds=90.424 budget_exhausted=True` (old code: 312s+ unbounded per tick, blocked all export passes) → exports pass again |
| `5027d6d` | startup coverage-gap narrator (`Collector._narrate_startup_coverage_gaps`), deterministic `event_id = sha256(startup_coverage_gap\|{asset}\|{gap_start_ns})` for restart-storm dedup | narrator code live; `event_id` passthrough in `MarketsLog.append_event` |
| `8b1cba2` | committed `5027d6d` mentioned "interpolates" in a docstring | removed |
| `a544d7f` | `markets_latest` frozen since 2026-10-14: int64 epoch-ms fragments beside ISO strings → `pa.Table.from_pylist` ArrowInvalid every compact, backfill WARN-loop only. New `_build_union_table`: build verbatim FIRST; only on genuine union failure retry ONCE with **numeric-only** epoch-ms ISO-cast (2020–2040 band). Digit-STRING casts (`REAL_EPOCH_MS_STR`) and garbage pass through everywhere | backfill run 10:40 10-17: **no WARN** (273 historical WARNs now stopped); authority view 1.5MB(10-06) → 5.9MB, 31,764 rows, **all 29,731 settled `settlement_ts_utc` canonical ISO strings**, 9 markets upgraded inferred→official. Fail-closed retained for truly unmergeable unions |
| `745e8eb` | trades `asset=UNKNOWN` partitions: caller-labeled UNKNOWN while the trade resolved exactly in the roster (row carried `series_id='BTC-5m'`). `_handle_trade_message` now takes the roster/book asset when caller is UNKNOWN; never overrides a resolved caller asset; unresolvable stays UNKNOWN | 0 `asset_unknown_partition` events since restart **across all datasets** (was ~1.5/min; trades ~200/day unjoinable) |
| `fd39794` | narrator dead-in-prod since `5027d6d`: `cfg.storage.data_dir` is a **str**, `base / "book..."` → `/ : str and str` TypeError; tests passed because the harness passed a **Path**. Fix: `base = Path(str(base))` | regression test with the exact live str shape; FAIL-on-old proven (same TypeError on HEAD) |

Offline-backfill note: `resolution_backfill` is a pm2 cron job (`*/15 * * * *`); it spawns
python fresh from disk, so `a544d7f` became live WITHOUT a collector restart.

## 3. Outage-evidence backfill (offline, idempotent, all measured)

Audited: the two 2026-10-17 outages had **no parent `coverage_gap` rows**. Row-level tick
census (streaming per-file reads, minute buckets, numpy-safe) measured:
- `2026-10-16 20:09 → 23:09` (179 absent buckets) — identical across all 7 lanes
- `2026-10-17 01:00 → 08:46` (465 absent buckets) — identical across all 7 lanes

14 parent rows emitted through the repo's own `ParquetWriter`/`MarketsLog.append_event`
(path: `event_type=coverage_gap`, `reason=outage_window_narration`, deterministic
`event_id = sha256(outage_window_narration|{asset}|{gap_start_ns})`, granularity note,
`gap_start_ns`). Re-running is idempotent; no ticks were filled. Audit WAL copy drained
after parquet landing (its payload was exclusively our 14 rows) so a future collector
restart cannot double-replay them.

Kill switch for future repeats: `OSError: [Errno 75] Value too large` from
`dt.datetime.fromtimestamp(ns)` — it wants SECONDS, never raw ns.

## 4. External ground truth ("our data vs pmdata")

- Exporter parity vs PMData sample (`data/pmdata/`, BTC-5m day 2026-10-04,
  slug `btc-updown-5m-1791138600`, ref: 135,325 L2 rows + 2,258 trades):
  **mid diff = 0 ticks**, all four event-type classes covered, count gate N/A by design
  (we firehose; PMData samples thresholded rows). Harness: `scripts/pmdata_diff.py`
  (+ `tests/test_pmdata_diff.py`; sample-note: `data/pmdata_test/` is an EMPTY scaffold dir).
- Independent venue cross-check (Kalshi KXBTC15M vs our BTC-15m resolution labels,
  exact UTC window-boundary alignment, zero tolerance):
  **30/30 agreement, 0 disagreement, 0 unmatchable** on the supplementary pass
  (`paperlog/kalshi_xcheck/summary.json`). Primary pass 20/20 unmatchable is a recency
  fact — our 15m lane historically stopped writing 2026-10-02 (see §7 backlog).
- `markets_latest` (settlement authority for export_pmdata/backtest_exec/backfill_pmdata)
  live and canonical again (§2, `a544d7f`).

## 5. Live quality state (measured 2026-10-18 09:2x)

Gate probe (`python scripts/gate_probe.py`, density% / CompNorm% gate >0.1):

| Lane | Density% | CompNorm% | Note |
|---|---|---|---|
| BTC | 100.0 | 0.0 | clean |
| ETH | 100.0 | 0.0 | clean |
| SOL | 100.0 | 0.0–0.14 flapping | transient (illiquid windows under worker load) |
| DOGE | 100.0 | 0.0 | clean |
| BNB | 99.5 | 0.0 | clean |
| XRP | 100.0 | 0.0 | cleared (10-17 0.2 was post-restart transient) |
| HYPE | 91.5→97.6 | 0.1 | **REST-dead upstream**: Polymarket `/book` genuinely 404s these windows even with true token IDs; collector's 27s stale→WS-provisional-promote→live cycle accounts for the deficit |

- `measure_1006_rate.py`: live% 36.76 → 70 → 74–88 across restarts; **0 stale-at-1006
  episodes** in recent spans.
- **Freshness-label share (measured 2026-10-18, by design — not a bug):** ~37–52% of
  `book_snapshots_500ms` rows read `stale` (last 2 days: 928k/2.49M; last hour per asset:
  BTC 39%, ETH 37%, BNB 49%, XRP 50%, SOL 47%, DOGE 45%, HYPE 52%). This is the
  `FRESH_BOOK_AGE_MS=5000` freshness design (frame-silence >5s reads stale honestly);
  uniform across liquidity classes, so it tracks venue frame cadence, not collector
  health. The density gate (§5 table) measures bucket coverage, not liveness-share.
  If a future cycle wants delta-labeled rows, design it as a NEW column — never touch
  `book_state` semantics, and never fill a stale row's content.
- Kaggle: first post-restart verified upload **2026-10-17T23:21:14** `✓ Upload success
  gghgg1/polymarket-5m-crypto` (was 26h stale since 10-16 13:08) + per-file uploads
  observed; **prune resumed** 23:45 (expired XRP trades quarantined then verified-reapable).
- Post-deploy `asset_unknown_partition` events: **0** in any dataset (was ~115/79/77 trio
  of per-dataset trickle on 10-17 alone).

## 6. Pipeline + ops facts

- pm2: `polymarket-collector` (id 11), `polymarket-watchdog` (6), `polymarket-resolution-backfill`
  (8, cron `*/15`, "stopped" between runs is NORMAL), `polymarket-compact` (7, stopped).
- **Full restart procedure**: `pm2 stop polymarket-collector` can HANG (pm2 daemon 55min CPU) —
  bypass with `pkill -INT -f polymarket_collector.cli`, escalate `pkill -KILL` after ~8s, then
  `pm2 start polymarket-collector` (autorestart will not fire if pm2 marked it stopped).
  Restart signature to verify new code: log line `[startup] replayed … rows from WAL` +
  no `coverage_gap narrate err`, then on the next export pass
  `[export] quarantine pre-check increment: … budget_exhausted=...`.
- "Quiet window" doctrine revised: gate `scripts/quiet_window.py` never exits 0 (as soon as
  one big asset staging ends, the next tick queues another job; a pass takes ~4h+).
  Mid-worker restart is SAFE by design (staging is tmp+rename; an in-flight job is clean
  discarded, no corruption). Accept one discarded staging pass instead of stale Kaggle days.
- Lane cadence reality: tick fires only on pass completion → per-lane verified upload
  every ~4–12h while a full 39-file pass runs serialized (~20 min per big asset, e.g.
  `BTC_book_snapshots_clean`). Options shaping a future cycle: progressive per-asset
  upload, or parallel staging (box: 2 cores, load ~4).
- Disk: 35–38G free on `/dev/sdb` (98G); `_quarantine` reap active; l2 retention 3 days.
- Untracked root files (`GATE_ANALYSIS.md`, `disk_watch_report.txt`, `monitor_disk.sh`,
  `.wtest.txt`, `logs/heartbeat-mirror.jsonl`) are pre-existing ops artifacts — do NOT commit.

## 7. Backlog / next cycles (priority order)

1. ~~`stale_no_episode`~~ **CLOSED 2026-10-18 (`a072785`, §2): emit-path find-or-create
   parent; 20 fresh parents minted in the first 3 min post-deploy.** Residual: the ~50
   historical orphan rids (≤0.28% of rows, 10-17/18) keep their honest orphan tags —
   rows are never rewritten; the counting rule for "episode-backed rows" should treat
   post-`a072785` rows as fully joined.
2. Kaggle cadence design decision (§6 third bullet) — progressive upload vs per-pass.
   Consider raising it given the ~37–52% stale-share finding above is a per-pass
   quality constant, not a defect.
3. 1h lane will re-stage its older datasets (clean/trades/chainlink keep their last
   verified cutoffs until its next tick; books-only are already fresh through 10-17 11:39).
4. 15m lane writes stopped 2026-10-02 (Kalshi primary pass consequence) — investigate
   the lane's history or accept documented.
5. Pre-existing (NOT from this pass, identical on stashed HEAD) failing tests, verified
   2026-10-18 (9, none new): `test_export_timeout.py::test_stream_snapshot_completes_on_real_files`,
   `test_chaos_ws_stale.py` (2), `test_rest_timeout.py::test_sleep_capped_at_2s_when_retry_after_large`,
   `test_silent_except_lint.py` (3), `test_r_fixes.py::test_r1_prewarm_gate…`, `test_stale_healing.py::test_heal_bg…`.
6. HYPE **parked by user decision** ("lets not use hype for now"): no WS-only banking
   policy change; its ~6.5% grid deficit is upstream REST 404s (verified against
   `https://clob.polymarket.com/book` with genuine token IDs) and is evidence-narrated.
   Do NOT implement WS-only from t0 for HYPE without an explicit user OK.

## 8. Quick commands

- `./.venv/bin/python scripts/gate_probe.py` (per-lane density/CompNorm table + gate verdict)
- `./.venv/bin/python scripts/measure_1006_rate.py` (live%/episode census)
- `./.venv/bin/python scripts/quiet_window.py` (gate exit 0 only when truly quiet)
- `./.venv/bin/python scripts/kalshi_xcheck.py` (external ground truth, report-only)
- `./.venv/bin/python scripts/pmdata_diff.py` (parity harness; needs ours+sample dirs)
- `python3 scripts/…` matches `.venv/bin/python` — prefer `.venv`.
