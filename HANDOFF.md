# HANDOFF — autonomous data-quality loop on polymarket-data-collector

Branch `perfect/pmdata-parity` · box `/home/fese/polymarket-data-collector` · rewritten 2026-10-19 ~12:1x UTC

---

## 1. Mission

Live Polymarket CLOB collector → 99.9%-backtestable data per `docs/PERFECT_DATA_SPEC.md`
(as amended 2026-10-18: quality = content correctness + fresh-share, `stale` is an honest
freshness label, never a defect): every field present/correct, every gap explicit and
narrated, nothing fabricated. Each cycle: measure real numbers → fix the single largest
gap → gate through tests → commit → pull --rebase → push.

Real-Data-Only policy (AGENT.md/AGENTS.md) is inviolable:
- never fill/interpolate/synthesize gaps; gaps = `book_state='stale'` + `resync_episodes`
  + `collector_events` evidence rows, evidence never pruned
- all writes via ParquetWriter (atomic tmp+rename); retention → `_quarantine` first,
  only after verified upload; rows are NEVER rewritten
- Verification gate before every commit: adjacent pytest green (9 known-failing are
  whitelisted, see §7.5), `ruff check --select F,E9` (no NEW hits), forbidden grep over
  diff = 0 (`synthetic|mock|fake|interpolat` — literal, watch the diff!), FAIL-on-old
  proof (new test must fail on pre-fix source), then commit → pull --rebase → push
- pm2-managed processes; opencode budget ≤2 concurrent subagents — use ONE mainly
  (models `nvidia/z-ai/glm-5.3`, flash fallback); session keeps tool outputs SMALL
  (counts/tails only — kernel OOM killed sessions here; pytest full suite can OOM the
  box, run targeted subsets)

## 2. What changed in the 2026-10-18/19 pass (all pushed on `perfect/pmdata-parity`)

| Commit | Change | Verified outcome |
|---|---|---|
| `a072785` | **backlog #1 `stale_no_episode` CLOSED** (emit-path find-or-create): `_ensure_stale_row_parent` mints the resync_episodes parent for a stale row whose rid joins no episode — books marked stale off the WS path (cursor recovery, quiet books) previously emitted every 2Hz row with a permanently orphaned uuid. Evidence-only, idempotent (dict-lookup per row after first mint; `handle_disconnect` RAM-inserts before persist → no mint storm) | deployed via pkill-INT restart 11:39 10-18 (restart #194, WAL replayed 761): **20 fresh parents in first 3 min**, all assets. FAIL-on-old 5/5 |
| `0ae92e1` | **completeness probe honest**: local `book_snapshots_clean` hive no longer materialized (clean view lives in per-lane Kaggle staging) → probe printed clean=0/missing=172800 every day. Now: local clean hive → per-date staging artifact → `clean_source="raw_footers_only"` fallback; cumulative staging NEVER attributed to one date; `to_dict()` superset with `clean_source` | first real daily numbers: BTC raw 175,982 ratio 1.0184; HYPE 0.8989 (real shortfall, now visible). FAIL-on-old 5/5 |
| `11239a7` | **scorecard gate live** (`scripts/scorecard.py`): one compact acceptance table per lane — density/CompNorm (reuse gate_probe), daily completeness (child), T1 day scan (bounded: dup approx-census, best-price bounds, orphans, offgrid, zero-price, sampled G6 depth audit), uploads/cycle, official-share, verdict | two idempotent real-data runs; first-ever scorecard: G6 **0 mismatches/10.9k**, dupes/offgrid/badpx/zero **0**, CompNorm **0.0%** all lanes |
| `61b181d` | scorecard refinements: orphans vs FULL `resync_episodes` table (cross-day false positives gone), fair per-lane budgets; new `scripts/density_split.py` (read-only missing-tick attribution) | density split measured (§4) — **empty-skip EXONERATED (~0)**; post_roll + outage dominate |
| `5793f0d` / `1ec07a2` | docs: spec §3.3 staleness bar → fresh-share contract; DATA_CARD HYPE structural block; handoff cycle notes | operator-validated |

## 3. Data quality state (measured 2026-10-18/19)

**T1 (zero tolerance): AT TARGET** — on live data: identity cols null-free (1.14M rows/day),
dup `(condition_id, ts)` 0, off-grid 0, bad-price 0, zero-price 0, G6 depth-recompute 0/10.9k,
oracle 1.0s cadence identical across all 7 assets (~225.8k ticks/3d each), `markets_latest`
canonical (31,764 rows, 29,731 settled ISO-canonical). External validation: PMData parity
**mid diff = 0 ticks** (BTC-5m 2026-10-04 sample, `scripts/pmdata_diff.py`); Kalshi
cross-check **30/30 agreement** (`paperlog/kalshi_xcheck/summary.json`).

**T2 (≥99.9%): ~2-3 points short** — live density 96.8–97.7% sampled (target ≥99).
Attributed 2026-10-18 (`density_split.py`): **post_roll ~35%** (first 120s of each window),
**outage ~30%** (collector restarts/deploy gaps), **other ~35%** (unattributed — needs deeper
pass), empty-skip ≈0 (NOT a suspect). BTC 18,862 / ETH 16,898 / SOL 20,120 missing of ~270k.

**T3 (bounded): semantics fixed** — `stale` = honest freshness label (`FRESH_BOOK_AGE_MS=5000`,
~40–55% of 500ms rows; spec amended). CompNorm ≤0.1% gate PASS (HYPE flaps 0.3–0.5% —
market structure: median OBI −0.167 ask-heavy, thin like DOGE/BNB, no Binance spot exists;
pipeline never consumes Binance — DATA_CARD §HYPE).

**T4 (attribution): ~99% closed** — emit-path minting live; **~2.9k historical orphan
`resync_id` rows remain** (one transition window 10-18, full-table-confirmed true orphans,
bounded, no ongoing loss — 168,078 fresh mints persist fine). Treat as ACCEPTED evidence-debt
or run one evidence-only narration pass (repo's own writer, deterministic event_ids — precedent:
2026-10-17 outage narration, §5 old version).

**Resolution:** official-share **86.0%** (29,336 official / 4,786 `inferred_nearest`,
target ≥99%) — grinding down passively via the `*/15` backfill cron; scorecard tracks it.

## 4. Ops facts

- pm2: `polymarket-collector` (11, ~1.1GB steady, cap 1500M), `polymarket-watchdog` (6),
  `polymarket-resolution-backfill` (8, cron `*/15`, "stopped" between runs is NORMAL),
  `polymarket-compact` (7, stopped by design).
- **Full restart procedure**: `pm2 stop polymarket-collector` can HANG — bypass with
  `pkill -INT -f polymarket_collector.cli`, escalate `pkill -KILL` after ~8s, then
  `pm2 start polymarket-collector`. Verify: `[startup] replayed … rows from WAL`, no
  `coverage_gap narrate err`, then `[export] quarantine pre-check increment: …`.
  Mid-worker restart is SAFE (staging tmp+rename; accept one discarded pass).
- **Local hive is now a ROLLING 2h mirror** (operator-authorized `c5896b3`, root-disk
  pressure): verified-uploaded rows are pruned locally, full history lives on Kaggle.
  CONSEQUENCE: a next-day full-day scorecard of a pruned date reads ~7% density — an
  artifact, NOT loss. Same-day scoring semantics still to be built (§7.2).
- Disks: data hive on `/dev/sdb` 98G at **37%** (healthy); root `/` 19G at **99% (361M
  free) — operator flag raised 2026-10-19**: driver is `~/.local/share/opencode` at 5.8G
  (user's session store, old rule: do not touch silently). Options offered: trim old
  opencode sessions / journald vacuum / move to sdb. AWAITING OPERATOR CALL.
- Lane cadence: exporter pass serialized ~4–12h per full 39-file cycle; per-lane verified
  upload every ~4–12h (progressive upload = §7.4).
- Untracked root files (`GATE_ANALYSIS.md`, `disk_watch_report.txt`, `monitor_disk.sh`,
  `.wtest.txt`, `logs/heartbeat-mirror.jsonl`) + `paperlog/` churn are ops artifacts —
  do NOT commit; `paperlog/kalshi_xcheck` is tracked but regenerates (commit only if
  deliberate).

## 5. Subagent workflow (user-directed)

- Code changes go through ONE subagent mainly (`general`), ≤2 concurrent; docs/measurements
  may be inline. Pass each subagent the full gate contract (pytest subset green, ruff no-new,
  forbidden grep 0, FAIL-on-old, explicit-path commit → pull --rebase → push) + OOM discipline
  (bounded scans, small outputs, no full-table reads) + "only the named files" rule.
- If a subagent ends without a report, resume its sessionID; verify the commit/push state
  directly with git before trusting any claim.
- When the main session's context gets full: re-read the diff of the current change.

## 6. Quick commands

- `./.venv/bin/python scripts/scorecard.py --date <YYYY-MM-DD>` (acceptance table + verdict;
  same-day only for full-day dates while local is a 2h mirror)
- `./.venv/bin/python scripts/density_split.py --date <d> --asset <A>` (missing-tick attribution)
- `./.venv/bin/python scripts/gate_probe.py` (density/CompNorm live-window gate)
- `./.venv/bin/python scripts/measure_1006_rate.py` (live%/episode census)
- `./.venv/bin/python scripts/kalshi_xcheck.py` / `scripts/pmdata_diff.py` (external ground truth)
- `./.venv/bin/python -c "from polymarket_collector.completeness import compute_daily_completeness; …"` (daily, footer-only)

## 7. Backlog / next cycles (priority order)

1. **Post-roll density fix** (largest attributed bucket ~35%): reduce the stale window in
   the first 120s after each window opens (promotion speed-up); attribute the "other" ~35%
   deeper BEFORE touching write cadence (candidate: fresh-generation/restart windows).
   FAIL-on-old + emit-path-safe (evidence never rewritten).
2. **Scorecard same-day/rolling semantics**: prune-aware verdict (full-day dates score
   same-day or from the remote mirror; add a `stale_date` warning instead of a fake-FAIL
   density number). Small.
3. **Optional orphan narration** (~2.9k rows): one evidence-only pass via repo's own
   writer, deterministic event_ids (outage-narration precedent) — or document ACCEPTED.
4. **Kaggle cadence <1h**: progressive per-asset upload (v1: fixed daily slot). Prerequisite
   for the PMData-format deliverable.
5. **Scheduled PMData-format export**: wire `export_pmdata` after the daily export pass →
   `{ASSET}-{TF}.zip` + manifest → own Kaggle dataset; extend `pmdata_diff` to all
   assets × days and assert in scorecard. ("Data such as PMData" is otherwise DONE:
   parity mid=0 proven.)
6. **15m lane** (writes stopped 2026-10-02) + 1h re-stage: investigate or accept documented.
7. **9 known-failing tests → green** (whitelist → hard gate): `test_export_timeout` (1),
   `test_chaos_ws_stale` (2), `test_rest_timeout` (1), `test_silent_except_lint` (3),
   `test_r_fixes` (1), `test_stale_healing` (1).
8. **HYPE**: parked by operator decision; reclassified STRUCTURAL (oracle/book WS healthy;
   OBI normalized in research). Remaining lever: WS-only banking from t0 (explicit OK
   required) to recover its last ~2.6% density.
9. **Root disk 99%** (`~/.local/share/opencode` 5.8G): awaiting operator choice (§4).
10. Final acceptance: 24h full-day scorecard soak without budget cuts.

**Estimate to formal 99.9% pass on 5m lanes (BTC/ETH/SOL/DOGE/BNB/XRP, HYPE parked):
~3–4 focused cycles** — items 1–2 (density + semantics), 4–5 (deliverables); item 8
(HYPE) pending the operator; resolution share completes passively.

---

Standing rules (unchanged): real data only; no manual deletes of `data/`; gaps stay honest;
never `git add -A` (foreign TS scaffold + ops artifacts appear/disappear — stage explicit
paths only); do NOT push from dev boxes unless the operator says so; evidence rows are never
rewritten; the collector's `book_state` semantics are never repurposed (new metric = new column).
