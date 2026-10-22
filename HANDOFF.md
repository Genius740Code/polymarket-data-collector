# HANDOFF — autonomous data-quality loop on polymarket-data-collector

Branch `perfect/pmdata-parity` · box `/home/fese/polymarket-data-collector` · updated 2026-10-22 ~16:4x UTC (2026-10-22 disk-emergency cycle)

## 0. 2026-10-22 cycle state (measured; read first)

- **DISK EMERGENCY at 16:30 UTC: `data/` 97% (3.6G free)** — ~12GB/day l2_raw
  ingestion vs every drain gate fail-closed. Root: 1h lane verified-upload
  checkpoint frozen 2026-10-19 14:01 (1h export passes died to OOM mid-flight
  9+ times since 10-20: pm2 `SIGKILL` motif at exit-code-0 formation, WAL
  replays **0 rows lost** every time — write path durable). The config's own
  2026-09-22 ENOSPC post-mortem is the same motif.
  - **MITIGATION in effect**: `config/collector.yaml` `timeframes: [5m]`
    (1h PAUSED with dated comment + revert conditions, the documented
    2026-09-21-style cut). Deployed on the next restart by the armed watcher
    `/tmp/opencode/restart_watcher.sh` (fires ONLY when `scripts/quiet_window.py`
    exits 0; exact-pid INT→KILL escalation; pm2 respawns).
  - Post-restart expect: 5m-owned cutoffs (2026-10-22 08:18 verified upload)
    become the gate → ~17G of `book_snapshots_500ms/date=2026-10-20..22`
    prunes, book_events 5G follows, disk back toward ~70-75%.
  - 1h data is safe on Kaggle to 2026-10-19 14:01. Re-add `- 1h` only per the
    config comment conditions.
- **#1a landed `d66ed4a`** (memory-bounded `markets_log.compact()`, two-pass
  streaming; fixture FAIL 371.5MB→PASS ~103MB; production-scale rehearsal
  ~255MB vs ~500MB+ old). Verified live in the collector since the 10-21
  13:17 restart. Deaths continue → **#1a alone was not the whole crash cause**.
- **#4a landed `85875f4`** (prewarm gap-tail stop 60s→180s, retry 10s→2s).
  DEPLOYED already (current process started after the commit). Baseline to
  beat: 24h log evidence — 2443 5m discoveries, 330 post-open (13.5%),
  p50 lag 43.3s / p90 220.6s / max 299s, 82 windows >120s (77 `via=poll`);
  86.5% discovered pre-open. Re-measure after a 24h soak.
- **#5 crash motif MEASURED (next code cycle):** in the 14:04–14:39 incarnation
  (`logs/collector-out-0.log` lines 595268–601827): during the 5m lane tick the
  parent climbed `[mem] rss=530→850MB` with `ebuf` pinned 11–17k rows and
  `eps` 57→322 the whole pass (sweep stalled mid-tick = the 2026-09-25
  "pinned +600MB" motif, still live), while the export worker peaks 430MB–1GB
  (cap 1000MB). Steady baseline: collector ~780MB + watchdog ~170MB + pm2
  daemon 309MB + agent session ~455MB ≈ over 3.9GB with any worker spike.
  Deaths at cycle boundaries: post-upload validation (13:17), `[flush] 19996`
  (15:35), `[prune] quarantined…` (18:27), whale worker write (19:01).

## 0b. Subagent completions 2026-10-21/22 (both gate-proven, pushed)

| Commit | Item | Proof |
|---|---|---|
| `d66ed4a` | #1a OOM-bounded compact | 4/4 new tests green (re-run), FAIL-on-old wall-measured, ruff 0 new (1 pre-existing F401) |
| `85875f4` | #4a prewarm tail | 6 FAIL-on-old red→green, 86 passed / 1 pre-existing red (`test_r_fixes`, proven red at HEAD), ruff 0 new, forbidden 0 |


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

T4 metric caveat (2026-10-20): `disconnect_reason='stale_no_episode'` in `resync_episodes`
is now the BY-DESIGN label emitted on every minted parent
(`_ensure_stale_row_parent`, fix `a072785`), so any census that ranks episodes by
`disconnect_reason` will show `stale_no_episode` as the top bucket — that is the fix
working, NOT fresh unparented orphans. Fresh-orphan proof post-restart 19:55 was 0/637
sampled collector_events (2026-10-20 21:0x probe); the 2.9k historical rows stay the only debt.
`scripts/measure_1006_rate.py` should either exclude by-design parent labels or split
"minted-parent" vs "legacy-orphan" so nobody re-opens #1 by mistake.

**Resolution:** official-share **86.0%** (29,336 official / 4,786 `inferred_nearest`,
target ≥99%) — grinding down passively via the `*/15` backfill cron; scorecard tracks it.

## 2b. What changed in the 2026-10-20 cycle (ops crisis + restore, all verified live)

| Item | Event (measured, wall-stamped UTC) |
|---|---|
| pm2 wipe discovered 11:08 | collector survived as ORPHAN (ppid=1) but was ZOMBIE-WEENED: last cursor snap 2026-10-19 20:12 (age 23.7h narrated at startup as `coverage_gap`), heartbeats kept ticking, Kaggle last upload 2026-10-19 14:42 (29h stall). Watchdog + resolution-backfill NOT running since the wipe. |
| Fleet restored 19:55 | quiet_window GREEN (exit 0) → pkill -INT (ignored, `Rsl`) → pkill -KILL (note: `-f` pattern also SIGKILLs the invoking shell — quote patterns or use exact pids!) → `pm2 start ecosystem.config.cjs` (collector+watchdog+compact+backfill all online) → `pm2 save`. Books recovered `stale_ms=7`; live% 85.68→90.32 post-restart. |
| OOM crash motif PROVEN | 3 collector deaths today: ~11:08 (pm2 daemon wiped = presumed OOM), 20:43 (mid-export; WAL replay 38,271 rows OK), 21:25 (mid-export; WAL 35,664 rows OK). EVERY crash lands within ~10 min of a resolution-backfill cron run (cron `*/15`, ~500MB `markets_log.compact()` full-table read) while the collector's export build subprocess peaks ~1GB on a 3.9GB box → swap 4095/4095 → kernel OOM-kill takes the collector, export pass dies mid-build, ~40min/cycle lost + fresh outage gaps. |
| Cron storm removed 21:26 | `pm2 delete polymarket-resolution-backfill` (cron `*/15` storm) — second removal today, `polymarket-compact` deleted ~20:20 (both had "stopped by design" precedent in the 10-19 handoff). Fleet now collector + watchdog ONLY, `pm2 save`ued. Memory after deletion: available 300-400M → 1735M, swap draining (4095→3957). |
| Kaggle stall | export tick fired 20:07:56 (first), 20:52:38 (1h lane); staging cutoff ADVANCED 1792419186→1792434460; verified-upload STILL PENDING at 21:29 — revisit before trusting quality claims. Official-share upgrade (86%→99%) is CRON-DEPENDENT: with backfill deleted, share freezes until a solution lands (see backlog #1b). |

WAL integrity note: across all 3 crashes today zero rows lost (replay counts = flushed counts, startup lines show `post-replay flush persisted N (replayed=N)`).

## 4. Ops facts (updated 2026-10-20 — heavy overrides vs the 2026-10-19 notes)

- pm2 fleet NOW: `polymarket-collector` (pid varies, ~0.5–1.2GB, cap 1500M) +
  `polymarket-watchdog` (~170MB). `polymarket-resolution-backfill` (was cron `*/15`,
  ~500MB peak) and `polymarket-compact` are **DELETED from pm2** (21:26 / ~20:20 EDT)
  — see §4b consequence + revert paths. "stopped between runs is NORMAL" no longer
  applies (it does not run at all until re-added).

## 4b. Ops consequences + revert paths (2026-10-20 state)

- If the collector dies again, pm2 respawns it (↺ counter tracks the count; today ↺=2 by 21:26).
- Backfill re-enable path: `pm2 start ecosystem.config.cjs --only polymarket-resolution-backfill` — but NOT until the memory-bounded compaction fix (backlog #1b) or you re-create the OOM storm deliberately.
- Manual backfill burst (safe when no `[kaggle` tick is mid-pass, confirms fast): `.venv/bin/python -m polymarket_collector.resolution_backfill --config config/collector.yaml` (~500MB, a few min at 100% CPU).
- Confirm pm2 list persists: `pm2 ls` after `pm2 save`. Note compact/backfill have cron restarts defined INSIDE ecosystem.config.cjs — a bare `pm2 stop` leaves the cron armed; usage `pm2 delete` to fully retract.

Legacy notes (2026-10-19, superseded where marked):

- pm2 (HISTORICAL): `polymarket-collector` (11, ~1.1GB steady, cap 1500M), `polymarket-watchdog` (6),
  `polymarket-resolution-backfill` (8, cron `*/15`, "stopped" between runs is NORMAL — DELETED 2026-10-20),
  `polymarket-compact` (7, stopped by design — DELETED 2026-10-20).
- **Full restart procedure**: `pm2 stop polymarket-collector` can HANG — bypass with
  `pkill -INT -f polymarket_collector.cli`, escalate `pkill -KILL` after ~8s, then
  `pm2 start polymarket-collector`. Verify: `[startup] replayed … rows from WAL`, no
  `coverage_gap narrate err`, then `[export] quarantine pre-check increment: …`.
  Mid-worker restart is SAFE (staging tmp+rename; accept one discarded pass).
- **Local hive is now a ROLLING 2h mirror** (operator-authorized `c5896b3`, root-disk
  pressure): verified-uploaded rows are pruned locally, full history lives on Kaggle.
  CONSEQUENCE: a next-day full-day scorecard of a pruned date reads ~7% density — an
  artifact, NOT loss. Same-day scoring semantics still to be built (§7.2).
- Disks: data hive on `/dev/sdb` 98G at **69% / 30G free** (older note said 37% — superseded); root `/` 19G at **78%** (~4G free, the 2026-10-19 99% flag RESOLVED by entries on the operator's side — `~/.local/share/opencode` no longer listed as the driver at the measured size).
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

1a. **OOM resilience (ACTIVE blocker)** — three collector OOM-deaths on 2026-10-20
   (~11:08 pm2 wipe, 20:43, 21:25; every death ≤10 min into a backfill cron window).
   root cause: `resolution_backfill` runs `markets_log.compact()` = full-table
   pyarrow read (~500MB spike) while the collector's export build subprocess peaks
   ~1GB (3.9GB box, swap 4095/4095). Fix the chang e (bounded/filtered compaction,
   only rows needed for pending upgrades) via ONE subagent + full gate
   (`tests` targeted subsets; FAIL-on-old is a real memory-profile test — e.g.
   compact of a fixture 100k-row markets_log must stay <150MB peak RSS).
   Until fixed: keep backfill OUT of pm2 (deleted 21:26), manual bursts only.

1b. **Kaggle verified-upload still pending after 29h+ stall** (last checkpoint
   2026-10-19 20:09). Export ticks fire (20:07, 20:52) but each died mid-build to the
   OOM churn — the quiet-box pass after 21:26 should complete; VERIFY the checkpoint
   ts advances + `[kaggle] verified upload` (or equivalent) before trusting quality
   numbers. If it does not advance without OOM pressure, the 3600s worker budget or
   the 4852-file × 2.5s estimate needs measurement (`measure_export_impact.py`).

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
