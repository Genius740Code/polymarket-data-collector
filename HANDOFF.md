# HANDOFF — perfect-data autonomous loop on polymarket-data-collector

> REWRITE 2026-10-23 ~10:5x UTC, replacing the 2026-10-19/20 body. Purpose: a fresh
> autonomous session (this or a sub-agent chain) can continue the mission on
> `perfect/pmdata-parity` without re-deriving anything. History worth keeping is
> compressed at the bottom; operational truth is the top.

Mission unchanged: live Polymarket CLOB collector → **99.9%-backtestable data** per
`docs/PERFECT_DATA_SPEC.md` (fresh-share + content correctness; `stale` is an honest
freshness label, never a defect). Every gap `book_state='stale'` + `resync_episodes` +
`collector_events`; evidence rows never pruned; retention (quarantine→reap) gates on
verified upload. **Real Data Only** — no synthetic/interpolated/fabricated content
(see AGENT.md/AGENTS.md; `synthetic|mock|fake|interpolat` never appears in diffs).

---

## 1) Current state (measured 2026-10-23 10:4x UTC)

**Repo/branch**: `perfect/pmdata-parity`, HEAD `26c632b`. All pushing flows through the
gate below; never `git add -A` (foreign TS scaffold + ops artifacts churn).

**Stack of landed, gate-proven fixes (all pushed):**

| Commit | Fix | Deploy state |
|---|---|---|
| `d66ed4a` #1a | memory-bounded `markets_log.compact()` (two-pass streaming; fixture FAIL 371.5MB → PASS ~103MB; prod-scale ~255MB) | ✅ LIVE since 10-21 13:17 |
| `85875f4` #4a | prewarm gap-tail stop 60s→180s, retry 10s→2s (`rollover.py:1330-1331`) | ✅ LIVE since 10-21 19:01 restart |
| `a5c03ed` #5a | bounded flush batches (`parquet_writer.py`: `MAX_ROWS_PER_WRITE_BATCH=20k`, wide-842-col family cap 1200 rows ≈ 47MB; 40.1KB/row measured) | ⏳ PUSHED, NOT deployed — running pid 1840264 predates it |
| `bef1b30` #5b | dead staging-tmp reaper (`export.py:6042`, grace 120min, ≥1 call/pass at `prepare_kaggle_staging_5m` top; recovers leaked per-pid ~140MB tmps) | ⏳ PUSHED, NOT deployed |
| `26c632b` #6 | scorecard coverage semantics: `classify_coverage()` pure fn → `partial-ok` = SOAK verdict for same-day, `stale-local` = WARN-only (data on Kaggle) for pruned ended days | ✅ LIVE (probe script, next run uses it) |
| config `c2fb5a0`+ | 1h lane PAUSED (`timeframes: [5m]`, dated comment + re-add conditions); `disk_space_min_bytes 1G→8G`; `l2_retention_days 3→2`; `l2_quarantine_max_deletes_per_cycle 1→3` | ✅ LIVE since 10-22 19:37 |

**Live fleet (pm2)**: `polymarket-collector` pid 1840264, uptime ~15h, ↺21 online,
RSS ~780MB ± hist; `polymarket-watchdog` online. **↺ counter frozen at 21 since 10-22
19:41** → ≥15h crash-free (previous best was hours).

**Night telemetry (2026-10-22 20:37 → 10:45, 357 samples/5min, `/tmp/opencode/night_log.csv`)**:
free disk 14→40G (the l2 dated partitions Oct 12 draining via quarantine-reap), settling ~14G;
`mem_avail` 455-1311MB; collector RSS 674-837 (median 781) — the OLD un-capped flush still
spiked inside that path (deploy #5a to bound it). **0 overnight respawns.**

**Disaster-fixed states**:
- Disk: 99% (1.7G free) at 10-22 19:2x → 78-86.7% now via the disk-guard funnel
  (8G threshold) + l2 retention ring (2d). The 500ms-hive coverage proof was frozen at
  2026-10-20 19:27 (500ms staging commits) — it heals as 500ms passes commit again.
- 1h lane: paused; Kaggle verified to 2026-10-19 14:01. Re-add only per config
  comment conditions (stable disk <75% 24h + one completed 1h verified upload).

**Quality baselines to re-measure (last measured 10-18/20):**
- Live density 96.8-97.7% sampled (goal ≥99.9%): buckets post_roll ~35% / outage ~30% /
  other ~35% (`scripts/density_split.py`).
- Post-roll discoveries (10-21 24h log evidence): 2443 5m windows, 330 (13.5%)
  discovered post-open p50 43.3s / p90 220.6s / max 299s, 82 >120s (77 `via=poll`).
  #4a deployed pre-streak → re-measure after a 24h soak.
- Official-share 86.0% (29,336 official / 4,786 `inferred_nearest`) — backfill-dependent
  (cron deleted as OOM hazard; #1a makes re-enabling safe; re-enable once RSS bounded).
- Content: dup/offgrid/badpx/zero = 0; CompNorm 0.0% all lanes; G6 0/10.9k;
  PMData mid-diff 0 ticks (BTC-5m 2026-10-04); Kalshi xcheck 30/30.
- T4 debt: ~2.9k historical orphan resync rows (bounded, no ongoing loss post `a072785`
  minting) — treat ACCEPTED or one evidence-only narration pass (deterministic ids).

## 2) CURRENT BLOCKER: deploy #5a+#5b (running pid is 15h old)

The quiet-window gate never found a 10-min green gap overnight — the 5m export pass
cycled back-to-back (each pass re-scans assets whose tmps were killed pre-publish,
~20-40min each x many datasets; gaps between passes exist but the probe window missed
them within the 8h arm). DEPLOY OPTIONS (order):
1. **Re-arm the watcher with a 24h window** (`/tmp/opencode/restart_watcher.sh` has
   `seq 1 96` → change to `seq 1 288`; it polls quiet_window every 300s and restarts
   on green). Honest pass-boundary gap will occur within ~24-30h of pass runtime.
2. If the pass completes a full cycle: the tick result `[kaggle:5m] staging N files -> …
   succ X prune Y` prints, the gate turns GREEN, watcher fires → deploy happens.
3. If a crash predates the green gate: pm2 respawns into HEAD automatically (any death
   deploys #5a+#5b).
After deploy VERIFY: `[flush]` totals unchanged, `[mem]` peaks < 900MB steady (the
747-850MB spike motif dies), the `[export] dead-tmp reap:` line prints per pass, and
`uptime` counter resets with ↺+1.

## 3) The subagent protocol (USER-DIRECTED WORKFLOW)

- **Budget HARD LIMIT: ≤2 concurrent subagents. Models ONLY: `nvidia/z-ai/glm-5.3`
  (preferred) or `nvidia/z-ai/glm-5.3-flash` (light scans). Never guess other model
  IDs.** One subagent per cycle = ONE narrow file/dir scope, "touch nothing else".
- Give every subagent the FULL gate contract (next block) + OOM discipline
  (bounded scans, small outputs, never the full pytest suite — it can OOM the
  3.9GB box; `-p no:cacheprovider`; RAM barely ok while the live collector runs —
  targeted suites only) + REAL-NUMBERS reporting contract.
- Subagent output is UNTRUSTED until verified: confirm the commit exists
  (`git log --oneline -1`), re-run the new test file yourself, re-grep forbidden
  words on the commit diff, re-check `git show --numstat` (2 files only, as
  claimed). Known fabrication modes to catch: invented "cycles", zero-byte
  reports, sleep-yield death, invented numbers.
- If a subagent ends without a report: resume its sessionID.

## 4) Verification gate (before EVERY commit)

1. Targeted pytest subsets green (the test files that touch your files; find them
   with `grep -rl "keyword" tests`). Full suite can OOM the box — never.
2. `./.venv/bin/ruff check --select F,E9 <files>` — no NEW hits vs HEAD.
3. Forbidden grep over the diff = 0: `git diff | grep -icE 'synthetic|mock|fake|interpolat'`.
4. Behavior change proven FAIL-on-old: new test fails on pre-fix source
   (stash the fix, run, record numbers, restore).
5. Commit per item, explicit paths only; `git pull --rebase` then push
   `origin perfect/pmdata-parity`. (Pre-existing working-tree churn to leave
   exactly as found: 25 `paperlog/*` deletions, a `foreign-churn` stash,
   plus any dated config edits we made.)

## 5) Restart discipline (deployment)

- Restart ONLY when `python scripts/quiet_window.py` exits 0. The watcher script
  (`/tmp/opencode/restart_watcher.sh`) encodes: exact-pid kill (INT → KILL after
  90s — bare `-f` patterns kill your own shell), pm2 respawns into HEAD.
- Any out-of-band death auto-deploys HEAD — that's fine.
- After deploy confirm: the new pid's `[startup] replayed … rows` line + the
  deployed fix's signature line (`[mem]`/`dead-tmp reap`/scan-budget prints) + ports +
  no `coverage_gap narrate err`.
- l2 disk-drain funnel fires automatically under `disk_space_min_bytes=8G`
  (16h runway at ~12GB/day l2_raw ingest). Don't raise retention_days back to 3
  without measuring disk steady-state again.

## 6) Backlog / next cycles (priority order)

1. **Deploy #5a+#5b** (unverified: the flush spikes still kill RAM headroom) →
   then VERIFY (below) → push docs update.
2. **#4a 24h post-roll soak re-measure**: fresh `window_discovered` census vs the
   10-21 baseline (p50 43.3s/p90 220.6s, 82 windows >120s). Expect post-open share ≪13.5%.
   Then `density_split.py` re-blame (post_roll bucket ~35%).
3. **Backfill re-enable** (official-share 86→99%): `pm2 start ecosystem.config.cjs
   --only polymarket-resolution-backfill` — ONLY after #5a deployed and RSS bounded
   for ~2h (cron influx vs caps). The bounded-compact fix (d66ed4a) protects it.
4. **PMData-format export deliverable** (`export_pmdata` wired after the daily pass →
   `{ASSET}-{TF}.zip` + manifest → own Kaggle dataset; extend `pmdata_diff` to all
   assets×days; assert in scorecard). This turns "PMData parity proven" into "PMData
   format delivered".
5. Optional: ~2.9k orphan narration OR document ACCEPTED; 9 whitelisted failing
   tests → green (list below); 15m lane + 1h re-stage (operator decisions);
   HYPE WS-only banking (operator explicit OK required).
6. **Final acceptance**: 24h full-day scorecard soak without budget cuts, all lanes
   `partial-ok`/`full` semantics (#6) — then formal PASS at ≥99.9%.

Whitelisted (preexisting known-failing) tests: `test_export_timeout` (1), `test_chaos_ws_stale` (2),
`test_rest_timeout` (1), `test_silent_except_lint` (3), `test_r_fixes` (1), `test_stale_healing` (1).

## 7) Quick commands

```
./.venv/bin/python scripts/scorecard.py --date <YYYY-MM-DD>   # acceptance table + verdict (coverage semantics #6 live)
./.venv/bin/python scripts/density_split.py --date <d> --asset <A>   # missing-tick attribution
./.venv/bin/python scripts/gate_probe.py                      # density/CompNorm live-window gate
./.venv/bin/python scripts/measure_1006_rate.py               # live%/episode census
./.venv/bin/python scripts/pmdata_diff.py                      # external ground truth (all-assets parity check next)
./.venv/bin/python scripts/quiet_window.py                     # deployment gate (exit 0 = safe)
pm2 ls; df -h data | tail -1; free -m | sed -n 2p              # steady-state probes
```

Ops scripts in /tmp/opencode (re-armable): `restart_watcher.sh` (gate-gated deploy),
`night_logger.sh` (5-min CSV telemetry), `night_log.csv` (dawn data source).

## 8) Standing rules (unchanged)

- Real data only; no manual deletes of `data/`; gaps stay honest; never rewrite rows.
- Do not push from dev boxes unless the operator says so (this session explicitly OK'd).
- Never `git add -A`. Stage explicit paths only.
- The collector's `book_state` semantics are never repurposed; new metric = new column.
- `paperlog/kalshi_xcheck` regenerates; commit only if deliberate; untracked ops
  artifacts (`GATE_ANALYSIS.md`, `disk_watch_report.txt`, `monitor_disk.sh`,
  `.wtest.txt`, `logs/heartbeat-mirror.jsonl`) never commit.
- Kaggle checkpoint (`data/kaggle_staging/*/gghgg1/*/_kaggle_state.json`,
  `_progress.json`) = single source of truth for verified uploads; the coverage proof
  per (lane,dataset) gates all deletes; quarantine reap only under `_quarantine/`.

---

## History appendix (compressed,OPS-critical facts from 2026-10-17–23 cycles)

- 2026-10-18/19: T1 (zero-tolerance identity/integrity/dup/offgrid/badpx/zero/G6)
  AT TARGET; T2 ~2-3 pts short (density); T3 stale semantics fixed (spec §3.3
  amendment); T4 emit-path minting live `a072785`, 168,078 fresh mints persist fine.
  `stale_no_episode` in `resync_episodes` = BY-DESIGN minted-parent label since
  `a072785` (measure scripts must split minted-parent vs legacy-orphan).
- 2026-10-20 ops crisis: pm2 wipe → zombie collector (cursor frozen 23.7h narrated as
  coverage_gap); fleet restored 19:55; OOM crash motif PROVEN — every death within
  ~10min of a `*/15` backfill cron; cron storm removed 21:26 (pm2 now collector+watchdog).
- 2026-10-21: 5m verified upload advanced 19h backlog (Oct 21 13:15); the #1b gate
  never broke — throughput + crashes were the cause. #1a/#4a landed (gate-proven
  FAIL-on-old on real numbers); 9+ SIGKILL OOM deaths (WAL replayed 0 rows lost
  every time). The 500ms-hive coverage proof froze at 2026-10-20 19:27 while passes
  kept dying mid-flight.
- 2026-10-22: disk emergency 99% (root: drain gates fail-closed on the frozen 1h
  checkpoint = the 2026-09-22 ENOSPC post-mortem re-run) → fixed by pausing 1h
  (config `c2fb5a0`), raising the guard threshold to 8G, l2 retention 3→2d, reaper
  3/cycle → 99% → 78-86% through the quarantine-reap evidence lines
  (`[quarantine-reap] deleted … age 230.5h`). Dead-tmp leak measured (~140MB per
  published-worker death ≈ 4-5GB) — #5b reaper lands the fix in code (no manual deletes).
- 2026-10-23 dawn: 15h+ crash-free (record), 357 telemetry samples collected, disk
  healthy (14-40G free), RSS median 781MB. Deploy of #5a/#5b still pending on the
  quiet-gate mechanism or the next natural death.
