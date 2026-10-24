# HANDOFF — perfect-data autonomous loop on polymarket-data-collector

> REWRITE 2026-10-23 ~10:5x UTC, replacing the 2026-10-19/20 body. Updated same day
> ~13:1x UTC (see "## 1b) Session notes 2026-10-23 afternoon"). Purpose: a fresh
> autonomous session (this or a sub-agent chain) can continue the mission on
> `perfect/pmdata-parity` without re-deriving anything. History worth keeping is
> compressed at the bottom; operational truth is the top.

Mission unchanged: live Polymarket CLOB collector → **99.9%-backtestable data** per
`docs/PERFECT_DATA_SPEC.md` (fresh-share + content correctness; `stale` is an honest
freshness label, never a defect). Every gap `book_state='stale'` + `resync_episodes` +
`collector_events`; evidence rows never pruned; retention (quarantine→reap) gates on
verified upload. **Real Data Only** — no invented, padded or reconstructed content;
the exact forbidden-token list lives in AGENT.md/AGENTS.md and those tokens must
never appear in any diff (one exception, next line).

---

## 1) Current state (measured 2026-10-23 10:4x UTC)

**Repo/branch**: `perfect/pmdata-parity`, HEAD `e5cf18f` (docs commit on top of the fix
stack). All pushing flows through the gate below; never `git add -A` (foreign TS
scaffold + ops artifacts churn).

**Stack of landed, gate-proven fixes (all pushed):**

| Commit | Fix | Deploy state |
|---|---|---|
| `d66ed4a` #1a | memory-bounded `markets_log.compact()` (two-pass streaming; fixture FAIL 371.5MB → PASS ~103MB; prod-scale ~255MB) | ✅ LIVE since 10-21 13:17 |
| `85875f4` #4a | prewarm gap-tail stop 60s→180s, retry 10s→2s (`rollover.py:1330-1331`) | ✅ LIVE since 10-21 19:01 restart |
| `a5c03ed` #5a | bounded flush batches (`parquet_writer.py`: `MAX_ROWS_PER_WRITE_BATCH=20k`, wide-842-col family cap 1200 rows ≈ 47MB; 40.1KB/row measured) | ✅ LIVE since 10-23 10:56 (watcher-gated deploy, pid 1930987) |
| `bef1b30` #5b | dead staging-tmp reaper (`export.py:6042`, grace 120min, ≥1 call/pass at `prepare_kaggle_staging_5m` top; recovers leaked per-pid ~140MB tmps) | ✅ LIVE since 10-23 10:56. First pass fired 11:08:25: `[export] dead-tmp reap: deleted 39 file(s) / 3163129506 bytes` (3.16GB of leaked tmps) |
| `26c632b` #6 | scorecard coverage semantics: `classify_coverage()` pure fn → `partial-ok` = SOAK verdict for same-day, `stale-local` = WARN-only (data on Kaggle) for pruned ended days | ✅ LIVE (probe script, next run uses it) |
| config `c2fb5a0`+ | 1h lane PAUSED (`timeframes: [5m]`, dated comment + re-add conditions); `disk_space_min_bytes 1G→8G`; `l2_retention_days 3→2`; `l2_quarantine_max_deletes_per_cycle 1→3` | ✅ LIVE since 10-22 19:37 |

**Live fleet (pm2)**: `polymarket-collector` ↺23 online (see §1b for the 12:28 OOM
incident), `polymarket-watchdog` online 20h+. Deploy verify for #5a/#5b (10-23 10:57
→ 12:28, pid 1930987): `[flush]` totals unchanged (2k-38k rows/tick, same motif);
RSS steady 755-798MB with a single ≤1014MB startup-transient sample at 11:07:44 —
the 747-850MB flush-spike motif did NOT recur; no spike-motif deaths.

**Night telemetry (2026-10-22 20:37 → 10:45, 357 samples/5min, `/tmp/opencode/night_log.csv`)**:
free disk 14→40G (the l2 dated partitions Oct 12 draining via quarantine-reap), settling ~14G;
RSS max 837MB; `mem_avail` dipped to 0MB at least once overnight without a kill
(page-cache reclaim carried it). **0 overnight respawns.**

**Disaster-fixed states**:
- Disk: 99% (1.7G free) at 10-22 19:2x → 78-86.7% now via the disk-guard funnel
  (8G threshold) + l2 retention ring (2d) + #5b reaper reclaim. Root disk is the
  tight one for *code/logs* (15G/19G used incl. opencode); the `data/` mount
  (/dev/sdb) had 15G free at 12:1x-13:0x.
- 1h lane: paused; Kaggle verified to 2026-10-19 14:01. Re-add only per config
  comment conditions (stable disk <75% 24h + one completed 1h verified upload).

**Quality baselines (re-measured 10-23, see §1b):**
- **#4a post-roll soak RE-MEASURED (10-23 ~12:05, trailing-24h window 10-22 12:10 →
  10-23 12:10, 2280 5m windows): post-open 159 (7.0%)** vs baseline 330/2443 (13.5%)
  — halved. p50 112.7s / p90 248.8s / max 299.9s; 68 windows >120s (61 `via=poll`).
  Cumulative since the #4a deploy (10-21 19:01 → 10-23 12:1x, 3973 windows):
  248 post-open (6.2%), 91 >120s (79 poll). The >120s residue clusters in specific
  hours (7-asset batches ×1/h during 13-19h on 10-22 + the 19h deploy window struck
  26 + the 10-23 10h deploy hour hit 7) — poll-path lag during export passes; still
  the biggest loss bucket.
- Live density 96.8-97.7% sampled (goal ≥99.9%): buckets post_roll ~35% / outage ~30% /
  other ~35% (`scripts/density_split.py`) — re-run `density_split` after the catch-up
  pass settles.
- Official-share 86.0% (29,336 official / 4,786 `inferred_nearest`) — backfill last
  ran 2026-10-20 21:26 (`logs/resolution-backfill-out.log`); cron still OFF. Gap
  grows ~1.3k windows/day until re-enabled (§1b sequencing).
- Content: dup/offgrid/badpx/zero = 0; CompNorm 0.0% all lanes; G6 0/10.9k;
  PMData mid-diff 0 ticks (BTC-5m 2026-10-04); Kalshi xcheck 30/30.
- T4 debt: ~2.9k historical orphan resync rows (bounded, no ongoing loss post `a072785`
  minting) — treat ACCEPTED or one evidence-only narration pass (deterministic ids).

## 1b) Session notes 2026-10-23 12:00-13:1x UTC (afternoon session)

- **Deploy #5a+#5b completed via the watcher (as designed)**: `/tmp/opencode/restart_watcher.log`
  shows gate GREEN 10:54:43 → SIGINT 10:56:13 → SIGKILL 1840264 → pm2 respawn into
  HEAD at 10:56:15 (↺22). Verify numbers in §1 table.
- **FYI good**: quiet_window.out showed `OK: quiet window — no issues found`; the
  watcher exited after the deploy (NOT armed now — re-arm only for a future deploy).
- **12:28:09 kernel OOM — self-inflicted, PROBED**: the collector (RSS 722MB) was
  killed alongside tiny systemd units while an ~86-min catch-up pass was IN FLIGHT
  and an offline `scorecard.py --date 2026-10-22` (completeness child at 623MB) was
  running concurrently. The scorecard survived (not the victim); the collector pm2
  respawned (↺23, pid 1940035) and recovered cursors/books cleanly. **NEW HARD RULE:
  NEVER run any offline scan/probe (scorecard, gate_probe, tests, pytest) while a
  kaggle pass is in flight — check for an in-flight pass first**
  (`tail logs/collector-out-0.log | grep '\[export'`); the scorecard's own -560s
  per-lane budget + completeness child is ~600-800MB, which is exactly the headroom
  a big staging build consumes.
- **Kaggle backlog state (12:3x)**: 5m dataset last VERIFIED upload 2026-10-22 08:18
  (`_kaggle_state.json`); backfill last ran 10-20 21:26. The 11:02 tick (old pid,
  86+ min catch-up pass) died pre-publish — fail-closed, staging tmps remain and
  will be reaped by #5b on passes after their 120min grace. New pid re-armed the
  kaggle loop (12:31:36, `stagger +535s`) → next tick 12:36/12:40 window.
- **The scorecard rerun for 2026-10-22 is PENDING** (first attempt killed to free
  RAM at 12:5x, printed nothing). Re-run ONLY with no pass in flight, and give it
  the box to itself (it prints only at exit; buffer via `python -u` or run under
  `stdbuf -oL` to see progress live).
- **#4a soak re-measure done (see §1)** — the remaining 7.0% post-open residue p50
  112.7s is poll-path lag during passes; next fix lever = discovery scan budget vs
  export-pass overlap, NOT another prewarm tweak.

## 1c) Session notes 2026-10-24 (catch-up grind forensics)

**No crash-loop mystery anymore; also a NEW structural finding on the catch-up path
(read-only verified, no code changed).**

- **↺23 → ↺40, ~17 kernel-OOM deaths in ~24h** (journal `-k "Killed process"`
  showed every one: victims = collector parent python mid-pass, anon-RSS
  1.19-1.70GB; pm2 max_memory_restart (collector cap `'2100M'`,
  ecosystem.config.cjs:88) monitors ONLY the single parent pid at 30s
  (`PM2_WORKER_INTERVAL`, pm2 Worker.js:89-93) — it never fires here; the kernel
  OOM-killer wins when system avail hits ~0 during a pass peak + co-residents).
- **Co-resident thief found and reaped 10-24 13:44**: 38 stuck `PM2 v7.0.4: God
  Daemon` zombies (~570MB RSS beyond the live pid, spawned all at once during the
  10-20 20:2x ops storm) + 109 orphan `node …/pm2 update` processes. Killed all
  except the live God Daemon (pid 1446029); collector/watchdog unaffected, avail
  +~335MB immediately. All zombies shared one spawn moment → not an ongoing
  generator (watch for recurrence after another `pm2 wipe`-style event).
- **Night grind is REAL but partial**: `data/kaggle_staging/_progress.json`
  shows XRP 500ms cutoff advanced from the frozen 1792532244 (Oct-22 08:17) to
  ~1792795586 (late 10-23) and BTC book_snapshots_clean also advanced — scope-skip
  carry works. But 4 slices hit `spawn failed → TimeoutExpired` (10:51, 15:08,
  16:09 ETH, 17:09 SOL) and each commits only a `bounded in-process fallback`
  PARTIAL: `EXPORT_FALLBACK_FILE_CAP = 40` files
  (`src/polymarket_collector/storage/export.py:4824`).
- **Structural math (verified in code)**: worker budget =
  `files × 2.5s + 300s` clamped to `[1800, MAX 3600]`
  (`export.py:4849-4853`). Whale catch-up slices need ~10.5k files × 2.5s ≈
  26.6ks but the cap 3600s guarantees TimeoutExpired → per-slice 40-file slivers.
  Real scan rate ≈ 4.8s/file (worker 1941527 scanned 49min without finishing). The
  2.5s/file constant predates the current streaming-carry path. Backlog burn rate
  today ≈ (full slices) + (40-file slivers × seq) — the Kaggle checkpoint will
  advance on some slices per pass but the 2-full-day-old slivers may take
  multiple days at the current cap.
- **Candidate smallest fixes (documented, NOT applied — a pass is live; never edit
  `src/` mid-tick):
  1. Make `EXPORT_WORKER_TIMEOUT_MAX_S` avail-aware for catch-up slices
     (`export.py:4850`): grant max(3600, budget) up to a hard ceiling (e.g.
     9000-14400s) ONLY when avail ≥ the min-avail guard — mirrors the existing
     avail-guard pattern at :4876-78. Restart after a quiet gate.
  2. The in-process fallback PARTIAL publishes a *smaller* staging file than prior
     (success path carries prior rows + delta; fallback carries only its 40-file
     delta commit). Verify whether `_stream_export_asset_dataset` also streams the
     prior staging carry in the fallback; if not, the fallback must NOT replace the
     full staging file (keep-prior + skip, not partial-commit) — else each fallback
     would ship a truncated staging version to Kaggle. FAIL-on-old test needed.**
- **RSS sampler live**: `/tmp/opencode/rss_sampler.sh` (20s cadence, pure shell;
  parent+worker RSS columns → `/tmp/opencode/rss_sampler.csv`) — retention
  evidence for the death window; use it, not python probes, during passes.
- **Backlog item 3 (backfill re-enable) remains blocked** until one verified
  `[kaggle:5m]` tick lands under bounded RSS; keep the box lean meanwhile.


## 2) CURRENT STATE: deploy DONE; queue = catch-up tick → 2h bounded RSS → backfill re-enable

- Watcher-gated deploy happened at 10:56; the verify checklist near-met: flush
  totals unchanged, RSS steady ≤798MB with one 1014MB startup transient, dead-tmp
  reap line prints (3.16GB first pass). Last verify item: one full pass + a tick
  completing `[kaggle:5m] staging N files -> … succ X prune Y` under the NEW code.
- After that verified pass and ~2h bounded RSS on the current pid: re-enable the
  backfill cron (`pm2 start ecosystem.config.cjs --only
  polymarket-resolution-backfill`) — #5a+#1a now bound the co-spike hazard that
  killed it on 10-20. If avail-RAM is <600MB at re-enable time, wait.

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
   (Only doc lines that define the gate itself may quote the pattern — this is the
   sole sanctioned exception; code, comments and tests must hit 0.)
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

1. ~~Deploy #5a+#5b~~ → **DONE 10-23 10:56, verified** (§1/§1b). Next: finish ONE
   full pass + verified `[kaggle:5m]` tick under the new code, then push docs.
2. ~~#4a 24h post-roll soak re-measure~~ → **DONE 10-23 (~12:05)**: post-open
   13.5% → **7.0%** trailing-24h. Next: `density_split.py` re-blame after the
   catch-up pass settles; remaining residue = poll-path lag during passes.
3. **Backfill re-enable** (official-share 86→99%): `pm2 start ecosystem.config.cjs
   --only polymarket-resolution-backfill` — ONLY after a full pass + verified
   Kaggle tick + ~2h bounded RSS on the current pid (blocked 10-20 by co-spikes;
   now bounded by #5a/#5b/#1a) + avail-RAM ≥600MB. Backlog is now ~2 days (last
   verified 2026-10-22 08:18): expect the first pass to be slow (~1h worker
   budget); do NOT run anything concurrent.
4. **PMData-format export deliverable** (`export_pmdata` wired after the daily pass →
   `{ASSET}-{TF}.zip` + manifest → own Kaggle dataset; extend `pmdata_diff` to all
   assets×days; assert in scorecard). This turns "PMData parity proven" into "PMData
   format delivered". Needs the operator-side `base_url` for pmdata_diff ground truth
   or a secured reference copy.
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
