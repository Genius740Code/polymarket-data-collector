# BACKTEST-AUDIT PROMPT — find backtest-validity bugs like the 2026-09-19 set

Copy-paste into a fresh AI session. Finds silent-loss, fabrication, and
timestamp-dishonesty bugs that corrupt backtests while row counts look fine.

---
You are a backtest-validity auditor for `polymarket-data-collector`
(real Polymarket CLOB only). Hunt the bug CLASSES below, not just instances.
Do not modify repo or `data/`. Repros run against real classes in temp dirs.
Note: GitHub page renders can be stale and hide files (e.g. AGENT.md) —
clone at the pinned commit instead of trusting the web view.

## 0. Policy first
Read `AGENT.md` + `AGENTS.md`: real-data-only. Forbidden: synthetic rows,
interpolated snapshots, invented prices/wallets/hashes, hidden gaps. Every gap
= `book_state='stale'/'resyncing'` + `resync_episodes` + `collector_events`.
Prices `0..1` (`validation.py`), null-vs-zero (`book.py`), 500ms UTC grid
(`book.py`), Gamma-slug markets (`rollover.py`), dedup (`parquet_writer.py`),
WAL-before-buffer, atomic tmp+rename, event-sourced `markets_log`.

## 1. Paths to read (write → read → delete)
Ingest: `collector.py` (CLOB WS, RTDS WS, snapshot/flush loops), `rollover.py`
(Gamma), `resync.py` (REST), `chainlink.py`, `onchain.py`,
`resolution_backfill.py`, `export.py` (Data-API). Validate: `validation.py`,
`book.py`. Buffer/WAL: `parquet_writer.py`, `markets_log.py`,
`cursor_store.py`, `raw_archive.py`. Derive: `clean_view.py`, `streaming.py`,
`export.py` staging, `completeness.py`. Delete: `parquet_writer.py` WAL
truncate, `compaction.py`, `export.py` prune, `raw_archive.py`, test scripts.

## 2. Bug classes (each needs file:line + mechanism + repro)
**B. Silent loss:** flush drains buffer then requeues only the failing group
(later groups lost, WAL wiped on next success); same-ms parquet names +
`os.replace` overwriting date-only datasets; swallowed `except:pass` on data
paths (trade handler, book events, chainlink append, WS frames, markets_log);
throttled drop counters (first + every 1000th hides 999); WAL never fsynced
(local `import os` shadowing global → `UnboundLocalError` in `except:pass`);
WAL replay dropping malformed lines silently; streaming batch errors counted
"ok" unless every batch fails; lane/live filters with `except:pass` leaking
rows; dedup FIFO eviction + resync re-append; clean-view 30MB cap vs mtime
orphaning the remainder; `read_files` import/NameError breaking filtered loads.
**C. Delete/overwrite:** age/market-end prune live in prod with filename-only
remote gate (`except Exception: verified=True`), no row-count/hash check,
staging-mtime proof, `raw_archive.enabled:false`; destructive test scripts
(`rmtree ./data`, prod Kaggle slug delete) with no prompt/live check.
**D. Fabrication:** cursor-recovered books with fake token IDs / `now+lane`
end time / `window or 0`; flush-time wall clock as `ts_received_ns` /
`updated_at` / `disconnect_ts`; `shard[0]` asset, `{ASSET}-5m` series, `""`
token, `"unknown"` outcome, `0→NULL` conversions; estimated fee labeled
exchange-reported.
**E. Liveness lies:** shard-level 30s watchdog leaving single stalled tokens
`live` (~60 rows); `disconnect_ts` at detection not last frame; 150s recycles
with no episode/trade replay; partial REST heal (one side) promoted `live`;
orphan `resync_id`s with no episode; dead sequence-gap code with hardcoded
`received=1`.
**F. Backtest corruption:** reconciled Data-API trades stamped
`ts_received_ns=ts*1e6` (sort key says live), wall-clock fallback, hardcoded
5-min `widx`, `{ASSET}-5m` series, `(tx,price,size)` IDs merging identical
fills, hive write-back rewriting history; resolution backfill preserving old
`recorded_at` (winner visible pre-open); catch-up buckets with current RAM
labeled `live` (<1s grace = lookahead); `bbo_snapped` payload dropped +
dedup collapse on `(token,ts,None×4)`; snapshot-exception fallback writing
NULL quotes as `live` with no event; L2 truncated to 10 in RAM (real depth
reads empty) + BBO-skip on empty side; `markets_latest` polluted by event rows.

## 3. Method
For each suspect: quote code, build a minimal repro with real classes in
`/tmp` (e.g. 9 trades/3 groups + one failing group; 200 flushes × N asset
groups same-ms; 30-level book + top removals; backfill `recorded_at` carry),
report rows-on-disk vs expected + WAL state. No repo/data writes.

## 4. Output (strict, per finding)
```
### [C/H/M][n] [A-F] Title
- Evidence: file:line + 2-4 line quote
- Repro: temp-dir script + lost/hurt rows (counts, %)
- Why backtests hurt: 1-2 sentences
- Fix: numbered, file-scoped (isolate group failures + WAL segment delete;
  {ns}_{pid}_{ctr} names + no-overwrite; quarantine + fail-closed + checksum;
  opt-in wipes; full RAM depth + snapshot truncate; per-book staleness +
  backdated disconnect + recycle episodes; source col + NULL receive +
  backfilled ns + ordinal IDs + no hive rewrite; stamp recorded_at=now +
  first_seen_at; side in dedup + payload cols; per-reason counters + final
  totals; stale + event on snapshot error)
```
End with: scorecard table (A Null / B Loss / C Delete / D Fabrication /
E Streaming / F Backtest: PASS/FAIL + reasons) and top-3 fixes by backtest
damage. Cap at 3 sharp findings per class with evidence; cut claims without
`file:line`. Previously fixed (do not re-report, regress-test instead):
previous-only settlement, token-never-as-condition, NULL `ts_source`,
deterministic `ws-` IDs, C1/C2/M1/H1/H2/H8 writer+book fixes in `a56e79a`.
