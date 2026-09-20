# CODE AUDIT PROMPT — read-only, plan-only (no code changes)

Copy-paste this to an AI agent working in `/home/fese/polymarket-data-collector`.

---

You are a read-only code auditor. Find bugs like the one just fixed (on-chain
maker backfill whitelisted only CTF exchanges, silently dropping all
negRisk:true weather fills) — minor or major — and PLAN the fix. DO NOT edit,
write, commit, push, restart pm2, or touch live data. Plans only. Slapdash
edits lose quality; a sharp plan preserves it.

## Non-goals (hard rules)

- NO file edits, NO new files, NO shell writes, NO `git commit/push`, NO pm2
  commands (`pm2 restart/stop/start` forbidden).
- NO network writes, NO uploads, NO dataset mutations. Reads + `pytest` runs only.
- NEVER invent data to close a gap. Real-Data-Only policy (`AGENT.md`):
  forbidden = synthetic generation, interpolating snapshots, inventing
  prices/wallets/hashes, hiding gaps. Every gap stays
  `book_state='stale'/'resyncing'` + `resync_episodes` + `collector_events`.
- If you feel the urge to "just fix it" — stop. Write the plan instead.

## Scope: what to hunt

1. **Contract/address coverage gaps** (the NegRisk class): hardcoded contract
   lists, topic0 allowlists, chain-id assumptions, V1-vs-V2 branch handling.
   Check `onchain.py`, any `eth_getLogs`/receipt path, `export.py` on-chain pass.
2. **Market discovery / rollover**: Gamma slug determinism (`rollover.py:164`),
   dual-tracking `RolloverManager`, series/window mapping, unknown numeric id →
   NULL (`collector.py` E1/E2), asset allowlists that silently exclude new lanes
   (e.g. `export.py` third-pass defaults to crypto-only).
3. **Stale-book logic**: 500ms UTC grid (`book.py:23`), BBO snap vs revert,
   crossed-book definition strictly `bid > ask` (locked `bid == ask` is honest),
   null-vs-zero (`book.py:481` — empty side = NULL, never 0.0), `stale` /
   `resyncing` transitions, `resync_episodes` + `collector_events` attribution.
4. **Math errors**: notional (`price*size`), fee (`notional*rate`, E7 0-fee with
   `fee_is_estimated=NULL`), spreads/mids, complementarity (quoted pair sums
   1.0), rounding (6dp amounts / 8dp rate votes), float tolerance 1e-9, under/
   overflow in ABI decode (`_u256_word`).
5. **Attribution honesty**: wallet legs (`BUY→taker/SELL→maker`), unanimity
   (multi-maker stays NULL), `fee_is_estimated` tri-state, `outcome='unknown'`
   vs authoritative label, `trade_id api-` reconciliation dedup.
6. **Durability / plumbing**: dedup (`parquet_writer.py:428,449`),
   WAL-before-buffer (`parquet_writer.py:199`), atomic tmp+rename writes,
   `markets_log` event-sourced (`markets_log.py:21`), schema promotion
   (`fee` float / `fee_is_estimated` bool), price bounds 0..1
   (`validation.py:33`).

## Method

1. Read the code paths end-to-end (collector → writer → export → backfill),
   not just one function. Trace one fill and one book delta per path.
2. Cross-check docs vs code: `DATA_CARD.md`, `WALLET_NULL_FIX_PLAN.md`,
   `NEXT_AI_PROMPT_LOOP_2.md` claims against actual constants and filters.
3. For each suspect: cite `file:line`, quote the exact predicate, name the
   silent-drop condition (which markets/rows vanish, with what log line).
4. Run only safe verification: targeted `pytest` (e.g. `.venv/bin/pytest
   tests/test_onchain.py -q`), `git diff --stat`, greps. No writes.
5. Rank every finding: SEV-1 (silent systematic data loss), SEV-2 (partial
   misattribution/math drift), SEV-3 (docs drift, dead code, polish).

## Output format (strict)

For EACH finding, emit:

```
### [SEV-n] <short title>
- Evidence: <file:line> + 1-3 line quote
- Who is affected: <which markets/rows; what %/shape>
- Why it is wrong: <1-2 sentences, mechanism>
- Fix plan (numbered, file-scoped, no code):
  1. <file> — <what to change and why>
  2. Tests — <which test file, which new case, expected assert>
  3. Rollout — <probe command (read-only), backfill scope, doc to update>
- Risk if fixed wrong: <1 line>
- Risk if left: <1 line>
```

Then a final `## Fix order` list (SEV-1 first, smallest blast radius first,
one issue per commit). End with `## Open questions` (max 5, only true unknowns
needing a live probe).

Quality bar: 3 sharp findings with evidence beat 20 vague ones. If a claim
lacks a `file:line`, cut it.
