# Wallet NULL Fix Plan — Polymarket 5m Crypto Trades

> Dataset: `gghgg1/polymarket-5m-crypto` · Table: `*_trades.parquet` · Columns: `maker_wallet / taker_wallet / wallet`
> Policy: `AGENT.md:9` / `AGENTS.md` — never fabricate wallets/hashes; missing stays `NULL`, never `0` or guessed `0x`.
> Known caveat: `DATA_CARD.md:18` — CLOB market WS carries no wallets; backfill is via Data-API at export + 15-min heal, unattributable stays NULL.

## 1. Current state (measured, not re-derived)

### 1.1 Where wallets come from

* **At ingestion `collector.py:267` `_extract_wallets` / `collector.py:346` `_handle_trade_message`:** CLOB `wss://ws-subscriptions-clob.polymarket.com/ws/market` `last_trade_price` frames have no `proxyWallet`. Harvest tries `proxyWallet/proxy_wallet/maker/taker/owner` `collector.py:298` but in practice hive `data/trades/` is ~100% NULL before export — honest signal, not a bug. `schemas.py:135` `TRADES_SCHEMA` marks wallets nullable for this reason.

* **Export enrichment `storage/export.py:121` `_backfill_trade_wallets` (pass 1, every Kaggle build):** One fetch per `condition_id` to `https://data-api.polymarket.com/trades?market=<cid>&takerOnly=false`. Each fill appears as 2 legs sharing `(tx_hash,price,size)` — `SELL` leg = maker `proxyWallet`, `BUY` leg = taker. `_unambiguous_wallet` `export.py:111` only assigns when all rows at key agree; `export.py:250` `BUY→taker/ SELL→maker` mapping + `export.py:226` tx-level fallback for multi-fill txs with single buyer/single seller. Result kept `NULL` on ambiguity — never guessed. Progress: BTC `36%→5%` wallet NULL after this fix `TEST_RUN_REPORT_2026-09-05_RFIXES.md:47`, `fee`/`outcome` 0% NULL `export.py:292`.

* **Self-heal pass 2 `storage/export.py:452` `second_pass_enrich_trades` (15-min cron via `resolution_backfill`):** Data-API indexes fills late (~15 min, `handoff.md:163`). Re-queries hive trades rows still NULL `export.py:506` and writes back via `storage/export.py:395` `_writeback_enriched_trades` (NULLs only, atomic `tmp+rename`, idempotent, `api-` rows never written back `export.py:412`). Guard: deferred if newest trade `<900s` old `export.py:483` — often logs `deferred: newest trade is 622s old` and does zero work.

* **On-chain pass 3 `storage/export.py:520` `third_pass_onchain_wallets` + `onchain.py:187` `fetch_receipt_fills`:** Decodes `OrderFilled` logs on CTF Exchange V1 `onchain.py:29` `0x4bFb...` / V2 `onchain.py:30` `0xE111...` (`ORDERFILLED_V1_TOPIC`/`V2_TOPIC` `onchain.py:34`). Per-fill join `onchain.py:103` `backfill_wallets_from_fills` on `(tx_hash, token_id)` survives bundle txs; fallback `onchain.py:252` `backfill_wallets_from_chain` on `tx_hash` unanimity. Currently needs `DEFAULT_RPC_URL` `onchain.py:37` public node, capped `max_txs=1500` newest-first `export.py:567`.

### 1.2 Why many NULLs remain today

| Bucket | Cause | Evidence | Fixable? |
|---|---|---|---|
| `wallet` 5% / `maker_wallet` 74% | Data-API exposes maker leg only for minority of fills | `TEST_RUN_REPORT_2026-09-05_RFIXES.md:50` | Requires on-chain |
| Rows with `transaction_hash IS NULL` | CLOB msg lacked `transactionHash/hash` (`collector.py:416`), `_backfill_trade_wallets` skips them `export.py:186` | Count hive `where transaction_hash is null` | Yes — extend key extraction + secondary join by `(price,size,ts)` |
| Late-indexed fills (first export at +30s) | API not yet indexed | `handoff.md:163` 12-18% BTC missed, `second_pass` deferred | Yes — ensure cron runs |
| Multi-fill tx ambiguity | 2+ distinct makers at same `(tx,price,size)` → `NULL` kept `export.py:111` | `tests/test_r_fixes.py:300` ambiguous stays NULL | Honest — keep NULL |
| API itself has no wallet for fill | `export.py:163` `proxyWallet=""` → `wallet` stays NULL | `tests/test_r_fixes.py:201` | Honest — keep NULL |
| On-chain not executed | RPC unavailable / `rpc_failed` / `max_txs` exhausts old windows | `export.py:572` `rpc_failed` path | Yes — infra + batching |
| `token_id` empty → chain per-token join misses | `onchain.py:134` requires `tok`, falls to tx-level map which may be ambiguous | `tests/test_onchain.py:123` bundle case | Partial — use tx fallback + V1 asset_ids |

Expected vs fixable: tach `DATA_CARD.md:18` expected NULL (unattributable, maker minority) is distinct from fixable NULL (missing tx hash, never-healed late fill, never-ran on-chain). Conflating them hides the lever.

## 2. Diagnose before fixing (P0, 1 day, no code)

1. **Run the null audit exactly as `NEXT_AI_PROMPT_KAGGLE_NULL_AUDIT.md:11` prescribes:** `kaggle datasets download gghgg1/polymarket-5m-crypto --unzip -p kaggle_audit_<date>` → 39 files (7×5 per-asset + 4 globals). For each `*_trades.parquet` load with `pyarrow`/`pandas`, compute `rows, null%, zero%, distinct` per `maker_wallet/taker_wallet/wallet/transaction_hash/token_id`. Break down per asset (BTC vs DOGE skew is known).
2. **Hive vs staging gap:** _Plain_ read via `storage/export.py:597` `_read_dataset_per_asset_plain` (no enrichment side-effect) vs staging enriched table — quantify heal delta per asset. Inspect `data/collector_events/parquet` for `second_pass_enrich_trades` `deferred` / `files_rewritten` logs and `onchain pass done: {filled_maker,filled_taker,txs_queried,rpc_failed}` `export.py:593`.
3. **Count unfetchable rows:** `transaction_hash IS NULL`, price rounding mismatch (`round(float(price),6)` `export.py:241` vs API), `api-` reconciled rows share (`export.py:384` `inserted`). This yields the fixable ceiling before touching RPC.

Pass gate: per-asset `wallet` null, `maker_wallet` null, `tx_hash` null, `distinct wallets` recorded once.

## 3. Fix sequence (P1→P3, each independently shippable, tests green before/after)

### P1 — Make the existing 2 passes actually heal (no new data source, ≤1 sprint)

* **Ensure cron wiring:** `ecosystem.config.js` `polymarket-resolution-backfill` every 15 min must call `resolution_backfill.main` → `second_pass_enrich_trades` + `third_pass_onchain_wallets` with `assets=[BTC,ETH,SOL,HYPE,BNB,XRP,DOGE]`. Verify on VPS `pm2 logs` shows `second-pass enrichment: {rows_needed, files_rewritten}` not always `deferred`.
* **Freshness guard tuning `export.py:483`:** Defer threshold `900s` is correct for API indexing, but pass 1 at export already ran at `+30s`. Rename second pass to run only when `age ≥900s` is fine — just ensure it actually runs on the *next* cron tick (not blocked by `pm2` 1G cap restarting). Add alert when `rows_needed>0 && files_rewritten==0 && rpc_failed==false` for 2 consecutive cycles → means Data-API still indexing, not a bug.
* **Write-back idempotence:** Already atomic per part `export.py:443` `pq.write_table(tmp) → _os_replace_safe`; never overwrite non-NULL `export.py:438` / `onchain.py:268`. No change needed — confirm via `tests/test_b_fixes.py:35` + `tests/test_onchain.py:66` green.
* **Tx-hash harvesting at collection:** CLOB message key is `transactionHash` *or* `hash` `collector.py:416` + `msg.get("hash")`. Probe live WS for 5 min: confirm which trade frames carry `hash` vs `transactionHash` vs none; extend `_extract_wallets` key list if new alias appears. This alone recovers the `tx_hash IS NULL` bucket so pass 1 can join.
* **Verification:** `pytest tests/test_r_fixes.py:142` (both-legs attribution) + `tests/test_b_fixes.py:59` hive write-back; staging `wallet null` per asset should drop from `5%` toward `2-3%` without any RPC, `maker_wallet` stays `~74%` (expected).

### P2 — Data-API join robustness (no RPC, pure mapping)

* **Adaptive pagination already `export.py:139` `_fetch` (60 pages, early break on `oldest_needed_ms` `export.py:166`).** Verify 429/500 retry with backoff still present; raise `max_pages` only if any market exceeds 30k fills/window (BTC peak measured ~4k, headroom ok).
* **Price/size exactness:** Both sides use `round(float(x),6)` `export.py:209`/`export.py:241`/`export.py:314`. Confirm API returns same rounding; if mismatches observed in audit, add epsilon bucket (e.g. `abs(a-b)<1e-6` within same `tx_hash`) but keep unanimity gate — no guessing.
* **Side mapping strictness:** `collector.py:405` `side` uppercased; `export.py:245` `BUY→takerPool / SELL→makerPool`. If CLOB `side` missing, row currently falls to `export.py:271` generic `either` pool — acceptable. Keep `wallet = taker || maker` priority `export.py:269`.
* **Outcome/fee backfill:** Already 0% unknown/NULL after fix `export.py:284`/`export.py:349` — monitor stays 0%.

### P3 — On-chain maker fill (closes ~50-60% of remaining maker NULLs)

*Goal:* lift `maker_wallet` from `74%` NULL toward `40-50%` without ever assigning an ambiguous wallet.

* **Contract + topic ground truth `onchain.py:1` / `onchain.py:10`:** V1 `CTF_EXCHANGE_V1` `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`, V2 `0xE111180000d2663C0091e4f400237545B87B996B`, topics `onchain.py:34` verified 2026-09-08 live 174 logs. Maker/taker are `topics[2]/[3]` `onchain.py:84` — identical for both versions.
* **Fetch strategy shift:** Today `onchain.py:187` does one `eth_getTransactionReceipt` per `tx_hash` (simple, no Bloom). This is correct for ≤1500 txs but hits public-node rate limits. Complement with single `eth_getLogs` batch filtered by `address=[V1,V2]` + `topics=[[V1_TOPIC,V2_TOPIC]]` + `transactionHash ∈ set` when provider supports `or` via multiple calls; otherwise retain per-receipt with retry + `timeout=20` `onchain.py:188`.
* **Key provenance:** Provide `.env` `ALCHEMY_POLYGON_URL` or `POLYGON_RPC_URL` override `export.py:533` `rpc_url or DEFAULT_RPC_URL`; public node `onchain.py:37` is fallback, not primary. `DEFAULT_RPC_URL` free tier must not be the only path — document in `RUNBOOK.md`.
* **Join order `export.py:582`:** `backfill_wallets_from_fills` `onchain.py:103` per `(tx_hash, token_id)` first — survives bundle txs where same tx has distinct makers per token `tests/test_onchain.py:111`; then `backfill_wallets_from_chain` `onchain.py:252` tx-level unanimity fallback for rows missing `token_id`. Needs `token_id` decimal string match `onchain.py:90` for V2 or `maker_asset_id/taker_asset_id` string for V1 `onchain.py:93` — normalize `token_id` to string before compare.
* **Caps & scheduling:** `max_txs=1500` newest-first `export.py:567` via `need_tx_order` sorted by `ts_received_ns` — ensures recent windows heal first; old windows ride next cron. Do not raise cap without measuring free-tier quota; run p50 latency: receipt fetch ~80ms ×1500 = 2 min — fits 15-min cron.
* **Honesty keeps:** `onchain.py:57` multi-maker same `(tx,token)` stays NULL; `onchain.py:182` multi-maker same tx stays NULL. Verified `tests/test_onchain.py:47` / `tests/test_onchain.py:134`.

## 4. Not in scope / explicitly rejected

* **Synthetic wallets (`synthetic_mode`, `synth-` report_ids, `fee_is_estimated=false` with invented fee):** Forbidden `AGENT.md:9`. `fee_is_estimated` `schemas.py:153` only `true` when derived from market's exchange-reported rate `export.py:327`, else `NULL`.
* **Interpolating missing wallets from clustering/heuristics:** Forbidden — unattributable stays NULL.
* **Changing `TRADES_SCHEMA` to non-nullable:** Would force fabrication — keep nullable `schemas.py:158`.
* **Replacing null-vs-zero policy `schemas.py:69` `book.py:481`:** Empty book side stays NULL, never 0 — same principle for wallets.

## 5. Acceptance criteria

* `kaggle datasets download` audit: `wallet` NULL <5% overall, every asset 2-8% (BTC worst case); `taker_wallet` 90-99% filled; `maker_wallet` 40-60% after P3 (vs 74% before); `transaction_hash` NULL ≤5% (collection fix).
* `pytest` green including `tests/test_r_fixes.py`, `tests/test_b_fixes.py`, `tests/test_onchain.py` — honesty cases `unknown wallet stays NULL` still assert.
* `resolution_backfill` logs show `second_pass` writes hive files + `onchain pass done: {filled_maker>0 || deferred honest}` each cycle; `rpc_failed==false` on healthy runs.
* `markets_summary.parquet` `unique_traders` stays correct (counts distinct non-null `wallet` only `export.py:753`).
* `DATA_CARD.md:18` updated with new post-P3 maker coverage number; remaining NULLs documented as honest.

## 6. Operational checklist

* [ ] P0 audit artifact `kaggle_null_audit_<date>.md` committed with per-asset wallet null% table.
* [ ] Live 5-min probe of CLOB `last_trade_price` frame `hash`/`transactionHash`/`proxyWallet` fields logged (no repro needed if spec unchanged — `handoff.md:158` AsyncAPI confirms `hash` required on `price_change` but not on `last_trade_price`).
* [ ] `.env` `ALCHEMY_POLYGON_URL` present on VPS before P3; `DEFAULT_RPC_URL` fallback logged as `WARN` when used.
* [ ] `pm2 save` after `ecosystem.config.js` cron change; `data/` never wiped before enrichment has healed (cooldown 15 min).
* [ ] No push from dev boxes without operator ACK — `handoff.md:18` standing rule.
