# §18 Verification Gate — Status

> **Gate, not checklist** — §1A must not be implemented until these are answered **against live payload captures**, not docs or assumption.

Run `polymarket-verify-gate --live --config config/collector.yaml` against production.

| # | Question (§18) | Status | Details |
|---|---|---|---|
| 1 | Does CLOB market-channel WS feed expose monotonic `sequence_number` per token on `price_change`/`book`/`last_trade_price`? | ⏳ open | Capture WS messages for BTC+ETH markets for ≥10 min; inspect for `sequence_number`/`seq`/`sequence`. If absent → sequence gap detection unavailable; §1A full-book diff becomes PRIMARY (not fallback). Dedup falls back to `(token_id, ts_received_ns, event_type, new_best_bid/ask)` per §4/§5. |
| 2 | Does CLOB REST expose full L2 book per token (not just BBO)? | ⏳ open | Probe `GET https://clob.polymarket.com/book?token_id=<valid>` for `bids`/`asks` arrays with >2 levels. If only BBO/spread → §1A resync needs redesign before coding (cannot wholesale replace book). |
| 3 | Is there a settlement report/tx endpoint for `settlement_report_id`/`settlement_tx_hash` (§6A)? | ⏳ open | Fetch a recently resolved market; check for `settlement_report_id`/`tx_hash`. If absent → all resolutions will be `settlement_source=inferred_nearest` — document explicitly and exclude from strict-accuracy backtests unless opted in. |
| 4 | What rate limits apply to REST endpoints for rollover discovery (§1) and resync (§1A)? | ⏳ open | Inspect `Retry-After` / `X-RateLimit-*` headers on 429; size backoff params (`discovery_backoff_max`, `resync_rest_backoff_max`, `reconnect_backoff_max`) against measured limits, not guesswork. |

**To mark resolved:** attach a saved payload capture (`data/_captures/<date>/`) and the `polymarket-verify-gate --live --json-out` output. Do not mark via documentation alone.
