# Polymarket CLOB/RTDS WebSocket Resilience — Research Findings

*Compiled 2026-09-05. Sources are linked inline; "docs" = docs.polymarket.com (AI-generated/maintained, may drift).*

---

## 0. The one finding that changes the diagnosis

Your observed pattern — **`1011` closes with reason `"keepalive ping timeout"` coming from your own client, worse when you tighten `ping_interval`/`ping_timeout`** — is the signature of the Python `websockets` library's *protocol-level* ping/pong keepalive timing out, not a server-imposed idle limit.

- The official docs are explicit that the market channel (and RTDS) use an **application-level** heartbeat: send the **text frame `"PING"`** every 10s (5s for RTDS); server replies with text `"PONG"`. This is *separate* from the RFC6455 control-frame ping/pong that `websockets` sends automatically. [Docs](https://docs.polymarket.com/market-data/websocket/overview), [agent-skills](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)
- `websockets`' own FAQ says verbatim: *"If you are referring to Ping and Pong frames defined in the WebSocket protocol, don't bother, because websockets handles them for you. If you are connecting to a server that defines its own heartbeat at the application level, then you need to build that logic into your application."* And: *`ConnectionClosedError: sent 1011 ... keepalive ping timeout` means the connection suffered excessive latency and was closed **by your own client** after its internal ping went unanswered within `ping_timeout` (default 20s).* [websockets FAQ](https://websockets.readthedocs.io/en/stable/faq/common.html)
- Polymarket's server evidently does not answer RFC6455 control-frame pings quickly/consistently (it's built around the app-level text heartbeat instead). That explains both halves of what you saw: (a) the 3–5 minute closes — occasional protocol-ping round-trips exceed even the 20s default — and (b) tightening the window making it *worse* — you're racing a server that wasn't built to answer that frame type fast.
- A live GitHub issue against `py-clob-client` shows a user running with `ping_interval=None` (protocol pings fully disabled) and a manual app-level `"PING"` every 10s, confirming this is the working pattern others have converged on. [py-clob-client#292](https://github.com/Polymarket/py-clob-client/issues/292)

**Action:** connect with `websockets.connect(url, ping_interval=None, ping_timeout=None)` and implement the documented text-frame heartbeat yourself, with your own liveness watchdog on top (see §Architecture). This is very likely to make your defaults-are-safest observation obsolete — you may be able to run *tighter* effective liveness detection than 20s once you're not fighting the library's own control-frame timer.

---

## 1. Official docs: heartbeat, idle timeouts, caps, rate limits

| Question | Finding | Source |
|---|---|---|
| Heartbeat | Market channel: client sends text `"PING"` every 10s → server replies `"PONG"`. RTDS: every 5s. Both are **application-level**, not RFC6455 control frames. Formally specified in Polymarket's AsyncAPI spec ("Polymarket WebSocket API 1.0.0"): a `ping` message with payload const `PING`, summary *"Client heartbeat — send every 10 seconds"*, plus a `pong` response message. | [Real-Time Data docs](https://docs.polymarket.com/market-data/websocket/overview), [AsyncAPI spec](https://docs.polymarket.com/asyncapi.json) |
| Heartbeat ordering constraint | The server **rejects a `"PING"` sent before the first subscription** with close code **1008 "invalid subscription payload"** — subscribe first, then start the ping timer. Demonstrated with a raw public-endpoint probe. | [nautilus_trader#4864](https://github.com/nautechsystems/nautilus_trader/issues/4864) |
| Idle timeout (market channel) | **Not documented.** The only published number is for the *Perpetuals* WS (`ws.perpetuals.polymarket.com`, a different product): 60s of inactivity → close. No equivalent number is published for `ws-subscriptions-clob.polymarket.com/ws/market`. | [Perps ping doc](https://docs.polymarket.com/api-reference/wss/perps-ping.md) |
| Max subscriptions/connection | **Not published.** NautilusTrader's adapter treats 200 as a *self-chosen* conservative ceiling, explicitly stating Polymarket publishes no cap and that "high per-connection subscription counts have been observed to silently stall a connection." Note: the historical 100-token cap was officially **removed on 2025-05-28** ("unlimited token IDs now allowed"), the same changelog entry adding the optional `initial_dump` subscribe field (default `true`) controlling whether the initial book snapshot is sent. | [NautilusTrader Polymarket integration](https://nautilustrader.io/docs/latest/integrations/polymarket/), [Predictions Changelog](https://docs.polymarket.com/changelog/predictions.md), [AsyncAPI `SubscriptionRequest` schema](https://docs.polymarket.com/asyncapi.json) |
| Max connections/IP | **Not published** for the market/RTDS WS. Polymarket applies Cloudflare IP-level throttling generally (sliding-window, can produce 429/temporary blocks under sustained overshoot), separate from per-signer CLOB order/cancel token buckets. | [Rate limits doc](https://docs.polymarket.com/api-reference/perps/rate-limits), [NautilusTrader rate limiting section](https://nautilustrader.io/docs/latest/integrations/polymarket/#rate-limiting) |
| Rate limit on (re)subscribe | Docs state only that "WebSocket connections have separate limits for connection count, active subscriptions, and inbound messages" without publishing numbers for the market channel specifically. | [Rate limits doc](https://docs.polymarket.com/api-reference/perps/rate-limits) |
| Dynamic subscribe/unsubscribe | **This now exists and is documented** — see §4, it directly answers your "silently ignored" observation. | [wss-overview](https://docs.polymarket.com/developers/CLOB/websocket/wss-overview), [agent-skills](https://github.com/Polymarket/agent-skills/blob/main/websocket.md) |

Net: there is no official SLA to design against. Every number in the architecture below (§Architecture) is either reverse-engineered from your measurements, from NautilusTrader's production defaults, or from a live probe you should run yourself (§Risks).

---

## 2. How other clients architect this

**No public client — official or community — runs a dual/hot-standby pool with automatic failover for the market channel.** The pattern converges instead on: single connection per logical shard, generous backoff reconnect, and (in the best implementations) a data-inactivity watchdog independent of the transport-level ping.

- **NautilusTrader** (Rust, production-grade, the most sophisticated public implementation found): shards subscriptions across a *pool* of market connections capped at `ws_max_subscriptions` (default 200), grows the pool lazily, closes a connection once it owns zero assets, and **replays only that connection's own assets on reconnect** — i.e., reconnect is per-shard, not a single all-or-nothing socket. No standby connection; reconnect is reactive. [Docs](https://nautilustrader.io/docs/latest/integrations/polymarket/#data)
- **`@nevuamarkets/poly-websockets`** (TypeScript, MIT, ~76★): one connection per `WSSubscriptionManager` instance, dynamic add/remove subscriptions without reconnecting, automatic reconnection on drop. Explicitly documents running *multiple independent manager instances* for parallel connections, but each is an isolated single-socket client with no shared-state failover between them. [README](https://github.com/nevuamarkets/poly-websockets)
- **Go SDK** (`GoPolymarket/polymarket-go-sdk`): exposes `ReconnectDelay`, `ReconnectMaxDelay`, `ReconnectMultiplier`, `ReconnectMax`, `HeartbeatInterval`, `HeartbeatTimeout`, `ReadTimeout` (default `DefaultReadTimeout = 60s`) as first-class config — i.e., official-adjacent Go tooling treats reconnect-with-backoff as the whole HA story, no redundancy layer. [pkg.go.dev](https://pkg.go.dev/github.com/GoPolymarket/polymarket-go-sdk/pkg/clob/ws)
- **py-clob-client issue #292 workaround**: "a data inactivity watchdog that force-reconnects after 120 seconds of silence, combined with REST `/midpoint` spot-checks" — this is the most battle-tested community pattern for detecting the "connected but silently stalled" failure mode (see §4). [Issue](https://github.com/Polymarket/py-clob-client/issues/292)
- Every market-maker repo surveyed (`poly-maker`, `polymarket-market-maker`, `Polymarket-Automated-Trading-Bot`, `polymarket-trade-engine`) advertises only "automatic reconnection with exponential backoff" — none document a standby socket.

**Conclusion for your dual-connection design**: you'd be building past the current state of the art, not implementing a known pattern. That's not a reason not to do it (a 5-minute BTC market genuinely can't absorb a multi-second gap), but budget extra testing — there's no reference implementation to diff your behavior against.

---

## 3. SSE / WebSocket v2 / roadmap

**Nothing found.** All three interface tabs in current docs (TypeScript, Python, raw API) for the market and RTDS streams point at the same underlying WebSocket endpoints (`wss://ws-subscriptions-clob.polymarket.com/ws/market`, `wss://ws-live-data.polymarket.com`); the TS/Python "clients" are thin subscribe()-style wrappers over the same sockets, not a new transport. No SSE endpoint, no `/ws/v2`, no changelog entry or roadmap mention. [Docs changelog](https://docs.polymarket.com/changelog) shows a July 2026 CLOB matching-latency change and a Neg Risk Adapter migration, nothing about transport. Treat this as "no roadmap exists" rather than "we searched and didn't find the roadmap" — there's no forward-looking statement from Polymarket on this at all.

---

## 4. Known community issues: silent adds, drops, gaps

- **Your "second subscribe silently ignored" finding is now stale** — current docs describe an explicit `operation` field that Polymarket added for exactly this:
  ```json
  {"assets_ids": ["<new_token_id>"], "operation": "subscribe", "custom_feature_enabled": true}
  {"assets_ids": ["<old_token_id>"], "operation": "unsubscribe"}
  ```
  This modifies the token set on the **existing connection**, no reconnect needed. [wss-overview](https://docs.polymarket.com/developers/CLOB/websocket/wss-overview), [agent-skills](https://github.com/Polymarket/agent-skills/blob/main/websocket.md). The community TS library `@nevuamarkets/poly-websockets` implements exactly this (`addSubscriptions`/`removeSubscriptions` with no reconnect). Recommend re-testing against this exact payload shape before concluding a fresh connection is required — your original test likely predated this being documented, or used a plain repeated `{"type":"market","assets_ids":[...]}` frame (which docs suggest is treated as the *initial* subscribe shape, not an incremental one) rather than the `operation` field.
- **Connected-but-silent freeze** (distinct from a clean disconnect): `py-clob-client#292` — 6 connections × 250 tokens each, app-level PING/PONG kept working, REST prices stayed correct, but `book`/`price_change` stopped arriving for hours after a batch of connections dropped (1006) and reconnected. Reporter explicitly flags subscription count (250/connection) as an untested suspect. Their workaround: **inactivity watchdog forcing reconnect after 120s of silence**, cross-checked against REST `/midpoint`. [Issue #292](https://github.com/Polymarket/py-clob-client/issues/292)
- **Same failure mode on RTDS**: `real-time-data-client#26` — stream "stops after ~20 minutes" with the socket reporting healthy/open, no close/error event fired. Unresolved as of writing; no official response found in the issue. [Issue #26](https://github.com/Polymarket/real-time-data-client/issues/26)
- **RTDS filter bugs**: `real-time-data-client#34` — `market_slug`/`event_slug` filters on the `activity`/`trades` topic silently return zero messages (no error) even though docs describe them as supported; only the empty/no-filter form works. A community blog post (BlueWhale-Quant-Lab) independently found the *documented* comma-separated `crypto_prices` filter string (`"btcusdt,ethusdt"`) is rejected by the live server with a regex error, while a JSON-array value is silently accepted but returns nothing — i.e., **don't trust RTDS filter behavior against docs; test each filter shape live and fall back to unfiltered + client-side filtering** (which is what your current `{"topic":...,"type":"*"}` unfiltered pattern already does — keep doing that). [Issue #34](https://github.com/Polymarket/real-time-data-client/issues/34), [blog post](https://dev.to/bluewhale-quant-lab/polymarkets-price-websocket-can-stall-while-connected-and-the-docs-wont-warn-you-gc3)
- **Root cause of the RTDS freeze, from raw-frame capture** (2026-08-19, `real-time-data-client#26`): the freezes are **server-side deletion of the connection's row in the backend subscription registry** while the socket stays open. The tell: re-sending the subscribe frame on a frozen connection returns a Postgres FK-violation error (`#23503` — the parent connection row is gone), and re-subscribing *never* succeeded (0/15 attempts). Conclusion: **"Recovery must replace the socket, not re-subscribe on it."** The official client (v1.4.2) only reconnects from `onerror`/`onclose` and has no data-plane liveness detection — [PR #46](https://github.com/Polymarket/real-time-data-client/pull/46) improves backoff/close handling but still cannot detect this state by itself. [Comment by rasoolsomji](https://github.com/Polymarket/real-time-data-client/issues/26), follow-up by osr21 (2026-08-20).
- **Additional community confirmations of the CLOB market-channel freeze**: independent reports in the `py-clob-client#292` thread across Python and TS clients, multiple IPs, with a Polymarket team member ("fednerpolymarket") responding "I'll investigate" (2026-04-23); still open/locked as of 2026-05. Also reported there: **missing initial `book` dump** ("receiving price_changes but never the initial dump").
- **Recommended fix pattern across all of the above, convergently**: never trust "socket is open" or even "app-level pong is answering" as proof of liveness. Track a **last-valid-data-message timestamp** per connection/subscription and force-reconnect on staleness (blog post uses 45s for RTDS Chainlink cadence; py-clob-client issue uses 120s for the CLOB market channel at higher subscription counts; PolyWatch's operator uses 30s for platform-wide feeds and argues narrow single-market feeds can't distinguish "frozen" from "quiet" at any threshold — for those, prefer scheduled proactive reconnect). Given your 3–5 min baseline disconnect cadence, a 30–45s data-staleness watchdog is a safe independent layer under whatever the transport does. [PolyWatch](https://github.com/osr21/Polywatch), [issue #26 discussion](https://github.com/Polymarket/real-time-data-client/issues/26)

---

## 5. RTDS Chainlink roundId / reportId / TWAP metadata

**No.** Both RTDS Chainlink topics deliberately strip Chainlink's report-level metadata:

- `crypto_prices_chainlink` (spot) payload is just `{symbol, timestamp, value}` — no round ID.
- `crypto_prices_twap_thirty` / `crypto_prices_twap_sixty` (the TWAP topics) payload is `{symbol, value, full_accuracy_value, timestamp, window_s}` — `full_accuracy_value` is the exact signed-E18 fixed-point TWAP value, but there is still no `roundId`, `reportId`, or feed ID. Docs explicitly say **"Reports do not include symbol or window labels [in the raw Chainlink layer]. Maintain that mapping yourself"** — implying RTDS is deliberately a stripped-down relay. [Chainlink TWAP docs](https://docs.polymarket.com/market-data/chainlink-twap.md) (verified URL: the `.md` variant resolves; the bare page may 404/redirect)
- Official RTDS scope note (2026-01-16 changelog): "RTDS docs updated to reflect RTDS supports **comments and crypto prices only**" — legacy CLOB topics and `clob_auth` were removed; RTDS is not a fallback for book data. Also relevant: the 2026-08-14 changelog entry — 5-minute crypto markets now resolve via a **60-second Chainlink TWAP**. [Predictions Changelog](https://docs.polymarket.com/changelog/predictions.md)
- To get `feedID`, `observationsTimestamp`, `validFromTimestamp`, `expiresAt`, or the original signed report (round/report identity), you must go **directly to Chainlink Data Streams** (`wss://ws.dataengine.chain.link` / `api.dataengine.chain.link`) with your own Chainlink API credentials — a completely separate integration from Polymarket's RTDS, requiring the `@chainlink/data-streams-sdk` and DON signature verification if you need trust-sensitive use. Polymarket even links a **sponsored API key request form** for this (`pm-ds-request.streams.chain.link`) specifically for 15-minute crypto market users, suggesting this dual-integration is the intended production pattern for anyone who needs round-level provenance. [Same doc, "Use Chainlink Data Streams" section]

**Implication for you**: if your bone-reaper / sniper-style bots need to correlate against the *exact* on-chain Chainlink round that resolves a market, RTDS alone cannot give you that; you already have a working direct-RPC AggregatorV3 path in `polyalpha`/`onchain_chainlink.py` for BTC/ETH — that remains the only source of round-level truth, RTDS is display-only.

---

## 6. Ordering / delivery guarantees; gap reconciliation

- **No documented delivery guarantee** (no at-least-once, no exactly-once, no sequence numbers) exists anywhere in official docs for the market or RTDS channels. This matches your own observation.
- What *does* exist as a reconciliation primitive: the `book` message carries an optional **`hash`** field (a hash of the book state) alongside `timestamp` — formally specified in the AsyncAPI spec, where `BookEvent.hash` is a **required** field ("Hash of the orderbook content") and `PriceChangeMessage.hash` is required too ("Hash of the order that caused this change"). NautilusTrader's adapter reproduces this hash from the wire values/level order and **rejects a snapshot whose hash doesn't reproduce**, before it's allowed to update local state — this is the closest thing to an integrity check Polymarket provides, and it's opt-in (you have to compute it yourself; the server doesn't reject anything for you). [AsyncAPI spec](https://docs.polymarket.com/asyncapi.json), [NautilusTrader "Book snapshot validation"](https://nautilustrader.io/docs/latest/integrations/polymarket/#book-snapshot-validation)
- **`price_change` deltas are documented as unordered-across-assets-but-batched**: "A single `price_change` payload can contain interleaved updates for several assets" (NautilusTrader groups by instrument before applying). Within one asset's stream on one connection there's no documented reordering, but there's also no proof against it — treat within-connection ordering as "probably fine, not guaranteed."
- **Tick-size changes as an epoch boundary**: when a `tick_size_change` event fires, old book levels can be invalid on the new price grid. NautilusTrader's pattern — drop the local book, gate/drop incremental deltas until a fresh snapshot arrives, then reseed — is the correct generic response to *any* suspected gap, not just tick-size changes. This generalizes directly to your reconnect-and-REST-snapshot recovery, which is already the right shape.
- **How others actually reconcile gaps in practice**: nobody found relies on the WS alone. The universal pattern is REST `/book` (full snapshot) or `/midpoint` as ground truth, either (a) proactively on a timer, (b) reactively after a reconnect, or (c) reactively after a liveness-watchdog fires. Your own approach (REST snapshot heals books without WS) matches every production pattern surveyed — you already have the right recovery primitive; the only gap is **detecting that you need it fast enough**, which is a liveness-watchdog problem, not a REST-endpoint problem.

---

## Recommended Architecture

### Topology: 1 primary + 1 warm standby, per asset-connection (not truly "hot" in the zero-gap sense — see caveat)

```
                    ┌─────────────────────┐
                    │   Connection Pool     │
                    │   Manager (asyncio)   │
                    └──────────┬────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
 ┌──────▼──────┐        ┌──────▼──────┐        ┌──────▼──────┐
 │  Primary A   │        │  Standby A'  │        │ (repeat per  │
 │ 14 tokens    │◄──────►│ same 14      │        │  asset conn, │
 │ ACTIVE       │  swap   │ tokens       │        │  x7 today)   │
 │              │  role   │ WARM         │        │              │
 └──────────────┘        └──────────────┘        └──────────────┘
        │                      │
        └──────────┬───────────┘
                    ▼
         Downstream consumers read
         only from whichever socket
         is currently "ACTIVE"
```

Given your current shape (7 asset-connections × 14 tokens), this means **14 sockets total** (7 primary + 7 standby) rather than 7. That's well inside "one connection can hold many asset_ids" territory per-socket, and nowhere near the ~200-connection subscription ceiling NautilusTrader self-imposes — the constraining resource is connection *count* per IP (undocumented, but Cloudflare-throttled), not subscription count per connection. Recommend load-testing 14 simultaneous connections from one IP before committing to this topology at scale (see Risks §3).

**Why not a single pool with N+1 spare capacity instead of literal pairs?** Because of your #3 finding: a second `subscribe` on an *established* connection may not behave as a hot-add if you're not using the new `operation` field correctly (§4), and even with `operation: "subscribe"` working, a freshly-added token on a live connection has an unknown warm-up latency before its first `book` snapshot arrives (the py-clob-client freeze issue shows subscription acceptance does not imply data delivery). A standby that is **already fully subscribed and has already received its `book` snapshot before failover** is the only way to guarantee zero data loss at swap time; adding tokens to a spare connection *at* failover time reintroduces exactly the gap you're trying to eliminate.

### Role-swap trigger and procedure

1. **Detection** (independent of transport-level health): track `last_data_ts` per connection, updated on *any* valid `book`/`price_change`/`last_trade_price`/`tick_size_change` message (not on PONG — PONG proves the app-level heartbeat loop is alive, not that market data is flowing, per the py-clob-client freeze case where PING/PONG "worked perfectly" while data was dead).
   - Staleness threshold: start at **30s** for primary (tokens trade roughly continuously near a 5-min window's midpoint; 30s of total silence across 14 active BTC/ETH up/down tokens should never happen legitimately) — tune down from there once you've watched real quiet-market behavior; the community RTDS pattern used 45s for slower Chainlink cadence, yours is a busier book so can likely go tighter.
   - Also treat a `1006`/`1011`/any close frame as immediate (not staleness-timed) failover trigger.
2. **Swap**: on trigger, flip the "ACTIVE" flag to the standby atomically (single asyncio variable/event) — downstream consumers should already be idle-subscribed to both connections' output queues and just start reading from the newly-active one. This is why the standby must already be *warm* (subscribed + first snapshot received), not just *connected*.
3. **Recycle the dead primary**: don't try to resurrect it in place. Close it, and open a **new** standby connection to replace the one that just got promoted — resubscribe all 14 tokens, wait for `book` snapshots on all of them (with a bounded timeout — see Risks §1 on subscribe-without-data), *then* mark it warm/eligible as the new standby.
4. **Planned recycle** (independent of failure): given the observed 3–5 min organic disconnect rate, don't wait for failure — proactively rotate the *standby* (not the active) on a timer, e.g. every 3 minutes, swapping it for a freshly-connected one, so you always have a standby that's "young" rather than one that's itself approaching its own silent-freeze risk window. This turns your unavoidable ~3–5 min churn into a non-event because it's always the *idle* leg being recycled.

### Ping/keepalive changes

- **Disable `websockets`' protocol-level ping entirely**: `ping_interval=None` (per §0 finding — this removes the self-inflicted 1011s).
- **Implement the documented app-level heartbeat yourself**: send text frame `"PING"` every 10s on the market channel (5s on RTDS), on both primary and standby sockets independently.
- Treat *app-level* pong absence as a secondary signal (connection genuinely dead at the transport), and *data staleness* (§ above) as the primary failover trigger — these are different failure modes (py-clob-client#292 proves a connection can be transport-alive/heartbeat-alive and still data-dead).
- Keep your existing REST `/book` snapshot-heal path as the reconciliation mechanism after any reconnect/promotion, exactly as you already do — nothing here needs to change.

### Concrete parameters (starting point — tune against your own probe, see Risks)

| Parameter | Recommendation | Basis |
|---|---|---|
| `ping_interval` (websockets protocol) | `None` (disabled) | §0 — this is very likely your actual bug, not a server idle limit |
| App-level `"PING"` cadence | 10s (market), 5s (RTDS) | Docs |
| Data-staleness failover threshold | 30s (market channel, given your busy 14-token books) | Derived; community RTDS analog used 45s for slower feed |
| Standby proactive-recycle interval | 3 min | Below your observed 3–5 min organic disconnect floor, so recycling is always "ahead of" the failure |
| Reconnect backoff (both legs, on unplanned drop) | exponential, 0.5s base, ×2, cap 10s, small jitter | Standard practice across all surveyed clients (Go SDK exposes exactly this shape as config). Polymarket's own retry guidance for matching-engine restarts suggests starting at 1–2s and capping around 30s — [Matching Engine Restarts](https://docs.polymarket.com/trading/matching-engine); during restart windows order endpoints return HTTP 425, and the engine runs post-only for 2 min after each restart |
| Per-connection token count | keep at 14 (well under any observed-safe range; py-clob-client's freeze happened at 250) | §4 |
| Total simultaneous connections from one IP | 14 (7 pairs) — validate via probe before scaling further | Undocumented cap; Cloudflare IP throttling is the real constraint |

---

## Risks / Unknowns Requiring a Live Probe

1. **Does the documented `operation: "subscribe"` field actually deliver data promptly on an established connection, or does it share the "accepted but silent" failure mode from `py-clob-client#292`?**
   Probe: on an already-connected, already-subscribed (14-token) socket, send `{"assets_ids": ["<new_token>"], "operation": "subscribe", "custom_feature_enabled": true}` and measure time-to-first-`book`-event for the new token, across ~20 trials at different times of day. Compare against a fresh connection's time-to-first-`book`. If the incremental path is meaningfully slower or occasionally silent, your standby-must-be-pre-warmed design (above) is validated as necessary rather than optional.

2. **Is your observed 3–5 minute idle-regardless-of-activity disconnect actually independent of the protocol-ping bug in §0, or does fixing §0 make it disappear entirely?**
   Probe: run one connection with `ping_interval=None` + correct app-level text `"PING"`/`"PONG"` handling, nothing else changed, for several hours, and log every close code/reason. If closes drop to near-zero, the entire dual-connection architecture above can likely be simplified to primary+backoff-reconnect with a data-staleness watchdog, no standby needed. If closes persist even with the app-level heartbeat implemented correctly, that's evidence of a genuine server/LB-side idle-connection recycle (undocumented), and the standby-pool design is necessary rather than a hedge.

3. **What is the actual safe ceiling for simultaneous connections from one IP, and does it degrade gracefully (429/close) or silently (freeze, per §4)?**
   Probe: from the same VPS you already run on, open connections in batches (e.g., 4, 8, 14, 20, 30), each independently subscribed and each independently tracked for time-to-first-book and steady-state message rate. Watch specifically for the "accepted subscription, zero data" freeze pattern from `py-clob-client#292`, not just for outright connection refusal — that issue shows the failure mode is silent, so a naive "did connect() succeed" check will not detect it.

4. **Does the book `hash` field validate cleanly across a reconnect/promotion boundary for your data, and is it worth the implementation cost?**
   Probe: capture raw `book` messages including `hash` across a period covering several natural reconnects, and check whether you can reproduce the hash from the wire bids/asks/order as NautilusTrader does. If yes, add it as a cheap sanity check on standby promotion (reject/re-fetch if the first snapshot's self-consistency looks wrong) before trusting a freshly-promoted standby's first book as authoritative.

5. **Confirm whether your originally-observed "silently ignored second subscribe" was the old plain-`assets_ids` re-send (likely dropped as a no-op duplicate initial-subscribe) versus the new `operation` field never having been tried** — this determines whether §4's fix is real or whether the underlying server behavior is unchanged and only the docs are new. Cheapest possible probe, do this first before the others.
---

## Live probe results (2026-09-05, run against production endpoints)

**Probe: does the documented `operation` field actually hot-add tokens? (Risks #5/#1) — YES.**

Method: fresh `websockets` connection to `wss://ws-subscriptions-clob.polymarket.com/ws/market`,
initial subscribe with the old shape `{"assets_ids": ["<btc-up-token>"], "type": "market"}`;
wait for the full `book` frame; then send additional subscribes on the SAME connection.

| Probe | Payload | Result |
|---|---|---|
| Control (old shape, second send) | `{"assets_ids": ["<eth-up-token>"], "type": "market"}` | **IGNORED** — no frames for the new token (only the base token's `price_change` kept flowing). Confirms the handoff's original observation. |
| `operation`-add, same asset (BTC down token) | `{"assets_ids": [...], "operation": "subscribe", "type": "market", "custom_feature_enabled": true}` | **WORKS** — full `book` frame for the new token arrived immediately on the established connection. |
| `operation`-add, cross-asset (ETH up token on a BTC-subscribed socket) | same shape | **WORKS** — `book` frame for the ETH token arrived on the BTC-subscribed connection. |

Caveats observed: an intervening old-shape send may leave the connection in a state where a
subsequent `operation`-add did not deliver a frame within the probe window (seen once, not
reproduced on a clean connection). Treat hot-add as reliable on clean connections and verify
the expected `book` snapshot arrives after every add (which the standby/promotion logic does
anyway).

**Implications for the architecture section above:**
- The rollover hot path no longer needs a reconnect per 5-minute window: subscribe new-window
  tokens with `operation: "subscribe"`, retire old ones with `operation: "unsubscribe"`.
- The "one shared connection for all 7 assets × 2 tokens" goal is unblocked: tokens can be
  added incrementally on one connection. The warm-standby recommendation stands (the
  connected-but-frozen failure mode from `py-clob-client#292` / `real-time-data-client#26`
  still requires replacing the socket), but "adding tokens requires a fresh connection" in the
  handoff's verified facts is **superseded**.
- `initial_dump` (changelog 2025-05-28) defaults to `true`, which matches what the probe saw
  (full `book` on add) — do not set it to `false`; that snapshot is the gap-healing primitive.
