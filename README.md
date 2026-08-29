# Polymarket Collector — BTC/ETH/SOL 5-min Up/Down Markets

Implements [PLAN.md v3](PLAN.md) — continuous 24/7 collection for BTC, ETH, SOL 5-minute binary markets with zero silent data loss.

> **Default read path for research:** `book_snapshots_clean` (§9B) — `book_state='live'` only. Querying `book_snapshots_500ms` directly includes `stale`/`resyncing` rows intentionally.

## Quick start

```bash
pip install -e ".[dev]"
cp config/collector.example.yaml config/collector.yaml  # edit assets, endpoints, thresholds
# 1. Verification gate (§18) — must pass before live run
polymarket-verify-gate --config config/collector.yaml

# 2. Start collector (one process handles BTC/ETH/SOL via asyncio tasks)
polymarket-collector --config config/collector.yaml

# 3. In a second shell / systemd unit — watchdog (§17A)
polymarket-watchdog --config config/collector.yaml
```

## Layout

```
src/polymarket_collector/
  config.py            # §0 configurable assets, not hardcoded
  enums.py             # §7 status/outcome, §3 book_state, §8 event types
  validation.py        # §3A sanity bounds [0,1] price, >=0 size
  book.py              # OrderBookState per (asset, condition_id), depth aggregates §3
  rollover.py          # §1 rollover procedure, dual-tracking overlap
  resync.py            # §1A disconnect/resync + buffer-and-replay
  storage/
    cursor_store.py    # §1B crash/restart durable cursor (per-asset or WAL shared)
    parquet_writer.py  # §10A batched flush + backpressure + dedup (§4,§5)
    markets_log.py     # §9A event-sourced markets + markets_latest compaction
    clean_view.py      # §9B book_snapshots_clean
    compaction.py      # §10A periodic compaction (temp + atomic rename)
    raw_archive.py     # §13 short-retention raw WS archive
    schemas.py         # Arrow/Parquet schemas for every table (§2-§6,§8)
  chainlink.py         # §6, §6A settlement ground truth
  collector.py         # main asyncio collector (ties all sections)
  completeness.py      # §15 daily completeness rollup
  clock.py             # §14 NTP sync + 50ms threshold
  capacity.py          # §11A capacity planning estimate
  verify_gate.py       # §18 hard gate before §1A may be built/run
  watchdog/
    watchdog.py        # §17A heartbeat + alerting
```

## Storage format (§11)

```
data/
  book_snapshots_500ms/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/part-*.parquet
  book_snapshots_clean/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/part-*.parquet   # §9B filtered view
  book_events/...
  trades/...
  chainlink_events/...
  markets_log/date=YYYY-MM-DD/part-*.parquet
  markets_latest/markets_latest.parquet
  resync_episodes/date=YYYY-MM-DD/part-*.parquet
  collector_events/date=YYYY-MM-DD/part-*.parquet
  raw_ws_archive/date=YYYY-MM-DD/asset={BTC,ETH,SOL}/raw-*.jsonl    # 24-48h rolling
```

Partitions are UTC calendar days, flushed in batches (§10A) and compacted daily.

## Config-driven assets (§0)

Don't hardcode `[BTC,ETH,SOL]` — add a fourth asset via:

```yaml
assets: [BTC, ETH, SOL, AVAX]
series_ids:
  BTC: "BTC-5MIN"
  ETH: "ETH-5MIN"
  SOL: "SOL-5MIN"
  AVAX: "AVAX-5MIN"
```

Before enabling, confirm a live 5-min market actually exists for that asset (§0).

## Clock & timestamps (§14)

- Every row preserves `ts_source` and `ts_received_ns` distinctly.
- VM clock must be NTP-synced (`chrony`); `clock_issue` fires if drift > 50ms.
- Snapshots use a single scheduler aligned to `floor(unix_ms/500)*500` UTC (§3).

## Null-vs-zero (§3)

Empty book side → `null`, never `0`. `0` is a real zero-size level (rare). Applies uniformly to `*_bid/*_ask/*_bid_size/*_ask_size`, L2 levels, and `depth_Nc`.

## Depth aggregates (§3)

`depth_1c/5c/10c` = cumulative size within 1¢/5¢/10¢ of **that side's own best price** (not mid, not opposite side). Computed from stored L2 levels.

## Testing (§19)

```bash
pytest tests/test_chaos_*.py -v   # disconnect / sequence gap / malformed / REST failure / crash / backpressure
pytest -q                         # full suite (unit + chaos)
```

Each chaos scenario asserts `book_state`, `resync_episodes`, and `book_snapshots_clean` invariants — not merely "no exception".

## Verification gate (§18)

`polymarket-verify-gate` probes the live Polymarket CLOB REST+WS endpoints and checks:

- WS messages carry monotonic `sequence_number` per token
- REST exposes full L2 book per token (not just BBO)
- Settlement report/tx reachable vs `inferred_nearest` fallback
- Actual rate-limit headers → sizes backoff parameters

§1A must not be coded/run until this gate passes against a live payload capture.
