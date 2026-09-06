"""Arrow / Parquet schemas for every table (§2-§6, §8, §11).

Schemas mirror PLAN.md field lists exactly, including §3 null-vs-zero
(nullable columns) and §6A settlement fields. Used by ParquetWriter.
"""
from __future__ import annotations

import pyarrow as pa


# Common
SCHEMA_VERSION_FIELD = pa.field("schema_version", pa.string(), nullable=False)

# §2 + §6A markets — time first, condition_id second
# §3.5 enrichment: slug/window_label/window_size_seconds/recorded_at added for Kaggle plan.md §3
MARKETS_SCHEMA = pa.schema([
    pa.field("updated_at", pa.string(), nullable=False),
    pa.field("recorded_at", pa.string(), nullable=True),  # alias of updated_at for Kaggle JSON (§3.2)
    pa.field("market_start_ts", pa.string(), nullable=True),  # ISO8601
    pa.field("market_end_ts", pa.string(), nullable=True),
    pa.field("market_start_ts_ms", pa.int64(), nullable=True),  # epoch ms alias (§3.2)
    pa.field("market_end_ts_ms", pa.int64(), nullable=True),
    pa.field("resolution_ts", pa.string(), nullable=True),
    pa.field("condition_id", pa.string(), nullable=False),
    pa.field("market_id", pa.string(), nullable=False),
    pa.field("slug", pa.string(), nullable=True),  # §3.1 e.g. btc-updown-5m-1774390200
    pa.field("series_id", pa.string(), nullable=False),
    pa.field("window_index", pa.int64(), nullable=False),
    pa.field("window_label", pa.string(), nullable=True),  # §3.1 5m/15m/1h/4h/1d
    pa.field("window_size_seconds", pa.int64(), nullable=True),
    pa.field("asset", pa.string(), nullable=False),
    pa.field("up_token_id", pa.string(), nullable=False),
    pa.field("down_token_id", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("resolution_outcome", pa.string(), nullable=False),
    pa.field("question", pa.string(), nullable=True),
    pa.field("resolution_rule", pa.string(), nullable=True),
    pa.field("resolution_source", pa.string(), nullable=True),
    pa.field("tick_size", pa.float64(), nullable=True),  # from Gamma orderPriceMinTickSize at discovery
    pa.field("minimum_order_size", pa.float64(), nullable=True),  # backfilled from CLOB /markets/{cid} (resolution_backfill)
    pa.field("minimum_notional", pa.float64(), nullable=True),
    # fee_information / resolution_rule / resolution_source dropped 2026-09-05:
    # never populated by Gamma or CLOB for 5m crypto markets (100% null forever)
    pa.field("reported_volume", pa.float64(), nullable=True),
    pa.field("reported_liquidity", pa.float64(), nullable=True),
    # §6A settlement ground truth
    pa.field("settlement_report_id", pa.string(), nullable=True),
    pa.field("settlement_price", pa.float64(), nullable=True),
    pa.field("settlement_ts_utc", pa.string(), nullable=True),
    pa.field("settlement_tx_hash", pa.string(), nullable=True),
    pa.field("resolution_confirmed_at", pa.string(), nullable=True),
    pa.field("settlement_source", pa.string(), nullable=True),  # on_chain_confirmed | inferred_nearest
])

# §3 book_snapshots_500ms — wide flat-column (dynamic due to l2_levels, so base + generated)
# time first, condition_id second per user request
def snapshot_schema(l2_levels: int = 10) -> pa.Schema:
    fields = [
        pa.field("ts_snapshot_utc", pa.string(), nullable=False),
        pa.field("ts_snapshot_ns", pa.int64(), nullable=False),
        pa.field("condition_id", pa.string(), nullable=False),
        pa.field("market_id", pa.string(), nullable=False),
        pa.field("series_id", pa.string(), nullable=False),
        pa.field("window_index", pa.int64(), nullable=False),
        pa.field("asset", pa.string(), nullable=False),
        pa.field("snapshot_id", pa.string(), nullable=False),
        pa.field("up_token_id", pa.string(), nullable=False),
        pa.field("down_token_id", pa.string(), nullable=False),
        # top-of-book (nullable per null-vs-zero)
        pa.field("up_bid", pa.float64(), nullable=True),
        pa.field("up_ask", pa.float64(), nullable=True),
        pa.field("up_bid_size", pa.float64(), nullable=True),
        pa.field("up_ask_size", pa.float64(), nullable=True),
        pa.field("down_bid", pa.float64(), nullable=True),
        pa.field("down_ask", pa.float64(), nullable=True),
        pa.field("down_bid_size", pa.float64(), nullable=True),
        pa.field("down_ask_size", pa.float64(), nullable=True),
    ]
    # L2 levels
    for outcome in ("up", "down"):
        for side in ("bid", "ask"):
            for lvl in range(1, l2_levels + 1):
                fields.append(pa.field(f"{outcome}_{side}_level_{lvl}_price", pa.float64(), nullable=True))
                fields.append(pa.field(f"{outcome}_{side}_level_{lvl}_size", pa.float64(), nullable=True))
    # depth aggregates (§3 — nullable if best is null)
    for outcome in ("up", "down"):
        for side in ("bid", "ask"):
            for thc in (1, 5, 10):
                fields.append(pa.field(f"{outcome}_{side}_depth_{thc}c", pa.float64(), nullable=True))
    # state
    fields.extend([
        pa.field("market_time_remaining_ms", pa.int64(), nullable=False),
        pa.field("up_book_age_ms", pa.int64(), nullable=True),
        pa.field("down_book_age_ms", pa.int64(), nullable=True),
        pa.field("is_rollover_window", pa.bool_(), nullable=False),
        pa.field("book_state", pa.string(), nullable=False),
        pa.field("resync_id", pa.string(), nullable=True),
        pa.field("book_crossed", pa.bool_(), nullable=False),
    ])
    return pa.schema(fields)


# §4 book_events — time first, condition_id second
BOOK_EVENTS_SCHEMA = pa.schema([
    pa.field("ts_source", pa.string(), nullable=True),
    pa.field("ts_received_ns", pa.int64(), nullable=False),
    pa.field("condition_id", pa.string(), nullable=False),
    pa.field("market_id", pa.string(), nullable=False),
    pa.field("series_id", pa.string(), nullable=False),
    pa.field("window_index", pa.int64(), nullable=False),
    pa.field("asset", pa.string(), nullable=False),
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("token_id", pa.string(), nullable=False),
    pa.field("outcome", pa.string(), nullable=False),
    pa.field("event_type", pa.string(), nullable=False),
    pa.field("sequence_number", pa.int64(), nullable=True),
    pa.field("old_best_bid", pa.float64(), nullable=True),
    pa.field("new_best_bid", pa.float64(), nullable=True),
    pa.field("old_best_ask", pa.float64(), nullable=True),
    pa.field("new_best_ask", pa.float64(), nullable=True),
    pa.field("old_bid_size", pa.float64(), nullable=True),
    pa.field("new_bid_size", pa.float64(), nullable=True),
    pa.field("old_ask_size", pa.float64(), nullable=True),
    pa.field("new_ask_size", pa.float64(), nullable=True),
    pa.field("threshold_config_id", pa.string(), nullable=True),
    # sequence_number dropped 2026-09-05: CLOB market channel sends none (100% null)
])

# §5 trades — time first, condition_id second, with transaction_hash + wallet fields (no RPC)
# wallet fields come from CLOB REST/WS (proxyWallet/maker/taker) — no on-chain RPC required
TRADES_SCHEMA = pa.schema([
    pa.field("ts_source", pa.string(), nullable=True),
    pa.field("ts_received_ns", pa.int64(), nullable=False),
    pa.field("condition_id", pa.string(), nullable=False),
    pa.field("market_id", pa.string(), nullable=False),
    pa.field("series_id", pa.string(), nullable=False),
    pa.field("window_index", pa.int64(), nullable=False),
    pa.field("asset", pa.string(), nullable=False),
    pa.field("trade_id", pa.string(), nullable=False),
    pa.field("transaction_hash", pa.string(), nullable=True),
    pa.field("token_id", pa.string(), nullable=False),
    pa.field("outcome", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("size", pa.float64(), nullable=False),
    pa.field("notional", pa.float64(), nullable=True),
    pa.field("fee", pa.float64(), nullable=True),
    pa.field("fee_is_estimated", pa.bool_(), nullable=True),  # true if fee was 0.07% fallback, false if exchange reported
    pa.field("side", pa.string(), nullable=True),
    pa.field("aggressor_side", pa.string(), nullable=True),
    # sequence_number dropped 2026-09-05: CLOB sends no sequence numbers (100% null)
    # wallet — from CLOB trade payload (maker/taker proxy wallet), no RPC
    pa.field("maker_wallet", pa.string(), nullable=True),   # maker proxy wallet (0x...)
    pa.field("taker_wallet", pa.string(), nullable=True),   # taker proxy wallet (0x...)
    pa.field("wallet", pa.string(), nullable=True),         # canonical wallet (taker if present else maker) for single-col queries
])

# §6 chainlink_events — time first
# twap/twap_window_seconds/round_id/sequence_number dropped 2026-09-05: the RTDS
# payload carries none of them (100% null); rolling TWAP is a downstream derived
# metric, not a stored column. report_id kept as the §6A settlement join key.
CHAINLINK_SCHEMA = pa.schema([
    pa.field("ts_source", pa.string(), nullable=True),
    pa.field("ts_received_ns", pa.int64(), nullable=False),
    pa.field("asset", pa.string(), nullable=False),
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=True),
    pa.field("source", pa.string(), nullable=True),
    pa.field("price", pa.float64(), nullable=True),
    pa.field("report_id", pa.string(), nullable=True),
])

# §8 collector_events — time first, condition_id second
COLLECTOR_EVENTS_SCHEMA = pa.schema([
    pa.field("ts_utc", pa.string(), nullable=False),
    pa.field("ts_received_ns", pa.int64(), nullable=False),
    pa.field("condition_id", pa.string(), nullable=True),
    pa.field("asset", pa.string(), nullable=True),
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("event_type", pa.string(), nullable=False),
    pa.field("connection_id", pa.string(), nullable=True),
    pa.field("market_id", pa.string(), nullable=True),
    pa.field("token_id", pa.string(), nullable=True),
    pa.field("details", pa.string(), nullable=True),
])

# §1A resync_episodes — time first, condition_id second
RESYNC_EPISODES_SCHEMA = pa.schema([
    pa.field("disconnect_ts_utc", pa.string(), nullable=False),
    pa.field("reconnect_ts_utc", pa.string(), nullable=True),
    pa.field("resync_rest_fetch_ts_utc", pa.string(), nullable=True),
    pa.field("resync_completed_ts_utc", pa.string(), nullable=True),
    pa.field("condition_id", pa.string(), nullable=True),
    pa.field("asset", pa.string(), nullable=False),
    pa.field("resync_id", pa.string(), nullable=False),
    pa.field("disconnect_reason", pa.string(), nullable=False),
    pa.field("gap_duration_ms", pa.int64(), nullable=True),
    pa.field("snapshots_missed_estimate", pa.int64(), nullable=True),
    pa.field("resync_attempt_count", pa.int64(), nullable=False),
])

# §9 event_thresholds_config
THRESHOLDS_SCHEMA = pa.schema([
    pa.field("threshold_config_id", pa.string(), nullable=False),
    pa.field("effective_from_ts", pa.string(), nullable=False),
    pa.field("effective_to_ts", pa.string(), nullable=True),
    pa.field("spread_change_threshold", pa.float64(), nullable=False),
    pa.field("size_change_threshold_pct", pa.float64(), nullable=False),
    pa.field("depth_change_threshold_pct", pa.float64(), nullable=False),
    pa.field("crossing_threshold", pa.float64(), nullable=False),
])


# Map dataset name → schema for generic writer
SCHEMAS = {
    "markets_log": MARKETS_SCHEMA,
    "book_snapshots_500ms": None,  # dynamic via snapshot_schema()
    "book_events": BOOK_EVENTS_SCHEMA,
    "trades": TRADES_SCHEMA,
    "chainlink_events": CHAINLINK_SCHEMA,
    "collector_events": COLLECTOR_EVENTS_SCHEMA,
    "resync_episodes": RESYNC_EPISODES_SCHEMA,
    "event_thresholds_config": THRESHOLDS_SCHEMA,
    "markets_summary": None,  # derived export, built by export.build_markets_summary()
}


# Analyst-facing one-row-per-market summary (§Kaggle markets_summary.parquet).
# Purely derived: every field is computed from the other datasets at export time.
# Underlying open/close = nearest chainlink tick to the window boundary.
# Outcome OHLC = mid price ((bid+ask)/2) from clean snapshots, first/last/min/max.
# avg_spread = mean(ask - bid) across the window's clean snapshots per outcome.
MARKETS_SUMMARY_SCHEMA = pa.schema([
    pa.field("condition_id", pa.string(), nullable=False),
    pa.field("asset", pa.string(), nullable=False),
    pa.field("slug", pa.string(), nullable=True),
    pa.field("window_start_ts", pa.string(), nullable=True),   # ISO8601
    pa.field("window_end_ts", pa.string(), nullable=True),     # ISO8601
    pa.field("window_start_ts_ms", pa.int64(), nullable=True),
    pa.field("window_end_ts_ms", pa.int64(), nullable=True),
    pa.field("window_index", pa.int64(), nullable=True),
    pa.field("up_token_id", pa.string(), nullable=True),
    pa.field("down_token_id", pa.string(), nullable=True),
    pa.field("resolution_outcome", pa.string(), nullable=True),   # up | down | tie | unknown
    pa.field("settlement_price", pa.float64(), nullable=True),
    pa.field("settlement_source", pa.string(), nullable=True),
    # underlying (chainlink) reference at window boundaries
    pa.field("underlying_open", pa.float64(), nullable=True),
    pa.field("underlying_open_ts_utc", pa.string(), nullable=True),
    pa.field("underlying_open_tolerance_s", pa.int64(), nullable=True),
    pa.field("underlying_close", pa.float64(), nullable=True),
    pa.field("underlying_close_ts_utc", pa.string(), nullable=True),
    pa.field("underlying_close_tolerance_s", pa.int64(), nullable=True),
    # outcome-token OHLC (mid price from clean snapshots)
    pa.field("up_open", pa.float64(), nullable=True),
    pa.field("up_high", pa.float64(), nullable=True),
    pa.field("up_low", pa.float64(), nullable=True),
    pa.field("up_close", pa.float64(), nullable=True),
    pa.field("down_open", pa.float64(), nullable=True),
    pa.field("down_high", pa.float64(), nullable=True),
    pa.field("down_low", pa.float64(), nullable=True),
    pa.field("down_close", pa.float64(), nullable=True),
    # activity
    pa.field("traded_volume", pa.float64(), nullable=True),   # sum(price*size), trades incl. api- rows
    pa.field("fill_count", pa.int64(), nullable=True),        # number of fills
    pa.field("unique_traders", pa.int64(), nullable=True),    # distinct non-null wallet values
    pa.field("avg_spread_up", pa.float64(), nullable=True),
    pa.field("avg_spread_down", pa.float64(), nullable=True),
    pa.field("snapshot_count", pa.int64(), nullable=True),    # clean snapshots contributing to OHLC
])
