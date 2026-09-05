"""Enums — §7 status/outcome, §3 book_state, §8 collector event types.

All string values are lowercase to match Parquet storage convention and to
avoid case-sensitivity bugs at query time.
"""
from __future__ import annotations

from enum import Enum


class StrEnum(str, Enum):
    def __str__(self) -> str:  # pragma: no cover
        return self.value


# §7 -----------------------------------------------------------------------
class MarketStatus(StrEnum):
    pending = "pending"
    active = "active"
    closed = "closed"
    resolved = "resolved"


class ResolutionOutcome(StrEnum):
    up = "up"
    down = "down"
    tie = "tie"
    voided = "voided"
    disputed = "disputed"
    unknown = "unknown"


# §3 -----------------------------------------------------------------------
class BookState(StrEnum):
    live = "live"
    stale = "stale"
    resyncing = "resyncing"


# §8 -----------------------------------------------------------------------
class CollectorEventType(StrEnum):
    # connection lifecycle
    connected = "connected"
    disconnected = "disconnected"
    reconnected = "reconnected"
    # §1A WS + resync
    ws_disconnected = "ws_disconnected"
    ws_reconnect_attempt = "ws_reconnect_attempt"
    ws_reconnected = "ws_reconnected"
    resync_started = "resync_started"
    resync_completed = "resync_completed"
    resync_failed = "resync_failed"
    # subscriptions / markets
    subscription_started = "subscription_started"
    subscription_failed = "subscription_failed"
    market_added = "market_added"
    market_removed = "market_removed"
    # §1 rollover
    rollover_started = "rollover_started"
    rollover_miss = "rollover_miss"
    rollover_completed = "rollover_completed"
    coverage_gap = "coverage_gap"
    rate_limited = "rate_limited"
    # data quality
    snapshot_gap = "snapshot_gap"
    event_gap = "event_gap"
    sequence_gap = "sequence_gap"
    duplicate_event = "duplicate_event"
    book_anomaly = "book_anomaly"
    resolution_stuck = "resolution_stuck"
    write_failed = "write_failed"
    backpressure = "backpressure"
    # R-5: the 500ms scheduler briefly falling behind emits catch-up snapshots —
    # that is scheduler lag, NOT writer backpressure (writer dropped_total stayed 0).
    # Kept as its own event type so backpressure keeps meaning "writer refused a row".
    scheduler_lag = "scheduler_lag"
    # §1B
    collector_started = "collector_started"
    collector_restarted = "collector_restarted"
    clock_issue = "clock_issue"
    # 500ms snapshot-loop liveness (do NOT misuse `connected` — that is a real
    # WS connection-state event; a periodic heartbeat here poisoned telemetry)
    snapshot_heartbeat = "snapshot_heartbeat"
    # Kaggle 10-min upload
    kaggle_upload_started = "kaggle_upload_started"
    kaggle_upload_success = "kaggle_upload_success"
    kaggle_upload_failed = "kaggle_upload_failed"
    # liquidity filtering — no RPC, Gamma only
    low_liquidity = "low_liquidity"


class DisconnectReason(StrEnum):
    network_error = "network_error"
    heartbeat_timeout = "heartbeat_timeout"
    exchange_close = "exchange_close"
    process_pause = "process_pause"
    unknown = "unknown"


class SettlementSource(StrEnum):
    on_chain_confirmed = "on_chain_confirmed"
    inferred_nearest = "inferred_nearest"
