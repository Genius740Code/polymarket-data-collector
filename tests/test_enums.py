"""Tests for enums — §7/§8 completeness."""
from polymarket_collector.enums import MarketStatus, ResolutionOutcome, BookState, CollectorEventType, DisconnectReason, SettlementSource


def test_resolution_outcome_covers_all():
    vals = {e.value for e in ResolutionOutcome}
    assert {"up", "down", "tie", "voided", "disputed", "unknown"} <= vals


def test_market_status():
    assert MarketStatus.active.value == "active"
    assert MarketStatus.resolved.value == "resolved"


def test_book_state():
    assert BookState.live.value == "live"
    assert BookState.stale.value == "stale"
    assert BookState.resyncing.value == "resyncing"


def test_collector_event_types_cover_plan():
    vals = {e.value for e in CollectorEventType}
    required = {"ws_disconnected", "ws_reconnected", "resync_started", "resync_completed", "resync_failed",
                "coverage_gap", "rate_limited", "sequence_gap", "duplicate_event", "book_anomaly",
                "resolution_stuck", "backpressure", "collector_started", "collector_restarted", "clock_issue",
                "rollover_miss", "rollover_started", "rollover_completed"}
    assert required <= vals


def test_disconnect_reason():
    assert DisconnectReason.network_error.value == "network_error"
    assert SettlementSource.on_chain_confirmed.value == "on_chain_confirmed"
