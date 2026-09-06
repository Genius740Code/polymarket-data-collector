"""Tests for Arrow schemas — §11 partitioning, §6A settlement fields."""
import pyarrow as pa
from polymarket_collector.storage.schemas import (
    MARKETS_SCHEMA, BOOK_EVENTS_SCHEMA, TRADES_SCHEMA, CHAINLINK_SCHEMA,
    COLLECTOR_EVENTS_SCHEMA, RESYNC_EPISODES_SCHEMA, snapshot_schema
)


def test_markets_schema_has_settlement():
    names = MARKETS_SCHEMA.names
    for field in ["settlement_report_id", "settlement_price", "settlement_ts_utc", "settlement_tx_hash", "resolution_confirmed_at", "settlement_source"]:
        assert field in names, f"missing §6A field {field}"


def test_snapshot_schema_wide():
    schema = snapshot_schema()  # default: 10 levels (A3 — observed depth ~4-8)
    names = schema.names
    # top-of-book nullable
    assert "up_bid" in names and schema.field("up_bid").nullable is True
    # L2 80 cols by default
    l2_cols = [n for n in names if "_level_" in n]
    assert len(l2_cols) == 80  # 2 outcomes *2 sides*10 levels*2 (price/size)
    # depth aggregates (§3 — nullable)
    for outcome in ("up", "down"):
        for side in ("bid", "ask"):
            for thc in (1, 5, 10):
                col = f"{outcome}_{side}_depth_{thc}c"
                assert col in names
                assert schema.field(col).nullable is True
    # state
    assert schema.field("book_state").nullable is False
    assert schema.field("book_crossed").nullable is False
    assert schema.field("resync_id").nullable is True
    # 20-level variant (old hive files keep 20 columns; export tolerates trailing extras)
    schema20 = snapshot_schema(l2_levels=20)
    assert len([n for n in schema20.names if "_level_" in n]) == 160


def test_all_schemas_have_schema_version_where_expected():
    # schema_version removed per user request to save space / Kaggle simplicity
    for s, name in [(MARKETS_SCHEMA, "markets"), (BOOK_EVENTS_SCHEMA, "book_events"), (TRADES_SCHEMA, "trades"), (CHAINLINK_SCHEMA, "chainlink")]:
        assert "schema_version" not in s.names, f"{name} should not have schema_version after deletion"


def test_resync_episodes_has_gap_fields():
    names = RESYNC_EPISODES_SCHEMA.names
    assert "gap_duration_ms" in names
    assert "snapshots_missed_estimate" in names
    assert "resync_attempt_count" in names
    assert "disconnect_reason" in names


def test_no_fake_sequence_number_or_dead_columns():
    # Audit 2026-09-06: CLOB sends no sequence numbers; the old local tick counter
    # must never reappear in any collected schema. collector_events.token_id was
    # never populated by any emitter (100% null across all runs).
    assert "sequence_number" not in BOOK_EVENTS_SCHEMA.names
    assert "sequence_number" not in TRADES_SCHEMA.names
    assert "sequence_number" not in CHAINLINK_SCHEMA.names
    assert "token_id" not in COLLECTOR_EVENTS_SCHEMA.names


def test_snapshot_schema_persists_book_hash():
    # Audit 2026-09-06: A4 book-integrity hashes must be stored, not just validated.
    schema = snapshot_schema(l2_levels=10)
    for f in ("up_book_hash", "down_book_hash"):
        assert f in schema.names
        assert schema.field(f).nullable is True
    from polymarket_collector.book import BookSnapshot
    snap = BookSnapshot(
        snapshot_id="s1", schema_version="3.2.0", series_id="ser", window_index=1,
        condition_id="0xc", market_id="m1", asset="BTC",
        up_token_id="up", down_token_id="down",
        ts_snapshot_utc="2026-09-06T00:00:00.000Z", ts_snapshot_ns=0,
        up_bid=0.5, up_ask=0.6, up_bid_size=10.0, up_ask_size=10.0,
        down_bid=0.4, down_ask=0.3, down_bid_size=10.0, down_ask_size=10.0,
        l2={}, depths={}, market_time_remaining_ms=1000,
        up_book_age_ms=None, down_book_age_ms=None,
        is_rollover_window=False, book_state="live",
        resync_id=None, book_crossed=False,
        up_book_hash="abc123", down_book_hash=None,
    )
    flat = snap.to_flat_dict()
    assert flat["up_book_hash"] == "abc123"
    assert flat["down_book_hash"] is None
