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
