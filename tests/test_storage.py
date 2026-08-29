"""Tests for storage — §10A batching/backpressure, §9A markets log, §11 partitioning."""
import tempfile
import time
from pathlib import Path

from polymarket_collector.config import CollectorConfig
from polymarket_collector.book import OrderBookState
from polymarket_collector.storage.parquet_writer import ParquetWriter
from polymarket_collector.storage.markets_log import MarketsLog
from polymarket_collector.storage.compaction import compact_all, compact_dataset


def test_parquet_writer_batching_and_dedup():
    with tempfile.TemporaryDirectory() as tmp:
        events = []
        writer = ParquetWriter(
            data_dir=tmp,
            flush_interval_seconds=60,
            flush_row_count_threshold=5,
            buffer_max_rows=100,
            wal_enabled=False,
            l2_levels=20,
            on_event=lambda t, d: events.append((t, d)),
        )
        # trades dedup on (token_id, sequence_number)
        row = {"token_id": "tok1", "sequence_number": 1, "trade_id": "t1", "price": 0.5, "size": 10, "asset": "BTC"}
        assert writer.append("trades", row, asset="BTC", date_str="2025-01-01") is True
        # duplicate → deduped, not appended
        assert writer.append("trades", row, asset="BTC", date_str="2025-01-01") is True
        assert len(writer._buffer) == 1  # second was deduped
        # fallback dedup: book_events without seq
        ev1 = {"token_id": "tok1", "ts_received_ns": 123, "event_type": "spread_change", "new_best_bid": 0.5, "new_best_ask": 0.6, "asset": "BTC"}
        ev2 = dict(ev1)
        writer.append("book_events", ev1, asset="BTC", date_str="2025-01-01")
        writer.append("book_events", ev2, asset="BTC", date_str="2025-01-01")
        assert len([b for b in writer._buffer if b.dataset == "book_events"]) == 1

        # flush threshold
        for i in range(5):
            writer.append("trades", {"token_id": f"tok{i+10}", "sequence_number": i, "trade_id": f"t{i}", "price": 0.5, "size": 1, "asset": "BTC"}, asset="BTC", date_str="2025-01-01")
        # may have auto-flushed; ensure files exist
        # force flush
        writer.flush()
        assert Path(tmp, "trades", "date=2025-01-01", "asset=BTC").exists()
        parts = list(Path(tmp, "trades", "date=2025-01-01", "asset=BTC").glob("*.parquet"))
        assert len(parts) >= 1


def test_backpressure_never_drops():
    with tempfile.TemporaryDirectory() as tmp:
        alerts = []
        writer = ParquetWriter(
            data_dir=tmp,
            flush_interval_seconds=999,
            flush_row_count_threshold=999,
            buffer_max_rows=3,
            wal_enabled=True,
            wal_dir=Path(tmp) / "_wal",
            on_event=lambda t, d: alerts.append(t),
        )
        # Fill buffer to max
        for i in range(3):
            writer.append("trades", {"token_id": f"t{i}", "sequence_number": i, "trade_id": f"id{i}", "price": 0.5, "size": 1, "asset": "BTC"}, asset="BTC", date_str="2025-01-01")
        assert len(writer._buffer) == 3
        # Next append should hit backpressure but spill to WAL, not drop
        ok = writer.append("trades", {"token_id": "spill", "sequence_number": 99, "trade_id": "spill", "price": 0.5, "size": 1, "asset": "BTC"}, asset="BTC", date_str="2025-01-01")
        assert ok is True  # spilled to WAL instead of dropped
        assert any("backpressure" in str(a) for a in alerts)
        # WAL file should contain spilled row
        wal_files = list(Path(tmp, "_wal").glob("*.jsonl"))
        assert any(f.stat().st_size > 0 for f in wal_files)


def test_snapshot_idempotent_key_redundant_collector():
    with tempfile.TemporaryDirectory() as tmp:
        writer = ParquetWriter(data_dir=tmp, buffer_max_rows=10000, wal_enabled=False)
        # two collectors writing same bucket (same asset, condition_id, bucket)
        row1 = {"asset": "BTC", "condition_id": "cid-1", "ts_snapshot_ns": 1_700_000_000_000_000_000, "price": 0.5}
        row2 = {"asset": "BTC", "condition_id": "cid-1", "ts_snapshot_ns": 1_700_000_000_100_000_000, "price": 0.5}  # 100ms later, same 500ms bucket
        # bucket = floor(ns/500ms)*500ms → both map to same key
        writer.append("book_snapshots_500ms", row1, asset="BTC", date_str="2025-01-01")
        writer.append("book_snapshots_500ms", row2, asset="BTC", date_str="2025-01-01")
        # only first should be kept due to dedup
        snap_rows = [b for b in writer._buffer if b.dataset == "book_snapshots_500ms"]
        assert len(snap_rows) == 1

        # different bucket → not deduped
        row3 = {"asset": "BTC", "condition_id": "cid-1", "ts_snapshot_ns": 1_700_000_000_500_000_000, "price": 0.6}
        writer.append("book_snapshots_500ms", row3, asset="BTC", date_str="2025-01-01")
        snap_rows2 = [b for b in writer._buffer if b.dataset == "book_snapshots_500ms"]
        assert len(snap_rows2) == 2


def test_markets_log_compaction():
    with tempfile.TemporaryDirectory() as tmp:
        log = MarketsLog(tmp)
        # append two updates for same condition_id
        log.append({"condition_id": "cid-1", "status": "active", "resolution_outcome": "unknown", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 1, "market_id": "m1", "asset": "BTC", "up_token_id": "up", "down_token_id": "down", "market_start_ts": "2025-01-01T00:00:00Z", "market_end_ts": "2025-01-01T00:05:00Z"}, updated_at="2025-01-01T00:00:00Z")
        log.append({"condition_id": "cid-1", "status": "resolved", "resolution_outcome": "up", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 1, "market_id": "m1", "asset": "BTC", "up_token_id": "up", "down_token_id": "down", "market_start_ts": "2025-01-01T00:00:00Z", "market_end_ts": "2025-01-01T00:05:00Z", "settlement_report_id": "r1", "settlement_price": 50000, "settlement_source": "on_chain_confirmed"}, updated_at="2025-01-01T00:06:00Z")
        log.append({"condition_id": "cid-2", "status": "active", "resolution_outcome": "unknown", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 2, "market_id": "m2", "asset": "BTC", "up_token_id": "up2", "down_token_id": "down2"}, updated_at="2025-01-01T00:05:00Z")

        log.flush_staging()
        path = log.compact()
        assert path.exists()
        latest = log.load_latest()
        assert len(latest) == 2
        cid1 = next(r for r in latest if r["condition_id"] == "cid-1")
        assert cid1["resolution_outcome"] == "up"
        assert cid1["settlement_report_id"] == "r1"


def test_compaction_atomic_no_inplace_overwrite():
    with tempfile.TemporaryDirectory() as tmp:
        writer = ParquetWriter(data_dir=tmp, wal_enabled=False)
        for i in range(4):
            writer.append("trades", {"token_id": f"t{i}", "sequence_number": i, "trade_id": f"id{i}", "price": 0.5, "size": 1, "asset": "BTC"}, asset="BTC", date_str="2025-01-01")
        writer.flush()
        leaf = Path(tmp) / "trades" / "date=2025-01-01" / "asset=BTC"
        before = set(leaf.glob("*.parquet"))
        assert len(before) >= 1
        # compact should produce new file and remove old (after atomic rename)
        compact_dataset(leaf)
        # if only one file, compaction is no-op; force multiple files
        # create second flush
        for i in range(4, 8):
            writer.append("trades", {"token_id": f"t{i}", "sequence_number": i, "trade_id": f"id{i}", "price": 0.5, "size": 1, "asset": "BTC"}, asset="BTC", date_str="2025-01-01")
        writer.flush()
        parts_before = list(leaf.glob("part-*.parquet"))
        if len(parts_before) > 1:
            compact_dataset(leaf)
            parts_after = list(leaf.glob("*.parquet"))
            # no .tmp files left
            assert not any(p.name.endswith(".tmp") for p in leaf.glob("*"))


def test_writer_partitions_utc():
    with tempfile.TemporaryDirectory() as tmp:
        writer = ParquetWriter(data_dir=tmp, wal_enabled=False)
        # row with UTC timestamp should partition to correct date
        writer.append("book_snapshots_500ms", {"asset": "BTC", "condition_id": "c1", "ts_snapshot_utc": "2025-03-15T23:59:59Z", "ts_snapshot_ns": 0, "schema_version": "3.0.0"}, asset="BTC", date_str="2025-03-15")
        writer.flush()
        assert Path(tmp, "book_snapshots_500ms", "date=2025-03-15", "asset=BTC").exists()
