"""Tests for §9B canonical clean view — book_snapshots_clean is default for backtests."""
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.book import OrderBookState
from polymarket_collector.storage.clean_view import build_clean_view, load_clean
from polymarket_collector.storage.markets_log import MarketsLog
import time


def test_clean_view_filters_stale_and_disputed():
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        # Create markets_latest with one disputed market
        log = MarketsLog(tmp)
        log.append({"condition_id": "cid-live", "status": "resolved", "resolution_outcome": "up", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 1, "market_id": "m1", "asset": "BTC", "up_token_id": "up", "down_token_id": "down"}, updated_at="2025-01-01T00:10:00Z")
        log.append({"condition_id": "cid-disputed", "status": "resolved", "resolution_outcome": "disputed", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 2, "market_id": "m2", "asset": "BTC", "up_token_id": "up2", "down_token_id": "down2"}, updated_at="2025-01-01T00:10:00Z")
        log.append({"condition_id": "cid-unknown", "status": "active", "resolution_outcome": "unknown", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 3, "market_id": "m3", "asset": "BTC", "up_token_id": "up3", "down_token_id": "down3"}, updated_at="2025-01-01T00:10:00Z")
        log.flush_staging()
        log.compact()

        # Create book_snapshots_500ms with mixed book_state
        # Need to write parquet files manually in hive partition layout
        date_str = "2025-01-01"
        asset = "BTC"
        snapshots = [
            {"snapshot_id": "1", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 1, "condition_id": "cid-live", "market_id": "m1", "asset": "BTC", "up_token_id": "up", "down_token_id": "down", "ts_snapshot_utc": "2025-01-01T00:00:00Z", "ts_snapshot_ns": 0, "up_bid": 0.5, "up_ask": 0.6, "up_bid_size": 10, "up_ask_size": 10, "down_bid": 0.4, "down_ask": 0.5, "down_bid_size": 10, "down_ask_size": 10, "market_time_remaining_ms": 1000, "up_book_age_ms": 0, "down_book_age_ms": 0, "is_rollover_window": False, "book_state": "live", "resync_id": None, "book_crossed": False},
            {"snapshot_id": "2", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 1, "condition_id": "cid-live", "market_id": "m1", "asset": "BTC", "up_token_id": "up", "down_token_id": "down", "ts_snapshot_utc": "2025-01-01T00:00:00.500Z", "ts_snapshot_ns": 500_000_000, "up_bid": 0.51, "up_ask": 0.6, "up_bid_size": 9, "up_ask_size": 10, "down_bid": 0.4, "down_ask": 0.5, "down_bid_size": 10, "down_ask_size": 10, "market_time_remaining_ms": 500, "up_book_age_ms": 0, "down_book_age_ms": 0, "is_rollover_window": False, "book_state": "stale", "resync_id": "r1", "book_crossed": False},
            {"snapshot_id": "3", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 2, "condition_id": "cid-disputed", "market_id": "m2", "asset": "BTC", "up_token_id": "up2", "down_token_id": "down2", "ts_snapshot_utc": "2025-01-01T00:00:01Z", "ts_snapshot_ns": 1_000_000_000, "up_bid": 0.5, "up_ask": 0.6, "up_bid_size": 10, "up_ask_size": 10, "down_bid": 0.4, "down_ask": 0.5, "down_bid_size": 10, "down_ask_size": 10, "market_time_remaining_ms": 1000, "up_book_age_ms": 0, "down_book_age_ms": 0, "is_rollover_window": False, "book_state": "live", "resync_id": None, "book_crossed": False},
            {"snapshot_id": "4", "schema_version": "3.0.0", "series_id": "BTC-5MIN", "window_index": 3, "condition_id": "cid-unknown", "market_id": "m3", "asset": "BTC", "up_token_id": "up3", "down_token_id": "down3", "ts_snapshot_utc": "2025-01-01T00:00:02Z", "ts_snapshot_ns": 2_000_000_000, "up_bid": 0.5, "up_ask": 0.6, "up_bid_size": 10, "up_ask_size": 10, "down_bid": 0.4, "down_ask": 0.5, "down_bid_size": 10, "down_ask_size": 10, "market_time_remaining_ms": 1000, "up_book_age_ms": 0, "down_book_age_ms": 0, "is_rollover_window": False, "book_state": "live", "resync_id": None, "book_crossed": False},
        ]
        for s in snapshots:
            for lvl in range(1, 21):
                s[f"up_bid_level_{lvl}_price"] = None
                s[f"up_bid_level_{lvl}_size"] = None
                s[f"up_ask_level_{lvl}_price"] = None
                s[f"up_ask_level_{lvl}_size"] = None
                s[f"down_bid_level_{lvl}_price"] = None
                s[f"down_bid_level_{lvl}_size"] = None
                s[f"down_ask_level_{lvl}_price"] = None
                s[f"down_ask_level_{lvl}_size"] = None
            for th in (1, 5, 10):
                s[f"up_bid_depth_{th}c"] = None
                s[f"up_ask_depth_{th}c"] = None
                s[f"down_bid_depth_{th}c"] = None
                s[f"down_ask_depth_{th}c"] = None

        src_dir = base / "book_snapshots_500ms" / f"date={date_str}" / f"asset={asset}"
        src_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(snapshots)
        pq.write_table(table, str(src_dir / "part-000.parquet"))

        # build clean view (default: exclude stale + disputed only — unknown is normal for active markets)
        n = build_clean_view(tmp)
        assert n == 2  # snapshots 1 and 4 qualify (live + not disputed); unknown not excluded

        clean = load_clean(tmp)
        assert clean is not None
        assert clean.num_rows == 2
        ids = [r["snapshot_id"] for r in clean.to_pylist()]
        assert "1" in ids
        assert "4" in ids
        assert "2" not in ids  # stale
        assert "3" not in ids  # disputed

        # opt-in disputed
        n2 = build_clean_view(tmp, opt_in_disputed=True)
        clean2 = load_clean(tmp)
        assert clean2 is not None
        # with opt_in, disputed included but stale still excluded → 3 rows (1,3,4)
        assert clean2.num_rows == 3


def test_clean_view_empty_no_crash():
    with tempfile.TemporaryDirectory() as tmp:
        n = build_clean_view(tmp)
        assert n == 0
        assert load_clean(tmp) is None
