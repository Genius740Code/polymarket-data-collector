"""Tests for §15 completeness — expected vs actual, gap tracking."""
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.completeness import compute_daily_completeness


def test_completeness_empty():
    with tempfile.TemporaryDirectory() as tmp:
        rows = compute_daily_completeness(tmp, "2025-01-01")
        # no data → still returns per-asset rows with 0 actual
        assert len(rows) >= 1
        for r in rows:
            assert r.expected_snapshots == 172800
            assert r.actual_snapshots == 0
            assert r.missing_intervals == 172800


def test_completeness_with_snapshots():
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        # write 2 snapshot files for BTC
        snap_dir = base / "book_snapshots_500ms" / "date=2025-01-01" / "asset=BTC"
        snap_dir.mkdir(parents=True, exist_ok=True)
        rows = [{"snapshot_id": str(i), "asset": "BTC", "book_state": "live"} for i in range(10)]
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, str(snap_dir / "part-001.parquet"))

        clean_dir = base / "book_snapshots_clean" / "date=2025-01-01" / "asset=BTC"
        clean_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(clean_dir / "part-001.parquet"))

        result = compute_daily_completeness(tmp, "2025-01-01")
        btc = next(r for r in result if r.asset == "BTC")
        assert btc.actual_snapshots == 10
        assert btc.actual_clean_snapshots == 10
        assert btc.missing_intervals == 172800 - 10


def test_completeness_resync_gaps():
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        re_dir = base / "resync_episodes" / "date=2025-01-01"
        re_dir.mkdir(parents=True, exist_ok=True)
        tbl = pa.Table.from_pylist([
            {"asset": "BTC", "gap_duration_ms": 5000, "resync_id": "r1"},
            {"asset": "BTC", "gap_duration_ms": 2000, "resync_id": "r2"},
            {"asset": "ETH", "gap_duration_ms": 1000, "resync_id": "r3"},
        ])
        pq.write_table(tbl, str(re_dir / "part-001.parquet"))
        result = compute_daily_completeness(tmp, "2025-01-01")
        btc = next(r for r in result if r.asset == "BTC")
        assert btc.resync_episode_count == 2
        assert btc.total_gap_ms == 7000
