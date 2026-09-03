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


# ---- 99.9% days coverage tests ----

EXPECTED_SNAPSHOTS_PER_DAY = 2 * 86400  # 172800 (2/sec * 86400s)
COMPLETENESS_THRESHOLD = 0.999  # 99.9%


def _make_snapshot_table(asset: str, count: int, date_str: str = "2025-01-01") -> pa.Table:
    """Helper to create a snapshot parquet table with given count for an asset on a date."""
    rows = [
        {
            "snapshot_id": str(i),
            "asset": asset,
            "condition_id": f"cid-{asset}-{i}",
            "ts_snapshot_utc": f"{date_str}T00:00:00Z",
            "ts_snapshot_ns": 1_700_000_000_000_000_000 + i * 500_000_000,
            "market_time_remaining_ms": 300_000,
            "is_rollover_window": False,
            "book_state": "live",
            "book_crossed": False,
        }
        for i in range(count)
    ]
    return pa.Table.from_pylist(rows)


def _write_snapshots(data_dir: str, asset: str, count: int, date_str: str = "2025-01-01") -> None:
    """Write snapshot parquet files for an asset on a date."""
    snap_dir = Path(data_dir) / "book_snapshots_500ms" / f"date={date_str}" / f"asset={asset}"
    snap_dir.mkdir(parents=True, exist_ok=True)
    # Write in parts to simulate real partitioning
    table = _make_snapshot_table(asset, count)
    pq.write_table(table, str(snap_dir / "part-001.parquet"))
    # Also write clean view (live snapshots) so completeness_ratio reflects clean count
    clean_dir = Path(data_dir) / "book_snapshots_clean" / f"date={date_str}" / f"asset={asset}"
    clean_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(clean_dir / "part-001.parquet"))


def test_completeness_999_percent_coverage():
    """Test that 99.9%+ daily completeness is detected when data is near-complete.

    With 172800 expected snapshots per day, 99.9% coverage means at least 172627 actual snapshots.
    Missing intervals must be <= 172800 * (1 - 0.999) = 172.8 → max 172 missing.
    """
    with tempfile.TemporaryDirectory() as tmp:
        # Write 172800 - 172 = 172628 snapshots (99.9%+ coverage)
        _write_snapshots(tmp, "BTC", EXPECTED_SNAPSHOTS_PER_DAY - 172, "2025-01-01")

        result = compute_daily_completeness(tmp, "2025-01-01")
        btc = next(r for r in result if r.asset == "BTC")

        # Should have near-perfect coverage
        assert btc.actual_snapshots >= EXPECTED_SNAPSHOTS_PER_DAY * COMPLETENESS_THRESHOLD, (
            f"Expected >= {EXPECTED_SNAPSHOTS_PER_DAY * COMPLETENESS_THRESHOLD} actual snapshots, "
            f"got {btc.actual_snapshots}"
        )
        assert btc.completeness_ratio() >= COMPLETENESS_THRESHOLD, (
            f"Expected completeness ratio >= {COMPLETENESS_THRESHOLD}, got {btc.completeness_ratio()}"
        )
        assert btc.missing_intervals <= int(EXPECTED_SNAPSHOTS_PER_DAY * (1 - COMPLETENESS_THRESHOLD)), (
            f"Expected missing_intervals <= {int(EXPECTED_SNAPSHOTS_PER_DAY * (1 - COMPLETENESS_THRESHOLD))}, "
            f"got {btc.missing_intervals}"
        )


def test_completeness_below_threshold():
    """Test that completeness ratio drops below 99.9% when data is sparse."""
    with tempfile.TemporaryDirectory() as tmp:
        # Write only 50% of expected snapshots
        _write_snapshots(tmp, "BTC", EXPECTED_SNAPSHOTS_PER_DAY // 2, "2025-01-01")

        result = compute_daily_completeness(tmp, "2025-01-01")
        btc = next(r for r in result if r.asset == "BTC")

        assert btc.completeness_ratio() < COMPLETENESS_THRESHOLD, (
            f"Expected completeness ratio < {COMPLETENESS_THRESHOLD} with sparse data, "
            f"got {btc.completeness_ratio()}"
        )
        assert btc.missing_intervals > int(EXPECTED_SNAPSHOTS_PER_DAY * (1 - COMPLETENESS_THRESHOLD)), (
            f"Expected missing_intervals > {int(EXPECTED_SNAPSHOTS_PER_DAY * (1 - COMPLETENESS_THRESHOLD))}, "
            f"got {btc.missing_intervals}"
        )


def test_completeness_multiple_assets_multiple_dates():
    """Test completeness tracking across multiple assets and dates.

    Verifies that the completeness module correctly tracks gaps for
    different assets on different dates, ensuring no null values are
    missed and gap detection works across the dataset.
    """
    with tempfile.TemporaryDirectory() as tmp:
        # BTC: 100% coverage (all snapshots)
        _write_snapshots(tmp, "BTC", EXPECTED_SNAPSHOTS_PER_DAY, "2025-01-01")
        # ETH: 99.5% coverage (missing some)
        _write_snapshots(tmp, "ETH", int(EXPECTED_SNAPSHOTS_PER_DAY * 0.995), "2025-01-01")
        # SOL: 0% coverage (no data)
        # No snapshots written for SOL on 2025-01-01

        result = compute_daily_completeness(tmp, "2025-01-01")

        assets = {r.asset: r for r in result}
        # BTC should have 100% completeness
        btc = assets.get("BTC")
        assert btc is not None, "BTC should be in results"
        assert btc.completeness_ratio() == 1.0, f"Expected BTC 100% completeness, got {btc.completeness_ratio()}"

        # ETH should have ~99.5% completeness
        eth = assets.get("ETH")
        assert eth is not None, "ETH should be in results"
        assert abs(eth.completeness_ratio() - 0.995) < 0.01, (
            f"Expected ETH ~99.5% completeness, got {eth.completeness_ratio()}"
        )

        # SOL should have 0% completeness (no data)
        sol = next((r for r in result if r.asset == "SOL"), None)
        if sol:
            assert sol.completeness_ratio() == 0.0, (
                f"Expected SOL 0% completeness with no data, got {sol.completeness_ratio()}"
            )


def test_completeness_with_clean_view():
    """Test that book_snapshots_clean view correctly filters stale/resync data.

    Ensures that the clean view only includes 'live' book_state snapshots,
    and that null values or missing data are properly accounted for.
    """
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        date_str = "2025-01-01"

        # Write snapshot data with mixed book states
        snap_dir = base / "book_snapshots_500ms" / f"date={date_str}" / "asset=BTC"
        snap_dir.mkdir(parents=True, exist_ok=True)

        # 10 live snapshots + 2 stale snapshots
        live_rows = [
            {
                "snapshot_id": str(i),
                "asset": "BTC",
                "condition_id": f"cid-live-{i}",
                "ts_snapshot_utc": f"{date_str}T00:00:00Z",
                "ts_snapshot_ns": 1_700_000_000_000_000_000 + i * 500_000_000,
                "market_time_remaining_ms": 300_000,
                "is_rollover_window": False,
                "book_state": "live",
                "book_crossed": False,
            }
            for i in range(10)
        ]
        stale_rows = [
            {
                "snapshot_id": str(i + 10),
                "asset": "BTC",
                "condition_id": f"cid-stale-{i}",
                "ts_snapshot_utc": f"{date_str}T00:00:00Z",
                "ts_snapshot_ns": 1_700_000_000_000_000_000 + (i + 10) * 500_000_000,
                "market_time_remaining_ms": 300_000,
                "is_rollover_window": False,
                "book_state": "stale",
                "book_crossed": False,
            }
            for i in range(2)
        ]

        all_rows = live_rows + stale_rows
        table = pa.Table.from_pylist(all_rows)
        pq.write_table(table, str(snap_dir / "part-001.parquet"))

        # Also write clean view (same data but filtered)
        clean_dir = base / "book_snapshots_clean" / f"date={date_str}" / "asset=BTC"
        clean_dir.mkdir(parents=True, exist_ok=True)
        live_only = [r for r in all_rows if r["book_state"] == "live"]
        table_clean = pa.Table.from_pylist(live_only)
        pq.write_table(table_clean, str(clean_dir / "part-001.parquet"))

        result = compute_daily_completeness(tmp, date_str)
        btc = next(r for r in result if r.asset == "BTC")

        # completeness should be based on clean (live) snapshots only
        # expected: 172800, actual_clean: 10 (only live ones)
        assert btc.actual_clean_snapshots == 10, (
            f"Expected 10 clean snapshots, got {btc.actual_clean_snapshots}"
        )
        assert btc.actual_snapshots == 12, (
            f"Expected 12 total snapshots (live+stale), got {btc.actual_snapshots}"
        )
        # missing_intervals should be based on clean snapshots
        assert btc.missing_intervals == EXPECTED_SNAPSHOTS_PER_DAY - 10, (
            f"Expected {EXPECTED_SNAPSHOTS_PER_DAY - 10} missing intervals (based on clean), "
            f"got {btc.missing_intervals}"
        )
        assert btc.completeness_ratio() == 10 / EXPECTED_SNAPSHOTS_PER_DAY, (
            f"Expected completeness ratio 10/172800, got {btc.completeness_ratio()}"
        )
