"""Regression tests for compute_daily_completeness clean-count source resolution.

Design (chosen once, applies everywhere):
  Priority for the per-date clean count, all FOOTER-ONLY (never reading rows):
    1. local per-date clean hive  (book_snapshots_clean/date=.../asset=...)
       → clean_source == "local_clean_hive", old behavior unchanged.
    2. staging clean artifact scoped to THIS date (a `date=` segment on its
       path under kaggle_staging/) → clean_source == "staging".
    3. else: NONE. The production architecture builds the clean view per-lane
       inside Kaggle staging at export time, cumulative over the rolling
       retention window, so a single date is never footer-countable from it.
       `actual_clean_snapshots` stays None — NOT 0, because 0 would fabricate
       proof of an empty day. `clean_source == "raw_footers_only"` marks the
       degradation, and both completeness_ratio() and missing_intervals fall
       back to the REAL raw 500ms footer counts (raw rows / expected).

  Gaps stay gaps: nothing is invented away.
  When raw==0 too, missing_intervals == expected (an honestly empty day),
  matching the pre-fix contract for empty directories.
"""
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.completeness import compute_daily_completeness

EXPECTED = 2 * 86400  # 172800


def _write_parquet(path: Path, n_rows: int) -> None:
    rows = [{"snapshot_id": str(i), "book_state": "live"} for i in range(n_rows)]
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path))


def test_local_clean_hive_unchanged():
    """(a) Local per-date clean hive present → old behavior + clean_source."""
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _write_parquet(base / "book_snapshots_500ms" / "date=2025-01-01" / "asset=BTC" / "part-001.parquet", 12)
        _write_parquet(base / "book_snapshots_clean" / "date=2025-01-01" / "asset=BTC" / "part-001.parquet", 10)

        rows = compute_daily_completeness(tmp, "2025-01-01")
        btc = next(r for r in rows if r.asset == "BTC")
        d = btc.to_dict()
        assert btc.actual_snapshots == 12
        assert btc.actual_clean_snapshots == 10
        assert btc.missing_intervals == EXPECTED - 10
        assert btc.completeness_ratio() == 10 / EXPECTED
        assert d["clean_source"] == "local_clean_hive"


def test_raw_only_falls_back_to_raw_footers():
    """(b) No local clean hive, raw 500ms files exist → ratio from RAW footers.

    Pre-fix this printed clean=0.0 and missing=172800 (all missing) even with
    a full raw day on disk. Post-fix: actual_clean_snapshots stays None
    (never a fabricated 0), clean_source == "raw_footers_only", and the
    ratio/missing degrade to the real raw footer count.
    """
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _write_parquet(base / "book_snapshots_500ms" / "date=2025-01-01" / "asset=BTC" / "part-001.parquet", 100_000)

        rows = compute_daily_completeness(tmp, "2025-01-01")
        btc = next(r for r in rows if r.asset == "BTC")
        d = btc.to_dict()
        assert btc.actual_snapshots == 100_000
        assert btc.actual_clean_snapshots is None
        assert d["clean_source"] == "raw_footers_only"
        # no more all-missing number for a day with real raw rows
        assert btc.missing_intervals == EXPECTED - 100_000
        assert btc.missing_intervals < EXPECTED
        assert btc.completeness_ratio() == 100_000 / EXPECTED


def test_no_data_at_all_is_an_honest_empty_day():
    """Empty dir: raw==0 → clean=None, source raw_footers_only, missing==expected.

    Consistent with the pre-fix contract for no data (nothing fabricated in
    either direction); clean_source marks where the numbers came from.
    """
    with tempfile.TemporaryDirectory() as tmp:
        rows = compute_daily_completeness(tmp, "2025-01-01")
        assert len(rows) >= 1
        for r in rows:
            d = r.to_dict()
            assert r.actual_clean_snapshots is None
            assert d["clean_source"] == "raw_footers_only"
            assert r.actual_snapshots == 0
            assert r.missing_intervals == EXPECTED


def test_staging_per_date_scoped_clean_artifact_used():
    """A staging clean artifact scoped to this date (date= path segment) is
    footer-countable per day → clean_source == "staging"."""
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _write_parquet(
            base / "kaggle_staging" / "testlane" / "date=2025-01-01" / "BTC_book_snapshots_clean.parquet", 77
        )
        _write_parquet(
            base / "book_snapshots_500ms" / "date=2025-01-01" / "asset=BTC" / "part-001.parquet", 500
        )

        rows = compute_daily_completeness(tmp, "2025-01-01")
        btc = next(r for r in rows if r.asset == "BTC")
        assert btc.actual_clean_snapshots == 77
        assert btc.to_dict()["clean_source"] == "staging"
        assert btc.missing_intervals == EXPECTED - 77


def test_cumulative_staging_clean_artifact_never_attributed_to_one_date():
    """The production cumulative staging clean file (no date= segment) must
    NOT be counted as this date's clean count — it spans the rolling
    retention window; attributing it to one date would fabricate a number."""
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _write_parquet(
            base / "kaggle_staging" / "5m" / "slug" / "BTC_book_snapshots_clean.parquet", 999_999
        )
        _write_parquet(
            base / "book_snapshots_500ms" / "date=2025-01-01" / "asset=BTC" / "part-001.parquet", 1_000
        )

        rows = compute_daily_completeness(tmp, "2025-01-01")
        btc = next(r for r in rows if r.asset == "BTC")
        assert btc.actual_clean_snapshots is None
        assert btc.to_dict()["clean_source"] == "raw_footers_only"
        assert btc.missing_intervals == EXPECTED - 1_000
