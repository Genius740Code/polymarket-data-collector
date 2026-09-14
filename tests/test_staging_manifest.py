"""Staging upload manifest (2026-09-14): resources must list only files on disk.

Regression: LOW's first 51-city export staged 208 stats keys but
WUHAN_book_snapshots_500ms.parquet was never written (WUHAN discovered
mid-export — zero hive inputs at worker time, zero at validation time, so
both gates passed). dataset_create_version then failed 5x with
"does not exist" and the whole version was lost. The manifest now excludes
keys with no staged file; the next tick rebuilds them.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from polymarket_collector.storage.export import _staging_resources


def test_missing_staged_file_excluded(tmp_path):
    (tmp_path / "HONG-KONG_trades.parquet").write_bytes(b"fake")
    (tmp_path / "EMPTY_book_events.parquet").write_bytes(b"")  # present-but-empty still ships
    stats = {
        "kaggle_staging/1d/d/HONG-KONG_trades.parquet": 10,
        "kaggle_staging/1d/d/EMPTY_book_events.parquet": 0,
        "kaggle_staging/1d/d/WUHAN_book_snapshots_500ms.parquet": 0,
    }
    resources, missing = _staging_resources(stats, tmp_path, "weather low 1d", "gghgg1/polymarket-weather-low")
    paths = [r["path"] for r in resources]
    assert "HONG-KONG_trades.parquet" in paths, paths
    assert "EMPTY_book_events.parquet" in paths, paths  # exists on disk -> ships
    assert "WUHAN_book_snapshots_500ms.parquet" not in paths, paths
    assert missing == ["WUHAN_book_snapshots_500ms.parquet"], missing


def test_all_present_nothing_missing(tmp_path):
    (tmp_path / "A.parquet").write_bytes(b"x")
    resources, missing = _staging_resources({"rel/A.parquet": 5}, tmp_path, "f", "d")
    assert [r["path"] for r in resources] == ["A.parquet"]
    assert missing == []
    assert resources[0]["description"].endswith("— d")
