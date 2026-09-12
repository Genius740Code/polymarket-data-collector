"""Subprocess-isolated dataset builds: identical output, bounded parent RAM."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow.parquet as pq

from polymarket_collector.storage.export import (
    _build_in_subprocess,
    _commit_staging_file,
)
from test_streaming_export import _hive  # mini-hive with hex/dup/mixed series


def test_worker_build_and_commit(tmp_path):
    base = _hive(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    res = _build_in_subprocess(
        base, out, ["book_snapshots_500ms"], ["BTC"], "5m", 10, False, True)
    assert res is not None and "error" not in res
    rel = next(k for k in res if k.endswith("BTC_book_snapshots_500ms.parquet"))
    info = res[rel]
    assert info["rows"] == 4, info
    out_path = out / "BTC_book_snapshots_500ms.parquet"
    n = _commit_staging_file(
        out_path, Path(info["tmp"]) if info["tmp"] else None, info["rows"],
        ds="book_snapshots_500ms", rolling_window=True, l2_levels=10,
        base=base, out=out)
    assert n == 4
    got = pq.read_table(out_path)
    assert got.num_rows == 4
    assert {r["market_id"] for r in got.to_pylist()} == {"9", "42"}
