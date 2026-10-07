"""Worker-timeout regression: 1h book_snapshots builds must complete.

2026-10-07: per-asset book_snapshots_500ms workers burned the full 900s
spawn budget and failed closed on a loaded box (thousands of tiny flush
files, 842-wide hive rows vs ~122 staging columns, full-width decode).
The build now reads column-projected batches one date partition at a time
(gc/trim between partitions), logs progress every ~60s, and runs under an
1800s spawn budget (cap kept as a backstop, fail-closed kept).
"""

import inspect
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage.export import (
    _build_in_subprocess,
    _date_partition_of,
    _get_schema,
    _group_paths_by_date,
    _load_market_id_map,
    _projected_read_columns,
    _stream_export_asset_dataset,
)


def test_date_partition_of() -> None:
    assert _date_partition_of(Path("data/book_snapshots_500ms/date=2026-10-07/asset=BTC/f.parquet")) == "2026-10-07"
    assert _date_partition_of(Path("data/markets_latest/markets_latest.parquet")) == ""
    assert _date_partition_of(Path("f.parquet")) == ""


def test_group_paths_by_date_preserves_order() -> None:
    files = [Path(f"data/d/date={d}/asset=BTC/{n}.parquet")
             for d, n in [("2026-10-06", "b"), ("2026-10-07", "a"),
                           ("2026-10-06", "c"), ("2026-10-07", "d")]]
    groups = _group_paths_by_date(files)
    assert [g[0] for g in groups] == ["2026-10-06", "2026-10-07"]
    assert [p.name for p in groups[0][1]] == ["b.parquet", "c.parquet"]
    assert [p.name for p in groups[1][1]] == ["a.parquet", "d.parquet"]
    assert _group_paths_by_date([]) == []


def test_projected_read_columns() -> None:
    schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
    file_cols = ["a", "b", "wide_1", "wide_2", "series_id"]
    got = _projected_read_columns(file_cols, schema, ["series_id", "asset"])
    assert got == ["a", "b", "series_id"]  # schema order first, then extras; wide cols dropped
    # schema evolution: file predates a schema column -> absent (writer null-fills)
    got2 = _projected_read_columns(["a", "series_id"], schema, ["series_id"])
    assert got2 == ["a", "series_id"]
    assert _projected_read_columns([], schema, ["series_id"]) is None
    assert _projected_read_columns(None, schema, ["series_id"]) is None


def test_worker_spawn_budget_kept_but_raised() -> None:
    sig = inspect.signature(_build_in_subprocess)
    assert sig.parameters["timeout_s"].default == 1800
    src = inspect.getsource(_build_in_subprocess)
    assert "timeout=timeout_s" in src  # budget still enforced, never removed


def _real_hive():
    return Path(__file__).resolve().parents[1] / "data" / "book_snapshots_500ms"


def test_stream_snapshot_completes_on_real_files(tmp_path) -> None:
    """Bounded real-data smoke: 3 true hive files (842-wide rows) -> staging.

    Skipped when data/ is absent. Asserts identical row content (exact key
    sets) for the 1h lane, lane purity, schema, stats accounting, and no
    tmp litter — no fabricated prices anywhere.
    """
    hive = _real_hive()
    src_files = sorted((hive / "date=2026-10-07" / "asset=BTC").glob("*.parquet"))[:3]
    if not src_files or not (hive.parent / "markets_latest" / "markets_latest.parquet").exists():
        pytest.skip("no real hive data present")
    mini = tmp_path / "data"
    for f in src_files:
        dst = mini / "book_snapshots_500ms" / "date=2026-10-07" / "asset=BTC" / f.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(f, dst)
    ml_src = hive.parent / "markets_latest" / "markets_latest.parquet"
    ml_dst = mini / "markets_latest" / "markets_latest.parquet"
    ml_dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ml_src, ml_dst)

    mid_map = _load_market_id_map(mini)
    io_stats: dict = {}
    out_tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    rows = _stream_export_asset_dataset(
        mini, "book_snapshots_500ms", "BTC", out_tmp, "1h", 10, mid_map,
        io_stats=io_stats)
    assert rows > 0
    assert out_tmp.exists()
    assert io_stats.get("files_ok") == 3
    assert io_stats.get("files_failed", 0) == 0

    got = pq.read_table(str(out_tmp))
    schema = _get_schema("book_snapshots_500ms", 10)
    assert got.schema.names == schema.names
    assert set(got.column("asset").to_pylist()) == {"BTC"}
    assert set(got.column("series_id").to_pylist()) == {"BTC-1h"}
    got_keys = set(zip(got.column("asset").to_pylist(),
                       got.column("condition_id").to_pylist(),
                       got.column("ts_snapshot_ns").to_pylist()))
    assert len(got_keys) == got.num_rows  # dedup exact, no double-count

    # independent key census from narrow-column reads of the same files
    want_keys = set()
    for f in src_files:
        t = pq.read_table(str(f), columns=["asset", "condition_id", "ts_snapshot_ns", "series_id"])
        for a, c, ts, s in zip(t.column("asset").to_pylist(), t.column("condition_id").to_pylist(),
                               t.column("ts_snapshot_ns").to_pylist(), t.column("series_id").to_pylist()):
            if a == "BTC" and s == "BTC-1h":
                want_keys.add((a, c, ts))
    assert got_keys == want_keys

    litter = [p for p in tmp_path.rglob("*.tmp.*") if p.is_file()]
    assert litter == []
