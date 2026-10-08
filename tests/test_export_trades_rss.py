"""TRADES pre-pass rss-cap regression (2026-10-08).

Post-rebirth all 6 TRADES staging workers rss-cap-aborted sequentially
inside the trades union-schema pre-pass (thousands of tiny hive files,
~2MB/file untrimmed heap, trim only every 100 files). The fix bounds the
peak (gc + malloc_trim every 25 files) and logs per-100-files progress;
union semantics stay identical (columns accumulate, first non-null wins).
Fail-closed + 1000MB rss cap + worker timeout are untouched.
"""

import inspect
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage import export as ex


def test_union_field_add_first_non_null_wins() -> None:
    fields: dict = {}
    order: list = []
    ex._union_field_add(fields, order, "price", pa.float64())
    ex._union_field_add(fields, order, "price", pa.int64())
    assert fields["price"] == pa.float64()  # first type wins
    assert order == ["price"]
    ex._union_field_add(fields, order, "wallet", pa.null())
    ex._union_field_add(fields, order, "wallet", pa.string())
    assert fields["wallet"] == pa.string()  # null upgrades once
    assert order == ["price", "wallet"]
    ex._union_field_add(fields, order, "wallet", pa.int64())
    assert fields["wallet"] == pa.string()  # non-null never replaced


def test_union_field_add_never_raises() -> None:
    fields: dict = {}
    order: list = []
    ex._union_field_add(fields, order, "ok", pa.int64())
    ex._union_field_add(None, None, "x", pa.int64())  # type: ignore[arg-type]
    ex._union_field_add(fields, order, "ok", None)  # type: ignore[arg-type]
    assert order == ["ok"] and fields["ok"] == pa.int64()


def test_prepass_bounds_heap_and_logs_progress() -> None:
    src = inspect.getsource(ex._stream_export_trades_dataset)
    assert "_TRIM_EVERY = 25" in src
    assert "_gc_s.collect()" in src and "_malloc_trim()" in src
    assert "trades pre-pass" in src and "elapsed=" in src and "rss=" in src
    assert "coerce_ts_source_ms" in src


def test_rss_cap_and_fail_closed_kept() -> None:
    src = inspect.getsource(ex._build_worker_main)
    assert "_limit_mb: int = 1000" in src  # cap stays, never raised
    assert "rss-cap-abort" in src
    outer = inspect.getsource(ex._stream_export_trades_dataset)
    assert "fail closed" in outer  # per-file fail-closed accounting kept


def _real_trades_files(asset="BTC", limit=40):
    base = Path(__file__).resolve().parents[1] / "data" / "trades"
    files = sorted(p for p in base.rglob("*.parquet") if not p.name.endswith(".tmp")
                   and f"asset={asset}" in str(p))
    return files[:limit]


def _accumulate(files):
    fields: dict = {}
    order: list = []
    for p in files:
        try:
            sch = pq.read_schema(str(p))
        except Exception:
            continue
        for f in sch:
            ex._union_field_add(fields, order, f.name, f.type)
    return fields, order


def test_schema_union_prefix_matches_full_on_real_subset() -> None:
    files = _real_trades_files()
    if len(files) < 16:
        pytest.skip("no real trades hive data present")
    prefix_fields, prefix_order = _accumulate(files[:8])
    full_fields, full_order = _accumulate(files)
    for p in files[8:]:
        try:
            sch = pq.read_schema(str(p))
        except Exception:
            continue
        for f in sch:
            ex._union_field_add(prefix_fields, prefix_order, f.name, f.type)
    assert prefix_order == full_order
    assert prefix_fields == full_fields


def test_stream_trades_bounded_real_subset(tmp_path) -> None:
    cands = _real_trades_files(limit=60)
    if len(cands) < 8:
        pytest.skip("no real trades hive data present")
    try:
        files = sorted(cands, key=lambda p: p.stat().st_size)[:6]
    except OSError:
        pytest.skip("no real trades hive data present")
    mini = tmp_path / "data"
    for f in files:
        rel = f.relative_to(f.parents[3])
        dst = mini / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(f, dst)
    io_stats: dict = {}
    out_tmp = tmp_path / "BTC_trades.parquet.tmp"
    rows = ex._stream_export_trades_dataset(
        mini, "BTC", out_tmp, None, deadline_s=300, io_stats=io_stats)
    assert rows > 0
    assert out_tmp.exists()
    assert io_stats.get("files_ok", 0) >= len(files) - io_stats.get("files_failed", 0)
    got = pq.read_table(str(out_tmp))
    assert got.num_rows == rows  # totals never raise past what was read
    assert "condition_id" in got.schema.names and "trade_id" in got.schema.names
    litter = [p for p in tmp_path.rglob("*.tmp.*") if p.is_file()]
    assert litter == []
