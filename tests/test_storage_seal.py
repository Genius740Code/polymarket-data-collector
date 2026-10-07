"""Read-side seal tests — flush-boundary partials.

Real repo-layout fixtures only: rows go through the repo's own ParquetWriter
(atomic tmp+rename) in /tmp, then are read back sealed vs open. Row values
are constant literals (price 0.5, size 10) — no invented market data.
"""
import os
import tempfile
import time
from pathlib import Path

from polymarket_collector.storage.parquet_io import (
    SEAL_GRACE_S,
    is_sealed,
    list_sealed_dataset_files,
    list_sealed_files,
    read_table,
)
from polymarket_collector.storage.parquet_writer import ParquetWriter
from polymarket_collector.storage.streaming import iter_source_files, stream_batches
from polymarket_collector.storage import export_isolation


DATE_STR = "2026-10-07"


def _trade(i):
    return {
        "token_id": "tok-seal",
        "sequence_number": 100 + i,
        "trade_id": f"seal-t{i}",
        "price": 0.5,
        "size": 10,
        "asset": "BTC",
    }


def _write_flush_file(tmp, rows, date_str=DATE_STR):
    """Append rows via ParquetWriter and flush; return the final .parquet path."""
    writer = ParquetWriter(
        data_dir=tmp,
        flush_interval_seconds=3600,
        flush_row_count_threshold=100000,
        buffer_max_rows=1000,
        wal_enabled=True,
        on_event=None,
    )
    try:
        for r in rows:
            assert writer.append("trades", dict(r), asset="BTC", date_str=date_str) is True
        assert writer.flush() == len(rows)
    finally:
        writer.close()
    leaf = Path(tmp) / "trades" / f"date={date_str}" / "asset=BTC"
    finals = sorted(
        (p for p in leaf.glob("*.parquet") if not p.name.endswith(".tmp")),
        key=lambda p: p.name,
    )
    assert finals
    return finals[-1]


def _backdate(path, age_s):
    old = time.time() - age_s
    os.utime(path, (old, old))


def test_fresh_flush_file_is_open_not_sealed():
    with tempfile.TemporaryDirectory() as tmp:
        f = _write_flush_file(tmp, [_trade(1), _trade(2)])
        assert f.exists()
        assert is_sealed(f) is False
        assert list_sealed_files([f]) == []


def test_aged_file_is_sealed_and_reads_back():
    with tempfile.TemporaryDirectory() as tmp:
        f = _write_flush_file(tmp, [_trade(1), _trade(2)])
        _backdate(f, SEAL_GRACE_S + 60)
        assert is_sealed(f) is True
        assert list_sealed_files([f]) == [f]
        t = read_table(f)
        assert t is not None and t.num_rows == 2


def test_seal_splits_open_from_sealed_in_one_leaf():
    with tempfile.TemporaryDirectory() as tmp:
        old = _write_flush_file(tmp, [_trade(1)])
        _backdate(old, SEAL_GRACE_S + 60)
        new = _write_flush_file(tmp, [_trade(2)])
        assert old != new
        sealed = list_sealed_files([old, new])
        assert sealed == [old]
        ds = list_sealed_dataset_files(Path(tmp) / "trades")
        assert ds == [old]


def test_fail_closed_markers_excluded():
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "nope.parquet"
        assert is_sealed(missing) is False
        assert is_sealed(Path(tmp) / "x.parquet.tmp") is False
        assert list_sealed_files([missing, None, 123]) == []
        assert list_sealed_files(None) == []
        assert list_sealed_dataset_files(Path(tmp) / "no-such-dataset") == []


def test_totals_never_raise():
    assert is_sealed(None) is False
    assert is_sealed(123) is False
    assert list_sealed_files(None) == []
    assert list_sealed_files([None, 123, "/nope/x.parquet"]) == []
    assert list_sealed_dataset_files("/nope") == []
    assert list_sealed_dataset_files(None) == []


def test_staging_path_legacy_default_unchanged_and_seal_opt_in():
    with tempfile.TemporaryDirectory() as tmp:
        f = _write_flush_file(tmp, [_trade(1)])
        # Legacy default: fresh file visible (no behavior change).
        assert f in iter_source_files(tmp, "trades", "BTC")
        assert f in export_isolation.snapshot_files(tmp, "trades", "BTC")
        # Opt-in seal: fresh (open) file excluded from the staging path.
        assert iter_source_files(tmp, "trades", "BTC", seal_grace_s=120) == []
        assert export_isolation.snapshot_files(tmp, "trades", "BTC", seal_grace_s=120) == []
        assert list(stream_batches(tmp, "trades", "BTC", seal_grace_s=120)) == []
        # After aging past the grace the file is staging-visible again.
        _backdate(f, 600)
        assert f in iter_source_files(tmp, "trades", "BTC", seal_grace_s=120)
        assert f in export_isolation.snapshot_files(tmp, "trades", "BTC", seal_grace_s=120)
        batches = list(stream_batches(tmp, "trades", "BTC", seal_grace_s=120))
        assert sum(b.num_rows for b in batches) == 1
