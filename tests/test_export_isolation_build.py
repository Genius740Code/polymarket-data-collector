"""Workstream 2A (build): export-isolation contract tests.

Covers the WAL-cutoff snapshot design in storage/export_isolation.py:
1. cutoff correctness — hive files newer than the snapshot cutoff stay invisible;
2. flush-timeout fail-closed — a stalled flush keeps the previous snapshot, never partial;
3. no-loss within two cycles — rows held in WAL/buffer during a skipped cycle all stage next cycle.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage import export_isolation as iso
from polymarket_collector.storage.export import _read_dataset_per_asset
from polymarket_collector.storage.parquet_writer import ParquetWriter


def _write_hive_rows(root: Path, dataset: str, asset: str, name: str, trade_ids: list) -> Path:
    d = root / dataset / "date=2026-01-02" / f"asset={asset}"
    d.mkdir(parents=True, exist_ok=True)
    t = pa.table({
        "token_id": ["tok-1"] * len(trade_ids),
        "sequence_number": list(range(len(trade_ids))),
        "trade_id": trade_ids,
        "asset": [asset] * len(trade_ids),
        "ts_source": [1_000_000 + i for i in range(len(trade_ids))],
    })
    p = d / name
    pq.write_table(t, str(p), compression="zstd")
    return p


def test_cutoff_hides_newer_rows(tmp_path: Path):
    old = _write_hive_rows(tmp_path, "trades", "BTC", "part-old.parquet", ["old-1", "old-2"])
    new = _write_hive_rows(tmp_path, "trades", "BTC", "part-new.parquet", ["new-1"])
    now = time.time()
    os.utime(old, (now - 100, now - 100))
    os.utime(new, (now + 100, now + 100))
    snap = iso.begin_snapshot(tmp_path, datasets=["trades"], assets=["BTC"])
    assert snap["ok"] is True
    cutoff = float(snap["cutoff_ts"])
    assert old.stat().st_mtime <= cutoff < new.stat().st_mtime
    vis = iso.visible_files([old, new], cutoff)
    assert [Path(p).name for p in vis] == ["part-old.parquet"]
    # isolation listing agrees
    listed = [p.name for p in iso.snapshot_files(tmp_path, "trades", "BTC", cutoff)]
    assert listed == ["part-old.parquet"]
    # the in-process export reader honors the same cutoff
    tbl = _read_dataset_per_asset(tmp_path, "trades", "BTC", cutoff_ts=cutoff)
    assert tbl is not None
    assert sorted(tbl.column("trade_id").to_pylist()) == ["old-1", "old-2"]
    # without cutoff both files read (legacy behavior preserved)
    tbl_all = _read_dataset_per_asset(tmp_path, "trades", "BTC")
    assert sorted(tbl_all.column("trade_id").to_pylist()) == ["new-1", "old-1", "old-2"]


def test_flush_timeout_fail_closed(tmp_path: Path):
    def _stalled() -> int:
        time.sleep(5.0)
        return 1

    prev = {"ok": True, "cutoff_ts": 1_700_000_000.0}
    snap = iso.begin_snapshot(
        tmp_path, datasets=["trades"], assets=["BTC"],
        flush_fn=_stalled, flush_timeout_s=0.3, previous=prev,
    )
    assert snap["ok"] is False
    assert snap["reason"] == "flush_timeout"
    # previous snapshot retained — the caller keeps prior staging, ships nothing partial
    assert snap["cutoff_ts"] == 1_700_000_000.0

    # a fast flush advances the snapshot normally
    snap2 = iso.begin_snapshot(
        tmp_path, datasets=["trades"], assets=["BTC"],
        flush_fn=lambda: 7, flush_timeout_s=5.0, previous=prev,
    )
    assert snap2["ok"] is True
    assert snap2["flushed"] == 7
    assert float(snap2["cutoff_ts"]) > 1_700_000_000.0


def test_no_loss_within_two_cycles(tmp_path: Path):
    w = ParquetWriter(
        str(tmp_path), flush_interval_seconds=3600, flush_row_count_threshold=10**9,
        buffer_max_rows=10**9, wal_enabled=True,
    )
    rows = [
        {"token_id": "tok-9", "sequence_number": i, "trade_id": f"cyc-{i}",
         "asset": "BTC", "ts_source": 2_000_000 + i}
        for i in range(6)
    ]
    for r in rows:
        assert w.append("trades", dict(r), asset="BTC", date_str="2026-01-02") is True
    assert len(w._buffer) == 6  # all rows still in RAM buffer, nothing durable in hive yet

    # cycle 1: flush stalls -> skip, prior (empty) staging kept, rows stay in WAL/buffer
    def _stalled() -> int:
        time.sleep(5.0)
        return w.flush()

    snap1 = iso.begin_snapshot(
        tmp_path, datasets=["trades"], assets=["BTC"],
        flush_fn=_stalled, flush_timeout_s=0.3, previous=None,
    )
    assert snap1["ok"] is False and snap1["reason"] == "flush_timeout"
    assert len(w._buffer) == 6  # nothing dropped, nothing pruned

    # cycle 2: flush succeeds -> cutoff taken after durability; every row stages
    snap2 = iso.begin_snapshot(
        tmp_path, datasets=["trades"], assets=["BTC"],
        flush_fn=w.flush, flush_timeout_s=30.0, previous=snap1,
    )
    assert snap2["ok"] is True
    assert int(snap2["flushed"]) == 6
    cutoff = float(snap2["cutoff_ts"])
    staged_ids: list = []
    for p in iso.snapshot_files(tmp_path, "trades", "BTC", cutoff):
        staged_ids.extend(pq.read_table(str(p)).column("trade_id").to_pylist())
    assert sorted(staged_ids) == [f"cyc-{i}" for i in range(6)]
    try:
        w.close()
    except Exception:
        pass
