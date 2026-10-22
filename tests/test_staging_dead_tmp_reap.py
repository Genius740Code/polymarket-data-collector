"""2026-10-22 #5b disk leak: dead publish tmps are never cleaned anywhere.

The staging publish is atomic tmp+rename with the WORKER PID embedded in the
tmp name (streaming._publish_tmp: X.parquet.tmp.<pid>.tmp over export's
X.parquet.tmp), so a SIGKILLed worker's scratch is never overwritten by the
next worker — each respawn (17 in ~28h measured; last three leaks ETH 18:29
148MB, BTC 18:59 158MB, BTC 19:15 133MB) strands a new 100-160MB tmp under
data/kaggle_staging/ and GBs accumulated (5m ~3.4GB + 1h ~1.1GB).
_staging_resources excludes tmp names from upload manifests but nothing
deleted them; the pre-upload janitor only runs when a pass REACHES the
upload, which an OOM/kill loop never does.

_reap_dead_staging_tmps (called ONCE at the top of the staging-prepare path,
prepare_kaggle_staging_5m) walks data/kaggle_staging/** bounded and deletes
ONLY *.tmp-named files older than the 120min grace (a live worker's in-flight
tmp is younger; longest measured staging pass ~1h/worker).

Covers:
- exactly the dead tmps (>120min) are deleted; a fresh tmp (<120min), a
  real (non-tmp) parquet however old, and dead tmps inside _quarantine/
  and _wal/ all survive
- deleted-bytes in the ONE summary line + result dict are the on-disk sizes
  of the files actually unlinked (honest accounting)
- idempotent: a second pass over an already-reaped tree deletes 0/0
- bounded: the walk stops at the file/elapsed budget and SAYS so in the
  summary line (never claims a completed sweep it did not do)
- never raises: missing root / root-is-a-file / unreadable entries
- the staging-prepare entry itself fires the reap once (the call site)
"""
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.export import (
    _STAGING_DEAD_TMP_GRACE_MIN,
    _reap_dead_staging_tmps,
    prepare_kaggle_staging_5m,
)

DEAD1 = "ETH_book_snapshots_500ms.parquet.tmp.999999.tmp"   # measured leak shape
DEAD2 = "BTC_book_snapshots_500ms.parquet.tmp.tmp"          # second scratch shape
FRESH1 = "BTC_trades.parquet.tmp.777777.tmp"                # live worker's in-flight
REAL1 = "BTC_book_events.parquet"
QUAR1 = "_quarantine/DEAD_book.parquet.tmp.666666.tmp"
WAL1 = "_wal/wal_book.parquet.tmp.555555.tmp"


def _staging_root(tmp_path: Path) -> Path:
    return tmp_path / "kaggle_staging"


def _lane_dir(root: Path) -> Path:
    return root / "5m" / "gghgg1" / "polymarket-5m-crypto"


def _put(path: Path, size: int, age_min: float, real: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if real:
        pq.write_table(pa.table({"ts": [123], "condition_id": ["c1"]}),
                       str(path), compression="zstd")
    else:
        path.write_bytes(b"A" * size)
    now = time.time()
    os.utime(path, (now - age_min * 60.0, now - age_min * 60.0))
    return path


def _names(root: Path) -> set:
    return {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}


def test_reap_deletes_only_dead_tmps(tmp_path: Path, capsys) -> None:
    root = _staging_root(tmp_path)
    lane = _lane_dir(root)
    dead1 = _put(lane / DEAD1, 1111, 180.0)      # (a) dead: 180min > 120min grace
    dead2 = _put(lane / DEAD2, 2222, 125.0)      # (a) dead: 125min > 120min grace
    fresh = _put(lane / FRESH1, 333, 60.0)       # (b) fresh: 60min < grace — live worker
    real = _put(lane / REAL1, 0, 180.0, real=True)  # (c) real parquet, OLD but non-tmp
    quar = _put(lane / QUAR1, 444, 300.0)        # (d) dead tmp inside _quarantine/
    wal = _put(lane / WAL1, 555, 300.0)          # (d) dead tmp inside _wal/
    before = _names(root)

    res = _reap_dead_staging_tmps(root)
    out = capsys.readouterr().out

    after = _names(root)
    # exactly the two dead tmps are gone
    assert after == before - {str(dead1.relative_to(root)), str(dead2.relative_to(root))}
    # survivors: fresh tmp, real parquet (old!), quarantine + wal contents
    for p in (fresh, real, quar, wal):
        assert p.is_file(), f"survivor wrongly touched: {p}"
    # honest accounting: deleted bytes = the on-disk sizes actually unlinked
    assert res["deleted"] == 2
    assert res["bytes"] == 1111 + 2222
    assert res["budget_exhausted"] is False
    # scanned counts only VISITED files: the _quarantine/ and _wal/ subtrees
    # are pruned from the walk structurally (rule: never touched at all),
    # so exactly the 4 files in the lane dir were visited
    assert res["scanned"] == 4
    # ONE honest summary line with counts + bytes + grace
    assert f"[export] dead-tmp reap: deleted 2 file(s) / {1111 + 2222} bytes (grace 120min)" in out
    assert out.count("dead-tmp reap:") == 1


def test_grace_constant_is_120_minutes() -> None:
    # grace must stay 120min: 2x the longest measured staging pass (~1h/worker)
    assert _STAGING_DEAD_TMP_GRACE_MIN == 120


def test_reap_covers_all_three_scratch_shapes_idempotent(tmp_path: Path, capsys) -> None:
    root = _staging_root(tmp_path)
    lane = _lane_dir(root)
    _put(lane / "A_book_snapshots_500ms.parquet.tmp.111111.tmp", 100, 200.0)
    _put(lane / "B_book_snapshots_500ms.parquet.tmp.tmp", 200, 200.0)
    _put(lane / "C_book_snapshots_500ms.parquet.tmp", 300, 200.0)  # export's own publish tmp

    first = _reap_dead_staging_tmps(root)
    capsys.readouterr()
    assert first["deleted"] == 3
    assert first["bytes"] == 100 + 200 + 300

    # idempotent: nothing left to reap, second pass deletes 0/0 bytes
    second = _reap_dead_staging_tmps(root)
    out2 = capsys.readouterr().out
    assert second["deleted"] == 0
    assert second["bytes"] == 0
    assert second["budget_exhausted"] is False
    assert "deleted 0 file(s) / 0 bytes (grace 120min)" in out2


def test_reap_scan_budget_stops_early_and_says_so(tmp_path: Path, capsys) -> None:
    root = _staging_root(tmp_path)
    lane = _lane_dir(root)
    for i in range(3):
        _put(lane / f"DEAD{i}_trades.parquet.tmp.{100000 + i}.tmp", 1000 + i, 300.0)

    res = _reap_dead_staging_tmps(root, max_files=2)
    out = capsys.readouterr().out

    # walk stopped after the file budget: honest truncation flag + line text.
    # max_files=2 examines 2 entries (both reaped); the stop triggers when
    # the 3rd entry is reached and is never processed — scanned==3.
    assert res["budget_exhausted"] is True
    assert res["scanned"] == 3
    assert res["deleted"] == 2
    assert "walk stopped at scan budget - next pass continues" in out
    # the file the budget never reached is still on disk (next pass reaps it)
    remaining = sorted(p.name for p in lane.glob("*.tmp*"))
    assert len(remaining) == 1


def test_reap_never_raises(tmp_path: Path) -> None:
    # missing root: zeros, no exception
    res = _reap_dead_staging_tmps(tmp_path / "nope" / "kaggle_staging")
    assert res == {"deleted": 0, "bytes": 0, "scanned": 0, "budget_exhausted": False}
    # root is a regular FILE, not a dir: zeros, no exception
    f = tmp_path / "kaggle_staging_is_a_file"
    f.write_bytes(b"x")
    res2 = _reap_dead_staging_tmps(f)
    assert res2 == {"deleted": 0, "bytes": 0, "scanned": 0, "budget_exhausted": False}
    # unreadable directory below the root does not raise either
    root = _staging_root(tmp_path)
    bad = root / "5m" / "gghgg1" / "unreadable"
    bad.mkdir(parents=True)
    os.chmod(bad, 0o000)
    try:
        res3 = _reap_dead_staging_tmps(root)
        assert res3["deleted"] == 0
    finally:
        os.chmod(bad, 0o755)


def test_staging_prepare_entry_fires_the_reap_once(tmp_path: Path, capsys) -> None:
    """The ONE call site: prepare_kaggle_staging_5m reaps dead tmps at pass start.

    datasets=['markets_log'] keeps the entry bounded (no per-asset worker
    spawns, no Data-API calls — empty tmp hive builds one schema-empty
    markets file), exactly like the measured OLD/NEW proof runs.
    """
    data = tmp_path / "data"
    root = data / "kaggle_staging"
    lane = _lane_dir(root)
    _put(lane / DEAD1, 1111, 180.0)   # dead
    _put(lane / FRESH1, 333, 60.0)    # fresh — must survive
    _put(lane / REAL1, 0, 180.0, real=True)

    prepare_kaggle_staging_5m(str(data), assets=["BTC"], datasets=["markets_log"])
    out = capsys.readouterr().out

    assert not (lane / DEAD1).exists(), "dead tmp survived the staging-prepare pass"
    assert (lane / FRESH1).is_file(), "fresh (live-worker) tmp must survive"
    assert (lane / REAL1).is_file(), "real parquet must survive"
    assert "[export] dead-tmp reap: deleted 1 file(s) / 1111 bytes (grace 120min)" in out
    assert out.count("dead-tmp reap:") == 1
