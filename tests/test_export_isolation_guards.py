"""Export isolation guards (workstream 2B): staging builds must not steal the loop.

Real-data-only: every hive here is a tmp_path fixture; never touches data/.
Guards the fail-closed contract in src/polymarket_collector/storage/export.py:
- worker/flush timeout -> prior staging kept, no exception (old snapshot used)
- cutoff excludes post-cutoff rows (late files belong to the next export)
- gap evidence (collector_events/resync_episodes) never pruned
- prune delete path is quarantine-move only (failed move keeps the file)
"""

import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.export import (
    _commit_staging_file,
    _manifests_match,
    _source_manifest,
    cleanup_local_data,
)

GAP_DATASETS = ("collector_events", "resync_episodes")


def _write(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path), compression="zstd")


def _lanes() -> list:
    try:
        from polymarket_collector.config import CollectorConfig

        lanes = [str(t).lower() for t in (CollectorConfig.load().timeframes or [])]
        return lanes or ["5m"]
    except Exception:
        return ["5m"]


def test_flush_timeout_keeps_prior_snapshot_no_exception(tmp_path: Path) -> None:
    """Worker timeout (rows=None) keeps the old snapshot; shrink also fails closed."""
    base = tmp_path / "data"
    out = base / "kaggle_staging" / "t"
    out.mkdir(parents=True)
    prior = out / "BTC_book_snapshots_500ms.parquet"
    rows = [{"book_state": "live", "v": i} for i in range(5)]
    _write(prior, rows)
    before = prior.read_bytes()
    kw = dict(ds="book_snapshots_500ms", rolling_window=False, l2_levels=10, base=base, out=out)
    assert _commit_staging_file(prior, None, None, **kw) == 5  # timeout: no result
    assert prior.read_bytes() == before
    shrunk = prior.with_suffix(".parquet.tmp")
    _write(shrunk, rows[:2])
    assert _commit_staging_file(prior, shrunk, 2, **kw) == 5  # shrink: keep prior
    assert prior.read_bytes() == before
    assert not shrunk.exists()


def test_cutoff_excludes_post_cutoff_rows(tmp_path: Path) -> None:
    """Files with mtime > cutoff are the NEXT export's input, not this one's."""
    base = tmp_path / "data"
    d = base / "book_snapshots_500ms" / "date=2026-10-03" / "asset=BTC"
    f1 = d / "book_snapshots_500ms_1.parquet"
    f2 = d / "book_snapshots_500ms_2.parquet"
    _write(f1, [{"book_state": "live"}])
    _write(f2, [{"book_state": "live"}])
    old = time.time() - 3600
    os.utime(f1, (old, old))  # f2 lands mid-build -> post-cutoff
    cutoff = time.time() - 60
    at_cutoff = _source_manifest(base, "book_snapshots_500ms", "BTC", cutoff)
    full = _source_manifest(base, "book_snapshots_500ms", "BTC", time.time() + 60)
    assert at_cutoff["n"] == 1
    assert full["n"] == 2
    assert not _manifests_match(at_cutoff, full)


def _prune_hive(base: Path, now: float) -> dict:
    """Minimal hive: empty markets_latest, gap evidence, one prune-eligible chainlink file."""
    ml = base / "markets_latest"
    ml.mkdir(parents=True)
    pq.write_table(
        pa.table({"condition_id": pa.array([], type=pa.string()),
                  "market_end_ts_ms": pa.array([], type=pa.int64())}),
        str(ml / "markets_latest.parquet"), compression="zstd")
    for ds in GAP_DATASETS:
        p = base / ds / "date=2026-01-01" / f"{ds}_1.parquet"
        _write(p, [{"ts_received_ns": int((now - 30 * 86400) * 1e9), "note": "gap-trail"}])
        os.utime(p, (now - 8 * 3600, now - 8 * 3600))
    cl = base / "chainlink_events" / "date=2026-01-01" / f"chainlink_events_{int(now - 8 * 3600)}.parquet"
    _write(cl, [{"ts_received_ns": int((now - 30 * 86400) * 1e9)}])
    os.utime(cl, (now - 8 * 3600, now - 8 * 3600))
    for lane in _lanes():  # per-dataset coverage proof needs every lane's staging
        sp = base / "kaggle_staging" / lane / "BTC_chainlink_events.parquet"
        sp.parent.mkdir(parents=True, exist_ok=True)
        sp.touch()
        os.utime(sp, (now - 4 * 3600, now - 4 * 3600))
    return {"chainlink": cl}


def _run_prune(base: Path, now: float) -> dict:
    return cleanup_local_data(
        str(base), assets=["BTC"], timeframe_labels=["5m"], rolling_window=True,
        retention_hours=0, dry_run=False, reap_quarantine=False,
        checkpoint_ms=int(now * 1000))


def test_gap_evidence_datasets_never_pruned(tmp_path: Path) -> None:
    now = time.time()
    base = tmp_path / "data"
    _prune_hive(base, now)
    stats = _run_prune(base, now)
    for ds in GAP_DATASETS:
        p = base / ds / "date=2026-01-01" / f"{ds}_1.parquet"
        assert p.exists(), f"{ds} gap evidence must stay in the live hive"
        assert not (base / "_quarantine" / ds).exists()
    assert all("collector_events" not in k and "resync_episodes" not in k for k in stats)


def test_prune_delete_path_is_quarantine_only(tmp_path: Path) -> None:
    now = time.time()
    base = tmp_path / "data"
    paths = _prune_hive(base, now)
    cl = paths["chainlink"]
    content = cl.read_bytes()
    stats = _run_prune(base, now)
    rel = str(cl.relative_to(base))
    assert not cl.exists(), "eligible file must leave the live hive"
    qfile = base / "_quarantine" / rel
    assert qfile.exists(), "delete path is quarantine-move only, never unlink"
    assert qfile.read_bytes() == content
    assert stats.get(rel, 0) == 1
