"""2026-09-20 disk-full fix: the _quarantine/ dir must be bounded.

The verified-upload prune MOVES files to <data_dir>/_quarantine/ (same
filesystem, was direct unlink) and nothing ever deleted them, so every
"prune" freed 0 bytes while the hive kept growing until ENOSPC killed the
collector (2026-09-19 incident: 5 lanes x 1h round-robin vs 6h retention,
last successful prunes deleted 8 then 0 files).

Covers:
- reap deletes aged files, keeps young ones, never touches the live hive
- size cap deletes oldest-first until under budget (even within grace)
- dry_run reports without deleting
- cleanup_local_data reaps even when the prune early-returns (no upload yet)
- retention_cadence_check flags lane-cadence > retention (the incident math)
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from polymarket_collector.collector import retention_cadence_check
from polymarket_collector.config import CollectorConfig
from polymarket_collector.storage.export import cleanup_local_data
from polymarket_collector.storage.quarantine import reap_quarantine


def _mk(path: Path, size: int = 100, age_hours: float = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    if age_hours:
        old = time.time() - age_hours * 3600
        os.utime(path, (old, old))
    return path


def test_reap_deletes_old_keeps_young_and_spares_hive(tmp_path):
    live = _mk(tmp_path / "book_snapshots_500ms" / "date=2026-09-20" / "asset=BTC" / "f.parquet",
               age_hours=100)
    old = _mk(tmp_path / "_quarantine" / "book_snapshots_500ms" / "old.parquet", age_hours=100)
    young = _mk(tmp_path / "_quarantine" / "book_snapshots_500ms" / "young.parquet", age_hours=1)
    stats = reap_quarantine(tmp_path, max_age_hours=72, max_total_bytes=10**9)
    assert not old.exists()
    assert young.exists()
    assert live.exists(), "reaper must never touch the live hive"
    assert stats["files_deleted"] == 1
    assert stats["files_kept"] == 1


def test_reap_size_cap_oldest_first_within_grace(tmp_path):
    # all young (inside grace) but over a tiny cap -> oldest go first
    a = _mk(tmp_path / "_quarantine" / "a.parquet", size=500, age_hours=10)
    b = _mk(tmp_path / "_quarantine" / "b.parquet", size=500, age_hours=5)
    c = _mk(tmp_path / "_quarantine" / "c.parquet", size=500, age_hours=1)
    stats = reap_quarantine(tmp_path, max_age_hours=72, max_total_bytes=1000)
    assert not a.exists(), "oldest must go first over cap"
    assert b.exists() and c.exists(), "1500-500=1000 <= cap stops the cap pass"
    assert stats["bytes_kept"] <= 1000


def test_reap_dry_run_deletes_nothing(tmp_path):
    old = _mk(tmp_path / "_quarantine" / "old.parquet", age_hours=200)
    stats = reap_quarantine(tmp_path, max_age_hours=72, dry_run=True)
    assert old.exists()
    assert stats["files_deleted"] == 1, "dry-run still reports would-delete"


def test_reap_missing_quarantine_is_noop(tmp_path):
    stats = reap_quarantine(tmp_path / "nope")
    assert stats == {"files_deleted": 0, "bytes_deleted": 0, "files_kept": 0, "bytes_kept": 0}


def test_cleanup_reaps_even_when_prune_early_returns(tmp_path):
    # No verified upload anywhere -> prune returns {} early, but the reaper
    # must still run (its age/cap policy is independent of checkpoints).
    old = _mk(tmp_path / "_quarantine" / "book_events" / "stale.parquet", age_hours=100)
    out = cleanup_local_data(tmp_path, rolling_window=True, retention_hours=6,
                             quarantine_retention_hours=72,
                             quarantine_max_bytes=10**9)
    assert out == {}
    assert not old.exists(), "reap must run even when prune early-returns"


def test_cleanup_reap_disabled_when_asked(tmp_path):
    old = _mk(tmp_path / "_quarantine" / "stale.parquet", age_hours=100)
    cleanup_local_data(tmp_path, rolling_window=False, reap_quarantine=False)
    assert old.exists()


def test_retention_cadence_check_flags_incident_math():
    cfg = CollectorConfig(timeframes=["5m", "15m", "1h", "4h", "1d"])
    cfg.kaggle.rolling_window = True
    cfg.kaggle.upload_interval_seconds = 3600
    cfg.kaggle.local_retention_hours = 6
    msg = retention_cadence_check(cfg)
    assert msg is not None and "UNDERPROVISIONED" in msg


def test_retention_cadence_check_quiet_when_fitting():
    cfg = CollectorConfig(timeframes=["5m"])
    cfg.kaggle.rolling_window = True
    assert retention_cadence_check(cfg) is None
    cfg2 = CollectorConfig(timeframes=["5m", "15m", "1h", "4h", "1d"])
    cfg2.kaggle.rolling_window = False  # cumulative mode never prunes
    assert retention_cadence_check(cfg2) is None
    cfg3 = CollectorConfig(timeframes=["5m", "15m"])
    cfg3.kaggle.rolling_window = True
    cfg3.kaggle.upload_interval_seconds = 3600
    cfg3.kaggle.local_retention_hours = 48
    assert retention_cadence_check(cfg3) is None
