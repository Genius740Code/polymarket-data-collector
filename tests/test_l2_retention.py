"""l2_raw time-partitioned retention (move-to-quarantine, default disabled).

Fixture filesystems only (tmp_path) — never the live hive. Covers:
- disabled-by-default (unset / zero / negative retention moves nothing);
- eligible partition moves with a manifest row (partition, rows, sha, moved_ts);
- each guard blocks independently (age, market-end, unknown-cid, checkpoint);
- failed move keeps the source;
- gap-evidence datasets rejected;
- reap bounded + oldest-first + off-by-default (only delete in the feature).
"""

import datetime as _dt
import json as _json

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.config import CollectorConfig
from polymarket_collector.storage import l2_retention as lr

NOW = int(_dt.datetime(2026, 10, 6, tzinfo=_dt.timezone.utc).timestamp() * 1000)
RETENTION_DAYS = 7
CUTOFF = NOW - RETENTION_DAYS * 86_400_000  # 2026-09-29T00:00Z

OLD_DAY = "2026-09-01"
YOUNG_DAY = "2026-10-05"
CID_OLD = "0x" + "aa" * 32
CID_LIVE = "0x" + "bb" * 32
CID_UNKNOWN = "0x" + "cc" * 32
END_OLD = int(_dt.datetime(2026, 9, 2, tzinfo=_dt.timezone.utc).timestamp() * 1000)
END_LIVE = NOW + 86_400_000


def _write(path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path), compression="zstd")


def _l2_row(cid, ts_ms):
    return {"condition_id": cid, "ts_source": ts_ms, "asset": "BTC",
            "event_type": "book", "frame_json": "{}"}


def _hive_l2_old(base, day=OLD_DAY, cid=CID_OLD, n=3):
    t0 = int(_dt.datetime(2026, 9, 1, tzinfo=_dt.timezone.utc).timestamp() * 1000)
    _write(base / "l2_raw" / f"date={day}" / "asset=BTC" / "l2.parquet",
           [_l2_row(cid, t0 + i) for i in range(n)])
    return base


def _markets(base, rows):
    _write(base / "markets_latest" / "markets_latest.parquet", rows)
    return base


def _ended_markets(base):
    return _markets(base, [{"condition_id": CID_OLD, "market_end_ts_ms": END_OLD}])


# 1. disabled by default -------------------------------------------------------

def test_disabled_by_default_nothing_moves(tmp_path):
    _ended_markets(_hive_l2_old(tmp_path))
    for bad in (None, 0, -3):
        out = lr.apply_l2_retention(tmp_path, retention_days=bad, now_ms=NOW,
                                     checkpoint_ms=NOW)
        assert out["disabled"] is True and out["moved"] == []
    assert (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists()
    assert not (tmp_path / "_quarantine").exists()


def test_config_defaults_disabled():
    cfg = CollectorConfig()
    assert cfg.storage.l2_retention_days is None
    assert cfg.storage.l2_quarantine_reap_enabled is False


# 2. eligible partition moves with manifest ------------------------------------

def test_eligible_partition_moves_with_manifest(tmp_path):
    _ended_markets(_hive_l2_old(tmp_path, n=4))
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=NOW)
    assert [m["partition"] for m in out["moved"]] == [f"date={OLD_DAY}"]
    assert not (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists()
    dest = tmp_path / "_quarantine" / "l2_raw" / f"date={OLD_DAY}"
    assert (dest / "asset=BTC" / "l2.parquet").exists(), "move preserves layout"
    man = pq.read_table(str(tmp_path / "_quarantine" / "l2_raw" / lr.MANIFEST_NAME)).to_pylist()
    assert len(man) == 1
    row = man[0]
    assert set(row) == {"partition", "rows", "sha", "moved_ts"}
    assert row["partition"] == f"date={OLD_DAY}"
    assert row["rows"] == 4
    assert len(row["sha"]) == 40 and row["moved_ts"] > 0


def test_second_partition_appends_manifest(tmp_path):
    _ended_markets(_hive_l2_old(tmp_path))
    _hive_l2_old(tmp_path, day="2026-09-02")
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=NOW)
    assert len(out["moved"]) == 2
    man = pq.read_table(str(tmp_path / "_quarantine" / "l2_raw" / lr.MANIFEST_NAME)).to_pylist()
    assert sorted(r["partition"] for r in man) == ["date=2026-09-01", "date=2026-09-02"]


# 3. guards block independently -------------------------------------------------

def test_young_partition_stays(tmp_path):
    _ended_markets(_hive_l2_old(tmp_path, day=YOUNG_DAY))
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=NOW)
    assert out["moved"] == []
    assert f"date={YOUNG_DAY}" in out["kept"]
    assert (tmp_path / "l2_raw" / f"date={YOUNG_DAY}").exists()


def test_live_market_stays(tmp_path):
    _markets(_hive_l2_old(tmp_path, cid=CID_LIVE),
             [{"condition_id": CID_LIVE, "market_end_ts_ms": END_LIVE}])
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=NOW)
    assert out["moved"] == []
    assert "market-end" in out["kept"][f"date={OLD_DAY}"] or "leeway" in out["kept"][f"date={OLD_DAY}"]
    assert (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists()


def test_unknown_condition_stays(tmp_path):
    _markets(_hive_l2_old(tmp_path, cid=CID_UNKNOWN),
             [{"condition_id": CID_OLD, "market_end_ts_ms": END_OLD}])
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=NOW)
    assert out["moved"] == []
    assert "unknown" in out["kept"][f"date={OLD_DAY}"].lower()
    assert (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists()


def test_missing_checkpoint_stays(tmp_path):
    # no kaggle state anywhere + no explicit checkpoint -> fail closed
    _ended_markets(_hive_l2_old(tmp_path))
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, lanes=["5m"])
    assert out["moved"] == [] and out.get("checkpoint_ms") is None
    assert (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists()


def test_stale_checkpoint_stays(tmp_path):
    _ended_markets(_hive_l2_old(tmp_path))
    stale = END_OLD - 3_600_000  # checkpoint an hour before the partition end
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=stale)
    assert out["moved"] == []
    assert "checkpoint" in out["kept"][f"date={OLD_DAY}"]
    assert (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists()


def test_slowest_lane_gates(tmp_path):
    # fast lane uploaded now, slow lane only up to Sep 2 -> partition end
    # (Sep 1 23:59) is past the slow checkpoint? No: Sep1 < Sep2 passes, so
    # use a slow checkpoint BEFORE the partition end to prove gating.
    _ended_markets(_hive_l2_old(tmp_path))
    for lane, ckpt in (("5m", NOW), ("1h", END_OLD - 10_000)):
        sdir = tmp_path / "kaggle_staging" / lane
        sdir.mkdir(parents=True, exist_ok=True)
        (sdir / "_kaggle_state.json").write_text(_json.dumps(
            {"gghgg1/polymarket-5m-crypto": {"last_upload_unix_ms": ckpt}}))
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, lanes=["5m", "1h"])
    assert out["moved"] == [], "slowest lane must gate the move"


def test_dry_run_moves_nothing(tmp_path):
    _ended_markets(_hive_l2_old(tmp_path))
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=NOW, dry_run=True)
    assert out["moved"] == []
    assert (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists()
    assert not (tmp_path / "_quarantine").exists()


# 4. failed move keeps source ----------------------------------------------------

def test_failed_move_keeps_source(tmp_path):
    _ended_markets(_hive_l2_old(tmp_path))
    dest = tmp_path / "_quarantine" / "l2_raw" / f"date={OLD_DAY}"
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "sentinel").write_text("already there")
    res = lr.quarantine_partition(tmp_path, "l2_raw", f"date={OLD_DAY}")
    assert res["moved"] is False
    assert (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists(), "source kept on failed move"
    assert (dest / "sentinel").exists()


# 5. gap-evidence datasets rejected ----------------------------------------------

def test_gap_evidence_datasets_rejected(tmp_path):
    for ds in ("collector_events", "resync_episodes", "markets_log", "markets_latest"):
        assert lr.dataset_allowed(ds) is False
        d = tmp_path / ds / "date=2026-09-01"
        d.mkdir(parents=True, exist_ok=True)
        (d / "f.parquet").write_bytes(b"x")
        res = lr.quarantine_partition(tmp_path, ds, "date=2026-09-01")
        assert res["moved"] is False and "refused" in res["reason"]
        assert d.exists(), f"{ds} must never be touched"
    assert lr.dataset_allowed("l2_raw") is True
    assert not (tmp_path / "_quarantine").exists()


# 6. reap: bounded + oldest-first + off-by-default --------------------------------

def _seed_quarantine(base, days=("2026-09-01", "2026-09-02", "2026-09-03")):
    for day in days:
        d = base / "_quarantine" / "l2_raw" / f"date={day}" / "asset=BTC"
        d.mkdir(parents=True, exist_ok=True)
        (d / "l2.parquet").write_bytes(b"y" * 64)
    live = base / "l2_raw" / f"date={YOUNG_DAY}" / "asset=BTC"
    live.mkdir(parents=True, exist_ok=True)
    (live / "live.parquet").write_bytes(b"z" * 32)
    return base


def test_reap_off_by_default(tmp_path):
    _seed_quarantine(tmp_path)
    stats = lr.reap_l2_quarantine(tmp_path)
    assert stats["deleted"] == 0
    assert (tmp_path / "_quarantine" / "l2_raw" / "date=2026-09-01").exists()


def test_reap_no_pressure_deletes_nothing(tmp_path):
    _seed_quarantine(tmp_path)
    stats = lr.reap_l2_quarantine(tmp_path, enabled=True, min_free_bytes=0)
    assert stats["deleted"] == 0, "free >= 0 means no pressure — keep all"
    assert (tmp_path / "_quarantine" / "l2_raw" / "date=2026-09-01").exists()


def test_reap_bounded_oldest_first(tmp_path):
    _seed_quarantine(tmp_path)
    stats = lr.reap_l2_quarantine(tmp_path, enabled=True,
                                  min_free_bytes=10 ** 18,
                                  max_deletes_per_cycle=1)
    assert stats["deleted"] == 1
    assert not (tmp_path / "_quarantine" / "l2_raw" / "date=2026-09-01").exists()
    assert (tmp_path / "_quarantine" / "l2_raw" / "date=2026-09-02").exists()
    assert (tmp_path / "_quarantine" / "l2_raw" / "date=2026-09-03").exists()
    # live hive untouched by the only deleter
    assert (tmp_path / "l2_raw" / f"date={YOUNG_DAY}" / "asset=BTC" / "live.parquet").exists()


def test_reap_dry_run_deletes_nothing(tmp_path):
    _seed_quarantine(tmp_path)
    stats = lr.reap_l2_quarantine(tmp_path, enabled=True,
                                  min_free_bytes=10 ** 18,
                                  max_deletes_per_cycle=10, dry_run=True)
    assert stats["would_delete"], "dry-run still reports"
    assert (tmp_path / "_quarantine" / "l2_raw" / "date=2026-09-01").exists()


# 7. collector disk-guard wiring (2026-10-16 operator enablement) ---------------

def _guard_collector(tmp_path, **storage_over):
    """Bare Collector (no __init__/sockets), stubbed LOW-DISK writer.

    _disk_guard_tick only touches config.storage.*, writer.check_disk_space
    and (on tmp_path) the reap/staging helpers — safe without a live hive.
    """
    from types import SimpleNamespace

    from polymarket_collector.collector import Collector

    cfg = CollectorConfig()
    cfg.storage.data_dir = str(tmp_path)
    for k, v in storage_over.items():
        setattr(cfg.storage, k, v)
    c = Collector.__new__(Collector)
    c.config = cfg
    c.writer = SimpleNamespace(
        check_disk_space=lambda min_bytes: {"free_bytes": 100,
                                            "min_bytes": min_bytes,
                                            "data_dir": str(tmp_path)})
    c._last_disk_check_monotonic = 0.0
    return c


def test_disk_guard_wires_apply_then_reap(tmp_path, monkeypatch):
    """Low disk + keys set -> apply runs, THEN the bounded reap, in order."""
    import polymarket_collector.collector as coll

    calls: list = []

    def _fake_apply(d, **kw):
        calls.append(("apply", kw))
        return {"moved": [{"partition": "date=x"}], "kept": {}}

    def _fake_reap(d, **kw):
        calls.append(("reap", kw))
        return {"deleted": 1, "bytes_deleted": 4096, "kept": 0}

    monkeypatch.setattr(coll, "apply_l2_retention", _fake_apply)
    monkeypatch.setattr(coll, "reap_l2_quarantine", _fake_reap)
    c = _guard_collector(tmp_path, l2_retention_days=3,
                         l2_quarantine_reap_enabled=True,
                         l2_quarantine_max_deletes_per_cycle=1)
    c._disk_guard_tick()
    assert [name for name, _ in calls] == ["apply", "reap"], \
        "apply must stage partitions before the reap frees them"
    assert calls[0][1].get("retention_days") == 3
    reap_kw = calls[1][1]
    assert reap_kw.get("enabled") is True
    assert reap_kw.get("max_deletes_per_cycle") == 1, "bounded: 1 delete/cycle"
    assert reap_kw.get("min_free_bytes") == c.config.storage.disk_space_min_bytes


def test_disk_guard_l2_skipped_when_keys_absent(tmp_path, monkeypatch):
    """Merge default (keys absent) = zero behavior change: no l2 calls."""
    import polymarket_collector.collector as coll

    calls: list = []
    monkeypatch.setattr(coll, "apply_l2_retention",
                        lambda d, **kw: calls.append(("apply", kw)) or {"moved": []})
    monkeypatch.setattr(coll, "reap_l2_quarantine",
                        lambda d, **kw: calls.append(("reap", kw)) or {})
    c = _guard_collector(tmp_path)  # defaults: days=None, reap_enabled=False
    c._disk_guard_tick()
    assert calls == [], "l2 retention must stay dormant when keys are absent"


def test_disk_guard_l2_skipped_without_disk_pressure(tmp_path, monkeypatch):
    """Healthy disk -> guard returns before the l2 block (pressure-gated)."""
    import polymarket_collector.collector as coll

    calls: list = []
    monkeypatch.setattr(coll, "apply_l2_retention",
                        lambda d, **kw: calls.append(("apply", kw)) or {"moved": []})
    monkeypatch.setattr(coll, "reap_l2_quarantine",
                        lambda d, **kw: calls.append(("reap", kw)) or {})
    c = _guard_collector(tmp_path, l2_retention_days=3,
                         l2_quarantine_reap_enabled=True)
    c.writer.check_disk_space = lambda min_bytes: None  # no alert
    c._disk_guard_tick()
    assert calls == [], "no disk pressure -> no l2 retention calls"


def test_disk_guard_l2_never_raises(tmp_path, monkeypatch):
    """A raising l2 helper must not break the guard tick."""
    import polymarket_collector.collector as coll

    def _boom(d, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(coll, "apply_l2_retention", _boom)
    monkeypatch.setattr(coll, "reap_l2_quarantine", _boom)
    c = _guard_collector(tmp_path, l2_retention_days=3,
                         l2_quarantine_reap_enabled=True)
    c._disk_guard_tick()  # must not raise


# 8. shipped collector.yaml carries the operator keys -----------------------------

def test_shipped_yaml_operator_keys():
    from pathlib import Path

    yml = Path(__file__).resolve().parents[1] / "config" / "collector.yaml"
    cfg = CollectorConfig.from_yaml(yml)
    assert cfg.storage.l2_retention_days == 3
    assert cfg.storage.l2_quarantine_reap_enabled is True
    assert cfg.storage.l2_quarantine_max_deletes_per_cycle == 1


# 9. bounded per-tick enumeration (2026-10-16 100%-disk hang fix) ---------------
# Old path rglob+parquet-read EVERY file before deciding eligibility —
# O(files) on the saturated 56G hive never returned (apply blocked >100s,
# timeout-killed). Eligibility now comes from directory names via one
# single-level scandir; per-file work is capped per tick and spills over.

def _hive_l2_n_files(base, day=OLD_DAY, cid=CID_OLD, n_files=8, rows=1):
    """Many-file partition: n_files real parquet files under asset=BTC."""
    t0 = int(_dt.datetime(2026, 9, 1, tzinfo=_dt.timezone.utc).timestamp() * 1000)
    d = base / "l2_raw" / f"date={day}" / "asset=BTC"
    for i in range(n_files):
        _write(d / f"part-{i:05d}.parquet",
               [_l2_row(cid, t0 + i * 500) for _ in range(rows)])
    return base


def test_2000_file_partition_decides_under_5s(tmp_path):
    """A 2000-file partition must decide (and stay) in one bounded tick."""
    import time as _time

    _ended_markets(_hive_l2_n_files(tmp_path, n_files=2000))
    cap = lr.SCAN_FILES_PER_PARTITION_TICK
    t0 = _time.monotonic()
    out = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=NOW)
    wall = _time.monotonic() - t0
    assert wall < 5.0, f"decision path walked files: {wall:.2f}s for 2000 files"
    assert out["moved"] == [], "2000 files > per-tick cap — spilled, not moved"
    reason = out["kept"][f"date={OLD_DAY}"]
    assert f"{cap}/2000 files this tick" in reason, reason
    assert (tmp_path / "l2_raw" / f"date={OLD_DAY}").exists()
    out2 = lr.apply_l2_retention(tmp_path, retention_days=RETENTION_DAYS,
                                 now_ms=NOW, checkpoint_ms=NOW)
    assert f"{2 * cap}/2000 files this tick" in out2["kept"][f"date={OLD_DAY}"], \
        "scan must resume forward across ticks, not restart from zero"


def test_bounded_scan_matches_one_pass_rows_sha_cids(tmp_path):
    """Bounded scan must reproduce the one-pass helpers exactly."""
    base = _ended_markets(_hive_l2_old(tmp_path, n=4))
    t0 = int(_dt.datetime(2026, 9, 1, tzinfo=_dt.timezone.utc).timestamp() * 1000)
    _write(base / "l2_raw" / f"date={OLD_DAY}" / "asset=ETH" / "l2.parquet",
           [_l2_row(CID_OLD, t0 + 999)])
    src = base / "l2_raw" / f"date={OLD_DAY}"
    exp_rows, exp_sha = lr._partition_rows_and_sha(src, base)  # one-pass reference
    scan = lr._scan_partition_files_bounded(
        src, base, file_cap=lr.SCAN_FILES_PER_PARTITION_TICK)
    assert scan["done"] and not scan["unreadable"]
    assert scan["cids"] == lr._partition_condition_ids(src)
    assert (scan["rows"], scan["sha"]) == (exp_rows, exp_sha)
    assert exp_rows == 5


def test_manifest_identical_to_full_scan(tmp_path):
    """Manifest row from the bounded path == the one-pass (rows, sha)."""
    base = _ended_markets(_hive_l2_old(tmp_path, n=4))
    t0 = int(_dt.datetime(2026, 9, 1, tzinfo=_dt.timezone.utc).timestamp() * 1000)
    _write(base / "l2_raw" / f"date={OLD_DAY}" / "asset=ETH" / "l2.parquet",
           [_l2_row(CID_OLD, t0 + 999)])
    src = base / "l2_raw" / f"date={OLD_DAY}"
    exp_rows, exp_sha = lr._partition_rows_and_sha(src, base)
    out = lr.apply_l2_retention(base, retention_days=RETENTION_DAYS,
                                now_ms=NOW, checkpoint_ms=NOW)
    assert [m["partition"] for m in out["moved"]] == [f"date={OLD_DAY}"]
    man = pq.read_table(str(base / "_quarantine" / "l2_raw" / lr.MANIFEST_NAME)).to_pylist()
    assert len(man) == 1
    assert man[0]["rows"] == exp_rows and man[0]["sha"] == exp_sha


def test_spilled_scan_resumes_next_tick(tmp_path):
    """cap < file count: tick 1 spills with progress logged, tick 2 moves."""
    base = _ended_markets(_hive_l2_n_files(tmp_path, n_files=3))
    src = base / "l2_raw" / f"date={OLD_DAY}"
    exp_rows, exp_sha = lr._partition_rows_and_sha(src, base)
    out1 = lr.apply_l2_retention(base, retention_days=RETENTION_DAYS,
                                 now_ms=NOW, checkpoint_ms=NOW, scan_file_cap=2)
    assert out1["moved"] == []
    assert "2/3 files this tick" in out1["kept"][f"date={OLD_DAY}"]
    assert src.exists(), "spilled partition stays in the live hive"
    out2 = lr.apply_l2_retention(base, retention_days=RETENTION_DAYS,
                                 now_ms=NOW, checkpoint_ms=NOW, scan_file_cap=2)
    assert [m["partition"] for m in out2["moved"]] == [f"date={OLD_DAY}"], out2["kept"]
    man = pq.read_table(str(base / "_quarantine" / "l2_raw" / lr.MANIFEST_NAME)).to_pylist()
    assert man[0]["rows"] == exp_rows and man[0]["sha"] == exp_sha, \
        "resumed scan must reproduce the one-pass manifest exactly"
    assert not src.exists()
    assert str(src) not in lr._SCAN_PROGRESS, "moved partition drops its scan state"
