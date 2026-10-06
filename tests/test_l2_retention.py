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
