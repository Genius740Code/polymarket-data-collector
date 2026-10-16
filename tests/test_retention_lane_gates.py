"""Retention-side lane gating (2026-10-16): the retention leeway, the
emergency zero-leeway prune and the quarantine-reap gate are all scoped
PER OWNING LANE after the prune-independence fix.

- emergency prune (retention_hours=0, the disk-guard path in
  collector.py `_run_emergency_prune`): a fresh 5m checkpoint frees 5m-
  owned rows immediately while the stale 1h lane keeps its own — the
  2026-09-24 "emergency prune done: 0 files" death spiral loses its
  global veto;
- the market-end cutoff is each file's OWNING lanes' own
  ``min(now, verified_upload) - retention`` — two files of the same age
  can live in different leeways;
- the quarantine reap stays fail-closed (no verified upload on ANY lane
  -> no reap) but no longer waits for EVERY lane: one verified lane is
  enough, because quarantined rows were already proven uploaded when
  they were moved (or are unreadable review stubs).

Each "per-lane relief" test FAILS on the pre-2026-10-16 code (global min
checkpoint + missing-lane veto), verified via git-stash run.
"""

import json
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import polymarket_collector.config as _CFG
from polymarket_collector.storage.export import cleanup_local_data

H = 3600 * 1000
D = 24 * H
DS = {"5m": "gghgg1/polymarket-5m-crypto", "1h": "gghgg1/polymarket-1h-crypto"}
SNAP = "BTC_book_snapshots_500ms.parquet"


def _fake_cfg(monkeypatch, timeframes=("5m", "1h")):
    fake = type("C", (), {
        "kaggle": type("K", (), {
            "rolling_window": True, "local_retention_hours": 48,
            "datasets": {tf: DS.get(tf, f"gghgg1/polymarket-{tf}-crypto")
                         for tf in timeframes}})(),
        "timeframes": list(timeframes),
    })()
    monkeypatch.setattr(_CFG.CollectorConfig, "load",
                        classmethod(lambda cls, *a, **k: fake))
    return fake


def _markets(base: Path, rows):
    p = base / "markets_latest"
    p.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "condition_id": [r["cid"] for r in rows],
        "market_end_ts_ms": [r["end"] for r in rows],
        "series_id": [r.get("series") for r in rows],
        "window_size_seconds": [r.get("ws") for r in rows],
    }), str(p / "markets_latest.parquet"))
    return base


def _hive_file(base: Path, name, cids, mtime_ms, dataset="book_snapshots_500ms"):
    p = base / dataset / "date=x" / "asset=BTC" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"condition_id": cids}), str(p))
    os.utime(p, (mtime_ms / 1000,) * 2)
    return p


def _lane_state(base: Path, lane, upload_ms):
    sp = base / "kaggle_staging" / lane / "_kaggle_state.json"
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(json.dumps({DS[lane]: {"last_upload_unix_ms": upload_ms}}))


def _lane_staging(base: Path, lane, mtime_ms, names=(SNAP,)):
    for name in names:
        p = base / "kaggle_staging" / lane / "gghgg1" / "ds" / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"")
        os.utime(p, (mtime_ms / 1000,) * 2)


def _two_lane_hive(tmp_path, monkeypatch):
    _fake_cfg(monkeypatch)
    base = Path(tmp_path) / "data"
    now = int(time.time() * 1000)
    _markets(base, [
        {"cid": "cid-5m", "end": now - 10 * H, "series": "BTC-5m", "ws": 300},
        {"cid": "cid-1h", "end": now - 10 * H, "series": "BTC-1h", "ws": 3600},
    ])
    f5m = _hive_file(base, "five-m.parquet", ["cid-5m"], now - 20 * H)
    f1h = _hive_file(base, "one-h.parquet", ["cid-1h"], now - 20 * H)
    fmix = _hive_file(base, "mixed.parquet", ["cid-5m", "cid-1h"], now - 20 * H)
    return base, now, f5m, f1h, fmix


def test_emergency_zero_leeway_prunes_5m_rows_while_1h_stale(tmp_path, monkeypatch):
    """FAIL-on-old: disk-guard emergency prune (retention_hours=0) — 5m's
    fresh checkpoint frees 5m-owned rows NOW; the 27h-stale 1h lane keeps
    its own rows and the shared mixed file. The old global min checkpoint
    made every emergency prune delete 0 files until ENOSPC."""
    base, now, f5m, f1h, fmix = _two_lane_hive(tmp_path, monkeypatch)
    _lane_state(base, "5m", now)
    _lane_staging(base, "5m", now - 60_000)
    _lane_state(base, "1h", now - 27 * H)
    _lane_staging(base, "1h", now - 27 * H)
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=0,
                               reap_quarantine=False)
    assert not f5m.exists(), f"emergency prune must free 5m-owned rows: {stats}"
    assert f1h.exists(), "stale 1h keeps its own rows even at zero leeway"
    assert fmix.exists(), "mixed file needs BOTH owning lanes even at zero leeway"
    assert list(stats.keys()) == [str(f5m.relative_to(base))], stats


def test_retention_leeway_is_scoped_to_owning_lanes(tmp_path, monkeypatch):
    """FAIL-on-old: two files of the SAME market age sit in different
    leeways — the 5m-owned one (cutoff = 5m ckpt - 48h) prunes, the
    1h-owned twin (cutoff = 1h's 27h-stale ckpt - 48h) stays."""
    base, now, f5m, f1h, fmix = _two_lane_hive(tmp_path, monkeypatch)
    # markets ended 51h ago: past 5m's cutoff (2h ckpt + 48h = 50h),
    # inside 1h's cutoff (27h + 48h = 75h)
    _markets(base, [
        {"cid": "cid-5m", "end": now - 51 * H, "series": "BTC-5m", "ws": 300},
        {"cid": "cid-1h", "end": now - 51 * H, "series": "BTC-1h", "ws": 3600},
    ])
    _lane_state(base, "5m", now - 2 * H)
    _lane_staging(base, "5m", now - 2 * H - 60_000)
    _lane_state(base, "1h", now - 27 * H)
    _lane_staging(base, "1h", now - 27 * H)
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    assert not f5m.exists(), f"51h-old rows are past 5m's own cutoff: {stats}"
    assert f1h.exists(), "same-age 1h rows stay inside 1h's own leeway"
    assert fmix.exists(), "mixed file takes the SLOWEST owning lane's cutoff"


def test_quarantine_reap_needs_any_verified_lane_not_every_lane(tmp_path, monkeypatch):
    """FAIL-on-old: the reap gate follows per-lane independence — one
    verified lane (5m) is enough; a never-uploaded 1h lane no longer pins
    the review buffer. With NO verified lane at all it stays fail-closed."""
    _fake_cfg(monkeypatch)
    base = Path(tmp_path) / "data"
    old = base / "_quarantine" / "book_events" / "stale.parquet"
    old.parent.mkdir(parents=True, exist_ok=True)
    old.write_bytes(b"x" * 100)
    _os_old = time.time() - 100 * 3600
    os.utime(old, (_os_old, _os_old))

    # 1h never uploaded, 5m verified now -> reap runs (old code: the
    # missing-lane veto returned {} BEFORE the reap and pinned the buffer)
    _lane_state(base, "5m", int(time.time() * 1000))
    out = cleanup_local_data(str(base), rolling_window=True, retention_hours=6,
                             quarantine_retention_hours=72,
                             quarantine_max_bytes=10 ** 9)
    assert not old.exists(), f"reap must run once ANY lane verified: {out}"

    # NO verified upload anywhere -> fail closed, the reap must not run
    base2 = Path(tmp_path) / "data2"
    old2 = base2 / "_quarantine" / "book_events" / "stale.parquet"
    old2.parent.mkdir(parents=True, exist_ok=True)
    old2.write_bytes(b"x" * 100)
    os.utime(old2, (_os_old, _os_old))
    out2 = cleanup_local_data(str(base2), rolling_window=True, retention_hours=6,
                              quarantine_retention_hours=72,
                              quarantine_max_bytes=10 ** 9)
    assert out2 == {}, out2
    assert old2.exists(), "reap must stay fail-closed with zero verified lanes"


def test_chainlink_keeps_cross_lane_gate(tmp_path, monkeypatch):
    """Documented narrow coupling: chainlink_events rows carry no
    condition_id (lane ownership unprovable) and every lane's dataset
    ships its own copy — the old cross-lane gate STAYS for them. A stale
    1h lane keeps chainlink files even though 5m is fresh."""
    _fake_cfg(monkeypatch)
    base = Path(tmp_path) / "data"
    now = int(time.time() * 1000)
    _markets(base, [{"cid": "cid-5m", "end": now - 30 * D,
                     "series": "BTC-5m", "ws": 300}])
    clk = _hive_file(base, "clk.parquet", [], now - 20 * H, dataset="chainlink_events")
    pq.write_table(pa.table({"asset": ["BTC"],
                             "ts_received_ns": [(now - 30 * D) * 1_000_000]}), str(clk))
    os.utime(clk, ((now - 20 * H) / 1000,) * 2)
    _lane_state(base, "5m", now)
    _lane_staging(base, "5m", now - 60_000, names=(SNAP, "BTC_chainlink_events.parquet"))
    _lane_state(base, "1h", now - 27 * H)
    _lane_staging(base, "1h", now - 27 * H, names=(SNAP, "BTC_chainlink_events.parquet"))
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    assert clk.exists(), f"chainlink must keep the cross-lane gate: {stats}"
    # both lanes fresh + covering -> the cross-lane gate passes
    _lane_state(base, "1h", now)
    _lane_staging(base, "1h", now - 60_000, names=(SNAP, "BTC_chainlink_events.parquet"))
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    assert not clk.exists(), f"all lanes verified must free chainlink: {stats}"
