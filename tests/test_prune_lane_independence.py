"""Per-lane prune independence (2026-10-16): a stale lane blocks only itself.

Production evidence: the 1h lane had not verified for 27h+ (3.5h+ whale
staging builds, worker timeouts/kills) while 5m verified hourly. The old
slowest-lane gate — min(last_upload) across ALL lanes, plus a missing-lane
veto that returned {} — coupled two INDEPENDENT Kaggle datasets (separate
slugs, separate versions): 5m's post-upload prune waited on 1h, staging
grew to 18G (5m 13G + 1h 4.2G) and the disk hit 99.97%. Mirrors f3f1b77,
which shipped the same independence for UPLOAD (worker aborts fail-closed
PER LANE, never blocking the other lanes' upload).

Ownership physics: each lane's staging is series-filtered to its OWN
markets (_lane_series_mask / _read_dataset_per_asset), so only the OWNING
lane's upload ever carries a row. The prune therefore gates each hive
file on the lanes that OWN rows in it (per-condition series_id /
window_size_seconds from markets_latest — the never-pruned resolution
map, verified live: series are exactly ``BTC-5m``/``BTC-1h``):

- a fresh 5m upload prunes 5m-OWNED files while 1h stays stale-blocked;
- a stale / never-uploaded lane keeps ONLY its own rows' files (and
  mixed-lane flush files that contain them) — never the whole prune;
- the mirror holds: a fresh 1h prunes 1h-owned rows while 5m is stale;
- unattributable rows keep the conservative CROSS-LANE gate (weather
  series, disabled lanes, series/ws-free markets_latest) — the
  2026-09-11 data-loss class stays closed;
- gap evidence (collector_events / resync_episodes / markets_log /
  markets_latest) is NEVER pruned, whatever the lane states.

Every "prunes while stale" test here FAILS on the pre-2026-10-16 code
(verified via git-stash run): the old global min checkpoint / missing-lane
veto kept the file.
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


def _fake_cfg(monkeypatch, timeframes):
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
    """markets_latest with lane attribution columns (prod schema has both)."""
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


def _layout(tmp_path, monkeypatch, *, m5, m1, s5, s1, lanes=("5m", "1h")):
    """Two-lane hive: 5m-owned / 1h-owned / mixed files, all same shape.

    Files land 20h ago (newer than 1h's stale staging horizon of
    ckpt-3h, older than 5m's fresh staging horizon), markets ended 50h
    ago: past 5m's 48h leeway, inside 1h's stale-27h+48h leeway.
    """
    _fake_cfg(monkeypatch, lanes)
    base = Path(tmp_path) / "data"
    now = int(time.time() * 1000)
    _markets(base, [
        {"cid": "cid-5m", "end": now - 50 * H, "series": "BTC-5m", "ws": 300},
        {"cid": "cid-1h", "end": now - 50 * H, "series": "BTC-1h", "ws": 3600},
    ])
    f5m = _hive_file(base, "five-m.parquet", ["cid-5m"], now - 20 * H)
    f1h = _hive_file(base, "one-h.parquet", ["cid-1h"], now - 20 * H)
    fmix = _hive_file(base, "mixed.parquet", ["cid-5m", "cid-1h"], now - 20 * H)
    _lane_state(base, "5m", m5)
    _lane_staging(base, "5m", s5)
    _lane_state(base, "1h", m1)
    _lane_staging(base, "1h", s1)
    return base, f5m, f1h, fmix


def test_5m_owned_rows_prune_while_1h_stale_blocked(tmp_path, monkeypatch):
    """HEADLINE (FAIL-on-old): 5m verifies hourly, 1h is 27h stale — the 5m
    lane's OWN rows must prune on 5m's own verified upload while every file
    the 1h lane owns (or shares) stays blocked."""
    now = int(time.time() * 1000)
    base, f5m, f1h, fmix = _layout(
        tmp_path, monkeypatch,
        m5=now, s5=now - 60_000,              # 5m verified now, staging verified
        m1=now - 27 * H, s1=now - 27 * H)     # 1h 27h stale (staging == ckpt: verified)
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    assert not f5m.exists(), f"5m-owned file must prune on 5m's own gate: {stats}"
    assert f1h.exists(), f"stale 1h lane must still block its OWN rows: {stats}"
    assert fmix.exists(), f"mixed flush file needs BOTH owning lanes: {stats}"
    assert list(stats.keys()) == [str(f5m.relative_to(base))], stats


def test_missing_1h_upload_blocks_only_1h_rows(tmp_path, monkeypatch):
    """FAIL-on-old: a NEVER-uploaded lane no longer vetoes the whole prune —
    it keeps only its own rows' files (2026-09-22 veto class, scoped)."""
    now = int(time.time() * 1000)
    _fake_cfg(monkeypatch, ("5m", "1h"))
    base = Path(tmp_path) / "data"
    _markets(base, [
        {"cid": "cid-5m", "end": now - 30 * D, "series": "BTC-5m", "ws": 300},
        {"cid": "cid-1h", "end": now - 30 * D, "series": "BTC-1h", "ws": 3600},
    ])
    f5m = _hive_file(base, "five-m.parquet", ["cid-5m"], now - 20 * H)
    f1h = _hive_file(base, "one-h.parquet", ["cid-1h"], now - 20 * H)
    fmix = _hive_file(base, "mixed.parquet", ["cid-5m", "cid-1h"], now - 20 * H)
    _lane_state(base, "5m", now)
    _lane_staging(base, "5m", now - 60_000)
    # 1h: no state, no staging — never verified
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    assert not f5m.exists(), f"5m-owned file must prune (1h missing must not veto): {stats}"
    assert f1h.exists(), "never-uploaded 1h lane keeps its own rows"
    assert fmix.exists(), "mixed file needs its never-uploaded owner too"
    assert list(stats.keys()) == [str(f5m.relative_to(base))], stats


def test_1h_prunes_on_own_upload_while_5m_stale(tmp_path, monkeypatch):
    """FAIL-on-old (mirror): independence is symmetric — 1h's fresh upload
    prunes 1h-owned rows even while 5m is 27h stale."""
    now = int(time.time() * 1000)
    base, f5m, f1h, fmix = _layout(
        tmp_path, monkeypatch,
        m5=now - 27 * H, s5=now - 27 * H,   # 5m stale
        m1=now, s1=now - 60_000)            # 1h fresh
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    assert not f1h.exists(), f"1h-owned file must prune on 1h's own gate: {stats}"
    assert f5m.exists(), "stale 5m lane must still block its OWN rows"
    assert fmix.exists(), "mixed file needs BOTH owning lanes"


def test_stale_1h_alone_blocks_only_itself(tmp_path, monkeypatch):
    """"1h alone still blocked": single-lane config, 1h stale — its rows
    stay; once 1h verifies its OWN upload, its OWN rows prune (per-lane
    strictness, no cross-lane rescue)."""
    now = int(time.time() * 1000)
    _fake_cfg(monkeypatch, ("1h",))
    base = Path(tmp_path) / "data"
    _markets(base, [{"cid": "cid-1h", "end": now - 50 * H,
                     "series": "BTC-1h", "ws": 3600}])
    f1h = _hive_file(base, "one-h.parquet", ["cid-1h"], now - 20 * H)
    _lane_state(base, "1h", now - 27 * H)
    _lane_staging(base, "1h", now - 27 * H)
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    assert f1h.exists(), f"stale lane must block its own rows: {stats}"
    # its own verified upload unblocks its own rows — nothing else changed
    _lane_state(base, "1h", now)
    _lane_staging(base, "1h", now - 60_000)
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    assert not f1h.exists(), f"fresh own-lane upload must prune its own rows: {stats}"


def test_per_lane_guards_stay_strict(tmp_path, monkeypatch):
    """Fail-closed per lane: unverified staging, unknown condition,
    unattributable series and inside-leeway markets each keep the file —
    same strictness as the old gate, scoped per owning lane."""
    now = int(time.time() * 1000)
    _fake_cfg(monkeypatch, ("5m", "1h"))
    base = Path(tmp_path) / "data"
    _markets(base, [
        {"cid": "cid-5m", "end": now - 30 * D, "series": "BTC-5m", "ws": 300},
        {"cid": "cid-4h", "end": now - 30 * D, "series": "BTC-4h", "ws": 14400},
        {"cid": "cid-wx", "end": now - 30 * D, "series": "WEATHER-LOW-1D", "ws": None},
        {"cid": "cid-soon", "end": now - 24 * H, "series": "BTC-5m", "ws": 300},
    ])
    # 5m VERIFIED-UPLOADED an hour ago but its staging was REBUILT since
    # (failed upload) — unverified staging must not prove coverage.
    _lane_state(base, "5m", now - H)
    _lane_staging(base, "5m", now)
    _lane_state(base, "1h", now)
    _lane_staging(base, "1h", now - 60_000)
    f_unverified = _hive_file(base, "unverified.parquet", ["cid-5m"], now - 20 * H)
    f_ghost = _hive_file(base, "ghost.parquet", ["cid-ghost"], now - 20 * H)
    f_disabled = _hive_file(base, "disabled.parquet", ["cid-4h"], now - 20 * H)
    f_weather = _hive_file(base, "weather.parquet", ["cid-wx"], now - 20 * H)
    f_leeway = _hive_file(base, "leeway.parquet", ["cid-soon"], now - 20 * H)
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48,
                               reap_quarantine=False)
    for f in (f_unverified, f_ghost, f_disabled, f_weather, f_leeway):
        assert f.exists(), f"{f.name} must stay: strict per-lane guard {stats}"


def test_gap_evidence_never_pruned_under_lane_independence(tmp_path, monkeypatch):
    """Gap evidence (collector_events / resync_episodes / markets_log /
    markets_latest) is NEVER pruned — no lane state can ever change that,
    and l2-retention refuses the same datasets outright."""
    from polymarket_collector.storage.l2_retention import (
        GAP_EVIDENCE_DATASETS,
        dataset_allowed,
        quarantine_partition,
    )

    now = int(time.time() * 1000)
    _fake_cfg(monkeypatch, ("5m", "1h"))
    base = Path(tmp_path) / "data"
    _markets(base, [
        {"cid": "cid-5m", "end": now - 30 * D, "series": "BTC-5m", "ws": 300},
        {"cid": "cid-1h", "end": now - 30 * D, "series": "BTC-1h", "ws": 3600},
    ])
    f5m = _hive_file(base, "five-m.parquet", ["cid-5m"], now - 20 * H)
    gap = {}
    for ds in ("collector_events", "resync_episodes", "markets_log"):
        gap[ds] = _hive_file(base, "gap.parquet", ["cid-5m"], now - 20 * H, dataset=ds)
    ml_rows = pq.read_metadata(str(base / "markets_latest" / "markets_latest.parquet")).num_rows
    _lane_state(base, "5m", now)
    _lane_staging(base, "5m", now - 60_000)
    _lane_state(base, "1h", now - 27 * H)
    _lane_staging(base, "1h", now - 27 * H)
    # emergency zero-leeway prune — the most aggressive mode
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=0,
                               reap_quarantine=False)
    assert not f5m.exists(), f"eligible 5m-owned file should prune: {stats}"
    for ds, f in gap.items():
        assert f.exists(), f"gap evidence {ds} must NEVER prune"
        assert not (base / "_quarantine" / ds).exists()
        assert ds not in stats and all(ds not in k for k in stats)
        assert dataset_allowed(ds) is False
        assert quarantine_partition(base, ds, "date=x")["moved"] is False
    assert pq.read_metadata(str(base / "markets_latest" / "markets_latest.parquet")).num_rows == ml_rows
    assert set(GAP_EVIDENCE_DATASETS) == {"collector_events", "resync_episodes",
                                          "markets_log", "markets_latest"}
