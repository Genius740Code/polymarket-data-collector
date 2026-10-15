"""Worker-timeout regression: 1h book_snapshots builds must complete.

2026-10-07: per-asset book_snapshots_500ms workers burned the full 900s
spawn budget and failed closed on a loaded box (thousands of tiny flush
files, 842-wide hive rows vs ~122 staging columns, full-width decode).
The build now reads column-projected batches one date partition at a time
(gc/trim between partitions), logs progress every ~60s, and runs under an
1800s spawn budget (cap kept as a backstop, fail-closed kept).
"""

import inspect
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage.export import (
    EXPORT_WORKER_TIMEOUT_MIN_AVAIL_MB,
    _build_in_subprocess,
    _date_partition_of,
    _get_schema,
    _group_paths_by_date,
    _load_market_id_map,
    _projected_read_columns,
    _stream_export_asset_dataset,
    _worker_timeout_s,
)


def test_date_partition_of() -> None:
    assert _date_partition_of(Path("data/book_snapshots_500ms/date=2026-10-07/asset=BTC/f.parquet")) == "2026-10-07"
    assert _date_partition_of(Path("data/markets_latest/markets_latest.parquet")) == ""
    assert _date_partition_of(Path("f.parquet")) == ""


def test_group_paths_by_date_preserves_order() -> None:
    files = [Path(f"data/d/date={d}/asset=BTC/{n}.parquet")
             for d, n in [("2026-10-06", "b"), ("2026-10-07", "a"),
                           ("2026-10-06", "c"), ("2026-10-07", "d")]]
    groups = _group_paths_by_date(files)
    assert [g[0] for g in groups] == ["2026-10-06", "2026-10-07"]
    assert [p.name for p in groups[0][1]] == ["b.parquet", "c.parquet"]
    assert [p.name for p in groups[1][1]] == ["a.parquet", "d.parquet"]
    assert _group_paths_by_date([]) == []


def test_projected_read_columns() -> None:
    schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
    file_cols = ["a", "b", "wide_1", "wide_2", "series_id"]
    got = _projected_read_columns(file_cols, schema, ["series_id", "asset"])
    assert got == ["a", "b", "series_id"]  # schema order first, then extras; wide cols dropped
    # schema evolution: file predates a schema column -> absent (writer null-fills)
    got2 = _projected_read_columns(["a", "series_id"], schema, ["series_id"])
    assert got2 == ["a", "series_id"]
    assert _projected_read_columns([], schema, ["series_id"]) is None
    assert _projected_read_columns(None, schema, ["series_id"]) is None


def test_worker_spawn_budget_kept_but_raised() -> None:
    sig = inspect.signature(_build_in_subprocess)
    assert sig.parameters["timeout_s"].default == 1800
    src = inspect.getsource(_build_in_subprocess)
    assert "timeout=timeout_s" in src  # budget still enforced, never removed


def test_timeout_expired_fails_closed_keeps_prior(tmp_path, capsys, monkeypatch) -> None:
    """P4 pin (2026-10-12): a TimeoutExpired spawn returns None and the caller
    keeps prior staging — the scope-skip fix must not touch this contract.
    The worker's captured stdout tail (progress lines proving steady work,
    not a hang) rides along for the log."""
    import subprocess as _sp

    def _timeout(*a, **k):
        raise _sp.TimeoutExpired(
            cmd="worker", timeout=1800,
            output="[export:worker] progress BTC/book_snapshots_500ms: 1461/1754 files "
                   "date=2026-10-11 rows=932000 elapsed=1761s")

    monkeypatch.setattr(_sp, "run", _timeout)
    base = Path(tmp_path) / "data"
    res = _build_in_subprocess(base, tmp_path, ["book_snapshots_500ms"], ["BTC"],
                               "5m", 10, False, True)
    assert res is None  # fail closed: None -> caller keeps prior staging
    logged = capsys.readouterr().out
    assert "[export:worker] spawn failed" in logged
    assert "TimeoutExpired" in logged
    assert "keeps prior staging (fail closed)" in logged
    assert "1461/1754" in logged  # the worker stdout tail is not swallowed


def _real_hive():
    return Path(__file__).resolve().parents[1] / "data" / "book_snapshots_500ms"


def test_stream_snapshot_completes_on_real_files(tmp_path) -> None:
    """Bounded real-data smoke: 3 true hive files (842-wide rows) -> staging.

    Skipped when data/ is absent. Asserts identical row content (exact key
    sets) for the 1h lane, lane purity, schema, stats accounting, and no
    tmp litter — no fabricated prices anywhere.
    """
    hive = _real_hive()
    src_files = sorted((hive / "date=2026-10-07" / "asset=BTC").glob("*.parquet"))[:3]
    if not src_files or not (hive.parent / "markets_latest" / "markets_latest.parquet").exists():
        pytest.skip("no real hive data present")
    mini = tmp_path / "data"
    for f in src_files:
        dst = mini / "book_snapshots_500ms" / "date=2026-10-07" / "asset=BTC" / f.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(f, dst)
    ml_src = hive.parent / "markets_latest" / "markets_latest.parquet"
    ml_dst = mini / "markets_latest" / "markets_latest.parquet"
    ml_dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ml_src, ml_dst)

    mid_map = _load_market_id_map(mini)
    io_stats: dict = {}
    out_tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    rows = _stream_export_asset_dataset(
        mini, "book_snapshots_500ms", "BTC", out_tmp, "1h", 10, mid_map,
        io_stats=io_stats)
    assert rows > 0
    assert out_tmp.exists()
    assert io_stats.get("files_ok") == 3
    assert io_stats.get("files_failed", 0) == 0

    got = pq.read_table(str(out_tmp))
    schema = _get_schema("book_snapshots_500ms", 10)
    assert got.schema.names == schema.names
    assert set(got.column("asset").to_pylist()) == {"BTC"}
    assert set(got.column("series_id").to_pylist()) == {"BTC-1h"}
    got_keys = set(zip(got.column("asset").to_pylist(),
                       got.column("condition_id").to_pylist(),
                       got.column("ts_snapshot_ns").to_pylist()))
    assert len(got_keys) == got.num_rows  # dedup exact, no double-count

    # independent key census from narrow-column reads of the same files
    want_keys = set()
    for f in src_files:
        t = pq.read_table(str(f), columns=["asset", "condition_id", "ts_snapshot_ns", "series_id"])
        for a, c, ts, s in zip(t.column("asset").to_pylist(), t.column("condition_id").to_pylist(),
                               t.column("ts_snapshot_ns").to_pylist(), t.column("series_id").to_pylist()):
            if a == "BTC" and s == "BTC-1h":
                want_keys.add((a, c, ts))
    assert got_keys == want_keys

    litter = [p for p in tmp_path.rglob("*.tmp.*") if p.is_file()]
    assert litter == []


# ------------------------------------------- 2026-10-15: scaled whale-lane budget

def _tiny_hive_1043(tmp_path):
    """DOGE-event-scale hive: 1043 one-row snapshot files + markets_latest."""
    import datetime as _dt
    import os as _os
    import sys as _sys
    import time as _time

    _sys.path.insert(0, str(Path(__file__).resolve().parent))
    from polymarket_collector.storage.schemas import snapshot_schema
    from test_streaming_export import _snap

    base = tmp_path / "data"
    d = "2026-10-10"
    day0 = (_dt.date.fromisoformat(d).toordinal() - 719163) * 86_400_000_000_000
    mtime = _time.time() - 7200
    part = base / "book_snapshots_500ms" / f"date={d}" / "asset=BTC"
    part.mkdir(parents=True, exist_ok=True)
    for f in range(1043):
        p = part / f"f{f:05d}.parquet"
        pq.write_table(pa.Table.from_pylist(
            [_snap(f, day0 + f * 500_000_000)], schema=snapshot_schema(10)), str(p))
        _os.utime(p, (mtime, mtime))
    ml = base / "markets_latest"
    ml.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"condition_id": ["0xaaa"], "market_id": ["42"]}),
                   str(ml / "markets_latest.parquet"))
    return base


def test_worker_timeout_scales_with_file_count() -> None:
    """2026-10-15 DOGE event (17:03 UTC): the carried worker processed
    904/1043 files (~1.43M rows, ~800 rows/s ≈ ~2.0s/file) and died on the
    flat 1800s TimeoutExpired -> 40-file PARTIAL fallback every cycle. The
    budget now scales with the frozen input size — files × 2.5s (measured
    ~2.0s/file + 25% margin) + 300s headroom, clamped to [1800, 3600],
    avail-guarded — so the same lane gets 2907s (linear need ~2076s,
    ~40% headroom) and bigger frozen lists ride the cap instead of timing
    out. Never below the 1800s baseline; never above it on a tight box."""
    assert _worker_timeout_s(0, avail_mb=99999) == 1800
    assert _worker_timeout_s(None, avail_mb=99999) == 1800
    assert _worker_timeout_s("garbage", avail_mb=99999) == 1800
    assert _worker_timeout_s(400, avail_mb=99999) == 1800  # 1300 -> baseline floor
    assert _worker_timeout_s(600, avail_mb=99999) == 1800  # 1800 -> baseline floor
    assert _worker_timeout_s(1043, avail_mb=99999) == 2907  # the DOGE event list
    assert _worker_timeout_s(1320, avail_mb=99999) == 3600  # 3600 -> cap
    assert _worker_timeout_s(99999, avail_mb=99999) == 3600
    # avail-guard: scaling above the baseline is only granted with memory
    # headroom — a tight box keeps the 1800s baseline and the lane still
    # fails closed into the bounded PARTIAL fallback exactly as before.
    assert _worker_timeout_s(1043, avail_mb=550) == 1800
    assert _worker_timeout_s(99999, avail_mb=550) == 1800
    assert _worker_timeout_s(99999, avail_mb=EXPORT_WORKER_TIMEOUT_MIN_AVAIL_MB) == 3600
    # small lanes keep the old budget regardless of the guard
    assert _worker_timeout_s(10, avail_mb=0) == 1800


def test_spawn_budget_scaled_at_call_site(tmp_path, monkeypatch, capsys) -> None:
    """The parent passes the scaled budget to every worker spawn, keyed on
    the pre-spawn manifest (ALL files <= cutoff, carried or not), and logs
    the derivation honestly; a memory-tight box gets the 1800s baseline
    (fail-closed fallback path unchanged)."""
    import time as _t

    import polymarket_collector.storage.export as E

    base = _tiny_hive_1043(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    captured: list = []

    def _fake_build(*a, **k):
        captured.append({"timeout_s": k.get("timeout_s"),
                         "skip_watermark": k.get("skip_watermark")})
        return {}  # no __manifest__ -> coverage fails closed; nothing published

    monkeypatch.setattr(E, "_build_in_subprocess", _fake_build)
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=["book_snapshots_500ms"], rolling_window=True,
        cutoff_ts=_t.time() + 300)
    assert captured, "worker spawn must run"
    assert captured[0]["timeout_s"] == 2907  # keyed on the full manifest n
    logged = capsys.readouterr().out
    assert "worker budget 2907s" in logged
    assert "files=1043" in logged
    assert "2.5s" in logged  # the measured per-file rate is in the derivation

    # memory-tight box: the avail-guard keeps the baseline. Cycle 1 published
    # nothing, so the spawn runs again through the same path.
    monkeypatch.setattr(E, "_avail_mb", lambda: 550)
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=["book_snapshots_500ms"], rolling_window=True,
        cutoff_ts=_t.time() + 300)
    assert captured[-1]["timeout_s"] == 1800
    assert len(captured) == 2
