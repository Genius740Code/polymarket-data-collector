"""Spawn-path regression: full-exception logging + bounded in-process fallback.

2026-10-11 audit (box-killer): persistent ``[export:worker] spawn failed``
lines rotated out of the pm2 log with only ``{e}`` printed — no traceback —
so the spawn killer (fork ENOMEM vs EMFILE vs worker timeout) stayed
unnameable, and because the caller fails closed the staging froze on prior
files until spawns healed. export.py now (a) logs the FULL traceback plus
parent context (pid/rss/MemAvailable/open-fds) at the spawn site, and (b)
falls back to a file-capped in-process build for the local-only whale lanes
only. These tests pin, on tmp fixtures only (no real-hive scans):

- real spawns still succeed with prod-shaped args (payload via temp file,
  module-level worker entry, repo-root cwd, cutoff honored);
- the spawn-failure log carries the FULL untruncated exception text, the
  traceback, the context block, and the honest keep-prior wording (the old
  ``caller falls back in-process`` text was stale and is gone);
- the bounded fallback stays fail-closed: never on worker-reported error
  dicts (the worker ran and aborted itself), never below the memory gate,
  never for trades/chainlink (network/concat must not run in-process),
  never on unreadable inputs, and never past the cumulative monotonic
  guard; PARTIAL cap staging logs scanned/total honestly and is never
  checkpoint-marked (next cycle retries the full worker build).
"""

import os
import subprocess
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import sys as _sys

_sys.path.insert(0, str(Path(__file__).resolve().parent))

import polymarket_collector.storage.export as E
from polymarket_collector.storage.export import (
    EXPORT_FALLBACK_FILE_CAP,
    EXPORT_FALLBACK_MIN_AVAIL_MB,
    _build_in_subprocess,
    _stream_export_asset_dataset,
)
from polymarket_collector.storage.schemas import snapshot_schema
from test_streaming_export import _hive, _snap

_SNAP_DS = "book_snapshots_500ms"


def _recorder(name, calls, ret=0):
    """Stand-in that records the call instead of doing the work."""
    def _f(*a, **k):
        calls.append(name)
        return ret
    return _f


def _write_one_snap_file(part_dir: Path, ii: int, t0: float) -> Path:
    p = part_dir / f"n{ii:02d}.parquet"
    pq.write_table(
        pa.Table.from_pylist([_snap(ii, 1000 + ii * 100)], schema=snapshot_schema(10)),
        str(p))
    # explicit mtimes: deterministic newest-first ordering across files
    os.utime(p, (t0 + ii, t0 + ii))
    return p


def _snap_hive(tmp_path: Path, n_files: int):
    base = tmp_path / "data"
    part = base / _SNAP_DS / "date=2026-10-11" / "asset=BTC"
    part.mkdir(parents=True)
    t0 = time.time() - 3600
    for ii in range(n_files):
        _write_one_snap_file(part, ii, t0)
    ml = base / "markets_latest"
    ml.mkdir(parents=True)
    pq.write_table(pa.table({"condition_id": ["0xaaa"], "market_id": ["42"]}),
                   str(ml / "markets_latest.parquet"))
    return base


def test_spawn_succeeds_prod_shaped_args(tmp_path) -> None:
    """Real worker spawn on a mini hive: result JSON, tmp staging, manifests.

    Prod shape: payload via temp JSON file (two small argv paths — never
    oversized argv), module-level worker entry, repo-root cwd, and a real
    cutoff timestamp like the collector loop passes.
    """
    base = _hive(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    res = _build_in_subprocess(
        base, out, [_SNAP_DS], ["BTC"], "5m", 10, False, True,
        timeout_s=180, cutoff_ts=time.time() + 300)
    assert res is not None, "spawn must succeed on a healthy box"
    assert "error" not in res
    rel = next(k for k in res if k.endswith("BTC_book_snapshots_500ms.parquet"))
    info = res[rel]
    assert info["rows"] == 4, info
    assert info["tmp"] and Path(info["tmp"]).exists()
    mani = res.get("__manifest__") or {}
    assert int((mani.get(f"{_SNAP_DS}/BTC") or {}).get("n") or 0) == 2


def test_spawn_failure_logs_full_traceback_and_context(tmp_path, capsys, monkeypatch) -> None:
    """The spawn-failure line must name its cause: FULL exception, no truncation."""
    long_msg = "spawn-killer-" + "z" * 2500  # longer than any old 1500-char tail cap

    def _boom(*a, **k):
        raise OSError(12, long_msg)

    monkeypatch.setattr(subprocess, "run", _boom)
    base = _hive(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    res = _build_in_subprocess(
        base, out, [_SNAP_DS], ["BTC"], "5m", 10, False, True, timeout_s=60)
    assert res is None  # fail-closed contract unchanged: None -> keep prior staging
    logged = capsys.readouterr().out
    assert "[export:worker] spawn failed" in logged
    assert "OSError" in logged
    assert long_msg in logged  # FULL exception string, nothing clipped
    assert "Traceback (most recent call last)" in logged
    assert "_boom" in logged  # the traceback frames name the raising code
    assert "ctx: parent pid=" in logged
    assert "rss=" in logged and "avail=" in logged and "open_fds=" in logged
    assert "keeps prior staging (fail closed)" in logged
    assert "falls back in-process" not in logged  # stale/lying text is gone


def test_worker_error_dict_never_falls_back(monkeypatch, tmp_path, capsys) -> None:
    """Worker ran and aborted itself (rss-cap) -> keep prior, no in-process build."""
    calls: list = []
    monkeypatch.setattr(E, "_build_in_subprocess", lambda *a, **k: {"error": "rss-cap-abort"})
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)  # gate open: proves it is not the gate
    monkeypatch.setattr(E, "_stream_export_asset_dataset", _recorder("stream", calls))
    base = _hive(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    stats = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_SNAP_DS], rolling_window=True)
    assert calls == [], "worker-reported errors must not trigger the in-process fallback"
    logged = capsys.readouterr().out
    assert "worker failed reason=rss-cap-abort" in logged
    assert "keeping prior staging (fail closed)" in logged
    assert stats, stats


def test_spawn_none_below_memory_gate_keeps_prior(monkeypatch, tmp_path, capsys) -> None:
    """Spawn failed AND the box is loaded -> honest skip, fail closed, no scan."""
    calls: list = []
    monkeypatch.setattr(E, "_build_in_subprocess", lambda *a, **k: None)
    monkeypatch.setattr(E, "_avail_mb", lambda: EXPORT_FALLBACK_MIN_AVAIL_MB - 1)
    monkeypatch.setattr(E, "_stream_export_asset_dataset", _recorder("stream", calls))
    base = _hive(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_SNAP_DS], rolling_window=True)
    assert calls == [], "the bounded fallback must respect the MemAvailable gate"
    logged = capsys.readouterr().out
    assert "worker failed" in logged and "keeping prior staging (fail closed)" in logged
    assert not (out / "BTC_book_snapshots_500ms.parquet").exists()


def test_spawn_none_trades_never_falls_back_inprocess(monkeypatch, tmp_path) -> None:
    """trades builds do Data-API network work — they must never run in-process."""
    calls: list = []
    monkeypatch.setattr(E, "_build_in_subprocess", lambda *a, **k: None)
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)
    monkeypatch.setattr(E, "_stream_export_trades_dataset", _recorder("trades", calls))
    base = _hive(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=["trades"], rolling_window=True)
    assert calls == [], "trades must keep prior staging (fail closed), never fall back"
    assert not (out / "BTC_trades.parquet").exists()


def test_spawn_none_fallback_fail_closed_on_unreadable_inputs(monkeypatch, tmp_path, capsys) -> None:
    """Fallback runs but ANY unreadable input keeps prior staging (same rule as workers)."""
    monkeypatch.setattr(E, "_build_in_subprocess", lambda *a, **k: None)
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)
    base = _hive(tmp_path)
    part = base / _SNAP_DS / "date=2026-09-10" / "asset=BTC"
    (part / "bad.parquet").write_bytes(b"not a parquet file")
    out = tmp_path / "staging"
    out.mkdir()
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_SNAP_DS], rolling_window=True)
    logged = capsys.readouterr().out
    assert "unreadable inputs" in logged
    assert "keeping prior staging (fail closed)" in logged
    assert not (out / "BTC_book_snapshots_500ms.parquet").exists()
    assert not list(out.glob("*.tmp")), "no tmp litter from the aborted fallback"


def test_spawn_none_fallback_stages_partial_with_honest_log(monkeypatch, tmp_path, capsys) -> None:
    """File-capped fallback stages the NEWEST cap files and says PARTIAL loudly."""
    monkeypatch.setattr(E, "_build_in_subprocess", lambda *a, **k: None)
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)
    n_total = EXPORT_FALLBACK_FILE_CAP + 5
    base = _snap_hive(tmp_path, n_total)
    out = tmp_path / "staging"
    out.mkdir()
    stats = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_SNAP_DS], rolling_window=True)
    staged = out / "BTC_book_snapshots_500ms.parquet"
    assert staged.exists(), "fallback must publish a fresh staging file"
    got = pq.read_table(str(staged))
    assert got.num_rows == EXPORT_FALLBACK_FILE_CAP
    ids = set(got.column("snapshot_id").to_pylist())
    # newest-first selection: the 5 oldest files are exactly the capped-away ones
    assert ids == {f"s{ii}" for ii in range(5, n_total)}
    logged = capsys.readouterr().out
    assert "spawn failed → bounded in-process fallback" in logged
    assert "PARTIAL" in logged
    assert f"{EXPORT_FALLBACK_FILE_CAP}/{n_total} source files" in logged
    assert f"cap {EXPORT_FALLBACK_FILE_CAP}" in logged
    assert "full worker rebuild deferred to next cycle" in logged
    assert f"scan: {EXPORT_FALLBACK_FILE_CAP} files (capped from {n_total}" in logged
    # NOT checkpoint-marked: the next cycle must retry the full worker build
    assert not (base / "kaggle_staging" / "_progress.json").exists()
    # same rel-key rule as the caller: full path when staging is outside base
    rel = str(staged.relative_to(base)) if staged.is_relative_to(base) else str(staged)
    assert stats[rel] == EXPORT_FALLBACK_FILE_CAP


def test_spawn_none_fallback_respects_cumulative_monotonic_guard(monkeypatch, tmp_path, capsys) -> None:
    """Cumulative mode: a short fallback build must never shrink staging."""
    monkeypatch.setattr(E, "_build_in_subprocess", lambda *a, **k: None)
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)
    base = _hive(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    # prior staging written now, then backdated so the hive files (written by
    # _hive just before) look newer and the fast-path mtime skip does not
    # short-circuit the spawn attempt
    prior = out / "BTC_book_snapshots_500ms.parquet"
    pq.write_table(pa.Table.from_pylist(
        [_snap(500 + i, 50_000 + i * 100) for i in range(10)], schema=snapshot_schema(10)),
        str(prior))
    _old = time.time() - 7200
    os.utime(prior, (_old, _old))
    stats = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_SNAP_DS], rolling_window=False)  # cumulative: monotonic guard on
    rel = str(prior.relative_to(base)) if prior.is_relative_to(base) else str(prior)
    assert stats[rel] == 10, "monotonic guard must keep the larger prior staging"
    assert pq.read_metadata(str(prior)).num_rows == 10
    logged = capsys.readouterr().out
    assert "monotonic guard kept prior staging" in logged
    assert "built 4" in logged  # honest: what the fallback actually built
    assert "keeping prior staging (fail closed)" not in logged  # the WARN path was not taken


def test_stream_max_files_cap_keeps_newest_then_oldest_order(tmp_path, capsys) -> None:
    """Unit pin for the cap knob: newest-N selection, honest stats, order kept."""
    base = _snap_hive(tmp_path, 4)
    io: dict = {}
    tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    n = _stream_export_asset_dataset(
        base, _SNAP_DS, "BTC", tmp, "5m", 10, {}, io_stats=io, max_files=2)
    assert n == 2
    assert io["files_capped"] == 2
    assert io.get("files_failed", 0) == 0
    got = pq.read_table(str(tmp))
    assert set(got.column("snapshot_id").to_pylist()) == {"s2", "s3"}  # the two newest
    logged = capsys.readouterr().out
    assert "scan: 2 files (capped from 4 at 2, newest-first) elapsed=" in logged
    # default (None) keeps the legacy whole-scan behavior and line format
    io2: dict = {}
    tmp2 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp.2"
    n2 = _stream_export_asset_dataset(
        base, _SNAP_DS, "BTC", tmp2, "5m", 10, {}, io_stats=io2)
    assert n2 == 4
    assert "files_capped" not in io2
    logged2 = capsys.readouterr().out
    assert "scan: 4 files elapsed=" in logged2
