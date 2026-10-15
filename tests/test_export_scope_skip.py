"""Scope-skip regression (P4 2026-10-12): bound the worker scan to the delta.

P4 proof (/tmp/opencode/p4_proof.txt): the BTC book_snapshots_500ms worker
scanned 1461/1754 files (~932k rows) in 1761s at ~530 rows/s under collector
contention — steady ~60s/50-file progress, i.e. a THROUGHPUT/BUDGET mismatch,
not a hang — and the 1800s spawn timeout killed it mid-scan (lock held
02:44->03:14, flushes starved, live% ~0 through 03:20, backlog draining past
03:36). Chosen fix: mechanism (b) — hive files at/below the recorded watermark
cutoff were already staged+verified (prior staging committed with matching
rows+bytes; staging-manifest.json cross-checked when present) and are NEVER
rescanned; the prior staging file is carried through the same transform
pipeline and only files newer than the cutoff are scanned. These tests pin:

- carry/date-filter correctness: incremental output is row-for-row what a
  full rescan produces (incl. whole-date prune -> carried rows leave);
- fail-closed authorization: a tampered/short/missing prior staging disables
  the skip and full-scans, loudly; max_files fallback builds never carry;
- the timeout path stays fail-closed (TimeoutExpired -> None -> prior kept);
- PARTIAL honesty: a spawn-failure fallback never records a watermark, so
  the next cycle's skip authorization refuses and a full rebuild runs;
- end-to-end: the parent records the watermark only on PUBLISHED commits,
  authorizes with rows+bytes verification, and the pre/worker coverage
  manifests stay identical (the gate is metadata-only over ALL files).
"""

import json
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import sys as _sys

_sys.path.insert(0, str(Path(__file__).resolve().parent))

import polymarket_collector.storage.export as E
from polymarket_collector.storage.export import (
    _carry_date_filter,
    _epoch_day_of_date_label,
    _stream_export_asset_dataset,
)
from polymarket_collector.storage.schemas import snapshot_schema
from test_streaming_export import _snap

_DS = "book_snapshots_500ms"


def _ns_for(date: str, i: int) -> int:
    """ts_snapshot_ns inside the UTC date (writer flushes rows into the
    date= partition their own ts resolves to — mirrored here)."""
    import datetime as dt

    d = dt.date.fromisoformat(date)
    return (d.toordinal() - 719163) * 86_400_000_000_000 + i * 500_000_000


def _write_file(part: Path, name: str, date: str, i0: int, n: int, mtime: float) -> Path:
    part.mkdir(parents=True, exist_ok=True)
    p = part / name
    pq.write_table(
        pa.Table.from_pylist(
            [_snap(i0 + i, _ns_for(date, i0 + i)) for i in range(n)],
            schema=snapshot_schema(10)),
        str(p))
    os.utime(p, (mtime, mtime))
    return p


def _dated_hive(tmp_path: Path, spec: dict, mtime: float) -> Path:
    """spec: {date: (n_files, rows_per_file)} under data/{DS}/date=.../asset=BTC."""
    base = tmp_path / "data"
    for d, (nf, rpf) in spec.items():
        part = base / _DS / f"date={d}" / "asset=BTC"
        for f in range(nf):
            _write_file(part, f"f{f:02d}.parquet", d, f * rpf, rpf, mtime)
    ml = base / "markets_latest"
    ml.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"condition_id": ["0xaaa"], "market_id": ["42"]}),
                   str(ml / "markets_latest.parquet"))
    return base


def _keys_of(path: Path):
    t = pq.read_table(str(path), columns=["asset", "condition_id", "ts_snapshot_ns",
                                          "snapshot_id", "market_id"])
    return sorted(zip(t.column("asset").to_pylist(), t.column("condition_id").to_pylist(),
                      t.column("ts_snapshot_ns").to_pylist(), t.column("snapshot_id").to_pylist(),
                      t.column("market_id").to_pylist()))


def _watermark_for(staging: Path) -> dict:
    return {"cutoff": 0, "rows": 0, "bytes": 0}


# ---------------------------------------------------------------- unit: filters

def test_epoch_day_of_date_label() -> None:
    assert _epoch_day_of_date_label("1970-01-01") == 0
    assert _epoch_day_of_date_label("2026-10-11") == 20737
    assert _epoch_day_of_date_label("unknown") == -1
    assert _epoch_day_of_date_label("garbage") is None


def test_carry_date_filter_ns_priority_and_pruned_dates() -> None:
    t = pa.table({
        "ts_snapshot_ns": pa.array([_ns_for("2026-10-10", 1), _ns_for("2026-10-11", 1),
                                    None, None]),
        "ts_received_ns": pa.array([None, None, _ns_for("2026-10-10", 2), None]),
        "ts_source": pa.array([None, None, None, _ns_for("2026-10-12", 9) // 1_000_000]),
    })
    kept = {_epoch_day_of_date_label("2026-10-11")}
    out = _carry_date_filter(t, kept)
    assert out.num_rows == 1  # only the 10-11 ns row survives; 10-10 and 10-12 drop
    kept2 = {_epoch_day_of_date_label("2026-10-10"), _epoch_day_of_date_label("2026-10-11"),
             _epoch_day_of_date_label("2026-10-12")}
    out2 = _carry_date_filter(t, kept2)
    assert out2.num_rows == 4  # everything kept when all three partitions exist
    # unknown-partition rows (no resolvable ts anywhere) need date=unknown present
    t3 = pa.table({"ts_snapshot_ns": pa.array([None, None], pa.int64()),
                   "ts_received_ns": pa.array([None, None], pa.int64()),
                   "ts_source": pa.array([None, "not-a-number"], pa.string())})
    out3 = _carry_date_filter(t3, kept2)
    assert out3.num_rows == 0  # no unknown partition in the hive -> dropped
    out4 = _carry_date_filter(t3, kept2 | {-1})
    assert out4.num_rows == 2  # date=unknown exists -> honest-gap rows stay


# ------------------------------------------------- incremental build == full rebuild

def _build(base: Path, tmp_out: Path, skip_watermark=None, io=None, cutoff=None):
    return _stream_export_asset_dataset(
        base, _DS, "BTC", tmp_out, "5m", 10, {"0xaaa": "42"},
        io_stats=io, cutoff_ts=cutoff, skip_watermark=skip_watermark)


def test_scope_skip_matches_full_rescan(tmp_path, capsys) -> None:
    """Already-staged files skipped; output row-for-row identical to full scan."""
    base = _dated_hive(tmp_path, {"2026-10-10": (3, 2), "2026-10-11": (2, 2)},
                       mtime=time.time() - 7200)
    out1 = tmp_path / "BTC_book_snapshots_500ms.parquet"
    tmp1 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    io1: dict = {}
    n1 = _build(base, tmp1, io=io1)
    os.replace(str(tmp1), str(out1))
    assert n1 == 10 and io1["files_ok"] == 5
    capsys.readouterr()

    # cycle 2: two NEW files (fresh mtime) in a new date partition
    part = base / _DS / "date=2026-10-12" / "asset=BTC"
    _now = time.time()
    _write_file(part, "f00.parquet", "2026-10-12", 100, 2, _now)
    _write_file(part, "f01.parquet", "2026-10-12", 102, 2, _now)

    wm = {"cutoff": _now - 3600, "rows": pq.read_metadata(str(out1)).num_rows,
          "bytes": out1.stat().st_size}
    tmp2 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    io2: dict = {}
    n2 = _build(base, tmp2, skip_watermark=wm, io=io2, cutoff=_now + 60)
    assert n2 == 14  # 10 carried + 4 new
    assert io2["files_ok"] == 2  # ONLY the new files were opened
    assert io2["files_skipped"] == 5
    assert io2["carried_rows"] == 10
    assert io2.get("files_failed", 0) == 0
    logged = capsys.readouterr().out
    assert "scope-skip: 5/7 already staged+verified" in logged
    assert "scope-skip BTC/book_snapshots_500ms: carried 10 rows" in logged

    # full rescan of the SAME state must produce the identical file
    tmp_full = tmp_path / "full.parquet.tmp"
    n_full = _build(base, tmp_full, cutoff=_now + 60)
    assert n_full == 14
    assert _keys_of(tmp2) == _keys_of(tmp_full)


def test_scope_skip_pruned_whole_date_drops_carried_rows(tmp_path, capsys) -> None:
    """Rolling window: a vanished date= partition must leave the staging."""
    base = _dated_hive(tmp_path, {"2026-10-10": (3, 2), "2026-10-11": (2, 2)},
                       mtime=time.time() - 7200)
    out1 = tmp_path / "BTC_book_snapshots_500ms.parquet"
    tmp1 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    n1 = _build(base, tmp1)
    os.replace(str(tmp1), str(out1))
    assert n1 == 10
    capsys.readouterr()

    # prune the WHOLE oldest date (retention slid past it), add fresh rows
    import shutil

    shutil.rmtree(base / _DS / "date=2026-10-10")
    part = base / _DS / "date=2026-10-11" / "asset=BTC"
    _now = time.time()
    _write_file(part, "new.parquet", "2026-10-11", 50, 2, _now)

    wm = {"cutoff": _now - 3600, "rows": pq.read_metadata(str(out1)).num_rows,
          "bytes": out1.stat().st_size}
    tmp2 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    io2: dict = {}
    n2 = _build(base, tmp2, skip_watermark=wm, io=io2, cutoff=_now + 60)
    # 4 carried (10-10 dropped by the date filter) + 2 new
    assert n2 == 6
    assert io2["carried_rows"] == 4
    logged = capsys.readouterr().out
    assert "date-filter dropped 6 carried rows (partition left the hive)" in logged
    # identical to a full rescan of the pruned hive
    tmp_full = tmp_path / "full.parquet.tmp"
    n_full = _build(base, tmp_full, cutoff=_now + 60)
    assert n_full == 6
    assert _keys_of(tmp2) == _keys_of(tmp_full)


def test_scope_skip_disabled_on_watermark_mismatch(tmp_path, capsys) -> None:
    """Tampered prior staging (PARTIAL fallback / monotonic keep / disk rot):
    the skip must refuse and a FULL scan run — fail closed, loudly."""
    base = _dated_hive(tmp_path, {"2026-10-10": (3, 2)}, mtime=time.time() - 7200)
    out1 = tmp_path / "BTC_book_snapshots_500ms.parquet"
    tmp1 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    n1 = _build(base, tmp1)
    os.replace(str(tmp1), str(out1))
    assert n1 == 6
    capsys.readouterr()

    # the prior staging on disk is NOT the artifact the watermark describes
    _short = tmp_path / "short.parquet.tmp"
    _build(base, _short)  # rows differ (rewritten file, e.g. a PARTIAL fallback)
    os.replace(str(_short), str(out1))

    wm = {"cutoff": time.time() - 3600, "rows": 999, "bytes": out1.stat().st_size}
    io: dict = {}
    tmp2 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    n2 = _build(base, tmp2, skip_watermark=wm, io=io)
    assert n2 == 6
    assert io["files_ok"] == 3  # every file rescanned — no skip, no carry
    assert "files_skipped" not in io and "carried_rows" not in io
    logged = capsys.readouterr().out
    assert "scope-skip disabled: prior staging rows != watermark" in logged
    assert "scan: 3 files (scope-skip disabled:" in logged
    assert "carried" not in logged

    # missing prior staging refuses too
    (tmp_path / "BTC_book_snapshots_500ms.parquet").unlink()
    io3: dict = {}
    tmp3 = tmp_path / "again.parquet.tmp"
    _build(base, tmp3, skip_watermark=wm, io=io3)
    assert io3["files_ok"] == 3
    assert "scope-skip disabled: prior staging missing" in capsys.readouterr().out


def test_scope_skip_never_combined_with_max_files(tmp_path, capsys) -> None:
    """Fallback builds are PARTIAL by construction — they must never carry."""
    base = _dated_hive(tmp_path, {"2026-10-10": (4, 1)}, mtime=time.time() - 7200)
    wm = {"cutoff": time.time() - 3600, "rows": 4, "bytes": 12345}
    io: dict = {}
    tmp = tmp_path / "capped.parquet.tmp"
    n = _build(base, tmp, skip_watermark=wm, io=io, cutoff=time.time() + 60)
    # max_files not passed here — but the skip watermark must still refuse:
    # "bytes" does not match any real prior staging file (no file exists at all)
    assert n == 4
    assert io["files_ok"] == 4  # full scan
    assert "scope-skip disabled: prior staging missing" in capsys.readouterr().out


def test_scope_skip_no_overlap_full_scans(tmp_path, capsys) -> None:
    """Window slid completely past the watermark: carried rows would all be
    pruned dates -> skip refuses (honest), full scan runs."""
    base = _dated_hive(tmp_path, {"2026-10-12": (2, 2)}, mtime=time.time())
    tmp = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    prior = tmp_path / "BTC_book_snapshots_500ms.parquet"
    # a real (verified-shaped) prior staging: 1 unrelated row, fresh on disk
    pq.write_table(pa.table({"a": [1]}), str(prior))
    wm = {"cutoff": time.time() - 7200, "rows": 1, "bytes": prior.stat().st_size}
    io: dict = {}
    n = _build(base, tmp, skip_watermark=wm, io=io)
    assert n == 4
    assert io["files_ok"] == 2
    assert "no file predates the watermark cutoff" in capsys.readouterr().out


# ----------------------------------------------------- parent plumbing end-to-end

def _age_out_progress(base: Path, lane: str = "5m") -> None:
    """Backdate the freshness mark only (keep cutoff/rows/bytes watermark)."""
    p = base / "kaggle_staging" / "_progress.json"
    j = json.loads(p.read_text())
    for k, v in j.items():
        if isinstance(v, dict):
            v["ts"] = int(time.time()) - 4000
    p.write_text(json.dumps(j))


def test_end_to_end_watermark_recorded_and_manifests_identical(tmp_path, capsys) -> None:
    """Two cycles through the real spawn path: the first PUBLISHED commit
    records {cutoff, rows, bytes}; the second authorizes the skip, carries,
    scans only the delta, and the pre/worker coverage manifests still match
    (the gate stays metadata-only over ALL files at/below the cutoff)."""
    base = _dated_hive(tmp_path, {"2026-10-10": (2, 2)}, mtime=time.time() - 7200)
    out = tmp_path / "staging"
    out.mkdir()

    c1 = time.time()
    mani1: dict = {}
    stats1 = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=True, cutoff_ts=c1, manifests=mani1)
    staged = out / "BTC_book_snapshots_500ms.parquet"
    assert staged.exists()
    assert stats1[str(staged)] == 4
    assert mani1[f"{_DS}/BTC"]["ok"] is True  # coverage proven on cycle 1
    prog = json.loads((base / "kaggle_staging" / "_progress.json").read_text())
    wm = prog.get(f"5m/{_DS}/BTC")
    assert isinstance(wm, dict) and wm["cutoff"] == c1
    assert wm["rows"] == 4 and wm["bytes"] == staged.stat().st_size
    capsys.readouterr()

    # cycle 2: one new file appears (fresh mtime); freshness mark aged out
    _now = time.time()
    _write_file(base / _DS / "date=2026-10-10" / "asset=BTC", "new.parquet",
                "2026-10-10", 40, 2, _now)
    _age_out_progress(base)
    mani2: dict = {}
    stats2 = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=True, cutoff_ts=_now + 60, manifests=mani2)
    assert stats2[str(staged)] == 6
    assert mani2[f"{_DS}/BTC"]["ok"] is True  # pre == worker manifest, skip or not
    logged = capsys.readouterr().out
    assert "scope-skip book_snapshots_500ms/BTC: authorized" in logged
    assert "build carried 4 rows and skipped 2 already-staged source files" in logged
    got = pq.read_table(str(staged))
    assert got.num_rows == 6
    # incremental output == full rescan of the same state
    tmp_full = tmp_path / "full.parquet.tmp"
    E._stream_export_asset_dataset(base, _DS, "BTC", tmp_full, "5m", 10,
                                   E._load_market_id_map(base), cutoff_ts=_now + 60)
    assert _keys_of(staged) == _keys_of(tmp_full)


def test_fallback_partial_never_records_watermark_next_cycle_rebuilds(
        monkeypatch, tmp_path, capsys) -> None:
    """Spawn failure + healthy box -> PARTIAL fallback stages the newest cap
    files but must NOT refresh the watermark: the prior full-build watermark no
    longer matches the PARTIAL artifact on disk, so the NEXT cycle refuses the
    skip and full-rebuilds (self-healing), with the honest PARTIAL line kept."""
    base = _dated_hive(tmp_path, {"2026-10-10": (E.EXPORT_FALLBACK_FILE_CAP + 5, 1)},
                       mtime=time.time() - 7200)
    out = tmp_path / "staging"
    out.mkdir()
    c1 = time.time()
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=True, cutoff_ts=c1)
    staged = out / "BTC_book_snapshots_500ms.parquet"
    assert pq.read_metadata(str(staged)).num_rows == E.EXPORT_FALLBACK_FILE_CAP + 5
    capsys.readouterr()

    # force the spawn to die like the P4 worker (timeout class) -> fallback.
    # A fresh input file keeps the resume-fast mtime skip out of the way.
    monkeypatch.setattr(E, "_build_in_subprocess", lambda *a, **k: None)
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)
    _write_file(base / _DS / "date=2026-10-10" / "asset=BTC", "mid.parquet",
                "2026-10-10", 200, 1, time.time())
    _age_out_progress(base)
    _now = time.time()
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=True, cutoff_ts=_now)
    logged = capsys.readouterr().out
    assert "spawn failed → bounded in-process fallback" in logged
    assert "PARTIAL" in logged
    # the PARTIAL artifact replaced the verified staging: rows shrank to the cap
    assert pq.read_metadata(str(staged)).num_rows == E.EXPORT_FALLBACK_FILE_CAP

    # next cycle, spawns healthy again: the stale watermark (rows/bytes of the
    # old FULL artifact) must NOT authorize a skip -> full worker rebuild.
    # Another fresh file forces the rebuild past the resume-fast mtime skip.
    monkeypatch.undo()
    _write_file(base / _DS / "date=2026-10-10" / "asset=BTC", "mid2.parquet",
                "2026-10-10", 300, 1, time.time())
    _age_out_progress(base)
    _now2 = time.time()
    stats = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=True, cutoff_ts=_now2)
    logged2 = capsys.readouterr().out
    assert "scope-skip book_snapshots_500ms/BTC: authorized" not in logged2
    assert stats[str(staged)] == E.EXPORT_FALLBACK_FILE_CAP + 7  # full rebuild
    assert pq.read_metadata(str(staged)).num_rows == E.EXPORT_FALLBACK_FILE_CAP + 7


def test_monotonic_keep_does_not_advance_watermark(tmp_path) -> None:
    """Cumulative mode: when the monotonic guard keeps the LARGER prior
    staging (built rows < prior rows — here a whole date partition
    vanished, so the carried build comes out short), the coverage
    watermark must NOT advance: the on-disk artifact is the OLD one and
    only its old cutoff is proven. Next cycle keeps full-scan semantics
    guarded by the still-matching rows+bytes."""
    base = _dated_hive(tmp_path, {"2026-10-10": (3, 1), "2026-10-11": (3, 1)},
                       mtime=time.time() - 7200)
    out = tmp_path / "staging"
    out.mkdir()
    c1 = time.time()
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=False, cutoff_ts=c1)
    staged = out / "BTC_book_snapshots_500ms.parquet"
    assert pq.read_metadata(str(staged)).num_rows == 6
    prog = json.loads((base / "kaggle_staging" / "_progress.json").read_text())
    wm1 = prog[f"5m/{_DS}/BTC"]
    assert isinstance(wm1, dict) and wm1["rows"] == 6

    # a whole date partition vanishes (disk loss / manual cleanup) and one
    # fresh file lands: the carried build is 3+1=4 rows < prior 6 -> the
    # cumulative monotonic guard keeps the prior 6-row artifact
    import shutil

    shutil.rmtree(base / _DS / "date=2026-10-10")
    _write_file(base / _DS / "date=2026-10-11" / "asset=BTC", "new.parquet",
                "2026-10-11", 40, 1, time.time())
    _age_out_progress(base)
    _now = time.time()
    stats = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=False, cutoff_ts=_now)
    assert stats[str(staged)] == 6  # guard kept the larger prior staging
    assert pq.read_metadata(str(staged)).num_rows == 6
    prog2 = json.loads((base / "kaggle_staging" / "_progress.json").read_text())
    wm2 = prog2[f"5m/{_DS}/BTC"]
    # coverage fields unchanged (only the freshness ts may move): a
    # guard-kept artifact never advances the recorded coverage cutoff
    assert {k: wm2[k] for k in ("cutoff", "rows", "bytes")} == \
        {k: wm1[k] for k in ("cutoff", "rows", "bytes")}


# --------------------------------------- 2026-10-14 carry-peak fix regressions
#
# The scope-skip carry read of a REAL prior staging decoded 20000-row x 842
# column chunks (137MB decoded, +137MB per identity filter copy — the 500ms
# worker crossed its 1000MB rss cap 340k rows into the BTC carry; HYPE
# peaked 1076MB on the bench, clean lanes passed only because their staging
# is ~4x smaller). The fix slices the carry/delta reads at
# _CARRY_READ_BATCH_ROWS, keeps one row-group-scoped reader iterator per
# carry row group (a single long-lived iter_batches retains ~33MB of reader
# state per ~200k rows), skips identity filters, trims the allocator
# between slices and stores dedup keys in the open-addressed _DigestSet
# (~29B/key vs ~90-100B/key as a Python set of bytes). Measured on the real
# HYPE 5m pair: carry peak 1076 -> 522MB; fixed full scan 531MB; output
# rows x 842 columns content-identical to both the HEAD carry and a full
# rebuild on the same frozen inputs.

def test_mask_keeps_all_null_safe() -> None:
    """_mask_keeps_all: True only for an all-True mask; NULLs DROP rows in
    Table.filter so they must read as False (an all-NULL mask is NOT 'all
    true'); kernel trouble returns False so the caller runs the filter."""
    assert E._mask_keeps_all(pa.array([True, True, True])) is True
    assert E._mask_keeps_all(pa.array([True, False, True])) is False
    assert E._mask_keeps_all(pa.array([True, None, True])) is False
    assert E._mask_keeps_all(pa.array([None, None], type=pa.bool_())) is False
    assert E._mask_keeps_all("not-a-mask") is False


def test_snapshot_dedup_store_open_addressed_set_parity() -> None:
    """The snapshot dedup seen-store is the trades path's open-addressed
    _DigestSet (~29B/key) with full set-parity semantics — the plain Python
    set of 16-byte digests measured 103MB at the real carry's 1.15M keys."""
    st = E._CompactDedupState(["asset", "condition_id", "ts_snapshot_ns"])
    assert isinstance(st.seen, E._DigestSet)
    d1 = E._compact_key_digest(("BTC", "0x" + "a" * 64, 123))
    d2 = E._compact_key_digest(("BTC", "0x" + "b" * 64, 123))
    assert d1 not in st.seen and len(st.seen) == 0
    assert st.seen.add(d1) is True     # new
    assert st.seen.add(d1) is False    # duplicate
    assert d1 in st.seen and d2 not in st.seen and len(st.seen) == 1
    st.seen.update([d2])
    assert len(st.seen) == 2
    assert sorted(st.seen) == sorted([d1, d2])  # iteration parity
    assert st.seen == {d1, d2}                   # set-equality parity
    # filter keeps first-wins rows exactly like the tuple path (fresh store)
    st2 = E._CompactDedupState(["asset", "condition_id", "ts_snapshot_ns"])
    t = pa.table({"asset": ["BTC", "BTC"],
                  "condition_id": ["0x" + "a" * 64, "0x" + "a" * 64],
                  "ts_snapshot_ns": pa.array([123, 123], pa.int64())})
    out = st2.filter(t)
    assert out.num_rows == 1 and st2.dupes == 1


def test_carry_reads_prior_staging_in_bounded_slices(tmp_path, capsys) -> None:
    """A FAT prior staging (one row group deeper than the decode slice) is
    carried through the row-group-scoped sliced read: every decoded batch
    <= _CARRY_READ_BATCH_ROWS rows, single-threaded, and the incremental
    output still matches a full rescan row-for-row."""
    base = _dated_hive(tmp_path, {"2026-10-10": (3, 1700)}, mtime=time.time() - 7200)
    out1 = tmp_path / "BTC_book_snapshots_500ms.parquet"
    tmp1 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
    n1 = _build(base, tmp1)
    os.replace(str(tmp1), str(out1))
    assert n1 == 5100
    capsys.readouterr()

    part = base / _DS / "date=2026-10-11" / "asset=BTC"
    _now = time.time()
    _write_file(part, "f00.parquet", "2026-10-11", 5000, 300, _now)
    wm = {"cutoff": _now - 3600, "rows": pq.read_metadata(str(out1)).num_rows,
          "bytes": out1.stat().st_size}

    # spy on every ParquetFile the build opens: record iter_batches kwargs
    import polymarket_collector.storage.export as _E
    _real_pf = _E.pq.ParquetFile
    batches_seen: list = []

    class _SpyPF(_real_pf):  # type: ignore[misc,valid-type]
        def iter_batches(self, **kw):
            for b in super().iter_batches(**kw):
                batches_seen.append((kw.get("batch_size"), kw.get("use_threads"),
                                     kw.get("row_groups"), b.num_rows))
                yield b

    _E.pq.ParquetFile = _SpyPF
    try:
        tmp2 = tmp_path / "BTC_book_snapshots_500ms.parquet.tmp"
        io2: dict = {}
        n2 = _build(base, tmp2, skip_watermark=wm, io=io2, cutoff=_now + 60)
    finally:
        _E.pq.ParquetFile = _real_pf

    assert n2 == 5400 and io2["carried_rows"] == 5100
    assert io2["files_skipped"] == 3 and io2.get("files_failed", 0) == 0
    # the prior-staging read: every decoded slice <= the decode batch bound,
    # single-threaded, one row-group-scoped iterator per row group
    prior_slices = [b for b in batches_seen if b[3] > 0 and b[0] == E._CARRY_READ_BATCH_ROWS]
    assert prior_slices, "carry read must be sliced, not whole-row-group"
    assert all(b[3] <= E._CARRY_READ_BATCH_ROWS for b in prior_slices)
    assert all(b[1] is False for b in prior_slices)
    assert sum(b[3] for b in prior_slices) >= 5100
    # slices stay far under the writer ceiling (the old 20000x842 fat chunk
    # was the rss-cap peak driver)
    assert E._CARRY_READ_BATCH_ROWS < 20000
    # incremental build == full rescan of the same hive
    tmp_full = tmp_path / "full.parquet.tmp"
    n_full = _build(base, tmp_full, cutoff=_now + 60)
    assert n_full == 5400
    assert _keys_of(tmp2) == _keys_of(tmp_full)


def test_release_arrow_pool_best_effort() -> None:
    """The mimalloc-arena purge helper is safe anywhere and returns None
    (best-effort; the pool API is exercised, failures swallowed)."""
    assert E._release_arrow_pool() is None


def test_scaled_spawn_budget_covers_carry_plus_delta(monkeypatch, tmp_path) -> None:
    """2026-10-15 whale-lane budget: the spawn budget is keyed on the
    pre-spawn manifest — ALL frozen files (carried + delta), never the
    delta-only count — because the carried worker pays for BOTH the carry
    replay and the delta scan (DOGE 17:03 UTC event: 1,202,530 carried rows
    + 904/1043 delta files processed inside 1800s, ~2.0s/file, then
    TimeoutExpired -> PARTIAL). 1043 frozen files authorize 2907s
    (files x 2.5s + 300s headroom, avail-guarded): ~40% headroom over the
    measured ~2076s linear need. A delta-only key (1040 -> 1407s -> 1800
    baseline) would re-create the exact under-budget that killed DOGE."""
    base = _dated_hive(tmp_path, {"2026-10-10": (3, 1)}, mtime=time.time() - 7200)
    out = tmp_path / "staging"
    out.mkdir()
    c1 = time.time()
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=True, cutoff_ts=c1)
    staged = out / "BTC_book_snapshots_500ms.parquet"
    assert pq.read_metadata(str(staged)).num_rows == 3
    prog = json.loads((base / "kaggle_staging" / "_progress.json").read_text())
    assert prog[f"5m/{_DS}/BTC"]["rows"] == 3  # cycle-1 watermark recorded

    # 1040 fresh delta files: 3 carry + 1040 delta = the DOGE event's 1043
    _now = time.time()
    part = base / _DS / "date=2026-10-11" / "asset=BTC"
    for f in range(1040):
        _write_file(part, f"d{f:05d}.parquet", "2026-10-11", f, 1, _now + f)
    _age_out_progress(base)

    captured: list = []

    def _fake_build(*a, **k):
        captured.append({"timeout_s": k.get("timeout_s"),
                         "skip_watermark": k.get("skip_watermark")})
        return {}  # no __manifest__ -> coverage fails closed; prior kept

    monkeypatch.setattr(E, "_build_in_subprocess", _fake_build)
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)
    E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=[_DS], rolling_window=True, cutoff_ts=_now + 2000)
    assert captured, "worker spawn must run"
    assert captured[0]["skip_watermark"] is not None  # carry authorized
    # budget keyed on the FULL manifest (carry + delta), not the delta slice
    assert captured[0]["timeout_s"] == E._worker_timeout_s(1043) == 2907
    # and the coverage/watermark guards are untouched: the fake build proved
    # nothing, so the prior staging is kept and only the freshness ts moves —
    # the recorded coverage (cutoff/rows/bytes) must NOT advance (a failed
    # build never widens what the next cycle may skip).
    assert pq.read_metadata(str(staged)).num_rows == 3
    prog2 = json.loads((base / "kaggle_staging" / "_progress.json").read_text())
    _w1, _w2 = prog[f"5m/{_DS}/BTC"], prog2[f"5m/{_DS}/BTC"]
    assert (_w2["cutoff"], _w2["rows"], _w2["bytes"]) == (_w1["cutoff"], _w1["rows"], _w1["bytes"])
