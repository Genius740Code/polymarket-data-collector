"""Prune-aware coverage semantics for scripts/scorecard.py (backlog #2).

The local book_snapshots_500ms/ hive is a ROLLING mirror: rows are pruned
locally after every verified Kaggle upload, so a next-day scorecard of a
pruned date reads a low local row count that is a mirror artifact, not a
loss, and a same-day score is a partial day by construction.
classify_coverage() is the pure classifier behind the verdict modifiers:

  full    — ended date, local coverage at/above the threshold → the legacy
            full-day gate semantics apply unchanged
  partial — same-day date (wall clock before the day's end) → verdict
            prints SOAK; a numeric density fail never fires on coverage
            alone
  stale   — ended date, local rows < ~40% of the grid expectation →
            stale-local WARN when the kaggle checkpoint verifies the
            upload, a standing density FAIL when it does not; never a
            loss claim, never a completeness claim

Every fixture is a real pyarrow parquet file written into tmp_path and
every datetime is explicitly constructed and injected — classify_coverage
never reads the process clock. Real numbers only; honest gaps stay
explicit (n/a(partial)).
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import scorecard  # noqa: E402

assert hasattr(scorecard, "classify_coverage"), (
    "scripts/scorecard.py at this HEAD has no classify_coverage("
    "date_utc, local_rows, expected_est, now_utc) -> (full|partial|stale, "
    "reason) — the prune-aware coverage semantics (backlog #2: same-day "
    "partial-ok SOAK, pruned-day stale-local warn) are not implemented here"
)

UTC = timezone.utc
# Fixed, injected evaluation instants (never the process clock):
NOW = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)
MORNING = datetime(2026, 10, 22, 0, 30, 0, tzinfo=UTC)
# One full UTC day of grid slots for a single asset lane (asserted against
# scorecard's own first-principles arithmetic in the first test):
DAY_ROWS = scorecard.FULL_DAY_ROWS_PER_ASSET
P40 = 69_120      # exactly 40% of 172,800
P40_MINUS = 69_119
P7 = 12_096       # ~7% — the measured pruned-mirror reading


# ---------------------------------------------------------------------------
# Grid arithmetic — the expectation derives from the partition's own fields
# ---------------------------------------------------------------------------


def test_full_day_expectation_from_grid_arithmetic():
    # 500ms grid -> 1_000_000_000 ns / 500_000_000 ns = 2 ticks per second
    assert scorecard.GRID_NS == 500_000_000
    ticks_per_s = 1_000_000_000 // scorecard.GRID_NS
    # one UTC day = 24*60*60 s; per-asset full-day rows = day * ticks/s
    assert scorecard.FULL_DAY_ROWS_PER_ASSET == 24 * 60 * 60 * ticks_per_s
    assert scorecard.FULL_DAY_ROWS_PER_ASSET == 172_800


# ---------------------------------------------------------------------------
# classify_coverage — the pure date classifier
# ---------------------------------------------------------------------------


def test_same_day_40pct_rows_is_partial():
    klass, reason = scorecard.classify_coverage(
        datetime(2026, 10, 22).date(), P40, DAY_ROWS, NOW
    )
    assert klass == "partial"
    assert "same-day" in reason


def test_ended_date_7pct_rows_is_stale():
    klass, reason = scorecard.classify_coverage(
        datetime(2026, 10, 19).date(), P7, DAY_ROWS, NOW
    )
    assert klass == "stale"
    assert "expected_est" in reason


def test_ended_date_full_rows_is_full():
    klass, reason = scorecard.classify_coverage(
        datetime(2026, 10, 19).date(), DAY_ROWS, DAY_ROWS, NOW
    )
    assert klass == "full"


def test_today_partial_early_morning_is_partial():
    # 30 min into the day: 1_800 s * 2 ticks/s = 3_600 real rows so far
    klass, reason = scorecard.classify_coverage(
        datetime(2026, 10, 22).date(), 3_600, DAY_ROWS, MORNING
    )
    assert klass == "partial"
    assert "same-day" in reason


def test_ended_date_exactly_40pct_is_not_stale():
    # the stale threshold is a strict below-40% (integer compare, no
    # float drift): at exactly 40% the legacy full-day semantics apply
    klass, _ = scorecard.classify_coverage(
        datetime(2026, 10, 19).date(), P40, DAY_ROWS, NOW
    )
    assert klass == "full"
    klass, _ = scorecard.classify_coverage(
        datetime(2026, 10, 19).date(), P40_MINUS, DAY_ROWS, NOW
    )
    assert klass == "stale"


def test_classify_accepts_datetime_date_and_naive_now():
    # a datetime date_utc is narrowed to its date; a naive now is read as UTC
    klass, _ = scorecard.classify_coverage(
        datetime(2026, 10, 22, 6, 0, tzinfo=UTC), P40, DAY_ROWS, NOW
    )
    assert klass == "partial"
    klass, _ = scorecard.classify_coverage(
        datetime(2026, 10, 22).date(), P40, DAY_ROWS,
        datetime(2026, 10, 22, 12, 0, 0),  # naive wall clock → UTC
    )
    assert klass == "partial"


# ---------------------------------------------------------------------------
# lane_footer_rows — real parquet footers in tmp_path
# ---------------------------------------------------------------------------


def _write_lane(d: Path, rows_per_file: list) -> None:
    d.mkdir(parents=True, exist_ok=True)
    for i, n in enumerate(rows_per_file):
        rows = [
            {
                "condition_id": f"c{i}-{j}",
                "ts_snapshot_ns": 1_700_000_000_000_000_000 + j * 500_000_000,
                "snapshot_id": f"s{i}-{j}",
                "market_id": "m1",
                "series_id": "sr1",
                "book_state": "live",
                "resync_id": "",
                "up_bid": 0.49,
                "up_ask": 0.51,
                "down_bid": 0.48,
                "down_ask": 0.50,
            }
            for j in range(n)
        ]
        pq.write_table(pa.Table.from_pylist(rows), str(d / f"part-{i:03d}.parquet"))


def test_lane_footer_rows_sums_real_parquet_rows(tmp_path):
    d = tmp_path / "asset=BTC"
    _write_lane(d, [2, 3, 1])
    pq.write_table(  # atomic in-flight write: excluded by day_files
        pa.Table.from_pylist([{"ts_snapshot_ns": 1, "condition_id": "x"}]),
        str(d / "part-900.parquet.tmp"),
    )
    (d / "broken.parquet").write_bytes(b"not a parquet file")
    rows, files, unread, cut = scorecard.lane_footer_rows(
        d, deadline=time.monotonic() + 30
    )
    assert rows == 6 and files == 4 and unread == 1 and cut is False


def test_lane_footer_rows_skips_non_snapshot_schema(tmp_path):
    d = tmp_path / "asset=ETH"
    d.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{"unrelated": 1}]), str(d / "alien.parquet"))
    rows, files, unread, cut = scorecard.lane_footer_rows(
        d, deadline=time.monotonic() + 30
    )
    assert rows == 0 and files == 1 and unread == 1 and cut is False


def test_lane_footer_rows_cut_scan_never_looks_complete(tmp_path):
    d = tmp_path / "asset=SOL"
    _write_lane(d, [5])
    rows, files, unread, cut = scorecard.lane_footer_rows(
        d, deadline=time.monotonic() - 1
    )
    assert cut is True  # deadline in the past → honest cut, no partial sum


# ---------------------------------------------------------------------------
# kaggle_verified_covers — the verified-upload checkpoint (metadata only)
# ---------------------------------------------------------------------------


def _write_state(kaggle_root: Path, lane: str, dataset: str, entry: dict) -> None:
    lane_dir = kaggle_root / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    state = {f"gghgg1/{dataset}": entry}
    (lane_dir / "_kaggle_state.json").write_text(json.dumps(state))


def test_kaggle_verified_covers_build_start_gate(tmp_path):
    kr = tmp_path / "kaggle_staging"
    # real recorded shape: build_start cutoff 2026-10-21T19:14:52Z, verified
    # upload 2026-10-22T08:18:04Z → covers every date ending <= the cutoff
    _write_state(kr, "5m", "polymarket-5m-crypto", {
        "last_upload_utc": "2026-10-22T08:18:04.689961+00:00",
        "build_start_unix_ms": int(
            datetime(2026, 10, 21, 19, 14, 52, tzinfo=UTC).timestamp() * 1000
        ),
    })
    end_1019 = datetime(2026, 10, 20, tzinfo=UTC).timestamp()
    covered, newest = scorecard.kaggle_verified_covers(kr, end_1019)
    assert covered is True
    assert newest == "2026-10-22T08:18:04.689961+00:00"
    # date 2026-10-21 ends 2026-10-22T00:00Z — AFTER the cutoff: not covered
    end_1021 = datetime(2026, 10, 22, tzinfo=UTC).timestamp()
    covered2, _ = scorecard.kaggle_verified_covers(kr, end_1021)
    assert covered2 is False


def test_kaggle_verified_covers_falls_back_to_last_upload(tmp_path):
    kr = tmp_path / "kaggle_staging"
    _write_state(kr, "1h", "polymarket-1h-crypto", {
        "last_upload_utc": "2026-10-22T08:18:04+00:00",
    })  # no build_start → the literal last_upload_utc is the frontier
    covered, _ = scorecard.kaggle_verified_covers(
        kr, datetime(2026, 10, 22, tzinfo=UTC).timestamp()
    )
    assert covered is True
    covered2, _ = scorecard.kaggle_verified_covers(
        kr, datetime(2026, 10, 23, tzinfo=UTC).timestamp()
    )
    assert covered2 is False


def test_kaggle_verified_covers_no_state(tmp_path):
    covered, newest = scorecard.kaggle_verified_covers(
        tmp_path / "kaggle_staging", datetime(2026, 10, 20, tzinfo=UTC).timestamp()
    )
    assert covered is False and newest is None


# ---------------------------------------------------------------------------
# coverage_entries — the census wiring (real files + real state json)
# ---------------------------------------------------------------------------


def _entries(tmp_path, date_str, now, assets=("BTC",), state_entry=None):
    base = tmp_path / "data"
    snap_base = base / "book_snapshots_500ms" / f"date={date_str}"
    kr = base / "kaggle_staging"
    if state_entry is not None:
        _write_state(kr, "5m", "polymarket-5m-crypto", state_entry)
    return scorecard.coverage_entries(snap_base, list(assets), date_str, kr, now)


def test_coverage_entries_ended_pruned_date_without_checkpoint(tmp_path):
    _write_lane(tmp_path / "data" / "book_snapshots_500ms" / "date=2026-10-19" / "asset=BTC", [3, 2])
    cov = _entries(tmp_path, "2026-10-19", NOW)
    e = cov["BTC"]
    assert e["rows"] == 5 and e["files"] == 2 and e["unread"] == 0
    assert e["expected_est"] == DAY_ROWS
    assert e["klass"] == "stale"          # 5 rows << 40% of the grid day
    assert e["verified"] is False         # no checkpoint → the FAIL must stand
    assert e["modifier"] == "stale-noverify"


def test_coverage_entries_ended_pruned_date_with_verified_checkpoint(tmp_path):
    _write_lane(tmp_path / "data" / "book_snapshots_500ms" / "date=2026-10-19" / "asset=BTC", [3, 2])
    cov = _entries(tmp_path, "2026-10-19", NOW, state_entry={
        "last_upload_utc": "2026-10-22T08:18:04.689961+00:00",
        "build_start_unix_ms": int(
            datetime(2026, 10, 21, 19, 14, 52, tzinfo=UTC).timestamp() * 1000
        ),
    })
    e = cov["BTC"]
    assert e["klass"] == "stale" and e["verified"] is True
    assert e["modifier"] == "stale-local"
    assert e["verified_upload"] == "2026-10-22T08:18:04.689961+00:00"


def test_coverage_entries_same_day_is_partial_ok(tmp_path):
    _write_lane(tmp_path / "data" / "book_snapshots_500ms" / "date=2026-10-22" / "asset=BTC", [4])
    cov = _entries(tmp_path, "2026-10-22", NOW)
    e = cov["BTC"]
    assert e["rows"] == 4
    assert e["klass"] == "partial"       # the day is still in progress
    assert e["modifier"] == "partial-ok"


def test_coverage_entries_full_local_day_is_full(tmp_path):
    rows_needed = DAY_ROWS
    _write_lane(tmp_path / "data" / "book_snapshots_500ms" / "date=2026-10-19" / "asset=BTC", [rows_needed])
    cov = _entries(tmp_path, "2026-10-19", NOW)
    e = cov["BTC"]
    assert e["rows"] == DAY_ROWS and e["klass"] == "full" and e["modifier"] == "full"


def test_coverage_entries_cut_scan_stays_na_partial(tmp_path):
    _write_lane(tmp_path / "data" / "book_snapshots_500ms" / "date=2026-10-19" / "asset=BTC", [3])
    base = tmp_path / "data"
    cov = scorecard.coverage_entries(
        base / "book_snapshots_500ms" / "date=2026-10-19", ["BTC"], "2026-10-19",
        base / "kaggle_staging", NOW, deadline_s=-1.0,
    )
    e = cov["BTC"]
    assert e["rows"] is None and e["partial"] is True
    assert "klass" not in e or e.get("klass") is None
    assert e["modifier"] == "n/a(partial)"


def test_coverage_entries_records_fresher_staging_manifest(tmp_path):
    base = tmp_path / "data"
    d = base / "book_snapshots_500ms" / "date=2026-10-19" / "asset=BTC"
    _write_lane(d, [3])
    built = datetime.now(tz=UTC)  # real clock; the local fixture is backdated below
    mf = base / "kaggle_staging" / "15m" / "gghgg1" / "polymarket-15m-crypto"
    mf.mkdir(parents=True)
    (mf / "staging-manifest.json").write_text(json.dumps({
        "built_at_utc": built.isoformat(),
        "files": [{"name": "BTC_book_snapshots_500ms.parquet", "rows": 3_799}],
    }))
    for p in d.glob("*.parquet"):  # manifest fresher than every local file
        old = time.time() - 300
        os.utime(p, (old, old))
    cov = _entries(tmp_path, "2026-10-19", NOW)
    e = cov["BTC"]
    assert e["staged_rows"] == 3_799
    # an older-than-local manifest must NOT be recorded as fresher evidence
    (mf / "staging-manifest.json").write_text(json.dumps({
        "built_at_utc": (built - timedelta(hours=2)).isoformat(),
        "files": [{"name": "BTC_book_snapshots_500ms.parquet", "rows": 3_799}],
    }))
    cov2 = _entries(tmp_path, "2026-10-19", NOW)
    assert "staged_rows" not in cov2["BTC"]


# ---------------------------------------------------------------------------
# gate_verdict — the prune-aware verdict modifiers
# ---------------------------------------------------------------------------


def _cov(klass, rows=None, verified=None):
    e = {"klass": klass, "expected_est": DAY_ROWS}
    if rows is not None:
        e["rows"] = rows
    if verified is not None:
        e["verified"] = verified
        e["verified_upload"] = "2026-10-22T08:18:04.689961+00:00"
    return e


def _official_ok():
    return {"settled": 10, "official": 10, "inferred": 0, "other": 0}


def test_gate_verdict_same_day_density_fail_becomes_soak():
    probe = {"BTC": (40.0, 0.0, 10, False)}
    blocks = {"BTC": scorecard.T1Block(asset="BTC")}
    status, fails = scorecard.gate_verdict(
        probe, blocks, _official_ok(), 100.0, False,
        coverage={"BTC": _cov("partial", rows=P40)},
    )
    assert status == "SOAK"
    assert not any("density" in f for f in fails)  # coverage alone never FAILs


def test_gate_verdict_same_day_clean_is_soak_not_pass():
    probe = {"BTC": (100.0, 0.0, 10, False)}
    blocks = {"BTC": scorecard.T1Block(asset="BTC")}
    status, fails = scorecard.gate_verdict(
        probe, blocks, _official_ok(), 100.0, False,
        coverage={"BTC": _cov("partial", rows=P40)},
    )
    assert status == "SOAK" and fails == []


def test_gate_verdict_same_day_keeps_non_coverage_metrics():
    # CompNorm is NOT coverage: it keeps its FAIL even on a same-day date
    probe = {"BTC": (40.0, 0.5, 10, False)}
    blocks = {"BTC": scorecard.T1Block(asset="BTC")}
    status, fails = scorecard.gate_verdict(
        probe, blocks, _official_ok(), 100.0, False,
        coverage={"BTC": _cov("partial", rows=P40)},
    )
    assert status == "FAIL"
    assert any("CompNorm=0.5%" in f for f in fails)
    assert not any("density=" in f for f in fails)


def test_gate_verdict_same_day_unclassified_lane_density_fail_stands():
    probe = {"BTC": (40.0, 0.0, 10, False), "ETH": (40.0, 0.0, 10, False)}
    blocks = {"BTC": scorecard.T1Block(asset="BTC"), "ETH": scorecard.T1Block(asset="ETH")}
    cov = {"BTC": _cov("partial", rows=P40),
           "ETH": {"klass": None, "expected_est": DAY_ROWS}}  # scan cut → no claim
    status, fails = scorecard.gate_verdict(
        probe, blocks, _official_ok(), 100.0, False, coverage=cov
    )
    assert status == "FAIL"
    assert any(f.startswith("ETH density=40%") for f in fails)
    assert not any(f.startswith("BTC density=") for f in fails)


def test_gate_verdict_stale_verified_density_fail_becomes_warn():
    probe = {"BTC": (7.0, 0.0, 10, False)}
    blocks = {"BTC": scorecard.T1Block(asset="BTC")}
    status, fails = scorecard.gate_verdict(
        probe, blocks, _official_ok(), 100.0, False,
        coverage={"BTC": _cov("stale", rows=P7, verified=True)},
    )
    assert status == "WATCH"  # warn-only: never a gate FAIL, never a PASS
    warn = [f for f in fails if "WARN stale-local" in f]
    assert len(warn) == 1
    assert "WARN stale-local (data on Kaggle; see kaggle checkpoint)" in warn[0]
    assert "locally pruned, verified-uploaded" in warn[0]
    assert "12,096/172,800" in warn[0]
    # the density FAIL line itself is gone — only the warn carries the number
    assert not any(f.startswith("BTC density=") for f in fails)


def test_gate_verdict_stale_without_checkpoint_keeps_density_fail():
    probe = {"BTC": (7.0, 0.0, 10, False)}
    blocks = {"BTC": scorecard.T1Block(asset="BTC")}
    status, fails = scorecard.gate_verdict(
        probe, blocks, _official_ok(), 100.0, False,
        coverage={"BTC": _cov("stale", rows=P7, verified=False)},
    )
    assert status == "FAIL"
    assert any("density=7%" in f for f in fails)


def test_gate_verdict_na_partial_density_never_suppressed():
    # missing evidence is not a coverage artifact: the n/a fail stands
    probe = {"BTC": (None, 0.0, 0, True)}
    blocks = {"BTC": scorecard.T1Block(asset="BTC")}
    status, fails = scorecard.gate_verdict(
        probe, blocks, _official_ok(), 100.0, True,
        coverage={"BTC": _cov("partial", rows=P40)},
    )
    assert any("density=n/a(partial)" in f for f in fails)
    assert status == "WATCH"


def test_gate_verdict_legacy_call_without_coverage_unchanged():
    probe = {"BTC": (97.0, 0.05, 10, False)}
    blocks = {"BTC": scorecard.T1Block(asset="BTC")}
    status, fails = scorecard.gate_verdict(probe, blocks, _official_ok(), 100.0, False)
    assert status == "FAIL"
    assert any("density=97%" in f for f in fails)


# ---------------------------------------------------------------------------
# render_scorecard — operator-visible lines
# ---------------------------------------------------------------------------


def _snap(fails, status, coverage, dens=100.0):
    return {
        "date": "2026-10-22",
        "probe": {"BTC": (dens, 0.0, 10, False)},
        "blocks": {"BTC": scorecard.T1Block(asset="BTC")},
        "completeness": {},
        "completeness_status": "partial",
        "official": _official_ok(),
        "official_share": 100.0,
        "official_partial": False,
        "lanes_state": [],
        "partial": False,
        "status": status,
        "fails": fails,
        "resync_unread": 0,
        "resync_overflow": False,
        "coverage": coverage,
        "now_utc": NOW.isoformat(),
    }


def test_render_semantics_line_and_coverage_columns(capsys):
    cov = {"BTC": {"rows": P40, "files": 1, "unread": 0, "partial": False,
                   "expected_est": DAY_ROWS, "klass": "partial",
                   "modifier": "partial-ok", "verified": False,
                   "verified_upload": None}}
    scorecard.render_scorecard(_snap([], "SOAK", cov), ["BTC"])
    out = capsys.readouterr().out
    assert ("[scorecard] coverage semantics: same-day=partial-ok, "
            "pruned-days=stale-local(warn-only), full-day=unchanged") in out
    assert "69,120/172,800" in out
    assert "partial-ok" in out
    assert "verdict: SOAK" in out


def test_render_warn_section_split_from_failing(capsys):
    warn_line = (
        "BTC: WARN stale-local (data on Kaggle; see kaggle checkpoint) "
        "locally pruned, verified-uploaded — density=7% "
        "local_rows=12,096/172,800 verified_upload=2026-10-22T08:18:04+00:00"
    )
    cov = {"BTC": {"rows": P7, "files": 1, "unread": 0, "partial": False,
                   "expected_est": DAY_ROWS, "klass": "stale",
                   "modifier": "stale-local", "verified": True,
                   "verified_upload": "2026-10-22T08:18:04+00:00"}}
    scorecard.render_scorecard(_snap([warn_line], "WATCH", cov, dens=7.0), ["BTC"])
    out = capsys.readouterr().out
    assert "FAILING METRICS" not in out
    assert "WARN stale-local (warn-only" in out
    assert warn_line in out
    assert "12,096/172,800" in out
    assert "stale-local" in out
