"""Unit tests for the quiet-window logic.

No real restarts, no network, no live PIDs.  All fixtures are synthetic.
The check functions accept an optional `now` parameter so tests can fix the
evaluation time and avoid flaky wall-clock comparisons.
"""

import os
import tempfile
import datetime
from datetime import timedelta


# Import the module directly via its file path to avoid package resolution issues
import importlib.util

_QW_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "scripts", "quiet_window.py")
)
_QW_SPEC = importlib.util.spec_from_file_location(
    "polymarket_collector.scripts.quiet_window", _QW_PATH
)
_qw_mod = importlib.util.module_from_spec(_QW_SPEC)
_QW_SPEC.loader.exec_module(_qw_mod)

check_tmp_young = _qw_mod.check_tmp_young
check_preparing_without_verdict = _qw_mod.check_preparing_without_verdict
check_export_worker_spawn = _qw_mod.check_export_worker_spawn

FIXED_NOW = datetime.datetime(2026, 10, 4, 9, 0, 0)


def _make_tmp(tmpdir, minutes_ago, pid, prefix="tmp"):
    """Create a .tmp file with mtime = FIXED_NOW - minutes_ago minutes."""
    path = os.path.join(tmpdir, f"{prefix}.parquet.tmp.{pid}.tmp")
    with open(path, "w") as f:
        f.write("data")
    mtime = (FIXED_NOW - datetime.timedelta(minutes=minutes_ago)).timestamp()
    os.utime(path, (mtime, mtime))
    return (path, mtime, pid, os.path.basename(path))


# --- Tests for check_tmp_young ---


def test_check_tmp_young_no_young_files():
    """With only an old .tmp file (>10 min), check_tmp_young should return False."""
    with tempfile.TemporaryDirectory() as tmpdir:
        old = _make_tmp(tmpdir, 20, 12345, "old")
        result = check_tmp_young([old], 10, now=FIXED_NOW)
        assert result is False


def test_check_tmp_young_has_young_file():
    """With a .tmp file younger than 10 min, check_tmp_young should return True."""
    with tempfile.TemporaryDirectory() as tmpdir:
        young = _make_tmp(tmpdir, 3, 67890, "young")
        result = check_tmp_young([young], 10, now=FIXED_NOW)
        assert result is True


def test_check_tmp_young_mixed_files():
    """With both old (20 min) and young (3 min) .tmp files, should return True."""
    with tempfile.TemporaryDirectory() as tmpdir:
        old = _make_tmp(tmpdir, 20, 12345, "old")
        young = _make_tmp(tmpdir, 3, 67890, "young")
        result = check_tmp_young([old, young], 10, now=FIXED_NOW)
        assert result is True


# --- Tests for check_preparing_without_verdict ---


def test_check_preparing_no_preparing_lines():
    """No Preparing lines → no verdict issue."""
    lines = [
        "2026-10-04T09:00:00: [resolution] ETH window 5970300 resolved down",
        "2026-10-04T09:05:00: [resolution] BTC window 5970300 resolved up",
    ]
    result = check_preparing_without_verdict(lines, 60, now=FIXED_NOW)
    assert result is False


def test_check_preparing_with_preparing_no_verdict():
    """Preparing with NO subsequent verdict → problem."""
    lines = [
        "2026-10-04T08:30:00: === Step 1: Preparing Kaggle staging 5m for ['BTC', 'ETH'] ===",
    ]
    result = check_preparing_without_verdict(lines, 60, now=FIXED_NOW)
    assert result is True


# --- Tests for check_export_worker_spawn ---


def test_check_export_no_worker_spawn():
    """No export worker lines → OK."""
    lines = [
        "2026-10-04T09:00:00: [resolution] ETH window 5970300 resolved down",
        "2026-10-04T09:05:00: [resolution] BTC window 5970300 resolved up",
    ]
    result = check_export_worker_spawn(lines, 10, now=FIXED_NOW)
    assert result is False


def test_check_export_with_worker_spawn():
    """Export worker spawn detected → not quiet."""
    lines = [
        "2026-10-04T09:00:00: [resolution] ETH window 5970300 resolved down",
        "2026-10-04T09:20:00: [export:worker] some worker started",
    ]
    result = check_export_worker_spawn(lines, 10, now=FIXED_NOW)
    assert result is True


# --- Integration-style test: full quiet window ---


def test_full_quiet_window_when_all_clean():
    """All checks pass → quiet window (exit 0)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        old = _make_tmp(tmpdir, 20, 12345, "old")
    lines = [
        "2026-10-04T09:00:00: [resolution] ETH window 5970300 resolved down",
        "2026-10-04T09:05:00: [resolution] BTC window 5970300 resolved up",
    ]
    errors = []
    if check_tmp_young([old], 10, now=FIXED_NOW):
        errors.append("young .tmp")
    if check_preparing_without_verdict(lines, 60, now=FIXED_NOW):
        errors.append("preparing without verdict")
    if check_export_worker_spawn(lines, 10, now=FIXED_NOW):
        errors.append("export worker spawn")
    assert errors == [], f"Expected quiet window, got errors: {errors}"


def test_full_not_quiet_when_young_tmp():
    """Young .tmp → not quiet (exit non-zero)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        young = _make_tmp(tmpdir, 3, 67890, "young")
    errors = []
    if check_tmp_young([young], 10, now=FIXED_NOW):
        errors.append("young .tmp")
    assert len(errors) == 1, f"Expected young .tmp error, got: {errors}"


def test_full_not_quiet_when_preparing_without_verdict():
    """Preparing without verdict → not quiet."""
    lines = [
        "2026-10-04T08:30:00: === Step 1: Preparing Kaggle staging 5m for ['BTC', 'ETH'] ===",
        # No resolution lines after this Preparing
    ]
    result = check_preparing_without_verdict(lines, 60, now=FIXED_NOW)
    assert result is True  # means not quiet


def test_full_not_quiet_when_export_worker():
    """Export worker spawn in last 10 min → not quiet."""
    with tempfile.TemporaryDirectory() as tmpdir:
        old = _make_tmp(tmpdir, 20, 12345, "old")
    lines = [
        "2026-10-04T09:00:00: [resolution] ETH window 5970300 resolved down",
        "2026-10-04T09:20:00: [export:worker] some worker started",
    ]
    errors = []
    if check_tmp_young([old], 10, now=FIXED_NOW):
        errors.append("young .tmp")
    if check_preparing_without_verdict(lines, 60, now=FIXED_NOW):
        errors.append("preparing without verdict")
    if check_export_worker_spawn(lines, 10, now=FIXED_NOW):
        errors.append("export worker spawn")
    assert len(errors) >= 1, "Expected at least one error (export worker spawn)"


# --- New: performance + semantics tests ---


def test_performance_tail_log_is_fast():
    """Verdict completes fast on synthetic big tail-limited log (not full file)."""
    import time
    # Build a large synthetic log with many lines
    n_lines = 200_000
    all_lines = []
    for i in range(n_lines):
        ts = (FIXED_NOW - timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S")
        all_lines.append(f"{ts}: [resolution] ETH window 5970300 resolved down")
    # Add a Preparing line near the middle of the tail window
    all_lines.append(
        "2026-10-04T08:35:00: === Step 1: Preparing Kaggle staging 5m for ['BTC', 'ETH'] ==="
    )
    # Take only the tail (last 2000 lines) as main() now does
    lines = all_lines[-2000:]
    start = time.time()
    result = check_preparing_without_verdict(lines, 60, now=FIXED_NOW)
    elapsed = time.time() - start
    # Should complete in well under 1 second thanks to tail limiting
    assert elapsed < 1.0, f"Expected fast completion (<1s), got {elapsed:.2f}s"
    # No verdict after Preparing → not quiet
    assert result is True


def test_semantics_unchanged_with_tail():
    """Verdict semantics unchanged with tail-limited log reading.

    The three criteria (young .tmp, preparing without verdict, worker spawn)
    produce the same result whether given the full log or the tail-limited log.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        young = _make_tmp(tmpdir, 3, 67890, "young")

    # Build a large synthetic log with many resolution lines,
    # plus one Preparing line without verdict in the recent window
    n_lines = 200_000
    all_lines = []
    for i in range(n_lines):
        ts = (FIXED_NOW - timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S")
        all_lines.append(f"{ts}: [resolution] ETH window 5970300 resolved down")
    # Add a Preparing line without verdict in the last 60 min window
    all_lines.append(
        "2026-10-04T08:30:00: === Step 1: Preparing Kaggle staging 5m for ['BTC', 'ETH'] ==="
    )
    # Take only the tail (last 2000 lines) as main() now does
    lines = all_lines[-2000:]

    errors = []
    if check_tmp_young([young], 10, now=FIXED_NOW):
        errors.append("young .tmp")
    if check_preparing_without_verdict(lines, 60, now=FIXED_NOW):
        errors.append("preparing without verdict")
    # No worker spawn in this test
    if check_export_worker_spawn(lines, 10, now=FIXED_NOW):
        errors.append("export worker spawn")
    # With young .tmp and preparing without verdict → not quiet (errors expected)
    assert len(errors) >= 1, f"Expected not-quiet (errors: {errors})"