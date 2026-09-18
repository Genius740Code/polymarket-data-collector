"""Continuous data-quality watchdog.

Loops until 3 hours of no issues are detected in the collector data.
On each iteration:
  1. Run the test suite
  2. Check book_snapshots_500ms for books labeled 'live' that should be 'stale'
  3. Check heartbeat/watchdog state
  4. If issues found: delete stale/problematic data, re-run the pipeline
  5. If 3 hours pass with zero issues: git commit and exit
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

DATA_DIR = Path("data")
BOOKS_500MS = DATA_DIR / "book_snapshots_500ms"
CLEAN_VIEW = DATA_DIR / "book_snapshots_clean"
HEARTBEAT = DATA_DIR / "heartbeat.json"

NO_ISSUES_TIMEOUT_S = 3 * 3600  # 3 hours
CHECK_INTERVAL_S = 30


def run_tests() -> bool:
    """Run the test suite. Returns True if all tests pass."""
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"TESTS FAILED:\n{result.stdout[-500:]}\n{result.stderr[-500:]}")
        return False
    print("All tests passed")
    return True


def check_stale_live_mismatch() -> int:
    """Report live/stale row counts in book_snapshots_500ms.

    NOTE: a high live share is the HEALTHY steady state (the clean view is
    live-only), so this is report-only and always returns 0 mismatches.
    Carried-forward stale books are caught by resync_episodes/coverage_gap
    accounting and the heartbeat check — not by counting live rows.
    Returns the count of potentially mismatched rows (always 0).
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pyarrow.compute as pc
    except ImportError:
        print("pyarrow not available — skipping stale/live check")
        return 0

    total = 0
    live_rows = 0
    for date_dir in BOOKS_500MS.glob("date=*"):
        for asset_dir in date_dir.glob("asset=*"):
            for parquet_file in asset_dir.glob("*.parquet"):
                try:
                    tbl = pq.read_table(parquet_file)
                except Exception as e:
                    print(f"  unreadable {parquet_file}: {e} (left in place)")
                    continue
                total += tbl.num_rows
                try:
                    mask = pc.equal(tbl.column("book_state"), pa.scalar("live"))
                    live_rows += tbl.filter(mask).num_rows
                except Exception:
                    pass
    if total > 0:
        print(f"Total rows: {total}, Live: {live_rows} (healthy steady state — not an issue)")
    return 0


def check_heartbeat_stale() -> bool:
    """Check if heartbeat is older than 15s (watchdog should have fired)."""
    if not HEARTBEAT.exists():
        return True  # no heartbeat = issue
    try:
        hb = json.loads(HEARTBEAT.read_text())
        ts_ns = hb.get("ts_ns", 0)
        age_s = (time.time_ns() - ts_ns) / 1e9
        if age_s > 15:
            print(f"Heartbeat stale: {age_s:.1f}s old")
            return True
    except Exception:
        pass
    return False


def delete_problematic_data() -> None:
    """Quarantine (never delete) data that could cause watchdog failures.

    Real-data-only policy: primary hive rows are never unlinked by automation.
    Suspect partitions are MOVED to data/_quarantine/<timestamp>/ for human
    review, and only when WATCHDOG_ALLOW_QUARANTINE=1 is set — otherwise this
    is a dry-run that prints what WOULD be quarantined. The old behavior
    (rmtree of the clean view + unlink of every file in >95%-live partitions,
    where >95% live is NORMAL) destroyed healthy primary data.
    """
    import shutil
    allowed = os.environ.get("WATCHDOG_ALLOW_QUARANTINE") == "1"
    ts = time.strftime("%Y%m%dT%H%M%S")
    qroot = DATA_DIR / "_quarantine" / f"watchdog-{ts}"
    print(f"Quarantine {'APPLY' if allowed else 'DRY-RUN'} (WATCHDOG_ALLOW_QUARANTINE={'1' if allowed else 'unset'}):")
    # Clean view is DERIVED (rebuildable from the hive) — still quarantine, not rmtree
    if CLEAN_VIEW.exists():
        dest = qroot / "book_snapshots_clean"
        print(f"  {'move' if allowed else 'would move'} {CLEAN_VIEW} -> {dest}")
        if allowed:
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(CLEAN_VIEW), str(dest))
            except Exception as e:
                print(f"  quarantine failed: {e}")

    # Heartbeat is a single JSON pointer — back it up before replacing
    if HEARTBEAT.exists():
        dest = qroot / "heartbeat.json"
        print(f"  {'move' if allowed else 'would move'} {HEARTBEAT} -> {dest}")
        if allowed:
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(HEARTBEAT), str(dest))
            except Exception as e:
                print(f"  quarantine failed: {e}")

    # NEVER truncate high-live partitions: live>95% is the healthy steady
    # state (the clean view itself is live-only). Report only.
    for date_dir in BOOKS_500MS.glob("date=*"):
        for asset_dir in date_dir.glob("asset=*"):
            parquet_files = list(asset_dir.glob("*.parquet"))
            if not parquet_files:
                continue
            # Check if this partition is mostly live (suggests watchdog didn't work)
            import pyarrow as pa
            import pyarrow.parquet as pq
            import pyarrow.compute as pc
            try:
                tbl = pq.read_table(parquet_files[0])
            except Exception as e:
                print(f"  unreadable first file in {date_dir.name}/{asset_dir.name}: {e} (left in place for quarantine helper)")
                continue
            mask = pc.equal(tbl.column("book_state"), pa.scalar("live"))
            live_ratio = sum(mask.to_pylist()) / tbl.num_rows if tbl.num_rows else 0
            if live_ratio > 0.95:
                # Too many live rows — likely watchdog never fired
                print(f"  NOTE high-live partition (healthy — NOT touched): {date_dir.name}/{asset_dir.name} live_ratio={live_ratio:.3f}")


def git_commit_all() -> None:
    """Disabled: automation must never auto-commit (it once committed a
    deleted-data state with a 'clean' message, destroying provenance).
    Prints the git status for a human to review instead."""
    print("git auto-commit DISABLED by policy — review `git status` manually; no commit made.")


def main() -> None:
    no_issue_since = None

    while True:
        print(f"\n=== Iteration at {time.strftime('%H:%M:%S')} ===")

        # 1. Run tests
        tests_ok = run_tests()

        # 2. Check for data issues
        stale_live = check_stale_live_mismatch()
        hb_stale = check_heartbeat_stale()
        issues = not tests_ok or stale_live > 0 or hb_stale

        if issues:
            no_issue_since = None
            # Quarantine suspect data (dry-run unless explicitly allowed) and re-loop
            delete_problematic_data()
            # Re-run collector start would go here — skip for safety
            print("Issues detected — quarantined (or dry-run), will re-loop")
            time.sleep(CHECK_INTERVAL_S)
            continue

        # No issues this iteration
        if no_issue_since is None:
            no_issue_since = time.time()
            print("No issues detected — starting 3h timer")
        elif time.time() - no_issue_since >= NO_ISSUES_TIMEOUT_S:
            print("3 hours with no issues — committing and exiting")
            git_commit_all()
            return
        else:
            remaining = int(NO_ISSUES_TIMEOUT_S - (time.time() - no_issue_since))
            mins = remaining // 60
            secs = remaining % 60
            print(f"No issues for {mins}m {secs}s remaining before commit")

        time.sleep(CHECK_INTERVAL_S)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nWatchdog stopped by user")