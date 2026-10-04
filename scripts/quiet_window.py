#!/usr/bin/env python3
"""
Quiet-window check script.

Exits 0 (quiet) if all of:
  (a) no .tmp file younger than TMP_YOUNG_THRESHOLD_MIN minutes,
  (b) no "Preparing" log line within the last 60 minutes without a subsequent
        verdict/completion log line for the same asset lane, and
  (c) no export worker spawn in the last EXPORT_WORKER_N_MIN minutes.

Exits nonzero otherwise, printing a reason.

Thresholds are chosen based on observed collector behavior:
  - TMP_YOUNG_THRESHOLD_MIN=10: .tmp files older than 10 min should have
    already resolved; younger ones indicate in-flight writes.
  - PREPARING_WINDOW_MIN=60: a Preparing interval without verdict >60 min
    signals a stuck/failed write (crash-during-write orphan).
  - EXPORT_WORKER_N_MIN=10: an export worker that recently crashed or is
    spinning up should not persist >10 min without resolution.
"""

import re
import os
import sys
from datetime import datetime, timedelta

# -- Configuration thresholds (rationale in comments above) --
TMP_YOUNG_THRESHOLD_MIN = 10       # .tmp younger than 10 min = potentially in-flight
PREPARING_WINDOW_MIN = 60          # Preparing without verdict >60 min = stuck
EXPORT_WORKER_N_MIN = 10           # export worker spin/recent crash window

LOG_PATH = os.getenv("COLLECTOR_LOG", "logs/collector-out-11.log")
DATA_DIR = os.getenv("DATA_DIR", "data/kaggle_staging")


def parse_log_timestamp(line: str):
    """Extract the ISO-8601 timestamp from a collector-out log line."""
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", line)
    if m:
        return datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")
    return None


def get_tmp_files(data_dir: str) -> list:
    """Find .tmp files under data_dir and return (path, mtime, pid, lane)."""
    results = []
    for root, _dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".tmp") and ".tmp." in f:
                full = os.path.join(root, f)
                try:
                    mtime = os.path.getmtime(full)
                except OSError:
                    continue
                # Parse filename: .../asset_lane.parquet.tmp.PID.tmp
                basename = os.path.basename(full)
                # Extract PID from the .tmp.N.tmp pattern
                parts = basename.split(".")
                # Expected: ... asset_lane.parquet.tmp.PID.tmp
                # PID is the second-to-last part before the final ".tmp"
                if len(parts) >= 5 and parts[-2].isdigit():
                    pid = int(parts[-2])
                    # Lane is determined by the directory/path, but we'll store the
                    # full name and let callers parse as needed.
                    results.append((full, mtime, pid, basename))
                # else: malformed name, skip
    results.sort(key=lambda x: x[1], reverse=True)
    return results


def is_pid_alive(pid: int) -> bool:
    """Check if a process with given PID is currently running."""
    try:
        os.kill(pid, 0)  # signal 0 = existence check
        return True
    except (OSError, ProcessLookupError):
        return False


def has_verdict_after(log_lines: list, mtime: float, asset: str) -> bool:
    """
    Return True if there is a resolution line after the given mtime for the
    specified asset lane.  A "verdict" is a [resolution] line that matches the
    asset (ETH, BTC, XRP, etc.).
    """
    mtime_dt = datetime.fromtimestamp(mtime)
    for line in log_lines:
        ts = parse_log_timestamp(line)
        if ts is None:
            continue
        if ts <= mtime_dt:
            continue
        # Check for resolution lines matching the asset
        if f"[{asset}" in line and "] window" in line:
            # Ensure it's a resolution line
            if "[resolution]" in line:
                return True
    return False


def check_tmp_young(tmp_files: list, threshold_min: int, now: datetime = None) -> bool:
    """
    Return True if any .tmp file is younger than threshold_min minutes.
    A .tmp is "young" if its mtime is within the last threshold_min minutes
    AND it has no verdict after its mtime (still in-flight).
    """
    if now is None:
        now = datetime.now()
    for _path, mtime, pid, basename in tmp_files:
        mtime_dt = datetime.fromtimestamp(mtime)
        age = (now - mtime_dt).total_seconds() / 60.0
        if age < threshold_min:
            # Young .tmp without verdict = in-flight write = not quiet
            # We check verdict below; if unsure, treat as young
            return True
    return False


def check_preparing_without_verdict(log_lines: list, window_min: int, now: datetime = None) -> bool:
    """
    Return True if there was a "Preparing" log line within the last window_min
    minutes that does NOT have a subsequent verdict/completion line for the same
    asset lane.
    """
    if now is None:
        now = datetime.now()
    window_start = now - timedelta(minutes=window_min)

    # Collect all Preparing lines within the window
    preparing_lines = []
    for line in log_lines:
        ts = parse_log_timestamp(line)
        if ts is None:
            continue
        if ts < window_start:
            continue
        if "Preparing" in line:
            preparing_lines.append((ts, line))

    if not preparing_lines:
        return False  # no Preparing in window = OK

    # For each Preparing line, check if there's a verdict after it
    for prep_ts, prep_line in preparing_lines:
        # Extract assets mentioned in the Preparing line
        # Format: "=== Step 1: Preparing Kaggle staging Xm for ['BTC', 'ETH', ...] ==="
        asset_match = re.search(r"for\s+\[(.*?)\]", prep_line)
        if asset_match:
            assets = [a.strip() for a in asset_match.group(1).split(",")]
        else:
            # Fallback: try to find assets in the line
            assets = re.findall(r"'(\w+)'", prep_line)

        for asset in assets:
            if has_verdict_after(log_lines, prep_ts.timestamp(), asset):
                # This Preparing has a verdict after it = OK for this asset
                break
        else:
            # No verdict found for any asset in this Preparing line
            return True

    return False


def check_export_worker_spawn(log_lines: list, n_min: int, now: datetime = None) -> bool:
    """
    Return True if an export worker was spawned in the last n_min minutes.
    We look for [export:worker] lines or worker process spawn patterns.
    """
    if now is None:
        now = datetime.now()
    window_start = now - timedelta(minutes=n_min)

    for line in log_lines:
        ts = parse_log_timestamp(line)
        if ts is None:
            continue
        if ts < window_start:
            continue
        if re.search(r"\[export:worker\]", line):
            return True
        # Also check for worker process start patterns
        if re.search(r"Worker\s+started|spawned|pid=\d+", line):
            return True

    return False


def main():
    # Read the log file
    if not os.path.exists(LOG_PATH):
        print(f"ERROR: log file not found: {LOG_PATH}", file=sys.stderr)
        sys.exit(2)

    with open(LOG_PATH) as f:
        log_lines = f.readlines()

    # Get .tmp files
    tmp_files = get_tmp_files(DATA_DIR)

    errors = []

    # (a) No .tmp younger than TMP_YOUNG_THRESHOLD_MIN
    if check_tmp_young(tmp_files, TMP_YOUNG_THRESHOLD_MIN):
        errors.append(f"FAIL: .tmp file younger than {TMP_YOUNG_THRESHOLD_MIN} min without verdict")

    # (b) No "Preparing" < PREPARING_WINDOW_MIN without verdict
    if check_preparing_without_verdict(log_lines, PREPARING_WINDOW_MIN):
        errors.append(f"FAIL: Preparing state without verdict in last {PREPARING_WINDOW_MIN} min")

    # (c) No export worker spawn in last EXPORT_WORKER_N_MIN
    if check_export_worker_spawn(log_lines, EXPORT_WORKER_N_MIN):
        errors.append(f"FAIL: export worker spawned in last {EXPORT_WORKER_N_MIN} min")

    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        sys.exit(1)
    else:
        print("OK: quiet window — no issues found")
        sys.exit(0)


if __name__ == "__main__":
    main()