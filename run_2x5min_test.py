#!/usr/bin/env python3
"""One-command 2x5min live test + Kaggle upload (7 assets: BTC/ETH/SOL/HYPE/BNB/XRP/DOGE).

Usage:
    python run_2x5min_test.py             # wipe local data + delete Kaggle dataset + run
    python run_2x5min_test.py --keep-data # run without wiping anything
    run_2x5min_test.cmd                   # same as the first line (Windows double-click)

Steps:
  1. wipe local collected data (data/* dataset dirs, cursors, WAL, analyses)
  2. delete the Kaggle dataset gghgg1/polymarket-5m-crypto (tolerated if absent)
  3. run the collector in test mode: pre-warm, collect 2 x 5-min windows live,
     upload the 31 staging files to Kaggle as a fresh dataset, write analysis
  4. print the ground-truth summary (windows, completeness, nulls, upload)
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
KAGGLE_DATASET = "gghgg1/polymarket-5m-crypto"


def wipe_local_data() -> None:
    if not DATA_DIR.exists():
        print("[wipe] no local data dir — nothing to wipe")
        return
    removed = 0
    for p in sorted(DATA_DIR.iterdir()):
        try:
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
            removed += 1
        except Exception as e:
            print(f"[wipe] WARN could not remove {p.name}: {e}")
    print(f"[wipe] removed {removed} items from {DATA_DIR}")


def delete_kaggle_dataset() -> None:
    cmd = ["kaggle", "datasets", "delete", KAGGLE_DATASET, "--yes"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except FileNotFoundError:
        r = subprocess.run(
            [sys.executable, "-m", "kaggle", "datasets", "delete", KAGGLE_DATASET, "--yes"],
            capture_output=True, text=True, timeout=120,
        )
    out = (r.stdout + r.stderr).strip().splitlines()
    tail = out[-1] if out else ""
    if r.returncode == 0:
        print(f"[kaggle] deleted dataset {KAGGLE_DATASET}")
    else:
        print(f"[kaggle] dataset delete skipped ({tail or 'not present'}) — continuing")


def run_test() -> int:
    log_path = ROOT / f"test_run_{time.strftime('%Y%m%d_%H%M%S')}.log"
    cmd = [
        sys.executable, "-m", "polymarket_collector.cli",
        "--config", "config/collector.yaml",
        "--test-mode", "--test-markets", "2",
    ]
    print(f"[run] {' '.join(cmd)}")
    print(f"[run] log -> {log_path.name}")
    env = {**__import__("os").environ, "PYTHONUNBUFFERED": "1"}  # live log lines
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.Popen(cmd, cwd=str(ROOT), stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                                errors="replace", env=env)
        for line in proc.stdout:  # type: ignore[union-attr]
            sys.stdout.write(line)
            log.write(line)
            log.flush()
        code = proc.wait()
    print(f"[run] collector exited with code {code}, log saved to {log_path.name}")
    return code


def print_summary() -> None:
    for name in ("test_analysis_final.json", "test_analysis.json"):
        p = DATA_DIR / name
        if not p.exists():
            continue
        try:
            a = json.loads(p.read_text())
        except Exception as e:
            print(f"[summary] could not parse {name}: {e}")
            continue
        checks = a.get("checks", {})
        uploads = a.get("kaggle_uploads_during_test", [])
        print("\n================ RUN SUMMARY ================")
        print(f"analysis file        : {name}")
        print(f"finished_at_utc      : {a.get('finished_at_utc')}")
        print(f"discovered windows   : {checks.get('discovered_windows_total')}")
        print(f"expected snapshots   : {checks.get('expected_book_snapshots')}")
        print(f"actual snapshots     : {checks.get('actual_book_snapshots')}")
        print(f"snapshot completeness: {checks.get('snapshot_completeness_pct')}%")
        print(f"clean completeness   : {checks.get('clean_completeness_pct')}%")
        print(f"book_state histogram : {checks.get('book_state_histogram')}")
        print(f"trades rows          : {checks.get('trades_rows')}")
        print(f"chainlink rows       : {checks.get('chainlink_rows')}")
        print(f"collector events     : {checks.get('collector_events_by_type')}")
        print(f"kaggle uploads       : {len(uploads)}"
              + (f" (status: {[u.get('tag') for u in uploads]})" if uploads else ""))
        try:
            st = a.get("kaggle_staging", {})
            print(f"staging files        : {st.get('files')} (expected {st.get('expected')})")
        except Exception:
            pass
        print("=============================================\n")
        return
    print("[summary] no analysis file found — test did not complete?")


def run_post_test_finalize() -> None:
    """B-7: after the run, resolve everything ended (official CLOB outcome) and
    push the final Kaggle version carrying resolutions + enriched trades."""
    cmd = [sys.executable, "-m", "polymarket_collector.resolution_backfill",
           "--config", "config/collector.yaml", "--reupload"]
    print("[finalize] resolution backfill + final Kaggle version")
    env = {**__import__("os").environ, "PYTHONUNBUFFERED": "1"}
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
    if proc.returncode != 0:
        print(f"[finalize] backfill exited with {proc.returncode}")


def main() -> int:
    ap = argparse.ArgumentParser(description="One-command 2x5min live test + Kaggle upload")
    ap.add_argument("--keep-data", action="store_true",
                    help="do NOT wipe local data / delete the Kaggle dataset before the run")
    args = ap.parse_args()

    if args.keep_data:
        print("[skip] --keep-data given: local data and Kaggle dataset left untouched")
    else:
        print("[1/3] wiping local collected data...")
        wipe_local_data()
        print("[2/3] deleting Kaggle dataset (fresh start)...")
        delete_kaggle_dataset()
    print("[3/3] running 2x5min live test with Kaggle upload...")
    code = run_test()
    print_summary()
    run_post_test_finalize()
    return code


if __name__ == "__main__":
    raise SystemExit(main())
