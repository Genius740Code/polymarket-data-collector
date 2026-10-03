#!/usr/bin/env python3
"""Read-only export-impact probe: is live% flat across a staging build?
Never full-scans: last-3 files/asset, book_state column only. Read-only.
"""
import argparse
import glob
import re
from collections import Counter

import pyarrow.parquet as pq

ASSETS = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
MEMPAT = r"\[mem\]"


def live_pct(date):
    c = Counter()
    for a in ASSETS:
        fs = sorted(glob.glob(f"data/book_snapshots_500ms/date={date}/asset={a}/*.parquet"))
        for f in fs[-3:]:
            try:
                c.update(pq.read_table(f, columns=["book_state"]).column("book_state").to_pylist())
            except Exception:
                pass
    n = sum(c.values())
    return (100.0 * c.get("live", 0) / n if n else 0.0), n


def tail_grep(path, pat, last=400):
    try:
        with open(path, errors="replace") as fh:
            lines = fh.readlines()[-last:]
    except OSError:
        return None
    return next((ln.strip() for ln in reversed(lines) if re.search(pat, ln)), None)


def main():
    ap = argparse.ArgumentParser(description="live% flatness probe (read-only)")
    ap.add_argument("--date", default="2026-10-03")
    ap.add_argument("--log", default="logs/collector-out-0.log")
    ap.add_argument("--staging", default="data/kaggle_staging")
    ap.add_argument("--during", action="store_true", help="label sample as mid-build")
    ap.add_argument("--before", default=None, help="prior 'pct n' pair, e.g. '85.67 3321'")
    a = ap.parse_args()
    pct, n = live_pct(a.date)
    print(f"[{'during' if a.during else 'after'}] live%={pct:.2f} n={n} date={a.date}")
    tmp = len(glob.glob(f"{a.staging}/**/*.parquet.tmp", recursive=True))
    print(f"[staging] tmp_files={tmp} marker={tail_grep(a.log, 'Preparing.*staging|staging prepared')}")
    print(f"[mem] {tail_grep(a.log, MEMPAT)}")
    if a.before:
        try:
            bp, bn = a.before.split()
            print(f"[delta] live% {pct - float(bp):+.2f}pp n {n - int(bn):+d} vs before({a.before})")
        except ValueError:
            print("[delta] --before must look like '85.67 3321'")


if __name__ == "__main__":
    main()
