"""Read-only probe: live% (book_snapshots) + 1006-episode rate (resync_episodes).

Reads parquet only, never the live collector. Bounded samples (last-N files)
so it finishes in seconds — never a full-dataset scan.
Usage: python3 scripts/measure_1006_rate.py [date=2026-10-03] [n_ep_files=400]
"""
import glob
import os
import sys
from collections import Counter
from datetime import datetime

import pyarrow.parquet as pq

date = sys.argv[1] if len(sys.argv) > 1 else "2026-10-03"
n_ep = int(sys.argv[2]) if len(sys.argv) > 2 else 400

tot, rows = Counter(), 0  # live% from last-3 files per asset
for asset in sorted(os.listdir(f"data/book_snapshots_500ms/date={date}")):
    fs = sorted(glob.glob(f"data/book_snapshots_500ms/date={date}/{asset}/*.parquet"))[-3:]
    for f in fs:
        for r in pq.read_table(f, columns=["book_state"]).to_pylist():
            tot[r["book_state"]] += 1
            rows += 1
print(f"live% {100.0 * tot.get('live', 0) / max(1, rows):.2f} "
      f"stale={tot.get('stale', 0)} live={tot.get('live', 0)} "
      f"resyncing={tot.get('resyncing', 0)} n={rows}")

fs = sorted(glob.glob(f"data/resync_episodes/date={date}/*.parquet"))[-n_ep:]
reasons, ts, n1006, n = Counter(), [], 0, 0  # 1006-episode rate over sample span
for f in fs:
    for r in pq.read_table(f, columns=["disconnect_reason", "disconnect_ts_utc"]).to_pylist():
        n += 1
        dr = str(r["disconnect_reason"])
        reasons["1006" if "1006" in dr else dr[:40]] += 1
        n1006 += "1006" in dr
        try:
            ts.append(datetime.fromisoformat(str(r["disconnect_ts_utc"]).replace("Z", "+00:00")))
        except Exception:
            pass
span_h = (max(ts) - min(ts)).total_seconds() / 3600 if len(ts) > 1 else 0
print(f"episodes n={n} n1006={n1006} span_h={span_h:.2f} "
      f"ep1006/h={n1006 / max(span_h, 1e-9):.1f} ep_total/h={n / max(span_h, 1e-9):.1f}")
print("top reasons:", reasons.most_common(8))
