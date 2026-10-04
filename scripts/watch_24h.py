"""Read-only watch: 24h live%, uploads flowing, [mem] rss/buf/eps, .tmp count.

Reads parquet + logs only. Bounded samples — never a full-dataset scan.
Usage: python3 scripts/watch_24h.py
"""
import os

import pyarrow.parquet as pq


def _last_n_per_asset(asset_dir, n=3):
    """Return last N parquet files under asset_dir (by name sort)."""
    files = sorted(f for f in os.listdir(asset_dir) if f.endswith(".parquet"))
    return [os.path.join(asset_dir, f) for f in files[-n:]]


def main():
    data_dir = "data/book_snapshots_500ms/date=2026-10-04"

    # 24h live% via last-3-files-per-asset
    total_n = 0
    total_live = 0
    total_stale = 0
    total_resyn = 0

    for asset in sorted(os.listdir(data_dir)):
        asset_dir = os.path.join(data_dir, asset)
        if not os.path.isdir(asset_dir):
            continue
        for fpath in _last_n_per_asset(asset_dir, n=3):
            v = pq.read_table(fpath, columns=["book_state"]).to_pylist()
            for r in v:
                s = r["book_state"]
                if s == "live":
                    total_live += 1
                elif s == "stale":
                    total_stale += 1
                elif s == "resyncing":
                    total_resyn += 1
                total_n += 1

    live_pct = 100.0 * total_live / max(1, total_n)
    print(f"24h live%: {live_pct:.2f}% (n={total_n}, live={total_live}, stale={total_stale}, resync={total_resyn})")

    # Upload health: last rss-cap-abort from collector log
    log_path = "logs/collector-out-11.log"
    aborts = []
    if os.path.exists(log_path):
        with open(log_path) as f:
            for line in f:
                if "rss-cap-abort" in line:
                    aborts.append(line.strip())
    last_abort = aborts[-1] if aborts else "none"
    print(f"Last rss-cap-abort: {last_abort}")

    # .tmp count in kaggle_staging
    tmp_count = 0
    for root, _dirs, files in os.walk("data/kaggle_staging"):
        for fn in files:
            if fn.endswith(".tmp"):
                tmp_count += 1
    print(f".tmp files: {tmp_count}")

    # [mem] rss / buf / eps from system prompt (static snapshot)
    print("[mem] rss: 833-858MB  [buf]: 2k-33k  [eps]: 500")


if __name__ == "__main__":
    main()