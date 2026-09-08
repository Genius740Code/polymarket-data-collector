#!/usr/bin/env python3
"""Leak-hunt probe (session 2): run the collector on the plain prod path with
in-process counters + tracemalloc growth ranking, sampled from inside the event
loop. Writes real data to ./data per AGENTS.md (no synthetic, no deletes).

Usage: .venv/Scripts/python.exe leak_probe.py [--duration-sec 780] [--sample-sec 60]
Output: tee to logs/leak_probe_<ts>.log by the caller.
"""
from __future__ import annotations

import argparse
import asyncio
import ctypes
import ctypes.wintypes as wt
import gc
import os
import sys
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_collector.config import CollectorConfig  # noqa: E402
from polymarket_collector.collector import Collector  # noqa: E402


def rss_mb() -> float:
    """Ground-truth RSS from outside the Python heap (Windows psapi / Linux proc)."""
    if os.name == "nt":
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        h = ctypes.c_void_p(-1)  # GetCurrentProcess pseudo-handle
        class PMC(ctypes.Structure):
            _fields_ = [
                ("cb", wt.DWORD), ("PageFaultCount", wt.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]
        pmc = PMC()
        pmc.cb = ctypes.sizeof(PMC)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.c_void_p, wt.DWORD]
        if not psapi.GetProcessMemoryInfo(h, ctypes.byref(pmc), pmc.cb):
            return -1.0
        return pmc.WorkingSetSize / (1024 * 1024)
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


def counters(c: Collector) -> dict:
    resync = c.resync
    open_eps = [ep for ep in resync._episodes.values() if ep.resync_completed_ts_utc is None]
    buf_lens = {rid: len(q) for rid, q in resync._buffers.items()}
    pending = sum(len(b.pending_events) for b in c.books.values())
    seqs = sum(len(b.sequence_numbers) for b in c.books.values())
    return {
        "rss_mb": round(rss_mb(), 1),
        "books": len(c.books),
        "markets": len(c.markets),
        "books_pending_events": pending,
        "book_seq_entries": seqs,
        "chainlink_events": len(c._chainlink_events),
        "resync_episodes": len(resync._episodes),
        "resync_open_episodes": len(open_eps),
        "resync_buffers": len(resync._buffers),
        "resync_buffered_msgs": sum(buf_lens.values()),
        "resync_max_buffer": max(buf_lens.values()) if buf_lens else 0,
        "episode_latest": len(c._episode_latest),
        "episode_persisted": len(c._episode_persisted),
        "closed_cids": len(c._closed_cids),
        "resolved_cids": len(c._resolved_cids),
        "resolution_stuck_emitted": len(c._resolution_stuck_emitted),
        "heal_inflight": len(c._heal_inflight),
        "coverage_gapped": len(c._coverage_gapped),
        "conn_tokens": sum(len(v) for v in c._conn_tokens.values()),
        "ws_connected": sum(1 for v in c._ws_connected.values() if v),
    }


def top_growth(prev, cur, n=8):
    if prev is None:
        return []
    stats = {s.traceback[0]: s.size_diff for s in cur.compare_to(prev, "lineno")}
    # aggregate per file+line, keep only real growth
    items = [(k, v) for k, v in stats.items() if v > 512 * 1024]
    items.sort(key=lambda kv: -kv[1])
    return items[:n]


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration-sec", type=int, default=780)
    ap.add_argument("--sample-sec", type=int, default=60)
    args = ap.parse_args()

    cfg = CollectorConfig.load(str(ROOT / "config" / "collector.yaml"))
    print(f"[probe] config: assets={cfg.assets} timeframes={getattr(cfg, 'timeframes', '?')} "
          f"test_mode={cfg.test_mode.enabled} raw_archive={cfg.raw_archive.enabled} "
          f"data_dir={cfg.storage.data_dir} kaggle_upload_interval={cfg.kaggle.upload_interval_seconds}s")

    tracemalloc.start(1)
    c = Collector(cfg)
    await c.start()
    print(f"[probe] collector started at {datetime.now(timezone.utc).isoformat()}")

    prev_snap = None
    prev_counters = None
    t0 = time.monotonic()
    sample_no = 0
    try:
        while time.monotonic() - t0 < args.duration_sec:
            await asyncio.sleep(args.sample_sec)
            sample_no += 1
            gc.collect()
            snap = tracemalloc.take_snapshot()
            cnt = counters(c)
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"=== T+{time.monotonic() - t0:.0f}s sample {sample_no} ({ts}) ===")
            if prev_counters:
                deltas = {k: cnt[k] - prev_counters[k] for k in cnt if k != "rss_mb"}
                growing = {k: v for k, v in deltas.items() if v != 0}
                print(f"counters: {cnt}")
                print(f"deltas:   {growing if growing else '(all flat)'}")
                print(f"rss: {prev_counters['rss_mb']} -> {cnt['rss_mb']} MB "
                      f"(delta {cnt['rss_mb'] - prev_counters['rss_mb']:+.1f})")
            else:
                print(f"counters: {cnt}")
            for fname, growth in top_growth(prev_snap, snap):
                print(f"  tracemalloc +{growth / 1024 / 1024:.1f}MB  {fname}")
            prev_snap, prev_counters = snap, cnt
    finally:
        print(f"[probe] stopping at {datetime.now(timezone.utc).isoformat()}")
        await c.stop()
        print(f"[probe] final counters: {counters(c)}")
        print(f"[probe] final rss: {rss_mb():.1f} MB")
        tracemalloc.stop()


if __name__ == "__main__":
    asyncio.run(main())
