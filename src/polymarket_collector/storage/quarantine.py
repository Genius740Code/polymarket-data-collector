"""Quarantine unreadable parquet stubs — Real-Data-Only gap healing.

A SIGKILL mid-flush (seen 2026-09-13 LOW: 5 footer-less finals at
1789331692*) leaves non-zero-byte files whose footer never landed.
`pyarrow: Parquet magic bytes not found in footer`. The export fail-closes
on them forever (`export.py` pre-validation gate), so the next tick aborts
identically until they are moved aside.

This helper MOVES (never deletes) every `*.parquet` (non-`.tmp`) that
`pq.read_metadata` cannot open into `<data_dir>/_quarantine/<relpath>`,
preserving relative layout for audit. 0-byte files are moved too — they
used to slip the `failed_bytes`-only gate and ship silently partial
staging. WAL-replayed rows already live in post-restart flush files, so
quarantine loses nothing; the 16h outage gap stays honestly represented
by the existing `coverage_gap` / `resync_episodes` / `collector_events`
rows (never interpolated).

Usage:
    python -m polymarket_collector.storage.quarantine ./data-weather-low
    python -m polymarket_collector.storage.quarantine ./data-weather-low --dry-run
"""
from __future__ import annotations

import argparse
from pathlib import Path


def quarantine_unreadable(data_dir: str | Path, dry_run: bool = False) -> list[dict]:
    """Move unreadable parquet files aside. Returns [{path, size, error, quarantined_to}]."""
    import pyarrow.parquet as pq

    base = Path(data_dir)
    moved: list[dict] = []
    if not base.exists():
        print(f"[quarantine] data dir missing: {base}")
        return moved
    for p in sorted(base.rglob("*.parquet")):
        if p.name.endswith(".tmp"):
            continue
        if "_quarantine" in p.parts:
            continue
        try:
            pq.read_metadata(str(p))
            continue  # readable — leave alone
        except Exception as e:
            err = str(e)[:200]
            try:
                size = p.stat().st_size
            except OSError:
                size = -1
            rel = p.relative_to(base)
            dest = base / "_quarantine" / rel
            print(f"[quarantine] unreadable {rel} ({size}B): {err}")
            if dry_run:
                moved.append({"path": str(p), "size": size, "error": err,
                              "quarantined_to": str(dest) + " (dry-run)"})
                continue
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                import os as _os
                _os.replace(str(p), str(dest))
                moved.append({"path": str(p), "size": size, "error": err,
                              "quarantined_to": str(dest)})
                print(f"[quarantine] moved -> {dest}")
            except Exception as me:
                print(f"[quarantine] WARN could not move {p}: {me}")
    if not moved:
        print("[quarantine] no unreadable files — nothing to do")
    else:
        print(f"[quarantine] quarantined {len(moved)} file(s); gaps remain honest "
              f"(see coverage_gap/resync_episodes/collector_events)")
    return moved


def main() -> None:
    ap = argparse.ArgumentParser(description="Move unreadable parquet stubs to _quarantine/")
    ap.add_argument("data_dir", help="hive root, e.g. ./data-weather-low")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    quarantine_unreadable(args.data_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
