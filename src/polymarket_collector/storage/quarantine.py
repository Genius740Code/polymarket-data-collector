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


AUDIT_STATE_NAME = "quarantine_audit.json"


def quarantine_unreadable_bounded(
    data_dir: str | Path,
    dry_run: bool = False,
    mtime_lookback_s: float = 120.0,
    time_budget_s: float = 90.0,
) -> dict:
    """Incremental wall-clock-budgeted variant of quarantine_unreadable().

    Why (2026-10-17 incident): the export path calls the unbounded audit on
    EVERY lane tick, and it `rglob`s the whole hive (~40k+ parquet files)
    with one footer read per file — measured 312s for just 4 of 10 dataset
    dirs; the 08:51/09:00 ticks never finished their pass and Kaggle went
    ~20h without a verified upload, which blocks the verified-upload prune
    and re-creates the ENOSPC crash-loop outage.

    Safety (real-data-only policy — same invariants as quarantine_unreadable):
    - Never fabricates rows; it only MOVES footer-unreadable files into
      data/_quarantine/ (never deletes, never rewrites live data).
    - Fail-closed property retained for the files that can actually break
      the current staging build: every file whose mtime is newer than the
      last audit-weekmark (minus the lookback overlap) is footer-checked
      on this tick. Files verified readable at an earlier tick are not
      re-read — they were readable on disk then, and only a writer crash
      produces footer-less stubs, and every such stub is written with a
      fresh mtime, so it is covered by the watermark window.
    - A wall-clock budget bounds the tick; if exceeded, the watermark is
      NOT advanced, so the next tick re-checks the same window (a stub is
      never skipped past by the budget, only re-planned).

    State: <data_dir>/watchdog/quarantine_audit.json — atomic tmp+rename.
    First-ever run on a data_dir performs one full audit (records complete)
    and persists the watermark; process restarts do not repeat it.

    Returns {"moved": [...], "scanned": int, "skip_mtime_hint_s": float,
    "since_ns": int, "watermark_ns": int, "budget_exhausted": bool,
    "seconds": float}.
    """
    import json as _json
    import os as _os
    import time as _time

    import pyarrow.parquet as pq

    base = Path(data_dir)
    started = _time.monotonic()
    now_ns = _time.time() * 1e9

    state_dir = base / "watchdog"
    state_path = state_dir / AUDIT_STATE_NAME
    state: dict = {}
    if state_path.exists():
        try:
            state = _json.loads(state_path.read_text())
        except Exception as e:
            print(f"[quarantine] WARN audit state unreadable ({e}); full audit")
            state = {}
    since_ns = int(state.get("watermark_ns") or 0)
    full_audit = since_ns <= 0
    lookback_ns = int(mtime_lookback_s * 1e9)
    if not full_audit:
        # Overlap window: a stub flushed while the previous scan was still
        # enumerating could carry an mtime older than that scan's start —
        # subtracting the lookback makes the NEXT scan re-check that border
        # instead of silently skipping past a real stub.
        since_ns = max(0, since_ns - lookback_ns)

    moved: list[dict] = []
    scanned = 0
    budget_exhausted = False
    if not base.exists():
        return {"moved": moved, "scanned": 0, "since_ns": since_ns,
                "watermark_ns": 0, "budget_exhausted": False,
                "seconds": _time.monotonic() - started}

    def _budget_left() -> float:
        return time_budget_s - (_time.monotonic() - started)

    for p in sorted(base.rglob("*.parquet")):
        if p.name.endswith(".tmp"):
            continue
        if "_quarantine" in p.parts:
            continue
        try:
            st = p.stat()
        except OSError:
            # File vanished mid-scan — a later tick re-checks the window.
            continue
        # Footer-read gate: only candidate files inside the watermark window.
        # Unreadable-stub detection REQUIRES the footer read, but readable-
        # since-the-last-tick files are re-read every tick today at ~5-8 min
        # of pure footer I/O per pass; stat() alone is ~1000x cheaper.
        if not full_audit and st.st_mtime_ns < since_ns:
            continue
        if _budget_left() <= 0:
            budget_exhausted = True
            break
        scanned += 1
        try:
            pq.read_metadata(str(p))
            continue  # readable — leave alone
        except Exception as e:
            err = str(e)[:200]
            size = st.st_size
            rel = p.relative_to(base)
            dest = base / "_quarantine" / rel
            print(f"[quarantine] unreadable {rel} ({size}B): {err}")
            if dry_run:
                moved.append({"path": rel, "size": size, "error": err,
                              "quarantined_to": str(dest) + " (dry-run)"})
                continue
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                _os.replace(str(p), str(dest))
                moved.append({"path": rel, "size": size, "error": err,
                              "quarantined_to": str(dest)})
                print(f"[quarantine] moved -> {dest}")
            except Exception as me:
                print(f"[quarantine] WARN could not move {p}: {me}")

    seconds = _time.monotonic() - started
    # Advance the watermark only on a complete (non-budget-exhausted) scan,
    # and leave the lookback margin unscanned-territory: files written while
    # this scan was running can carry an mtime inside the margin, so the
    # next tick re-checks them — a stub is never skipped past.
    # Fail-closed: an exhausted scan does NOT advance the watermark, so the
    # next tick re-audits the same window instead of claiming coverage it
    # did not do.
    new_watermark = max(0, int(now_ns - lookback_ns)) if not budget_exhausted else 0
    if not budget_exhausted:
        state = {"watermark_ns": int(new_watermark), "since_ns": int(since_ns),
                 "scanned": int(scanned), "seconds": round(seconds, 3),
                 "ts_utc": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())}
        try:
            state_dir.mkdir(parents=True, exist_ok=True)
            tmp_state = state_path.with_name(state_path.name + ".tmp")
            tmp_state.write_text(_json.dumps(state))
            _os.replace(str(tmp_state), str(state_path))
        except Exception as e:
            print(f"[quarantine] WARN could not persist audit state: {e}")
            # without a persisted watermark the next tick re-audits the full
            # tree — correct (fail-closed), merely slower.
            new_watermark = 0
    else:
        print(f"[quarantine] audit budget {time_budget_s}s exhausted after {scanned} "
              f"scanned ({seconds:.1f}s); window re-checked next tick")

    if not moved:
        print(f"[quarantine] no unreadable files in window{'' if full_audit else ' (incremental)'} — nothing to do")
    else:
        print(f"[quarantine] quarantined {len(moved)} file(s); gaps remain honest "
              f"(see coverage_gap/resync_episodes/collector_events)")

    if full_audit and not budget_exhausted:
        print(f"[quarantine] full audit baseline recorded (scanned={scanned}, {seconds:.1f}s)")

    return {"moved": moved, "scanned": scanned, "since_ns": since_ns,
            "watermark_ns": int(new_watermark), "budget_exhausted": budget_exhausted,
            "seconds": round(seconds, 3)}


def reap_quarantine(
    data_dir: str | Path,
    max_age_hours: float = 72,
    max_total_bytes: int = 1_073_741_824,
    dry_run: bool = False,
) -> dict:
    """Boundedly delete aged/over-cap files under <data_dir>/_quarantine/.

    2026-09-20 DISK-FULL FIX: the verified-upload prune MOVES files here
    (same filesystem, was direct unlink) and nothing ever deleted them, so
    every "prune" freed 0 bytes while the hive kept growing (~500MB/h on
    35 lanes) until ENOSPC killed the collector. The quarantine is a review
    buffer, not an archive: files older than max_age_hours are deleted, then
    oldest-first until the total is under max_total_bytes.

    Safety (real-data-only policy):
    - ONLY touches files under <data_dir>/_quarantine/ — the live hive is
      never scanned or modified here.
    - Prune-moved files were deleted only after a VERIFIED Kaggle upload, so
      history survives in Kaggle versions (delete_old_versions=False).
    - Unreadable stubs moved by quarantine_unreadable() were never readable
      (no footer); their rows survive via WAL replay. The age grace gives
      operators a manual-review window before they are dropped.
    - Gaps stay honest: nothing is interpolated, fabricated, or hidden —
      coverage_gap / resync_episodes / collector_events rows are untouched.

    Returns {"files_deleted": int, "bytes_deleted": int, "files_kept": int,
    "bytes_kept": int} (deleted counts are would-delete under dry_run).
    """
    import time as _time

    base = Path(data_dir)
    stats = {"files_deleted": 0, "bytes_deleted": 0, "files_kept": 0, "bytes_kept": 0}
    qdir = base / "_quarantine"
    if not qdir.exists():
        return stats
    now = _time.time()
    try:
        entries: list = []
        for p in sorted(qdir.rglob("*")):
            try:
                if not p.is_file() or p.is_symlink():
                    continue
            except OSError:
                continue
            try:
                st = p.stat()
            except OSError:
                continue
            entries.append((p, st.st_size, st.st_mtime))
    except Exception as e:
        print(f"[quarantine-reap] WARN could not scan {qdir}: {e}")
        return stats

    def _delete(p: Path, size: int, reason: str) -> None:
        if dry_run:
            try:
                _rel = p.relative_to(base)
            except Exception:
                _rel = p
            print(f"[quarantine-reap] dry-run would delete {_rel} ({size}B, {reason})")
            stats["files_deleted"] += 1
            stats["bytes_deleted"] += size
            return
        try:
            p.unlink()
            stats["files_deleted"] += 1
            stats["bytes_deleted"] += size
            print(f"[quarantine-reap] deleted {p.relative_to(base)} ({size}B, {reason})")
        except OSError as e:
            print(f"[quarantine-reap] WARN could not delete {p.relative_to(base)}: {e}")
            stats["files_kept"] += 1
            stats["bytes_kept"] += size

    # Pass 1: age — anything older than the review grace goes.
    survivors: list = []
    try:
        max_age_s = float(max_age_hours) * 3600.0
    except Exception:
        max_age_s = 72 * 3600.0
    for p, size, mtime in entries:
        try:
            age_s = now - mtime
            age_h = age_s / 3600.0
        except Exception:
            age_s, age_h = 0.0, 0.0
        if age_s > max_age_s:
            _delete(p, size, f"age {age_h:.1f}h > {max_age_hours}h")
        else:
            survivors.append((p, size, mtime))
    # Pass 2: size cap — oldest-first until under budget (age grace does not
    # protect against the cap: a full disk kills collection, kept files lose).
    try:
        cap = int(max_total_bytes)
    except Exception:
        cap = 1_073_741_824
    if cap >= 0:
        total = sum(s for _, s, _ in survivors)
        if total > cap:
            for p, size, mtime in sorted(survivors, key=lambda e: e[2]):
                if total <= cap:
                    break
                try:
                    age_h = (now - mtime) / 3600.0
                except Exception:
                    age_h = 0.0
                _delete(p, size, f"over cap ({total}B > {cap}B, age {age_h:.1f}h)")
                total -= size
                survivors = [(q, s, m) for (q, s, m) in survivors if q != p]
    for _, size, _ in survivors:
        stats["files_kept"] += 1
        stats["bytes_kept"] += size
    if stats["files_deleted"] or stats["bytes_deleted"]:
        print(f"[quarantine-reap] {'would delete' if dry_run else 'deleted'} "
              f"{stats['files_deleted']} files / {stats['bytes_deleted']}B "
              f"(kept {stats['files_kept']} files / {stats['bytes_kept']}B)")
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Move unreadable parquet stubs to _quarantine/")
    ap.add_argument("data_dir", help="hive root, e.g. ./data-weather-low")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--reap", action="store_true",
                    help="also bound the _quarantine/ dir (delete aged/over-cap files)")
    ap.add_argument("--max-age-hours", type=float, default=72,
                    help="quarantine review grace before age-deletion (default 72)")
    ap.add_argument("--max-bytes", type=int, default=1_073_741_824,
                    help="quarantine size cap in bytes, oldest-first (default 1GiB)")
    args = ap.parse_args()
    quarantine_unreadable(args.data_dir, dry_run=args.dry_run)
    if args.reap:
        reap_quarantine(args.data_dir, max_age_hours=args.max_age_hours,
                        max_total_bytes=args.max_bytes, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
