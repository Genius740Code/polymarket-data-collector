"""Export isolation: stable-snapshot staging reads, off the collection loop (a) WAL-cutoff manifest: begin_snapshot records cutoff_ts + visible file list at cycle start; readers honor mtime<=cutoff, newer rows stay buffered/flushed for the next cycle. No IPC, no hot-path change; any failure falls back to the old path or skips the cycle.
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

SNAPSHOT_FILENAME = "_snapshot.json"
DEFAULT_FLUSH_TIMEOUT_S = 30.0


def _utc_now() -> float:
    try:
        return time.time()
    except Exception:
        return 0.0


def is_visible(path: str | Path, cutoff_ts: float) -> bool:
    """True when a hive file belongs to the snapshot (mtime <= cutoff). Total: never raises."""
    try:
        if cutoff_ts is None:
            return True
        return Path(path).stat().st_mtime <= float(cutoff_ts)
    except Exception:
        return False


def visible_files(paths: List[str | Path], cutoff_ts: Optional[float]) -> List[Path]:
    """Filter a file list to the snapshot. Missing/unstatable files are dropped. Total."""
    try:
        if cutoff_ts is None:
            return [Path(p) for p in (paths or [])]
        return [Path(p) for p in (paths or []) if is_visible(p, cutoff_ts)]
    except Exception:
        return []


def snapshot_files(
    data_dir: str | Path,
    dataset: str,
    asset_upper: Optional[str] = None,
    cutoff_ts: Optional[float] = None,
    seal_grace_s: Optional[float] = None,
) -> List[Path]:
    """Hive source files for (dataset, asset) visible at cutoff_ts. Metadata only. Total.

    seal_grace_s: when set, the read-side seal also applies — files younger
    than the grace (the still-open flush window) are excluded. None keeps the
    legacy cutoff-only behavior (no rows dropped by default).
    """
    try:
        base = Path(data_dir)
        root = base / dataset
        if not root.exists():
            return []
        if asset_upper is None:
            pats = [p for p in root.rglob("*.parquet") if not p.name.endswith(".tmp")]
        else:
            pats = {p for p in root.glob(f"date=*/asset={asset_upper}/*.parquet")}
            pats.update(p for p in root.glob(f"date=*/asset={str(asset_upper).lower()}/*.parquet"))
            if not pats:
                pats = {p for p in root.rglob("*.parquet")}
            pats = {p for p in pats if not p.name.endswith(".tmp")}
        files = sorted(pats, key=str)
        if cutoff_ts is not None:
            files = [p for p in files if is_visible(p, cutoff_ts)]
        if seal_grace_s is not None:
            try:
                from .parquet_io import list_sealed_files as _sealed

                files = _sealed(files, seal_grace_s)
                files = sorted(files, key=str)
            except Exception:
                pass
        return files
    except Exception:
        return []


def manifest_for(
    data_dir: str | Path,
    dataset: str,
    asset_upper: Optional[str] = None,
    cutoff_ts: Optional[float] = None,
) -> Dict[str, Any]:
    """Metadata-only coverage manifest for the snapshot (n/bytes/digest). Total."""
    try:
        base = Path(data_dir)
        files: List[tuple] = []
        total = 0
        for p in snapshot_files(data_dir, dataset, asset_upper, cutoff_ts):
            try:
                st = p.stat()
            except OSError:
                continue
            try:
                rel = str(p.relative_to(base))
            except Exception:
                rel = p.name
            files.append((rel, st.st_size, st.st_mtime_ns))
            total += st.st_size
        files.sort()
        h = hashlib.sha1()
        for name, size, mt in files:
            h.update(f"{name}|{size}|{mt}\n".encode())
        return {"n": len(files), "bytes": total, "digest": h.hexdigest()[:16]}
    except Exception:
        return {"n": 0, "bytes": 0, "digest": "error"}


def _bounded_flush(
    flush_fn: Callable[[], Any],
    timeout_s: float,
) -> tuple:
    """Run flush_fn with a timeout. Returns (ok, result). Timeout never blocks the caller. Total."""
    try:
        timeout_s = float(timeout_s)
    except Exception:
        timeout_s = DEFAULT_FLUSH_TIMEOUT_S
    if timeout_s <= 0:
        timeout_s = DEFAULT_FLUSH_TIMEOUT_S
    box: Dict[str, Any] = {}

    def _run() -> None:
        try:
            box["result"] = flush_fn()
        except Exception as e:  # noqa: BLE001 - record, caller decides
            box["error"] = repr(e)[:300]

    try:
        t = threading.Thread(target=_run, daemon=True, name="export-isolation-flush")
        t.start()
        t.join(timeout=timeout_s)
        if t.is_alive():
            return False, {"reason": "flush_timeout", "timeout_s": timeout_s}
        if "error" in box:
            return False, {"reason": "flush_error", "error": box["error"]}
        return True, box.get("result", 0)
    except Exception as e:  # noqa: BLE001 - fail closed
        return False, {"reason": "flush_spawn_failed", "error": repr(e)[:200]}


def begin_snapshot(
    data_dir: str | Path,
    datasets: Optional[List[str]] = None,
    assets: Optional[List[str]] = None,
    flush_fn: Optional[Callable[[], Any]] = None,
    flush_timeout_s: float = DEFAULT_FLUSH_TIMEOUT_S,
    previous: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Open one isolated staging cycle. Total: never raises, never drops rows.

    With flush_fn: a bounded flush is attempted first so acknowledged
    RAM-buffer rows land in parquet before the cutoff is taken. On timeout
    (or flush error) the snapshot is NOT advanced — the caller must skip the
    cycle and keep prior staging; buffered rows stay durable in WAL/buffer
    for the next cycle (fail closed, no loss, no prune of gap evidence).
    Without flush_fn: cutoff is taken immediately (export layer has no writer
    handle; the collector flushes under its own lock before calling staging).
    """
    try:
        prev_cutoff = None
        try:
            prev_cutoff = float((previous or {}).get("cutoff_ts")) if (previous or {}).get("cutoff_ts") is not None else None
        except Exception:
            prev_cutoff = None
        flushed: Any = 0
        if flush_fn is not None:
            ok, res = _bounded_flush(flush_fn, flush_timeout_s)
            if not ok:
                return {
                    "ok": False,
                    "reason": (res or {}).get("reason", "flush_failed"),
                    "cutoff_ts": prev_cutoff,
                    "previous": previous,
                    "detail": res,
                }
            flushed = res
        cutoff_ts = _utc_now()
        manifests: Dict[str, Any] = {}
        try:
            ds_list = list(datasets) if datasets else []
            asset_list = [str(a).upper() for a in (assets or [])] or [None]
            for ds in ds_list:
                for au in asset_list:
                    key = f"{ds}/{au}" if au else f"{ds}/GLOBAL"
                    manifests[key] = manifest_for(data_dir, ds, au, cutoff_ts)
        except Exception:
            pass
        return {"ok": True, "cutoff_ts": cutoff_ts, "manifests": manifests, "flushed": flushed}
    except Exception as e:  # noqa: BLE001 - fail closed to old path
        try:
            prev = (previous or {}).get("cutoff_ts")
        except Exception:
            prev = None
        return {"ok": False, "reason": "snapshot_error", "cutoff_ts": prev, "error": repr(e)[:200]}


def write_snapshot_file(staging_parent: str | Path, snapshot: Dict[str, Any]) -> Optional[str]:
    """Record the cycle snapshot beside (never inside) the upload folder, via tmp+rename. Total."""
    try:
        parent = Path(staging_parent)
        parent.mkdir(parents=True, exist_ok=True)
        tmp = parent / (SNAPSHOT_FILENAME + ".tmp")
        final = parent / SNAPSHOT_FILENAME
        payload = {
            "cutoff_ts": (snapshot or {}).get("cutoff_ts"),
            "ok": bool((snapshot or {}).get("ok")),
            "reason": (snapshot or {}).get("reason"),
            "flushed": (snapshot or {}).get("flushed", 0),
            "manifests": (snapshot or {}).get("manifests", {}),
        }
        tmp.write_text(json.dumps(payload, indent=2))
        import os as _os

        _os.replace(str(tmp), str(final))
        return str(final)
    except Exception:
        return None
