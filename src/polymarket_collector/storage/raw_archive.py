"""Short-retention raw WebSocket archive — §13.

Rolling 24-48h buffer of raw, unprocessed WS messages. For replay/re-derive
after a bug in event-detection/snapshot logic. NOT an outage backfill (§13
scope note: only replays messages actually received; gaps where nothing was
captured need a secondary source like Dome API).
"""
from __future__ import annotations

import datetime
from .. import jsonfast as json
import time
from pathlib import Path
from typing import Any, Dict


class RawArchive:
    """Append raw messages as JSONL, partitioned by date/asset, with TTL pruning."""

    def __init__(self, base_path: str | Path, retention_hours: int = 36, enabled: bool = True):
        self.base = Path(base_path)
        self.retention_hours = retention_hours
        self.enabled = enabled
        if self.enabled:
            self.base.mkdir(parents=True, exist_ok=True)
        # PERF: cache current-hour buffered handle per asset (was mkdir +
        # open/close per WS frame). Same lines, same partitioning; rotate on
        # hour/date. Flush per line preserves process-crash durability
        # (open/close had no fsync either, so identical guarantees).
        self._handles: Dict[str, Any] = {}
        self._handle_paths: Dict[str, Path] = {}

    def _partition_path(self, asset: str, ts_ms: int | None = None) -> Path:
        dt = datetime.datetime.fromtimestamp((ts_ms or int(time.time() * 1000)) / 1000, tz=datetime.timezone.utc)
        date_str = dt.date().isoformat()
        d = self.base / f"date={date_str}" / f"asset={asset.upper()}"
        d.mkdir(parents=True, exist_ok=True)
        hour = dt.strftime("%H")
        return d / f"raw-{date_str}T{hour}.jsonl"

    def _handle_for(self, asset_u: str, path: Path):
        """Return cached buffered handle for path, rotating on hour change."""
        key = asset_u
        cur = self._handle_paths.get(key)
        fh = self._handles.get(key)
        if fh is not None and cur is not None and cur == path:
            try:
                if not getattr(fh, "closed", False):
                    return fh
            except Exception:
                pass
        # rotate: close stale handle for this asset
        if fh is not None:
            try:
                try:
                    fh.flush()
                except Exception:
                    pass
                fh.close()
            except Exception:
                pass
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        try:
            fh = open(path, "a", encoding="utf-8", buffering=8192)
        except Exception:
            return None
        self._handles[key] = fh
        self._handle_paths[key] = path
        return fh

    def close(self) -> None:
        for fh in list(self._handles.values()):
            try:
                try:
                    fh.flush()
                except Exception:
                    pass
                fh.close()
            except Exception:
                pass
        self._handles.clear()
        self._handle_paths.clear()

    def append(self, asset: str, raw_msg: Dict[str, Any] | str, ts_ms: int | None = None) -> None:
        if not self.enabled:
            return
        try:
            asset_u = asset.upper()
        except Exception:
            asset_u = asset
        # Compute partition without per-frame mkdir (handle path mkdirs on rotate).
        try:
            dt = datetime.datetime.fromtimestamp((ts_ms or int(time.time() * 1000)) / 1000, tz=datetime.timezone.utc)
            date_str = dt.date().isoformat()
            hour = dt.strftime("%H")
            path = self.base / f"date={date_str}" / f"asset={asset_u}" / f"raw-{date_str}T{hour}.jsonl"
        except Exception:
            try:
                path = self._partition_path(asset, ts_ms)
            except Exception:
                return
        # PERF 2026-09-12 (#2): single json.dumps per frame (was 2-3x dumps +
        # a json.loads round-trip). Same line content: {ts_received_ns, payload}.
        # NOTE: dict branch omits "asset", str branch includes it — preserved exactly.
        try:
            if isinstance(raw_msg, (dict, list)):
                line = json.dumps({"ts_received_ns": time.time_ns(), "payload": raw_msg}, default=str)
            else:
                s = raw_msg
                try:
                    payload = json.loads(s) if s.startswith("{") else s
                except Exception:
                    payload = s
                line = json.dumps({"ts_received_ns": time.time_ns(), "asset": asset_u, "payload": payload}, default=str)
        except Exception:
            return
        fh = None
        try:
            fh = self._handle_for(asset_u, path)
        except Exception:
            fh = None
        if fh is not None:
            try:
                fh.write(line + "\n")
                fh.flush()
                return
            except Exception:
                pass
        try:
            with open(path, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def prune(self) -> int:
        """Delete files older than retention_hours. Returns count deleted."""
        if not self.enabled or not self.base.exists():
            return 0
        # Flush + close handles so mtimes are current and deleted files
        # are not held open.
        try:
            self.close()
        except Exception:
            pass
        cutoff = time.time() - self.retention_hours * 3600
        deleted = 0
        for p in self.base.rglob("raw-*.jsonl"):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
                    deleted += 1
            except Exception:
                continue
        # remove empty date/asset dirs
        for d in sorted(self.base.rglob("*"), reverse=True):
            try:
                if d.is_dir() and not any(d.iterdir()):
                    d.rmdir()
            except Exception:
                continue
        return deleted

    def replay(self, asset: str, date_str: str | None = None):
        """Yield raw messages for a given asset/date (for re-derive)."""
        if not self.base.exists():
            return
        try:
            # Ensure buffered handles are visible to the reader.
            for fh in list(self._handles.values()):
                try:
                    fh.flush()
                except Exception:
                    pass
        except Exception:
            pass
        pattern = self.base / (f"date={date_str}" if date_str else "date=*") / f"asset={asset.upper()}" / "raw-*.jsonl"
        for path in sorted(self.base.glob(str(pattern.relative_to(self.base)))):
            try:
                with open(path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        yield json.loads(line)
            except Exception:
                continue
