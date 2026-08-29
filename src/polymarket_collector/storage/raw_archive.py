"""Short-retention raw WebSocket archive — §13.

Rolling 24-48h buffer of raw, unprocessed WS messages. For replay/re-derive
after a bug in event-detection/snapshot logic. NOT an outage backfill (§13
scope note: only replays messages actually received; gaps where nothing was
captured need a secondary source like Dome API).
"""
from __future__ import annotations

import datetime
import json
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

    def _partition_path(self, asset: str, ts_ms: int | None = None) -> Path:
        dt = datetime.datetime.fromtimestamp((ts_ms or int(time.time() * 1000)) / 1000, tz=datetime.timezone.utc)
        date_str = dt.date().isoformat()
        d = self.base / f"date={date_str}" / f"asset={asset.upper()}"
        d.mkdir(parents=True, exist_ok=True)
        hour = dt.strftime("%H")
        return d / f"raw-{date_str}T{hour}.jsonl"

    def append(self, asset: str, raw_msg: Dict[str, Any] | str, ts_ms: int | None = None) -> None:
        if not self.enabled:
            return
        path = self._partition_path(asset, ts_ms)
        entry = raw_msg if isinstance(raw_msg, str) else json.dumps(raw_msg, default=str)
        # enrich with receive timestamp
        envelope = json.dumps({"ts_received_ns": time.time_ns(), "asset": asset.upper(), "payload": raw_msg if isinstance(raw_msg, dict) else json.loads(entry) if entry.startswith("{") else entry})
        # if raw_msg was dict, store as JSON line directly with added ts
        line = json.dumps({"ts_received_ns": time.time_ns(), "payload": raw_msg}) if isinstance(raw_msg, dict) else envelope
        with open(path, "a") as f:
            f.write(line + "\n")

    def prune(self) -> int:
        """Delete files older than retention_hours. Returns count deleted."""
        if not self.enabled or not self.base.exists():
            return 0
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
