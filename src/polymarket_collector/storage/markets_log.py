"""Markets event-sourced log + compacted latest view — §9A.

Parquet is append-only; mutating resolution_outcome in place is unsafe with
concurrent readers. Solution: append-only markets_log + periodic compaction to
markets_latest (one row per condition_id, most recent state).
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from ..enums import MarketStatus, ResolutionOutcome


class MarketsLog:
    """Append-only log for market metadata (§2 + §6A settlement fields)."""

    def __init__(self, data_dir: str | Path, writer=None):
        self.data_dir = Path(data_dir)
        self.writer = writer  # optional ParquetWriter (batched)
        # small staging store for buffering before parquet flush (§9A)
        self._staging: List[Dict] = []

    def append(self, market: Dict, updated_at: Optional[str] = None) -> None:
        """Append a new state snapshot for a market (condition_id)."""
        import datetime
        row = dict(market)
        row["updated_at"] = updated_at or datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        # ensure required fields have defaults
        row.setdefault("schema_version", "3.0.0")
        row.setdefault("status", MarketStatus.pending.value)
        row.setdefault("resolution_outcome", ResolutionOutcome.unknown.value)
        row.setdefault("settlement_source", None)
        self._staging.append(row)
        # if writer provided, also enqueue for batched parquet flush
        if self.writer:
            import datetime as dtmod
            date_str = dtmod.datetime.now(tz=dtmod.timezone.utc).date().isoformat()
            self.writer.append("markets_log", row, asset=None, date_str=date_str)

    def _normalize_rows(self, rows: List[Dict]) -> List[Dict]:
        """Ensure all rows have union of keys (pyarrow from_pylist drops cols not in first row)."""
        if not rows:
            return rows
        union_keys = set()
        for r in rows:
            union_keys.update(r.keys())
        # fill missing with None for consistent schema inference
        normalized = []
        for r in rows:
            nr = dict(r)
            for k in union_keys:
                nr.setdefault(k, None)
            normalized.append(nr)
        return normalized

    def flush_staging(self) -> int:
        """Flush staging to writer or direct parquet (for tests without writer)."""
        if not self._staging:
            return 0
        if self.writer:
            # already appended via append(); just clear staging
            n = len(self._staging)
            self._staging.clear()
            return n
        # direct write without writer (test path)
        import datetime as dtmod
        date_str = dtmod.datetime.now(tz=dtmod.timezone.utc).date().isoformat()
        out_dir = self.data_dir / "markets_log" / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        normalized = self._normalize_rows(self._staging)
        table = pa.Table.from_pylist(normalized)
        tmp = out_dir / f"part-{uuid.uuid4().hex[:8]}.parquet.tmp"
        final = out_dir / tmp.name.replace(".tmp", "")
        pq.write_table(table, str(tmp), compression="zstd")
        tmp.rename(final)
        n = len(self._staging)
        self._staging.clear()
        return n

    # -- compaction --------------------------------------------------------
    def compact(self, parquet_data_dir: Optional[Path] = None) -> Path:
        """Rebuild markets_latest.parquet — one row per condition_id, latest updated_at.

        Reads all markets_log parquet files under data_dir and writes atomically.
        """
        base = Path(parquet_data_dir) if parquet_data_dir else self.data_dir
        log_root = base / "markets_log"
        latest_dir = base / "markets_latest"
        latest_dir.mkdir(parents=True, exist_ok=True)

        # collect all rows (including in-memory staging)
        all_rows: List[Dict] = list(self._staging)
        if log_root.exists():
            for part in log_root.rglob("*.parquet"):
                try:
                    table = pq.read_table(str(part))
                    all_rows.extend(table.to_pylist())
                except Exception:
                    continue

        # deduplicate: keep row with max updated_at per condition_id
        latest: Dict[str, Dict] = {}
        for r in all_rows:
            cid = r.get("condition_id")
            if not cid:
                continue
            cur = latest.get(cid)
            if cur is None or r.get("updated_at", "") > cur.get("updated_at", ""):
                latest[cid] = r

        rows = list(latest.values())
        # Ensure empty table still has schema
        if rows:
            rows = self._normalize_rows(rows)
            table = pa.Table.from_pylist(rows)
        else:
            table = pa.Table.from_pylist([], schema=pa.schema([]))

        tmp_path = latest_dir / "markets_latest.parquet.tmp"
        final_path = latest_dir / "markets_latest.parquet"
        pq.write_table(table, str(tmp_path), compression="zstd")
        # atomic rename (§10A)
        tmp_path.rename(final_path)
        return final_path

    def load_latest(self, parquet_data_dir: Optional[Path] = None) -> List[Dict]:
        base = Path(parquet_data_dir) if parquet_data_dir else self.data_dir
        p = base / "markets_latest" / "markets_latest.parquet"
        if not p.exists():
            return []
        try:
            return pq.read_table(str(p)).to_pylist()
        except Exception:
            return []
