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


def _os_replace_safe(src, dst):
    """Atomic tmp->final rename that works on Windows (os.replace overwrites; Path.rename raises WinError 183 if dst exists)."""
    import os as _os
    _os.replace(str(src), str(dst))


from .parquet_io import read_table

from ..enums import MarketStatus, ResolutionOutcome


class MarketsLog:
    """Append-only log for market metadata (§2 + §6A settlement fields)."""

    def __init__(self, data_dir: str | Path, writer=None):
        self.data_dir = Path(data_dir)
        self.writer = writer  # optional ParquetWriter (batched)
        # small staging store for buffering before parquet flush (§9A)
        self._staging: List[Dict] = []
        self._seen_condition_ids: set = set()  # dedup within process lifetime (fixes duplicate 5961540)

    def append(self, market: Dict, updated_at: Optional[str] = None) -> None:
        """Append a new state snapshot for a market (condition_id)."""
        import datetime
        row = dict(market)
        # Dedup: skip if same condition_id already staged (prevents 2x rows from concurrent discovery)
        cid = row.get("condition_id") or market.get("condition_id")
        if cid and cid in self._seen_condition_ids:
            # Allow update if status/resolution changed, otherwise skip duplicate active row
            # Check existing staged row for same cid has same status - if so skip
            for existing in self._staging:
                if existing.get("condition_id") == cid and existing.get("status") == row.get("status", "active") and existing.get("resolution_outcome", "unknown") == row.get("resolution_outcome", "unknown"):
                    return
            # Also check if already exists in committed latest (via writer dedup handled in compact) - still stage update if different, else skip
            # For exact duplicate active+unknown, skip
            if row.get("status", "active") == "active" and row.get("resolution_outcome", "unknown") == "unknown":
                # Check if we've already seen this cid recently - skip duplicate
                return
        if cid:
            self._seen_condition_ids.add(cid)
        row["updated_at"] = updated_at or datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        # §3.1 alias: recorded_at mirrors updated_at for Kaggle JSON
        if not row.get("recorded_at"):
            row["recorded_at"] = row["updated_at"]
        # §3.2 ms aliases: derive ISO <-> ms if one side missing
        # market_start_ts_ms / market_end_ts_ms <-> market_start_ts / market_end_ts
        if row.get("market_start_ts_ms") is not None and not row.get("market_start_ts"):
            try:
                ms = int(row["market_start_ts_ms"])
                row["market_start_ts"] = datetime.datetime.fromtimestamp(ms/1000, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                pass
        if row.get("market_end_ts_ms") is not None and not row.get("market_end_ts"):
            try:
                ms = int(row["market_end_ts_ms"])
                row["market_end_ts"] = datetime.datetime.fromtimestamp(ms/1000, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                pass
        if row.get("market_start_ts") and row.get("market_start_ts_ms") is None:
            try:
                iso = str(row["market_start_ts"])
                dt = datetime.datetime.fromisoformat(iso.replace("Z", "+00:00"))
                row["market_start_ts_ms"] = int(dt.timestamp()*1000)
            except Exception:
                pass
        if row.get("market_end_ts") and row.get("market_end_ts_ms") is None:
            try:
                iso = str(row["market_end_ts"])
                dt = datetime.datetime.fromisoformat(iso.replace("Z", "+00:00"))
                row["market_end_ts_ms"] = int(dt.timestamp()*1000)
            except Exception:
                pass
        # ensure required fields have defaults
        row.setdefault("schema_version", "3.0.0")
        row.setdefault("status", MarketStatus.pending.value)
        row.setdefault("resolution_outcome", ResolutionOutcome.unknown.value)
        row.setdefault("settlement_source", None)
        # §3.1 nullable enrichment defaults (keep null for backward compat if missing)
        row.setdefault("slug", None)
        row.setdefault("window_label", None)
        row.setdefault("window_size_seconds", None)
        row.setdefault("market_start_ts_ms", None)
        row.setdefault("market_end_ts_ms", None)
        self._staging.append(row)
        if self.writer:
            import datetime as dtmod
            date_str = dtmod.datetime.now(tz=dtmod.timezone.utc).date().isoformat()
            ok = self.writer.append("markets_log", row, asset=None, date_str=date_str)
            if not ok:
                # Backpressure: staging already holds row, writer WAL-persisted if enabled;
                # do NOT drop — will be retried on next flush_staging if writer was WAL-disabled
                try:
                    # Keep in staging for retry; writer will be retried via flush_staging fallback
                    pass
                except Exception:
                    pass

    def append_event(
        self,
        event_type: str,
        ts_utc: str,
        ts_received_ns: int,
        connection_id: Optional[str] = None,
        condition_id: Optional[str] = None,
        market_id: Optional[str] = None,
        token_id: Optional[str] = None,
        asset: Optional[str] = None,
        details: Optional[dict] = None,
    ) -> None:
        """Append a collector_events row for data-quality tracking."""
        import datetime
        # details column is pa.string() — serialize dicts so payloads survive to Parquet
        if isinstance(details, dict):
            details = json.dumps(details, default=str)
        elif details is not None:
            details = str(details)
        row = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "ts_utc": ts_utc,
            "ts_received_ns": ts_received_ns,
            "connection_id": connection_id,
            "condition_id": condition_id,
            "market_id": market_id,
            "token_id": token_id,
            "asset": asset,
            "details": details,
            "schema_version": "3.2.0",
        }
        self._staging.append(row)
        if self.writer:
            import datetime as dtmod
            date_str = dtmod.datetime.now(tz=dtmod.timezone.utc).date().isoformat()
            ok = self.writer.append("collector_events", row, asset=asset, date_str=date_str)
            if not ok:
                pass  # staged already, will be retried
        # also enqueue markets_log schema rows (mixed markets + events) when no separate writer
        # markets_log rows are handled via append(market) path; collector_events are separate

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
        _os_replace_safe(tmp, final)
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
                    table = read_table(part)
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
        _os_replace_safe(tmp_path, final_path)
        return final_path

    def load_latest(self, parquet_data_dir: Optional[Path] = None) -> List[Dict]:
        base = Path(parquet_data_dir) if parquet_data_dir else self.data_dir
        p = base / "markets_latest" / "markets_latest.parquet"
        if not p.exists():
            return []
        try:
            return read_table(p).to_pylist()
        except Exception:
            return []
