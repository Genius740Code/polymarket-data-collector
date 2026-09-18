"""Markets event-sourced log + compacted latest view — §9A.

Parquet is append-only; mutating resolution_outcome in place is unsafe with
concurrent readers. Solution: append-only markets_log + periodic compaction to
markets_latest (one row per condition_id, most recent state).
"""
from __future__ import annotations

import datetime as _dt_top
import json
import sys as _sys_top
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

    # P0 leak hunt session 2 (2026-09-08): _seen_condition_ids grew unbounded
    # (~2k cids/day per 5m lane). It suppresses duplicate ACTIVE rows from
    # rollover re-discovery — a market seen long ago is final and its
    # suppression window has passed, so FIFO eviction past the cap is safe.
    MAX_SEEN_CONDITION_IDS = 20000

    def __init__(self, data_dir: str | Path, writer=None):
        self.data_dir = Path(data_dir)
        self.writer = writer  # optional ParquetWriter (batched)
        # small staging store for buffering before parquet flush (§9A)
        self._staging: List[Dict] = []
        self._seen_condition_ids: set = set()  # dedup within process lifetime (fixes duplicate 5961540)
        self._seen_order: List[str] = []  # FIFO order for the cap eviction
        # PERF: cid -> set((status, outcome)) for staged markets rows.
        # Same skip decision as the old linear scan, O(1). Collector_events
        # rows share _staging but never match (their status/outcome are None
        # vs row defaults active/unknown) — index stores raw .get() values
        # so the comparison stays exact.
        self._staging_index: Dict[str, set] = {}
        # H1 (audit 2026-09-18): rows the writer refused (backpressure WAL
        # failure) wait here for retry instead of being dropped when
        # flush_staging() clears _staging. Bounded (oldest dropped with a
        # count) so a permanently-dead writer cannot grow RAM unboundedly.
        # No event emission from this path: the writer's on_event callback
        # routes back through here (append_event) and must not recurse.
        self._unsent: List[tuple] = []  # (dataset, row, asset, date_str)
        self._unsent_dropped: int = 0
        self.MAX_UNSENT = 10_000

    def append(self, market: Dict, updated_at: Optional[str] = None) -> None:
        """Append a new state snapshot for a market (condition_id)."""
        row = dict(market)
        # Dedup: skip if same condition_id already staged (prevents 2x rows from concurrent discovery)
        cid = row.get("condition_id") or market.get("condition_id")
        if cid and cid in self._seen_condition_ids:
            # Allow update if status/resolution changed, otherwise skip duplicate active row
            # Check existing staged row for same cid has same status - if so skip
            _want = (row.get("status", "active"), row.get("resolution_outcome", "unknown"))
            try:
                _have = self._staging_index.get(cid)
                if _have is not None and _want in _have:
                    return
                # Fallback scan only when index misses (e.g. rows staged
                # before this process version): preserves exact old behavior.
                if _have is None:
                    for existing in self._staging:
                        if existing.get("condition_id") == cid and existing.get("status") == row.get("status", "active") and existing.get("resolution_outcome", "unknown") == row.get("resolution_outcome", "unknown"):
                            return
            except Exception:
                for existing in self._staging:
                    if existing.get("condition_id") == cid and existing.get("status") == row.get("status", "active") and existing.get("resolution_outcome", "unknown") == row.get("resolution_outcome", "unknown"):
                        return
            # Also check if already exists in committed latest (via writer dedup handled in compact) - still stage update if different, else skip
            # For exact duplicate active+unknown, skip
            if row.get("status", "active") == "active" and row.get("resolution_outcome", "unknown") == "unknown":
                # Check if we've already seen this cid recently - skip duplicate
                return
        if cid and cid not in self._seen_condition_ids:
            self._seen_condition_ids.add(cid)
            self._seen_order.append(cid)
            if len(self._seen_order) > self.MAX_SEEN_CONDITION_IDS:
                evict = self._seen_order[:len(self._seen_order) - self.MAX_SEEN_CONDITION_IDS]
                del self._seen_order[:len(self._seen_order) - self.MAX_SEEN_CONDITION_IDS]
                self._seen_condition_ids.difference_update(evict)
        row["updated_at"] = updated_at or _dt_top.datetime.now(tz=_dt_top.timezone.utc).isoformat().replace("+00:00", "Z")
        # §3.1 alias: recorded_at mirrors updated_at for Kaggle JSON
        if not row.get("recorded_at"):
            row["recorded_at"] = row["updated_at"]
        # §3.2 ms aliases: derive ISO <-> ms if one side missing
        # market_start_ts_ms / market_end_ts_ms <-> market_start_ts / market_end_ts
        if row.get("market_start_ts_ms") is not None and not row.get("market_start_ts"):
            try:
                ms = int(row["market_start_ts_ms"])
                row["market_start_ts"] = _dt_top.datetime.fromtimestamp(ms/1000, tz=_dt_top.timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                pass
        if row.get("market_end_ts_ms") is not None and not row.get("market_end_ts"):
            try:
                ms = int(row["market_end_ts_ms"])
                row["market_end_ts"] = _dt_top.datetime.fromtimestamp(ms/1000, tz=_dt_top.timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                pass
        if row.get("market_start_ts") and row.get("market_start_ts_ms") is None:
            try:
                iso = str(row["market_start_ts"])
                dt = _dt_top.datetime.fromisoformat(iso.replace("Z", "+00:00"))
                row["market_start_ts_ms"] = int(dt.timestamp()*1000)
            except Exception:
                pass
        if row.get("market_end_ts") and row.get("market_end_ts_ms") is None:
            try:
                iso = str(row["market_end_ts"])
                dt = _dt_top.datetime.fromisoformat(iso.replace("Z", "+00:00"))
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
        # Keep index in sync (raw .get() values, exactly like the scan).
        try:
            if cid:
                _s = self._staging_index.get(cid)
                if _s is None:
                    _s = set()
                    self._staging_index[cid] = _s
                _s.add((row.get("status"), row.get("resolution_outcome")))
        except Exception:
            pass
        if self.writer:
            date_str = _dt_top.datetime.now(tz=_dt_top.timezone.utc).date().isoformat()
            ok = self.writer.append("markets_log", row, asset=None, date_str=date_str)
            if not ok:
                # Backpressure: keep the ORIGINAL date_str for retry so the row
                # stays in its honest date= partition (never re-date on retry).
                self._stage_unsent("markets_log", row, None, date_str)

    def append_event(
        self,
        event_type: str,
        ts_utc: str,
        ts_received_ns: int,
        connection_id: Optional[str] = None,
        condition_id: Optional[str] = None,
        market_id: Optional[str] = None,
        asset: Optional[str] = None,
        details: Optional[dict] = None,
    ) -> None:
        """Append a collector_events row for data-quality tracking."""
        # details column is pa.string() — serialize dicts so payloads survive to Parquet
        # PERF: intern repeated small strings (same values, less RAM; import hoisted).
        try:
            event_type = _sys_top.intern(str(event_type)) if isinstance(event_type, str) else event_type
            if isinstance(asset, str):
                asset = _sys_top.intern(asset)
        except Exception:
            pass
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
            "asset": asset,
            "details": details,
            "schema_version": "3.2.0",
        }
        self._staging.append(row)
        if self.writer:
            date_str = _dt_top.datetime.now(tz=_dt_top.timezone.utc).date().isoformat()
            ok = self.writer.append("collector_events", row, asset=asset, date_str=date_str)
            if not ok:
                self._stage_unsent("collector_events", row, asset, date_str)
        # also enqueue markets_log schema rows (mixed markets + events) when no separate writer
        # markets_log rows are handled via append(market) path; collector_events are separate

    def _stage_unsent(self, dataset: str, row: Dict, asset: Optional[str], date_str: str) -> None:
        """Queue a writer-refused row for retry (H1). Bounded; oldest dropped
        with an exact count and a loud print (never silent)."""
        try:
            self._unsent.append((dataset, dict(row), asset, date_str))
            if len(self._unsent) > self.MAX_UNSENT:
                _drop = len(self._unsent) - self.MAX_UNSENT
                del self._unsent[:_drop]
                self._unsent_dropped += _drop
                print(f"[markets_log] WARN unsent overflow: dropped {_drop} oldest rows "
                      f"(total dropped={self._unsent_dropped}); writer backpressure unresolved")
        except Exception:
            pass

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
        # H1: retry writer-refused rows FIRST (original date_str preserved).
        # Rows are cleared from _unsent only once the writer accepts them, so a
        # refused row is never dropped by the staging clear below.
        if self._unsent and self.writer:
            _still: List[tuple] = []
            for _ds, _row, _asset, _date in self._unsent:
                try:
                    _ok = self.writer.append(_ds, _row, asset=_asset, date_str=_date)
                except Exception:
                    _ok = False
                if not _ok:
                    _still.append((_ds, _row, _asset, _date))
            self._unsent = _still
        if not self._staging:
            return 0
        if self.writer:
            # already appended via append(); just clear staging (+ index, same lifecycle)
            n = len(self._staging)
            self._staging.clear()
            try:
                self._staging_index.clear()
            except Exception:
                pass
            return n
        # direct write without writer (test path)
        date_str = _dt_top.datetime.now(tz=_dt_top.timezone.utc).date().isoformat()
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
        try:
            self._staging_index.clear()
        except Exception:
            pass
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
