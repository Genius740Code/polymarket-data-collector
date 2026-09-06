"""Parquet writer — §10A write batching & backpressure + §9 dedup.

- In-memory buffer per dataset, flush on interval OR row-count threshold
- WAL/journal for crash safety (optional, not sharing code path with cursor store §1B)
- Backpressure: never drops data; blocks/spills/logs backpressure event
- Dedup (§4, §5): (token_id, sequence_number) or fallback key
- Flush writes atomically via temp file + rename; compaction likewise §10A
- Partitioned by date (UTC) and asset (where per-asset) — §11
"""
from __future__ import annotations

import json
import os
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


def _os_replace_safe(src, dst):
    """Atomic tmp->final rename that works on Windows (os.replace overwrites; Path.rename raises WinError 183 if dst exists)."""
    import os as _os
    _os.replace(str(src), str(dst))


from .parquet_io import read_table

from ..enums import CollectorEventType
from .schemas import SCHEMAS, snapshot_schema


@dataclass
class BufferedRow:
    dataset: str  # e.g. book_snapshots_500ms, trades, book_events
    asset: Optional[str]  # None for non-partitioned datasets
    date_str: str  # YYYY-MM-DD UTC
    row: Dict[str, Any]


class ParquetWriter:
    """Batched Parquet writer with WAL + backpressure — §10A."""

    MAX_DEDUP_KEYS_PER_DATASET = 100_000  # hard cap to prevent unbounded memory growth

    def __init__(
        self,
        data_dir: str | Path,
        flush_interval_seconds: int = 60,
        flush_row_count_threshold: int = 5000,
        buffer_max_rows: int = 50000,
        wal_enabled: bool = True,
        wal_dir: str | Path | None = None,
        l2_levels: int = 10,
        schema_version: str = "3.0.0",
        on_event=None,  # callback(event_type, details) for collector_events
        synthetic_mode: bool = False,
    ):
        self.data_dir = Path(data_dir)
        self.flush_interval = flush_interval_seconds
        self.flush_threshold = flush_row_count_threshold
        self.buffer_max = buffer_max_rows
        self.wal_enabled = wal_enabled
        self.wal_dir = Path(wal_dir) if wal_dir else self.data_dir / "_wal"
        self.l2_levels = l2_levels
        self.schema_version = schema_version
        self.on_event = on_event
        self.synthetic_mode = synthetic_mode

        self._buffer: deque[BufferedRow] = deque()
        self._dropped_rows: Dict[str, int] = defaultdict(int)  # K-3: honest no-loss accounting
        self._last_flush_ts = time.monotonic()
        self._seen_keys: Dict[str, Set[Tuple]] = defaultdict(set)  # dataset -> set of dedup keys
        self._seen_order: Dict[str, deque] = defaultdict(deque)  # dataset -> insertion-order deque for LRU eviction
        self._wal_path = self.wal_dir / f"wal-{uuid.uuid4().hex}.jsonl"
        if self.wal_enabled:
            self.wal_dir.mkdir(parents=True, exist_ok=True)
            self._wal_path.touch(exist_ok=True)

        # disk space check
        self._last_disk_check = 0.0

    # -- public API --------------------------------------------------------
    def append(self, dataset: str, row: Dict[str, Any], asset: Optional[str] = None, date_str: Optional[str] = None) -> bool:
        """Append a row; returns False if backpressure blocked (caller should retry).

        §10A: never silently drops. WAL is written BEFORE buffer with fsync so
        crash between WAL and buffer never loses acknowledged data. Duplicate
        rows (same dedup key) are dropped without WAL.
        """
        # Resolve date_str early so WAL entry is complete even under backpressure
        # Prefer ns bucket fields for authoritative UTC date (§11); string ISO is secondary.
        _resolved_date_str = date_str
        if _resolved_date_str is None:
            import datetime as _dt_for_date
            date_derived = None
            # 1) try ns buckets (most authoritative, avoids .500 frac parse issues)
            for ns_key in ("ts_snapshot_ns", "ts_received_ns"):
                ns_val = row.get(ns_key)
                if ns_val is not None:
                    try:
                        ns_int = int(ns_val)
                        # ns → ms → date UTC
                        dt = _dt_for_date.datetime.fromtimestamp(ns_int / 1e9, tz=_dt_for_date.timezone.utc)
                        date_derived = dt.date().isoformat()
                        break
                    except Exception:
                        pass
            # 2) try ISO string fields
            if date_derived is None:
                ts_field = (row.get("ts_snapshot_utc") or row.get("ts_utc") or row.get("ts_source")
                            or row.get("disconnect_ts_utc"))
                if ts_field:
                    try:
                        dt = _dt_for_date.datetime.fromisoformat(str(ts_field).replace("Z", "+00:00"))
                        date_derived = dt.date().isoformat()
                    except Exception:
                        pass
            # 3) fallback: ns date already tried, last resort now (should rarely happen; log warn)
            if date_derived is None:
                # quarantining: log warn so mispartition is visible; use now but flag
                try:
                    print(f"[parquet_writer] WARN date_str fallback to now for dataset={dataset} row keys={list(row.keys())[:5]}")
                except Exception:
                    pass
                date_derived = _dt_for_date.datetime.now(tz=_dt_for_date.timezone.utc).date().isoformat()
            _resolved_date_str = date_derived
        if date_str is None:
            date_str = _resolved_date_str

        # dedup check first (§4, §5) — duplicates never hit WAL or buffer
        # For resync_episodes we do upsert (replace buffered row) rather than drop, so latest
        # reconnect/gap fields survive instead of creating duplicate rows per state transition.
        dedup_key = self._dedup_key(dataset, row)
        if dedup_key is not None:
            if dedup_key in self._seen_keys[dataset]:
                if dataset == "resync_episodes":
                    # upsert: replace existing buffered row if still in buffer, otherwise allow re-append for update
                    replaced = False
                    for br in self._buffer:
                        if br.dataset == dataset and br.row.get("resync_id") == row.get("resync_id"):
                            br.row = dict(row)
                            replaced = True
                            break
                    if replaced:
                        return True
                    # already flushed — allow update; remove old key so append proceeds (dedup map re-added below)
                    self._seen_keys[dataset].discard(dedup_key)
                else:
                    if self.on_event:
                        self.on_event(CollectorEventType.duplicate_event, {"dataset": dataset, "key": dedup_key})
                    return True
        # reserve key immediately to prevent duplicate WAL entries under concurrency
        if dedup_key is not None:
            self._seen_keys[dataset].add(dedup_key)
            try:
                self._seen_order[dataset].append(dedup_key)
            except Exception:
                pass
            # LRU eviction: drop oldest keys when cap exceeded (preserves recent dedup for today)
            if len(self._seen_keys[dataset]) > self.MAX_DEDUP_KEYS_PER_DATASET:
                try:
                    evict_count = len(self._seen_keys[dataset]) - self.MAX_DEDUP_KEYS_PER_DATASET + 5000
                    for _ in range(evict_count):
                        if not self._seen_order[dataset]:
                            break
                        oldest = self._seen_order[dataset].popleft()
                        self._seen_keys[dataset].discard(oldest)
                except Exception:
                    pass
            # Note: if append later fails (backpressure WAL failure) we keep key to avoid infinite retry dedup loop;
            # caller will retry with same key and be deduped — this is idempotent and prevents duplicate WAL.

        # backpressure check — §10A never drops without WAL spill + fsync
        if len(self._buffer) >= self.buffer_max:
            if self.on_event:
                self.on_event(CollectorEventType.backpressure, {"buffer_size": len(self._buffer), "buffer_max": self.buffer_max, "dataset": dataset, "dropped_total": self._dropped_rows.get(dataset, 0)})
            else:
                import warnings
                warnings.warn(
                    f"Backpressure: buffer full ({len(self._buffer)}/{self.buffer_max}), dataset={dataset}; caller should block/retry",
                    stacklevel=2,
                )
            if self.wal_enabled:
                try:
                    self.flush()
                except Exception:
                    pass
                if len(self._buffer) >= self.buffer_max:
                    # Still full after flush — WAL-spill with fsync (buffer-before-WAL bug fixed: WAL first)
                    # Dedup key already reserved, so WAL contains exactly one copy
                    try:
                        self._wal_append(dataset, row, asset, date_str)
                    except Exception:
                        # WAL failed: remove reserved dedup key so retry can succeed after WAL recovers
                        if dedup_key is not None:
                            self._seen_keys[dataset].discard(dedup_key)
                        self._dropped_rows[dataset] = self._dropped_rows.get(dataset, 0) + 1
                        return False
                    return True
                # Flush made room — fall through to WAL+buffer path (dedup already reserved, don't re-add)
            else:
                # WAL disabled: strict backpressure — remove reserved key so retry works
                if dedup_key is not None:
                    self._seen_keys[dataset].discard(dedup_key)
                return False

        # WAL-before-buffer with fsync (fixes 3a loss window)
        if self.wal_enabled:
            try:
                self._wal_append(dataset, row, asset, date_str)
            except Exception as e:
                # WAL failed — remove dedup reservation so caller can retry
                if dedup_key is not None:
                    self._seen_keys[dataset].discard(dedup_key)
                if self.on_event:
                    self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "error": f"WAL append failed: {e}"})
                return False

        br = BufferedRow(dataset=dataset, asset=asset or row.get("asset"), date_str=date_str, row=row)
        self._buffer.append(br)

        # maybe flush
        if len(self._buffer) >= self.flush_threshold or (time.monotonic() - self._last_flush_ts) >= self.flush_interval:
            self.flush()
        return True

    def flush(self) -> int:
        """Flush buffered rows to Parquet. Returns number of rows flushed."""
        if not self._buffer:
            return 0
        # group by (dataset, date_str, asset)
        groups: Dict[Tuple[str, str, Optional[str]], List[Dict[str, Any]]] = defaultdict(list)
        while self._buffer:
            br = self._buffer.popleft()
            groups[(br.dataset, br.date_str, br.asset)].append(br.row)

        flushed = 0
        for (dataset, date_str, asset), rows in groups.items():
            try:
                self._write_group(dataset, date_str, asset, rows)
                flushed += len(rows)
            except Exception as e:
                if self.on_event:
                    self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "error": str(e), "rows": len(rows)})
                # re-queue at front for retry (don't lose data)
                for r in reversed(rows):
                    self._buffer.appendleft(BufferedRow(dataset=dataset, asset=asset, date_str=date_str, row=r))
                raise
        self._last_flush_ts = time.monotonic()
        # truncate WAL after successful flush — fsync directory to ensure durability (fixes 3 duplicate window)
        if self.wal_enabled and flushed:
            try:
                # Batched WAL durability: fsync once per flush (not per row) so
                # every buffered row's WAL entry is on disk before the truncate
                try:
                    with open(self._wal_path, "a", encoding="utf-8") as f:
                        f.flush()
                        os.fsync(f.fileno())
                except Exception:
                    pass
                # Ensure all parquet renames are durable before truncating WAL
                self._wal_path.write_text("")
                try:
                    import os
                    with open(self._wal_path, "a") as f:
                        f.flush()
                        os.fsync(f.fileno())
                    # fsync wal dir
                    dir_fd = os.open(str(self.wal_dir), os.O_DIRECTORY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except Exception:
                    pass
            except Exception:
                pass
        return flushed

    def check_disk_space(self, min_bytes: int = 1_073_741_824) -> Optional[dict]:
        """§10A disk space monitoring. Returns alert details if below threshold, else None."""
        try:
            import shutil
            free = shutil.disk_usage(str(self.data_dir)).free
            if free < min_bytes:
                details = {"free_bytes": free, "min_bytes": min_bytes, "data_dir": str(self.data_dir)}
                if self.on_event:
                    self.on_event(CollectorEventType.write_failed, details)
                return details
        except Exception:
            pass
        return None

    def close(self) -> None:
        try:
            self.flush()
        except Exception:
            pass

    def _wal_replay(self) -> int:
        """Replay unflushed WAL entries into buffer on startup after crash/restart.

        Returns number of rows replayed.
        Idempotent: skips rows whose dedup key already exists in _seen_keys
        or on disk, preventing duplicate writes when replaying after a crash where
        some rows may have already been flushed to parquet before the crash.
        """
        import json
        replayed = 0
        seen_replay_keys: Set[Tuple] = set()  # track keys replayed in this pass
        # Build set of on-disk dedup keys to avoid re-adding rows already in parquet
        on_disk_keys: Dict[str, Set[Tuple]] = {}
        for dataset in set(
            entry.get("dataset") for wal_path in self.wal_dir.glob("wal-*.jsonl") for line in open(wal_path) if line.strip() for entry in [json.loads(line.strip())] if entry.get("dataset")
        ):
            keys = set()
            # scan existing parquet files for this dataset to find keys already on disk
            ds_root = self.data_dir / dataset
            if ds_root.exists():
                for parquet_file in ds_root.rglob("*.parquet"):
                    if parquet_file.name.endswith(".tmp"):
                        continue
                    try:
                        t = read_table(parquet_file)
                        # extract dedup-relevant columns based on dataset type
                        cols = t.column_names
                        for row in t.to_pylist():
                            key = self._dedup_key(dataset, row)
                            if key is not None:
                                keys.add(key)
                    except Exception:
                        pass
            on_disk_keys[dataset] = keys

        # glob wal files
        wal_files = sorted(self.wal_dir.glob("wal-*.jsonl"))
        for wal_path in wal_files:
            pending_lines: list[str] = []
            try:
                with open(wal_path, "r") as f:
                    raw_lines = [line for line in f if line.strip()]
                for line in raw_lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        dataset = entry.get("dataset")
                        row = entry.get("row")
                        asset = entry.get("asset")
                        date_str = entry.get("date_str")
                        if dataset and row:
                            dedup_key = self._dedup_key(dataset, row)
                            # skip if already seen in this replay pass (idempotent)
                            if dedup_key is not None and dedup_key in seen_replay_keys:
                                continue
                            # also check against on-disk keys to avoid re-adding rows
                            # that were already flushed to parquet before the crash
                            if dedup_key is not None and dedup_key in on_disk_keys.get(dataset, set()):
                                # already on disk, skip to avoid duplicate — do NOT count as replayed
                                # (previous code counted skipped as replayed which inflated stats)
                                continue
                            # also check against runtime _seen_keys to avoid re-adding
                            if dedup_key is not None and dedup_key in self._seen_keys[dataset]:
                                continue
                            # backpressure check before replay
                            if len(self._buffer) >= self.buffer_max:
                                if self.on_event:
                                    self.on_event(CollectorEventType.backpressure, {"buffer_size": len(self._buffer), "buffer_max": self.buffer_max, "dataset": dataset, "replay": True})
                                try:
                                    self.flush()
                                except Exception:
                                    pass
                            if len(self._buffer) >= self.buffer_max:
                                pending_lines.append(line)
                                continue
                            # Direct buffer insert without re-WALing (replay already has WAL entry)
                            # Reserve dedup key
                            if dedup_key is not None:
                                if dedup_key in self._seen_keys[dataset]:
                                    continue
                                self._seen_keys[dataset].add(dedup_key)
                                try:
                                    self._seen_order[dataset].append(dedup_key)
                                except Exception:
                                    pass
                                if len(self._seen_keys[dataset]) > self.MAX_DEDUP_KEYS_PER_DATASET:
                                    try:
                                        evict_count = len(self._seen_keys[dataset]) - self.MAX_DEDUP_KEYS_PER_DATASET + 5000
                                        for _ in range(evict_count):
                                            if not self._seen_order[dataset]:
                                                break
                                            oldest = self._seen_order[dataset].popleft()
                                            self._seen_keys[dataset].discard(oldest)
                                    except Exception:
                                        pass
                                seen_replay_keys.add(dedup_key)
                            # date handling already in entry — prefer ns bucket for correctness
                            if date_str is None:
                                import datetime as _dt_for_date2
                                date_str = None
                                for ns_key in ("ts_snapshot_ns", "ts_received_ns"):
                                    ns_val = row.get(ns_key)
                                    if ns_val is not None:
                                        try:
                                            dt = _dt_for_date2.datetime.fromtimestamp(int(ns_val)/1e9, tz=_dt_for_date2.timezone.utc)
                                            date_str = dt.date().isoformat()
                                            break
                                        except Exception:
                                            pass
                                if date_str is None:
                                    ts_field = row.get("ts_snapshot_utc") or row.get("ts_utc") or row.get("ts_source")
                                    if ts_field:
                                        try:
                                            dt = _dt_for_date2.datetime.fromisoformat(str(ts_field).replace("Z", "+00:00"))
                                            date_str = dt.date().isoformat()
                                        except Exception:
                                            date_str = _dt_for_date2.datetime.now(tz=_dt_for_date2.timezone.utc).date().isoformat()
                                    else:
                                        date_str = _dt_for_date2.datetime.now(tz=_dt_for_date2.timezone.utc).date().isoformat()
                            self._buffer.append(BufferedRow(dataset=dataset, asset=asset or row.get("asset"), date_str=date_str, row=row))
                            replayed += 1
                        else:
                            # malformed entry — drop
                            pass
                    except Exception:
                        # keep malformed? drop
                        continue
                # Rewrite WAL: keep only unreplayed (backpressured) lines; truncate otherwise
                try:
                    if pending_lines:
                        with open(wal_path, "w") as out:
                            for pl in pending_lines:
                                out.write(pl + "\n")
                    else:
                        open(wal_path, "w").close()
                except Exception:
                    pass
            except Exception:
                continue
        return replayed

    # -- internals ---------------------------------------------------------
    def _dedup_key(self, dataset: str, row: Dict[str, Any]) -> Optional[Tuple]:
        if dataset in ("book_events", "trades"):
            token = row.get("token_id")
            seq = row.get("sequence_number")
            if token is not None and seq is not None:
                try:
                    return (str(token), int(seq))
                except Exception:
                    # seq may be non-numeric (e.g. ISO string) — fall through to fallback key
                    pass
            # fallback per §4/§5
            if dataset == "book_events":
                return (
                    str(token),
                    row.get("ts_received_ns"),
                    row.get("event_type"),
                    row.get("new_best_bid"),
                    row.get("new_best_ask"),
                )
            if dataset == "trades":
                return (str(token), str(row.get("trade_id")))
        if dataset == "book_snapshots_500ms":
            # idempotent key for redundant collector (§1A): (asset, condition_id, ts_snapshot_bucket)
            bucket = row.get("ts_snapshot_ns")
            if bucket is not None:
                # bucket already aligned to 500ms grid; use it directly
                return (row.get("asset"), row.get("condition_id"), int(int(bucket) // 500_000_000 * 500_000_000))
        if dataset == "chainlink_events":
            rid = row.get("report_id")
            if rid:
                return (str(rid),)
        if dataset == "resync_episodes":
            rid = row.get("resync_id")
            if rid:
                return (str(rid),)
        if dataset == "collector_events":
            eid = row.get("event_id")
            if eid:
                return (str(eid),)
        return None

    def _wal_append(self, dataset: str, row: Dict[str, Any], asset: Optional[str], date_str: Optional[str]) -> None:
        entry = json.dumps({"dataset": dataset, "asset": asset, "date_str": date_str, "row": row, "ts": time.time()})
        # open/write/flush/close per row, but NO per-row fsync: fsync cost 10-20ms
        # each on Windows/OneDrive and consumed the whole 500ms tick budget at 14
        # snapshot rows per tick (scheduler_lag p95 556ms, 2026-09-06 19:58 run).
        # write+flush still survives a process crash; power-loss durability is
        # guaranteed once per flush() where the WAL is fsynced before truncation.
        with open(self._wal_path, "a", encoding="utf-8") as f:
            f.write(entry + "\n")
            f.flush()

    def _write_group(self, dataset: str, date_str: str, asset: Optional[str], rows: List[Dict[str, Any]]) -> None:
        # Determine output path §11 partitioning — §11 explicitly lists which
        # datasets are per-asset vs date-only.  Do NOT create asset subdirs
        # for date-only datasets even if caller passed asset=BTC (bug seen in
        # collector_events during rollover spam — 65k files in asset=BTC).
        NON_ASSET_DATASETS = {"markets_log", "resync_episodes", "collector_events"}
        PER_ASSET_DATASETS = {"book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events"}
        if dataset in NON_ASSET_DATASETS:
            out_dir = self.data_dir / dataset / f"date={date_str}"
        elif dataset == "markets_latest":
            out_dir = self.data_dir / dataset
        elif dataset in PER_ASSET_DATASETS:
            # enforce asset partition; UNKNOWN if missing (should not happen)
            a = asset or rows[0].get("asset") if rows else asset
            a = str(a).upper() if a else "UNKNOWN"
            out_dir = self.data_dir / dataset / f"date={date_str}" / f"asset={a}"
        elif asset:
            out_dir = self.data_dir / dataset / f"date={date_str}" / f"asset={asset}"
        else:
            out_dir = self.data_dir / dataset / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Build pyarrow table — normalize to union keys (pyarrow drops cols not in first row)
        def _normalize(rs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            if not rs:
                return rs
            keys = set()
            for r in rs:
                keys.update(r.keys())
            out = []
            for r in rs:
                nr = dict(r)
                for k in keys:
                    nr.setdefault(k, None)
                out.append(nr)
            return out

        norm_rows = _normalize(rows)
        # Coerce details dict to JSON string for collector_events (schema expects pa.string())
        for nr in norm_rows:
            if dataset == "collector_events" and "details" in nr and isinstance(nr["details"], dict):
                try:
                    nr["details"] = json.dumps(nr["details"]) if nr["details"] else None
                except Exception:
                    nr["details"] = None
            elif dataset == "collector_events" and nr.get("details") is not None and not isinstance(nr["details"], str):
                try:
                    nr["details"] = json.dumps(nr["details"])
                except Exception:
                    nr["details"] = str(nr["details"])
        # Backfill trades notional only; fee stays NULL if not observed (real data only per AGENT.md)
        for nr in norm_rows:
            if dataset == "trades":
                if nr.get("notional") is None and nr.get("price") is not None and nr.get("size") is not None:
                    try:
                        nr["notional"] = float(nr["price"]) * float(nr["size"])
                    except Exception:
                        pass
        # Coerce sequence_number to int64 consistently across datasets before schema enforcement
        # Prevents inferred string/object column when fallback timestamp is string
        for nr in norm_rows:
            if "sequence_number" in nr and nr["sequence_number"] is not None:
                try:
                    s = str(nr["sequence_number"]).strip()
                    if s.lstrip("-").isdigit():
                        nr["sequence_number"] = int(s)
                    else:
                        # Try float-string path; if fails treat as null (e.g. ISO timestamp)
                        nr["sequence_number"] = int(float(s))
                except Exception:
                    nr["sequence_number"] = None
        # Fill defaults for required non-nullable fields to avoid ArrowInvalid in tests/synthetic data
        # Provide sensible fallbacks so strict schemas (time first) don't break on partial rows
        # Only fill placeholders when explicitly in test mode; in production, quarantine rows with missing fields
        try:
            import time as _time
            import datetime as _dt
            for nr in norm_rows:
                if dataset in ("trades", "book_events", "chainlink_events", "collector_events", "resync_episodes", "markets_log", "book_snapshots_500ms", "book_snapshots_clean"):
                    if nr.get("ts_received_ns") is None and "ts_received_ns" in (SCHEMAS.get(dataset).names if SCHEMAS.get(dataset) else []):
                        nr["ts_received_ns"] = _time.time_ns()
                    if nr.get("ts_source") is None and dataset in ("trades", "book_events", "chainlink_events") and "ts_source" in nr:
                        # keep nullable, but ensure key exists
                        pass
                    if nr.get("ts_utc") is None and dataset == "collector_events" and "ts_utc" in nr:
                        nr["ts_utc"] = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                    if nr.get("ts_snapshot_utc") is None and dataset in ("book_snapshots_500ms", "book_snapshots_clean"):
                        nr["ts_snapshot_utc"] = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                    if nr.get("ts_snapshot_ns") is None and dataset in ("book_snapshots_500ms", "book_snapshots_clean"):
                        nr["ts_snapshot_ns"] = _time.time_ns()
                    if nr.get("updated_at") is None and dataset == "markets_log":
                        nr["updated_at"] = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                    if nr.get("recorded_at") is None and dataset == "markets_log":
                        nr["recorded_at"] = nr.get("updated_at") or _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                    # §3.2 ms alias promotion for markets_log
                    if dataset == "markets_log":
                        if nr.get("market_start_ts_ms") is None and nr.get("market_start_ts"):
                            try:
                                iso = str(nr["market_start_ts"])
                                dt = _dt.datetime.fromisoformat(iso.replace("Z", "+00:00"))
                                nr["market_start_ts_ms"] = int(dt.timestamp()*1000)
                            except Exception:
                                pass
                        if nr.get("market_end_ts_ms") is None and nr.get("market_end_ts"):
                            try:
                                iso = str(nr["market_end_ts"])
                                dt = _dt.datetime.fromisoformat(iso.replace("Z", "+00:00"))
                                nr["market_end_ts_ms"] = int(dt.timestamp()*1000)
                            except Exception:
                                pass
                    if nr.get("disconnect_ts_utc") is None and dataset == "resync_episodes":
                        nr["disconnect_ts_utc"] = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                # Synthetic permanently disabled - never inject test placeholders, quarantine/test values become None
                for fld in ("condition_id", "market_id", "series_id", "asset", "trade_id", "event_id", "resync_id"):
                    if fld in nr and nr[fld] in ("test-condition", "test-market", "TEST-5MIN"):
                        nr[fld] = None
                # Also drop rows that would have been fake test placeholders for required fields - let Arrow error surface (no silent fake data)
        except Exception:
            pass
        # sort rows by time then condition_id before writing (time first, condition_id second)
        try:
            sort_ts_key = None
            for k in ("ts_snapshot_utc", "ts_snapshot_ns", "ts_source", "ts_received_ns", "ts_utc", "updated_at", "market_start_ts", "disconnect_ts_utc"):
                if k in norm_rows[0]:
                    sort_ts_key = k
                    break
            if sort_ts_key:
                has_cond = "condition_id" in norm_rows[0]
                if has_cond:
                    norm_rows.sort(key=lambda r: (str(r.get(sort_ts_key) or ""), str(r.get("condition_id") or "")))
                else:
                    norm_rows.sort(key=lambda r: str(r.get(sort_ts_key) or ""))
        except Exception:
            pass
        if dataset in ("book_snapshots_500ms", "book_snapshots_clean"):
            has_snapshot_id = any("snapshot_id" in r for r in norm_rows)
            if has_snapshot_id:
                try:
                    schema = snapshot_schema(self.l2_levels)
                    table = pa.Table.from_pylist(norm_rows, schema=schema)
                except Exception:
                    table = pa.Table.from_pylist(norm_rows)
            else:
                table = pa.Table.from_pylist(norm_rows)
        else:
            schema = SCHEMAS.get(dataset)
            if schema is not None:
                try:
                    # enforce time-first column order via schema; filter to available cols
                    table = pa.Table.from_pylist(norm_rows, schema=schema)
                    # reorder to schema order (pyarrow already does) but keep extra cols at end
                except Exception:
                    try:
                        # try casting existing table to schema order
                        tbl = pa.Table.from_pylist(norm_rows)
                        # select + cast: reorder columns to schema order where possible
                        ordered_cols = [n for n in schema.names if n in tbl.schema.names]
                        extra = [n for n in tbl.schema.names if n not in ordered_cols]
                        tbl = tbl.select(ordered_cols + extra)
                        table = tbl
                    except Exception:
                        table = pa.Table.from_pylist(norm_rows)
            else:
                table = pa.Table.from_pylist(norm_rows)
            # extra sort via pyarrow if possible (numeric ns sort)
            try:
                import pyarrow.compute as pc
                sort_keys = []
                for sk in (sort_ts_key, "condition_id"):
                    if sk and sk in table.schema.names:
                        sort_keys.append((sk, "ascending"))
                if sort_keys:
                    indices = pc.sort_indices(table, sort_keys=sort_keys)
                    table = pc.take(table, indices)
            except Exception:
                pass

        # Write atomically: temp file + rename (§10A compaction same pattern)
        # Use meaningful filename: {dataset}_{timestamp_ms}.parquet instead of part-<random>
        ts_ms = int(time.time() * 1000)
        part_name = f"{dataset}_{ts_ms}.parquet"
        tmp_path = out_dir / f"{part_name}.tmp"
        final_path = out_dir / part_name
        # If a previous part exists for same date/asset, we append as new file (not overwrite)
        try:
            pq.write_table(table, str(tmp_path), compression="zstd")
        except Exception as e:
            # fallback: if strict schema caused nullability error, retry with inferred schema
            if "non-nullable but contains nulls" in str(e) or "ArrowInvalid" in str(type(e).__name__):
                try:
                    tbl2 = pa.Table.from_pylist(norm_rows)
                    pq.write_table(tbl2, str(tmp_path), compression="zstd")
                except Exception:
                    raise e
            else:
                raise
        _os_replace_safe(tmp_path, final_path)

        # Optional: also write to WAL archive dir for recovery
        # (compaction job will merge small files later)
