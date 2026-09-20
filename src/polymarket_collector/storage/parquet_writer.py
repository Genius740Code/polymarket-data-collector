"""Parquet writer — §10A write batching & backpressure + §9 dedup.

- In-memory buffer per dataset, flush on interval OR row-count threshold
- WAL/journal for crash safety (optional, not sharing code path with cursor store §1B)
- Backpressure: never drops data; blocks/spills/logs backpressure event
- Dedup (§4, §5): (token_id, sequence_number) or fallback key
- Flush writes atomically via temp file + rename; compaction likewise §10A
- Partitioned by date (UTC) and asset (where per-asset) — §11
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import time
import uuid
from collections import OrderedDict, defaultdict, deque
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

import sys as _sys

try:
    _intern = _sys.intern
except Exception:
    def _intern(s):  # type: ignore
        return s

# PERF: bounded ns-day -> date_str cache. append() hot path (56/s) did
# fromtimestamp+isoformat per row; the date changes at most once/day.
# Same date= values; fallback paths unchanged.
_DATE_CACHE: Dict[int, str] = {}


def _date_str_from_ts_field(ts_field: Any) -> Optional[str]:
    """Best-effort date= partition from a timestamp fallback field.

    Accepts int/float epoch-ms (ts_source now), numeric strings (old rows),
    or ISO-8601 strings. Returns None when unparseable — never raises.
    """
    if ts_field is None or ts_field == "" or isinstance(ts_field, bool):
        return None
    if isinstance(ts_field, (int, float)):
        try:
            f = float(ts_field)
            ms = f if f > 1e11 else f * 1000
            return _dt.datetime.fromtimestamp(ms / 1000, tz=_dt.timezone.utc).date().isoformat()
        except Exception:
            return None
    try:
        s = str(ts_field).strip()
        if not s:
            return None
        try:
            return _dt.datetime.fromtimestamp(
                float(s) / 1000, tz=_dt.timezone.utc).date().isoformat()
        except Exception:
            pass
        return _dt.datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def _date_str_from_ns(ns_val: Any) -> Optional[str]:
    try:
        ns_int = int(ns_val)
    except Exception:
        return None
    try:
        day = ns_int // 86_400_000_000_000
        cached = _DATE_CACHE.get(day)
        if cached is not None:
            return cached
        dt = _dt.datetime.fromtimestamp(ns_int / 1e9, tz=_dt.timezone.utc)
        s = dt.date().isoformat()
        # Bounded: at most 2 entries (day rollover edge).
        if len(_DATE_CACHE) >= 2:
            _DATE_CACHE.clear()
        _DATE_CACHE[day] = s
        return s
    except Exception:
        return None


@dataclass(slots=True)
class BufferedRow:
    dataset: str  # e.g. book_snapshots_500ms, trades, book_events
    asset: Optional[str]  # None for non-partitioned datasets
    date_str: str  # YYYY-MM-DD UTC
    row: Dict[str, Any]


class _OrderedSet(OrderedDict):
    """OrderedDict-backed set with add/discard compat (old set API)."""

    def add(self, key):
        self[key] = None

    def discard(self, key):
        self.pop(key, None)


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
        # M2 (audit 2026-09-16): lag-retry double-appends made duplicate_event
        # the dominant collector_events row (~8/s, pure noise bloat). Drops
        # are still exact (dedup key authoritative); only the event is
        # throttled — first + every Nth carries the running total.
        self._dupevent_count: Dict[str, int] = defaultdict(int)
        self._evict_total: Dict[str, int] = defaultdict(int)  # M3: dedup-key evictions per dataset
        self._last_flush_ts = time.monotonic()
        # C1/C2 (audit 2026-09-19): collision-proof file naming + flush failure
        # accounting. Counter + pid disambiguate same-ns flushes; fail counts
        # drive dead-letter instead of wedging the loop forever.
        self._file_counter = 0
        self._flush_fail_counts: Dict[Tuple[str, str, str], int] = defaultdict(int)
        # N2: first-failure wall-clock per group key. Dead-letter requires BOTH
        # min failures AND min age so 5 fast appends (~0.1s at write rate) can
        # never trip it — only a sustained fault over minutes does.
        self._flush_first_fail_ts: Dict[Tuple[str, str, str], float] = {}
        self._wal_malformed_total = 0
        self._ts_fill_count: Dict[str, int] = defaultdict(int)
        # PERF RAM: single OrderedDict[key]=None per dataset instead of
        # set + deque holding every key twice (100k x 7 datasets).
        # Same membership + FIFO eviction (popitem(last=False)); keys keep
        # the FULL (asset, cid, bucket) triple — asset is NOT dropped because
        # condition_id can be None on honest-gap rows (collapsing would false-dupe).
        # cids are sys.interned on insert/lookup so equality is identical.
        self._seen_keys: Dict[str, OrderedDict] = defaultdict(_OrderedSet)  # dataset -> OrderedDict[key, None]
        self._seen_order: Dict[str, deque] = defaultdict(deque)  # legacy alias, kept empty (see _seen_add/_seen_discard)
        # PERF: cache created output dirs (was mkdir per group per flush).
        self._mkdir_cache: Set[str] = set()
        self._wal_path = self.wal_dir / f"wal-{uuid.uuid4().hex}.jsonl"
        if self.wal_enabled:
            self.wal_dir.mkdir(parents=True, exist_ok=True)
            self._wal_path.touch(exist_ok=True)
        # PERF CPU: keep one append handle open for the WAL lifetime instead of
        # open/write/flush/close per row (~28 syscalls/s of path lookup + inode
        # lock). Same bytes, same per-row flush() (process-crash safe); fsync
        # still happens once per flush() before truncation (power-safe).
        self._wal_f: Any = None
        if self.wal_enabled:
            try:
                self._wal_f = open(self._wal_path, "a", encoding="utf-8", buffering=8192)
            except Exception:
                self._wal_f = None

        # disk space check
        self._last_disk_check = 0.0
        # C2 (audit 2026-09-18): pre-restart WAL files whose rows were replayed
        # into the buffer but not yet flushed. They are truncated only after the
        # first successful post-replay flush() — truncating at replay time opened
        # a loss window (crash between replay-truncate and flush lost the rows
        # the WAL existed to protect).
        self._replay_dirty: list = []

    # -- public API --------------------------------------------------------
    def append(self, dataset: str, row: Dict[str, Any], asset: Optional[str] = None, date_str: Optional[str] = None) -> bool:
        """Append a row; returns False if backpressure blocked (caller should retry).

        §10A: never silently drops. WAL is written BEFORE buffer with fsync so
        crash between WAL and buffer never loses acknowledged data. Duplicate
        rows (same dedup key) are dropped without WAL.
        """
        # Resolve date_str early so WAL entry is complete even under backpressure
        # Prefer ns bucket fields for authoritative UTC date (§11); string ISO is secondary.
        # PERF: top-level datetime (was per-row import) + ns fast-path
        # (datetime.fromtimestamp once, no fromisoformat attempt). Same date=.
        _resolved_date_str = date_str
        if _resolved_date_str is None:
            date_derived = None
            # 1) try ns buckets (most authoritative, avoids .500 frac parse issues)
            for ns_key in ("ts_snapshot_ns", "ts_received_ns"):
                ns_val = row.get(ns_key)
                if ns_val is not None:
                    date_derived = _date_str_from_ns(ns_val)
                    if date_derived is not None:
                        break
            # 2) try ISO string / ms-int fallback fields (ts_source is int now)
            if date_derived is None:
                ts_field = (row.get("ts_snapshot_utc") or row.get("ts_utc") or row.get("ts_source")
                            or row.get("disconnect_ts_utc"))
                if ts_field:
                    date_derived = _date_str_from_ts_field(ts_field)
            # 3) fallback: ns date already tried, last resort "unknown" partition
            # (honest gap — never mispartition to today; readers glob date=*
            # so date=unknown stays queryable and countable downstream).
            if date_derived is None:
                # quarantining: log warn so mispartition is visible; use unknown but flag
                try:
                    print(f"[parquet_writer] WARN date_str fallback to unknown for dataset={dataset} row keys={list(row.keys())[:5]}")
                except Exception:
                    pass
                try:
                    if self.on_event:
                        self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "reason": "date_unknown_partition", "keys": list(row.keys())[:5]})
                except Exception:
                    pass
                date_derived = "unknown"
            _resolved_date_str = date_derived
        if date_str is None:
            date_str = _resolved_date_str

        # E5: normalize aggressor side to lowercase at the write path
        # (enums.py convention) — covers every producer, incl. legacy callers.
        if dataset == "trades":
            _needs_lower = any(
                isinstance(row.get(_sk), str) and row.get(_sk) != row.get(_sk).lower()
                for _sk in ("side", "aggressor_side")
            )
            if _needs_lower:
                row = dict(row)
                for _sk in ("side", "aggressor_side"):
                    _sv = row.get(_sk)
                    if isinstance(_sv, str) and _sv:
                        row[_sk] = _sv.lower()

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
                    try:
                        self._seen_keys[dataset].pop(dedup_key, None)
                    except Exception:
                        pass
                else:
                    self._dupevent_count[dataset] += 1
                    _n = self._dupevent_count[dataset]
                    if self.on_event and (_n == 1 or _n % 10_000 == 0):
                        self.on_event(CollectorEventType.duplicate_event, {"dataset": dataset, "key": dedup_key, "dropped_total": _n, "evicted_total": self._evict_total.get(dataset, 0)})
                    return True
        # reserve key immediately to prevent duplicate WAL entries under concurrency
        # Single OrderedDict (was set+deque double-store). Same FIFO eviction.
        if dedup_key is not None:
            try:
                self._seen_keys[dataset][dedup_key] = None
            except Exception:
                pass
            # LRU eviction: drop oldest keys when cap exceeded (preserves recent dedup for today)
            # M3 (audit 2026-09-18): count evictions — a redelivery older than
            # the window is re-accepted as a new row (dupe bloat, tolerated
            # downstream). Surfaced in duplicate_event payloads for audit.
            if len(self._seen_keys[dataset]) > self.MAX_DEDUP_KEYS_PER_DATASET:
                try:
                    evict_count = len(self._seen_keys[dataset]) - self.MAX_DEDUP_KEYS_PER_DATASET + 5000
                    _od = self._seen_keys[dataset]
                    _ev = 0
                    for _ in range(evict_count):
                        try:
                            _od.popitem(last=False)
                            _ev += 1
                        except KeyError:
                            break
                    if _ev:
                        self._evict_total[dataset] = self._evict_total.get(dataset, 0) + _ev
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
                            try:
                                self._seen_keys[dataset].pop(dedup_key, None)
                            except Exception:
                                pass
                        self._dropped_rows[dataset] = self._dropped_rows.get(dataset, 0) + 1
                        return False
                    return True
                # Flush made room — fall through to WAL+buffer path (dedup already reserved, don't re-add)
            else:
                # WAL disabled: strict backpressure — remove reserved key so retry works
                if dedup_key is not None:
                    try:
                        self._seen_keys[dataset].pop(dedup_key, None)
                    except Exception:
                        pass
                return False

        # WAL-before-buffer with fsync (fixes 3a loss window)
        if self.wal_enabled:
            try:
                self._wal_append(dataset, row, asset, date_str)
            except Exception as e:
                # WAL failed — remove dedup reservation so caller can retry
                if dedup_key is not None:
                    try:
                        self._seen_keys[dataset].pop(dedup_key, None)
                    except Exception:
                        pass
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
        """Flush buffered rows to Parquet. Returns number of rows flushed.

        C1 (audit 2026-09-19): group failures no longer lose unprocessed
        groups. Rows stay tracked until durable; on failure the failing
        group AND every unprocessed group are requeued front-first, and the
        WAL is retained (truncate runs only on full success). A group
        failing 5 consecutive flushes is dead-lettered (JSONL + event)
        instead of wedging the loop forever.
        """
        if not self._buffer:
            return 0
        # group by (dataset, date_str, asset)
        groups: Dict[Tuple[str, str, Optional[str]], List[Dict[str, Any]]] = defaultdict(list)
        while self._buffer:
            br = self._buffer.popleft()
            groups[(br.dataset, br.date_str, br.asset)].append(br.row)

        flushed = 0
        # P0 fix: track partial-batch state. A dead-letter BREAK requeues
        # unprocessed later groups to _buffer (memory-only) — WAL must be
        # retained so a crash before the next flush cannot lose them.
        _batch_incomplete = False
        items = list(groups.items())
        for idx, ((dataset, date_str, asset), rows) in enumerate(items):
            try:
                self._write_group(dataset, date_str, asset, rows)
                flushed += len(rows)
                # success resets the consecutive-failure counter for this key
                try:
                    self._flush_fail_counts.pop((dataset, date_str, str(asset)), None)
                except Exception:
                    pass
                try:
                    self._flush_first_fail_ts.pop((dataset, date_str, str(asset)), None)
                except Exception:
                    pass
            except Exception as e:
                if self.on_event:
                    try:
                        self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "error": str(e), "rows": len(rows)})
                    except Exception:
                        pass
                # N2: dead-letter only after a SUSTAINED fault (min failures AND
                # min age). 5 fast appends must not trip it; backoff stays in
                # buffer-retry until the age gate passes.
                _fkey = (dataset, date_str, str(asset))
                try:
                    _now_m = time.monotonic()
                except Exception:
                    _now_m = 0.0
                try:
                    self._flush_fail_counts[_fkey] = self._flush_fail_counts.get(_fkey, 0) + 1
                    _fails = self._flush_fail_counts[_fkey]
                except Exception:
                    _fails = 1
                try:
                    _first = self._flush_first_fail_ts.get(_fkey)
                    if _first is None:
                        self._flush_first_fail_ts[_fkey] = _now_m
                        _first = _now_m
                    _age_s = max(0.0, _now_m - _first)
                except Exception:
                    _age_s = 0.0
                _MIN_FAILS = 5
                _MIN_AGE_S = 300.0  # 5 minutes of continuous failure
                if _fails >= _MIN_FAILS and _age_s >= _MIN_AGE_S:
                    # dead-letter: preserve rows on disk outside the hive + event.
                    # N2 fixes: (a) requeue later groups then BREAK — `continue`
                    # re-processed items[idx+1:] from the stale `items` list AND
                    # left them requeued, writing every later group twice;
                    # (b) a failed dead-letter write must NOT drop rows with a
                    # false `dead_lettered` event — requeue everything instead.
                    _dl_ok = False
                    try:
                        _dl_dir = self.data_dir / "_dead_letter" / dataset / f"date={date_str}"
                        _dl_dir.mkdir(parents=True, exist_ok=True)
                        _dl_path = _dl_dir / f"dead-{int(time.time_ns())}-{os.getpid()}.jsonl"
                        with open(_dl_path, "a", encoding="utf-8") as _df:
                            for _r in rows:
                                _df.write(json.dumps({"dataset": dataset, "asset": asset, "date_str": date_str, "row": _r}) + "\n")
                            try:
                                _df.flush()
                                os.fsync(_df.fileno())
                            except Exception:
                                pass
                        _dl_ok = True
                    except Exception as _dle:
                        _dl_ok = False
                        try:
                            if self.on_event:
                                self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "reason": "dead_letter_failed", "rows": len(rows), "error": str(_dle)[:200]})
                        except Exception:
                            pass
                    if _dl_ok:
                        try:
                            if self.on_event:
                                self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "reason": "dead_lettered", "rows": len(rows), "failures": _fails, "age_s": round(_age_s, 1)})
                        except Exception:
                            pass
                        try:
                            self._flush_fail_counts.pop(_fkey, None)
                        except Exception:
                            pass
                        try:
                            self._flush_first_fail_ts.pop(_fkey, None)
                        except Exception:
                            pass
                        # requeue ONLY the unprocessed later groups, skip poison group,
                        # then BREAK so the stale `items` tail is not written twice.
                        # P0: mark batch incomplete so WAL is retained below.
                        _batch_incomplete = True
                        for (d2, ds2, a2), r2 in reversed(items[idx + 1:]):
                            for r in reversed(r2):
                                self._buffer.appendleft(BufferedRow(dataset=d2, asset=a2, date_str=ds2, row=r))
                        break
                    # dead-letter write failed — requeue failing + later groups (no loss)
                    for (d2, ds2, a2), r2 in reversed(items[idx:]):
                        for r in reversed(r2):
                            self._buffer.appendleft(BufferedRow(dataset=d2, asset=a2, date_str=ds2, row=r))
                    raise
                # re-queue failing group AND every unprocessed group front-first (no loss)
                for (d2, ds2, a2), r2 in reversed(items[idx:]):
                    for r in reversed(r2):
                        self._buffer.appendleft(BufferedRow(dataset=d2, asset=a2, date_str=ds2, row=r))
                raise
            finally:
                # PERF RAM: release per-group Arrow/Py list peak promptly.
                try:
                    del rows
                except Exception:
                    pass
        try:
            del groups
        except Exception:
            pass
        # C7 sister-fix: flush builds transient Arrow/parquet state per group;
        # glibc holds the freed heap and the long-lived collector's peak
        # ratchets ~10MB/min toward max_memory_restart. Trimming once per
        # flush (not per row) returns it for negligible cost.
        try:
            from .streaming import malloc_trim as _trim

            _trim()
        except Exception:
            pass
        self._last_flush_ts = time.monotonic()
        # truncate WAL after successful flush — fsync directory to ensure durability (fixes 3 duplicate window)
        # P0: truncate ONLY on full success. A dead-letter BREAK above leaves
        # requeued rows in _buffer (memory-only); truncating the WAL then would
        # leave them with no durable copy until the next flush.
        if self.wal_enabled and flushed and not _batch_incomplete:
            try:
                # Batched WAL durability: flush + fsync the reused handle once
                # per flush (not per row) so every buffered row's WAL entry is
                # on disk before the truncate. Same guarantee, fewer syscalls.
                try:
                    _fh = getattr(self, "_wal_f", None)
                    if _fh is not None:
                        _fh.flush()
                        os.fsync(_fh.fileno())
                    else:
                        with open(self._wal_path, "a", encoding="utf-8") as f:
                            f.flush()
                            os.fsync(f.fileno())
                except Exception:
                    pass
                # Ensure all parquet renames are durable before truncating WAL
                self._wal_truncate()
                # M1 (audit 2026-09-19): use top-level os (a local `import os`
                # here shadowed the global and made os.fsync above raise
                # UnboundLocalError). WAL fsync now actually runs.
                try:
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
        # C2: replayed pre-restart WAL rows are durable now — truncate the files
        # _wal_replay retained. Runs only on the success path (an exception above
        # re-raises before reaching here, keeping the WAL intact for retry).
        # P0: same full-success gate — a partial batch keeps replay files too.
        _dirty = getattr(self, "_replay_dirty", None)
        if _dirty and flushed and not _batch_incomplete:
            try:
                for _p in list(_dirty):
                    try:
                        open(_p, "w").close()
                    except Exception:
                        continue
                _dirty.clear()
            except Exception:
                pass
        return flushed

    def replay_dead_letters(self, limit: int = 100_000) -> dict:
        """N2: requeue _dead_letter/*.jsonl rows back through append().

        Nothing in the repo read _dead_letter/ so good dead-letters never
        returned. Returns {"requeued": n, "files": m, "errors": [...]}.
        Callers delete a .jsonl file only when every row in it requeues
        (append True); partial files stay for the next pass. No hive writes
        happen here — rows flow through the normal WAL-before-buffer path.
        """
        import json as _js_dl
        stats: dict = {"requeued": 0, "files": 0, "errors": []}
        try:
            _root = self.data_dir / "_dead_letter"
            if not _root.exists():
                return stats
            for _fp in sorted(_root.rglob("*.jsonl")):
                try:
                    _lines = _fp.read_text(encoding="utf-8").splitlines()
                except Exception as _e:
                    stats["errors"].append(f"{_fp}: { _e}")
                    continue
                _ok_all = True
                for _ln in _lines:
                    if not _ln.strip():
                        continue
                    try:
                        _obj = _js_dl.loads(_ln)
                        _ok = bool(self.append(
                            _obj.get("dataset"),
                            _obj.get("row") or {},
                            asset=_obj.get("asset"),
                        ))
                    except Exception as _e2:
                        _ok = False
                        stats["errors"].append(f"{_fp}: {str(_e2)[:120]}")
                    if _ok:
                        stats["requeued"] += 1
                    else:
                        _ok_all = False
                        break  # backpressure — retry file next pass
                    if stats["requeued"] >= limit:
                        break
                if _ok_all:
                    try:
                        _fp.unlink()
                    except Exception:
                        pass
                    stats["files"] += 1
                if stats["requeued"] >= limit:
                    break
        except Exception as _e:
            stats["errors"].append(str(_e)[:200])
        return stats

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
        except Exception as e:
            # Never report a clean shutdown with rows unflushed — the WAL
            # still holds them, but the caller must know durability failed.
            try:
                print(f"[parquet_writer] ERROR close() flush failed ({len(self._buffer)} rows still buffered, WAL retained): {e}")
            except Exception:
                pass
            try:
                if self.on_event:
                    self.on_event(CollectorEventType.write_failed, {"reason": "close_flush_failed", "buffered": len(self._buffer), "error": str(e)[:300]})
            except Exception:
                pass
        try:
            _fh = getattr(self, "_wal_f", None)
            if _fh is not None:
                try:
                    _fh.flush()
                except Exception:
                    pass
                try:
                    _fh.close()
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            try:
                self._wal_f = None
            except Exception:
                pass

    # Bound for the on-disk dedup scan below: WAL content always postdates the
    # last successful flush (flush truncates the WAL after every write), so a
    # WAL row can only duplicate on-disk rows written by a flush that crashed
    # mid-way — i.e. files about as new as the WAL itself. Scanning the whole
    # hive is O(history) at every startup: 8+ min stall + GB RSS on real hives,
    # which trips pm2 max_memory_restart / earlyoom and restart-loops forever
    # (seen 2026-09-08: probe stuck 10 min in replay on a 175k-row hive).
    _REPLAY_SCAN_WINDOW_S = 2 * 3600
    _REPLAY_SCAN_ROW_CAP = 500_000
    # A crashed flush writes at most (#groups) files; newest-first + cap keeps
    # the dupe protection where it matters (the crash window) while bounding
    # file-open overhead on hives with thousands of uncompacted flush files
    # (seen 2026-09-08: 1354 files / 1.5M rows in 2.5h).
    _REPLAY_SCAN_MAX_FILES_PER_DATASET = 50

    # PERF 2026-09-12 (#10): dedup-key columns only for the replay scan.
    # The old path did read_table().to_pylist() on full 120-col snapshot rows
    # (up to 500k rows / 50 files at startup). Projecting to the columns
    # _dedup_key() actually reads cuts RSS/CPU ~10-20x with identical dedup
    # decisions. Caps unchanged (lowering them would risk dupes).
    _REPLAY_DEDUP_COLS = {
        "book_events": ["token_id", "sequence_number", "ts_source", "event_type", "old_best_bid", "new_best_bid", "old_best_ask", "new_best_ask"],
        "trades": ["token_id", "sequence_number", "trade_id"],
        "book_snapshots_500ms": ["asset", "condition_id", "ts_snapshot_ns"],
        "book_snapshots_clean": ["asset", "condition_id", "ts_snapshot_ns"],
        "chainlink_events": ["report_id", "asset", "event_id", "ts_source", "ts_received_ns", "price"],
        "resync_episodes": ["resync_id"],
        "collector_events": ["event_id"],
    }

    def _wal_replay(self) -> int:
        """Replay unflushed WAL entries into buffer on startup after crash/restart.

        Returns number of rows replayed.
        Idempotent: skips rows whose dedup key already exists in _seen_keys
        or on disk, preventing duplicate writes when replaying after a crash where
        some rows may have already been flushed to parquet before the crash.
        The on-disk scan is bounded to files newer than (oldest WAL mtime -
        2h): older files predate the last WAL truncate and cannot hold dupes
        of current WAL content in single-writer operation. A 500k-row cap with
        WARN fails open toward possible dupes (tolerated downstream) rather
        than OOM-killing the process on huge hives.
        """
        import json
        import time as _time
        replayed = 0
        seen_replay_keys: Set[Tuple] = set()  # track keys replayed in this pass
        # Build set of on-disk dedup keys to avoid re-adding rows already in parquet
        on_disk_keys: Dict[str, Set[Tuple]] = {}
        try:
            # Only NON-EMPTY WAL files matter: replay truncates consumed files,
            # so empty ones are husks from dead instances. Basing the cutoff on
            # all files (incl. days-old husks) would disable the bound.
            _wal_mtimes = [p.stat().st_mtime for p in self.wal_dir.glob("wal-*.jsonl")
                           if p.is_file() and p.stat().st_size > 0]
        except Exception:
            _wal_mtimes = []
        _scan_cutoff = (min(_wal_mtimes) if _wal_mtimes else _time.time()) - self._REPLAY_SCAN_WINDOW_S
        _scanned_rows = 0
        _scan_capped = False
        # 2026-09-11 DATA-LOSS FIX: the old one-liner parsed every line inline
        # (json.loads with no guard), so ONE truncated line (SIGKILL mid-append)
        # aborted the ENTIRE replay on every startup — 73MB of WAL sat
        # un-replayed forever while the same error logged each boot. Parse
        # defensively per line; the main loop below drops the bad line the
        # same way and truncates the file, so the corruption heals itself.
        _datasets_seen: Set[str] = set()
        for wal_path in self.wal_dir.glob("wal-*.jsonl"):
            try:
                with open(wal_path) as _wf:
                    for line in _wf:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except Exception:
                            # M11 (audit 2026-09-19): count malformed pre-scan lines instead of dropping silently
                            try:
                                self._wal_malformed_total += 1
                            except Exception:
                                pass
                            continue
                        if entry.get("dataset"):
                            _datasets_seen.add(entry["dataset"])
            except Exception:
                continue
        for dataset in _datasets_seen:
            keys = set()
            # scan existing parquet files for this dataset to find keys already on disk
            ds_root = self.data_dir / dataset
            if ds_root.exists():
                _cands = []
                for parquet_file in ds_root.rglob("*.parquet"):
                    if parquet_file.name.endswith(".tmp"):
                        continue
                    try:
                        if parquet_file.stat().st_mtime < _scan_cutoff:
                            continue  # predates last WAL truncate — cannot hold dupes
                        _cands.append(parquet_file)
                    except Exception:
                        pass
                # newest-first: crashed-flush dupes live in the newest files;
                # cap file count so thousand-file hives stay cheap
                try:
                    _cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                except Exception:
                    pass
                # Phase 1 (cheap): budget files by metadata row counts — a full
                # to_pylist on every candidate dominates on hives with huge
                # compacted partitions. Newest files first, stop at 2x row cap
                # (read pass below enforces the exact cap).
                _picked: list = []
                _budget = 0
                for _pf in _cands[:self._REPLAY_SCAN_MAX_FILES_PER_DATASET]:
                    if _scan_capped:
                        break
                    try:
                        import pyarrow.parquet as _pq
                        _nr = _pq.ParquetFile(str(_pf)).metadata.num_rows
                    except Exception:
                        _nr = 0
                    _picked.append(_pf)
                    _budget += _nr
                    if _budget >= 2 * self._REPLAY_SCAN_ROW_CAP:
                        break
                for parquet_file in _picked:
                    if _scan_capped:
                        break
                    try:
                        # Project to dedup cols only (same keys, ~10-20x less RAM).
                        t = None
                        _want = self._REPLAY_DEDUP_COLS.get(dataset)
                        if _want:
                            try:
                                import pyarrow.parquet as _pq2
                                try:
                                    _schema_names = _pq2.ParquetFile(str(parquet_file)).schema.names
                                except Exception:
                                    _schema_names = []
                                _proj = [c for c in _want if c in _schema_names] if _schema_names else list(_want)
                                if _proj:
                                    t = _pq2.ParquetFile(str(parquet_file)).read(columns=_proj)
                                else:
                                    t = read_table(parquet_file)
                            except Exception:
                                t = read_table(parquet_file)
                        else:
                            t = read_table(parquet_file)
                        if t is None:
                            continue
                        # extract dedup-relevant columns based on dataset type
                        cols = t.column_names
                        for row in t.to_pylist():
                            key = self._dedup_key(dataset, row)
                            if key is not None:
                                keys.add(key)
                            _scanned_rows += 1
                            if _scanned_rows >= self._REPLAY_SCAN_ROW_CAP:
                                _scan_capped = True
                                try:
                                    print(f"[wal-replay] WARN on-disk scan capped at {self._REPLAY_SCAN_ROW_CAP} rows — proceeding, rare dupes possible")
                                except Exception:
                                    pass
                                break
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
                            # Reserve dedup key (single OrderedDict)
                            if dedup_key is not None:
                                if dedup_key in self._seen_keys[dataset]:
                                    continue
                                try:
                                    self._seen_keys[dataset][dedup_key] = None
                                except Exception:
                                    pass
                                if len(self._seen_keys[dataset]) > self.MAX_DEDUP_KEYS_PER_DATASET:
                                    try:
                                        evict_count = len(self._seen_keys[dataset]) - self.MAX_DEDUP_KEYS_PER_DATASET + 5000
                                        _od2 = self._seen_keys[dataset]
                                        _ev2 = 0
                                        for _ in range(evict_count):
                                            try:
                                                _od2.popitem(last=False)
                                                _ev2 += 1
                                            except KeyError:
                                                break
                                        if _ev2:
                                            self._evict_total[dataset] = self._evict_total.get(dataset, 0) + _ev2
                                    except Exception:
                                        pass
                                seen_replay_keys.add(dedup_key)
                            # date handling already in entry — prefer ns bucket for correctness
                            if date_str is None:
                                date_str = None
                                for ns_key in ("ts_snapshot_ns", "ts_received_ns"):
                                    ns_val = row.get(ns_key)
                                    if ns_val is not None:
                                        date_str = _date_str_from_ns(ns_val)
                                        if date_str is not None:
                                            break
                                if date_str is None:
                                    ts_field = row.get("ts_snapshot_utc") or row.get("ts_utc") or row.get("ts_source")
                                    date_str = _date_str_from_ts_field(ts_field)
                                    if date_str is None:
                                        date_str = _dt.datetime.now(tz=_dt.timezone.utc).date().isoformat()
                            self._buffer.append(BufferedRow(dataset=dataset, asset=asset or row.get("asset"), date_str=date_str, row=row))
                            replayed += 1
                        else:
                            # malformed entry — count (M11), never silent
                            try:
                                self._wal_malformed_total += 1
                            except Exception:
                                pass
                    except Exception:
                        # malformed line — count (M11), never silent
                        try:
                            self._wal_malformed_total += 1
                        except Exception:
                            pass
                        continue
                # C2: retain fully-replayed files until the first successful
                # post-replay flush() truncates them (see flush()). Truncating
                # here lost replayed-but-unflushed rows on a second crash.
                # Only backpressured (unreplayed) lines are rewritten back.
                try:
                    if pending_lines:
                        with open(wal_path, "w") as out:
                            for pl in pending_lines:
                                out.write(pl + "\n")
                    elif raw_lines:
                        try:
                            self._replay_dirty.append(str(wal_path))
                        except Exception:
                            pass
                except Exception:
                    pass
            except Exception:
                continue
        # M11: surface malformed-line drops (never silent)
        try:
            if self._wal_malformed_total and self.on_event:
                self.on_event(CollectorEventType.write_failed, {"reason": "wal_malformed_dropped", "rows": self._wal_malformed_total})
        except Exception:
            pass
        return replayed

    # -- internals ---------------------------------------------------------
    def _dedup_key(self, dataset: str, row: Dict[str, Any]) -> Optional[Tuple]:
        # PERF: sys.intern on long id strings (cid/token) — same equality,
        # less RAM per key. Asset KEPT in the triple (None-cid honest gaps
        # would false-dupe across assets without it).
        # PERF: _intern hoisted to module level (was per-row import + closure).
        def _is(s):
            try:
                return _intern(str(s)) if s is not None else s
            except Exception:
                return s
        if dataset in ("book_events", "trades"):
            token = row.get("token_id")
            seq = row.get("sequence_number")
            if token is not None and seq is not None:
                try:
                    return (_is(token), int(seq))
                except Exception:
                    # seq may be non-numeric (e.g. ISO string) — fall through to fallback key
                    pass
            # fallback per §4/§5
            if dataset == "book_events":
                # M2 (audit 2026-09-18): key on EXCHANGE time (ts_source), not
                # receive time — a redelivered frame gets a new ts_received_ns
                # and previously never deduped. Old-BBO fields separate genuine
                # oscillations (same new BBO reached twice); when ts_source is
                # unknown there is no meaningful key: return None (store the
                # row) rather than risk false-duping distinct events.
                # H6 (audit 2026-09-19): include side — bid-snapped and
                # ask-snapped in one frame previously collapsed to one row.
                if row.get("ts_source") is None:
                    return None
                return (
                    _is(token),
                    row.get("ts_source"),
                    row.get("event_type"),
                    row.get("side"),
                    row.get("old_best_bid"),
                    row.get("new_best_bid"),
                    row.get("old_best_ask"),
                    row.get("new_best_ask"),
                )
            if dataset == "trades":
                # NULL/empty/"None" trade_ids must never collapse to one dedup
                # key (false-dupe silent loss). No key = store the row.
                _tid = row.get("trade_id")
                if _tid is None or (isinstance(_tid, str) and _tid.strip() in ("", "None")):
                    return None
                return (_is(token), str(_tid))
        if dataset == "book_snapshots_500ms":
            # idempotent key for redundant collector (§1A): (asset, condition_id, ts_snapshot_bucket)
            bucket = row.get("ts_snapshot_ns")
            if bucket is not None:
                # bucket already aligned to 500ms grid; use it directly
                return (row.get("asset"), _is(row.get("condition_id")), int(int(bucket) // 500_000_000 * 500_000_000))
        if dataset == "chainlink_events":
            # E9: report_id is 100% NULL (reserved — RTDS carries no reportId),
            # so (report_id,) never fires and burst duplicates slip through.
            # M2 (audit 2026-09-18): (asset, event_id) can never fire either —
            # event_id is a per-row uuid4 (chainlink.py), unique by
            # construction. Dedup on (asset, ts_source, price), requiring real
            # values: a NULL time/price key would false-dupe distinct ticks.
            rid = row.get("report_id")
            if rid:
                return (_is(rid),)
            if (row.get("asset") is not None and row.get("ts_source") is not None
                    and row.get("price") is not None):
                return (_is(row.get("asset")), row.get("ts_source"), row.get("price"))
        if dataset == "resync_episodes":
            rid = row.get("resync_id")
            if rid:
                return (_is(rid),)
        if dataset == "collector_events":
            eid = row.get("event_id")
            if eid:
                return (_is(eid),)
        return None

    def _wal_append(self, dataset: str, row: Dict[str, Any], asset: Optional[str], date_str: Optional[str]) -> None:
        # PERF: compact separators (was default ', '/': '). WAL is internal;
        # json.loads yields identical rows. Same durability semantics below.
        entry = json.dumps({"dataset": dataset, "asset": asset, "date_str": date_str, "row": row, "ts": time.time()}, separators=(",", ":"))
        # open handle reused across rows; NO per-row fsync: fsync cost 10-20ms
        # each on Windows/OneDrive and consumed the whole 500ms tick budget at 14
        # snapshot rows per tick (scheduler_lag p95 556ms, 2026-09-06 19:58 run).
        # write+flush still survives a process crash; power-loss durability is
        # guaranteed once per flush() where the WAL is fsynced before truncation.
        # Fallback to one-off open preserves old behavior if handle died.
        fh = getattr(self, "_wal_f", None)
        if fh is not None:
            try:
                fh.write(entry + "\n")
                fh.flush()
                return
            except Exception:
                pass
        with open(self._wal_path, "a", encoding="utf-8") as f:
            f.write(entry + "\n")
            f.flush()

    def _wal_truncate(self) -> None:
        """Truncate the active WAL after a successful flush; re-seek handle."""
        try:
            self._wal_path.write_text("")
        except Exception:
            return
        fh = getattr(self, "_wal_f", None)
        if fh is not None:
            try:
                fh.flush()
            except Exception:
                pass
            try:
                fh.seek(0)
            except Exception:
                # Handle went stale (e.g. file replaced) — reopen lazily.
                try:
                    fh.close()
                except Exception:
                    pass
                try:
                    self._wal_f = open(self._wal_path, "a", encoding="utf-8", buffering=8192)
                except Exception:
                    self._wal_f = None

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
            # enforce asset partition; UNKNOWN if missing (should not happen).
            # The row keeps an explicit "UNKNOWN" asset too (never NULL here)
            # so the partition and the row agree and the gap stays countable
            # instead of failing the non-nullable schema downstream.
            a = asset or rows[0].get("asset") if rows else asset
            a = str(a).upper() if a else "UNKNOWN"
            if a == "UNKNOWN":
                for _r in rows:
                    if not _r.get("asset"):
                        _r["asset"] = "UNKNOWN"
                try:
                    if self.on_event:
                        self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "reason": "asset_unknown_partition", "rows": len(rows)})
                    else:
                        print(f"[parquet_writer] WARN {dataset}: {len(rows)} rows with missing asset → asset=UNKNOWN (honest gap, unjoinable)")
                except Exception:
                    pass
            out_dir = self.data_dir / dataset / f"date={date_str}" / f"asset={a}"
        elif asset:
            out_dir = self.data_dir / dataset / f"date={date_str}" / f"asset={asset}"
        else:
            out_dir = self.data_dir / dataset / f"date={date_str}"
        # PERF: mkdir once per dir (was per group per flush). Same dirs.
        _od = str(out_dir)
        if _od not in self._mkdir_cache:
            out_dir.mkdir(parents=True, exist_ok=True)
            self._mkdir_cache.add(_od)

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
        # PERF: single fused coerce pass (was 4 full passes: details/notional/
        # sequence/defaults + SCHEMAS.get per row). Same per-row logic in the
        # same order; missing-ts time.time_ns/now fallbacks preserved as-is.
        _schema_obj = SCHEMAS.get(dataset)
        _schema_names = set(_schema_obj.names) if _schema_obj is not None else set()
        _is_coll = dataset == "collector_events"
        _is_tr = dataset == "trades"
        _is_ml = dataset == "markets_log"
        _is_re = dataset == "resync_episodes"
        _is_snap = dataset in ("book_snapshots_500ms", "book_snapshots_clean")
        _needs_ts_fill = dataset in ("trades", "book_events", "chainlink_events", "collector_events", "resync_episodes", "markets_log", "book_snapshots_500ms", "book_snapshots_clean")
        for nr in norm_rows:
            if _is_coll and "details" in nr and isinstance(nr["details"], dict):
                try:
                    nr["details"] = json.dumps(nr["details"]) if nr["details"] else None
                except Exception:
                    nr["details"] = None
            elif _is_coll and nr.get("details") is not None and not isinstance(nr["details"], str):
                try:
                    nr["details"] = json.dumps(nr["details"])
                except Exception:
                    nr["details"] = str(nr["details"])
            if _is_tr:
                if nr.get("notional") is None and nr.get("price") is not None and nr.get("size") is not None:
                    try:
                        nr["notional"] = float(nr["price"]) * float(nr["size"])
                    except Exception:
                        pass
                # H4: default source to live when absent (old callers)
                if nr.get("source") is None and "source" in _schema_names:
                    nr["source"] = "live"
            if "sequence_number" in nr and nr["sequence_number"] is not None:
                try:
                    s = str(nr["sequence_number"]).strip()
                    if s.lstrip("-").isdigit():
                        nr["sequence_number"] = int(s)
                    else:
                        nr["sequence_number"] = int(float(s))
                except Exception:
                    nr["sequence_number"] = None
            if _needs_ts_fill:
                try:
                    # M2: flush-time fill is estimated — flag it per row so
                    # backtests can exclude estimated receive times.
                    # H4 trades may honestly carry NULL (api_reconciled): only
                    # fill live rows (source is None/live); reconciled rows keep
                    # NULL + ts_backfilled_ns.
                    _is_reconciled = _is_tr and nr.get("source") == "api_reconciled"
                    if nr.get("ts_received_ns") is None and "ts_received_ns" in _schema_names and not _is_reconciled:
                        nr["ts_received_ns"] = time.time_ns()
                        if "ts_received_ns_estimated" in _schema_names:
                            nr["ts_received_ns_estimated"] = True
                        try:
                            self._ts_fill_count[dataset] = self._ts_fill_count.get(dataset, 0) + 1
                            _n = self._ts_fill_count[dataset]
                            if self.on_event and (_n == 1 or _n % 1000 == 0):
                                self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "reason": "ts_received_ns_estimated", "count": _n})
                        except Exception:
                            pass
                    elif "ts_received_ns_estimated" in _schema_names and nr.get("ts_received_ns_estimated") is None:
                        # explicit False when the producer supplied a real clock
                        if nr.get("ts_received_ns") is not None and not _is_reconciled:
                            nr["ts_received_ns_estimated"] = False
                    if nr.get("ts_utc") is None and _is_coll and "ts_utc" in nr:
                        nr["ts_utc"] = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                    # Snapshot bucket time is event time — never fabricate it at
                    # flush time (wrong-clock PIT corruption). Rows missing
                    # both fields are dropped below with a countable event
                    # (honest gap, never a guessed bucket).
                    if nr.get("ts_snapshot_utc") is None and _is_snap:
                        nr["_drop_missing_snapshot_ts"] = True
                    if nr.get("ts_snapshot_ns") is None and _is_snap:
                        nr["_drop_missing_snapshot_ts"] = True
                    if nr.get("updated_at") is None and _is_ml:
                        nr["updated_at"] = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                    if nr.get("recorded_at") is None and _is_ml:
                        nr["recorded_at"] = nr.get("updated_at") or _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                    if _is_ml:
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
                    if nr.get("disconnect_ts_utc") is None and _is_re:
                        nr["disconnect_ts_utc"] = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                except Exception:
                    pass
                for fld in ("condition_id", "market_id", "series_id", "asset", "trade_id", "event_id", "resync_id"):
                    if fld in nr and nr[fld] in ("test-condition", "test-market", "TEST-5MIN"):
                        nr[fld] = None
                        try:
                            if self.on_event:
                                self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "reason": "test_sentinel_scrubbed", "field": fld})
                        except Exception:
                            pass
        # Honest-gap drop: snapshot rows with no bucket time are unjoinable —
        # drop with a countable event instead of fabricating a bucket.
        try:
            _dropped_ts = [r for r in norm_rows if r.pop("_drop_missing_snapshot_ts", None)]
        except Exception:
            _dropped_ts = []
        if _dropped_ts:
            try:
                if self.on_event:
                    self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "reason": "missing_snapshot_ts_dropped", "rows": len(_dropped_ts)})
                else:
                    print(f"[parquet_writer] WARN dropped {len(_dropped_ts)} {dataset} rows with missing snapshot ts (honest gap)")
            except Exception:
                pass
            norm_rows = [r for r in norm_rows if r.get("ts_snapshot_ns") is not None and r.get("ts_snapshot_utc") is not None]
            if not norm_rows:
                return
        # sort rows by time then condition_id before writing (time first, condition_id second)
        try:
            sort_ts_key = None
            for k in ("ts_snapshot_utc", "ts_snapshot_ns", "ts_source", "ts_received_ns", "ts_utc", "updated_at", "market_start_ts", "disconnect_ts_utc"):
                if k in norm_rows[0]:
                    sort_ts_key = k
                    break
            if sort_ts_key:
                has_cond = "condition_id" in norm_rows[0]
                # Numeric-aware ordering: mixed int/str timestamps must not
                # sort lexically ("100" < "20"). NULLs last, then condition_id.
                def _sort_key(r):
                    v = r.get(sort_ts_key)
                    try:
                        if v is None or (isinstance(v, str) and not v.strip()):
                            return (1, 0.0, str(r.get("condition_id") or "") if has_cond else "")
                        return (0, float(v) if not isinstance(v, str) or v.strip().lstrip("-").replace(".", "", 1).isdigit() else float("inf"), str(r.get("condition_id") or "") if has_cond else "")
                    except Exception:
                        return (1, 0.0, str(r.get("condition_id") or "") if has_cond else "")
                # String-ISO timestamps (ts_snapshot_utc/ts_utc): lexical order
                # is chronological for fixed-format ISO, so keep str ordering.
                try:
                    _sample = norm_rows[0].get(sort_ts_key)
                    if isinstance(_sample, str) and ("T" in _sample or "-" in _sample):
                        if has_cond:
                            norm_rows.sort(key=lambda r: (str(r.get(sort_ts_key) or "~~~"), str(r.get("condition_id") or "")))
                        else:
                            norm_rows.sort(key=lambda r: str(r.get(sort_ts_key) or "~~~"))
                    else:
                        norm_rows.sort(key=_sort_key)
                except Exception:
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
            # PERF 2026-09-12 (#8): drop the 2nd Arrow sort — rows are already
            # sorted in Python above (time, condition_id) before from_pylist,
            # so sort_indices/take only re-sorted the same keys at full-table
            # cost every flush. Row SET unchanged; per-file order follows the
            # Python sort (downstream readers sort anyway per E13).

        # Write atomically: temp file + rename (§10A compaction same pattern)
        # C2 (audit 2026-09-19): collision-proof names {dataset}_{ns}_{pid}_{ctr}.
        # The old {dataset}_{ms} name let two asset groups in one flush (same
        # ms, same date-only dir) overwrite each other via os.replace.
        # Refuse to overwrite: bump the counter while the candidate exists.
        try:
            self._file_counter += 1
        except Exception:
            self._file_counter = 1
        try:
            _pid = os.getpid()
        except Exception:
            _pid = 0
        _ns = time.time_ns()
        part_name = f"{dataset}_{_ns}_{_pid}_{self._file_counter}.parquet"
        tmp_path = out_dir / f"{part_name}.tmp"
        final_path = out_dir / part_name
        try:
            while final_path.exists():
                try:
                    self._file_counter += 1
                except Exception:
                    break
                part_name = f"{dataset}_{time.time_ns()}_{_pid}_{self._file_counter}.parquet"
                tmp_path = out_dir / f"{part_name}.tmp"
                final_path = out_dir / part_name
        except Exception:
            pass
        # If a previous part exists for same date/asset, we append as new file (not overwrite)
        try:
            pq.write_table(table, str(tmp_path), compression="zstd")
        except Exception as e:
            # fallback: if strict schema caused nullability error, retry with inferred schema
            if "non-nullable but contains nulls" in str(e) or "ArrowInvalid" in str(type(e).__name__):
                try:
                    if self.on_event:
                        self.on_event(CollectorEventType.write_failed, {"dataset": dataset, "reason": "nullable_schema_fallback", "error": str(e)[:300], "rows": len(norm_rows)})
                    else:
                        print(f"[parquet_writer] WARN {dataset}: strict schema rejected NULLs ({str(e)[:200]}); writing inferred schema so rows are preserved")
                except Exception:
                    pass
                try:
                    tbl2 = pa.Table.from_pylist(norm_rows)
                    pq.write_table(tbl2, str(tmp_path), compression="zstd")
                except Exception:
                    raise e
            else:
                raise
        # Crash-safe publish: fsync tmp content + parent dir BEFORE the
        # atomic rename so a SIGKILL/power loss can only leave a .tmp
        # orphan (skipped by readers) — never a footer-less final that
        # fail-closes every future Kaggle upload (seen 2026-09-13 LOW:
        # 5 footer-less finals at 1789331692*). Best-effort, never raises.
        try:
            with open(str(tmp_path), "rb") as _fh:
                try:
                    _fh.flush()
                except Exception:
                    pass
                try:
                    os.fsync(_fh.fileno())
                except Exception:
                    pass
            try:
                _dfd = os.open(str(out_dir), os.O_DIRECTORY)
                try:
                    os.fsync(_dfd)
                finally:
                    try:
                        os.close(_dfd)
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            pass
        # C2: never overwrite an existing part file. Names are unique
        # (ns+pid+counter) so this is defense-in-depth only.
        try:
            if final_path.exists():
                try:
                    self._file_counter += 1
                except Exception:
                    pass
                part_name = f"{dataset}_{time.time_ns()}_{_pid}_{self._file_counter}.parquet"
                final_path = out_dir / part_name
                if final_path.exists():
                    raise FileExistsError(str(final_path))
        except FileExistsError:
            raise
        except Exception:
            pass
        _os_replace_safe(tmp_path, final_path)
        try:
            _dfd2 = os.open(str(out_dir), os.O_DIRECTORY)
            try:
                os.fsync(_dfd2)
            finally:
                try:
                    os.close(_dfd2)
                except Exception:
                    pass
        except Exception:
            pass

        # Optional: also write to WAL archive dir for recovery
        # (compaction job will merge small files later)
