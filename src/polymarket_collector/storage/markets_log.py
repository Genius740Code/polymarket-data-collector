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


# compact() helper: first-win ordering column, never written to disk
_WIN_SEQ_COL = "__compact_win_seq"


from .parquet_io import concat, read_table

from ..enums import MarketStatus, ResolutionOutcome


class MarketsLog:
    """Append-only log for market metadata (§2 + §6A settlement fields)."""

    # P0 leak hunt session 2 (2026-09-08): _seen_condition_ids grew unbounded
    # (~2k cids/day per 5m lane). It suppresses duplicate ACTIVE rows from
    # rollover re-discovery — a market seen long ago is final and its
    # suppression window has passed, so FIFO eviction past the cap is safe.
    MAX_SEEN_CONDITION_IDS = 20000

    # compact() streaming row-batch bound (rows per read batch in both passes)
    _COMPACT_BATCH_ROWS = 2048
    # compact() piece coalescing: winner-row pieces are concatenated into
    # one running table every N pieces — the real hive has ~50k fragments
    # and one tiny arrow Table per fragment costs ~8KB of object/buffer
    # overhead, which alone ratchets RSS by hundreds of MB.
    _COMPACT_COALESCE_PIECES = 1024

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
            # Enrichment-aware dedup: repeated discovery polls for the same
            # active/unknown market must not bloat markets_log, but a second
            # discovery carrying FRESH enrichment (slug/window/tokens/market_id)
            # while the first copy is still in _unsent/unflushed buffer must
            # be staged (the old blanket early-return silently dropped it).
            if row.get("status", "active") == "active" and row.get("resolution_outcome", "unknown") == "unknown":
                try:
                    _enr = (row.get("slug"), row.get("market_id"), row.get("up_token_id"),
                            row.get("down_token_id"), row.get("window_index"), row.get("series_id"))
                    _last = getattr(self, "_last_enrichment", {}).get(cid) if cid else None
                    if _last is not None and _last == _enr:
                        return  # exact repeat poll — skip (anti-bloat, no loss)
                    if getattr(self, "_last_enrichment", None) is None:
                        self._last_enrichment = {}
                    if cid:
                        self._last_enrichment[cid] = _enr
                        # bound alongside _seen_order eviction
                        if len(self._last_enrichment) > self.MAX_SEEN_CONDITION_IDS:
                            try:
                                _old = next(iter(self._last_enrichment))
                                del self._last_enrichment[_old]
                            except Exception:
                                pass
                except Exception:
                    pass
        if cid and cid not in self._seen_condition_ids:
            self._seen_condition_ids.add(cid)
            self._seen_order.append(cid)
            if len(self._seen_order) > self.MAX_SEEN_CONDITION_IDS:
                evict = self._seen_order[:len(self._seen_order) - self.MAX_SEEN_CONDITION_IDS]
                del self._seen_order[:len(self._seen_order) - self.MAX_SEEN_CONDITION_IDS]
                self._seen_condition_ids.difference_update(evict)
        row["updated_at"] = updated_at or _dt_top.datetime.now(tz=_dt_top.timezone.utc).isoformat().replace("+00:00", "Z")
        # H5: always stamp recorded_at=now (was: keep old value on backfill ->
        # as-of join saw the winner before the market opened). first_seen_at
        # preserves the original discovery time for PIT joins.
        try:
            if not row.get("first_seen_at"):
                # carry forward the earliest known first_seen, else this write time
                _prev_first = None
                try:
                    _prev_first = getattr(self, "_first_seen", {}).get(cid)
                except Exception:
                    _prev_first = None
                row["first_seen_at"] = _prev_first or row.get("recorded_at") or row["updated_at"]
            try:
                if not hasattr(self, "_first_seen"):
                    self._first_seen = {}
                if cid and cid not in self._first_seen:
                    self._first_seen[cid] = row["first_seen_at"]
            except Exception:
                pass
        except Exception:
            pass
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
        event_id: Optional[str] = None,
    ) -> None:
        """Append a collector_events row for data-quality tracking.

        event_id: optional DETERMINISTIC id. Default None -> uuid4. Callers
        that must dedup across restart storms (e.g. the startup coverage-gap
        narrator) pass a hash of measured gap bounds so the same evidence
        row collapses to one copy instead of one per incarnation.
        """
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
            "event_id": str(event_id) if event_id else str(uuid.uuid4()),
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
        table = self._build_union_table(normalized)
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
    @staticmethod
    def _iso_from_epoch_ms(value) -> Optional[str]:
        """Canonicalize a NUMERIC epoch-ms value to ISO (schema: string field).

        2026-10-17: markets_log fragments from before the 2026-10-14 write fix
        carry settlement_ts_utc as int64 epoch-ms; those old backfill winner
        rows are unreachable by any future write, so the write-side cast left
        compact() failing fail-closed forever and markets_latest (AUTHORITY
        for settlement backtests: export_pmdata / backtest_exec /
        backfill_pmdata) froze.

        This converts only epoch-ms NUMBERS (int/int64/float — never a str)
        inside the 2020-2040 plausibility band. Digit-STRING rows (the
        2026-10-10 casts, REAL_EPOCH_MS_STR), ISO strings and garbage
        pass through verbatim: markets_log stays event-sourced, fragments are
        never pruned or rewritten, and only the rebuilt markets_latest
        VIEW may be canonical. Returns None (-> value left unchanged) for
        anything not plausibly epoch-ms; nothing is ever fabricated.
        """
        if isinstance(value, str):
            return None
        try:
            ms = int(value)
        except (TypeError, ValueError):
            return None
        # Epoch-ms plausibility band (2020-2040); out-of-band -> None
        # (unchanged, so an unmergeable union still raises -> fail-closed,
        # never guessed into seconds).
        if 1_400_000_000_000 <= ms < 2_200_000_000_000:
            return _dt_top.datetime.fromtimestamp(
                ms / 1000, tz=_dt_top.timezone.utc).isoformat().replace("+00:00", "Z")
        return None

    @classmethod
    def _canon_numeric_settlement_rows(cls, rows: List[Dict]) -> List[Dict]:
        """Copy rows, ISO-casting numeric epoch-ms settlement_ts_utc (see _iso_from_epoch_ms)."""
        out: List[Dict] = []
        for r in rows:
            nr = dict(r)
            ts = cls._iso_from_epoch_ms(nr.get("settlement_ts_utc"))
            if ts is not None:
                nr["settlement_ts_utc"] = ts
            out.append(nr)
        return out

    @classmethod
    def _build_union_table(cls, rows: List[Dict]):
        """pa.Table.from_pylist with a mixed-vintage fallback (2026-10-17).

        First builds the rows VERBATIM — string-only winners (ISO or the
        digit-string epoch casts) already union to one string column and stay
        untouched. Only when the union genuinely cannot build (pre-2026-10-14
        int64 epoch-ms beside str rows -> ArrowInvalid) it retries ONCE with
        numeric epoch-ms values ISO-cast. If that retry still fails the
        original error propagates and compact() remains fail-closed — the
        caller (backfill_resolutions) keeps its WARN path.
        """
        try:
            return pa.Table.from_pylist(rows)
        except Exception:
            pass
        return pa.Table.from_pylist(cls._canon_numeric_settlement_rows(rows))

    def compact(self, parquet_data_dir: Optional[Path] = None) -> Path:
        """Rebuild markets_latest.parquet — one row per condition_id, latest updated_at.

        Memory-bounded two-pass streaming rebuild (2026-10-21, backlog #1a).
        The old shape read EVERY markets_log fragment fully and accumulated
        all rows as Python dicts before dedup: measured on the real hive
        (192,241 rows / 40,433 condition_ids / 49,733 fragments) that is a
        ~500MB peak, which collided with the collector's ~1GB export build
        on this 3.9GB box — 3 kernel OOM kills of the collector on
        2026-10-20, each within 10min of a backfill cron window. This
        version never holds the log as dicts:

          pass 1 streams ONLY (condition_id, updated_at) per fragment in
          bounded row batches and keeps a winner index cid -> (updated_at,
          src, row) — a few hundred bytes per unique cid, the one structure
          the dedup cannot avoid;
          pass 2 re-reads just the winning rows (bounded batches, stops at
          each fragment's last winner) and assembles the exact table the
          dict-merge produced: same winners (max updated_at, first row
          wins exact ties), same first-win row order, same 2026-10-17
          ISO-cast canonicalization for mixed settlement vintages (applied
          piece-wise so the union stays columnar, see
          _canon_settlement_piece), same all-None -> null column typing
          from_pylist had, same atomic tmp+rename write.

        Event-sourced fragments are never rewritten or pruned. A fragment
        that fails to read in pass 1 is skipped whole (same net effect as
        the old per-file except); one that fails only in pass 2 is skipped
        with a loud WARN (its winner rows rejoin on the next rebuild —
        markets_latest is a rebuildable view, the log stays authoritative).
        """
        base = Path(parquet_data_dir) if parquet_data_dir else self.data_dir
        log_root = base / "markets_log"
        latest_dir = base / "markets_latest"
        latest_dir.mkdir(parents=True, exist_ok=True)

        # M7: _staging holds BOTH markets rows (append) and collector_events
        # rows (append_event) — only markets rows (status-bearing) may enter
        # markets_latest. Staging rows compete first, exactly as before.
        staging_rows: List[Dict] = [r for r in self._staging if "status" in r]
        staging_chunk = None
        if staging_rows:
            staging_chunk = self._build_union_table(self._normalize_rows(list(staging_rows)))
        parts: List[Path] = list(log_root.rglob("*.parquet")) if log_root.exists() else []

        # pass 1: winner index. cid -> [updated_at, src, row, seq]
        #   src: -1 = staging chunk, >=0 = index into parts (rglob order)
        #   row: row offset within that source
        #   seq: first-win order — the output position of the cid in the old
        #        dict-merge (latest[cid]=r kept the original insertion slot
        #        on replacement), restored by sorting on the unique seq.
        idx: Dict[str, List] = {}
        seq = 0
        for s_row, r in enumerate(staging_rows):
            cid = r.get("condition_id")
            if not cid:
                continue
            u = r.get("updated_at", "")
            cur = idx.get(cid)
            if cur is None:
                idx[cid] = [u, -1, s_row, seq]
                seq += 1
            elif u > cur[0]:  # strict: exact ties keep the first row, as before
                cur[0], cur[1], cur[2] = u, -1, s_row
        for f_idx, part in enumerate(parts):
            try:
                pf = pq.ParquetFile(str(part))
                names = pf.schema_arrow.names
                if "condition_id" not in names:
                    continue  # no candidate rows possible (old: every row cid-None -> skipped)
                cols = ["condition_id"] + (["updated_at"] if "updated_at" in names else [])
                row_off = 0
                for batch in pf.iter_batches(batch_size=self._COMPACT_BATCH_ROWS, columns=cols):
                    cvals = batch.column("condition_id").to_pylist()
                    uvals = (batch.column("updated_at").to_pylist()
                             if "updated_at" in names else [""] * batch.num_rows)
                    for i in range(batch.num_rows):
                        cid = cvals[i]
                        if not cid:
                            continue
                        u = uvals[i]
                        cur = idx.get(cid)
                        if cur is None:
                            idx[cid] = [u, f_idx, row_off + i, seq]
                            seq += 1
                        elif u > cur[0]:
                            cur[0], cur[1], cur[2] = u, f_idx, row_off + i
                    row_off += batch.num_rows
            except Exception as e:
                print(f"[markets_log] WARN compact pass1 unreadable fragment (skipped whole): {part}: {e}")
                continue

        # pass 2: re-read only the winning rows, batch-bounded. Pieces are
        # ISO-canonized at creation (mixed settlement vintages merge
        # columnar, see _canon_settlement_piece) and coalesced every
        # _COMPACT_COALESCE_PIECES so the piece count stays O(files/1024).
        per_src: Dict[int, Dict[int, int]] = {}
        for entry in idx.values():
            per_src.setdefault(entry[1], {})[entry[2]] = entry[3]
        chunk_tables: List[pa.Table] = []
        piece_buf: List[pa.Table] = []

        def _acc(piece: pa.Table) -> None:
            piece_buf.append(piece)
            if len(piece_buf) >= self._COMPACT_COALESCE_PIECES:
                chunk_tables.append(concat(piece_buf) if len(piece_buf) > 1 else piece_buf[0])
                piece_buf.clear()

        if staging_chunk is not None:
            sm = per_src.pop(-1, None)
            if sm:
                rows_sorted = sorted(sm)
                piece = staging_chunk.take(pa.array(rows_sorted, pa.int64()))
                piece = piece.append_column(
                    _WIN_SEQ_COL, pa.array([sm[r_] for r_ in rows_sorted], pa.int64()))
                _acc(self._canon_settlement_piece(piece))
        for src in sorted(per_src):
            rows_map = per_src[src]
            want = sorted(rows_map)
            wp = 0
            row_off = 0
            pieces: List[pa.Table] = []
            try:
                pf = pq.ParquetFile(str(parts[src]))
                for batch in pf.iter_batches(batch_size=self._COMPACT_BATCH_ROWS):
                    end = row_off + batch.num_rows
                    local_rows: List[int] = []
                    local_seqs: List[int] = []
                    while wp < len(want) and want[wp] < end:
                        r_ = want[wp]
                        local_rows.append(r_ - row_off)
                        local_seqs.append(rows_map[r_])
                        wp += 1
                    if local_rows:
                        piece = pa.Table.from_batches(
                            [batch.take(pa.array(local_rows, pa.int64()))])
                        pieces.append(piece.append_column(
                            _WIN_SEQ_COL, pa.array(local_seqs, pa.int64())))
                    row_off = end
                    if wp >= len(want):
                        break  # past this fragment's last winner — stop reading
            except Exception as e:
                print(f"[markets_log] WARN compact pass2 unreadable fragment "
                      f"(its winner rows rejoin on the next rebuild): {parts[src]}: {e}")
                continue
            for p in pieces:
                _acc(self._canon_settlement_piece(p))
            del pieces
        if piece_buf:
            chunk_tables.append(concat(piece_buf) if len(piece_buf) > 1 else piece_buf[0])
            piece_buf.clear()

        if not chunk_tables:
            # Ensure empty table still has schema (unchanged empty path)
            table = pa.Table.from_pylist([], schema=pa.schema([]))
        else:
            # verbatim union; a still-unmergeable schema pair (anything the
            # piece-level canon cannot reach) fails closed — the error
            # propagates and the caller keeps markets_latest unchanged,
            # exactly like the old code's terminal retry.
            merged = concat(chunk_tables)
            ordered = merged.sort_by([(_WIN_SEQ_COL, "ascending")]).drop_columns([_WIN_SEQ_COL])
            table = self._nullify_all_empty_columns(ordered)

        tmp_path = latest_dir / "markets_latest.parquet.tmp"
        final_path = latest_dir / "markets_latest.parquet"
        pq.write_table(table, str(tmp_path), compression="zstd")
        # atomic rename (§10A)
        _os_replace_safe(tmp_path, final_path)
        return final_path

    @classmethod
    def _canon_settlement_piece(cls, piece: "pa.Table") -> "pa.Table":
        """ISO-cast an integer/float-typed settlement_ts_utc piece in place.

        The production log still carries both settlement vintages (pre-2026-
        10-14 fragments typed int64 epoch-ms beside the canonical strings),
        and their winner rows cannot merge columnar. Casting at the PIECE
        level (a fragment's few winner rows take the Python detour, not the
        whole winner set) keeps the union columnar and bounded. Same
        contract as _iso_from_epoch_ms: only numeric values inside the
        2020-2040 plausibility band become ISO; digit-strings, out-of-band
        values and everything else pass through verbatim, and a still-
        unmergeable union fails closed at the concat."""
        if "settlement_ts_utc" not in piece.schema.names:
            return piece
        f = piece.schema.field("settlement_ts_utc")
        if not (pa.types.is_integer(f.type) or pa.types.is_floating(f.type)):
            return piece
        rows = piece.to_pylist()
        for r in rows:
            ts = cls._iso_from_epoch_ms(r.get("settlement_ts_utc"))
            if ts is not None:
                r["settlement_ts_utc"] = ts
        out = pa.Table.from_pylist(rows)
        del rows
        return out

    @staticmethod
    def _nullify_all_empty_columns(table: "pa.Table") -> "pa.Table":
        """Match the from_pylist output typing of the old dict-merge compact.

        pa.Table.from_pylist types a column whose values are ALL None as
        null; concat promotion instead keeps each fragment's declared type
        (e.g. today's all-null resolution_rule stays string). Production
        markets_latest carries such columns as null-typed, so the rebuilt
        view keeps exactly that: all-null columns become null-typed again.
        Values are unchanged (all None either way)."""
        for i, col in enumerate(table.columns):
            if col.null_count == table.num_rows:
                table = table.set_column(i, table.schema.field(i).with_type(pa.null()),
                                         pa.nulls(table.num_rows))
        return table

    def load_latest(self, parquet_data_dir: Optional[Path] = None) -> List[Dict]:
        base = Path(parquet_data_dir) if parquet_data_dir else self.data_dir
        p = base / "markets_latest" / "markets_latest.parquet"
        if not p.exists():
            return []
        try:
            return read_table(p).to_pylist()
        except Exception:
            return []
