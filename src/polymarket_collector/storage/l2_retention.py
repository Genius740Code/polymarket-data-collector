"""l2_raw time-partitioned retention — move-to-quarantine, never direct delete.

Problem: l2_raw is a verbatim WS firehose (~6.4G/day measured 2026-10-05:
87.6M rows / 6.36GB) while the data disk is 100G at 25% (~11 days headroom).

Safety contract (AGENT.md / AGENTS.md real-data-only):
- DISABLED by default: ``retention_days`` unset/None/<=0 retains everything
  (merge is zero-behavior-change; nothing moves until an operator sets
  ``retention_days`` and restarts).
- Retention applies ONLY to ``l2_raw`` ``date=`` partitions. Gap evidence
  (``collector_events`` / ``resync_episodes`` / ``markets_log`` /
  ``markets_latest``) is never touched by this code path — any other dataset
  is refused.
- Eligible partitions MOVE ``l2_raw/date=...`` -> ``_quarantine/l2_raw/date=...``
  via atomic ``os.replace`` (dir rename, same filesystem). A failed move
  KEEPS the source. No direct unlink of live data exists in this module.
- One manifest row per move (partition, rows, sha, moved_ts) is appended to
  ``_quarantine/l2_raw/_retention_manifest.parquet`` (tmp+rename publish).
- Guards (ALL must pass or the partition stays):
  1. partition older than cutoff (``now - retention_days``);
  2. every distinct ``condition_id`` in the partition maps to a market that
     ENDED before the cutoff (``markets_latest``; unknown condition -> keep);
  3. slowest-lane checkpoint passed: the partition's end-of-day is at or
     before the slowest lane's verified-upload checkpoint (same predicate
     ``export.py:cleanup_local_data`` uses — min ``last_upload_unix_ms``
     across lanes; no checkpoint -> keep).
- Reap (``reap_l2_quarantine``) is the ONLY delete in this feature: it
  removes ONLY ``_quarantine/l2_raw/date=*`` dirs, ONLY when enabled AND
  disk pressure demands it (free bytes below threshold), oldest-first,
  bounded to ``max_deletes_per_cycle`` per call. Default OFF.

Patterns followed: ``storage/compaction.py`` merge-verify-then-delete
(verify before any destructive step), ``storage/quarantine.py`` bounded
reaper (only bounded deleter), ``export.py`` rolling-window prune
(slowest-lane checkpoint + market-end cutoff, move-to-quarantine, failed
move keeps file). All helpers never raise (fail closed, log loudly).
"""

from __future__ import annotations

import datetime as _dt
import hashlib as _hashlib
import json as _json
import os as _os
import uuid as _uuid
from pathlib import Path

ALLOWED_DATASET = "l2_raw"

GAP_EVIDENCE_DATASETS = frozenset({
    "collector_events",
    "resync_episodes",
    "markets_log",
    "markets_latest",
})

QUARANTINE_DIRNAME = "_quarantine"
MANIFEST_NAME = "_retention_manifest.parquet"

DAY_MS = 86_400_000


def _log(msg: str) -> None:
    print(msg, flush=True)


def dataset_allowed(dataset: str) -> bool:
    """True only for the l2_raw dataset. Gap evidence is never eligible."""
    try:
        return str(dataset) == ALLOWED_DATASET
    except Exception:
        return False


def _parse_date_partition(name: str):
    """'date=YYYY-MM-DD' -> datetime.date, or None (never raises)."""
    try:
        if not name.startswith("date="):
            return None
        return _dt.datetime.strptime(name[5:], "%Y-%m-%d").date()
    except Exception:
        return None


def _partition_end_ms(day: _dt.date) -> int:
    """End-of-day UTC in epoch ms (last ms of the partition's date)."""
    try:
        start = _dt.datetime(day.year, day.month, day.day,
                             tzinfo=_dt.timezone.utc).timestamp() * 1000
        return int(start) + DAY_MS - 1
    except Exception:
        return -1


def _now_ms() -> int:
    try:
        return int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp() * 1000)
    except Exception:
        return 0


def _load_market_ends(base: Path) -> dict | None:
    """condition_id -> market_end_ts_ms from markets_latest (None = unreadable)."""
    try:
        latest = base / "markets_latest" / "markets_latest.parquet"
        if not latest.exists():
            _log("[l2-retention] WARN markets_latest missing — keeping all partitions")
            return None
        from .parquet_io import read_table as _read

        tbl = _read(latest)
        if tbl is None or "condition_id" not in tbl.schema.names:
            _log("[l2-retention] WARN markets_latest unreadable — keeping all partitions")
            return None
        ends: dict = {}
        cids = tbl.column("condition_id").to_pylist()
        if "market_end_ts_ms" in tbl.schema.names:
            mes = tbl.column("market_end_ts_ms").to_pylist()
        else:
            mes = [None] * len(cids)
        for cid, me in zip(cids, mes):
            if not cid:
                continue
            try:
                if me is not None:
                    ends[str(cid)] = int(me)
            except Exception:
                continue
        return ends
    except Exception as e:
        _log(f"[l2-retention] WARN market-end map failed ({e}) — keeping all partitions")
        return None


def _lane_checkpoint_from_state(state: dict, ds_key: str | None) -> int | None:
    """Best verified-upload ms for one lane (own dataset entry preferred)."""
    try:
        if not isinstance(state, dict):
            return None
        if ds_key and isinstance(state.get(ds_key), dict):
            v = state[ds_key].get("last_upload_unix_ms")
            try:
                return int(v) if v else None
            except Exception:
                return None
        vals = []
        for v in state.values():
            try:
                if isinstance(v, dict) and v.get("last_upload_unix_ms"):
                    vals.append(int(v["last_upload_unix_ms"]))
            except Exception:
                continue
        return max(vals) if vals else None
    except Exception:
        return None


def slowest_lane_checkpoint_ms(
    data_dir: str | Path,
    lanes: list[str] | None = None,
    explicit_ms: int | None = None,
) -> int | None:
    """Slowest-lane verified-upload checkpoint (same predicate export.py uses).

    ``min(last_upload_unix_ms)`` across all enabled lanes — a partition is
    safe only once ITS lane uploaded it. Any lane without a verified upload
    (or any read failure) yields None: fail closed, prune nothing.
    ``explicit_ms`` overrides (tests / explicit-checkpoint callers). Never raises.
    """
    try:
        if explicit_ms is not None:
            return int(explicit_ms)
        base = Path(data_dir)
        if lanes is None:
            try:
                from ..config import CollectorConfig as _CC

                lanes = [str(t).lower() for t in (_CC.load().timeframes or [])]
            except Exception:
                lanes = []
        if not lanes:
            lanes = ["5m"]
        lane_ds: dict = {}
        try:
            from ..config import CollectorConfig as _CCds

            _cfg = _CCds.load()
            for lane in lanes:
                try:
                    lane_ds[lane] = str(_cfg.kaggle.datasets[lane])
                except Exception:
                    try:
                        _dp = getattr(_cfg.kaggle, "dataset_prefix", None)
                        lane_ds[lane] = str(_dp) if _dp else "gghgg1/polymarket-5m-crypto"
                    except Exception:
                        lane_ds[lane] = "gghgg1/polymarket-5m-crypto"
        except Exception:
            for lane in lanes:
                lane_ds.setdefault(lane, "gghgg1/polymarket-5m-crypto")
        per_lane: dict = {}
        for lane in lanes:
            best = None
            for cand in (base / "kaggle_staging" / lane / "_kaggle_state.json",
                         base / "kaggle_staging" / "_kaggle_state.json"):
                try:
                    if not cand.exists():
                        continue
                    state = _json.loads(cand.read_text())
                    v = _lane_checkpoint_from_state(state, lane_ds.get(lane))
                    if v is not None:
                        best = v if best is None else max(best, v)
                except Exception:
                    continue
            if best is None:
                _log(f"[l2-retention] lane {lane} has no verified upload — "
                     f"retention skipped (fail closed)")
                return None
            per_lane[lane] = best
        if not per_lane:
            return None
        return min(per_lane.values())
    except Exception as e:
        _log(f"[l2-retention] WARN checkpoint resolve failed ({e}) — keeping all partitions")
        return None


def _partition_condition_ids(part_dir: Path) -> set | None:
    """Distinct condition_ids in a date= partition (None = unknown -> keep)."""
    try:
        import pyarrow.parquet as pq

        cids: set = set()
        files = sorted(p for p in part_dir.rglob("*.parquet") if not p.name.endswith(".tmp"))
        if not files:
            return set()
        for f in files:
            try:
                t = pq.read_table(str(f), columns=["condition_id"])
            except Exception:
                return None  # unreadable or no condition_id — conservative keep
            try:
                if t is None or t.num_rows == 0 or "condition_id" not in t.schema.names:
                    continue
                for c in t.column("condition_id").to_pylist():
                    if c:
                        cids.add(str(c))
            except Exception:
                return None
            finally:
                try:
                    del t
                except Exception:
                    pass
        return cids
    except Exception:
        return None


def _partition_rows_and_sha(part_dir: Path, base: Path) -> tuple[int, str]:
    """(total rows, sha1 over sorted relpath:size) for the manifest. Never raises."""
    try:
        import pyarrow.parquet as pq

        total = 0
        parts = []
        for f in sorted(part_dir.rglob("*.parquet")):
            if f.name.endswith(".tmp"):
                continue
            try:
                rel = str(f.relative_to(base))
            except Exception:
                rel = f.name
            try:
                size = f.stat().st_size
            except OSError:
                size = -1
            parts.append(f"{rel}:{size}")
            try:
                total += int(pq.read_metadata(str(f)).num_rows)
            except Exception:
                pass
        sha = _hashlib.sha1("\n".join(parts).encode()).hexdigest()
        return total, sha
    except Exception:
        return 0, "unknown"


def _append_manifest(base: Path, row: dict) -> bool:
    """Append one row to the quarantine manifest (tmp+rename). Never raises."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        from .parquet_io import read_table as _read

        qdir = base / QUARANTINE_DIRNAME / ALLOWED_DATASET
        qdir.mkdir(parents=True, exist_ok=True)
        manifest = qdir / MANIFEST_NAME
        schema = pa.schema([
            pa.field("partition", pa.string()),
            pa.field("rows", pa.int64()),
            pa.field("sha", pa.string()),
            pa.field("moved_ts", pa.int64()),
        ])

        def _conform(tbl):
            try:
                cols = []
                for f in schema:
                    if f.name in tbl.schema.names:
                        c = tbl.column(f.name)
                        cols.append(c.cast(f.type) if not c.type.equals(f.type) else c)
                    else:
                        cols.append(pa.array([None] * tbl.num_rows, type=f.type))
                return pa.table(cols, schema=schema)
            except Exception:
                return None

        new_tbl = pa.Table.from_pylist([{
            "partition": str(row.get("partition")),
            "rows": int(row.get("rows", 0)),
            "sha": str(row.get("sha", "")),
            "moved_ts": int(row.get("moved_ts", 0)),
        }], schema=schema)
        existing = _read(manifest) if manifest.exists() else None
        if existing is not None and existing.num_rows:
            existing = _conform(existing)
            out = pa.concat_tables([existing, new_tbl], promote_options="default") if existing is not None else new_tbl
        else:
            out = new_tbl
        tmp = qdir / f"{MANIFEST_NAME}.{_uuid.uuid4().hex[:8]}.tmp"
        try:
            pq.write_table(out, str(tmp), compression="zstd")
            try:
                with open(str(tmp), "rb") as _fh:
                    try:
                        _os.fsync(_fh.fileno())
                    except Exception:
                        pass
            except Exception:
                pass
            _os.replace(str(tmp), str(manifest))
            return True
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
    except Exception as e:
        _log(f"[l2-retention] WARN manifest append failed ({e}) — move already done, data safe in quarantine")
        return False


def quarantine_partition(
    data_dir: str | Path,
    dataset: str,
    partition: str,
    *,
    dry_run: bool = False,
) -> dict:
    """Move one ``<dataset>/<partition>`` dir into ``_quarantine/``.

    Refuses anything but ``l2_raw`` (gap evidence can never pass through
    this path). Atomic dir rename; a failed move keeps the source.
    Never raises.
    """
    try:
        base = Path(data_dir)
        if not dataset_allowed(dataset):
            return {"moved": False, "reason": f"refused: dataset {dataset!r} not eligible (l2_raw only)"}
        src = base / str(dataset) / str(partition)
        if not src.exists() or not src.is_dir():
            return {"moved": False, "reason": f"missing: {dataset}/{partition}"}
        dest = base / QUARANTINE_DIRNAME / str(dataset) / str(partition)
        if dest.exists():
            return {"moved": False, "reason": f"kept: quarantine target exists ({dest})"}
        rows, sha = _partition_rows_and_sha(src, base)
        if dry_run:
            return {"moved": False, "dry_run": True, "rows": rows, "sha": sha,
                    "reason": f"dry-run would move {dataset}/{partition} ({rows} rows)"}
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            _os.replace(str(src), str(dest))
        except Exception as mv:
            return {"moved": False, "reason": f"kept: move failed ({mv}); source left in place"}
        moved_ts = _now_ms()
        ok = _append_manifest(base, {"partition": str(partition), "rows": rows,
                                     "sha": sha, "moved_ts": moved_ts})
        _log(f"[l2-retention] quarantined {dataset}/{partition} "
             f"({rows} rows, sha {sha[:12]}…, manifest={'ok' if ok else 'FAILED-data-safe'})")
        return {"moved": True, "rows": rows, "sha": sha, "moved_ts": moved_ts,
                "dest": str(dest), "manifest_ok": ok}
    except Exception as e:
        return {"moved": False, "reason": f"kept: unexpected ({e})"}


def apply_l2_retention(
    data_dir: str | Path,
    *,
    retention_days: int | float | None = None,
    now_ms: int | None = None,
    checkpoint_ms: int | None = None,
    lanes: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """Time-partitioned retention for l2_raw. Never raises.

    Disabled (retain everything) when ``retention_days`` is None or <= 0.
    Otherwise every ``l2_raw/date=`` partition must pass ALL guards or it
    stays: age cutoff, market-end cutoff, slowest-lane checkpoint.
    Returns ``{"moved": [...], "kept": {partition: reason}, "disabled": bool}``.
    """
    try:
        base = Path(data_dir)
        if retention_days is None:
            return {"moved": [], "kept": {}, "disabled": True,
                    "reason": "disabled: retention_days unset (retain everything)"}
        try:
            retention_days_f = float(retention_days)
        except Exception:
            return {"moved": [], "kept": {}, "disabled": True,
                    "reason": f"disabled: retention_days {retention_days!r} unparsable"}
        if retention_days_f <= 0:
            return {"moved": [], "kept": {}, "disabled": True,
                    "reason": "disabled: retention_days <= 0 (retain everything)"}
        now = int(now_ms) if now_ms is not None else _now_ms()
        cutoff_ms = now - int(retention_days_f * DAY_MS)
        l2_root = base / ALLOWED_DATASET
        if not l2_root.exists():
            return {"moved": [], "kept": {}, "disabled": False, "cutoff_ms": cutoff_ms}
        checkpoint = slowest_lane_checkpoint_ms(base, lanes=lanes, explicit_ms=checkpoint_ms)
        if checkpoint is None:
            kept = {}
            for part in sorted(p for p in l2_root.iterdir() if p.is_dir()):
                kept[part.name] = "no verified slowest-lane checkpoint (fail closed)"
            return {"moved": [], "kept": kept, "disabled": False,
                    "cutoff_ms": cutoff_ms, "checkpoint_ms": None}
        end_by_cid = _load_market_ends(base)
        if end_by_cid is None:
            kept = {}
            for part in sorted(p for p in l2_root.iterdir() if p.is_dir()):
                kept[part.name] = "markets_latest unreadable (fail closed)"
            return {"moved": [], "kept": kept, "disabled": False,
                    "cutoff_ms": cutoff_ms, "checkpoint_ms": checkpoint}
        moved: list = []
        kept = {}
        for part in sorted(p for p in l2_root.iterdir() if p.is_dir()):
            day = _parse_date_partition(part.name)
            if day is None:
                kept[part.name] = "not a date= partition (left alone)"
                continue
            pend = _partition_end_ms(day)
            if pend >= cutoff_ms:
                kept[part.name] = f"younger than cutoff ({cutoff_ms})"
                continue
            if pend > checkpoint:
                kept[part.name] = "slowest-lane checkpoint has not passed this partition"
                continue
            cids = _partition_condition_ids(part)
            if cids is None:
                kept[part.name] = "condition_ids unreadable (fail closed)"
                continue
            if not cids:
                kept[part.name] = "empty partition (nothing to move)"
                continue
            ends = [end_by_cid.get(c) for c in cids]
            if any(e is None for e in ends):
                kept[part.name] = "unknown condition_id (market-end cutoff, conservative keep)"
                continue
            if max(ends) >= cutoff_ms:
                kept[part.name] = "some market inside the retention leeway (market-end cutoff)"
                continue
            res = quarantine_partition(base, ALLOWED_DATASET, part.name, dry_run=dry_run)
            if res.get("moved"):
                moved.append({"partition": part.name, **res})
            else:
                kept[part.name] = str(res.get("reason", "move declined"))
        return {"moved": moved, "kept": kept, "disabled": False,
                "cutoff_ms": cutoff_ms, "checkpoint_ms": checkpoint}
    except Exception as e:
        _log(f"[l2-retention] WARN retention pass failed ({e}) — keeping everything")
        return {"moved": [], "kept": {}, "disabled": False, "error": str(e)[:200]}


def _disk_free_bytes(path: Path) -> int | None:
    try:
        import shutil as _sh

        return int(_sh.disk_usage(str(path)).free)
    except Exception:
        return None


def reap_l2_quarantine(
    data_dir: str | Path,
    *,
    enabled: bool = False,
    min_free_bytes: int | None = None,
    max_deletes_per_cycle: int = 1,
    dry_run: bool = False,
) -> dict:
    """Bounded expiry of quarantined l2_raw partitions. Never raises.

    The ONLY delete in this feature. Removes ONLY
    ``_quarantine/l2_raw/date=*`` dirs (never the live hive, never the
    manifest), oldest-first, at most ``max_deletes_per_cycle`` per call,
    and ONLY when ``enabled`` AND disk pressure demands it
    (free bytes < ``min_free_bytes``; default OFF). ``dry_run`` reports
    without deleting.
    """
    stats = {"deleted": 0, "bytes_deleted": 0, "kept": 0, "would_delete": []}
    try:
        if not enabled:
            stats["reason"] = "disabled (default off)"
            return stats
        base = Path(data_dir)
        qroot = base / QUARANTINE_DIRNAME / ALLOWED_DATASET
        if not qroot.exists():
            stats["reason"] = "no l2 quarantine dir"
            return stats
        if min_free_bytes is None:
            try:
                from ..config import CollectorConfig as _CCm

                min_free_bytes = int(_CCm.load().storage.disk_space_min_bytes)
            except Exception:
                min_free_bytes = 1_073_741_824
        try:
            max_n = max(0, int(max_deletes_per_cycle))
        except Exception:
            max_n = 1
        free = _disk_free_bytes(base)
        if free is None:
            stats["reason"] = "disk free unreadable (fail closed)"
            return stats
        stats["free_bytes"] = free
        stats["min_free_bytes"] = int(min_free_bytes)
        if free >= int(min_free_bytes):
            stats["reason"] = f"no disk pressure (free {free}B >= {min_free_bytes}B)"
            cands = [p for p in qroot.iterdir() if p.is_dir() and p.name.startswith("date=")]
            stats["kept"] = len(cands)
            return stats
        cands = sorted(
            (p for p in qroot.iterdir() if p.is_dir() and p.name.startswith("date=")),
            key=lambda p: p.name,
        )
        deleted = 0
        for cand in cands:
            if deleted >= max_n:
                break
            try:
                size = sum(f.stat().st_size for f in cand.rglob("*") if f.is_file())
            except OSError:
                size = 0
            if dry_run:
                stats["would_delete"].append(str(cand.relative_to(base)))
                stats["deleted"] += 1
                stats["bytes_deleted"] += size
                deleted += 1
                continue
            try:
                import shutil as _sh2

                _sh2.rmtree(str(cand))
                stats["deleted"] += 1
                stats["bytes_deleted"] += size
                deleted += 1
                _log(f"[l2-retention-reap] deleted {cand.relative_to(base)} ({size}B, disk pressure)")
            except Exception as de:
                _log(f"[l2-retention-reap] WARN could not delete {cand}: {de}")
        try:
            rest = [p for p in qroot.iterdir() if p.is_dir() and p.name.startswith("date=")]
            stats["kept"] = len(rest)
        except Exception:
            pass
        return stats
    except Exception as e:
        _log(f"[l2-retention-reap] WARN reap failed ({e}) — keeping everything")
        stats["reason"] = f"error: {e}"[:200]
        return stats
