#!/usr/bin/env python
"""Acceptance scorecard — one compact table per lane for the 99.9% gate (spec §10).

Reuses existing infrastructure instead of rebuilding it:
- scripts/gate_probe.py  → density% (tick census) + CompNorm% (spread-normalized
  complementarity). It is a fully offline, footer/sampled-row probe (newest-file
  sample by mtime; --probe-window widens the sample; never a live WS/REST run),
  so it is part of the default scorecard run.
- src/polymarket_collector/completeness.py::compute_daily_completeness → daily
  raw/clean/source metrics. It re-reads the whole collector_events /
  resync_episodes partitions, which on this box takes ~5 min, so it runs in a
  spawned child process that overlaps the bounded scan; if it has not finished
  within its join allowance the clean metrics print n/a(partial) honestly.
- data/kaggle_staging/_progress.json + <tf>/_kaggle_state.json → uploads/cycle
  and last verified upload per lane (metadata only, no network).
- data/markets_latest/markets_latest.parquet → settlement-source share
  (streamed, bounded batches).

T1 block: bounded day scan of the per-date book_snapshots_500ms parts —
projected columns, streamed, tiny aggregate counters only (no tables kept in
memory), .tmp skipped, files replaced mid-read (compaction) skipped-and-counted,
wall-clock budget (--budget-s, default 560s) split as FAIR PER-LANE SLICES
(remaining / lanes-not-yet-run, recomputed after every lane — an early lane
hands its leftover wall time to the lanes still pending) with explicit
partial=yes rows and n/a(partial) for any metric that could not finish.
The orphan counter checks non-blank resync_ids against the FULL
resync_episodes table (id-only projected read of every date partition,
built once per run, deduped): episodes persist in the partition of the day
they started, so a day-scoped check false-positives rows written hours past
midnight. Honest exceptions only: a number is printed only when it was
actually computed from real files.

Coverage semantics (backlog #2, 2026-10-19 handoff §4b): the local
book_snapshots_500ms/ hive is a ROLLING mirror — verified-uploaded rows are
pruned locally after every Kaggle verified upload, so a next-day scorecard
of a pruned date reads a low local row count (~7% density) that is a mirror
artifact, not a loss (the data lives in the verified Kaggle dataset), and a
same-day score is a partial day by construction. A footer-only row census
per lane plus the grid-derived full-day expectation classify each scored
date (full|partial|stale) and the verdict reflects it: same-day=partial-ok
(SOAK — an in-progress day is neither accepted nor density-FAILED on
coverage alone), pruned days=stale-local (warn-only, backed by the kaggle
verified-upload checkpoint; the honest line is "locally pruned,
verified-uploaded"), full days unchanged.

Targets (docs/PERFECT_DATA_SPEC.md §10, as amended):
  density >= 99% | CompNorm <= 0.1% | every T1 counter = 0 | official-share >= 99%
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as _dt
import hashlib
import json
import math
import multiprocessing
import os
import random
import signal
import sys
import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import gate_probe  # noqa: E402  (offline window probe, reused as-is)

DATA_ROOT = Path("/home/fese/polymarket-data-collector/data")

# --- bounded-scan knobs (3.9GB box: keep every buffer tiny) ----------------
T1_WORKERS = 8            # parallel parquet readers for the core T1 scan
                          # (bench 2026-10-18: 55.2s vs 66.2s per full lane at 4
                          # on this 2-core box; bounded pending buffers only)
FOOTER_WORKERS = 8        # small-file footer/column reads parallelize better
DUP_BLOOM_BITS = 26       # 2^26 bits = 8 MiB fixed bound for the (condition, ts) set
DUP_BLOOM_WAYS = 3        # ~2e-4 false-positive rate at ~1.3M keys/day
IDENTITY_COLS = ("condition_id", "ts_snapshot_ns", "snapshot_id", "market_id", "series_id")
BEST_COLS = ("up_bid", "up_ask", "down_bid", "down_ask")
T1_CORE_COLS = [
    "ts_snapshot_ns", "condition_id", "snapshot_id", "market_id", "series_id",
    "book_state", "resync_id",
    "up_bid", "up_ask", "down_bid", "down_ask",
]
GRID_NS = gate_probe.GRID_NS  # 500ms
DEPTH_TOL = 1e-6
G6_SAMPLE_FILES = 20
G6_SAMPLE_ROWS_PER_FILE = 2000
G6_BATCH_ROWS = 256
G6_SIDES = (("up", "bid"), ("up", "ask"), ("down", "bid"), ("down", "ask"))
OFFICIAL_SOURCES = ("on_chain_confirmed", "polymarket_official")
INFERRED_SOURCES = ("inferred_nearest",)
RESYNC_ID_SET_CAP = 2_000_000  # exact per-run set; full episode table ~530k unique ids
RESYNC_LOAD_FRAC = 0.25  # share of the budget for loading the full resync-id set
LANE_SLICE_MIN_S = 5.0  # floor for a per-lane fair slice (empty lanes still run)
COMP_JOIN_ALLOWANCE_S = 600.0  # extra wall allowance to join the completeness child
                               # (measured 2026-10-18: it re-reads ~8522 collector_events
                               #  + ~6046 episode files per day, needs >500s here)
TARGETS = "targets: density>=99% | CompNorm<=0.1% | T1 counters=0 | official-share>=99%"

_G6_COLS: list | None = None  # built once (level price/size cols 1..100 + depth cols)


def _g6_cols() -> list:
    global _G6_COLS
    if _G6_COLS is None:
        cols: list = []
        for outc, side in G6_SIDES:
            for lvl in range(1, 101):
                cols.append(f"{outc}_{side}_level_{lvl}_price")
                cols.append(f"{outc}_{side}_level_{lvl}_size")
        for outc, side in G6_SIDES:
            for thc in (1, 5, 10):
                cols.append(f"{outc}_{side}_depth_{thc}c")
        _G6_COLS = cols
    return _G6_COLS


# ---------------------------------------------------------------------------
# Approximate-hash duplicate set (bounded memory; mission-approved approx)
# ---------------------------------------------------------------------------


class ApproxHashDupSet:
    """Bounded (2^26-bit, 8 MiB) multi-hash bit set for the duplicate-key census.

    Printed with approx=yes: a rare false collision (a fresh key hashing onto
    set cells) can inflate the dup counter; a false negative cannot (a real
    duplicate key always collides on all ways and is always counted), so a
    printed 0 is exact. Memory is fixed — the day's (condition_id,
    ts_snapshot_ns) population never touches it.
    """

    def __init__(self, bits: int = DUP_BLOOM_BITS, ways: int = DUP_BLOOM_WAYS) -> None:
        self.bits = bits
        self.ways = ways
        self.mask = (1 << bits) - 1
        self.cells = bytearray((1 << bits) // 8)
        self.items = 0

    def add_if_new(self, key_bytes: bytes) -> bool:
        """Insert key; return True when the key was new, False when seen-ish."""
        h = hashlib.blake2b(key_bytes, digest_size=16).digest()
        a = int.from_bytes(h[:8], "big")
        b = int.from_bytes(h[8:], "big")
        p2 = (b & self.mask) | 1  # odd, non-zero probe stride
        seen = True
        for i in range(self.ways):
            idx = (a + i * p2) & self.mask
            byte = self.cells[idx >> 3]
            bit = 1 << (idx & 7)
            if not byte & bit:
                seen = False
                self.cells[idx >> 3] = byte | bit
        if not seen:
            self.items += 1
        return not seen


# ---------------------------------------------------------------------------
# Scan state / partial handling
# ---------------------------------------------------------------------------


@dataclass
class ScanBudget:
    """Wall-clock budget shared by every phase of a scorecard run."""

    started: float = field(default_factory=time.monotonic)
    budget_s: float = 240.0

    def remaining(self) -> float:
        return self.started + self.budget_s - time.monotonic()

    def expired(self) -> bool:
        return self.remaining() <= 0


@dataclass
class T1Block:
    """Aggregate counters only — never row data (3.9GB box discipline)."""

    asset: str
    files_total: int = 0
    files_scanned: int = 0
    files_skipped: int = 0  # unreadable or replaced mid-read (compaction)
    rows_scanned: int = 0
    ident_nulls: dict = field(default_factory=dict)  # col -> null-or-blank count
    dup_key_count: int = 0   # approx-hash set; printed with approx legend
    out_of_range_rows: int = 0   # best price outside [0, 1]
    zero_price_rows: int = 0     # best price exactly 0.0
    state_counts: dict = field(default_factory=dict)  # book_state -> rows
    orphan_resync_rows: int = 0  # non-blank resync_id missing from the FULL episode table
    offgrid_rows: int = 0        # ts % 500ms != 0
    g6_rows_checked: int = 0
    g6_mismatch_rows: int = 0
    g6_files_sampled: int = 0
    g6_done: bool = False        # False → g6 fields degrade to n/a(partial)
    resync_ids_overflow: bool = False
    partial: bool = False

    def t1_total(self) -> int:
        return (
            sum(self.ident_nulls.values())
            + self.dup_key_count
            + self.out_of_range_rows
            + self.orphan_resync_rows
            + self.offgrid_rows
            + self.g6_mismatch_rows
        )


# ---------------------------------------------------------------------------
# Date / asset resolution
# ---------------------------------------------------------------------------


def resolve_date(data_root: Path, date_opt: str | None) -> str:
    if date_opt:
        date_str = date_opt
    else:
        base = data_root / "book_snapshots_500ms"
        date_dirs = sorted(
            (d for d in base.iterdir() if d.is_dir() and d.name.startswith("date=")),
            key=lambda d: d.name,
        )
        if not date_dirs:
            raise SystemExit("ERROR: no date partitions under book_snapshots_500ms/")
        date_str = date_dirs[-1].name.split("=", 1)[1]
    if not (data_root / "book_snapshots_500ms" / f"date={date_str}").exists():
        raise SystemExit(f"ERROR: no data partition for date {date_str}")
    return date_str


def resolve_assets(data_root: Path, date_str: str, assets_opt: str | None) -> list:
    src_root = data_root / "book_snapshots_500ms" / f"date={date_str}"
    found = sorted(d.name.split("=", 1)[1] for d in src_root.glob("asset=*") if d.is_dir())
    if assets_opt:
        wanted = [a.strip().upper() for a in assets_opt.split(",") if a.strip()]
        missing = [a for a in wanted if a not in found]
        if missing:
            raise SystemExit(
                f"ERROR: assets {','.join(missing)} have no data partition for {date_str}"
            )
        return wanted
    return found or []


def day_files(asset_dir: Path, since_mtime: float | None) -> list:
    """Non-.tmp parquet parts of the day path, deterministic order.

    since_mtime (mtime-scoped --hours window) keeps the honest note in the
    output that the scope is write-time bounded, not ts-scoped.
    """
    parts = sorted(p for p in asset_dir.glob("*.parquet") if not p.name.endswith(".tmp"))
    if since_mtime is not None:
        keep = []
        for p in parts:
            try:
                if p.stat().st_mtime >= since_mtime:
                    keep.append(p)
            except OSError:
                continue
        parts = keep
    return parts


# ---------------------------------------------------------------------------
# Prune-aware coverage semantics (backlog #2) — the local hive is a rolling
# mirror: rows are pruned after verified uploads, so local row counts alone
# must never be read as "data loss" (nor as local completeness).
# ---------------------------------------------------------------------------

# Per-lane (ONE asset) full-day expectation on the 500ms grid, derived from
# the grid this module already gates with — arithmetic from first principles,
# never a bare 172_800:
#   GRID_NS = 500_000_000 ns = 0.5 s per tick -> 1_000_000_000 // GRID_NS = 2 ticks/s
#   one UTC day = 24*60*60 = 86_400 s         -> 86_400 * 2 = 172_800 rows/asset-day
#   whole-partition expectation = (asset lanes in the partition) x 172_800 —
#   the per-lane line below prints the per-asset term and each lane's own
#   footer rows are compared against that same per-asset term.
ROWS_PER_SEC = 1_000_000_000 // GRID_NS                    # 2 (500ms grid)
DAY_SECONDS = 24 * 60 * 60                                 # 86_400 s of a UTC day
FULL_DAY_ROWS_PER_ASSET = DAY_SECONDS * ROWS_PER_SEC       # 172_800 rows/asset-day
STALE_LOCAL_FRAC = 0.40  # ~40% of the expectation: a pruned mirror date reads
                         # ~7% locally; a full local day reads >= 100%
COVERAGE_SCAN_S = 45.0   # footer-only census allowance for the whole run
                         # (17k footers across 7 lanes read in single-digit
                         # seconds here; generous headroom under write noise)


def classify_coverage(date_utc, local_rows, expected_est, now_utc):
    """Pure date classification of one scored date's LOCAL coverage.

    Inputs: the scored UTC date (date or datetime — a datetime is narrowed to
    its date), the lane's real local footer rows, the grid-derived full-day
    expectation (FULL_DAY_ROWS_PER_ASSET), and the wall clock, INJECTED — this
    function never reads the clock itself, so tests pin any instant without
    touching the process clock. Returns (klass, reason) with klass one of
    'full' | 'partial' | 'stale':

    - partial: the scored date is today (UTC) and the wall clock has not yet
      reached the day's end — a same-day score is a partial day by
      construction, so coverage alone must not FAIL it (verdict modifier
      partial-ok; the verdict line prints SOAK).
    - stale: the day HAS ended on the wall clock, but local_rows is below
      ~STALE_LOCAL_FRAC of expected_est — the rolling-mirror signature of a
      locally pruned, verified-uploaded date (modifier stale-local -> WARN,
      only when the kaggle checkpoint confirms; without it the density FAIL
      stands — never a loss claim, never a completeness claim).
    - full: the day ended with local coverage at/above the threshold —
      legacy full-day gate semantics apply unchanged.

    The threshold compare runs in integers (rows*5 < est*2 == rows/est < 2/5
    = 40% exactly), so the boundary is free of float drift: at exactly 40%
    the legacy semantics apply.
    """
    if isinstance(date_utc, _dt.datetime):
        date_utc = date_utc.date()
    if not isinstance(now_utc, _dt.datetime):  # a bare date has no time of day
        now_utc = _dt.datetime.combine(now_utc, _dt.time.min, tzinfo=_dt.timezone.utc)
    elif now_utc.tzinfo is None:  # a naive wall clock is read as UTC
        now_utc = now_utc.replace(tzinfo=_dt.timezone.utc)
    day_start = _dt.datetime(date_utc.year, date_utc.month, date_utc.day,
                             tzinfo=_dt.timezone.utc)
    day_end = day_start + _dt.timedelta(days=1)
    if date_utc == now_utc.date() and now_utc < day_end:
        return "partial", (
            f"same-day {date_utc.isoformat()}: wall clock {now_utc.isoformat()} "
            f"< day end {day_end.isoformat()} — partial day by construction"
        )
    if expected_est > 0 and local_rows * 5 < expected_est * 2:
        return "stale", (
            f"day ended {day_end.isoformat()}: local_rows={local_rows:,} "
            f"< {STALE_LOCAL_FRAC:.0%} of expected_est={expected_est:,} — "
            f"locally pruned/low-coverage (rolling mirror)"
        )
    return "full", (
        f"day ended {day_end.isoformat()}: local_rows={local_rows:,} "
        f"vs expected_est={expected_est:,}"
    )


def lane_footer_rows(asset_dir: Path, deadline: float,
                     workers: int = FOOTER_WORKERS) -> tuple:
    """Footer-only row census of one lane's day files -> (rows, files, unread, cut).

    pq.ParquetFile(path).metadata.num_rows reads the parquet footer and
    pf.schema_arrow carries the pq.read_schema facts off the SAME open
    footer — one open, no data pages — confirming the file is a snapshots
    part (ts_snapshot_ns column) before its rows count. .tmp files are
    excluded by day_files; unreadable or non-snapshot files are skipped and
    counted, never guessed. A scan stopped by its deadline returns cut=True
    so a partial sum can never be presented as a complete census.
    """
    files = day_files(asset_dir, None)
    total = len(files)
    if time.monotonic() > deadline:
        return 0, total, 0, True
    rows = 0
    unread = 0
    cut = False

    def _footer(fp: Path):
        try:
            pf = pq.ParquetFile(str(fp))
            if "ts_snapshot_ns" not in pf.schema_arrow.names:
                return None
            return int(pf.metadata.num_rows)
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        pending: deque = deque()
        it = iter(files)
        for _ in range(workers * 2):
            nxt = next(it, None)
            if nxt is None:
                break
            pending.append(ex.submit(_footer, nxt))
        while pending:
            if time.monotonic() > deadline:
                cut = True
                break
            fut = pending.popleft()
            n = fut.result()
            nxt = next(it, None)
            if nxt is not None:
                pending.append(ex.submit(_footer, nxt))
            if n is None:
                unread += 1
            else:
                rows += n
    if cut:
        for f in list(pending):  # cancel queued-but-unstarted footer reads
            f.cancel()
    return rows, total, unread, cut


def _parse_utc(value) -> object:
    """Best-effort ISO datetime parse -> aware UTC datetime, or None."""
    try:
        parsed = _dt.datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return parsed


def staging_manifest_rows(kaggle_root: Path) -> dict:
    """Freshest staged row count per asset from the exporter's staging manifests.

    data/kaggle_staging/*/gghgg1/*/staging-manifest.json records per-file
    FOOTER rows for the staged window files (e.g. BTC_book_snapshots_500ms
    .parquet) with its built_at_utc. Those are cutoff-bounded staging
    windows, NOT date-partition coverage, so the count is only ever printed
    as corroborating evidence and never fed to the classification.
    Returns {asset: (rows, built_at_utc)}; metadata-only, never raises.
    """
    out: dict = {}
    if not kaggle_root.exists():
        return out
    for mf in sorted(kaggle_root.glob("*/gghgg1/*/staging-manifest.json")):
        try:
            data = json.loads(mf.read_text()) or {}
        except Exception:
            continue
        built = str(data.get("built_at_utc") or "")
        for ent in data.get("files") or []:
            if not isinstance(ent, dict):
                continue
            name = str(ent.get("name") or "")
            if not name.endswith("_book_snapshots_500ms.parquet"):
                continue
            rows = ent.get("rows")
            if not isinstance(rows, int):
                continue
            asset = name[: -len("_book_snapshots_500ms.parquet")]
            prev = out.get(asset)
            if prev is None or built > str(prev[1]):
                out[asset] = (rows, built)
    return out


def kaggle_verified_covers(kaggle_root: Path, date_end_epoch_s: float) -> tuple:
    """(covered, newest_last_upload_iso) from kaggle_staging/*/_kaggle_state.json.

    An entry covers the scored date when its verified build's hive-read
    cutoff — build_start, the prune's OWN coverage proof (rows newer than
    every lane's build_start are never deleted; see export.py
    _write_kaggle_state) — is at or after the date's end, i.e. that one
    verified build read the whole date. Entries without build_start fall
    back to the literal last_upload_utc >= date end. Metadata-only: JSON
    reads, no network, no parquet reads; unreadable state files are skipped
    and never counted as coverage.
    """
    covered = False
    newest = None
    if not kaggle_root.exists():
        return covered, newest
    for state_path in sorted(kaggle_root.glob("*/_kaggle_state.json")):
        try:
            state = json.loads(state_path.read_text()) or {}
        except Exception:
            continue
        for _ds, info in state.items():
            if not isinstance(info, dict):
                continue
            lu = info.get("last_upload_utc")
            if not lu:
                continue  # no verified upload recorded for this entry
            if newest is None or str(lu) > str(newest):
                newest = lu
            bs_ms = info.get("build_start_unix_ms")
            if isinstance(bs_ms, (int, float)) and bs_ms > 0:
                frontier = bs_ms / 1000.0
            else:
                lu_dt = _parse_utc(lu)
                frontier = lu_dt.timestamp() if lu_dt else None
            if frontier is not None and frontier >= date_end_epoch_s:
                covered = True
    return covered, newest


def coverage_entries(snap_base: Path, assets: list, date_str: str,
                    kaggle_root: Path, now_utc,
                    deadline_s: float = COVERAGE_SCAN_S) -> dict:
    """Per-lane LOCAL coverage census + classification (metadata-only).

    Rows come from parquet footers of each lane's day files (never a data
    read); expected_est is the 500ms full-day grid arithmetic for ONE asset
    lane; classify_coverage() pins the date class against the INJECTED wall
    clock (run_scorecard passes datetime.now(tz=UTC) — every emitted number
    stays wall-stamped to that instant); kaggle_verified_covers() supplies
    the verified-upload checkpoint that separates an honestly pruned mirror
    (stale-local) from a density FAIL that must stand. A footer census cut
    short by its deadline leaves rows=None and no classification for the
    lanes it did not finish — never a complete-looking count, and the legacy
    verdict semantics apply to those lanes untouched.
    """
    try:
        date_utc = _dt.date.fromisoformat(date_str)
    except ValueError:
        date_utc = None  # resolve_date only proved the partition dir exists
    verified, verified_upload = (False, None)
    if date_utc is not None:
        day_end = (
            _dt.datetime(date_utc.year, date_utc.month, date_utc.day,
                         tzinfo=_dt.timezone.utc) + _dt.timedelta(days=1)
        )
        verified, verified_upload = kaggle_verified_covers(kaggle_root, day_end.timestamp())
    manifests = staging_manifest_rows(kaggle_root)
    deadline = time.monotonic() + deadline_s
    entries: dict = {}
    for asset in assets:
        asset_dir = snap_base / f"asset={asset}"
        rows, nfiles, unread, cut = lane_footer_rows(asset_dir, deadline)
        entry = {
            "rows": None if cut else rows,
            "files": nfiles,
            "unread": unread,
            "partial": cut,
            "expected_est": FULL_DAY_ROWS_PER_ASSET,
            "verified": verified,
            "verified_upload": verified_upload,
        }
        staged = manifests.get(asset)
        if staged is not None:
            s_rows, s_built = staged
            built_dt = _parse_utc(s_built)
            if built_dt is not None:
                # "when fresher": the manifest was built after every local
                # file of this lane was last written (a lane with no local
                # files is trivially fresher) — record it as evidence.
                newest_local = 0.0
                try:
                    for p in day_files(asset_dir, None):
                        newest_local = max(newest_local, p.stat().st_mtime)
                except OSError:
                    newest_local = 0.0
                if built_dt.timestamp() >= newest_local:
                    entry["staged_rows"] = s_rows
                    entry["staged_built"] = s_built
        if not cut and date_utc is not None:
            klass, reason = classify_coverage(
                date_utc, rows, FULL_DAY_ROWS_PER_ASSET, now_utc
            )
            entry["klass"] = klass
            entry["reason"] = reason
            if klass == "partial":
                entry["modifier"] = "partial-ok"
            elif klass == "stale":
                entry["modifier"] = "stale-local" if verified else "stale-noverify"
            else:
                entry["modifier"] = "full"
        else:
            entry["modifier"] = "n/a(partial)" if cut else "n/a"
        entries[asset] = entry
    return entries


# ---------------------------------------------------------------------------
# Full-table resync-id set (projected id-only read; exact, once per run)
# ---------------------------------------------------------------------------


def load_resync_ids(
    resync_dir: Path,
    deadline: float | None = None,
    workers: int = FOOTER_WORKERS,
) -> tuple:
    """resync_id-only projected read of the WHOLE resync_episodes tree.

    Episodes persist in the partition of the date they STARTED, while
    snapshot rows can carry the rid for hours into adjacent-day data —
    checking the rid set against only the day's partition produced orphan
    false positives (2026-10-18: BTC/BNB/DOGE/ETH "orphans" that were fully
    present under neighbouring partitions). The set is therefore built from
    ALL date partitions (``**/*.parquet``, .tmp skipped, deduped) exactly
    once per run; anything still absent from this FULL set afterwards is a
    real orphan.

    Returns (id_set, overflow_flag, unread_count). Overflow keeps the set
    capped and flags the orphan metric partial; unread counts files not read
    at all (unreadable, or the deadline cut the load short — a short load
    must never look like a complete one).
    """
    ids: set = set()
    overflow = False
    unread = 0
    if not resync_dir.exists():
        return ids, overflow, unread
    parts = sorted(
        p for p in resync_dir.glob("**/*.parquet") if not p.name.endswith(".tmp")
    )
    total = len(parts)
    done = 0

    def _read(fp: Path):
        try:
            return pq.read_table(str(fp), columns=["resync_id"])
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        pending: deque = deque()
        it = iter(parts)
        for _ in range(workers * 2):
            nxt = next(it, None)
            if nxt is None:
                break
            pending.append(ex.submit(_read, nxt))
        while pending:
            fut = pending.popleft()
            if deadline is not None and time.monotonic() > deadline:
                break
            tbl = fut.result()
            nxt = next(it, None)
            if nxt is not None:
                pending.append(ex.submit(_read, nxt))
            if tbl is None:
                unread += 1
            else:
                done += 1
                for rid in tbl["resync_id"].to_pylist():
                    if rid is None or rid == "":
                        continue
                    if len(ids) >= RESYNC_ID_SET_CAP:
                        overflow = True
                        break
                    ids.add(str(rid))
                if overflow:
                    break
    if done + unread < total:  # deadline stopped the load mid-table
        unread += total - done - unread
    return ids, overflow, unread


# ---------------------------------------------------------------------------
# Bounded T1 core scan + G6 depth-recompute spot check
# ---------------------------------------------------------------------------


def _agg_core(tbl, block: T1Block, bloom: ApproxHashDupSet, resync_ids: set) -> None:
    """Aggregate one projected T1-file table into tiny counters (no row data kept)."""
    cols = {
        name: (tbl[name].to_pylist() if name in tbl.schema.names else None)
        for name in T1_CORE_COLS
    }
    ident_nulls = block.ident_nulls
    states = block.state_counts
    for i in range(tbl.num_rows):
        block.rows_scanned += 1
        for c in IDENTITY_COLS:
            v = cols.get(c)
            lv = v[i] if v is not None else None
            if lv is None or lv == "":
                ident_nulls[c] = ident_nulls.get(c, 0) + 1
        ts = cols["ts_snapshot_ns"][i]
        if ts is not None and ts % GRID_NS != 0:
            block.offgrid_rows += 1
        cid = cols["condition_id"][i]
        if cid is not None and cid != "" and ts is not None:
            if not bloom.add_if_new(f"{cid}\x00{ts}".encode("utf-8", "surrogatepass")):
                block.dup_key_count += 1
        st = cols["book_state"][i]
        key = str(st) if st is not None else "none"
        states[key] = states.get(key, 0) + 1
        rid = cols["resync_id"][i]
        if rid not in (None, "") and str(rid) not in resync_ids:
            block.orphan_resync_rows += 1
        oor = False
        has_any = False
        zero_seen = False
        for c in BEST_COLS:
            v = cols[c]
            lv = v[i] if v is not None else None
            if lv is None or (isinstance(lv, float) and math.isnan(lv)):
                continue  # null (None or NaN sentinel) — guarded, never imputed
            has_any = True
            f = float(lv)
            if f == 0.0:
                zero_seen = True
            if f < 0.0 or f > 1.0:
                oor = True
        if oor:
            block.out_of_range_rows += 1
        if zero_seen and has_any:
            block.zero_price_rows += 1


def _safe_read_core(path: Path):
    try:
        return pq.read_table(str(path), columns=T1_CORE_COLS)
    except Exception:
        return None


def _is_nan(v) -> bool:
    return isinstance(v, float) and math.isnan(v)


def _g6_check_file(
    fp: Path, deadline: float | None, max_rows: int = G6_SAMPLE_ROWS_PER_FILE
) -> tuple:
    """Recompute the section-3 depth aggregates from the level columns.

    depth_{1,5,10}c = cumulative best-first size within N cents of the own-side
    best (raw level-1 price), the walk breaks once outside the 10c window
    (levels sorted best-first), price/size None-skipped, and the stored depth
    columns compared at depth tolerance DEPTH_TOL. Streams in bounded batches;
    returns (rows_checked, mismatch_rows). None-vs-None matches; None-vs-value
    is a mismatch (mirrors book.py: depth is non-null exactly when the raw
    best is non-null).
    """
    pf = pq.ParquetFile(str(fp))
    checked = 0
    mismatches = 0
    rows_done = 0
    for batch in pf.iter_batches(batch_size=G6_BATCH_ROWS, columns=_g6_cols()):
        if rows_done >= max_rows:
            break
        if deadline is not None and time.monotonic() > deadline:
            break
        take = min(batch.num_rows, max_rows - rows_done)
        avail = set(batch.schema.names)
        cols = {name: (batch.column(name).to_pylist() if name in avail else None)
                for name in _g6_cols()}
        for i in range(take):
            for outc, side in G6_SIDES:
                base = f"{outc}_{side}"
                best_col = cols[f"{base}_level_1_price"]
                best = best_col[i] if best_col is not None else None
                if best is None or _is_nan(best):
                    expect = (None, None, None)
                else:
                    b = float(best)
                    t1 = t5 = t10 = 0.0
                    for lvl in range(1, 101):
                        pc = cols[f"{base}_level_{lvl}_price"]
                        pr = pc[i] if pc is not None else None
                        if pr is None or _is_nan(pr):
                            continue
                        sc = cols[f"{base}_level_{lvl}_size"]
                        sz = sc[i] if sc is not None else None
                        if sz is None or _is_nan(sz):
                            continue
                        d = abs(float(pr) - b)
                        if d - 1e-9 > 0.10:
                            break
                        s = float(sz)
                        if d - 1e-9 <= 0.01:
                            t1 += s
                        if d - 1e-9 <= 0.05:
                            t5 += s
                        t10 += s
                    expect = (t1, t5, t10)
                for j, thc in enumerate((1, 5, 10)):
                    dc = cols[f"{base}_depth_{thc}c"]
                    got = dc[i] if dc is not None else None
                    if _is_nan(got):
                        got = None
                    exp = expect[j]
                    ok = (got is None and exp is None) or (
                        got is not None and exp is not None
                        and abs(float(got) - float(exp)) <= DEPTH_TOL
                    )
                    if not ok:
                        mismatches += 1
                        break
            checked += 1
        rows_done += take
    return checked, mismatches


def scan_lane_t1(
    asset_dir: Path,
    resync_ids: set,
    deadline: float,
    since_mtime: float | None = None,
    workers: int = T1_WORKERS,
    g6_seed: str = "scorecard",
) -> T1Block:
    """Bounded day scan for one lane: core counters + sampled G6 depth check."""
    block = T1Block(asset=asset_dir.name.split("=", 1)[-1])
    files = day_files(asset_dir, since_mtime)
    block.files_total = len(files)
    bloom = ApproxHashDupSet()
    scanned: list = []

    def _drain_and_refill(pending: dict, ex, it) -> None:
        while len(pending) < workers * 2:
            nxt = next(it, None)
            if nxt is None:
                return
            pending[ex.submit(_safe_read_core, nxt)] = nxt

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        pending: dict = {}
        it = iter(files)
        _drain_and_refill(pending, ex, it)
        while pending:
            if time.monotonic() > deadline:
                block.partial = True
                break
            fut, fp = pending.popitem()
            tbl = fut.result()
            _drain_and_refill(pending, ex, it)
            if tbl is None:
                block.files_skipped += 1
                continue
            try:
                _agg_core(tbl, block, bloom, resync_ids)
            except Exception:
                block.files_skipped += 1  # replaced mid-read (compaction) etc.
                continue
            block.files_scanned += 1
            scanned.append(fp)
        for f in list(pending):  # cancel queued-but-unstarted reads
            f.cancel()
        ex.shutdown(wait=True)

    # --- G6 depth-recompute spot check on a deterministic file sample ---
    if not scanned:
        block.g6_done = block.files_total == 0  # empty lane: honest zero;
        return block  # budget-partial never claims a G6 pass it did not run
    if not block.partial and time.monotonic() <= deadline:
        rng = random.Random(f"{g6_seed}|{asset_dir}")
        sample = rng.sample(scanned, min(G6_SAMPLE_FILES, len(scanned)))
        sampled_files = 0
        for fp in sample:
            if time.monotonic() > deadline:
                block.partial = True
                break
            try:
                c, m = _g6_check_file(fp, deadline)
            except Exception:
                block.files_skipped += 1
                continue
            sampled_files += 1
            block.g6_rows_checked += c
            block.g6_mismatch_rows += m
        else:
            block.g6_done = True
        block.g6_files_sampled += sampled_files
    return block


# ---------------------------------------------------------------------------
# Daily completeness (reused) — spawned child so budget/memory stay bounded
# ---------------------------------------------------------------------------


def _completeness_child(data_root: str, date_str: str, out_json: str) -> None:
    try:
        from polymarket_collector.completeness import compute_daily_completeness

        rows = compute_daily_completeness(Path(data_root), date_str)
        Path(out_json).write_text(json.dumps([r.to_dict() for r in rows]))
    except Exception as exc:  # the parent degrades to n/a(partial) — never invented values
        try:
            Path(out_json).write_text(json.dumps({"error": repr(exc)}))
        except Exception:
            pass


def _completeness_out_path() -> str:
    p = Path("/tmp/opencode/_scorecard_completeness.json")
    if p.parent.is_dir():
        return str(p)
    return tempfile.mktemp(prefix="scorecard_comp_", suffix=".json")


# ---------------------------------------------------------------------------
# Lane-level window probe (reused from gate_probe, offline)
# ---------------------------------------------------------------------------


def probe_lane(asset_dir: Path, asset: str, max_files: int) -> tuple:
    """Reuse gate_probe.analyze_lane: (density_pct, compnorm_pct, files, partial_flag)."""
    try:
        lr = gate_probe.analyze_lane(asset_dir, asset, max_files)
    except Exception:
        return (None, None, 0, True)
    return (lr.density_pct, lr.comp_spread_norm_pct, lr.files_sampled, False)


# ---------------------------------------------------------------------------
# Settlement official share (stream-bounded)
# ---------------------------------------------------------------------------


def official_settlement_share(markets_file: Path) -> tuple:
    """Share of settled (status='resolved') markets with an official
    settlement_source (on_chain_confirmed / polymarket_official) vs inferred.
    Streams bounded batches; returns (counts dict, partial flag). No row dump.
    """
    settled = official = inferred = other = 0
    partial = False
    try:
        pf = pq.ParquetFile(str(markets_file))
        cols = ["status", "settlement_source"]
        for batch in pf.iter_batches(batch_size=10_000, columns=cols):
            statuses = batch.column("status").to_pylist()
            sources = batch.column("settlement_source").to_pylist()
            for st, src in zip(statuses, sources):
                if st != "resolved":
                    continue
                settled += 1
                src_s = "" if src is None else str(src)
                if src_s in OFFICIAL_SOURCES:
                    official += 1
                elif src_s in INFERRED_SOURCES:
                    inferred += 1
                else:
                    other += 1
    except Exception:
        partial = True
    return (
        {"settled": settled, "official": official, "inferred": inferred, "other": other},
        partial,
    )


# ---------------------------------------------------------------------------
# Uploads/cycle + last verified upload per lane (metadata only, no network)
# ---------------------------------------------------------------------------

CYCLE_FRESH_S = 2700  # exporter cycle-freshness window (45 min, export.py _is_fresh)


def lanes_upload_state(kaggle_root: Path) -> list:
    """Per lane: last verified upload (from _kaggle_state.json) and staging
    streams completed within the last cycle window (from _progress.json)."""
    now = time.time()
    try:
        progress = json.loads((kaggle_root / "_progress.json").read_text()) or {}
    except Exception:
        progress = {}
    out = []
    lane_dirs = sorted(p for p in kaggle_root.iterdir() if p.is_dir()) if kaggle_root.exists() else []
    for lane_dir in lane_dirs:
        lane = lane_dir.name
        last_upload = None
        state_datasets = 0
        try:
            state = json.loads((lane_dir / "_kaggle_state.json").read_text()) or {}
        except Exception:
            state = {}
        for ds, info in state.items():
            lu = info.get("last_upload_utc") if isinstance(info, dict) else None
            if lu:
                state_datasets += 1
                if last_upload is None or str(lu) > str(last_upload):
                    last_upload = lu
        streams = [v for k, v in progress.items()
                   if isinstance(k, str) and k.startswith(lane + "/")]
        cycle_done = sum(
            1 for v in streams
            if isinstance(v, dict) and v.get("ts", 0) >= now - CYCLE_FRESH_S
        )
        out.append(
            {
                "lane": lane,
                "last_upload": last_upload if last_upload else "n/a",
                "datasets": state_datasets,
                "cycle_streams": f"{cycle_done}/{len(streams)}",
                "state": "verified" if state_datasets else "no-state",
            }
        )
    return out


# ---------------------------------------------------------------------------
# Verdict — PASS / WATCH / FAIL over the amended §10 targets
# ---------------------------------------------------------------------------


def gate_verdict(probe: dict, blocks: dict, official: dict,
                 official_share: float | None, partial_any: bool,
                 coverage: dict | None = None) -> tuple:
    """Returns (status, fail_lines). WATCH (never PASS) when any input was
    partial — a scorecard that could not finish a metric must not print PASS.

    coverage (optional; None — the legacy call — keeps the semantics below
    byte-for-byte) carries each lane's classify_coverage() result. A
    'partial' (same-day) lane's NUMERIC density fail is coverage-driven and
    is dropped — the density number stays printed in the table; when nothing
    else fails, the verdict line prints SOAK: an in-progress day is neither
    accepted nor density-FAILED on coverage alone. A 'stale' lane whose date
    is covered by a verified kaggle upload checkpoint gets a
    'WARN stale-local (...)' line INSTEAD of its density fail — warn-only,
    never a gate FAIL and never a completeness claim for the local mirror;
    without the checkpoint the density FAIL stands (never a loss claim
    either). n/a(partial) density, CompNorm, T1 counters and official-share
    keep their legacy semantics exactly. WARN lines are appended to the
    returned list but never count towards the FAIL decision."""
    fails = []
    warns = []
    cov = coverage or {}
    for asset, (dens, norm, _f, _p) in probe.items():
        entry = cov.get(asset) or {}
        klass = entry.get("klass")
        if dens is None:
            fails.append(f"{asset} density=n/a(partial) — target>=99%")
        elif dens < 99.0:
            if klass == "partial":
                pass  # same-day partial day: density stays in the table; coverage alone must not FAIL
            elif klass == "stale" and entry.get("verified"):
                loc = ""
                if isinstance(entry.get("rows"), int) and entry.get("expected_est"):
                    loc = f" local_rows={entry['rows']:,}/{entry['expected_est']:,}"
                vu = entry.get("verified_upload")
                warns.append(
                    f"{asset}: WARN stale-local (data on Kaggle; see kaggle checkpoint) "
                    f"locally pruned, verified-uploaded — density={dens:g}%{loc}"
                    + (f" verified_upload={vu}" if vu else "")
                )
            else:
                fails.append(f"{asset} density={dens:g}% — target>=99%")
        if norm is None:
            fails.append(f"{asset} CompNorm=n/a(partial) — target<=0.1%")
        elif norm > gate_probe.COMP_NORM_GATE_PCT:
            fails.append(f"{asset} CompNorm={norm:g}% — target<=0.1%")
    for asset, block in blocks.items():
        if block.partial:
            fails.append(f"{asset} T1-scan=partial (budget) — cannot gate-assert")
            continue
        t1 = block.t1_total()
        if t1 > 0:
            fails.append(f"{asset} T1-sum={t1} — target=0 ({_t1_detail(block)})")
    if official_share is None:
        fails.append("official-share=n/a(partial) — target>=99%")
    elif official_share < 99.0:
        fails.append(
            f"official-share={official_share:.1f}% — target>=99% "
            f"(official={official['official']}/{official['settled']} resolved, "
            f"inferred={official['inferred']}, other={official['other']})"
        )
    classes = {e.get("klass") for e in cov.values()
               if isinstance(e, dict) and e.get("klass")}
    same_day = classes == {"partial"}  # every classified lane is the in-progress day
    if fails:
        status = "FAIL" if not partial_any else "WATCH"
    elif same_day:
        status = "SOAK"  # coverage alone: never ACCEPT, never FAIL
    elif partial_any or warns:
        status = "WATCH"
    else:
        status = "PASS"
    return status, fails + warns


def _t1_detail(block: T1Block) -> str:
    parts = [
        f"identnulls={sum(block.ident_nulls.values())}",
        f"dup={block.dup_key_count}",
        f"badpx={block.out_of_range_rows}",
        f"orph={block.orphan_resync_rows}",
        f"offgrid={block.offgrid_rows}",
        f"g6mm={block.g6_mismatch_rows}",
    ]
    nulls = {c: n for c, n in block.ident_nulls.items() if n}
    if nulls:
        parts.append("nullcols=" + ",".join(f"{c}:{n}" for c, n in nulls.items()))
    return " ".join(parts)


def _fmt(v) -> str:
    return "n/a(partial)" if v is None else str(v)


# ---------------------------------------------------------------------------
# Composable core + render
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Acceptance scorecard — one compact line per lane (amended spec §10)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--date", default=None, help="YYYY-MM-DD UTC (default: latest 500ms partition)")
    ap.add_argument("--assets", default=None, help="comma list, e.g. BTC,ETH (default: all in partition)")
    ap.add_argument("--hours", type=float, default=None,
                    help="restrict the bounded scan to files written within the last N hours "
                         "(mtime-scoped: honest write-time bound, not ts-scoped)")
    ap.add_argument("--budget-s", type=float, default=560.0,
                    help="wall-clock budget for the MEASURED phase (full-table "
                         "resync-id census + the per-lane scans). It is split "
                         "as FAIR PER-LANE SLICES: slice = remaining / "
                         "lanes-not-yet-run, so every lane completes at its "
                         "own pace and an early lane hands its unused time to "
                         "the lanes still pending; measured 2026-10-18 on this "
                         "2-core box a full lane needs ~74s under the live "
                         "collector's write noise, so 560s (7 lanes → ~77s "
                         "each) runs all 7 lanes to completion. Overrun prints "
                         "partial=yes / n/a(partial), never invented values. "
                         "The reused daily-completeness child runs concurrently "
                         "and joins afterwards with its own allowance (may "
                         "extend total wall past this budget; that is "
                         "reported, not hidden).")
    ap.add_argument("--probe-window", action="store_true",
                    help="widen the offline window-probe sample (gate_probe) to --probe-files; "
                         "the default probe already runs offline on the newest 48 mtimes")
    ap.add_argument("--probe-files", type=int, default=120,
                    help="window-probe sample size when --probe-window is given")
    ap.add_argument("--data-root", default=str(DATA_ROOT), help=argparse.SUPPRESS)
    return ap


def run_scorecard(
    data_root: Path,
    date_str: str,
    assets: list,
    budget_s: float = 240.0,
    hours: float | None = None,
    probe_files: int = 48,
) -> dict:
    """Composable core (importable, testable): computes every block and returns
    a plain dict with all metrics. main() only renders it."""
    budget = ScanBudget(budget_s=budget_s)
    since_mtime = time.time() - hours * 3600.0 if hours else None
    snap_base = data_root / "book_snapshots_500ms" / f"date={date_str}"
    kaggle_root = data_root / "kaggle_staging"
    t1_deadline = budget.started + budget_s

    # The reused daily completeness is I/O-heavy (whole narration partitions
    # re-read): start it immediately so its work overlaps the resync census,
    # then FREEZE it for the per-lane scans. It is a strictly read-only
    # probe, so a SIGSTOP/SIGCONT freeze is safe — and it otherwise hogs the
    # same disk the lanes need (the I/O storm that starved HYPE/SOL/XRP/ETH
    # into partial=yes at the old shared 240s budget). It resumes and joins
    # after the lanes, with its own allowance; if it still has not finished
    # the clean metrics print n/a(partial) honestly.
    comp_ctx = multiprocessing.get_context("spawn")
    out_json = _completeness_out_path()
    comp_proc = comp_ctx.Process(
        target=_completeness_child, args=(str(data_root), date_str, out_json), daemon=True
    )
    comp_proc.start()
    comp_pid = None
    if comp_proc.is_alive() and comp_proc.pid:
        comp_pid = comp_proc.pid

    resync_ids, resync_overflow, resync_unread = load_resync_ids(
        data_root / "resync_episodes",  # FULL table: every date partition, once per run
        deadline=min(t1_deadline, time.monotonic() + RESYNC_LOAD_FRAC * budget_s),
    )

    probe: dict = {}
    blocks: dict = {}
    # --- per-lane fair slices (A2 2026-10-18): the old design gave every lane
    # the same GLOBAL deadline, so a leading lane's probe+scan time (plus the
    # concurrent completeness child's I/O storm) starved the trailing lanes
    # into partial=yes. Each lane now gets its own slice of whatever wall
    # budget is left: slice = remaining / lanes-not-yet-run. A lane that
    # finishes early hands its unused time to the lanes still pending. The
    # completeness child is frozen (read-only probe → SIGSTOP is safe) so the
    # slices run at full disk speed, then it wakes and joins.
    comp_frozen = False
    if comp_pid:
        try:
            os.kill(comp_pid, signal.SIGSTOP)
            comp_frozen = True
        except OSError:
            comp_frozen = False
    try:
        pending_assets = list(assets)
        while pending_assets and not budget.expired():
            n = len(pending_assets)
            slice_s = min(max(LANE_SLICE_MIN_S, budget.remaining() / n), budget.remaining())
            asset = pending_assets.pop(0)
            lane_deadline = time.monotonic() + slice_s
            asset_dir = snap_base / f"asset={asset}"
            probe[asset] = probe_lane(asset_dir, asset, probe_files if probe_files else 48)
            blocks[asset] = scan_lane_t1(
                asset_dir, resync_ids, deadline=lane_deadline,
                since_mtime=since_mtime, g6_seed=date_str,
            )
    finally:
        if comp_frozen:
            try:
                os.kill(comp_pid, signal.SIGCONT)
            except OSError:
                pass
            comp_frozen = False
    for pending_asset in pending_assets:
        # budget gone mid-loop: mark the not-yet-scanned lanes partial, honestly
        pb = T1Block(asset=pending_asset, partial=True)
        pb.files_total = len(day_files(snap_base / f"asset={pending_asset}", since_mtime))
        blocks[pending_asset] = pb
        probe[pending_asset] = (None, None, 0, True)

    # --- prune-aware coverage census (backlog #2): footer-only, bounded, and
    # wall-stamped to one real now_utc (datetime.now(tz=UTC), never frozen) ---
    now_utc = _dt.datetime.now(tz=_dt.timezone.utc)
    coverage = coverage_entries(
        snap_base, assets, date_str, kaggle_root, now_utc,
        deadline_s=COVERAGE_SCAN_S,
    )

    # join the completeness child with its own allowance AFTER the lanes
    comp_join_deadline = t1_deadline + COMP_JOIN_ALLOWANCE_S
    comp_map, comp_status = _collect_completeness(comp_proc, out_json, comp_join_deadline)

    official, official_partial = official_settlement_share(
        data_root / "markets_latest" / "markets_latest.parquet"
    )
    official_share = (
        official["official"] / official["settled"] * 100.0
        if official["settled"] > 0 and not official_partial
        else None
    )

    lanes_state = lanes_upload_state(kaggle_root)

    partial_any = (
        comp_status != "ok"
        or official_partial
        or resync_overflow
        or resync_unread > 0
        or any(b.partial for b in blocks.values())
    )
    status, fails = gate_verdict(
        probe, blocks, official, official_share, partial_any, coverage=coverage
    )
    return {
        "date": date_str,
        "probe": probe,
        "blocks": blocks,
        "completeness": comp_map,
        "completeness_status": comp_status,
        "official": official,
        "official_share": official_share,
        "official_partial": official_partial,
        "lanes_state": lanes_state,
        "partial": partial_any,
        "status": status,
        "fails": fails,
        "resync_unread": resync_unread,
        "resync_overflow": resync_overflow,
        "coverage": coverage,
        "now_utc": now_utc.isoformat(),
    }


def _collect_completeness(comp_proc, out_json: str, join_deadline: float) -> tuple:
    if join_deadline > time.monotonic():
        comp_proc.join(join_deadline - time.monotonic())
    if comp_proc.is_alive():
        comp_proc.terminate()
        comp_proc.join()
        return {}, "partial"
    try:
        payload = json.loads(Path(out_json).read_text())
    except Exception:
        return {}, "partial"
    if isinstance(payload, dict) and "error" in payload:
        return {}, "error"
    result = {r["asset"]: r for r in payload} if isinstance(payload, list) else {}
    return result, ("ok" if result else "partial")


def render_scorecard(snap: dict, assets: list) -> None:
    print(
        f"=== Acceptance scorecard — date {snap['date']} UTC, {', '.join(assets)} | {TARGETS} ==="
    )
    print(
        "[scorecard] coverage semantics: same-day=partial-ok, "
        "pruned-days=stale-local(warn-only), full-day=unchanged"
    )
    comp = snap["completeness"]
    comp_ok = snap["completeness_status"] == "ok"
    header = (
        f"{'lane':<5} {'tick/day':>8} {'clean%':>11} {'dens%':>6} {'norm%':>6} "
        f"{'files':>7} {'dup':>5} {'badpx':>5} {'orph':>6} {'offgrid':>7} {'zero':>5} "
        f"{'g6mm/n':>9} {'g6f':>4} {'states':>12} {'part':>4} {'cov':>17} {'covcls':>14}"
    )
    print(header)
    print("-" * len(header))
    for asset in assets:
        b = snap["blocks"][asset]
        dens, norm, _pf, _p = snap["probe"][asset]
        c = comp.get(asset) if comp_ok else None
        tick = c["actual_snapshots"] if c else None
        if c and c.get("actual_clean_snapshots") is not None:
            clean = f"{c['completeness_ratio'] * 100:.2f}({str(c.get('clean_source') or '?')[:3]})"
        elif c:
            clean = "n/s(raw)"  # clean count not derivable metadata-only (raw source)
        else:
            clean = "n/a(partial)"
        dupnote = f"{b.dup_key_count}*" if b.dup_key_count else str(b.dup_key_count)
        states = ",".join(f"{k[:4]}={v}" for k, v in sorted(b.state_counts.items()))
        g6 = (
            f"{b.g6_mismatch_rows}/{b.g6_rows_checked}"
            if b.g6_done
            else "n/a(partial)"
        )
        part = "yes" if b.partial else "no"
        cov_e = (snap.get("coverage") or {}).get(asset) or {}
        if cov_e.get("rows") is not None:
            cov_s = f"{cov_e['rows']:,}/{cov_e['expected_est']:,}"
        else:
            cov_s = "n/a(partial)" if cov_e else "n/a"
        covcls = cov_e.get("modifier") or "n/a"
        print(
            f"{asset:<5} {_fmt(tick):>8} {clean:>11} {_fmt(dens):>6} {_fmt(norm):>6} "
            f"{b.files_scanned:>3}/{b.files_total:<3} {dupnote:>5} {b.out_of_range_rows:>5} "
            f"{b.orphan_resync_rows:>6} {b.offgrid_rows:>7} {b.zero_price_rows:>5} "
            f"{g6:>9} {b.g6_files_sampled:>4} {states:>12} {part:>4} "
            f"{cov_s:>17} {covcls:>14}"
        )
        extra = []
        if b.files_skipped:
            extra.append(f"skipped_files={b.files_skipped} (replaced mid-read/unreadable)")
        if b.resync_ids_overflow:
            extra.append("resync-id set capped — orphan metric partial")
        if not b.g6_done and not b.partial and b.files_total:
            extra.append("g6 not sampled (budget)")
        if cov_e.get("unread"):
            extra.append(f"cov_footer_unread={cov_e['unread']}")
        if cov_e.get("partial"):
            extra.append("coverage census cut (budget) — classification n/a(partial)")
        if cov_e.get("staged_rows") is not None:
            extra.append(
                f"staging-manifest staged_rows={cov_e['staged_rows']:,} built "
                f"{cov_e['staged_built']} (fresher than local files)"
            )
        if cov_e.get("klass") == "stale" and not cov_e.get("verified"):
            extra.append(
                f"low local coverage, no verified-upload checkpoint at/after day end: "
                f"{cov_e.get('reason')}"
            )
        if extra:
            print("      note: " + ", ".join(extra))
    print(
        "(dup* = approx-hash census, fixed 8MiB bound, fresh key can rarely "
        "hash-collide → count may be slightly high, 0 is exact; "
        f"g6mm/n = depth-recompute mismatches/rows sampled, tol {DEPTH_TOL:g}; "
        "clean% source tag: sta=staging raw=raw-footers-only; "
        "cov = local footer rows/expected full-day rows per asset-day, "
        "covcls: partial-ok|stale-local|stale-noverify|full)"
    )

    off = snap["official"]
    share = snap["official_share"]
    share_s = "n/a(partial)" if share is None else f"{share:.1f}%"
    print(
        f"official-share: {share_s} — official={off['official']}/{off['settled']} resolved, "
        f"inferred={off['inferred']}, other={off['other']} (target>=99%)"
    )
    if snap["lanes_state"]:
        print("uploads (metadata-only, kaggle staging state, no network):")
        for lane_info in snap["lanes_state"]:
            print(
                f"  {lane_info['lane']}: last_verified_upload={lane_info['last_upload']} "
                f"cycle_streams(done/total)={lane_info['cycle_streams']} "
                f"datasets={lane_info['datasets']} state={lane_info['state']}"
            )
    if snap["completeness_status"] != "ok":
        print(
            f"daily-completeness: {snap['completeness_status']} — clean metrics n/a(partial)"
        )
    if snap["fails"]:
        warn_lines = [f for f in snap["fails"] if "WARN stale-local" in f]
        fail_lines = [f for f in snap["fails"] if "WARN stale-local" not in f]
        if fail_lines:
            print("FAILING METRICS:")
            for line in fail_lines:
                print(f"  - {line}")
        if warn_lines:
            print("WARN stale-local (warn-only, not gate-failing):")
            for line in warn_lines:
                print(f"  - {line}")
    print(f"verdict: {snap['status']}")


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)
    data_root = Path(args.data_root)
    date_str = resolve_date(data_root, args.date)
    assets = resolve_assets(data_root, date_str, args.assets)
    probe_files = args.probe_files if args.probe_window else 48
    snap = run_scorecard(
        data_root=data_root,
        date_str=date_str,
        assets=assets,
        budget_s=args.budget_s,
        hours=args.hours,
        probe_files=probe_files,
    )
    render_scorecard(snap, assets)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
