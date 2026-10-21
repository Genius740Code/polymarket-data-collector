"""compact() memory bound (backlog #1a, 2026-10-21): never a full-table read.

Production evidence (measured on this box 2026-10-21):
- data/markets_log: 49,733 readable fragments / 192,241 rows / 40,433 unique
  condition_ids (mult 4.75 rows per cid) / 607MB zstd on disk.
- data/markets_latest: 39,285 rows x 33 cols (6.9MB) — the rebuild target.
- Old MarketsLog.compact() read EVERY fragment fully, accumulated all rows
  as Python dicts (all_rows), then deduped: ~500MB peak on the real hive.
  Colliding with the collector's ~1GB export build on this 3.9GB box, the
  kernel OOM-killed the LIVE collector 3x on 2026-10-20 — each death within
  10min of a resolution_backfill cron window (compact is its last step).

Fix under test (markets_log.py compact(), 2026-10-21): two-pass streaming
rebuild — pass 1 reads only (condition_id, updated_at) in bounded batches
and keeps a small cid->(updated_at, src, row) winner index; pass 2 re-reads
only the winning rows. Output must stay VALUE- and ORDER-identical to the
pre-fix dict-merge (same winners, same first-win order, same 2026-10-17
mixed-vintage ISO-cast fallback, same all-None -> null column typing).

Fixture note: the ~100k-row fixture below reproduces the measured
production shape (row count, 33-column set, per-cid lifecycle multiplicity,
hive layout, zstd) in pytest's tmp dir only — test-scoped data, never
under data/. Fragment COUNT is compressed 50x (50x2000 rows) to keep the
test wall-time sane; the old code's peak was driven by total row count as
Python dicts, not by file count.

MEASURED (this box, 2026-10-21, fresh child process, /proc VmHWM reset at the
compact() call; same 100,000-row fixture both runs, collector left online):
- OLD compact() (fix stashed, git stash @ 4e2a238): FAILED the bound —
  child rss_before=57.6MB, peak=371.5MB, growth=313.9MB vs the 150MB
  budget (assert fired: "full-table read regression"). Scales coherently
  with the ~500MB production spike at 192,241 real rows.
- NEW compact() (this branch): PASSED — run 1: growth=101.7MB (peak
  159.4MB); run 2 post stash-restore: growth=103.4MB; run 3 after the
  piece-level settlement canon + piece coalescing: growth=104.8MB (peak
  163.0MB). ~3x under the old peak; winners (20,000) + winner index only,
  never all rows.
- Production-scale rehearsal (scratch-dir copy of the real log: 49,994
  fragments / 192,241 rows): growth=255.3MB (peak 313.2MB) vs the old
  ~500MB+ at the same scale; 40,573 winners; per-cid parity vs the live
  markets_latest the old code built: 39,271 unchanged winners, 0 diffs on
  sampled columns, 0 backwards winners, schema type-identical.
"""

import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.markets_log import MarketsLog
from polymarket_collector.storage.parquet_io import read_table

REPO_ROOT = Path(__file__).resolve().parents[1]

# real production shape (measured 2026-10-21): 20,000 cids x 5 lifecycle
# rows = 100,000 rows; per-cid steps: active -> closed -> inferred-resolved
# -> stale re-discovery (older updated_at, loses) -> official-resolved (wins)
N_CIDS = 20_000
ROWS_PER_CID = 5
N_FRAGMENTS = 50
EXPECTED_WINNERS = N_CIDS
RSS_BUDGET_MB = 150.0

ASSETS = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]

# the 33-column set of the real markets rows, with the types the real
# fragments carry (measured on the newest production fragment 2026-10-21)
FIXTURE_SCHEMA = pa.schema([
    ("updated_at", pa.string()), ("recorded_at", pa.string()),
    ("first_seen_at", pa.string()), ("market_start_ts", pa.string()),
    ("market_end_ts", pa.string()), ("market_start_ts_ms", pa.int64()),
    ("market_end_ts_ms", pa.int64()), ("resolution_ts", pa.string()),
    ("condition_id", pa.string()), ("market_id", pa.string()),
    ("slug", pa.string()), ("series_id", pa.string()),
    ("window_index", pa.int64()), ("window_label", pa.string()),
    ("window_size_seconds", pa.int64()), ("asset", pa.string()),
    ("up_token_id", pa.string()), ("down_token_id", pa.string()),
    ("status", pa.string()), ("resolution_outcome", pa.string()),
    ("question", pa.string()), ("tick_size", pa.float64()),
    ("reported_volume", pa.float64()), ("reported_liquidity", pa.float64()),
    ("settlement_price", pa.float64()), ("settlement_ts_utc", pa.string()),
    ("resolution_confirmed_at", pa.string()), ("settlement_source", pa.string()),
    ("schema_version", pa.string()), ("minimum_order_size", pa.float64()),
    ("resolution_rule", pa.string()), ("resolution_source", pa.string()),
    ("minimum_notional", pa.float64()),
])


def _iso(base_min: int) -> str:
    import datetime as _dt
    t = _dt.datetime(2026, 10, 1, tzinfo=_dt.timezone.utc) + _dt.timedelta(minutes=base_min)
    return t.isoformat().replace("+00:00", "Z")


def _lifecycle_row(cid_i: int, step: int) -> dict:
    """Row `step` of the real per-market lifecycle for condition #cid_i.

    updated_at ordering is what compact() dedups on (string compare), so
    the steps carry strictly increasing ISO stamps — except step 3, a
    stale re-discovery poll with an OLDER stamp that must LOSE (the exact
    production shape of a rollover re-discovery after resolution).
    """
    asset = ASSETS[cid_i % len(ASSETS)]
    t = cid_i * 10 + step
    start_ms = 1_700_000_000_000 + t * 60_000
    row = {
        "condition_id": f"0x{cid_i:064x}",
        "market_id": f"0x{cid_i:064x}",
        "slug": f"{asset.lower()}-up-or-down-5m-{cid_i}",
        "series_id": f"{asset}-5MIN",
        "asset": asset,
        "window_index": cid_i,
        "window_label": "5m",
        "window_size_seconds": 300,
        "up_token_id": str(2 * cid_i),
        "down_token_id": str(2 * cid_i + 1),
        "market_start_ts": _iso(t - 5),
        "market_end_ts": _iso(t),
        "market_start_ts_ms": start_ms,
        "market_end_ts_ms": start_ms + 300_000,
        "schema_version": "3.0.0",
    }
    if step == 0:  # discovery
        row.update({"updated_at": _iso(t), "recorded_at": _iso(t), "first_seen_at": _iso(t),
                    "status": "active", "resolution_outcome": "unknown",
                    "settlement_source": None, "resolution_ts": None,
                    "settlement_price": None, "settlement_ts_utc": None,
                    "resolution_confirmed_at": None, "tick_size": 0.01,
                    "minimum_order_size": None, "reported_volume": float(cid_i % 97),
                    "reported_liquidity": float(cid_i % 53), "question": None,
                    "resolution_rule": None, "resolution_source": None,
                    "minimum_notional": None})
    elif step == 1:  # window ended
        row.update({"updated_at": _iso(t), "recorded_at": _iso(t), "first_seen_at": _iso(t - 10),
                    "status": "closed", "resolution_outcome": "unknown",
                    "settlement_source": None, "resolution_ts": None,
                    "settlement_price": None, "settlement_ts_utc": None,
                    "resolution_confirmed_at": None, "tick_size": 0.01,
                    "minimum_order_size": 0.1, "reported_volume": float(cid_i % 97),
                    "reported_liquidity": float(cid_i % 53), "question": None,
                    "resolution_rule": None, "resolution_source": None,
                    "minimum_notional": None})
    elif step == 2:  # in-run inferred resolution (Chainlink nearest)
        row.update({"updated_at": _iso(t), "recorded_at": _iso(t), "first_seen_at": _iso(t - 20),
                    "status": "resolved", "resolution_outcome": "up" if cid_i % 2 else "down",
                    "settlement_source": "inferred_nearest", "resolution_ts": _iso(t),
                    "settlement_price": 1.0 if cid_i % 2 else 0.0,
                    "settlement_ts_utc": _iso(t), "resolution_confirmed_at": None,
                    "tick_size": 0.01, "minimum_order_size": 0.1,
                    "reported_volume": float(cid_i % 97), "reported_liquidity": float(cid_i % 53),
                    "question": None, "resolution_rule": None,
                    "resolution_source": None, "minimum_notional": None})
    elif step == 3:  # STALE re-discovery (older stamp — must lose the dedup)
        row.update({"updated_at": _iso(t - 40), "recorded_at": _iso(t - 40), "first_seen_at": _iso(t - 40),
                    "status": "active", "resolution_outcome": "unknown",
                    "settlement_source": None, "resolution_ts": None,
                    "settlement_price": None, "settlement_ts_utc": None,
                    "resolution_confirmed_at": None, "tick_size": 0.01,
                    "minimum_order_size": None, "reported_volume": float(cid_i % 97),
                    "reported_liquidity": float(cid_i % 53), "question": None,
                    "resolution_rule": None, "resolution_source": None,
                    "minimum_notional": None})
    else:  # step 4: official CLOB resolution (backfill re-append — wins)
        row.update({"updated_at": _iso(t), "recorded_at": _iso(t), "first_seen_at": _iso(t - 30),
                    "status": "resolved", "resolution_outcome": "up" if cid_i % 2 else "down",
                    "settlement_source": "polymarket_official", "resolution_ts": _iso(t),
                    "settlement_price": 1.0 if cid_i % 2 else 0.0,
                    "settlement_ts_utc": _iso(t), "resolution_confirmed_at": _iso(t),
                    "tick_size": 0.01, "minimum_order_size": 0.1,
                    "reported_volume": float(cid_i % 97), "reported_liquidity": float(cid_i % 53),
                    "question": None, "resolution_rule": None,
                    "resolution_source": None, "minimum_notional": None})
    return row


def _build_100k_row_log(data_dir: Path) -> None:
    """~100k-row markets_log hive in tmp (50 fragments x 2,000 rows)."""
    rows_per_fragment = (N_CIDS * ROWS_PER_CID) // N_FRAGMENTS
    cids_per_fragment = rows_per_fragment // ROWS_PER_CID
    for f in range(N_FRAGMENTS):
        rows = []
        for cid_i in range(f * cids_per_fragment, (f + 1) * cids_per_fragment):
            for step in range(ROWS_PER_CID):
                rows.append(_lifecycle_row(cid_i, step))
        out = data_dir / "markets_log" / "date=2026-10-01" / f"part-{f:04d}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows, schema=FIXTURE_SCHEMA)
        pq.write_table(table, str(out), compression="zstd")
        del rows, table


# runs in a FRESH child process so the measured peak is compact()'s alone
# (VmHWM is reset at the call boundary; ru_maxrss cross-checked)
_CHILD_SRC = r"""
import json, resource, sys
def _vm(field):
    try:
        for line in open("/proc/self/status"):
            if line.startswith(field):
                return int(line.split()[1])
    except Exception:
        return None
    return None
data_dir, expect_rows = sys.argv[1], int(sys.argv[2])
from polymarket_collector.storage.markets_log import MarketsLog
rss_before = _vm("VmRSS:")
reset_ok = False
try:
    with open("/proc/self/clear_refs", "w") as f:
        f.write("5")  # reset VmHWM: peak now isolates the compact() call
    reset_ok = True
except Exception:
    reset_ok = False
out_path = MarketsLog(data_dir).compact()
peak_kb = _vm("VmHWM:") if reset_ok else resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
import pyarrow.parquet as pq
latest_rows = pq.ParquetFile(str(out_path)).metadata.num_rows
print(json.dumps({
    "rss_before_kb": rss_before, "peak_kb": peak_kb,
    "rss_after_kb": _vm("VmRSS:"),
    "ru_maxrss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    "reset_ok": reset_ok, "latest_rows": latest_rows, "expected_rows": expect_rows,
}))
"""


def test_compact_peak_rss_under_150mb_on_100k_row_log(tmp_path):
    """THE bound: compact() on a ~100k-row log must stay under 150MB peak
    RSS growth in a fresh process. Old code: FAILED (all rows as Python
    dicts, see module docstring). New code: streaming winner index + winner
    rows only."""
    data_dir = tmp_path / "data"
    _build_100k_row_log(data_dir)
    import gc
    gc.collect()  # free fixture-build transients before spawning the child
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD_SRC, str(data_dir), str(EXPECTED_WINNERS)],
        cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0, f"child rc={proc.returncode}\n{proc.stderr[-1500:]}"
    m = json.loads(proc.stdout.strip().splitlines()[-1])
    if sys.platform.startswith("linux"):
        assert m["reset_ok"], "VmHWM reset unavailable: peak would not isolate compact()"
    before_mb = m["rss_before_kb"] / 1024.0
    peak_mb = m["peak_kb"] / 1024.0
    growth_mb = peak_mb - before_mb
    print(f"[compact-rss] rows={m['latest_rows']} rss_before={before_mb:.1f}MB "
          f"peak={peak_mb:.1f}MB growth={growth_mb:.1f}MB "
          f"ru_maxrss={m['ru_maxrss_kb'] / 1024.0:.1f}MB budget={RSS_BUDGET_MB:.0f}MB")
    assert m["latest_rows"] == EXPECTED_WINNERS, m
    assert growth_mb <= RSS_BUDGET_MB, (
        f"compact() peak RSS growth {growth_mb:.1f}MB exceeds the {RSS_BUDGET_MB:.0f}MB "
        f"bound (child rss_before={before_mb:.1f}MB, peak={peak_mb:.1f}MB) — "
        "full-table read regression"
    )
    # spot-verify winners on the written view (projected read, bounded)
    latest = read_table(data_dir / "markets_latest" / "markets_latest.parquet",
                        columns=["condition_id", "status", "settlement_source"])
    rows = latest.to_pylist()
    assert len(rows) == EXPECTED_WINNERS
    assert all(r["status"] == "resolved" and r["settlement_source"] == "polymarket_official"
               for r in rows[:200]), rows[:3]
    stale_loses = [r for r in rows if r["status"] == "active"]
    assert not stale_loses, "stale re-discovery rows must lose the dedup"


# --------------------------------------------------------------- parity guards

def _pre_fix_compact_reference(log: MarketsLog, data_dir: Path) -> list:
    """The pre-2026-10-21 compact(), kept here as the parity oracle:
    accumulate staging + every fragment row as dicts, keep max updated_at
    per condition_id (first row wins exact ties, cid-less rows skipped),
    then the old terminal step (normalize + union with the 2026-10-17
    ISO-cast fallback)."""
    all_rows = [dict(r) for r in log._staging if "status" in r]
    for part in list((data_dir / "markets_log").rglob("*.parquet")):
        t = read_table(part)
        if t is not None:
            all_rows.extend(t.to_pylist())
    latest: dict = {}
    for r in all_rows:
        cid = r.get("condition_id")
        if not cid:
            continue
        cur = latest.get(cid)
        if cur is None or r.get("updated_at", "") > cur.get("updated_at", ""):
            latest[cid] = r
    return log._build_union_table(
        log._normalize_rows(list(latest.values()))).to_pylist()


def test_compact_matches_pre_fix_merge_semantics(tmp_path):
    """Order- and value-exact parity vs the pre-fix dict-merge on a mixed
    fixture: staging participants (first-win positions), in-file
    replacement, exact-timestamp ties (first row wins), a fragment with NO
    updated_at column (rows compete at ""), cross-file replacement, the
    mixed int64/string settlement_ts_utc fallback, a cid-less row, and an
    events row (no status) that must never enter markets_latest."""
    base = tmp_path
    log = MarketsLog(base)
    norm = log._normalize_rows
    logdir = base / "markets_log" / "date=2026-10-21"
    logdir.mkdir(parents=True)
    a = norm([
        {"condition_id": "0x1", "updated_at": "2026-10-21T00:01:00Z", "status": "active",
         "resolution_outcome": "unknown", "tick_size": 0.01, "question": None,
         "schema_version": "3.0.0"},
        {"condition_id": "0x1", "updated_at": "2026-10-21T00:02:00Z", "status": "closed",
         "resolution_outcome": "unknown", "tick_size": 0.02, "question": None,
         "schema_version": "3.0.0"},
        {"condition_id": "0x2", "updated_at": "2026-10-21T00:09:00Z", "status": "resolved",
         "resolution_outcome": "up", "tick_size": 0.10, "question": None,
         "schema_version": "3.0.0"},
        # exact-timestamp tie, LATER row: must LOSE to the row above it
        {"condition_id": "0x2", "updated_at": "2026-10-21T00:09:00Z", "status": "resolved",
         "resolution_outcome": "up", "tick_size": 0.99, "question": None,
         "schema_version": "3.0.0"},
        {"updated_at": "2026-10-21T00:03:00Z", "status": "active", "schema_version": "3.0.0"},
    ])
    pq.write_table(pa.Table.from_pylist(a), str(logdir / "a.parquet"), compression="zstd")
    b = norm([{"condition_id": "0x3", "status": "closed",
               "resolution_outcome": "unknown", "schema_version": "3.0.0"}])
    pq.write_table(pa.Table.from_pylist(b), str(logdir / "b.parquet"), compression="zstd")
    c = norm([
        {"condition_id": "0x3", "updated_at": "2026-10-21T00:03:00Z", "status": "resolved",
         "resolution_outcome": "down", "settlement_ts_utc": 1791590700000,
         "schema_version": "3.0.0"},
        {"condition_id": "0x1", "updated_at": "2026-10-21T00:01:30Z", "status": "active",
         "resolution_outcome": "unknown", "schema_version": "3.0.0"},
    ])
    pq.write_table(pa.Table.from_pylist(c), str(logdir / "c.parquet"), compression="zstd")
    d = norm([{"condition_id": "0x4", "updated_at": "2026-10-21T00:04:00Z", "status": "resolved",
               "resolution_outcome": "up", "settlement_ts_utc": "2026-10-10T00:05:00Z",
               "schema_version": "3.0.0"}])
    pq.write_table(pa.Table.from_pylist(d), str(logdir / "d.parquet"), compression="zstd")

    log = MarketsLog(base)
    # staging rows compete FIRST (first-win positions), like the old all_rows
    log.append({"condition_id": "0x5", "status": "resolved", "resolution_outcome": "up",
                "schema_version": "3.0.0"}, updated_at="2026-10-21T00:99:00Z")
    log.append({"condition_id": "0x2", "status": "active", "resolution_outcome": "unknown",
                "schema_version": "3.0.0"}, updated_at="2026-10-21T00:01:00Z")
    log.append_event("startup", "2026-10-21T00:00:00Z", 1)  # no status: excluded

    ref = _pre_fix_compact_reference(log, base)
    out_path = log.compact()
    actual = log.load_latest()

    assert len(actual) == len(ref) == 5
    assert [r["condition_id"] for r in actual] == [r["condition_id"] for r in ref], \
        "first-win row order must match the old dict insertion order"
    for a_, r_ in zip(actual, ref):
        for k in set(a_) | set(r_):
            assert a_.get(k) == r_.get(k), (a_["condition_id"], k, a_.get(k), r_.get(k))
    by = {r["condition_id"]: r for r in actual}
    assert by["0x1"]["tick_size"] == 0.02          # in-file replacement
    assert by["0x2"]["tick_size"] == 0.10          # exact tie keeps the first row
    assert by["0x3"]["resolution_outcome"] == "down"  # beats the "" updated_at row
    assert by["0x3"]["settlement_ts_utc"] == "2026-10-10T00:05:00Z"  # ISO-cast fallback
    assert by["0x4"]["settlement_ts_utc"] == "2026-10-10T00:05:00Z"  # string verbatim
    assert len(log._staging) == 3  # compact never consumes staging
    # all-None columns keep the null typing from_pylist gave the old output
    schema = pq.read_schema(str(out_path))
    assert schema.field("question").type == pa.null()
    # events row must never leak into the view
    assert all("event_type" not in r for r in actual)


def test_compact_skips_unreadable_fragment(tmp_path, capsys):
    """A corrupt fragment is skipped whole with a loud WARN; every readable
    cid still compacts (fail-open for the view, the log is never touched)."""
    base = tmp_path
    log = MarketsLog(base)
    log.append({"condition_id": "0xgood1", "status": "active", "resolution_outcome": "unknown",
                "schema_version": "3.0.0"}, updated_at="2026-10-21T00:01:00Z")
    log.flush_staging()
    bad = base / "markets_log" / "date=2026-10-21" / "corrupt.parquet"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_bytes(b"this is not a parquet file")
    log.append({"condition_id": "0xgood2", "status": "resolved", "resolution_outcome": "up",
                "schema_version": "3.0.0"}, updated_at="2026-10-21T00:02:00Z")
    log.flush_staging()

    log.compact()
    latest = {r["condition_id"]: r for r in log.load_latest()}
    assert set(latest) == {"0xgood1", "0xgood2"}
    assert latest["0xgood2"]["resolution_outcome"] == "up"
    assert "WARN compact pass1 unreadable fragment" in capsys.readouterr().out


def test_compact_empty_log_writes_empty_view(tmp_path):
    """No fragments, no staging: the rebuilt view exists and is empty
    (unchanged empty-path contract)."""
    log = MarketsLog(tmp_path)
    out = log.compact()
    assert out.exists() and log.load_latest() == []
