"""2026-10-17 ENOSPC outage narrator: un-narrated collection down-time.

The 01:00-08:30 UTC disk-full crash-loop outages left ~7.5h of REAL missing
coverage with no parent evidence row: resync_episodes held only 14
market_added stragglers and collector_events held no coverage_gap row for
asset lanes over that window — gaps without narration violate AGENT.md §1
("honest gaps").

Covers:
- measures the newest on-disk snapshot ts per lane (a real hive read)
- emits ONE coverage_gap row per lane when down-time >= 120s, with
  gap_ms/gap_buckets/last_snapshot_ns in the details
- event_id is DETERMINISTIC (sha256 of asset+gap_start) and identical
  across two narrator calls over the same data — restart storms dedup
- no row when down-time < threshold or a lane has no on-disk snapshots
- gap bounds are measured, never interpolated; no snapshot rows are
  created (assert: snapshot ttl unchanged).
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.collector import Collector as PolymarketCollector


def _object_collector(tmp: Path):
    c = PolymarketCollector.__new__(PolymarketCollector)

    class _Writer:
        data_dir = tmp

    class _StorageCfg:
        data_dir = tmp

    class _Cfg:
        assets = ["BTC", "SOL"]
        storage = _StorageCfg()

    c.config = _Cfg()
    c._conn_ids = {}
    c.writer = _Writer()

    class _MarketsLog:
        def __init__(self):
            self.rows = []

        def append_event(self, event_type, ts_utc, ts_received_ns, condition_id=None,
                         market_id=None, asset=None, connection_id=None, details=None,
                         event_id=None):
            self.rows.append({
                "event_id": event_id or "uuid-fallback",
                "event_type": event_type,
                "asset": asset,
                "details": details,
            })
    _ml = _MarketsLog()
    c.markets_log = _ml
    return c, _ml


def _write_snapshots(tmp: Path, asset: str, last_ts_ns: int) -> None:
    d = tmp / "book_snapshots_500ms" / f"date={datetime.now(timezone.utc).strftime('%Y-%m-%d')}" / f"asset={asset}"
    d.mkdir(parents=True, exist_ok=True)
    ts = pa.array([last_ts_ns - 1_000_000_000, last_ts_ns], type=pa.int64())
    pq.write_table(pa.table({"ts_snapshot_ns": ts}), str(d / "old.parquet"))


def test_narrator_emits_measured_gap_rows(tmp_path):
    c, ml = _object_collector(tmp_path)
    old_ts = int(((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()) * 1e9)
    _write_snapshots(tmp_path, "BTC", old_ts)
    _write_snapshots(tmp_path, "SOL", old_ts)
    before = tmp_path.rglob("*.parquet")
    before_n = sum(1 for _ in before)

    emitted = PolymarketCollector._narrate_startup_coverage_gaps(c)
    assert emitted == 2, {"emitted": emitted, "rows": ml.rows}
    rows = [r for r in ml.rows if r["event_type"] == "coverage_gap"]
    assert len(rows) == 2
    by_asset = {r["asset"]: r for r in rows}
    for a in ("BTC", "SOL"):
        d = by_asset[a]["details"]
        assert d["last_snapshot_ns"] == old_ts
        assert d["gap_ms"] >= 500_000  # ~10 minutes measured (ms)
        assert d["gap_buckets"] >= 1000  # ~10 min / 0.5s
        assert d["last_file"] == "old.parquet"
        eid = by_asset[a]["event_id"]
        assert len(eid) == 64 and eid.isalnum(), eid
    # snapshots untouched — only evidence added
    after_n = sum(1 for _ in tmp_path.rglob("*.parquet"))
    assert before_n == after_n

    # Idempotency: same data -> same deterministic event_id set
    PolymarketCollector._narrate_startup_coverage_gaps(c)
    id_set2 = {r["event_id"] for r in ml.rows if r["event_type"] == "coverage_gap"}
    assert len(id_set2) <= 2, id_set2


def test_narrator_silent_below_threshold(tmp_path):
    c, ml = _object_collector(tmp_path)
    fresh_ts = int(((datetime.now(timezone.utc) - timedelta(seconds=10)).timestamp()) * 1e9)
    _write_snapshots(tmp_path, "BTC", fresh_ts)
    assert PolymarketCollector._narrate_startup_coverage_gaps(c) == 0
    assert ml.rows == []


def test_narrator_silent_when_lane_empty(tmp_path):
    c, ml = _object_collector(tmp_path)
    assert PolymarketCollector._narrate_startup_coverage_gaps(c) == 0
    assert ml.rows == []


def test_narrator_survives_str_data_dir(tmp_path):
    """2026-10-17 live bug: config stores data_dir as str, narrator did
    `base / sub` on the str → TypeError "/: 'str' and 'str'" and printed
    'coverage_gap narrate err ...' — narrator dead in prod while tests
    passed (harness passed a Path). Coercion must make the str shape work."""
    tmp = str(tmp_path)  # string form — the production shape
    old_ts = int(((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp()) * 1e9)
    _write_snapshots(Path(tmp), "BTC", old_ts)
    emitted = _narrate_with_str_dir(tmp)
    assert emitted == 1


def _narrate_with_str_dir(tmp: str):
    """Build an object collector with data_dir as a plain str, narrate."""
    c = PolymarketCollector.__new__(PolymarketCollector)

    class _Writer:
        data_dir = tmp

    class _StorageCfg:
        data_dir = tmp

    class _Cfg:
        assets = ["BTC"]
        storage = _StorageCfg()

    c.config = _Cfg()
    c._conn_ids = {}
    c.writer = _Writer()
    rows = []

    class _MarketsLog:
        def append_event(self, event_type, ts_utc, ts_received_ns, condition_id=None,
                         market_id=None, asset=None, connection_id=None, details=None,
                         event_id=None):
            rows.append({"event_id": event_id, "details": details})
    c.markets_log = _MarketsLog()
    n = PolymarketCollector._narrate_startup_coverage_gaps(c)
    assert n == len(rows), (n, len(rows))
    return n


def test_deterministic_event_id_survives_storm(tmp_path):
    """Two 'restarts' over identical on-disk data produce the same event_id."""
    c1, ml1 = _object_collector(tmp_path)
    c2, ml2 = _object_collector(tmp_path)
    old_ts = int(((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp()) * 1e9)
    _write_snapshots(tmp_path, "BTC", old_ts)
    PolymarketCollector._narrate_startup_coverage_gaps(c1)
    PolymarketCollector._narrate_startup_coverage_gaps(c2)
    id1 = {r["event_id"]: r["asset"] for r in ml1.rows}
    id2 = {r["event_id"]: r["asset"] for r in ml2.rows}
    assert id1 == id2 and len(id2) == 1, {"r1": id1, "r2": id2}
