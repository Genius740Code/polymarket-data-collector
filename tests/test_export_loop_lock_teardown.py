"""22:30 flush-gap + 22:22 post-publish RSS spike (prod Oct-13 evidence).

Mechanism 1 — the kaggle upload loop held `_kaggle_lock` across the
pre-staging flush AND the entire export worker (staging build + upload +
prune). The flush loop's 5s lock wait then skipped every parquet flush
for the whole export, the writer buffer ballooned (32,478 rows measured at
the next cycle's flush) and every snapshot-loop append hit the inline
threshold/backpressure flush() — a synchronous parquet+WAL+fsync write ON
the 500ms scheduler thread, contending with the export worker's staging
I/O -> coverage_gap 198.5s (278 ticks/lane) every cycle.

Mechanism 2 — the post-publish prune read each candidate hive file's
decision column WHOLE (`read_table(columns=["condition_id"])` +
`to_pylist()`): a compacted book_events file holds 24M+ rows, so the scan
materialized GBs of Arrow+Python transient inside the collector's export
thread right after publish (RSS 1.1GB -> 2.34GB at 22:22 -> pm2
max-memory-restart, 442 ticks/lane lost).

These tests pin the fixes:
- lock scope: the flush completes BEFORE the worker starts (staging
  inclusion), and the kaggle lock is NOT held while the worker runs — a
  flush-loop-style flush succeeds throughout a fake-slow staging build
  (the measured starvation chain is gone).
- teardown bound: on real staging/hive dirs the prune's Python-heap peak
  stays small while scanning a many-row file, and the keep/delete
  decisions are bit-identical to the documented retention math (fail
  closed on unknown cid, keep inside leeway, prune all-ended-old).
"""

import asyncio
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq

import polymarket_collector.collector as _C
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.storage.export import (
    _scan_file_cids_bounded,
    _scan_file_max_ts_ns_bounded,
    cleanup_local_data,
)


def make_cfg(tmpdir: str) -> CollectorConfig:
    cfg = CollectorConfig()
    cfg.storage.data_dir = tmpdir
    cfg.storage.wal_dir = tmpdir + "/_wal"
    cfg.raw_archive.path = tmpdir + "/raw_ws_archive"
    cfg.cursor_store.path = tmpdir + "/cursor_state"
    return cfg


# --- mechanism 1: kaggle lock scope -----------------------------------------

class _ProbeWriter:
    """Stand-in for ParquetWriter in the lock-scope test.

    The real writer's flush fsyncs parquet partitions (~700ms measured on
    this busy box) — that cost is the flush-loop's own concern (pinned by
    parquet_writer tests), not the lock-scope property under test here.
    The stub keeps flush()/append() observable and instant so the probe
    window measures LOCK behavior, not disk latency. flush() records into
    the shared order list exactly like the traced real flush would.
    """

    def __init__(self, order: list):
        self.order = order
        self.flushes = 0
        self.appends = 0

    def flush(self):
        self.order.append("tick_flush")
        self.flushes += 1
        return 0

    def append(self, dataset, row, asset=None, date_str=None):
        self.appends += 1
        return True


async def test_lane_tick_releases_kaggle_lock_during_slow_staging(tmp_path, monkeypatch):
    """Fake-slow staging build: the worker must run with the kaggle lock
    FREE (old code held it -> flush loop timed out -> writer buffer
    ballooned -> snapshot scheduler stalled), and the pre-staging flush
    must complete BEFORE the worker starts (staging inclusion)."""
    col = Collector(make_cfg(str(tmp_path / "col")))
    col._kaggle_uploads = []
    order: list[str] = []
    col.writer = _ProbeWriter(order)
    seen = {"lock_held_at_worker_start": None}

    def fake_export(*, data_dir, assets, timeframe_labels, l2_levels,
                    dataset_prefix, dry_run, **kw):
        order.append("worker_start")
        # the worker runs in a thread — read the loop-side lock flag
        seen["lock_held_at_worker_start"] = col._kaggle_lock.locked()
        time.sleep(0.4)  # fake-slow staging build
        order.append("worker_end")
        return {"export": {"x": 1}, "staging": {}, "kaggle_uploads": {},
                "cleanup": {}, "dry_run": dry_run}

    monkeypatch.setattr(_C, "export_and_upload_all_kaggle", fake_export)

    async def flush_loop_probe(stop: "asyncio.Future"):
        """Flush-loop stand-in: the same wait_for(acquire, ~5s) + flush the
        _flush_loop does every tick. Under the old lock scope this timed out
        for the whole export; now it must succeed repeatedly."""
        wins = 0
        while not stop.done():
            try:
                locked = await asyncio.wait_for(col._kaggle_lock.acquire(), timeout=0.05)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.02)
                continue
            try:
                if locked:
                    col.writer.flush()
                    wins += 1
                    order.append("flush_loop_flush")
            finally:
                col._kaggle_lock.release()
            await asyncio.sleep(0.02)
        return wins

    async def snapshot_tick_probe(stop: "asyncio.Future"):
        """Snapshot-loop stand-in: append every ~50ms grid tick. Never blocks
        on the kaggle lock (append is lock-free); the buffer stays drainable
        because the flush-loop probe above can always run."""
        ticks = 0
        while not stop.done():
            col.writer.append(
                "book_snapshots_500ms",
                {"ts_snapshot_ns": int(time.time_ns() // 500_000 * 500_000),
                 "condition_id": "0xprobe", "asset": "BTC"},
                asset="BTC")
            ticks += 1
            await asyncio.sleep(0.05)
        return ticks

    stop = asyncio.get_running_loop().create_future()
    probe = asyncio.create_task(flush_loop_probe(stop))
    ticks = asyncio.create_task(snapshot_tick_probe(stop))
    try:
        res = await col._kaggle_lane_tick("5m", dry_run=True, interval=600)
    finally:
        stop.set_result(True)
        probe_wins = await probe
        probe_ticks = await ticks

    # (1) staging inclusion: the tick's pre-staging flush completed BEFORE
    # the worker started (old ordering guarantee, unchanged)
    assert order.index("tick_flush") < order.index("worker_start"), order
    assert order.index("worker_start") < order.index("worker_end")
    # (2) lock scope: the worker NEVER saw the kaggle lock held (old code:
    # held for the whole export -> this is the FAIL-on-old assertion)
    assert seen["lock_held_at_worker_start"] is False, \
        "kaggle lock must be free while the staging worker runs"
    # (3) the flush loop kept flushing DURING the fake-slow staging build
    assert probe_wins >= 3, \
        f"flush loop must tick during staging (got {probe_wins} flushes)"
    # (4) the snapshot tick stand-in kept appending through the build
    assert probe_ticks >= 3, \
        f"snapshot appends must proceed during staging (got {probe_ticks})"
    # (5) result recorded + bounded history
    assert col._kaggle_uploads and col._kaggle_uploads[-1]["tf"] == "5m"
    assert res["export"] == {"x": 1}


# --- mechanism 2: bounded post-publish teardown ------------------------------

def _fake_cfg_cls(monkeypatch, timeframes=("5m",)):
    import polymarket_collector.config as _CFG
    ds = {tf: f"gghgg1/polymarket-{tf}-crypto" for tf in timeframes}
    fake = type("C", (), {
        "kaggle": type("K", (), {"rolling_window": True,
                                 "local_retention_hours": 48,
                                 "datasets": ds})(),
        "timeframes": list(timeframes),
    })()
    monkeypatch.setattr(_CFG.CollectorConfig, "load",
                        classmethod(lambda cls, *a, **k: fake))
    return fake


def _write_lane_state(base, lane, upload_ms):
    import json as _js
    sp = base / "kaggle_staging" / lane / "_kaggle_state.json"
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(_js.dumps({f"gghgg1/polymarket-{lane}-crypto":
                             {"last_upload_unix_ms": upload_ms}}))


def _write_staging(base, lane, files_mtimes_ms):
    for name, mtime_ms in files_mtimes_ms.items():
        p = base / "kaggle_staging" / lane / "gghgg1" / "ds" / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"")
        os.utime(p, (mtime_ms / 1000,) * 2)


def _mk_prune_layout(tmp_path, n_rows=800_000, old_age_d=30):
    """Real staging/hive dirs: one huge book_events file (single ended cid),
    one huge chainlink file, one mid-age snapshots file inside the leeway."""
    base = tmp_path / "data"
    now_ms = int(time.time() * 1000)
    old_end = now_ms - old_age_d * 24 * 3600 * 1000
    mid_end = now_ms - 11 * 24 * 3600 * 1000
    ml = base / "markets_latest"
    ml.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "condition_id": ["cid-old", "cid-mid"],
        "market_end_ts_ms": [old_end, mid_end],
    }), str(ml / "markets_latest.parquet"))

    # whale file: many rows, ONE ended market — old code materialized the
    # whole condition_id column for exactly this shape (24M rows in prod).
    d = base / "book_events" / "date=x" / "asset=BTC"
    d.mkdir(parents=True, exist_ok=True)
    big = d / "big.parquet"
    pq.write_table(pa.table({
        "condition_id": pa.array(["cid-old"] * n_rows, type=pa.string()),
        "seq": pa.array(list(range(n_rows)), type=pa.int64()),
    }), str(big))

    # chainlink whale for the TS branch (ts_received_ns, all older than cutoff)
    c = base / "chainlink_events" / "date=x" / "asset=BTC"
    c.mkdir(parents=True, exist_ok=True)
    clk = c / "clk.parquet"
    pq.write_table(pa.table({
        "ts_received_ns": pa.array([old_end * 1_000_000] * n_rows,
                                   type=pa.int64()),
    }), str(clk))

    # a mid-age snapshots file that must SURVIVE (leeway) on real content
    s = base / "book_snapshots_500ms" / "date=x" / "asset=BTC"
    s.mkdir(parents=True, exist_ok=True)
    f_mid = s / "mid.parquet"
    pq.write_table(pa.table({"condition_id": ["cid-mid"]}), str(f_mid))

    for _f in (big, clk, f_mid):
        os.utime(_f, (now_ms / 1000 - 10 * 86400,) * 2)
    _write_staging(base, "5m", {
        "BTC_book_events.parquet": now_ms,
        "BTC_chainlink_events.parquet": now_ms,
        "BTC_book_snapshots_500ms.parquet": now_ms,
    })
    _write_lane_state(base, "5m", now_ms)
    return base, big, clk, f_mid, n_rows


def test_teardown_peak_bounded_and_decisions_identical(tmp_path, monkeypatch):
    """The prune scans an 800k-row whale with a bounded Python-heap peak
    (old code: full-column to_pylist -> ~90MB of str objects) and still
    makes the exact documented decisions: prune the ended-old whale +
    aged chainlink, keep the inside-leeway file (cid-mid ended 11d ago,
    288h leeway)."""
    import tracemalloc

    _fake_cfg_cls(monkeypatch)
    base, big, clk, f_mid, n_rows = _mk_prune_layout(tmp_path)

    tracemalloc.start()
    try:
        stats = cleanup_local_data(str(base), rolling_window=True,
                                   retention_hours=288,
                                   reap_quarantine=False)
    finally:
        _cur, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    # decisions: retention math untouched
    assert not big.exists(), f"ended-old whale must prune: {stats}"
    assert not clk.exists(), f"aged chainlink must prune: {stats}"
    # cid-mid ended 11d ago — inside the 288h (12d) leeway -> kept, exactly
    # like the old scanner's `max(ends) >= cutoff_ms` guard computes it
    assert f_mid.exists(), "inside-leeway file must survive"
    assert stats[str(big.relative_to(base))] == n_rows
    assert stats[str(clk.relative_to(base))] == n_rows

    # bound: the whole-column materialization is gone. 800k str objects
    # measure ~90MB+ Python heap on the old path; the batched scan stays
    # in Arrow and only ever holds the DISTINCT cid set (1 entry here).
    assert peak < 20 * 1024 * 1024, \
        f"post-publish prune peak must stay bounded: {peak / 1e6:.1f}MB"


def test_scanner_cids_decisions_fail_closed(tmp_path):
    """Distinct-cid set semantics across batches + conservative keeps."""
    p = tmp_path / "mixed.parquet"
    # two cids interleaved across >1 batch so dedup must span batches
    n = 300_000
    cids = ["cid-old", "cid-old2"] * (n // 2)
    pq.write_table(pa.table({"condition_id": cids}), str(p))
    n_rows, got = _scan_file_cids_bounded(p)
    assert n_rows == n
    assert got == {"cid-old", "cid-old2"}, got

    # falsy values are dropped like the old `if c` filter
    p2 = tmp_path / "falsy.parquet"
    pq.write_table(pa.table({"condition_id": ["", None, "cid-old"]}), str(p2))
    n2, got2 = _scan_file_cids_bounded(p2)
    assert n2 == 3 and got2 == {"cid-old"}, (n2, got2)

    # no condition_id column -> conservative keep signal
    p3 = tmp_path / "nocol.parquet"
    pq.write_table(pa.table({"a": [1]}), str(p3))
    assert _scan_file_cids_bounded(p3) == (None, None)

    # empty file -> keep; garbage file -> keep (never raises)
    p4 = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"condition_id": pa.array([], type=pa.string())}),
                   str(p4))
    assert _scan_file_cids_bounded(p4) == (None, None)
    p5 = tmp_path / "garbage.parquet"
    p5.write_bytes(b"not parquet")
    assert _scan_file_cids_bounded(p5) == (None, None)


def test_scanner_max_ts_matches_old_semantics(tmp_path):
    """First present/valued column wins; ISO strings parse to ms->ns;
    all-null column falls through to the next candidate; missing/unreadable
    -> keep (None)."""
    old_ms = int(time.time() * 1000) - 30 * 24 * 3600 * 1000
    # ts_received_ns present + valued -> wins even if ts_utc is newer
    p = tmp_path / "a.parquet"
    pq.write_table(pa.table({
        "ts_received_ns": pa.array([old_ms * 1_000_000] * 10, type=pa.int64()),
        "ts_utc": ["2026-12-31T00:00:00Z"] * 10,
    }), str(p))
    assert _scan_file_max_ts_ns_bounded(p) == old_ms * 1_000_000

    # ts_received_ns all-null -> falls through to ISO ts_utc
    p2 = tmp_path / "b.parquet"
    pq.write_table(pa.table({
        "ts_received_ns": pa.array([None] * 4, type=pa.int64()),
        "ts_utc": ["2026-10-01T00:00:00.500Z", None, "2026-10-01T00:00:00Z", None],
    }), str(p2))
    got = _scan_file_max_ts_ns_bounded(p2)
    import datetime as _dt
    want_ms = int(_dt.datetime.fromisoformat(
        "2026-10-01T00:00:00.500+00:00").timestamp() * 1000)
    assert got == want_ms * 1_000_000, got

    # neither column present -> keep
    p3 = tmp_path / "c.parquet"
    pq.write_table(pa.table({"a": [1]}), str(p3))
    assert _scan_file_max_ts_ns_bounded(p3) is None
    p4 = tmp_path / "d.parquet"
    p4.write_bytes(b"garbage")
    assert _scan_file_max_ts_ns_bounded(p4) is None
