"""Kaggle-upload recovery (2026-09-11): the collector ran 44h with zero uploads.

Root causes fixed here:
1. The kaggle loop uploaded ALL lanes sequentially per tick (~40min/lane x4),
   never fitting inside one process incarnation (OOM/max-memory restarts every
   ~25-90min) — uploads never started. Now: ONE lane per tick, persistent
   round-robin cursor across restarts.
2. Step-1b pre-upload validation re-read the ENTIRE hive a second time
   (full data reads, GBs transient) and OOM-killed the box at the finish
   line. Now: metadata-only worker coverage manifests.
3. Trades workers died (full-table to_pylist bombs + unbounded Data-API
   enrichment past the 900s worker timeout). Now: sliced fallbacks +
   enrichment deadline with honest NULLs.
4. Compaction wrote single giant row groups that pin 100s of MB in the
   streaming reader. Now: row_group_size=20000.
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.collector import _next_kaggle_lane
from polymarket_collector.storage.compaction import compact_dataset
from polymarket_collector.storage.export import (
    _backfill_trade_wallets,
    _manifests_match,
    _source_manifest,
)
from polymarket_collector.storage.streaming import stream_batches


def test_lane_cursor_round_robin(tmp_path):
    lanes = ["5m", "15m", "1h", "4h"]
    got = [_next_kaggle_lane(tmp_path, lanes) for _ in range(6)]
    assert got == ["5m", "15m", "1h", "4h", "5m", "15m"], got
    # cursor persists (restart-safety: a fresh process continues the rotation)
    assert _next_kaggle_lane(tmp_path, lanes) == "1h"


def test_lane_cursor_single_and_empty(tmp_path):
    assert _next_kaggle_lane(tmp_path, ["5m"]) == "5m"
    assert _next_kaggle_lane(tmp_path, []) is None


def _write(p: Path, rows: int):
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"a": list(range(rows))}), str(p))


def test_source_manifest_cutoff_and_match(tmp_path):
    base = tmp_path / "data"
    d = base / "trades" / "date=2026-09-11" / "asset=BTC"
    _write(d / "old.parquet", 3)
    _write(d / "new.parquet", 5)
    now = time.time()
    os.utime(d / "old.parquet", (now - 100, now - 100))
    os.utime(d / "new.parquet", (now, now))
    pre = _source_manifest(base, "trades", "BTC", now - 50)
    assert pre["n"] == 1, pre  # only the old file is this cycle's input
    again = _source_manifest(base, "trades", "BTC", now - 50)
    assert _manifests_match(pre, again)
    # a backdated newcomer changes the digest -> mismatch fails closed
    _write(d / "sneaky.parquet", 7)
    os.utime(d / "sneaky.parquet", (now - 90, now - 90))
    assert not _manifests_match(pre, _source_manifest(base, "trades", "BTC", now - 50))
    # same inputs listed later still match (stability, no false alarm)
    m1 = _source_manifest(base, "trades", "BTC", now - 50)
    m2 = _source_manifest(base, "trades", "BTC", now - 50)
    assert m1["n"] == 2 and _manifests_match(m1, m2)


def test_stream_batches_stats_counts_failures(tmp_path):
    d = tmp_path / "ds"
    d.mkdir()
    pq.write_table(pa.table({"a": [1, 2, 3]}), str(d / "ok.parquet"))
    (d / "bad.parquet").write_bytes(b"not a parquet file at all")
    stats: dict = {}
    rows = list(stream_batches(tmp_path, "ds", None, stats=stats))
    assert sum(t.num_rows for t in rows) == 3
    assert stats["files_ok"] == 1, stats
    assert stats["files_failed"] == 1, stats


def _trades_table(n=4):
    return pa.table({
        "transaction_hash": [f"0x{'ab%02d' % i:0<64}"[:66] for i in range(n)],
        "condition_id": ["0x" + "c" * 64] * n,
        "wallet": [None] * n,
        "maker_wallet": [None] * n,
        "taker_wallet": [None] * n,
        "side": ["BUY"] * n,
        "outcome": ["unknown"] * n,
        "price": [0.5] * n,
        "size": [10.0] * n,
        "trade_id": [f"t{i}" for i in range(n)],
        "ts_source": [1789000000000 + i for i in range(n)],
    })


def test_enrichment_deadline_ships_honest_nulls_no_network(tmp_path, monkeypatch):
    import httpx

    def _boom(*a, **k):
        raise AssertionError("no network allowed past the deadline")

    monkeypatch.setattr(httpx, "get", _boom)
    t = _trades_table()
    # deadline already expired: zero markets fetched, rows preserved, NULLs kept
    out = _backfill_trade_wallets(t, tmp_path, asset="BTC", reconcile=False, deadline_s=-1)
    assert out.num_rows == t.num_rows
    assert out.column("wallet").null_count == t.num_rows  # honest NULLs, never fabricated


def test_compaction_writes_small_row_groups(tmp_path):
    leaf = tmp_path / "book_snapshots_500ms" / "date=2026-09-11" / "asset=BTC"
    leaf.mkdir(parents=True)
    for i in range(3):
        pq.write_table(
            pa.table({"a": list(range(15000)), "b": [float(i)] * 15000}),
            str(leaf / f"book_snapshots_500ms_{1000 + i}.parquet"))
    rows = compact_dataset(leaf)
    assert rows == 45000
    parts = [p for p in leaf.iterdir() if p.suffix == ".parquet"]
    assert len(parts) == 1, [p.name for p in parts]
    md = pq.read_metadata(str(parts[0]))
    assert md.num_row_groups >= 2, "giant single row group pins RAM in stream_batches"
    for rg in range(md.num_row_groups):
        assert md.row_group(rg).num_rows <= 20000


def _prune_hive(tmp_path, now_ms):
    """Tiny hive: cid-old ended 30d ago, cid-mid ended 11d ago."""
    import json as _js

    base = tmp_path / "data"
    old_end = now_ms - 30 * 24 * 3600 * 1000
    mid_end = now_ms - 11 * 24 * 3600 * 1000
    ml = base / "markets_latest"
    ml.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "condition_id": ["cid-old", "cid-mid"],
        "market_end_ts_ms": [old_end, mid_end],
    }), str(ml / "markets_latest.parquet"))
    d = base / "book_snapshots_500ms" / "date=x" / "asset=BTC"
    d.mkdir(parents=True, exist_ok=True)
    f_old = d / "old.parquet"
    pq.write_table(pa.table({
        "condition_id": ["cid-old"], "asset": ["BTC"],
        "series_id": ["BTC-5m"], "ts_snapshot_ns": [old_end * 1_000_000],
    }), str(f_old))
    f_mid = d / "mid.parquet"
    pq.write_table(pa.table({
        "condition_id": ["cid-mid"], "asset": ["BTC"],
        "series_id": ["BTC-5m"], "ts_snapshot_ns": [mid_end * 1_000_000],
    }), str(f_mid))
    return base, f_old, f_mid


def _write_lane_state(base, lane, upload_ms):
    import json as _js

    sp = base / "kaggle_staging" / lane / "_kaggle_state.json"
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(_js.dumps({"gghgg1/polymarket-x": {"last_upload_unix_ms": upload_ms}}))


def test_wal_replay_survives_truncated_line(tmp_path):
    """2026-09-11 DATA-LOSS FIX: one truncated WAL line (SIGKILL mid-append)
    aborted the ENTIRE replay on every startup (same char-469 error each
    boot) — 73MB of WAL sat un-replayed forever. Good lines around the bad
    one must still replay, and the file must truncate afterwards."""
    import json as _js

    from polymarket_collector.storage.parquet_writer import ParquetWriter

    wal_dir = tmp_path / "_wal"
    wal_dir.mkdir()
    good1 = {"dataset": "collector_events", "asset": "BTC", "date_str": "2026-09-11",
             "row": {"event_id": "e1", "event_type": "x"}, "ts": 1.0}
    good2 = {"dataset": "collector_events", "asset": "BTC", "date_str": "2026-09-11",
             "row": {"event_id": "e2", "event_type": "x"}, "ts": 2.0}
    (wal_dir / "wal-aaa.jsonl").write_text(
        _js.dumps(good1) + "\n"
        + _js.dumps(good2)[:47] + "\n"  # truncated mid-append (SIGKILL)
        + _js.dumps(good1).replace("e1", "e3") + "\n")
    w = ParquetWriter(data_dir=str(tmp_path), wal_enabled=True,
                      wal_dir=str(wal_dir), buffer_max_rows=10000)
    n = w._wal_replay()
    assert n == 2, f"good lines must replay around the truncated one, got {n}"
    assert (wal_dir / "wal-aaa.jsonl").stat().st_size == 0


def test_prune_fail_closed_without_any_upload(tmp_path):
    """No verified upload on any lane -> prune must delete nothing."""
    import time as _t

    from polymarket_collector.storage.export import cleanup_local_data

    now_ms = int(_t.time() * 1000)
    base, f_old, f_mid = _prune_hive(tmp_path, now_ms)
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48)
    assert stats == {}, stats
    assert f_old.exists() and f_mid.exists()


def test_prune_gated_by_slowest_lane(tmp_path, monkeypatch):
    """2026-09-11 data-loss regression: a fresh 5m upload must NOT authorize
    deletion of rows a lagging lane (15m) has not uploaded yet. The checkpoint
    is the MINIMUM last-upload across enabled lanes sharing the hive."""
    import time as _t

    import polymarket_collector.config as _CFG
    import polymarket_collector.storage.export as _E

    now_ms = int(_t.time() * 1000)
    # hermetic lanes (independent of the prod yaml on disk). export.py does
    # `from ..config import CollectorConfig` locally per call, so patch the
    # source class.
    _fake = type("C", (), {
        "kaggle": type("K", (), {"rolling_window": True, "local_retention_hours": 48})(),
        "timeframes": ["5m", "15m"],
    })()
    monkeypatch.setattr(_CFG.CollectorConfig, "load", classmethod(lambda cls, *a, **k: _fake))

    import os as _os

    # files landed 13d ago; staging snapshots files are fresh (mtime now) in
    # every lane below, so the coverage proof lets them through and this
    # test isolates the upload-time gating.
    # 5m uploaded now -> mid file (11d old, past 48h) would be pruned alone
    base, f_old, f_mid = _prune_hive(tmp_path, now_ms)
    _os.utime(f_old, (now_ms / 1000 - 13 * 86400,) * 2)
    _os.utime(f_mid, (now_ms / 1000 - 13 * 86400,) * 2)
    _write_staging(base, "5m", {"BTC_book_snapshots_500ms.parquet": now_ms})
    _write_staging(base, "15m", {"BTC_book_snapshots_500ms.parquet": now_ms})
    _write_lane_state(base, "5m", now_ms)
    stats = _E.cleanup_local_data(str(base), rolling_window=True, retention_hours=48)
    assert not f_old.exists() and not f_mid.exists(), stats

    # ...but with 15m lagging 10d, the mid file (11d old > 12d cutoff) survives
    base, f_old, f_mid = _prune_hive(tmp_path, now_ms)
    _os.utime(f_old, (now_ms / 1000 - 13 * 86400,) * 2)
    _os.utime(f_mid, (now_ms / 1000 - 13 * 86400,) * 2)
    _write_staging(base, "5m", {"BTC_book_snapshots_500ms.parquet": now_ms})
    _write_staging(base, "15m", {"BTC_book_snapshots_500ms.parquet": now_ms})
    _write_lane_state(base, "5m", now_ms)
    _write_lane_state(base, "15m", now_ms - 10 * 24 * 3600 * 1000)
    stats = _E.cleanup_local_data(str(base), rolling_window=True, retention_hours=48)
    assert not f_old.exists(), "30d-old file is past every cutoff and must prune"
    assert f_mid.exists(), f"11d-old file must survive while 15m lags: {stats}"


def _write_staging(base, lane, files_mtimes_ms):
    """Create lane staging files with given mtimes (content irrelevant —
    the coverage proof stats them, never reads them)."""
    import os as _os

    for name, mtime_ms in files_mtimes_ms.items():
        p = base / "kaggle_staging" / lane / "gghgg1" / "ds" / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"")
        _os.utime(p, (mtime_ms / 1000,) * 2)


def test_prune_coverage_proof_is_per_dataset(tmp_path, monkeypatch):
    """2026-09-12 DATA-LOSS FIX: min-upload gating is necessary but not
    sufficient — a lane can upload STALE staging (gate-skipped datasets keep
    prior files) that never covered newer hive rows. Freshness is per
    (lane, dataset): here snapshots staging is fresh (prunes) while trades
    staging is a days-old prior (blocks), even though both hive files hold
    30d-old markets and the lane just uploaded."""
    import os as _os
    import time as _t

    import polymarket_collector.config as _CFG
    import polymarket_collector.storage.export as _E
    import pyarrow.parquet as pq

    now_ms = int(_t.time() * 1000)
    _fake = type("C", (), {
        "kaggle": type("K", (), {"rolling_window": True, "local_retention_hours": 48})(),
        "timeframes": ["5m"],
    })()
    monkeypatch.setattr(_CFG.CollectorConfig, "load", classmethod(lambda cls, *a, **k: _fake))

    base, f_old, f_mid = _prune_hive(tmp_path, now_ms)
    # extra trades hive file, same old content
    t_path = base / "trades" / "date=x" / "asset=BTC" / "t.parquet"
    t_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"condition_id": ["cid-old"]}), str(t_path))
    for _f in (f_old, f_mid, t_path):
        _os.utime(_f, (now_ms / 1000 - 10 * 86400,) * 2)
    # snapshots staging fresh NOW, trades staging a 10d-old prior
    _write_staging(base, "5m", {"BTC_book_snapshots_500ms.parquet": now_ms,
                                "BTC_trades.parquet": now_ms - 10 * 86400 * 1000})
    _write_lane_state(base, "5m", now_ms)
    stats = _E.cleanup_local_data(str(base), rolling_window=True, retention_hours=0)
    assert not f_old.exists(), f"covered snapshots file must prune: {stats}"
    assert t_path.exists(), "trades staging is a stale prior — its hive rows were never uploaded"


def test_prune_coverage_needs_every_lane(tmp_path, monkeypatch):
    """One lane with stale dataset staging blocks that dataset's prune
    globally (lanes share one hive)."""
    import os as _os
    import time as _t

    import polymarket_collector.config as _CFG
    import polymarket_collector.storage.export as _E

    now_ms = int(_t.time() * 1000)
    _fake = type("C", (), {
        "kaggle": type("K", (), {"rolling_window": True, "local_retention_hours": 48})(),
        "timeframes": ["5m", "15m"],
    })()
    monkeypatch.setattr(_CFG.CollectorConfig, "load", classmethod(lambda cls, *a, **k: _fake))

    base, f_old, f_mid = _prune_hive(tmp_path, now_ms)
    _os.utime(f_old, (now_ms / 1000 - 10 * 86400,) * 2)
    _os.utime(f_mid, (now_ms / 1000 - 10 * 86400,) * 2)
    _write_lane_state(base, "5m", now_ms)
    _write_lane_state(base, "15m", now_ms)
    _write_staging(base, "5m", {"BTC_book_snapshots_500ms.parquet": now_ms})
    _write_staging(base, "15m", {"BTC_book_snapshots_500ms.parquet": now_ms - 10 * 86400 * 1000})
    stats = _E.cleanup_local_data(str(base), rolling_window=True, retention_hours=0)
    assert f_old.exists() and f_mid.exists(), f"stale 15m staging must block: {stats}"
    # 15m catches up -> the old file prunes
    _write_staging(base, "15m", {"BTC_book_snapshots_500ms.parquet": now_ms})
    stats = _E.cleanup_local_data(str(base), rolling_window=True, retention_hours=0)
    assert not f_old.exists(), stats


def test_write_kaggle_state_records_build_start(tmp_path):
    from polymarket_collector.storage.export import _write_kaggle_state
    import json as _js

    staging = tmp_path / "kaggle_staging" / "5m" / "gghgg1" / "ds"
    staging.mkdir(parents=True)
    (staging / "a.parquet").write_bytes(b"")
    _write_kaggle_state(staging, "gghgg1/ds", "notes", build_start_ms=123456789)
    st = _js.loads((tmp_path / "kaggle_staging" / "5m" / "_kaggle_state.json").read_text())
    assert st["gghgg1/ds"]["build_start_unix_ms"] == 123456789
    assert st["gghgg1/ds"]["last_upload_unix_ms"] is not None
