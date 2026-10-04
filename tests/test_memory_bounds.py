"""Memory-bounds forensics (2026-10-04, branch perfect/pmdata-parity @ f438a5f).

SPEC (bounded-memory proposal, 30-line cap):
 1. ONE global row budget: ParquetWriter._buffer already shared across datasets;
 2. enforce buffer_max_rows=80000 as a HARD cap (today over-cap rows are STILL
 3. appended at parquet_writer.py:393-399, so the "cap" is advisory under pressure).
 4. When hard cap hit: WAL-spill first (existing _wal_append+fsync pattern),
 5. return False (backpressure: caller retries, never silently dropped); the row
 6. is already WAL-durable so a crash loses nothing — flush() replays it.
 7. Split l2_raw (86% of WAL rows, verbatim multi-KB frame_json) to its own
 8. writer/buffer so WS firehose can backpressure without blocking snapshots.
 9. Flush driven by min(interval 120s, threshold 40000 rows): at ~333 rows/s
10. fleet the buffer routinely sits at ~40k rows x ~2.3KB ~= 90MB serialized
11. (~250MB live dicts); halve interval to 60s to halve steady-state RSS.
12. Resync replay: keep MAX_EPISODES=500 + MAX_BUFFERED_MSGS_PER_EPISODE=2000
13. (worst case 1M msgs) but run reap_expired_buffers on a 60s timer, NOT only
14. in the 120s flush tick that parks behind the export kaggle-lock (5s wait).
15. Never-final episodes (window rolled while stale) must hit the reaper, else
16. ebuf climbs 9k-40k and RSS ratchets 833MB+ until pm2 kills slow-lane export.
17. Bound Collector._episode_latest (collector.py:255, never evicted until stop,
18. ~500 entries/h) with the same finished-only FIFO as _evict_finished_episodes.
19. Keep dedup hard cap MAX_DEDUP_KEYS_PER_DATASET=100000/dataset (already FIFO).
20. Export workers: keep 1000MB rss-cap self-abort (fail closed, export.py:3309)
21. + 550MB staging floor skip (:3642); reap *.tmp orphans AFTER verified upload
22. so aborted ticks never accumulate staging garbage.
23. Fail-closed order on ANY cap: (1) WAL-spill+fsync, (2) backpressure/False,
24. (3) counted drop ONLY if WAL itself failed (_dropped_rows, never silent).
25. Acknowledged rows are never dropped from RAM without a WAL copy on disk.
26. Metrics to watch: [mem] buf (want <10k), ebuf (want <5k), eps (want <500),
27. WAL bytes (want <20MB), staging .tmp count (want 0 post-upload).
28. Real-data-only: caps convert would-be-OOM into counted backpressure+drops
29. (book_anomaly/counted), never interpolated snapshots or fabricated buckets.
30. END SPEC.

Live evidence 2026-10-04: active WAL held 39,999 rows / 94MB (86% l2_raw);
buf 2k-33k swings; ebuf 9k-40k; export rss-cap-abort 09:26:50; 162 WAL files
(45 nonempty, 91MB); staging 637MB with 6 *.tmp orphans.
"""

from __future__ import annotations

from types import SimpleNamespace

from polymarket_collector.resync import ResyncManager
from polymarket_collector.storage.parquet_writer import ParquetWriter


# Prod constants under test (config/collector.yaml + code defaults).
SNAPSHOT_INTERVAL_MS = 500
N_ASSETS = 7
MARKETS_PER_ASSET = 2  # dual-tracking rollover: current + next window
FLUSH_INTERVAL_S = 120  # config/collector.yaml:126
FLUSH_THRESHOLD_ROWS = 40000  # config/collector.yaml:130
BUFFER_MAX_ROWS = 80000  # config/collector.yaml:115


def _fleet_rows_per_second() -> float:
    """Snapshot rows/s at the 500ms x 7-asset grid + measured WS-rate share."""
    snap = (1000 / SNAPSHOT_INTERVAL_MS) * N_ASSETS * MARKETS_PER_ASSET  # 28/s
    # Live WAL 2026-10-04: snapshots were 2226/39999 (~6%); fleet ~= 28/0.06.
    return snap / 0.06


def test_snapshot_rate_bounded_over_1h_arithmetic():
    """Regression: current flush thresholds keep 500ms x 7-asset rate bounded.

    Pure arithmetic over a simulated 1h (no network, no I/O): rows accrue at
    the fleet rate, flush drains the whole buffer every FLUSH_INTERVAL_S or
    when FLUSH_THRESHOLD_ROWS is hit. Peak occupancy must stay under
    BUFFER_MAX_ROWS (else append() takes the over-cap path that appends to
    _buffer anyway — parquet_writer.py:393-399 — and RSS ratchets).
    """
    rate = _fleet_rows_per_second()  # ~466 rows/s fleet
    assert rate > 28  # snapshots alone are the floor, l2_raw dominates
    buf = 0.0
    peak = 0.0
    flushes = 0
    t = 0.0
    dt = 1.0
    since_flush = 0.0
    while t < 3600:
        buf += rate * dt
        since_flush += dt
        if buf >= FLUSH_THRESHOLD_ROWS or since_flush >= FLUSH_INTERVAL_S:
            buf = 0.0
            since_flush = 0.0
            flushes += 1
        peak = max(peak, buf)
        t += dt
    assert flushes >= 30  # interval-driven at minimum (3600/120)
    assert peak < BUFFER_MAX_ROWS, f"peak {peak:.0f} rows hits buffer_max"


def test_buffer_cap_trigger_spills_to_wal_before_drop(tmp_path):
    """Cap trigger: a full buffer WAL-spills first, never drops (fail closed).

    Bounded-buffer patch 2026-10-04 (branch perfect/pmdata-parity): the old
    advisory behavior (WAL-spill THEN buffer over cap, return True) grew
    _buffer unbounded, so the contract is now HARD — spill+fsync WAL, return
    False WITHOUT buffering; the caller retries and the row is WAL-durable,
    never silently dropped and never counted in _dropped_rows. (Migrated from
    the advisory-cap assertions; the fail-closed WAL-first ordering is kept.)
    """
    w = ParquetWriter(
        tmp_path, flush_row_count_threshold=10**9, buffer_max_rows=5, wal_enabled=True,
    )
    w._write_group = lambda *a, **k: (_ for _ in ()).throw(IOError("disk busy"))
    results = []
    for i in range(8):
        results.append(w.append("trades", {"trade_id": f"t{i}", "token_id": "tok"}))
    assert results[:5] == [True] * 5  # room: WAL + buffered, acknowledged
    assert results[5:] == [False] * 3  # hard cap: refused WITHOUT buffering
    assert sum(w._dropped_rows.values()) == 0  # WAL healthy: never counted lost
    assert len(w._buffer) <= 5  # HARD cap holds even with flush failing
    wal_text = (w._wal_path).read_text(encoding="utf-8")
    for i in range(8):
        assert f"t{i}" in wal_text  # WAL copy on disk BEFORE any buffer reliance
    # Caller retries after the fault heals: refused rows buffer + flush clean.
    w._write_group = ParquetWriter._write_group.__get__(w)
    for i in range(5, 8):
        assert w.append("trades", {"trade_id": f"t{i}", "token_id": "tok"}) is True
    assert len(w._buffer) <= 5
    w.flush()
    assert len(w._buffer) == 0 and len(w._l2_buffer) == 0
    import glob as _glob
    import os as _os

    import pyarrow.parquet as _pq

    disk = sum(
        _pq.ParquetFile(f).metadata.num_rows
        for f in _glob.glob(_os.path.join(str(tmp_path), "trades/**/*.parquet"), recursive=True)
    )
    assert disk == 8  # 5 originals + 3 retried — nothing lost, nothing duped


def test_wal_failure_returns_false_with_honest_accounting(tmp_path):
    """WAL-spill failure: append returns False, counts, releases dedup key."""
    w = ParquetWriter(
        tmp_path, flush_row_count_threshold=10**9, buffer_max_rows=2, wal_enabled=True,
    )
    w._write_group = lambda *a, **k: (_ for _ in ()).throw(IOError("disk busy"))
    assert w.append("trades", {"trade_id": "a", "token_id": "tok"})
    assert w.append("trades", {"trade_id": "b", "token_id": "tok"})
    w._wal_append = lambda *a, **k: (_ for _ in ()).throw(IOError("wal dead"))
    assert w.append("trades", {"trade_id": "c", "token_id": "tok"}) is False
    assert w._dropped_rows["trades"] == 1  # counted, never silent
    # Dedup key released so the caller can retry after WAL recovers.
    assert all("c" not in str(k) for k in w._seen_keys["trades"])


def test_dedup_map_respects_hard_cap(tmp_path):
    """_seen_keys never exceeds MAX_DEDUP_KEYS_PER_DATASET (FIFO eviction)."""
    w = ParquetWriter(
        tmp_path,
        flush_row_count_threshold=10**9,
        buffer_max_rows=10**9,
        wal_enabled=False,
    )
    n = ParquetWriter.MAX_DEDUP_KEYS_PER_DATASET + 10_000
    for i in range(n):
        assert w.append("trades", {"trade_id": f"k{i}", "token_id": "tok"})
    assert len(w._seen_keys["trades"]) <= ParquetWriter.MAX_DEDUP_KEYS_PER_DATASET
    assert w._evict_total["trades"] >= 10_000  # evictions counted (M3 audit)


def _resync_manager():
    cfg = SimpleNamespace(ws=SimpleNamespace(max_resync_duration_seconds=60))
    return ResyncManager(config=cfg, rest_fetcher=None)


def test_replay_buffer_per_episode_cap_with_overflow_accounting():
    """Replay buffers cap at MAX_BUFFERED_MSGS_PER_EPISODE with counted overflow."""
    rm = _resync_manager()
    rid = rm.handle_disconnect("BTC", "0xabc", reason="test", books={})
    for i in range(rm.MAX_BUFFERED_MSGS_PER_EPISODE + 500):
        rm.buffer_message(rid, {"i": i})
    assert len(rm._buffers[rid]) == rm.MAX_BUFFERED_MSGS_PER_EPISODE
    assert rm._buffer_dropped_total[rid] == 500  # honest overflow accounting


def test_finished_episode_eviction_bounds_episodes():
    """_evict_finished_episodes keeps len(_episodes) <= MAX_EPISODES."""
    rm = _resync_manager()
    rids = [rm.handle_disconnect("BTC", None, reason="test", books={}) for _ in range(rm.MAX_EPISODES + 50)]
    assert len(rm._episodes) == rm.MAX_EPISODES + 50  # open episodes never evicted
    for rid in rids:
        rm._episodes[rid].resync_completed_ts_utc = "2026-10-04T00:00:00Z"
    rm.handle_disconnect("ETH", None, reason="test", books={})
    assert len(rm._episodes) <= rm.MAX_EPISODES


def test_reaper_frees_never_final_buffers():
    """Expired never-final buffers are reaped (the 120s-tick leak shape)."""
    rm = _resync_manager()
    import time

    rid = rm.handle_disconnect("XRP", "0xabc", reason="test", books={})
    for i in range(100):
        rm.buffer_message(rid, {"i": i})
    assert len(rm._buffers[rid]) == 100
    rm._buffer_deadline[rid] = time.monotonic() - 1  # deadline passed, feed quiet
    assert rm.reap_expired_buffers() >= 1
    assert rid not in rm._buffers  # RAM freed; episode row already in parquet


# --- Bounded-buffer patch 2026-10-04 (appended, forensics-owned file) --------
# Hard-cap refusal, l2_raw isolation, and split-cap arithmetic. Existing
# functions above untouched (except the advisory-cap migration flagged in
# the patch report); these pin the new contract.

def _l2_row(i: int) -> dict:
    """Distinct l2_raw rows (dedup key covers token/ts/type/frame-hash)."""
    return {
        "token_id": f"tok{i}",
        "ts_source": 1759300000000 + i,
        "event_type": "book",
        "frame_json": f'{{"i":{i},"pad":"{"x" * 200}"}}',
        "asset": "BTC",
        "ts_received_ns": 111 + i,
    }


def _snap_row(i: int) -> dict:
    return {
        "asset": "BTC",
        "condition_id": "0x" + "ab" * 32,
        "ts_snapshot_ns": 1_700_000_000_000_000_000 + i * 500_000_000,
        "ts_snapshot_utc": "2026-10-04T00:00:00Z",
    }


def test_hard_cap_refusal_returns_false_with_wal_intact(tmp_path):
    """Hard-cap refusal: False WITHOUT buffering, WAL intact, nothing lost."""
    w = ParquetWriter(
        tmp_path, flush_row_count_threshold=10**9, buffer_max_rows=4,
        l2_raw_max_rows=4, wal_enabled=True,
    )
    w._write_group = lambda *a, **k: (_ for _ in ()).throw(IOError("disk busy"))
    assert all(w.append("trades", {"trade_id": f"r{i}", "token_id": "tok"}) for i in range(4))
    refused = w.append("trades", {"trade_id": "r4", "token_id": "tok"})
    assert refused is False  # hard cap: caller retries, row NOT buffered
    assert len(w._buffer) == 4  # cap never exceeded, even with flush failing
    assert w._dropped_rows.get("trades", 0) == 0  # WAL healthy: never counted lost
    wal_text = w._wal_path.read_text(encoding="utf-8")
    assert "r4" in wal_text  # refused row is WAL-durable at False return
    # Dedup reservation released so the retry is not swallowed as a dupe.
    assert all("r4" not in str(k) for k in w._seen_keys["trades"])
    # Fault heals: retry is accepted and the row reaches Parquet exactly once.
    w._write_group = ParquetWriter._write_group.__get__(w)
    assert w.append("trades", {"trade_id": "r4", "token_id": "tok"}) is True
    w.flush()
    import glob as _glob
    import os as _os

    import pyarrow.parquet as _pq

    disk = sum(
        _pq.ParquetFile(f).metadata.num_rows
        for f in _glob.glob(_os.path.join(str(tmp_path), "trades/**/*.parquet"), recursive=True)
    )
    assert disk == 5


def test_l2_raw_isolation_snapshot_accepted_while_l2_capped(tmp_path):
    """l2_raw isolation: a capped verbatim deque never starves snapshots."""
    w = ParquetWriter(
        tmp_path, flush_row_count_threshold=10**9, buffer_max_rows=100,
        l2_raw_max_rows=3, wal_enabled=True,
    )
    w._write_group = lambda *a, **k: (_ for _ in ()).throw(IOError("disk busy"))
    assert all(w.append("l2_raw", _l2_row(i), asset="BTC") for i in range(3))
    assert w.append("l2_raw", _l2_row(99), asset="BTC") is False  # l2 capped: shed
    assert len(w._l2_buffer) == 3  # isolation deque never exceeds its own cap
    assert w._dropped_rows.get("l2_raw", 0) == 0  # shed, not lost: WAL holds it
    assert "tok99" in w._wal_path.read_text(encoding="utf-8")  # refused row WAL-durable
    # Snapshot rows route to the MAIN deque — accepted despite l2 pressure.
    assert w.append("book_snapshots_500ms", _snap_row(1), asset="BTC") is True
    assert w.append("book_snapshots_500ms", _snap_row(2), asset="BTC") is True
    assert len(w._buffer) == 2
    assert len(w._l2_buffer) == 3
    # And the reverse: a full main buffer sheds snapshots while l2 still takes.
    w2 = ParquetWriter(
        tmp_path / "w2", flush_row_count_threshold=10**9, buffer_max_rows=1,
        l2_raw_max_rows=3, wal_enabled=True,
    )
    w2._write_group = lambda *a, **k: (_ for _ in ()).throw(IOError("disk busy"))
    assert w2.append("book_snapshots_500ms", _snap_row(1), asset="BTC") is True
    assert w2.append("book_snapshots_500ms", _snap_row(2), asset="BTC") is False
    assert w2.append("l2_raw", _l2_row(7), asset="BTC") is True  # l2 deque has room
    assert len(w2._l2_buffer) == 1 and len(w2._buffer) == 1


def test_1h_arithmetic_bound_holds_under_split_caps():
    """1h arithmetic: split caps (80k main + 40k l2) never bind when healthy.

    Same fleet rate as the pre-split test (~466 rows/s, 86% l2_raw share per
    live WAL 2026-10-04), flush drains the COMBINED occupancy every
    FLUSH_THRESHOLD_ROWS or FLUSH_INTERVAL_S. Both deque peaks must stay
    under their caps — i.e. backpressure fires only when flush stalls.
    """
    rate = _fleet_rows_per_second()
    l2_share = 0.86
    main = 0.0
    l2 = 0.0
    peak_main = 0.0
    peak_l2 = 0.0
    flushes = 0
    t = 0.0
    since_flush = 0.0
    while t < 3600:
        main += rate * (1 - l2_share)
        l2 += rate * l2_share
        since_flush += 1.0
        if (main + l2) >= FLUSH_THRESHOLD_ROWS or since_flush >= FLUSH_INTERVAL_S:
            main = 0.0
            l2 = 0.0
            since_flush = 0.0
            flushes += 1
        peak_main = max(peak_main, main)
        peak_l2 = max(peak_l2, l2)
        t += 1.0
    assert flushes >= 30
    assert peak_main < BUFFER_MAX_ROWS, f"main peak {peak_main:.0f}"
    assert peak_l2 < 40000, f"l2 peak {peak_l2:.0f} hits l2_raw_max_rows"
    assert (peak_main + peak_l2) < BUFFER_MAX_ROWS, "combined peak under old global cap"
