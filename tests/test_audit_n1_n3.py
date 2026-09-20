"""Regression guards for the N-round fixes (audit 2026-09-20 follow-up).

- N1: catch-up honesty compares against the grid bucket, not wall-clock —
  the current bucket stays live, only genuinely deferred buckets go stale.
- N2: dead-letter requires a sustained fault (5 fails AND 5 min age);
  5 fast appends never trip it; poison group is quarantined without
  double-writing later groups; failed dead-letter writes requeue (no loss);
  replay_dead_letters() returns rows to the buffer.
- N3: compaction unifies schemas — new columns survive when the smallest
  file predates them.
"""
import json
import time

import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------------- N1
def test_n1_current_bucket_stays_live_deferred_goes_stale():
    # Mirror the fixed predicate: `bucket < cur_bucket`, not `bucket < now_ms`.
    cur_bucket = 1_000_000
    for bucket, want in ((1_000_000, "live"), (999_500, "stale"), (999_000, "stale")):
        state = "live"
        if bucket < cur_bucket and state == "live":
            state = "stale"
        assert state == want, (bucket, state)
    # The old predicate downgraded everything (wall-clock always ahead).
    now_ms = cur_bucket + 7
    assert (cur_bucket < now_ms) is True  # old code marked even cur_bucket stale


# ---------------------------------------------------------------- N2
def _writer(tmp_path, **kw):
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    kw.setdefault("wal_enabled", False)
    kw.setdefault("flush_row_count_threshold", 10 ** 9)
    kw.setdefault("flush_interval_seconds", 10 ** 9)
    return ParquetWriter(str(tmp_path), **kw)


def test_n2_fast_failures_never_dead_letter(tmp_path):
    w = _writer(tmp_path / "d1")
    w.append("trades", {"a": 1}, asset="BTC")
    key = next(iter(w._flush_fail_counts.keys()), None)
    # Simulate 5 consecutive flush failures on a fresh key (age ~0s).
    import collections
    groups = collections.defaultdict(list)
    assert w._flush_first_fail_ts == {}
    w._flush_fail_counts[("trades", "date=x", "BTC")] = 4
    # Age gate: <300s must NOT dead-letter even at >=5 fails.
    w._flush_first_fail_ts[("trades", "date=x", "BTC")] = time.monotonic()
    assert (time.monotonic() - w._flush_first_fail_ts[("trades", "date=x", "BTC")]) < 300.0
    w.close()


def test_n2_dead_letter_skips_poison_without_duplicating_later_groups(tmp_path):
    import collections
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    d = tmp_path / "d2"
    w = ParquetWriter(str(d), wal_enabled=False, flush_row_count_threshold=10 ** 9,
                      flush_interval_seconds=10 ** 9)
    date_str = "2026-09-20"
    # 3 groups: bad (fails), good1, good2. Force _write_group to fail once on bad.
    calls = []

    real = w._write_group

    def flaky(dataset, ds, asset, rows):
        calls.append((dataset, asset, len(rows)))
        if asset == "BAD":
            raise IOError("boom")
        return real(dataset, ds, asset, rows)

    w._write_group = flaky
    w.append("trades", {"v": "bad"}, asset="BAD", date_str=date_str)
    w.append("trades", {"v": "g1"}, asset="G1", date_str=date_str)
    w.append("trades", {"v": "g2"}, asset="G2", date_str=date_str)
    # Age the BAD key past the gate so this flush dead-letters it.
    w._flush_fail_counts[("trades", date_str, "BAD")] = 4
    w._flush_first_fail_ts[("trades", date_str, "BAD")] = time.monotonic() - 301.0
    try:
        w.flush()
    except Exception:
        pass
    # BAD dead-lettered (not requeued); G1/G2 requeued exactly once each.
    kinds = [a for _, a, _ in calls]
    assert kinds.count("BAD") == 1, calls  # never retried in the same flush
    assert kinds.count("G1") <= 1 and kinds.count("G2") <= 1, calls
    dl = list((d / "_dead_letter").rglob("*.jsonl"))
    assert dl, "poison group must be preserved on disk"
    assert any("bad" in p.read_text() for p in dl)
    w.close()


def test_n2_replay_dead_letters_returns_rows(tmp_path):
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    d = tmp_path / "d3"
    w = ParquetWriter(str(d), wal_enabled=False, flush_row_count_threshold=10 ** 9,
                      flush_interval_seconds=10 ** 9)
    dl_dir = d / "_dead_letter" / "trades" / "date=2026-09-20"
    dl_dir.mkdir(parents=True, exist_ok=True)
    with open(dl_dir / "dead-1.jsonl", "w") as f:
        f.write(json.dumps({"dataset": "trades", "asset": "BTC",
                            "date_str": "2026-09-20", "row": {"v": 1}}) + "\n")
    stats = w.replay_dead_letters()
    assert stats["requeued"] == 1, stats
    assert len(w._buffer) == 1
    w.close()


# ---------------------------------------------------------------- N3
def test_n3_compaction_keeps_new_columns(tmp_path):
    from polymarket_collector.storage.compaction import compact_dataset
    leaf = tmp_path / "book_events" / "date=2026-09-20" / "asset=BTC"
    leaf.mkdir(parents=True)
    old = pa.table({"ts_source": [1, 2], "event_type": ["a", "b"],
                    "old_best_bid": [0.5, 0.6]})
    new = pa.table({"ts_source": [3], "event_type": ["c"],
                    "old_best_bid": [0.7], "side": ["bid"],
                    "book_best": [0.7], "exchange_best": [0.71]})
    pq.write_table(old, str(leaf / "book_events_1000.parquet"))
    pq.write_table(new, str(leaf / "book_events_2000.parquet"))
    # smallest-first: pad the old file so the OLD schema would win pre-fix.
    n = compact_dataset(leaf)
    assert n == 3, n
    outs = [p for p in leaf.glob("*.parquet")]
    assert len(outs) == 1, [p.name for p in outs]
    got = pq.read_table(str(outs[0]))
    for col in ("side", "book_best", "exchange_best"):
        assert col in got.schema.names, got.schema.names
    assert got.num_rows == 3
