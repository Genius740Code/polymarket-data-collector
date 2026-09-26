"""Regression tests — data-safety audit 2026-09-25 (WAL save path).

Covers the four parquet_writer fixes from the data-safety audit:
1. CRITICAL: partially-replayed WAL files must keep their full content until
   the first successful flush() persists the replayed rows, then shrink to
   the pending-only remainder (old code rewrote at replay time — a crash
   before the first flush lost replayed-but-unflushed rows).
2. WAL-spill failure (append False at spill time) must emit a throttled
   write_failed event carrying the dropped total (was counter-only).
3. Dead-letter lines missing their dataset must be skipped + counted (never
   materialize a data/None partition) and the file kept for review.
4. 0-byte WAL husks: close() removes the writer's OWN empty husk; replay
   janitors 7d+ empty husks; non-empty WALs are always kept.
"""
import json as _js
import os as _os
import time as _t

import pyarrow.parquet as pq
import pytest

from polymarket_collector.enums import CollectorEventType
from polymarket_collector.storage.parquet_writer import ParquetWriter


def _spill_row(seq, asset="BTC"):
    return {"trade_id": f"t{seq}", "token_id": "tok", "sequence_number": seq,
            "price": 0.5, "size": 1.0}


def test_partial_replay_wal_kept_until_flush(tmp_path, monkeypatch):
    """CRITICAL fix 2026-09-25: _wal_replay rewrote partially-replayed WAL
    files to pending-only IMMEDIATELY — dropping every already-replayed-but-
    unflushed row from the WAL, so a crash/restart between replay and the
    first successful flush lost those rows (PM2 restart storms under ENOSPC).
    The full file must be kept until flush() makes the replayed rows durable;
    only then is it atomically rewritten to the pending remainder."""
    wal_dir = tmp_path / "_wal"
    wal_dir.mkdir()
    rows = [{"dataset": "collector_events", "asset": "BTC", "date_str": "2026-09-25",
             "row": {"event_id": f"e{i}", "event_type": "x"}, "ts": float(i)}
            for i in range(3)]
    (wal_dir / "wal-aaa.jsonl").write_text(
        "".join(_js.dumps(r) + "\n" for r in rows))
    w = ParquetWriter(data_dir=str(tmp_path), wal_enabled=True,
                      wal_dir=str(wal_dir), buffer_max_rows=2,
                      flush_row_count_threshold=10_000)

    # Make the in-replay flush fail so one line goes pending (buffer stays full).
    def _fail(*a, **k):
        raise OSError("ENOSPC simulated")

    real_write_group = w._write_group
    monkeypatch.setattr(w, "_write_group", _fail)
    n = w._wal_replay()
    assert n == 2, f"2 lines replay, 1 pending behind full buffer, got {n}"

    # CRITICAL regression: the WAL file still holds ALL 3 lines — the replayed
    # rows are not yet durable (buffer is memory-only) and must stay in WAL.
    content = (wal_dir / "wal-aaa.jsonl").read_text()
    assert "e0" in content and "e1" in content and "e2" in content, \
        f"replayed rows must NOT be dropped from the WAL pre-flush: {content!r}"

    # First successful flush persists the replayed rows...
    monkeypatch.setattr(w, "_write_group", real_write_group)
    flushed = w.flush()
    assert flushed == 2
    # ...then the WAL shrinks to the pending-only remainder (e2 kept).
    content2 = (wal_dir / "wal-aaa.jsonl").read_text()
    assert "e2" in content2, f"pending line must survive the rewrite: {content2!r}"
    assert "e0" not in content2 and "e1" not in content2, \
        f"durable lines must be dropped post-flush: {content2!r}"

    # Crash-simulate: a fresh writer replays the pending remainder — no loss.
    w3 = ParquetWriter(data_dir=str(tmp_path), wal_enabled=True,
                       wal_dir=str(wal_dir), buffer_max_rows=10_000)
    n3 = w3._wal_replay()
    assert n3 == 1, f"pending remainder must replay, got {n3}"
    w3.flush()
    total = sum(pq.read_metadata(str(p)).num_rows
                for p in (tmp_path / "collector_events").rglob("*.parquet"))
    assert total == 3, f"all 3 rows durable across the simulated crash, got {total}"


def test_wal_spill_failure_emits_write_failed(tmp_path):
    """Audit fix 2026-09-25: a WAL-spill failure (append returning False at
    spill time) was counted in _dropped_rows but never emitted to
    collector_events — invisible to the gap trail. It must emit a throttled
    write_failed carrying the running dropped total."""
    events = []
    wal_dir = tmp_path / "_wal"
    wal_dir.mkdir()
    w = ParquetWriter(data_dir=str(tmp_path), wal_enabled=True,
                      wal_dir=str(wal_dir), buffer_max_rows=2,
                      flush_row_count_threshold=10_000,
                      on_event=lambda t, d: events.append((str(t), d)))

    # Fill the buffer via WAL replay (direct buffer insert — no _wal_append).
    for i in range(2):
        (wal_dir / f"wal-{i}.jsonl").write_text(_js.dumps({
            "dataset": "trades", "asset": "BTC", "date_str": "2026-09-25",
            "row": _spill_row(i)}) + "\n")
    assert w._wal_replay() == 2

    # Now make BOTH the flush (cannot make room) and the WAL spill fail.
    def _fail(*a, **k):
        raise OSError("ENOSPC simulated")

    w._write_group = _fail
    w._wal_append = _fail

    ok = w.append("trades", _spill_row(2), asset="BTC")
    assert ok is False, "spill failure must block (return False)"
    spill_events = [d for t, d in events
                    if t.endswith("write_failed")
                    and d.get("reason") == "wal_spill_failed_append_returned_false"]
    assert spill_events, f"spill failure must emit write_failed, got {events}"
    assert spill_events[0].get("dropped_total", 0) >= 1
    assert "ENOSPC" in spill_events[0].get("error", "")

    # Throttled: a second spill within 60s must NOT emit a second event.
    w._spill_fail_event_ts = _t.monotonic()
    ok2 = w.append("trades", _spill_row(3), asset="BTC")
    assert ok2 is False
    spill_events2 = [d for t, d in events
                     if t.endswith("write_failed")
                     and d.get("reason") == "wal_spill_failed_append_returned_false"]
    assert len(spill_events2) == len(spill_events), \
        "throttle must suppress the second spill event within 60s"


def test_dead_letter_missing_dataset_skipped(tmp_path):
    """Audit fix 2026-09-25: a dead-letter line without its dataset would
    append(dataset=None) and materialize a data/None/date=* partition in the
    hive. It must be skipped + counted (never silent) and the file kept for
    manual review when any line failed."""
    dl = tmp_path / "_dead_letter" / "trades" / "date=2026-09-25"
    dl.mkdir(parents=True)
    f = dl / "dead-1.jsonl"
    f.write_text(
        _js.dumps({"dataset": None, "asset": "BTC", "row": {"trade_id": "bad"}}) + "\n"
        + _js.dumps({"dataset": "trades", "asset": "BTC", "date_str": "2026-09-25",
                     "row": _spill_row(1)}) + "\n")
    w = ParquetWriter(data_dir=str(tmp_path), wal_enabled=False,
                      buffer_max_rows=100)
    stats = w.replay_dead_letters()
    assert stats["requeued"] == 1, stats
    assert any("missing dataset" in e for e in stats["errors"]), \
        f"skip must be counted in errors: {stats['errors']}"
    assert f.exists(), "partial file must stay for the next pass"
    assert not (tmp_path / "None").exists(), "no data/None partition may be materialized"


def test_wal_husk_cleanup_close_and_replay(tmp_path):
    """Audit fix 2026-09-25: every ParquetWriter left a 0-byte
    wal-<uuid>.jsonl husk behind — throwaway writers grew data/_wal forever
    (85 husks on a 2-day-old prod hive). close() removes the writer's OWN
    empty husk; _wal_replay janitors 7d+ empty husks; non-empty WALs are
    always kept (close-flush failure keeps them)."""
    wal_dir = tmp_path / "_wal"
    wal_dir.mkdir()

    # (a) close() removes the writer's own 0-byte husk.
    w1 = ParquetWriter(data_dir=str(tmp_path), wal_enabled=True, wal_dir=str(wal_dir))
    husk1 = w1._wal_path
    assert husk1.exists() and husk1.stat().st_size == 0
    w1.close()
    assert not husk1.exists(), "own 0-byte husk must be removed at close"

    # (b) a non-empty WAL survives close() (flush() with an empty buffer
    # returns 0 and never truncates — the WAL rows stay for the next replay).
    w2 = ParquetWriter(data_dir=str(tmp_path), wal_enabled=True, wal_dir=str(wal_dir))
    w2._wal_append("trades", _spill_row(1), "BTC", "2026-09-25")
    assert w2._wal_path.stat().st_size > 0
    w2.close()
    assert w2._wal_path.exists(), "non-empty WAL must survive close"

    # (c) 7d+ empty husks are janitored at replay; fresh empty husks kept.
    old_husk = wal_dir / "wal-old.jsonl"
    old_husk.write_text("")
    old_ts = _t.time() - 8 * 86400
    _os.utime(old_husk, (old_ts, old_ts))
    fresh_husk = wal_dir / "wal-fresh.jsonl"
    fresh_husk.write_text("")
    w3 = ParquetWriter(data_dir=str(tmp_path), wal_enabled=True, wal_dir=str(wal_dir))
    w3._wal_replay()
    assert not old_husk.exists(), "7d+ empty husk must be janitored at replay"
    assert fresh_husk.exists(), "fresh empty husk must be kept (active writer possible)"
