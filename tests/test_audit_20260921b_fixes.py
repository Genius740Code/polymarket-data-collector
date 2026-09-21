"""Regression tests — full-repo audit fixes (2026-09-21, follow-up).

Covers the data-loss paths the audit found untested:
- replay_dead_letters(limit) hitting the limit mid-file must NOT delete it
- gap-evidence staging guard: collector_events/resync_episodes must never
  shrink, even with check_monotonic=False (rolling-window mode)
- compaction merge-verify: output footer == rows written == rows read, and
  unreadable stubs are never deleted by compaction
"""
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from polymarket_collector.storage.compaction import compact_dataset
from polymarket_collector.storage.export import _verify_staging_row_counts
from polymarket_collector.storage.parquet_writer import ParquetWriter


def _ev_row(eid: str, ns: int) -> dict:
    return {"event_type": "coverage_gap", "event_id": eid,
            "ts_utc": "2026-09-21T00:00:00+00:00",
            "ts_received_ns": ns, "details": "regression"}


def test_dead_letter_limit_hit_keeps_file(tmp_path):
    w = ParquetWriter(str(tmp_path))
    dl = tmp_path / "_dead_letter"
    dl.mkdir(parents=True, exist_ok=True)
    fp = dl / "repro.jsonl"
    fp.write_text("\n".join(
        json.dumps({"dataset": "collector_events", "asset": None, "row": _ev_row(f"e{i}", 1000 + i)})
        for i in range(10)))
    stats = w.replay_dead_letters(limit=3)
    assert stats["requeued"] == 3
    assert stats["files"] == 0
    assert fp.exists(), "limit-hit mid-file must NOT delete the remainder (7 rows lost)"
    # Full pass requeues (dedup absorbs the 3 already-buffered rows) and
    # only then deletes — no row is ever destroyed by the limit path.
    stats2 = w.replay_dead_letters(limit=100_000)
    assert stats2["requeued"] == 10
    assert not fp.exists()
    w.close()


def test_gap_evidence_never_shrinks_even_rolling(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    schema_cols = ["condition_id", "asset", "series_id", "ts_snapshot_ns"]
    tbl = pa.table({c: [["c"][0] if c == "condition_id" else "x"] for c in schema_cols})
    pq.write_table(tbl, str(staging / "BTC_book_snapshots_500ms.parquet"))
    for ds in ("book_events", "trades", "chainlink_events"):
        pq.write_table(pa.table({c: [] for c in schema_cols}),
                       str(staging / f"BTC_{ds}.parquet"))
    import pyarrow.parquet as _pq
    # Prior upload had 50 gap rows; current staging shrank to 2.
    (staging.parent / "_kaggle_state.json").write_text(json.dumps(
        {"gghgg1/polymarket-5m-crypto": {"_last_staging_counts": {
            "collector_events.parquet": 50, "resync_episodes.parquet": 10}}}))
    pq.write_table(pa.table({"a": [1, 2]}), str(staging / "collector_events.parquet"))
    pq.write_table(pa.table({"a": [1]}), str(staging / "resync_episodes.parquet"))
    for f in ("markets.parquet", "markets_summary.parquet"):
        pq.write_table(pa.table({"a": [1]}), str(staging / f))
    # Rolling mode disables market-data monotonicity — but gap shrink must fail.
    assert _verify_staging_row_counts(staging, ["BTC"], check_monotonic=False) is False


def test_compaction_verifies_and_spares_unreadable(tmp_path):
    leaf = tmp_path / "leaf"
    leaf.mkdir()
    for i in range(3):
        pq.write_table(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}),
                       str(leaf / f"book_snapshots_500ms_{1000 + i}.parquet"))
    stub = leaf / "book_snapshots_500ms_9999.parquet"
    stub.write_bytes(b"not parquet at all")
    n = compact_dataset(leaf)
    assert n == 9, f"expected 9 merged rows, got {n}"
    finals = [p for p in leaf.iterdir() if p.suffix == ".parquet" and p.name.startswith("part-compacted-")]
    assert len(finals) == 1
    assert pq.read_metadata(str(finals[0])).num_rows == 9
    assert stub.exists(), "unreadable stubs must survive compaction for quarantine"
