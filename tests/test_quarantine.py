"""2026-09-14 LOW staging abort: footer-less finals fail-close uploads.

A SIGKILL mid-flush left truncated `*.parquet` files at final paths
(`Parquet magic bytes not found in footer`) plus 0-byte stubs
(`Parquet file size is 0 bytes`). The export must fail closed on ANY
unreadable input — including 0-byte stubs (failed=1, failed_bytes=0),
which the old `failed_bytes`-only gate let through to Kaggle.

Covers:
- pre-validation gate trips on failed-count as well as failed-bytes
- quarantine moves unreadable stubs aside, keeps readable files
- compaction never deletes an unreadable stub
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.compaction import compact_dataset
from polymarket_collector.storage.quarantine import quarantine_unreadable


def _gate_aborts(manifests: dict) -> bool:
    """Mirror of the export pre-validation gate (count AND bytes)."""
    lost = []
    for _mk, _mi in manifests.items():
        if not _mi.get("ok"):
            lost.append(_mk)
        _re = _mi.get("read_errors") or {}
        if int(_re.get("failed_bytes") or 0) > 0 or int(_re.get("failed") or _re.get("files_failed") or 0) > 0:
            lost.append(_mk)
    return bool(lost)


def test_gate_trips_on_zero_byte_stub():
    m = {"book_snapshots_500ms/LONDON": {"ok": True, "read_errors": {"failed": 1, "failed_bytes": 0, "ok": 5}}}
    assert _gate_aborts(m), "0-byte stub (failed=1, bytes=0) must abort, not ship partial staging"


def test_gate_trips_on_truncated_stub():
    m = {"book_snapshots_500ms/LONDON": {"ok": True, "read_errors": {"failed": 1, "failed_bytes": 45633, "ok": 5}}}
    assert _gate_aborts(m)


def test_gate_passes_clean():
    m = {"book_snapshots_500ms/LONDON": {"ok": True, "read_errors": {"failed": 0, "failed_bytes": 0, "ok": 5}}}
    assert not _gate_aborts(m)
    assert not _gate_aborts({"book_snapshots_500ms/LONDON": {"ok": True, "read_errors": {}}})


def test_quarantine_moves_only_unreadable(tmp_path):
    d = tmp_path / "date=2026-09-13" / "asset=LONDON"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"a": [1]}), str(d / "good.parquet"))
    (d / "truncated_1789331692627.parquet").write_bytes(b"BAD" * 1000)
    (d / "empty.parquet").write_bytes(b"")
    moved = quarantine_unreadable(tmp_path)
    assert len(moved) == 2, moved
    assert (d / "good.parquet").exists()
    assert (tmp_path / "_quarantine" / "date=2026-09-13" / "asset=LONDON" / "truncated_1789331692627.parquet").exists()
    # second run is clean (idempotent — never re-moves quarantine contents)
    assert quarantine_unreadable(tmp_path) == []


def test_compaction_preserves_unreadable_stub(tmp_path):
    leaf = tmp_path / "book_events" / "date=2026-09-13" / "asset=LONDON"
    leaf.mkdir(parents=True)
    t = pa.table({"a": [1, 2]})
    pq.write_table(t, str(leaf / "book_events_1.parquet"), compression="zstd")
    pq.write_table(t, str(leaf / "book_events_2.parquet"), compression="zstd")
    (leaf / "book_events_1789331692627.parquet").write_bytes(b"TRUNCATED-NOT-PARQUET" * 500)
    assert compact_dataset(leaf) == 4
    names = sorted(p.name for p in leaf.iterdir())
    assert any("1789331692627" in n for n in names), f"stub must survive compaction: {names}"
