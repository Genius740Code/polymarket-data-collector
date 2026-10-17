"""2026-10-17 ENOSPC loop: the export-path unreadable audit went unbounded.

`_export_and_upload_all_kaggle_impl` ran `quarantine_unreadable()` on every
lane tick — a full-tree `rglob("*.parquet")` with one footer read per file.
Measured on the live box: 312s for only 4 of 10 dataset dirs (12k+12k+10k+12k
files), 40k+ files total — the 08:51/09:00 ticks never completed their pass
and Kaggle went ~20h without a verified upload (no upload -> no verified
prune -> disk 0B free -> ~189 crash-loop restarts -> 7.5h collection outage
2026-10-17 01:00-08:30 UTC).

Covers:
- stubs inside the recent-mtime window ARE quarantined (fail-closed kept)
- readable files older than the watermark are NOT re-footer-read
  (the unbounded variant re-read everything — measured 312s/tick)
- budget exhaustion does NOT advance the watermark (never claims coverage
  it did not do); next tick re-checks the same window
- state persists across calls (process restart on the live box must not
  repeat the full baseline audit)
- quarantine contents are never re-moved (same invariant as full variant)
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.quarantine import (
    AUDIT_STATE_NAME,
    quarantine_unreadable_bounded,
)


def _leaf(tmp_path: Path) -> Path:
    d = tmp_path / "settlement" / "date=2026-10-17" / "asset=BTC"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_good(path: Path) -> None:
    pq.write_table(pa.table({"a": [1, 2]}), str(path))


def _stub(path: Path) -> None:
    path.write_bytes(b"BAD" * 1000)


def test_bounded_quarantines_recent_stub_and_keeps_good(tmp_path):
    leaf = _leaf(tmp_path)
    _write_good(leaf / "good.parquet")
    _stub(leaf / "stub.parquet")
    res = quarantine_unreadable_bounded(tmp_path, time_budget_s=30.0)
    moved = [m["path"] for m in res["moved"]]
    assert any(str(p).endswith("stub.parquet") for p in moved), res
    assert (leaf / "good.parquet").exists()
    assert (tmp_path / "_quarantine" / "settlement" / "date=2026-10-17" / "asset=BTC" / "stub.parquet").exists()
    assert res["budget_exhausted"] is False
    assert res["watermark_ns"] > 0


def test_incremental_does_not_rescan_watermarked_files(tmp_path, monkeypatch):
    leaf = _leaf(tmp_path)
    _write_good(leaf / "old_good.parquet")
    # baseline: full audit persists the watermark
    res0 = quarantine_unreadable_bounded(tmp_path, time_budget_s=30.0)
    assert res0["scanned"] >= 1

    reads = {"n": 0}
    real_meta = pq.read_metadata

    def counting(path, *a, **k):
        reads["n"] += 1
        return real_meta(path, *a, **k)

    monkeypatch.setattr(pq, "read_metadata", counting)

    # a stub written AFTER the baseline watermark must still be caught
    time.sleep(0.05)
    _stub(leaf / "fresh_stub.parquet")
    now_ns = int(time.time() * 1e9) + 10
    os.utime(leaf / "fresh_stub.parquet", ns=(now_ns, now_ns))

    res1 = quarantine_unreadable_bounded(tmp_path, time_budget_s=30.0)
    moved = [m["path"] for m in res1["moved"]]
    assert any(str(p).endswith("fresh_stub.parquet") for p in moved), res1
    # the old readable file was NOT re-footer-read (that's the 312s/tick tax)
    assert not any("old_good" in str(p) for p in moved)
    assert reads["n"] <= 2, {"footer_reads": reads["n"], "scanned": res1["scanned"]}


def test_budget_exhaustion_does_not_advance_watermark(tmp_path):
    leaf = _leaf(tmp_path)
    # many readable files: a tiny budget cannot finish their footers
    for i in range(40):
        _write_good(leaf / f"g{i:02d}.parquet")
    res0 = quarantine_unreadable_bounded(tmp_path, time_budget_s=0.001)
    assert res0["budget_exhausted"] is True
    assert res0["watermark_ns"] == 0, res0
    # a completed next tick DOES advance it and quarantines nothing readable
    res1 = quarantine_unreadable_bounded(tmp_path, time_budget_s=30.0)
    assert res1["budget_exhausted"] is False
    assert res1["moved"] == []
    assert res1["watermark_ns"] > 0


def test_state_persists_across_restarts(tmp_path):
    leaf = _leaf(tmp_path)
    _write_good(leaf / "g.parquet")
    quarantine_unreadable_bounded(tmp_path, time_budget_s=30.0)
    state = tmp_path / "watchdog" / AUDIT_STATE_NAME
    assert state.exists()
    import json

    j = json.loads(state.read_text())
    assert j["watermark_ns"] > 0


def test_no_rescan_of_quarantine_contents(tmp_path):
    leaf = _leaf(tmp_path)
    _stub(leaf / "stub.parquet")
    res0 = quarantine_unreadable_bounded(tmp_path, time_budget_s=30.0)
    assert len(res0["moved"]) == 1
    # idempotent: quarantine contents are skipped, live scan clean
    res1 = quarantine_unreadable_bounded(tmp_path, time_budget_s=30.0)
    assert res1["moved"] == []
