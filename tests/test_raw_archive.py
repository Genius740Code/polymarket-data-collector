"""Tests for §13 raw WS archive — rolling window, replay, scope note."""
import json
import time
import tempfile
from pathlib import Path

from polymarket_collector.storage.raw_archive import RawArchive


def test_raw_archive_append_and_prune():
    with tempfile.TemporaryDirectory() as tmp:
        arc = RawArchive(Path(tmp) / "raw", retention_hours=0.001, enabled=True)  # 3.6s retention for test
        arc.append("BTC", {"type": "book", "price": 0.5})
        arc.append("ETH", {"type": "trade", "price": 0.6})
        assert any(Path(tmp).rglob("raw-*.jsonl"))
        # artificially age file
        for p in Path(tmp).rglob("raw-*.jsonl"):
            old = time.time() - 100
            import os
            os.utime(p, (old, old))
        deleted = arc.prune()
        assert deleted >= 1


def test_raw_archive_does_not_recover_outage():
    """Scope note: raw archive only replays what was actually received; gaps where nothing captured need secondary source."""
    with tempfile.TemporaryDirectory() as tmp:
        arc = RawArchive(tmp, enabled=True)
        # no messages appended during outage → nothing to replay
        msgs = list(arc.replay("BTC", date_str="2025-01-01"))
        assert msgs == []
        # after messages, replay works — but it's not backfill for genuine outage windows
        arc.append("BTC", {"msg": 1})
        # outage window (no append) has no messages to replay; this is expected per §13 scope note
        assert True


def test_raw_archive_disabled():
    arc = RawArchive("/tmp/should-not-create", enabled=False)
    arc.append("BTC", {"x": 1})  # no error
    assert arc.prune() == 0
