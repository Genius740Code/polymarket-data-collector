"""Tests for §17A watchdog — heartbeat staleness, alerting."""
import asyncio
import json
import tempfile
import time
from pathlib import Path

import pytest

from polymarket_collector.config import CollectorConfig
from polymarket_collector.watchdog.watchdog import Heartbeat, Watchdog


def test_heartbeat_age():
    with tempfile.TemporaryDirectory() as tmp:
        hb = Heartbeat(Path(tmp) / "hb.json")
        assert hb.age_seconds() is None
        hb.beat({"extra": 1})
        assert hb.age_seconds() is not None
        assert hb.age_seconds() < 1.0
        time.sleep(0.05)
        assert hb.age_seconds() >= 0.04


@pytest.mark.asyncio
async def test_watchdog_detects_stale_heartbeat():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = CollectorConfig()
        cfg.storage.data_dir = tmp
        cfg.watchdog.heartbeat_stale_seconds = 1
        alerts = []
        wd = Watchdog(cfg, heartbeat_path=Path(tmp) / "hb.json", on_alert=lambda t, d: alerts.append(t))
        # no heartbeat → process_down
        fired = await wd.check_once()
        assert "process_down" in fired

        # fresh heartbeat → no alert
        wd.heartbeat.beat()
        alerts.clear()
        fired2 = await wd.check_once()
        assert "process_down" not in fired2

        # stale heartbeat → alert
        # manually age by writing old timestamp
        old_payload = {"ts_ns": time.time_ns() - 2_000_000_000, "ts_utc": "old"}
        Path(tmp, "hb.json").write_text(json.dumps(old_payload))
        fired3 = await wd.check_once()
        assert "process_down" in fired3


def test_watchdog_daily_summary():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = CollectorConfig()
        cfg.storage.data_dir = tmp
        wd = Watchdog(cfg, heartbeat_path=Path(tmp) / "hb.json")
        summary = wd.daily_summary(date_str="2025-01-01")
        assert "assets" in summary
        assert "date" in summary
