"""PERF-safe changes (2026-09-27): uvloop wiring + mem_report telemetry.

Zero data impact by construction — no schemas, no writers, no WS logic.
"""
import sys
from types import SimpleNamespace

import pytest


def test_uvloop_importable_or_fallback_documented():
    """uvloop must either import (fast loop) or the cli fallback path works."""
    try:
        import uvloop  # noqa: F401

        assert sys.platform != "win32"
    except ImportError:
        pytest.skip("uvloop not installed — cli falls back to asyncio default")


def test_cli_uvloop_fallback_structure():
    """cli.main must keep the ConnectionResetError belt-and-braces handler."""
    import inspect

    from polymarket_collector import cli

    src = inspect.getsource(cli.main)
    assert "uvloop" in src
    assert "loop_factory" in src
    assert "ConnectionResetError" in src


def test_mem_report_snapshot_shape():
    """Snapshot returns all expected keys with ints (or -1 fallbacks)."""
    from polymarket_collector.collector import Collector

    stub = SimpleNamespace(
        books={"a": 1, "b": 2},
        resync=SimpleNamespace(_episodes={"e": 1}, _buffers={"e": [1, 2, 3]}),
        writer=SimpleNamespace(_buffer=[1]),
    )
    snap = Collector._mem_report_snapshot(stub)
    for key in ("rss_mb", "books", "books_by_token", "episode_latest",
                "last_frame_ns", "underlying_cache", "heal_inflight",
                "chainlink_events", "writer_buffered", "resync_episodes",
                "resync_buffers", "resync_buffered_msgs"):
        assert key in snap, key
        assert isinstance(snap[key], int), (key, snap[key])
    assert snap["books"] == 2
    assert snap["resync_buffered_msgs"] == 3
    assert snap["writer_buffered"] == 1
    assert snap["books_by_token"] == -1  # missing attr -> honest fallback
    assert isinstance(snap["gc_counts"], list)


def test_mem_report_snapshot_never_raises():
    """Garbage in still yields a dict, never an exception."""
    from polymarket_collector.collector import Collector

    snap = Collector._mem_report_snapshot(object())
    assert isinstance(snap, dict)
    assert snap["books"] == -1


def test_mem_report_throttle():
    """Second call within the hour emits nothing."""
    from polymarket_collector.collector import Collector
    from polymarket_collector.enums import CollectorEventType

    seen = []

    class Fake(Collector):
        def __init__(self):
            pass  # skip heavy Collector.__init__

        def _collector_event(self, typ, details):
            seen.append((typ, details))

    f = Fake()
    f._maybe_mem_report(100000.0)
    assert len(seen) == 1
    assert seen[0][0] == CollectorEventType.mem_report
    f._maybe_mem_report(100001.0)  # same hour -> silent
    assert len(seen) == 1
    f._maybe_mem_report(100000.0 + 3601.0)  # next hour -> emits
    assert len(seen) == 2
