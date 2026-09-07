"""Task 0 regression — discovery events must survive a total network outage.

Overnight 2026-09-06→07 the box suffered a ~6h total outage (DNS dead). The
collector polled the whole time (WS reconnect loop kept cycling) yet ZERO
discovery_timeout / initial_discovery / discovery_poll rows reached
data/collector_events/ — isolation tests prove fetch_next_market emits
discovery_timeout correctly on DNS failure, so the drop had to be structural.

Reproduction here runs the FULL _run_asset_loop path (not the isolated fetch)
with every endpoint pointed at a guaranteed-dead host (.invalid, RFC 2606):
while the WS is down, discovery observability must keep flowing to parquet.

Gate: under simulated total outage, discovery_timeout rows MUST appear in
collector_events parquet.
"""
import asyncio
import contextlib
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from polymarket_collector.config import CollectorConfig
from polymarket_collector.collector import Collector
from polymarket_collector.rollover import MarketDiscovery

DEAD_HTTP = "https://nonexistent.invalid"
DEAD_WS = "wss://nonexistent.invalid"


def _outage_cfg(tmpdir: str) -> CollectorConfig:
    cfg = CollectorConfig()
    cfg.assets = ["BTC"]
    cfg.timeframes = ["5m"]
    cfg.storage.data_dir = tmpdir
    cfg.storage.wal_dir = str(Path(tmpdir) / "_wal")
    cfg.raw_archive.path = str(Path(tmpdir) / "raw_ws_archive")
    cfg.raw_archive.enabled = False
    cfg.cursor_store.path = str(Path(tmpdir) / "cursor_state")
    # land event rows in parquet quickly so the assertions read fresh data
    cfg.storage.flush_interval_seconds = 2
    cfg.storage.flush_row_count_threshold = 10
    # total outage: WS, REST heal, discovery fallback and RTDS all dead
    cfg.ws.url = DEAD_WS + "/ws/market"
    cfg.ws.rest_book_url = DEAD_HTTP + "/book"
    cfg.ws.rest_market_url = DEAD_HTTP + "/markets"
    cfg.chainlink.ws_url = DEAD_WS
    cfg.discovery_poll_interval_seconds = 2
    return cfg


def _read_event_rows(data_dir: str) -> list[dict]:
    rows: list[dict] = []
    root = Path(data_dir) / "collector_events"
    if not root.exists():
        return rows
    for f in sorted(root.rglob("*.parquet")):
        rows.extend(pq.read_table(str(f)).to_pylist())
    return rows


@pytest.mark.asyncio
async def test_simulated_outage_emits_discovery_timeout_events():
    """Full-path outage: discovery_timeout/initial_discovery must reach parquet."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = _outage_cfg(tmp)
        prev_gamma = MarketDiscovery.GAMMA_BASE
        MarketDiscovery.GAMMA_BASE = DEAD_HTTP  # Gamma dead too
        try:
            collector = Collector(cfg)
            start_task = asyncio.create_task(collector.start(enable_kaggle_loop=False))
            # ~25s of simulated outage: DNS failures are instant, so the discovery
            # poller (2s cadence) cycles repeatedly and the 10s emit throttle
            # still allows >=2 discovery_timeout events.
            await asyncio.sleep(25)
            await collector.stop()
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
                await asyncio.wait_for(start_task, timeout=10)
        finally:
            MarketDiscovery.GAMMA_BASE = prev_gamma

        rows = _read_event_rows(tmp)
        by_type: dict = {}
        for r in rows:
            by_type[r["event_type"]] = by_type.get(r["event_type"], 0) + 1

        # sanity: we really were in the WS-outage path (connect kept failing)
        assert by_type.get("ws_reconnect_attempt", 0) >= 1, f"outage not simulated: {by_type}"
        # THE GATE: discovery stayed observable through the outage
        assert by_type.get("discovery_timeout", 0) >= 1, (
            f"silent discovery under outage — missing discovery_timeout rows: {by_type}"
        )
        assert by_type.get("initial_discovery", 0) >= 1, (
            f"silent initial discovery under outage — missing initial_discovery rows: {by_type}"
        )
