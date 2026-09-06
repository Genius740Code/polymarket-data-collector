"""Multi-timeframe runner tests — lanes, per-TF staging, rolling prune, cursor."""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pyarrow as pa
import pytest

from polymarket_collector.config import CollectorConfig
from polymarket_collector.rollover import MarketInfo, RolloverManager, RolloverState
from polymarket_collector.storage.cursor_store import CursorState, CursorStore
from polymarket_collector.storage.export import (
    cleanup_local_data,
    export_per_asset_single_file,
    _read_dataset_per_asset,
)


def _cfg(tmp: str, timeframes=("5m", "15m")) -> CollectorConfig:
    return CollectorConfig(assets=["BTC", "ETH"], storage={"data_dir": tmp},
                           cursor_store={"path": str(Path(tmp) / "cursor_state")},
                           timeframes=list(timeframes))


def _market(asset: str, ws: int, ts: int, cid: str) -> MarketInfo:
    label = CollectorConfig.window_label_for(ws)
    return MarketInfo(
        condition_id=cid, market_id=cid, asset=asset,
        up_token_id=f"{cid}-UP", down_token_id=f"{cid}-DOWN",
        market_start_ts_ms=ts, market_end_ts_ms=ts + ws * 1000,
        window_index=ts // ws, series_id=f"{asset}-{label}",
        window_label=label, window_size_seconds=ws,
    )


def test_config_timeframe_validation():
    cfg = CollectorConfig(timeframes=["5m", "15m", "4h"])
    assert cfg.timeframe_window_sizes() == {"5m": 300, "15m": 900, "4h": 14400}
    # dedup + normalize
    cfg2 = CollectorConfig(timeframes=["15M", "15m"])
    assert cfg2.timeframes == ["15m"]
    # unknown label rejected
    with pytest.raises(Exception):
        CollectorConfig(timeframes=["2h"])
    # scaled cadence: longer windows poll less often (Gamma load stays flat)
    assert cfg.discovery_poll_interval_for(300) <= cfg.discovery_poll_interval_for(900) <= cfg.discovery_poll_interval_for(14400)
    # lead scales with window, never below base
    assert cfg.rollover_lead_for(86400) >= cfg.rollover_lead_for(300)
    # dataset map fallback
    assert cfg.kaggle_dataset_for("5m") == "gghgg1/polymarket-5m-crypto"
    assert cfg.kaggle_dataset_for("15m") == "gghgg1/polymarket-15m-crypto"


def test_rollover_lanes_independent():
    cfg = _cfg(str(Path.cwd()))
    mgr = RolloverManager(cfg, on_event=lambda t, d: None)
    now_ms = int(time.time() * 1000)
    m5 = _market("BTC", 300, now_ms - 1000, "cid-5m")
    m15 = _market("BTC", 900, now_ms - 1000, "cid-15m")
    # set currents per lane directly
    mgr.states[("BTC", "5m")].current = m5
    mgr.states[("BTC", "15m")].current = m15
    # active_markets returns the union across lanes
    actives = mgr.active_markets("BTC")
    assert {m.condition_id for m in actives} == {"cid-5m", "cid-15m"}
    # each market resolves back to its own lane
    assert mgr.state_for_market(m5) is mgr.states[("BTC", "5m")]
    assert mgr.state_for_market(m15) is mgr.states[("BTC", "15m")]
    # lane restriction (test mode) hides the other lane
    mgr.set_enabled_lanes(["15m"])
    assert {m.condition_id for m in mgr.active_markets("BTC")} == {"cid-15m"}
    assert mgr.enabled_lane_labels() == ["15m"]


def test_rollover_check_and_roll_all_discovers_per_lane():
    cfg = _cfg(str(Path.cwd()))
    events = []
    mgr = RolloverManager(cfg, on_event=lambda t, d: events.append((t, d.get("asset"))))

    async def fake_fetch(asset, after_ts_ms, strict_adjacent=False):
        ws = mgr.primary_tf if strict_adjacent else None
        # return a market for whichever lane's discovery asked
        return None

    # stub each lane's discovery with a distinct market
    discovered = {}

    def make_fake(tf, ws):
        async def fetch(asset, after_ts_ms, strict_adjacent=False):
            ts = after_ts_ms // 1000 // ws * ws
            cid = f"cid-{tf}"
            if cid in discovered:
                return None
            discovered[cid] = True
            return _market(asset, ws, ts * 1000, cid)
        return fetch

    for tf in mgr.lane_ws:
        mgr.discoveries[tf].fetch_next_market = make_fake(tf, mgr.lane_ws[tf])

    async def sub(m):
        return None

    asyncio.run(mgr.check_and_roll_all("BTC", sub))
    # both lanes discovered their own market
    assert mgr.states[("BTC", "5m")].current is not None
    assert mgr.states[("BTC", "15m")].current is not None
    assert mgr.states[("BTC", "5m")].current.condition_id == "cid-5m"
    assert mgr.states[("BTC", "15m")].current.condition_id == "cid-15m"
    assert mgr.states[("BTC", "5m")].current.series_id == "BTC-5m"
    assert mgr.states[("BTC", "15m")].current.series_id == "BTC-15m"


def test_cursor_store_lane_keying_and_migration(tmp_path):
    cfg = _cfg(str(tmp_path))
    store = CursorStore.for_asset(cfg, "BTC")
    # write a legacy (pre-multi-TF) table and verify migration preserves it as 5m
    import sqlite3
    conn = sqlite3.connect(str(store.db_path))
    conn.executescript(
        """
        DROP TABLE IF EXISTS cursor_state;
        CREATE TABLE cursor_state (
            asset TEXT PRIMARY KEY,
            current_window_index INTEGER NOT NULL,
            current_condition_id TEXT,
            next_condition_id TEXT,
            last_sequence_number_per_token TEXT NOT NULL DEFAULT '{}',
            last_snapshot_written_ts INTEGER,
            updated_at TEXT NOT NULL
        );
        INSERT INTO cursor_state VALUES ('BTC', 7, 'cid-legacy', NULL, '{}', 1234, '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()
    # re-open: migration must fold the legacy row into the 5m lane
    store2 = CursorStore.for_asset(cfg, "BTC")
    st = store2.load("BTC", window_label="5m")
    assert st is not None and st.current_condition_id == "cid-legacy" and st.current_window_index == 7
    # per-lane rows don't collide
    s5 = CursorState(asset="BTC", current_window_index=10, current_condition_id="cid-5m", window_label="5m")
    s15 = CursorState(asset="BTC", current_window_index=1, current_condition_id="cid-15m", window_label="15m")
    store2.save(s5)
    store2.save(s15)
    assert store2.load("BTC", "5m").current_condition_id == "cid-5m"
    assert store2.load("BTC", "15m").current_condition_id == "cid-15m"
    assert store2.load_all()[("BTC", "15m")].current_condition_id == "cid-15m"


def _write_parquet(path: Path, rows: list, schema_cols: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({c: [r.get(c) for r in rows] for c in schema_cols})
    import pyarrow.parquet as pq
    pq.write_table(table, str(path))


def test_tf_filter_and_rolling_prune(tmp_path):
    base = Path(tmp_path)
    now_ms = int(time.time() * 1000)
    old_end = now_ms - 100 * 3600 * 1000      # ended 100h ago (past 48h leeway)
    recent_end = now_ms - 2 * 3600 * 1000     # ended 2h ago (inside leeway)

    mk = lambda cid, end: {"condition_id": cid, "asset": "BTC", "market_end_ts_ms": end}
    markets_rows = [mk("cid-old", old_end), mk("cid-recent", recent_end)]
    _write_parquet(base / "markets_latest" / "markets_latest.parquet", markets_rows,
                   ["condition_id", "asset", "market_end_ts_ms"])

    # shared hive: 5m lane + 15m lane rows in the same partition file
    snap_rows = [
        {"condition_id": "cid-old", "asset": "BTC", "series_id": "BTC-5m", "ts_snapshot_ns": now_ms * 1e6},
        {"condition_id": "cid-recent", "asset": "BTC", "series_id": "BTC-5m", "ts_snapshot_ns": now_ms * 1e6},
        {"condition_id": "cid-old-15m", "asset": "BTC", "series_id": "BTC-15m", "ts_snapshot_ns": now_ms * 1e6},
    ]
    f = base / "book_snapshots_500ms" / "date=2026-09-06" / "asset=BTC" / "part-0.parquet"
    _write_parquet(f, snap_rows, ["condition_id", "asset", "series_id", "ts_snapshot_ns"])

    # TF filter: only the lane's rows come back
    t5 = _read_dataset_per_asset(base, "book_snapshots_500ms", "BTC", timeframe_label="5m")
    assert set(t5.column("series_id").to_pylist()) == {"BTC-5m"}
    t15 = _read_dataset_per_asset(base, "book_snapshots_500ms", "BTC", timeframe_label="15m")
    assert set(t15.column("series_id").to_pylist()) == {"BTC-15m"}

    # rolling prune: the file also contains a RECENT market's rows → kept (conservative)
    cfg = _cfg(str(base))
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48, checkpoint_ms=now_ms)
    assert f.exists(), "prune deleted a file containing an in-leeway market"

    # now isolate the old market in its own file → deletable
    f_old = base / "book_snapshots_500ms" / "date=2026-09-01" / "asset=BTC" / "part-0.parquet"
    _write_parquet(f_old, [{"condition_id": "cid-old", "asset": "BTC", "series_id": "BTC-5m", "ts_snapshot_ns": now_ms * 1e6}],
                   ["condition_id", "asset", "series_id", "ts_snapshot_ns"])
    stats = cleanup_local_data(str(base), rolling_window=True, retention_hours=48, checkpoint_ms=now_ms)
    assert not f_old.exists(), "file with only pre-cutoff markets should be deleted"
    assert f.exists(), "file with an in-leeway market must survive"

    # cumulative mode never deletes
    stats = cleanup_local_data(str(base), rolling_window=False, checkpoint_ms=now_ms)
    assert stats == {}


def test_rolling_staging_allows_shrink(tmp_path):
    from polymarket_collector.storage.export import _verify_staging_row_counts
    staging = Path(tmp_path) / "staging"
    staging.mkdir()
    schema_cols = ["condition_id", "asset", "series_id", "ts_snapshot_ns"]
    _write_parquet(staging / "BTC_book_snapshots_500ms.parquet",
                   [{"condition_id": "c", "asset": "BTC", "series_id": "BTC-5m", "ts_snapshot_ns": 0}], schema_cols)
    for ds in ("book_events", "trades", "chainlink_events", "book_snapshots_clean"):
        _write_parquet(staging / f"BTC_{ds}.parquet", [], schema_cols)
    for fname in ("markets.parquet", "collector_events.parquet", "resync_episodes.parquet", "markets_summary.parquet"):
        (staging / fname).write_bytes(b"")
    assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    # without ETH files this must fail the existence check either way
    assert _verify_staging_row_counts(staging, assets, check_monotonic=True) is False
    assert _verify_staging_row_counts(staging, assets, check_monotonic=False) is False
    # monotonic shrink-blocking: previously-big file now smaller must fail with check on
    import pyarrow.parquet as pq
    state = {"gghgg1/polymarket-5m-crypto": {"_last_staging_counts": {"BTC_book_snapshots_500ms.parquet": 5000}}}
    (staging.parent / "_kaggle_state.json").write_text(json.dumps(state))
    assert _verify_staging_row_counts(staging, assets, check_monotonic=True) is False
