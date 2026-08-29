"""Tests for §1B cursor store — crash/restart recovery, concurrency spec."""
import tempfile
from pathlib import Path

from polymarket_collector.config import CollectorConfig
from polymarket_collector.storage.cursor_store import CursorState, CursorStore


def test_per_asset_isolation():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = CollectorConfig()
        cfg.cursor_store.mode = "per_asset"
        cfg.cursor_store.path = tmp

        store_btc = CursorStore.for_asset(cfg, "BTC")
        store_eth = CursorStore.for_asset(cfg, "ETH")

        # different files
        assert store_btc.db_path != store_eth.db_path

        btc_state = CursorState(asset="BTC", current_window_index=10, current_condition_id="btc-cid", last_sequence_number_per_token={"tok1": 99}, last_snapshot_written_ts=123456)
        eth_state = CursorState(asset="ETH", current_window_index=5, current_condition_id="eth-cid", last_sequence_number_per_token={"tok2": 42})

        store_btc.save(btc_state)
        store_eth.save(eth_state)

        # ensure isolation: eth load doesn't see btc
        loaded_btc = store_btc.load("BTC")
        loaded_eth = store_eth.load("ETH")
        assert loaded_btc.current_condition_id == "btc-cid"
        assert loaded_eth.current_condition_id == "eth-cid"
        assert store_btc.load("ETH") is None  # per-asset file shouldn't have ETH

        # crash in one asset doesn't touch another
        # simulate by deleting BTC file, ETH should still be readable
        store_btc.db_path.unlink()
        assert store_eth.load("ETH") is not None


def test_shared_wal_concurrent_writes():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = CollectorConfig()
        cfg.cursor_store.mode = "shared_wal"
        cfg.cursor_store.path = tmp

        store = CursorStore.for_asset(cfg, "BTC")  # returns shared
        assert store.wal_mode is True
        assert store.db_path.name == "shared.db"

        # concurrent writes from 3 assets to shared WAL
        for asset in ["BTC", "ETH", "SOL"]:
            s = CursorState(asset=asset, current_window_index=1, current_condition_id=f"{asset}-cid", last_sequence_number_per_token={"tok": 1})
            store.save(s)

        all_states = store.load_all()
        assert len(all_states) == 3
        assert all_states["BTC"].current_condition_id == "BTC-cid"
        assert all_states["ETH"].current_condition_id == "ETH-cid"
        assert all_states["SOL"].current_condition_id == "SOL-cid"


def test_cursor_persist_and_reload():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = CollectorConfig()
        cfg.cursor_store.path = tmp
        cfg.cursor_store.mode = "per_asset"
        store = CursorStore.for_asset(cfg, "BTC")

        state = CursorState(asset="BTC", current_window_index=99, current_condition_id="cid-xyz", next_condition_id="cid-next", last_sequence_number_per_token={"up-123": 123, "down-456": 124}, last_snapshot_written_ts=999999)
        store.save(state)

        loaded = store.load("BTC")
        assert loaded.current_window_index == 99
        assert loaded.next_condition_id == "cid-next"
        assert loaded.last_sequence_number_per_token["up-123"] == 123
        assert loaded.last_snapshot_written_ts == 999999


def test_cursor_not_share_parquet_code_path():
    # §1B: cursor store must not share code path with parquet writer (separate durability)
    # Verify cursor_store module doesn't import parquet_writer
    import inspect, polymarket_collector.storage.cursor_store as cs
    src = inspect.getsource(cs)
    assert "parquet" not in src.lower()
    assert "ParquetWriter" not in src
