"""Per-tick underlying alignment (3.4.0) — snapshots carry the nearest
previous chainlink tick (previous-only, tolerance-gated, NULLs on gap)."""
from pathlib import Path

import pyarrow.parquet as pq

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.storage.parquet_writer import ParquetWriter
from polymarket_collector.storage.schemas import snapshot_schema


def _cfg(tmp):
    return CollectorConfig(assets=["BTC", "ETH"],
                           storage={"data_dir": str(tmp)},
                           cursor_store={"path": str(Path(tmp) / "cursor_state")},
                           timeframes=["5m"])


def _book():
    return OrderBookState(asset="BTC", condition_id="0xabc", market_id="1",
                          series_id="BTC", window_index=1,
                          up_token_id="up", down_token_id="down",
                          market_end_ts_ms=1_800_000_000_000)


def test_schema_has_nullable_underlying_cols():
    schema = snapshot_schema(10)
    for col in ("underlying_price", "underlying_ts_ns", "underlying_age_ms"):
        assert col in schema.names
        assert schema.field(col).nullable is True
    assert schema.field("underlying_price").type == schema.field("up_bid").type


def test_snapshot_flat_dict_defaults_to_null_underlying():
    row = _book().snapshot(ts_ms=1_700_000_000_000).to_flat_dict()
    assert row["underlying_price"] is None
    assert row["underlying_ts_ns"] is None
    assert row["underlying_age_ms"] is None


def test_stamp_uses_previous_tick_only(tmp_path):
    c = Collector(_cfg(tmp_path))
    bucket = 1_700_000_000_000
    c._note_chainlink_event({"price": 97_000.0}, "BTC", bucket - 500)
    c._note_chainlink_event({"price": 99_000.0}, "BTC", bucket + 500)  # future — not knowable
    row = _book().snapshot(ts_ms=bucket).to_flat_dict()
    c._stamp_underlying(row, bucket, "BTC")
    assert row["underlying_price"] == 97_000.0
    assert row["underlying_ts_ns"] == (bucket - 500) * 1_000_000
    assert row["underlying_age_ms"] == 500


def test_stamp_null_when_tick_too_old_or_missing(tmp_path):
    c = Collector(_cfg(tmp_path))
    bucket = 1_700_000_000_000
    c._note_chainlink_event({"price": 97_000.0}, "BTC", bucket - 5_000)  # beyond 2000ms tolerance
    row = _book().snapshot(ts_ms=bucket).to_flat_dict()
    c._stamp_underlying(row, bucket, "BTC")
    assert row["underlying_price"] is None
    assert row["underlying_ts_ns"] is None
    assert row["underlying_age_ms"] is None
    # empty store likewise → NULLs, never raises
    c2 = Collector(_cfg(tmp_path))
    row2 = _book().snapshot(ts_ms=bucket).to_flat_dict()
    c2._stamp_underlying(row2, bucket, "BTC")
    assert row2["underlying_price"] is None


def test_stamp_asset_isolated_and_rejects_null_price(tmp_path):
    c = Collector(_cfg(tmp_path))
    bucket = 1_700_000_000_000
    c._note_chainlink_event({"price": 3_000.0}, "ETH", bucket - 200)
    c._note_chainlink_event({"price": None}, "BTC", bucket - 100)  # priceless tick is not a tick
    row = _book().snapshot(ts_ms=bucket).to_flat_dict()
    c._stamp_underlying(row, bucket, "BTC")
    assert row["underlying_price"] is None
    assert row["underlying_ts_ns"] is None


def test_stamp_memoized_per_asset_bucket(tmp_path):
    c = Collector(_cfg(tmp_path))
    bucket = 1_700_000_000_000
    c._note_chainlink_event({"price": 97_000.0}, "BTC", bucket - 500)
    r1 = _book().snapshot(ts_ms=bucket).to_flat_dict()
    c._stamp_underlying(r1, bucket, "BTC")
    assert ("BTC", bucket) in c._underlying_cache
    r2 = _book().snapshot(ts_ms=bucket).to_flat_dict()
    c._stamp_underlying(r2, bucket, "BTC")
    assert (r2["underlying_price"], r2["underlying_ts_ns"], r2["underlying_age_ms"]) == \
           (r1["underlying_price"], r1["underlying_ts_ns"], r1["underlying_age_ms"])


def test_writer_roundtrip_preserves_underlying(tmp_path):
    c = Collector(_cfg(tmp_path))
    bucket = 1_700_000_000_000
    c._note_chainlink_event({"price": 97_123.5}, "BTC", bucket - 250)
    row = _book().snapshot(ts_ms=bucket).to_flat_dict()
    c._stamp_underlying(row, bucket, "BTC")
    w = ParquetWriter(data_dir=str(tmp_path), flush_interval_seconds=999,
                      flush_row_count_threshold=999, buffer_max_rows=100,
                      wal_enabled=False, l2_levels=10)
    assert w.append("book_snapshots_500ms", row, asset="BTC", date_str="2025-01-01") is True
    w.flush()
    parts = list(Path(tmp_path, "book_snapshots_500ms", "date=2025-01-01", "asset=BTC").glob("*.parquet"))
    assert parts, "no snapshot file flushed"
    tbl = pq.read_table(str(parts[0]))
    assert "underlying_price" in tbl.schema.names
    got = tbl.to_pylist()[0]
    assert got["underlying_price"] == 97_123.5
    assert got["underlying_age_ms"] == 250
