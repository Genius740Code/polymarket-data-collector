"""ts_source is int64 epoch-ms (was epoch-ms/ISO as strings pre-2026-09-13).

Covers the producer coercion, schema types, old-file transition normalization,
and the writer date fallback — for crypto and weather (shared code path).
"""
import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.chainlink import chainlink_event_from_ws
from polymarket_collector.storage.export import _normalize_ts_source_int
from polymarket_collector.storage.parquet_writer import _date_str_from_ts_field
from polymarket_collector.storage.schemas import (
    BOOK_EVENTS_SCHEMA,
    CHAINLINK_SCHEMA,
    TRADES_SCHEMA,
)
from polymarket_collector.validation import coerce_ts_source_ms


def test_coerce_ts_source_ms():
    assert coerce_ts_source_ms("1789324212650") == 1789324212650
    assert coerce_ts_source_ms(1789324212650) == 1789324212650
    assert coerce_ts_source_ms(1789324212.65) == 1789324212650  # seconds.float
    assert coerce_ts_source_ms("2026-09-05T20:35:00Z") == 1788640500000  # RTDS ISO
    assert coerce_ts_source_ms(None) is None
    assert coerce_ts_source_ms("") is None
    assert coerce_ts_source_ms("garbage") is None
    assert coerce_ts_source_ms(True) is None


def test_schemas_ts_source_int64():
    for schema in (TRADES_SCHEMA, BOOK_EVENTS_SCHEMA, CHAINLINK_SCHEMA):
        assert pa.types.is_int64(schema.field("ts_source").type)


def test_chainlink_producer_iso_to_int_ms():
    ev = chainlink_event_from_ws({"timestamp": "2026-09-05T20:35:00Z", "price": 1.0}, "BTC")
    assert ev.ts_source == 1788640500000
    ev2 = chainlink_event_from_ws({"timestamp": 1789324212650, "price": 1.0}, "BTC")
    assert ev2.ts_source == 1789324212650


def test_normalize_numeric_strings_to_int():
    t = pa.table({"ts_source": ["1789324212650", None], "asset": ["BTC", "BTC"]})
    out = _normalize_ts_source_int(t)
    assert pa.types.is_int64(out.schema.field("ts_source").type)
    assert out.column("ts_source").to_pylist() == [1789324212650, None]


def test_normalize_leaves_iso_and_int_alone():
    iso = pa.table({"ts_source": ["2026-09-05T20:35:00Z"], "asset": ["BTC"]})
    assert _normalize_ts_source_int(iso).schema.field("ts_source").type == pa.string()
    ints = pa.table({"ts_source": pa.array([1789324212650], type=pa.int64())})
    assert _normalize_ts_source_int(ints) is ints
    nocol = pa.table({"asset": ["BTC"]})
    assert _normalize_ts_source_int(nocol) is nocol


def test_writer_date_fallback_int_and_iso():
    assert _date_str_from_ts_field(1789324212650) == "2026-09-13"
    assert _date_str_from_ts_field("2026-09-05T20:35:00Z") == "2026-09-05"
    assert _date_str_from_ts_field("1789324212650") == "2026-09-13"
    assert _date_str_from_ts_field(None) is None
    assert _date_str_from_ts_field("garbage") is None


def test_roundtrip_int_through_writer_schema(tmp_path):
    # int rows must satisfy the int64 schema (previously stringified by producers)
    rows = [{
        "ts_source": 1789324212650, "ts_received_ns": 1, "condition_id": "c1",
        "market_id": "1", "series_id": "BTC-5m", "window_index": 1, "asset": "BTC",
        "trade_id": "t1", "transaction_hash": None, "token_id": "tok", "outcome": "up",
        "price": 0.5, "size": 1.0, "notional": 0.5, "fee": None, "fee_is_estimated": None,
        "side": "buy", "aggressor_side": "buy", "maker_wallet": None,
        "taker_wallet": None, "wallet": None,
    }]
    tbl = pa.Table.from_pylist(rows, schema=TRADES_SCHEMA)
    p = tmp_path / "t.parquet"
    pq.write_table(tbl, str(p))
    assert pq.read_table(str(p)).schema.field("ts_source").type == pa.int64()
