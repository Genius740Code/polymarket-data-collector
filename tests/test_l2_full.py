"""l2_full opt-in: default 10-level behavior unchanged, full = 100 columns."""
import time

import pyarrow.parquet as pq

from polymarket_collector.book import FULL_DEPTH_LEVELS, OrderBookState
from polymarket_collector.config import CollectorConfig
from polymarket_collector.storage.parquet_writer import ParquetWriter
from polymarket_collector.storage.schemas import snapshot_schema

CID = "0x" + "ab" * 32
NARROW_COLS = 4 * 10 * 2  # 4 sides x 10 levels x (price+size)
FULL_COLS = 4 * FULL_DEPTH_LEVELS * 2


def _book(**kw):
    args = dict(
        asset="BTC", condition_id=CID, market_id="mid-1", series_id="BTC-5m",
        window_index=7, up_token_id="up-1", down_token_id="dn-1",
        market_end_ts_ms=int(time.time() * 1000) + 300_000, l2_levels=10,
    )
    args.update(kw)
    return OrderBookState(**args)


def _fill(b, n=30):
    bids = [[round(0.90 - i * 0.001, 6), 10.0 + i] for i in range(n)]
    asks = [[round(0.92 + i * 0.001, 6), 5.0 + i] for i in range(n)]
    b.replace_from_rest_snapshot({
        "up_bids": bids, "up_asks": asks,
        "down_bids": bids, "down_asks": asks,
    })


def test_full_depth_levels_constant():
    assert FULL_DEPTH_LEVELS == 100


def test_defaults_unchanged():
    cfg = CollectorConfig()
    assert cfg.l2_full is False
    assert cfg.l2_levels == 10
    book = _book()
    assert book.l2_full is False
    snap = book.snapshot().to_flat_dict()
    l2_keys = [k for k in snap if "_level_" in k]
    assert len(l2_keys) == NARROW_COLS
    assert "up_bid_level_10_price" in snap
    assert "up_bid_level_11_price" not in snap


def test_narrow_truncates_ram_depth():
    book = _book()
    _fill(book)
    snap = book.snapshot().to_flat_dict()
    l2_keys = [k for k in snap if "_level_" in k]
    assert len(l2_keys) == NARROW_COLS
    assert "up_bid_level_11_price" not in snap
    # top-of-book + depths still computed over full RAM depth
    assert snap["up_bid"] == 0.90
    assert snap["up_bid_depth_1c"] is not None


def test_full_keeps_ram_depth():
    book = _book(l2_full=True)
    _fill(book)
    snap = book.snapshot().to_flat_dict()
    l2_keys = [k for k in snap if "_level_" in k]
    assert len(l2_keys) == FULL_COLS
    assert len(snapshot_schema(10).names) < len(snapshot_schema(FULL_DEPTH_LEVELS).names)
    assert len(snapshot_schema(FULL_DEPTH_LEVELS).names) - len(snapshot_schema(10).names) == (FULL_COLS - NARROW_COLS)
    # 30 real levels present, tail NULL (never 0)
    assert abs(snap["up_bid_level_11_price"] - 0.89) < 1e-9
    assert abs(snap["up_ask_level_30_price"] - (0.92 + 29 * 0.001)) < 1e-9
    assert snap["up_bid_level_31_price"] is None
    assert snap["down_ask_level_100_size"] is None


def test_config_opt_in_parses(tmp_path):
    cfg = CollectorConfig(**{"l2_full": True})
    assert cfg.l2_full is True
    p = tmp_path / "c.yaml"
    p.write_text("l2_full: true\nl2_levels: 10\n")
    assert CollectorConfig.from_yaml(p).l2_full is True


def _row(full):
    book = _book(l2_full=full)
    _fill(book)
    return book.snapshot().to_flat_dict()


def _read_rows(data_dir):
    files = sorted((data_dir / "book_snapshots_500ms").rglob("*.parquet"))
    assert files, "expected a flushed snapshot file"
    return pq.read_table(str(files[0]))


def test_writer_default_narrow_roundtrip(tmp_path):
    data = tmp_path / "data"
    w = ParquetWriter(data_dir=data, wal_enabled=False, l2_levels=10)
    assert w.l2_full is False
    assert w.append("book_snapshots_500ms", _row(False), asset="BTC") is True
    assert w.flush() == 1
    t = _read_rows(data)
    assert t.num_rows == 1
    assert "up_bid_level_10_price" in t.schema.names
    w.close()


def test_writer_full_roundtrip_no_column_loss(tmp_path):
    data = tmp_path / "data"
    w = ParquetWriter(data_dir=data, wal_enabled=False, l2_levels=10, l2_full=True)
    assert w.append("book_snapshots_500ms", _row(True), asset="BTC") is True
    assert w.flush() == 1
    t = _read_rows(data)
    assert t.num_rows == 1
    assert "up_bid_level_11_price" in t.schema.names
    assert f"up_bid_level_{FULL_DEPTH_LEVELS}_price" in t.schema.names
    col = t.column("up_bid_level_11_price").to_pylist()[0]
    assert abs(col - 0.89) < 1e-9
    assert t.column("up_bid_level_31_price").to_pylist()[0] is None
    w.close()


def test_writer_group_schema_infers_depth(tmp_path):
    w = ParquetWriter(data_dir=tmp_path / "d1", wal_enabled=False, l2_full=True)
    row = _row(True)
    # strip to 25 levels -> schema must widen to exactly 25, not 100
    keep = {}
    for k, v in row.items():
        if "_level_" in k:
            try:
                n = int(k.rsplit("_level_", 1)[1].split("_")[0])
            except Exception:
                continue
            if n > 25:
                continue
        keep[k] = v
    schema = w._snapshot_group_schema([keep])
    assert schema.names == snapshot_schema(25).names
    assert w._snapshot_group_schema([row]).names == snapshot_schema(100).names
    narrow = ParquetWriter(data_dir=tmp_path / "d2", wal_enabled=False)
    assert narrow._snapshot_group_schema([row]).names == snapshot_schema(10).names
