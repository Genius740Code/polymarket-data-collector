"""Streaming export module: per-file batches, footer ordering, narrow dedup."""
import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.streaming import (
    DedupState,
    iter_source_files,
    stream_batches,
    write_batches,
)


def _tbl(rows, schema=None):
    return pa.Table.from_pylist(rows, schema=schema)


def test_iter_orders_by_footer_ts(tmp_path):
    d = tmp_path / "ds" / "date=2026-09-10" / "asset=BTC"
    d.mkdir(parents=True)
    # write out of order: partB (older ts) second
    pq.write_table(_tbl([{"ts": 200, "v": "b"}]), str(d / "partB.parquet"))
    pq.write_table(_tbl([{"ts": 100, "v": "a"}]), str(d / "partA.parquet"))
    files = iter_source_files(tmp_path, "ds", asset=None, ts_col="ts")
    assert [p.name for p in files] == ["partA.parquet", "partB.parquet"]
    got = [r["v"] for t in stream_batches(tmp_path, "ds", ts_col="ts") for r in t.to_pylist()]
    assert got == ["a", "b"]


def test_dedup_state_narrow(tmp_path):
    ds = DedupState(["id"])
    t1 = _tbl([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
    t2 = _tbl([{"id": 2, "v": "b2"}, {"id": 3, "v": "c"}])
    assert ds.filter(t1).num_rows == 2
    out = ds.filter(t2)
    assert out.num_rows == 1 and out.to_pylist()[0]["id"] == 3
    assert ds.dupes == 1 and ds.kept == 3


def test_write_batches_fixed_schema(tmp_path):
    out = tmp_path / "out.parquet"
    schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
    batches = [_tbl([{"a": 1, "b": "x", "extra": 9.0}]), _tbl([{"a": 2}])]
    n = write_batches(iter(batches), out, schema=schema)
    assert n == 2
    got = pq.read_table(out)
    # writer relaxes nullable flags (values identical); compare names + values
    assert got.schema.names == schema.names
    assert got.column("a").to_pylist() == [1, 2]
    assert got.column("b").to_pylist() == ["x", None]
