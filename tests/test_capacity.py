"""Tests for §11A capacity planning."""
from polymarket_collector.capacity import estimate_from_schema, CapacityEstimate


def test_estimate_fields():
    est = estimate_from_schema(l2_levels=20)
    # snapshot schema should have ~ 11 id + 2 ts + 8 top + 160 L2 + 12 depth + 6 state = ~199 fields?
    assert est.fields_per_snapshot > 100
    assert est.fields_per_snapshot < 300


def test_daily_bytes():
    est = CapacityEstimate(bytes_per_row_uncompressed=2500, parquet_compression_ratio=0.25, assets=3)
    d = est.snapshot_daily_bytes()
    assert d["rows"] == 172800 * 3
    assert d["compressed_bytes"] == int(d["uncompressed_bytes"] * 0.25)
    total = est.total_daily_compressed()
    assert total > d["compressed_bytes"]
    md = est.to_dict()
    assert md["total_daily_compressed_mb"] > 0
    assert md["weekly_mb"] == round(md["total_daily_compressed_mb"] * 7, 2)
