"""Tests for pmdata_diff: pure local parquet compare (no API calls)."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.pmdata_diff import (
    compare_pmdata,
    render_markdown,
    row_mid,
    write_report,
)

SLUG = "btc-updown-5m-1758758700"
T0 = 1_758_758_701_000


def _l2_row(ts, bid, ask, etype="book"):
    return {
        "market_slug": SLUG,
        "timestamp": ts,
        "local_timestamp": ts * 1_000_000,
        "event_type": etype,
        "ask_prices": [ask] if ask is not None else None,
        "ask_sizes": [10.0] if ask is not None else None,
        "bid_prices": [bid] if bid is not None else None,
        "bid_sizes": [12.0] if bid is not None else None,
        "condition_id": "0x" + "aa" * 32,
        "asset": "BTC",
    }


def _write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path))


def _ours_sample_dirs(tmp_path, n_ours=100, n_sample=100, drift=0.0,
                        ours_types=None, sample_types=None):
    ours = tmp_path / "ours"
    sample = tmp_path / "sample"
    etypes = ["book", "price_change", "last_trade_price", "tick_size_change"]
    ot = ours_types or etypes
    st = sample_types or etypes
    orows = [_l2_row(T0 + i * 500, 0.60, 0.62, ot[i % len(ot)]) for i in range(n_ours)]
    srows = [_l2_row(T0 + i * 500, 0.60 + drift, 0.62 + drift, st[i % len(st)])
             for i in range(n_sample)]
    _write(ours / "l2" / f"{SLUG}.parquet", orows)
    _write(sample / f"{SLUG}.parquet", srows)
    return ours, sample


def test_pass_on_identical_layouts(tmp_path):
    ours, sample = _ours_sample_dirs(tmp_path)
    rep = compare_pmdata(ours, sample, SLUG)
    assert rep["status"] == "PASS", rep["reasons"]
    assert rep["row_count"]["pass"] is True
    assert rep["row_count"]["diff_pct"] == 0.0
    assert rep["mid_price"]["median_diff_ticks"] == 0.0
    assert rep["mid_price"]["pass"] is True
    assert rep["coverage_pass"] is True
    md = render_markdown(rep)
    assert "PASS" in md and SLUG in md


def test_row_count_tolerance_flags_drift(tmp_path):
    ours, sample = _ours_sample_dirs(tmp_path, n_ours=90, n_sample=100)
    rep = compare_pmdata(ours, sample, SLUG)
    assert rep["row_count"]["diff_pct"] == -0.10
    assert rep["row_count"]["pass"] is False
    assert rep["status"] == "FAIL"


def test_row_count_within_tolerance_passes(tmp_path):
    ours, sample = _ours_sample_dirs(tmp_path, n_ours=99, n_sample=100)
    rep = compare_pmdata(ours, sample, SLUG)
    assert rep["row_count"]["pass"] is True


def test_mid_diff_in_ticks_flags_drift(tmp_path):
    ours, sample = _ours_sample_dirs(tmp_path, drift=0.02)  # 2 ticks at 0.01
    rep = compare_pmdata(ours, sample, SLUG)
    assert rep["mid_price"]["median_diff_ticks"] == pytest.approx(2.0)
    assert rep["mid_price"]["pass"] is False
    assert rep["status"] == "FAIL"


def test_coverage_missing_on_both_sides_fails(tmp_path):
    ours, sample = _ours_sample_dirs(tmp_path, ours_types=["book", "price_change"],
                                     sample_types=["book", "price_change"])
    # Strip our dirs down to book-only too.
    rep = compare_pmdata(ours, sample, SLUG)
    ours_types = set(rep["ours"]["event_types"])
    assert "book" in ours_types and "price_change" in ours_types
    assert rep["missing_required_both_sides"] == ["last_trade_price", "tick_size_change"]
    assert rep["coverage_pass"] is False


def test_coverage_union_passes_when_sample_has_them(tmp_path):
    ours_dir = tmp_path / "ours"
    sample_dir = tmp_path / "sample"
    _write(ours_dir / "l2" / f"{SLUG}.parquet",
           [_l2_row(T0 + i * 500, 0.60, 0.62, "snapshot") for i in range(8)])
    _write(sample_dir / f"{SLUG}.parquet",
           [_l2_row(T0 + i * 500, 0.60, 0.62,
                    ["book", "price_change", "last_trade_price", "tick_size_change"][i % 4])
            for i in range(8)])
    rep = compare_pmdata(ours_dir, sample_dir, SLUG)
    assert rep["missing_required_both_sides"] == []
    assert rep["coverage_pass"] is True


def test_null_side_is_gap_no_mid(tmp_path):
    assert row_mid(_l2_row(T0, None, 0.62)) is None
    assert row_mid(_l2_row(T0, 0.60, None)) is None
    assert row_mid(_l2_row(T0, 0.60, 0.62)) == 0.61


def test_report_written_to_file(tmp_path):
    ours, sample = _ours_sample_dirs(tmp_path)
    rep = compare_pmdata(ours, sample, SLUG)
    path = write_report(rep, tmp_path / "reports")
    assert path.exists() and path.name.startswith(f"pmdata_diff_{SLUG}")
    assert "PMData parity diff" in path.read_text(encoding="utf-8")
