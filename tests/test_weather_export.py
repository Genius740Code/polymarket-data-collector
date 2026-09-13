"""Weather lane export: city-asset rows (series WEATHER-HIGH/LOW-1D) must survive
the `{asset}-{tf}` lane filter on both the legacy Table path and the streaming
path. Regression: the filter used to drop every weather snapshot, so staging
came out empty and the hourly Kaggle upload aborted forever (datasets 404)."""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.export import (
    _default_staging_datasets,
    _is_weather_upload,
    _kaggle_title,
    _lane_series_mask,
    _load_market_id_map,
    _read_dataset_per_asset,
    _staging_flavor,
    _stream_export_asset_dataset,
    _verify_staging_row_counts,
)
from polymarket_collector.storage.schemas import snapshot_schema


def _wsnap(i, ts, city="HONG-KONG", series="WEATHER-HIGH-1D", cid="0xaaa"):
    base = {
        "ts_snapshot_utc": "2026-09-13T00:00:00.000Z", "ts_snapshot_ns": ts,
        "condition_id": cid, "market_id": "9", "series_id": series,
        "window_index": 1, "asset": city, "snapshot_id": f"w{i}",
        "up_token_id": "u", "down_token_id": "d",
        "up_bid": 0.5, "up_ask": 0.6, "up_bid_size": 1.0, "up_ask_size": 1.0,
        "down_bid": 0.4, "down_ask": 0.5, "down_bid_size": 1.0, "down_ask_size": 1.0,
        "market_time_remaining_ms": 100, "up_book_age_ms": 5, "down_book_age_ms": 5,
        "is_rollover_window": False, "book_state": "live", "resync_id": None,
        "book_crossed": False, "up_book_hash": None, "down_book_hash": None,
    }
    for o in ("up", "down"):
        for s in ("bid", "ask"):
            for lvl in range(1, 11):
                base[f"{o}_{s}_level_{lvl}_price"] = 0.5
                base[f"{o}_{s}_level_{lvl}_size"] = 1.0
            for th in (1, 5, 10):
                base[f"{o}_{s}_depth_{th}c"] = 5.0
    return base


def _whive(tmp_path: Path):
    base = tmp_path / "data"
    d = base / "book_snapshots_500ms" / "date=2026-09-13" / "asset=HONG-KONG"
    d.mkdir(parents=True)
    ss = snapshot_schema(10)
    pq.write_table(pa.Table.from_pylist([
        _wsnap(1, 100), _wsnap(2, 200), _wsnap(3, 300, series="WEATHER-LOW-1D"),
    ], schema=ss), str(d / "a.parquet"))
    ml = base / "markets_latest"
    ml.mkdir(parents=True)
    pq.write_table(
        pa.table({"condition_id": ["0xaaa"], "market_id": ["42"]}),
        str(ml / "markets_latest.parquet"))
    return base


def test_lane_mask_passes_weather_series():
    col = pa.array(["WEATHER-HIGH-1D", "WEATHER-LOW-1D", "HONG-KONG-1d"])
    mask = _lane_series_mask(col, "HONG-KONG-1d")
    assert mask.to_pylist() == [True, True, True]


def test_lane_mask_keeps_crypto_lane_pure():
    col = pa.array(["BTC-5m", "BTC-15m"])
    mask = _lane_series_mask(col, "BTC-5m")
    assert mask.to_pylist() == [True, False]


def test_weather_table_path_keeps_rows(tmp_path):
    base = _whive(tmp_path)
    t = _read_dataset_per_asset(base, "book_snapshots_500ms", "HONG-KONG", timeframe_label="1d")
    assert t is not None and t.num_rows == 3


def test_kaggle_title_weather_and_crypto():
    assert _kaggle_title("gghgg1/polymarket-weather-high", "1d") == "Polymarket Weather High 1D"
    assert _kaggle_title("gghgg1/polymarket-weather-low", "1d") == "Polymarket Weather Low 1D"
    assert _kaggle_title("gghgg1/polymarket-5m-crypto", "5m") == "Polymarket 5m Crypto"
    assert _is_weather_upload("gghgg1/polymarket-weather-high")
    assert _is_weather_upload("./data-weather-low")
    assert not _is_weather_upload("gghgg1/polymarket-5m-crypto")
    assert _staging_flavor("gghgg1/polymarket-weather-high", "1d") == "weather high 1d"
    assert _staging_flavor("gghgg1/polymarket-weather-low", "1d") == "weather low 1d"
    assert _staging_flavor("gghgg1/polymarket-5m-crypto", "5m") == "5m crypto"


def test_weather_default_datasets_skip_chainlink():
    ds = _default_staging_datasets("./data-weather-high")
    assert "chainlink_events" not in ds
    assert "book_snapshots_500ms" in ds and "markets_summary" in ds
    ds_crypto = _default_staging_datasets("./data")
    assert "chainlink_events" in ds_crypto


def _weather_staging(tmp_path: Path, slug: str):
    staging = tmp_path / "kaggle_staging" / "1d" / "gghgg1" / slug
    staging.mkdir(parents=True)
    ss = snapshot_schema(10)
    pq.write_table(
        pa.Table.from_pylist([_wsnap(1, 100)], schema=ss),
        str(staging / "HONG-KONG_book_snapshots_500ms.parquet"),
    )
    for name in ("HONG-KONG_book_snapshots_clean.parquet", "HONG-KONG_book_events.parquet",
                 "HONG-KONG_trades.parquet", "markets.parquet", "collector_events.parquet",
                 "resync_episodes.parquet", "markets_summary.parquet"):
        pq.write_table(pa.table({"x": []}), str(staging / name))
    return staging


def test_verify_weather_staging_without_chainlink(tmp_path):
    staging = _weather_staging(tmp_path, "polymarket-weather-high")
    assert list(staging.glob("*_chainlink_events.parquet")) == []
    assert _verify_staging_row_counts(staging, ["HONG-KONG"], check_monotonic=False) is True


def test_verify_crypto_staging_still_requires_chainlink(tmp_path):
    staging = _weather_staging(tmp_path, "polymarket-5m-crypto")
    assert _verify_staging_row_counts(staging, ["HONG-KONG"], check_monotonic=False) is False


def test_weather_stream_path_matches_table(tmp_path):
    base = _whive(tmp_path)
    legacy = _read_dataset_per_asset(
        base, "book_snapshots_500ms", "HONG-KONG", timeframe_label="1d")
    out = tmp_path / "HONG-KONG_book_snapshots_500ms.parquet"
    tmp = tmp_path / "tmp.parquet.tmp"
    mmap = _load_market_id_map(base)
    n = _stream_export_asset_dataset(base, "book_snapshots_500ms", "HONG-KONG", tmp, "1d", 10, mmap)
    import os

    os.replace(str(tmp), str(out))
    streamed = pq.read_table(out)
    assert n == streamed.num_rows == legacy.num_rows == 3
