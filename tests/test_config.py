"""Tests for configurable assets — §0 not hardcoded."""
from polymarket_collector.config import CollectorConfig


def test_default_assets():
    cfg = CollectorConfig()
    assert cfg.assets == ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    assert len(cfg.assets) == 7
    assert cfg.test_mode.num_markets == 4  # 4×5m =20 min, 5m-only
    assert cfg.kaggle.test_upload_interval_seconds == 600
    assert cfg.kaggle.upload_interval_seconds == 3600
    assert cfg.window_size_seconds == 300


def test_add_fourth_asset_via_config():
    cfg = CollectorConfig(assets=["BTC", "ETH", "SOL", "AVAX"], series_ids={"BTC": "BTC-5MIN", "ETH": "ETH-5MIN", "SOL": "SOL-5MIN", "AVAX": "AVAX-5MIN"})
    assert "AVAX" in cfg.assets
    cfg.validate_assets_have_series()  # should not raise


def test_missing_series_id_raises():
    cfg = CollectorConfig(assets=["BTC", "FAKE"], series_ids={"BTC": "BTC-5MIN"})
    try:
        cfg.validate_assets_have_series()
        assert False, "should have raised"
    except ValueError as e:
        assert "FAKE" in str(e)


def test_assets_upper_normalized():
    cfg = CollectorConfig(assets=["btc", "eth"])
    assert cfg.assets == ["BTC", "ETH"]


def test_from_yaml_tmp(tmp_path):
    p = tmp_path / "cfg.yaml"
    p.write_text("assets: [BTC, ETH]\nrollover_lead_seconds: 45\n")
    cfg = CollectorConfig.from_yaml(p)
    assert cfg.assets == ["BTC", "ETH"]
    assert cfg.rollover_lead_seconds == 45

def test_snapshot_interval_ms():
    cfg = CollectorConfig()
    assert cfg.snapshot_interval_ms == 500
    assert cfg.l2_levels == 10
