"""Config — §0 assets are configurable, not hardcoded.

Loads `collector.example.yaml` shape via pydantic-settings; env var
`POLYMARKET_COLLECTOR_CONFIG` may point to a YAML file. Missing file falls
back to defaults matching the example.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# ------------------------------------------------------------------ sub-models
class EventThresholds(BaseModel):
    spread_change_threshold: float = 0.002
    size_change_threshold_pct: float = 0.10
    depth_change_threshold_pct: float = 0.15
    crossing_threshold: float = 0.0


class WsConfig(BaseModel):
    url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    rest_book_url: str = "https://clob.polymarket.com/book"
    rest_market_url: str = "https://clob.polymarket.com/markets"
    reconnect_backoff_initial_ms: int = 500
    reconnect_backoff_max_ms: int = 30000
    reconnect_jitter: bool = True
    resync_rest_backoff_initial_ms: int = 1000
    resync_rest_backoff_max_ms: int = 20000
    max_resync_duration_seconds: int = 60
    sequence_gap_detection: bool = True
    full_book_diff_interval_seconds: int = 45
    full_book_diff_tolerance: float = 0.0


class CursorStoreConfig(BaseModel):
    mode: Literal["per_asset", "shared_wal"] = "per_asset"
    path: str = "./data/cursor_state"
    flush_interval_seconds: int = 5


class ChainlinkConfig(BaseModel):
    ws_url: str = "wss://ws-live-data.polymarket.com"
    max_resolution_wait_seconds: int = 120
    settlement_source_preference: Literal["on_chain_confirmed", "inferred_nearest"] = "on_chain_confirmed"
    enable_binance_fallback: bool = False


class StorageConfig(BaseModel):
    data_dir: str = "./data"
    flush_interval_seconds: int = 60
    flush_row_count_threshold: int = 5000
    buffer_max_rows: int = 50000
    wal_enabled: bool = True
    wal_dir: str = "./data/_wal"
    disk_space_check_interval_seconds: int = 30
    disk_space_min_bytes: int = 1_073_741_824
    compaction_schedule: str = "daily"
    compaction_temp_suffix: str = ".tmp"


class RawArchiveConfig(BaseModel):
    enabled: bool = True
    retention_hours: int = 36
    path: str = "./data/raw_ws_archive"
    format: Literal["jsonl"] = "jsonl"


class ClockConfig(BaseModel):
    ntp_check_interval_seconds: int = 60
    clock_issue_threshold_ms: int = 50
    ntp_server: str = "pool.ntp.org"


class WatchdogConfig(BaseModel):
    heartbeat_interval_seconds: int = 5
    heartbeat_stale_seconds: int = 15
    alert_on: List[str] = Field(default_factory=lambda: [
        "ws_disconnected", "resync_failed", "coverage_gap", "rollover_miss",
        "backpressure", "write_failed", "clock_issue", "resolution_stuck", "book_anomaly",
    ])
    daily_summary_hour_utc: int = 0


class TestModeConfig(BaseModel):
    enabled: bool = False
    num_markets: int = 4  # number of 5-min windows to collect live (§1) — 4×5min = 20 min wall-clock (only 5m window, 1d too long)
    accelerate: bool = False  # ignored for real mode (always wall-clock); kept for CLI compat
    synthetic_seed: int | None = None
    window_size_seconds: int = 300  # override default 5-min window size (test only uses 5m)


class CapacityConfig(BaseModel):
    estimated_row_bytes_uncompressed: int = 2500
    parquet_compression_ratio: float = 0.25


class KaggleConfig(BaseModel):
    upload_interval_seconds: int = 3600  # hourly by default (prod)
    test_upload_interval_seconds: int = 600  # 10-min during test_mode (4 markets ×5m)
    username: str | None = None  # optional: override KAGGLE_USERNAME env
    key: str | None = None  # optional: override KAGGLE_KEY env
    dataset_prefix: str = "gghgg1/polymarket-5m-crypto"
    # per-plan.md §1.1 single 5m dataset (all assets share same slug, not per-asset suffix)
    # test_mode uploads every 10 min to same dataset, gated on closed markets only


# ------------------------------------------------------------------ top-level
class CollectorConfig(BaseSettings):
    """Top-level collector config — §0 + all tuning knobs."""

    model_config = SettingsConfigDict(env_prefix="POLYMARKET_", extra="ignore")

    assets: List[str] = Field(default_factory=lambda: ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"])
    series_ids: Dict[str, str] = Field(default_factory=lambda: {
        "BTC": "BTC", "ETH": "ETH", "SOL": "SOL", "HYPE": "HYPE", "BNB": "BNB", "XRP": "XRP", "DOGE": "DOGE",
    })
    window_size_seconds: int = 300  # default 5-min; 5m-only for test (1d too long, assume 5m validates others)
    schema_version: str = "3.2.0"

    rollover_lead_seconds: int = 30
    max_coverage_gap_seconds: int = 5
    discovery_poll_interval_seconds: int = 2
    discovery_backoff_max_seconds: int = 8

    snapshot_interval_ms: int = 500
    l2_levels: int = 20
    depth_thresholds_cents: List[int] = Field(default_factory=lambda: [1, 5, 10])

    event_thresholds: EventThresholds = Field(default_factory=EventThresholds)
    ws: WsConfig = Field(default_factory=WsConfig)
    cursor_store: CursorStoreConfig = Field(default_factory=CursorStoreConfig)
    chainlink: ChainlinkConfig = Field(default_factory=ChainlinkConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    raw_archive: RawArchiveConfig = Field(default_factory=RawArchiveConfig)
    clock: ClockConfig = Field(default_factory=ClockConfig)
    watchdog: WatchdogConfig = Field(default_factory=WatchdogConfig)
    capacity: CapacityConfig = Field(default_factory=CapacityConfig)
    kaggle: KaggleConfig = Field(default_factory=KaggleConfig)
    test_mode: TestModeConfig = Field(default_factory=TestModeConfig)
    synthetic_mode: bool = False  # when True, allows fallback synthetic data; default off for prod safety

    # optional overrides for tests
    _config_path: Optional[str] = None

    @field_validator("assets")
    @classmethod
    def assets_upper(cls, v: List[str]) -> List[str]:
        return [a.upper() for a in v]

    @field_validator("l2_levels")
    @classmethod
    def l2_range(cls, v: int) -> int:
        if not 10 <= v <= 20:
            raise ValueError("l2_levels must be 10..20 per §3")
        return v

    # -- YAML loader helper -------------------------------------------------
    @classmethod
    def from_yaml(cls, path: str | Path) -> "CollectorConfig":
        p = Path(path)
        if not p.exists():
            return cls()
        data: Dict[str, Any] = yaml.safe_load(p.read_text()) or {}
        return cls(**data)

    @classmethod
    def load(cls, explicit_path: str | Path | None = None) -> "CollectorConfig":
        """Resolution order: explicit_path > $POLYMARKET_COLLECTOR_CONFIG > ./config/collector.yaml > defaults."""
        candidates: List[Path] = []
        if explicit_path is not None:
            candidates.append(Path(explicit_path))
        env = os.environ.get("POLYMARKET_COLLECTOR_CONFIG")
        if env:
            candidates.append(Path(env))
        candidates.append(Path("config/collector.yaml"))
        candidates.append(Path("config/collector.example.yaml"))
        for c in candidates:
            if c.exists():
                return cls.from_yaml(c)
        return cls()

    def series_id_for(self, asset: str) -> str:
        ws = self.window_size_seconds
        if ws >= 86400:
            window_label = "1d"
        elif ws >= 14400:
            window_label = "4h"
        elif ws >= 3600:
            window_label = "1h"
        elif ws >= 900:
            window_label = "15m"
        else:
            window_label = "5m"
        return f"{asset.upper()}-{window_label}"

    def validate_assets_have_series(self) -> None:
        missing = [a for a in self.assets if a not in self.series_ids]
        if missing:
            raise ValueError(f"Missing series_ids for assets: {missing} — §0 requires explicit series_id")
