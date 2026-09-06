"""Config — §0 assets are configurable, not hardcoded.

Loads `collector.example.yaml` shape via pydantic-settings; env var
`POLYMARKET_COLLECTOR_CONFIG` may point to a YAML file. Missing file falls
back to defaults matching the example.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Literal, Optional

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


class LiquidityFilterConfig(BaseModel):
    """Liquidity filtering — §0 before enabling asset + §3 volume/liquidity gate.
    No RPC — uses Gamma reported_volume/reported_liquidity only.
    If market's liquidity < min_liquidity OR volume < min_volume, skip collection
    for that window (emit low_liquidity event) and try next window's market.
    """
    enabled: bool = False  # off by default — collect all unless user opts in
    min_liquidity: float = 0.0  # e.g. 500 means require liquidityNum >= 500
    min_volume: float = 0.0  # e.g. 1000 means require volumeNum >= 1000
    min_spread_liquidity_check: bool = False  # if true, check spread via book snapshot too


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
    # Multi-timeframe: one dataset per TF, keyed by window label. Missing entries
    # fall back to dataset_prefix (5m) — the slugs per plan.md §1.1 are
    # gghgg1/polymarket-{5m,15m,1h,4h,1d}-crypto.
    datasets: Dict[str, str] = Field(default_factory=lambda: {
        "5m": "gghgg1/polymarket-5m-crypto",
        "15m": "gghgg1/polymarket-15m-crypto",
        "1h": "gghgg1/polymarket-1h-crypto",
        "4h": "gghgg1/polymarket-4h-crypto",
        "1d": "gghgg1/polymarket-1d-crypto",
    })
    # Rolling-window uploads: each hourly upload contains the trailing
    # local_retention_hours of data (staging is rebuilt from the local hive, so
    # it can only contain what is still local). When False (legacy cumulative
    # mode) local data is never pruned and the monotonic row-count check applies.
    rolling_window: bool = False
    local_retention_hours: int = 48  # leeway before local prune after verified upload


# ------------------------------------------------------------------ top-level
class CollectorConfig(BaseSettings):
    """Top-level collector config — §0 + all tuning knobs."""

    model_config = SettingsConfigDict(env_prefix="POLYMARKET_", extra="ignore")

    assets: List[str] = Field(default_factory=lambda: ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"])
    series_ids: Dict[str, str] = Field(default_factory=lambda: {
        "BTC": "BTC", "ETH": "ETH", "SOL": "SOL", "HYPE": "HYPE", "BNB": "BNB", "XRP": "XRP", "DOGE": "DOGE",
    })
    window_size_seconds: int = 300  # default 5-min; primary lane (see timeframes)
    # Multi-timeframe lanes collected by this process. Each label maps to a
    # window size via WINDOW_SIZES_SECONDS and gets its own rollover lane,
    # discovery cadence and Kaggle dataset. Keep ["5m"] for the proven 5m-only
    # behavior; enable more ONLY after verify-gate --probe-timeframes confirms
    # the Gamma series actually exists (plan.md §7 gate).
    timeframes: List[str] = Field(default_factory=lambda: ["5m"])
    schema_version: str = "3.2.0"

    rollover_lead_seconds: int = 30
    max_coverage_gap_seconds: int = 5
    discovery_poll_interval_seconds: int = 2
    discovery_backoff_max_seconds: int = 8

    snapshot_interval_ms: int = 500
    l2_levels: int = 10
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
    liquidity_filter: LiquidityFilterConfig = Field(default_factory=LiquidityFilterConfig)
    synthetic_mode: bool = False  # DEPRECATED: synthetic data permanently disabled - always False, kept for backward compat

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

    def series_id_for(self, asset: str, window_label: str | None = None) -> str:
        if window_label is None:
            ws = self.window_size_seconds
            window_label = self.window_label_for(ws)
        return f"{asset.upper()}-{window_label}"

    # -- multi-timeframe helpers --------------------------------------------
    WINDOW_SIZES_SECONDS: ClassVar[Dict[str, int]] = {
        "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400,
    }

    @classmethod
    def window_label_for(cls, window_size_seconds: int) -> str:
        if window_size_seconds >= 86400:
            return "1d"
        if window_size_seconds >= 14400:
            return "4h"
        if window_size_seconds >= 3600:
            return "1h"
        if window_size_seconds >= 900:
            return "15m"
        return "5m"

    @classmethod
    def window_size_for(cls, window_label: str) -> int:
        try:
            return cls.WINDOW_SIZES_SECONDS[str(window_label).lower()]
        except KeyError:
            raise ValueError(
                f"Unknown timeframe '{window_label}' — expected one of {sorted(cls.WINDOW_SIZES_SECONDS)}"
            )

    @field_validator("timeframes")
    @classmethod
    def timeframes_valid(cls, v: List[str]) -> List[str]:
        labels = []
        for tf in v:
            tf_l = str(tf).lower()
            if tf_l not in cls.WINDOW_SIZES_SECONDS:
                raise ValueError(f"Unknown timeframe '{tf}' — expected one of {sorted(cls.WINDOW_SIZES_SECONDS)}")
            if tf_l not in labels:
                labels.append(tf_l)
        if not labels:
            labels = ["5m"]
        return labels

    def timeframe_window_sizes(self) -> Dict[str, int]:
        """Enabled lanes: label -> window size seconds (ordered, deduped)."""
        return {tf: self.window_size_for(tf) for tf in self.timeframes}

    def discovery_poll_interval_for(self, window_size_seconds: int) -> int:
        """Scaled discovery cadence — longer windows don't need 2s polling.

        Keeps total Gamma load flat as timeframes are added: 7 assets ×
        (2s + 5s + 15s + 30s + 60s cadences) ≈ 6 req/s, well under plan.md's
        429 threshold, vs 17.5 req/s if every lane polled at 2s.
        """
        base = self.discovery_poll_interval_seconds
        return {300: base, 900: max(5, base), 3600: max(15, base),
                14400: max(30, base), 86400: max(60, base)}.get(int(window_size_seconds), base)

    def rollover_lead_for(self, window_size_seconds: int) -> int:
        """Lookahead lead per lane: never below the configured base, scaled with
        window width (Gamma can index a fresh slug late; for a 1d window a 120s
        lead is disproportionate — allow generous lead without tight-looping)."""
        return max(self.rollover_lead_seconds, int(window_size_seconds) // 10)

    def kaggle_dataset_for(self, window_label: str) -> str:
        """Dataset slug for a timeframe; falls back to the 5m prefix."""
        try:
            return self.kaggle.datasets[window_label]
        except (KeyError, AttributeError):
            return self.kaggle.dataset_prefix

    def validate_assets_have_series(self) -> None:
        missing = [a for a in self.assets if a not in self.series_ids]
        if missing:
            raise ValueError(f"Missing series_ids for assets: {missing} — §0 requires explicit series_id")
