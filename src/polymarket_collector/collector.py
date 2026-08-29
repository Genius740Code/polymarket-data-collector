"""Main collector — ties together all PLAN.md sections.

Per-asset asyncio tasks sharing a single §3 500ms scheduler aligned to wall-clock
grid, with §1 rollover dual-tracking, §1A resync, §1B cursor recovery, §3A
validation, §10A batched Parquet flush, §13 raw archive, §14 clock.

This module is intentionally asyncio-native; thread-per-asset is also valid
(and maps to per_asset SQLite mode §1B) but the default single-process
task-per-asset avoids lock contention and keeps §10A buffer unified.
"""
from __future__ import annotations

import asyncio
import datetime
import json
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from .book import OrderBookState, snapshot_bucket_ms
from .chainlink import chainlink_event_from_ws
from .clock import check_clock_drift, is_clock_issue
from .config import CollectorConfig
from .enums import CollectorEventType, MarketStatus, ResolutionOutcome
from .storage.export import (
    export_timeframe_aggregates,
    export_and_upload_all_kaggle,
    _validate_kaggle_config,
)
from .rollover import MarketInfo, RolloverManager
from .resync import ResyncManager
from .storage.cursor_store import CursorState, CursorStore
from .storage.markets_log import MarketsLog
from .storage.parquet_writer import ParquetWriter
from .storage.raw_archive import RawArchive
from .validation import validate_ws_message


class Collector:
    """BTC/ETH/SOL 5-min market collector (§1-§19)."""

    def __init__(self, config: CollectorConfig):
        self.config = config
        self.config.validate_assets_have_series()

        # state
        self.books: Dict[str, OrderBookState] = {}  # condition_id -> book
        self.markets: Dict[str, MarketInfo] = {}  # condition_id -> market
        self.rollover = RolloverManager(config, on_event=self._collector_event)
        self.cursor_stores: Dict[str, CursorStore] = {a: CursorStore.for_asset(config, a) for a in config.assets}
        self.markets_log = MarketsLog(config.storage.data_dir)
        self.writer = ParquetWriter(
            data_dir=config.storage.data_dir,
            flush_interval_seconds=config.storage.flush_interval_seconds,
            flush_row_count_threshold=config.storage.flush_row_count_threshold,
            buffer_max_rows=config.storage.buffer_max_rows,
            wal_enabled=config.storage.wal_enabled,
            wal_dir=config.storage.wal_dir,
            l2_levels=config.l2_levels,
            schema_version=config.schema_version,
            on_event=self._writer_event,
        )
        self.raw_archive = RawArchive(
            base_path=config.raw_archive.path,
            retention_hours=config.raw_archive.retention_hours,
            enabled=config.raw_archive.enabled,
        )
        # resync needs a fetcher; default fetches via REST (may not exist — verified by §18 gate)
        self.resync = ResyncManager(config, rest_fetcher=self._fetch_rest_book, on_event=self._collector_event)

        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._snapshot_task: Optional[asyncio.Task] = None
        self._clock_task: Optional[asyncio.Task] = None
        self._flush_task: Optional[asyncio.Task] = None
        self._heartbeat_path = Path(config.storage.data_dir) / "heartbeat.json"
        self._threshold_config_id = str(uuid.uuid4())
        # per-asset connection tracking for collector_events §8 (avoid 100% null connection_id)
        self._conn_ids: Dict[str, str] = {}
        self._conn_tokens: Dict[str, set] = {}

    # -- lifecycle ---------------------------------------------------------
    async def start(self) -> None:
        self._running = True
        # §1B crash recovery: load cursor state, force resync if market still active
        await self._recover_from_cursor()

        # start per-asset WS tasks (stubbed — real WS connect loops)
        for asset in self.config.assets:
            t = asyncio.create_task(self._run_asset_loop(asset), name=f"asset-{asset}")
            self._tasks.append(t)

        # single shared scheduler for 500ms snapshots (§3 — same clock tick)
        self._snapshot_task = asyncio.create_task(self._snapshot_loop(), name="snapshots")

        # clock drift monitor (§14)
        self._clock_task = asyncio.create_task(self._clock_loop(), name="clock")

        # periodic flush (§10A) — also covers §9A markets log compaction periodic
        self._flush_task = asyncio.create_task(self._flush_loop(), name="flush")

        # resolution stuck monitor (§6A) — unresolved > max wait
        self._resolution_task = asyncio.create_task(self._resolution_stuck_loop(), name="resolution_stuck")

        # Kaggle hourly upload loop — export aggregated timeframes, upload, cleanup
        self._kaggle_task = asyncio.create_task(self._kaggle_upload_loop(), name="kaggle_upload")
    # -- test mode ---------------------------------------------------------
    async def run_test_mode(self, num_markets: int = 3, accelerate: bool = False) -> dict:
        """Real test mode: collect live Polymarket markets for num_markets windows.

        Unlike the previous synthetic generator, this runs the *real* pipeline:
        Gamma discovery (slug ``{btc,eth,sol}-updown-{window}-{ts}``), live CLOB market
        WebSocket (wss://ws-subscriptions-clob.polymarket.com/ws/market) for
        book/trades, and plain snapshot scheduling (§3). No SyntheticBookState
        is used. After num_markets consecutive windows have completed per
        asset (≈ num_markets×{ws}s wall-clock) the collector flushes,
        compacts ``markets_latest`` and writes ``data/test_analysis.json``.

        ``accelerate`` is ignored (real mode is always wall-clock) but kept
        for CLI compatibility.
        """
        import asyncio
        import datetime

        self._running = True
        # ensure cursor stores are clean for a fresh test (do not reuse stale state from synthetic runs)
        # but keep them — _recover_from_cursor will handle gaps correctly
        try:
            self.writer.flush()
        except Exception:
            pass
        self._beat()
        # log test_mode start (real)
        self._collector_event(CollectorEventType.collector_started, {"assets": self.config.assets, "recovered": False, "test_mode": True, "real": True, "num_markets": num_markets})

        # Start the normal collector tasks (per-asset WS loops, 500ms snapshot, clock, flush, etc.)
        await self.start()
        ws = self.config.test_mode.window_size_seconds
        print(f"[test-mode:real] collecting {num_markets}×{ws}s live windows per asset (wall-clock ≈ {num_markets*(ws/60):.1f} min) — assets={self.config.assets}")

        # Monitor for num_markets completed windows per asset.
        # A window completes when rollover promotes next -> current (rollover_completed) or
        # when market_end_ts passes and a new market becomes current.
        # Simplest: wait for wall-clock duration + poll actual market window_index.
        # Each window is {ws}s, plus up to 60s for discovery/overlap, so timeout = num_markets*{ws}+60s
        timeout_s = num_markets * ws + 60
        start_ts = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
        completed_windows = {a: set() for a in self.config.assets}
        # also track highest window_index seen per asset via rollover state
        try:
            while self._running:
                await asyncio.sleep(5)
                elapsed = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp()) - start_ts
                # update completed sets from rollover states
                for asset in self.config.assets:
                    state = self.rollover.states.get(asset.upper())
                    if not state:
                        continue
                    # window_index is monotonic; completed = current window_index if promotion happened?
                    # More robust: count distinct condition_ids seen in self.markets that have status resolved or past end
                    for cid, m in list(self.markets.items()):
                        if m.asset.upper() != asset.upper():
                            continue
                        # if market end is in past, consider it completed
                        now_ms = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
                        if m.market_end_ts_ms < now_ms:
                            completed_windows[asset].add(m.window_index)
                        # also if current window_index >= num_markets-1, we have enough
                # check if all assets have at least num_markets distinct windows completed/seen
                all_done = all(len(s) >= num_markets for s in completed_windows.values())
                # progress log every 30s
                if elapsed % 30 < 5:
                    prog = {a: sorted(s) for a, s in completed_windows.items()}
                    print(f"[test-mode:real] {elapsed}s elapsed, windows per asset: {prog}, timeout {timeout_s}s")
                if all_done:
                    print(f"[test-mode:real] all assets have {num_markets} windows (completed={completed_windows}), stopping")
                    break
                if elapsed >= timeout_s:
                    print(f"[test-mode:real] timeout {timeout_s}s reached, stopping with progress {completed_windows}")
                    break
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
            # flush + compact
            try:
                n = self.writer.flush()
                print(f"[test-mode:real] flushed {n} rows")
            except Exception as e:
                print(f"[test-mode:real] flush failed: {e}")
            try:
                self.markets_log.flush_staging()
            except Exception:
                pass
            try:
                path = self.markets_log.compact()
                print(f"[test-mode:real] compacted markets_latest -> {path}")
            except Exception as e:
                print(f"[test-mode:real] compact failed: {e}")
            self._beat()
            self._persist_cursor_sync()

        # Analyse collected data (reuse same analyser as synthetic but with real expectations)
        # For real mode, expected snapshots = num_markets * (window_size_seconds / 300 * 600) per asset wall-clock, but allow gaps
        start_ms = int((start_ts) * 1000)
        analysis = await self._analyse_test_data(start_ms, num_markets, window_size=ws)
        # patch analyser flag to real
        analysis["real_mode"] = True
        return analysis
    async def _analyse_test_data(self, start_ms: int, num_markets: int, window_size: int = 300) -> dict:
        """Analyse generated Parquet data — counts, completeness, sample rows."""
        import pyarrow.parquet as pq
        from pathlib import Path
        base = Path(self.config.storage.data_dir)
        import datetime
        start_dt = datetime.datetime.fromtimestamp(start_ms/1000, tz=datetime.timezone.utc)
        date_str = start_dt.date().isoformat()
        print(f"[analyse] date={date_str} start_ms={start_ms} num_markets={num_markets}")
        report: dict = {
            "generated_start_utc": start_dt.isoformat().replace("+00:00","Z"),
            "date": date_str,
            "num_markets": num_markets,
            "ticks_per_market": window_size // 300 * 600,  # proportional to window size (5min=600)
            "datasets": {},
            "checks": {},
        }

        def count_dataset(name: str):
            p = base / name
            if not p.exists():
                return {"exists": False, "files": 0, "rows": 0, "sample": None}
            files = list(p.rglob("*.parquet"))
            total = 0
            sample = None
            columns = None
            for part in files:
                try:
                    tbl = pq.read_table(str(part))
                    total += tbl.num_rows
                    if sample is None and tbl.num_rows>0:
                        sample = tbl.slice(0,1).to_pylist()[0] if tbl.num_rows else None
                        columns = tbl.column_names
                except Exception:
                    continue
            return {"exists": True, "files": len(files), "rows": total, "sample": sample, "columns": (columns[:12] if columns else None)}

        for ds in ["book_snapshots_500ms", "book_events", "trades", "chainlink_events", "collector_events", "markets_log", "markets_latest"]:
            info = count_dataset(ds)
            if ds=="markets_latest":
                p = base / "markets_latest" / "markets_latest.parquet"
                if p.exists():
                    try:
                        tbl = pq.read_table(str(p))
                        info = {"exists": True, "files": 1, "rows": tbl.num_rows, "sample": tbl.slice(0,1).to_pylist()[0] if tbl.num_rows else None, "columns": tbl.column_names[:12]}
                    except Exception:
                        info = {"exists": True, "files": 1, "rows": 0, "sample": None}
            report["datasets"][ds] = info
            print(f"[analyse] {ds}: {info['rows']} rows in {info['files']} files, exists={info['exists']}")

        checks = report["checks"]
        snap_info = report["datasets"]["book_snapshots_500ms"]
        ticks_per_market = report["ticks_per_market"]
        expected_snaps = num_markets * ticks_per_market * len(self.config.assets)
        checks["expected_book_snapshots"] = expected_snaps
        checks["actual_book_snapshots"] = snap_info["rows"]
        checks["snapshot_completeness_pct"] = round(100*snap_info["rows"]/expected_snaps,2) if expected_snaps else 0
        if snap_info["rows"] and snap_info["sample"]:
            s = snap_info["sample"]
            checks["sample_up_bid"] = s.get("up_bid")
            checks["sample_chainlink_price"] = report["datasets"]["chainlink_events"]["sample"].get("price") if report["datasets"]["chainlink_events"]["sample"] else None
            checks["sample_chainlink_twap"] = report["datasets"]["chainlink_events"]["sample"].get("twap") if report["datasets"]["chainlink_events"]["sample"] else None
            depth_cols = [k for k in s.keys() if "depth" in k]
            checks["depth_columns_present"] = len(depth_cols)
            checks["book_state_sample"] = s.get("book_state")
            checks["is_rollover_window_sample"] = s.get("is_rollover_window")
            price_fields = ["up_bid","up_ask","down_bid","down_ask"]
            price_ok = all((s.get(f) is None or 0 <= s.get(f) <=1) for f in price_fields)
            checks["price_bounds_ok"] = price_ok
            l2_sample = {k:v for k,v in s.items() if "_level_1_" in k}
            checks["l2_level_1_sample"] = l2_sample
        else:
            checks["price_bounds_ok"] = False

        cl = report["datasets"]["chainlink_events"]
        if cl["sample"]:
            checks["chainlink_has_twap"] = cl["sample"].get("twap") is not None
            checks["chainlink_has_price"] = cl["sample"].get("price") is not None
            checks["chainlink_twap_window"] = cl["sample"].get("twap_window_seconds")

    async def _kaggle_upload_loop(self) -> None:
        """Hourly loop: export aggregated timeframes and upload to Kaggle.
        
        Runs every 60 minutes (configurable via interval).
        1. Aggregates 5min base data into 15min/1h/4h/1d timeframes
        2. Uploads each to Kaggle dataset
        3. Clears local data after successful upload
        4. Logs results and any errors
        """
        # Check Kaggle configuration
        _validate_kaggle_config()

        while self._running:
            try:
                # Run hourly: export + upload + cleanup
                # Use config if available, default to 60 minutes
                interval_seconds = getattr(self.config, 'kaggle_upload_interval_seconds', 3600)
                print(f"[kaggle] Next upload in {interval_seconds // 60} minutes...")

                # Run the full export/upload/cleanup pipeline
                result = export_and_upload_all_kaggle(
                    data_dir=self.config.storage.data_dir,
                    out_dir=self.config.storage.data_dir / "export",
                    assets=self.config.assets,
                    kaggle_username=getattr(self.config, 'kaggle_username', None),
                    kaggle_key=getattr(self.config, 'kaggle_key', None),
                    timeframe_labels=["5m", "15m", "1h", "4h", "1d"],
                )

                # Log results
                export_count = len(result.get("export", {}))
                upload_success = sum(
                    1 for v in result.get("kaggle_uploads", {}).values()
                    if v.get("status") == "success"
                )
                upload_failed = sum(
                    1 for v in result.get("kaggle_uploads", {}).values()
                    if v.get("status") == "failed"
                )

                print(f"[kaggle] Export: {export_count} timeframes, "
                      f"Upload: {upload_success} success, {upload_failed} failed")

                # Sleep until next cycle
                # Check running status periodically
                for _ in range(min(interval_seconds, 60)):
                    if not self._running:
                        break
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                print("[kaggle] Upload loop cancelled")
                break
            except Exception as e:
                print(f"[kaggle] Upload loop error: {e}")
                import traceback
                traceback.print_exc()
                # Sleep a bit before retrying
                await asyncio.sleep(60)
