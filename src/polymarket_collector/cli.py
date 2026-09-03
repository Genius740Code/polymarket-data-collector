"""Collector CLI entrypoint."""
from __future__ import annotations

import argparse
import asyncio
import signal

from .config import CollectorConfig
from .collector import Collector


def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket collector — BTC/ETH/SOL/HYPE/BNB/XRP/DOGE 5m markets (PLAN.md v4) — 5m-only, 4 markets, 10-min Kaggle")
    ap.add_argument("--config", default=None, help="path to collector.yaml")
    ap.add_argument("--test-mode", action="store_true", help="live test mode: collect 4×5m live windows for 7 assets, 10-min Kaggle, then analyse and exit")
    ap.add_argument("--test-markets", type=int, default=None, help="override number of windows for test mode (default 4)")
    ap.add_argument("--no-accelerate", action="store_true", help="(deprecated) kept for compat")
    ap.add_argument("--window-size", type=int, default=None, help="window size in seconds (300=5min, 900=15min, 3600=1h, 14400=4h, 86400=1d)")
    ap.add_argument("--series-id", type=str, default=None, help="override series ID per asset (format: ASSET-WINDOW, e.g. BTC-1H)")
    args = ap.parse_args()

    cfg = CollectorConfig.load(args.config)
    # CLI overrides config
    if args.test_mode:
        cfg.test_mode.enabled = True
    if args.test_markets is not None:
        cfg.test_mode.num_markets = args.test_markets
    if args.no_accelerate:
        cfg.test_mode.accelerate = False
    # if test_mode is via config YAML but CLI flag not given, honour it
    is_test = cfg.test_mode.enabled or args.test_mode
    # CLI window-size override
    if args.window_size is not None:
        cfg.window_size_seconds = args.window_size
    # CLI series-id override (format: BTC-1H, ETH-15M, etc.)
    if args.series_id is not None:
        # parse "ASSET-WINDOW" and apply per asset
        for part in args.series_id.split(","):
            asset, sid = part.split("-", 1)
            cfg.series_ids[asset.upper()] = sid
    # if test_mode is via config YAML but CLI flag not given, honour it

    async def run():
        collector = Collector(cfg)
        loop = asyncio.get_running_loop()
        stop_requested = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_requested.set)
            except NotImplementedError:
                pass
        is_test = cfg.test_mode.enabled or args.test_mode
        if is_test:
            # live test — runs real pipeline for N windows, then analyses data and exits
            await collector.run_test_mode(num_markets=cfg.test_mode.num_markets, accelerate=cfg.test_mode.accelerate)
            print("test mode completed — data in", cfg.storage.data_dir)
            return
        await collector.start()
        # run until stopped
        try:
            while collector._running:
                try:
                    await asyncio.wait_for(stop_requested.wait(), timeout=1.0)
                    break
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            # single code path owns stop() exactly once
            await collector.stop()
            print("collector stopped")

    asyncio.run(run())


if __name__ == "__main__":
    main()
