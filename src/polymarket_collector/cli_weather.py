"""Weather collector entrypoint — high|low daily temperature brackets.

Uses the shared Collector engine (WS, books, snapshots, writers, resync) with
RolloverManager replaced by WeatherManager (event-set discovery). Must ONLY
run with config/collector.weather.{high,low}.yaml — guards below refuse the
crypto config/data dir so the BTC runner can never be disturbed.
"""
from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

from .collector import Collector
from .config import CollectorConfig
from .weather_discovery import WeatherDiscovery
from .weather_manager import WeatherManager


def _guard(cfg: CollectorConfig, path: str) -> str:
    data = Path(cfg.storage.data_dir).resolve()
    if data.name == "data" or str(data).rstrip("/").endswith("/data"):
        raise SystemExit(
            f"REFUSED: {path} resolves to crypto data dir ({data}). "
            "Weather must use ./data-weather-high or ./data-weather-low.")
    prefix = str(getattr(cfg.kaggle, "dataset_prefix", ""))
    if "crypto" in prefix.lower():
        raise SystemExit(
            f"REFUSED: kaggle prefix {prefix!r} looks like the crypto dataset. "
            "Weather must upload to polymarket-weather-high/low.")
    from .weather_manager import _infer_mode

    return _infer_mode(cfg)


async def _dry_run(cfg: CollectorConfig, mode: str) -> int:
    disc = WeatherDiscovery(
        mode=mode, on_event=None,
        liquidity_filter=getattr(cfg, "liquidity_filter", None))
    total = 0
    for asset in cfg.assets:
        brackets = await disc.discover_city_markets(asset)
        total += len(brackets)
        print(f"{asset}: {len(brackets)} brackets")
        for m in brackets[:4]:
            print(f"  {m.slug} yes={m.up_token_id[:10]}… "
                  f"vol={m.reported_volume} liq={m.reported_liquidity}")
        if len(brackets) > 4:
            print(f"  … +{len(brackets) - 4} more")
    print(f"mode={mode} cities={len(cfg.assets)} total_brackets={total} "
          f"tokens={total * 2}")
    return 0 if total else 1


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Polymarket weather collector — daily high/low temperature brackets")
    ap.add_argument("--config", default="config/collector.weather.high.yaml")
    ap.add_argument("--dry-run", action="store_true",
                    help="discover once per city, print brackets, exit (no writes)")
    ap.add_argument("--soak-minutes", type=float, default=None,
                    help="bounded live run for validation, then stop (kaggle loop off)")
    ap.add_argument("--with-chainlink", action="store_true",
                    help="keep the crypto RTDS task (default off: weather settles via CLOB winner)")
    args = ap.parse_args()

    cfg = CollectorConfig.load(args.config)
    mode = _guard(cfg, args.config)
    print(f"[weather:{mode}] config={args.config} assets={cfg.assets} "
          f"data={cfg.storage.data_dir} kaggle={cfg.kaggle.dataset_prefix}")

    if args.dry_run:
        rc = asyncio.run(_dry_run(cfg, mode))
        sys.exit(rc)

    async def run():
        collector = Collector(cfg)
        collector.rollover = WeatherManager(
            cfg, on_event=collector._collector_event)
        loop = asyncio.get_running_loop()
        stop_requested = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_requested.set)
            except NotImplementedError:
                pass
        soak = args.soak_minutes is not None
        await collector.start(enable_kaggle_loop=not soak)
        if not args.with_chainlink:
            try:
                if getattr(collector, "_chainlink_task", None) is not None:
                    collector._chainlink_task.cancel()
                    print("[weather] crypto RTDS task disabled (settlement via CLOB winner)")
            except Exception:
                pass
        print(f"[weather:{mode}] running — BTC runner untouched "
              f"(own data dir, own PM2 app)")
        try:
            if soak:
                try:
                    await asyncio.wait_for(stop_requested.wait(),
                                           timeout=float(args.soak_minutes) * 60)
                except asyncio.TimeoutError:
                    pass
            else:
                while collector._running:
                    try:
                        await asyncio.wait_for(stop_requested.wait(), timeout=1.0)
                        break
                    except asyncio.TimeoutError:
                        pass
        except asyncio.CancelledError:
            pass
        finally:
            await collector.stop()
            print("[weather] stopped — buffers flushed, cursor persisted")

    try:
        asyncio.run(run())
    except ConnectionResetError:
        pass


if __name__ == "__main__":
    main()
