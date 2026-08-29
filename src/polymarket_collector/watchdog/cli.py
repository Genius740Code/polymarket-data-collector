"""Watchdog CLI entrypoint."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from ..config import CollectorConfig
from .watchdog import Watchdog


def main() -> None:
    ap = argparse.ArgumentParser(description="Watchdog — §17A (separate process, heartbeat + alerting)")
    ap.add_argument("--config", default=None)
    ap.add_argument("--heartbeat-path", default=None)
    ap.add_argument("--once", action="store_true", help="check once and exit")
    args = ap.parse_args()

    cfg = CollectorConfig.load(args.config)
    wd = Watchdog(cfg, heartbeat_path=args.heartbeat_path)

    async def run():
        if args.once:
            fired = await wd.check_once()
            print(f"alerts fired: {fired}")
            print(f"daily summary: {wd.daily_summary()}")
        else:
            print(f"watchdog polling every {cfg.watchdog.heartbeat_interval_seconds}s (heartbeat stale {cfg.watchdog.heartbeat_stale_seconds}s)")
            await wd.run_forever()

    asyncio.run(run())


if __name__ == "__main__":
    main()
