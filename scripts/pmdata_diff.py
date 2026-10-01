"""Thin CLI wrapper for polymarket_collector.pmdata_diff (pure parquet compare)."""

import sys

from polymarket_collector.pmdata_diff import main

if __name__ == "__main__":
    sys.exit(main())
