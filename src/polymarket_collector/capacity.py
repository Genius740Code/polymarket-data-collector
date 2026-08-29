"""Capacity planning — §11A.

Size the pipeline before assuming 24/7 won't fill disk. Estimates from actual
field counts, with pilot-run calibration helper.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass


@dataclass
class CapacityEstimate:
    snapshot_rows_per_day_per_asset: int = 172800  # 2/sec * 86400
    assets: int = 3
    fields_per_snapshot: int = 0
    bytes_per_row_uncompressed: int = 2500
    parquet_compression_ratio: float = 0.25
    book_events_per_day_per_asset: int = 5000  # pilot guess
    trades_per_day_per_asset: int = 10000
    chainlink_per_day_per_asset: int = 86400  # 1/sec guess

    def snapshot_daily_bytes(self) -> dict:
        rows = self.snapshot_rows_per_day_per_asset * self.assets
        uncomp = rows * self.bytes_per_row_uncompressed
        comp = int(uncomp * self.parquet_compression_ratio)
        return {"rows": rows, "uncompressed_bytes": uncomp, "compressed_bytes": comp}

    def total_daily_compressed(self) -> int:
        snap = self.snapshot_daily_bytes()["compressed_bytes"]
        # rough: events/trades ~ 500 bytes/row compressed
        extra_rows = (self.book_events_per_day_per_asset + self.trades_per_day_per_asset + self.chainlink_per_day_per_asset) * self.assets
        extra = int(extra_rows * 500 * self.parquet_compression_ratio)
        return snap + extra

    def to_dict(self) -> dict:
        snap = self.snapshot_daily_bytes()
        total = self.total_daily_compressed()
        daily_mb = round(total / 1_048_576, 2)
        return {
            "snapshot_rows_per_day_per_asset": self.snapshot_rows_per_day_per_asset,
            "assets": self.assets,
            "fields_per_snapshot": self.fields_per_snapshot,
            "bytes_per_row_uncompressed": self.bytes_per_row_uncompressed,
            "parquet_compression_ratio": self.parquet_compression_ratio,
            "snapshot_daily": snap,
            "total_daily_compressed_bytes": total,
            "total_daily_compressed_mb": daily_mb,
            "weekly_mb": round(daily_mb * 7, 2),
            "monthly_mb": round(daily_mb * 30, 2),
            "note": "Re-check after pilot: event-driven tables vary; measure actual row sizes. Feed into compaction & raw_archive retention (§10A, §13).",
        }


def estimate_from_schema(l2_levels: int = 20) -> CapacityEstimate:
    # count fields: snapshot_schema
    from .storage.schemas import snapshot_schema
    schema = snapshot_schema(l2_levels)
    est = CapacityEstimate(fields_per_snapshot=len(schema), assets=3)
    return est


def main() -> None:
    ap = argparse.ArgumentParser(description="Capacity planning — §11A")
    ap.add_argument("--l2-levels", type=int, default=20)
    ap.add_argument("--assets", type=int, default=3)
    ap.add_argument("--bytes-per-row", type=int, default=2500)
    ap.add_argument("--compression", type=float, default=0.25)
    ap.add_argument("--json-out", type=str, default=None)
    args = ap.parse_args()

    est = estimate_from_schema(args.l2_levels)
    est.assets = args.assets
    est.bytes_per_row_uncompressed = args.bytes_per_row
    est.parquet_compression_ratio = args.compression

    result = est.to_dict()
    print(json.dumps(result, indent=2))
    # also human summary
    print(f"\nSnapshot rows/day: {est.snapshot_rows_per_day_per_asset} per asset × {est.assets} = {est.snapshot_rows_per_day_per_asset*est.assets}")
    print(f"Fields per snapshot: {est.fields_per_snapshot}")
    print(f"Daily compressed (est): {result['total_daily_compressed_mb']} MB")
    print(f"Weekly:  {result['weekly_mb']} MB")
    print(f"Monthly: {result['monthly_mb']} MB")
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Wrote {args.json_out}")


if __name__ == "__main__":
    main()
