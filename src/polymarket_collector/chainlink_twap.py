"""Chainlink TWAP — downstream derived table (§6).

Raw `chainlink_events` carry no TWAP (RTDS payload has none —
`chainlink.py:chainlink_event_from_ws`). This module derives trailing
30s/60s time-weighted average price from stored ticks:

  TWAP_W(T) = integral_{T-W}^{T} price(t) dt / W

price(t) = last tick at or before t (previous-tick carry). Honest gaps:
any window whose max inter-tick gap exceeds `max_gap_ms` (default 10s,
`PERFECT_DATA_SPEC §7`) emits NULL TWAP with `gap_max_ms` kept for audit.
Never interpolated, never carried across gaps. Derived rows are labelled
`source='derived_chainlink_rtds'`.

Layout (same hive convention as §11):
  data/chainlink_twap/date=YYYY-MM-DD/asset=BTC/part-twap-*.parquet

Usage:
  python -m polymarket_collector.chainlink_twap --asset BTC --date 2026-10-01
  python -m polymarket_collector.chainlink_twap --all-assets --dry-run
"""
from __future__ import annotations

import argparse
import datetime
import os
import uuid
from pathlib import Path
from typing import List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .storage.schemas import CHAINLINK_TWAP_SCHEMA

DATASET = "chainlink_twap"
WINDOWS = (30_000, 60_000)


def _to_ms(ts) -> Optional[int]:
    if ts is None or ts == "" or isinstance(ts, bool):
        return None
    if isinstance(ts, (int, float)):
        try:
            f = float(ts)
            return int(f if f > 1e11 else f * 1000)
        except Exception:
            return None
    s = str(ts).strip()
    if not s:
        return None
    try:
        f = float(s)
        return int(f if f > 1e11 else f * 1000)
    except Exception:
        pass
    try:
        dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _iso(ms: int) -> str:
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _load_ticks(data_dir: str | Path, asset: str, date_str: str) -> List[Tuple[int, float]]:
    leaf = Path(data_dir) / "chainlink_events" / f"date={date_str}" / f"asset={asset.upper()}"
    if not leaf.exists():
        return []
    try:
        import pyarrow.dataset as ds
        dataset = ds.dataset(str(leaf), format="parquet", exclude_invalid_files=True)
        cols = [c for c in ("ts_source", "price", "asset") if c in dataset.schema.names]
        if "ts_source" not in cols or "price" not in cols:
            return []
        ticks: List[Tuple[int, float]] = []
        for batch in dataset.to_batches(columns=cols, batch_size=50000):
            d = batch.to_pylist()
            for r in d:
                if "asset" in cols and r.get("asset") and str(r["asset"]).upper() != asset.upper():
                    continue
                ms = _to_ms(r.get("ts_source"))
                try:
                    px = float(r["price"]) if r.get("price") is not None else None
                except Exception:
                    px = None
                if ms is None or px is None or px != px or px <= 0 or px in (float("inf"), float("-inf")):
                    continue
                ticks.append((ms, px))
    except Exception:
        # fallback: per-file read
        ticks = []
        for f in sorted(leaf.glob("*.parquet")):
            if f.name.endswith(".tmp"):
                continue
            try:
                t = pq.read_table(str(f), columns=["ts_source", "price"])
            except Exception:
                continue
            for r in t.to_pylist():
                ms = _to_ms(r.get("ts_source"))
                try:
                    px = float(r["price"]) if r.get("price") is not None else None
                except Exception:
                    px = None
                if ms is None or px is None or px != px or px <= 0:
                    continue
                ticks.append((ms, px))
    ticks.sort()
    # dedup same-ms: keep last
    out: List[Tuple[int, float]] = []
    for ms, px in ticks:
        if out and out[-1][0] == ms:
            out[-1] = (ms, px)
        else:
            out.append((ms, px))
    return out


def _twap_at(ticks: List[Tuple[int, float]], times: List[int], T: int, W: int,
             max_gap_ms: int) -> Tuple[Optional[float], Optional[int], Optional[int]]:
    """Trailing TWAP ending at T over window W. ticks sorted. times = ts list."""
    import bisect
    lo = T - W
    # index of first tick > lo, and carry tick at or before lo
    i = bisect.bisect_right(times, lo)
    carry = i - 1  # may be -1 = none
    # ticks strictly inside (lo, T]
    j = bisect.bisect_right(times, T, lo=i)
    inside = j - i
    # Full-window rule: need a tick at or before lo to carry the opening
    # price. Otherwise the window is under-covered — NULL, not a diluted
    # partial average.
    if carry < 0:
        return None, inside, None
    # build segment points: start with carry (clamped to lo) then inside ticks
    pts: List[Tuple[int, float]] = []
    if carry >= 0:
        pts.append((lo, ticks[carry][1]))
    for k in range(i, j):
        pts.append((ticks[k][0], ticks[k][1]))
    if not pts:
        return None, 0, None
    # gap check: max distance between consecutive pts + tail to T
    gap = 0
    for k in range(1, len(pts)):
        gap = max(gap, pts[k][0] - pts[k - 1][0])
    gap = max(gap, T - pts[-1][0])
    if gap > max_gap_ms:
        return None, inside, gap
    area = 0.0
    for k in range(len(pts)):
        seg_end = pts[k + 1][0] if k + 1 < len(pts) else T
        area += pts[k][1] * (seg_end - pts[k][0])
    return area / W, inside, gap


def compute_grid(ticks: List[Tuple[int, float]], asset: str,
                 step_ms: int = 1000, max_gap_ms: int = 10_000) -> List[dict]:
    if not ticks:
        return []
    times = [t[0] for t in ticks]
    start = (ticks[0][0] // step_ms) * step_ms
    end = (ticks[-1][0] // step_ms) * step_ms
    rows: List[dict] = []
    T = start
    while T <= end:
        r: dict = {
            "ts_window_end_ms": T,
            "ts_window_end_utc": _iso(T),
            "ts_window_start_ms_60s": T - 60_000,
            "asset": asset.upper(),
            "source": "derived_chainlink_rtds",
        }
        t30, n30, _ = _twap_at(ticks, times, T, 30_000, max_gap_ms)
        t60, n60, g60 = _twap_at(ticks, times, T, 60_000, max_gap_ms)
        r["twap_30s"] = t30
        r["twap_60s"] = t60
        r["n_ticks_30s"] = n30
        r["n_ticks_60s"] = n60
        r["gap_max_ms_60s"] = g60
        rows.append(r)
        T += step_ms
    return rows


def build_asset(data_dir: str | Path, asset: str, date_str: str,
                step_ms: int = 1000, max_gap_ms: int = 10_000) -> pa.Table:
    ticks = _load_ticks(data_dir, asset, date_str)
    rows = compute_grid(ticks, asset, step_ms, max_gap_ms)
    if not rows:
        return pa.table({f.name: [] for f in CHAINLINK_TWAP_SCHEMA}, schema=CHAINLINK_TWAP_SCHEMA)
    cols: dict[str, list] = {f.name: [] for f in CHAINLINK_TWAP_SCHEMA}
    for r in rows:
        for f in CHAINLINK_TWAP_SCHEMA:
            cols[f.name].append(r.get(f.name))
    return pa.table(cols, schema=CHAINLINK_TWAP_SCHEMA)


def write_asset(table: pa.Table, data_dir: str | Path, asset: str, date_str: str) -> Optional[Path]:
    if table.num_rows == 0:
        return None
    leaf = Path(data_dir) / DATASET / f"date={date_str}" / f"asset={asset.upper()}"
    leaf.mkdir(parents=True, exist_ok=True)
    tmp = leaf / f"part-twap-{uuid.uuid4().hex[:8]}.parquet.tmp"
    final = leaf / f"part-twap-{uuid.uuid4().hex[:8]}.parquet"
    with pq.ParquetWriter(str(tmp), CHAINLINK_TWAP_SCHEMA, compression="zstd") as w:
        w.write_table(table)
    n = pq.read_metadata(str(tmp)).num_rows
    if n != table.num_rows:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"twap verify fail: out={n} written={table.num_rows}")
    os.replace(str(tmp), str(final))
    return final


def run(data_dir: str | Path, assets: List[str], date_str: str,
        step_ms: int = 1000, max_gap_ms: int = 10_000,
        dry_run: bool = False, overwrite: bool = False) -> dict:
    stats: dict = {"date": date_str, "assets": {}}
    for a in assets:
        leaf = Path(data_dir) / DATASET / f"date={date_str}" / f"asset={a.upper()}"
        existing = sorted(leaf.glob("*.parquet")) if leaf.exists() else []
        if existing and not overwrite and not dry_run:
            stats["assets"][a] = {"skipped": len(existing)}
            continue
        t = build_asset(data_dir, a, date_str, step_ms, max_gap_ms)
        n = t.num_rows
        cov = {}
        if n:
            import pyarrow.compute as pc
            cov = {
                "twap30_cov": float(pc.sum(pc.invert(pc.is_null(t.column("twap_30s")))).as_py()) / n,
                "twap60_cov": float(pc.sum(pc.invert(pc.is_null(t.column("twap_60s")))).as_py()) / n,
            }
        if dry_run:
            stats["assets"][a] = {"rows": n, **cov, "dry_run": True}
            print(f"[twap] {a} {date_str}: {n} grid rows {cov} (dry-run)")
            continue
        if overwrite:
            for f in existing:
                try:
                    f.unlink()
                except Exception:
                    pass
        p = write_asset(t, data_dir, a, date_str)
        stats["assets"][a] = {"rows": n, **cov, "file": str(p) if p else None}
        print(f"[twap] {a} {date_str}: {n} rows {cov} -> {p}")
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Derive trailing 30s/60s Chainlink TWAP (1s grid)")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--asset", action="append", default=None)
    ap.add_argument("--all-assets", action="store_true")
    ap.add_argument("--date", default=datetime.datetime.now(tz=datetime.timezone.utc).date().isoformat())
    ap.add_argument("--step-ms", type=int, default=1000)
    ap.add_argument("--max-gap-ms", type=int, default=10_000)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()
    assets = args.asset or (["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"] if args.all_assets else ["BTC"])
    run(args.data_dir, assets, args.date, args.step_ms, args.max_gap_ms, args.dry_run, args.overwrite)


if __name__ == "__main__":
    main()
