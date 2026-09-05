"""B-7: standalone resolution backfill — closed 5m markets → resolved, every 15 min.

Polymarket's CLOB market endpoint (clob.polymarket.com/markets/{conditionId})
keeps every market queryable indefinitely and exposes the OFFICIAL winner via
``tokens[].winner`` after settlement — unlike Gamma, which drops 5m markets from
slug lookup within minutes (probed live 2026-09-05).

The collector's in-run resolution loop uses the Chainlink open/end reference
(settlement_source=inferred_nearest); this script covers everything it could
not resolve — windows that ended mid-outage, before process start, or after
shutdown — and upgrades closed/unknown rows to the official outcome.

Append-only writes through MarketsLog (log row + atomic compact rebuild), so
concurrent readers never see partial state. Run on a schedule:

    python -m polymarket_collector.resolution_backfill --config config/collector.yaml
"""
from __future__ import annotations

import argparse
import datetime
import time
from pathlib import Path
from typing import Dict, List, Optional

import httpx

from .storage.markets_log import MarketsLog

CLOB_MARKET_URL = "https://clob.polymarket.com/markets/{cid}"


def _now_iso() -> str:
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_official_outcome(condition_id: str) -> Optional[Dict]:
    """Official outcome from the CLOB: the token flagged ``winner`` post-settlement.

    Returns {"outcome": "up"|"down", "price": float} or None when the market is
    not yet settled (or the fetch fails) — the caller simply retries next run.
    """
    try:
        resp = httpx.get(CLOB_MARKET_URL.format(cid=condition_id), timeout=10,
                         headers={"User-Agent": "Mozilla/5.0"})
    except Exception as e:
        print(f"[resolution-backfill] fetch failed {condition_id[:14]}…: {e}")
        return None
    if resp.status_code != 200:
        return None
    j = resp.json()
    if not j.get("closed"):
        return None
    for tok in j.get("tokens") or []:
        if tok.get("winner"):
            outcome = str(tok.get("outcome") or "").strip().lower()
            if outcome in ("up", "down"):
                try:
                    price = float(tok.get("price")) if tok.get("price") is not None else None
                except Exception:
                    price = None
                return {"outcome": outcome, "price": price}
    return None


def backfill_resolutions(data_dir: str | Path, dry_run: bool = False, max_fetch: int = 200) -> Dict:
    """Resolve every ended market still marked active/closed/unknown.

    Returns stats {"candidates", "resolved", "already", "pending"}.
    """
    base = Path(data_dir)
    log = MarketsLog(base)
    rows = log.load_latest()
    now_ms = int(time.time() * 1000)
    # one candidate per condition_id (markets_latest is already deduped, but be defensive)
    seen: set = set()
    candidates: List[Dict] = []
    for r in rows:
        cid = r.get("condition_id")
        if not cid or cid in seen:
            continue
        end_ms = r.get("market_end_ts_ms")
        try:
            end_ms = int(end_ms) if end_ms is not None else None
        except Exception:
            end_ms = None
        if end_ms is None or end_ms >= now_ms:
            continue
        if r.get("status") == "resolved" and r.get("resolution_outcome") in ("up", "down", "tie"):
            seen.add(cid)
            continue
        seen.add(cid)
        candidates.append(r)
    stats = {"candidates": len(candidates), "resolved": 0, "already": 0, "pending": 0}
    for r in candidates[:max_fetch]:
        cid = r["condition_id"]
        official = fetch_official_outcome(cid)
        if official is None:
            stats["pending"] += 1  # not settled yet — retried on the next run
            continue
        if dry_run:
            print(f"[resolution-backfill] would resolve {r.get('asset')} w{r.get('window_index')} {cid[:14]}… → {official['outcome']}")
            stats["resolved"] += 1
            continue
        new_row = dict(r)
        new_row.update({
            "status": "resolved",
            "resolution_outcome": official["outcome"],
            "settlement_price": official["price"],
            "settlement_source": "polymarket_official",
            "resolution_confirmed_at": _now_iso(),
            "resolution_ts": _now_iso(),
        })
        log.append(new_row)
        stats["resolved"] += 1
        print(f"[resolution-backfill] {r.get('asset')} w{r.get('window_index')} resolved {official['outcome']} (official)")
        time.sleep(0.2)  # gentle on the CLOB
    if not dry_run and stats["resolved"]:
        log.flush_staging()
        log.compact()
    print(f"[resolution-backfill] done: {stats}")
    return stats


def reupload_kaggle(data_dir: str | Path, assets: List[str], l2_levels: int = 20) -> bool:
    """Re-export staging and push a new Kaggle version carrying the resolutions."""
    from .storage.export import export_and_upload_all_kaggle, _validate_kaggle_config
    res = export_and_upload_all_kaggle(
        data_dir=str(data_dir),
        assets=assets,
        timeframe_labels=["5m"],
        l2_levels=l2_levels,
        dry_run=not _validate_kaggle_config(),
    )
    status = {k: v.get("status") for k, v in (res.get("kaggle_uploads") or {}).items()}
    print(f"[resolution-backfill] kaggle re-upload status: {status}")
    return any(s == "success" for s in status.values())


def main() -> None:
    from .config import CollectorConfig
    ap = argparse.ArgumentParser(description="Backfill official resolutions for ended 5m markets (run every 15 min)")
    ap.add_argument("--config", default="config/collector.yaml")
    ap.add_argument("--data-dir", default=None, help="override storage.data_dir")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--reupload", action="store_true", help="re-export staging + push a new Kaggle version after backfilling")
    args = ap.parse_args()
    cfg = CollectorConfig.load(args.config)
    data_dir = args.data_dir or cfg.storage.data_dir
    stats = backfill_resolutions(data_dir, dry_run=args.dry_run)
    if args.reupload and stats.get("resolved"):
        reupload_kaggle(data_dir, cfg.assets, cfg.l2_levels)


if __name__ == "__main__":
    main()
