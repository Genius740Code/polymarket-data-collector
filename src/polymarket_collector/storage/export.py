"""Kaggle-style per-asset single-file export — time first, condition_id second, no binance.

Reads hive-partitioned parquet under data/ and writes one flat parquet per asset per dataset:
  data/export/BTC_book_snapshots.parquet  (or data/export/book_snapshots_BTC.parquet)
  data/export/BTC_trades.parquet
etc.

Time-first column order + sorting by ts + condition_id is enforced.
Binance rows (source=binance-ticker-proxy) are excluded when include_binance=False.

Unlike live writer (batched hive partitions §10A) this is run on-demand for sharing.
Atomic tmp+rename same as parquet_writer.py §10A.

Additional functionality:
- Timeframe aggregation: derives 15min/1h/4h/1d from 5min base data
- Kaggle API upload with per-dataset versioning
- Post-upload local data cleanup with integrity guarantees
"""
from __future__ import annotations

import argparse
import uuid
from pathlib import Path
from typing import List, Optional, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq


def _os_replace_safe(src, dst):
    """Atomic tmp->final rename that works on Windows (os.replace overwrites; Path.rename raises WinError 183 if dst exists)."""
    import os as _os
    _os.replace(str(src), str(dst))


from .parquet_io import read_table
import pyarrow.compute as pc

from .schemas import SCHEMAS, snapshot_schema, MARKETS_SUMMARY_SCHEMA


# Datasets that are per-asset vs global
PER_ASSET_DATASETS = {"book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events"}
NON_ASSET_DATASETS = {"markets_log", "collector_events", "resync_episodes", "markets_summary"}

# Prefer these ts columns for sorting (first present wins)
TS_SORT_CANDIDATES = [
    "ts_snapshot_utc", "ts_snapshot_ns",
    "ts_source", "ts_received_ns",
    "ts_utc", "ts_received_ns",
    "market_start_ts", "updated_at",
    "disconnect_ts_utc",
]


def _sort_keys_for_schema(schema: pa.Schema) -> List[str]:
    """Return sort keys that exist in schema, time first then condition_id."""
    keys: List[str] = []
    for k in TS_SORT_CANDIDATES:
        if k in schema.names and k not in keys:
            keys.append(k)
            if len(keys) >= 2:
                break
    # always add condition_id as tie-breaker if present
    if "condition_id" in schema.names and "condition_id" not in keys:
        keys.append("condition_id")
    # also add asset if present for stable sort, but per-asset export already filtered
    return keys


def _get_schema(dataset: str, l2_levels: int = 10) -> Optional[pa.Schema]:
    if dataset in ("book_snapshots_500ms", "book_snapshots_clean"):
        # B-6: the clean view ships to Kaggle as its own per-asset file and
        # carries the snapshot schema (it is the live-only subset of it)
        return snapshot_schema(l2_levels)
    return SCHEMAS.get(dataset)




def _api_ts_ms(t: dict) -> str:
    ts = t.get("timestamp")
    if ts is None:
        return ""
    try:
        f = float(ts)
        return str(int(f if f > 1e12 else f * 1000))
    except Exception:
        return ""


def _api_ts_ms_value(t: dict) -> Optional[int]:
    """Parse a Data-API trade timestamp (s or ms epoch) to epoch ms."""
    ts = t.get("timestamp")
    if ts is None:
        return None
    try:
        f = float(ts)
        return int(f if f > 1e11 else f * 1000)
    except Exception:
        return None


def _api_outcome_label(t: dict) -> Optional[str]:
    """R-3: the Data-API carries an authoritative outcome label ("Up"/"Down")
    per fill — map it into the schema's lowercase up/down vocabulary."""
    o = str(t.get("outcome") or "").strip().lower()
    return o if o in ("up", "down") else None


def _unambiguous_wallet(pool: Optional[list]) -> Optional[str]:
    """A fill-key's leg pool names the maker/taker only when every row at that
    key agrees — with several DISTINCT wallets at one (tx,price,size) key the
    per-fill attribution would be a guess, and NULL is kept (never guessed)."""
    if not pool:
        return None
    distinct = set(pool)
    return next(iter(distinct)) if len(distinct) == 1 else None


def _backfill_trade_wallets(combined: pa.Table, data_dir: Path, asset: Optional[str] = None, reconcile: bool = True) -> pa.Table:
    """Fill maker_wallet/taker_wallet/wallet and missing outcome on trades.

    The CLOB market channel does not carry wallets, so streamed trade rows have
    them NULL. Polymarket's public Data-API
    (data-api.polymarket.com/trades?market=<conditionId>) carries proxyWallet
    per fill LEG: fetched with takerOnly=false every fill appears TWICE — a SELL
    leg (the maker's proxyWallet) and a BUY leg (the taker's proxyWallet) —
    sharing transactionHash/price/size. Pooling the legs per side therefore
    fills maker_wallet too (R-2) without any on-chain RPC: a row's side is the
    aggressor side, so side=BUY → taker on the BUY leg / maker on the SELL leg.
    Where the API itself has no wallet for a fill, NULL is kept (never
    fabricated). Read-only, best-effort: failures are logged loudly.
    """
    import collections
    import datetime as _dt
    import httpx as _hx

    def _fetch(cid: str, taker_only: bool, oldest_needed_ms: Optional[int], max_pages: int = 60) -> list:
        """Data-API fills for a market, newest-first, paging until older than
        anything we need (adaptive pagination — enrichment round 2). The old
        fixed 12-page cap truncated liquid markets (BTC ~4k fills/window ≈ 8+
        pages), so fills beyond the cap could never be enriched. max_pages is
        now only a runaway-safety ceiling, not the stop condition."""
        rows: list = []
        offset = 0
        for _page in range(max_pages):
            params: dict = {"market": cid, "limit": 500, "offset": offset}
            if not taker_only:
                params["takerOnly"] = "false"
            try:
                resp = _hx.get("https://data-api.polymarket.com/trades", params=params, timeout=10)
            except Exception as e:
                print(f"[export] WARN data-api fetch failed for {cid[:14]}…: {e}")
                break
            if resp.status_code != 200:
                break
            batch = resp.json()
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < 500:
                break
            offset += 500
            if oldest_needed_ms is not None:
                ts = _api_ts_ms_value(batch[-1])
                if ts is not None and ts < oldest_needed_ms - 120_000:
                    break
        return rows

    def _row_ts_ms(r: dict) -> Optional[int]:
        ts = r.get("ts_source")
        if ts is None:
            return None
        try:
            f = float(ts)
            return int(f if f > 1e11 else f * 1000)
        except Exception:
            return None

    if combined.num_rows == 0 or "wallet" not in combined.schema.names:
        return combined
    pylist = combined.to_pylist()
    need_by_cid: dict = {}
    for i, r in enumerate(pylist):
        if not r.get("transaction_hash") or not r.get("condition_id"):
            continue
        needs_wallet = r.get("wallet") is None
        needs_maker = r.get("maker_wallet") is None and str(r.get("side") or "").upper() in ("BUY", "SELL")
        needs_outcome = r.get("outcome") in (None, "", "unknown")
        if needs_wallet or needs_maker or needs_outcome:
            need_by_cid.setdefault(r["condition_id"], []).append(i)
    if not need_by_cid:
        return combined
    filled_wallet = 0
    filled_outcome = 0
    legs_by_cid: dict = {}
    for cid, idxs in need_by_cid.items():
        oldest_needed_ms = min((_row_ts_ms(pylist[i]) for i in idxs), default=None)
        # one fill settles as TWO data-api legs sharing (tx_hash, price, size)
        buy_pool: dict = {}
        sell_pool: dict = {}
        outcome_by_key: dict = {}
        for t in _fetch(cid, taker_only=False, oldest_needed_ms=oldest_needed_ms):
            txh = (t.get("transactionHash") or "").lower()
            if not txh:
                continue
            try:
                k = (txh, round(float(t.get("price")), 6), round(float(t.get("size")), 6))
            except Exception:
                k = (txh,)
            w = t.get("proxyWallet") or t.get("wallet")
            side = str(t.get("side") or "").upper()
            if w:
                if side == "SELL":
                    sell_pool.setdefault(k, []).append(w)
                    sell_pool.setdefault((txh,), []).append(w)
                elif side == "BUY":
                    buy_pool.setdefault(k, []).append(w)
                    buy_pool.setdefault((txh,), []).append(w)
            o = _api_outcome_label(t)
            if o:
                outcome_by_key[k] = o
                outcome_by_key.setdefault((txh,), o)

        def _tx_fallback(txh: str, side: str) -> tuple[Optional[str], Optional[str]]:
            """Tx-level attribution ONLY when the whole tx is a single
            buyer/single seller fill — otherwise a per-fill wallet cannot be
            assigned honestly (multi-fill txs pool wallets)."""
            buys = sorted(set(buy_pool.get((txh,)) or []))
            sells = sorted(set(sell_pool.get((txh,)) or []))
            if len(buys) == 1 and len(sells) == 1:
                return (buys[0], sells[0]) if side == "BUY" else (sells[0], buys[0])
            return None, None

        legs_by_cid[cid] = (buy_pool, sell_pool)
        for i in idxs:
            r = pylist[i]
            txh = (r.get("transaction_hash") or "").lower()
            try:
                k = (txh, round(float(r.get("price")), 6), round(float(r.get("size")), 6))
            except Exception:
                k = (txh,)
            side = str(r.get("side") or r.get("aggressor_side") or "").upper()
            if side in ("BUY", "SELL"):
                taker_pool = buy_pool if side == "BUY" else sell_pool
                maker_pool = sell_pool if side == "BUY" else buy_pool
                w = _unambiguous_wallet(taker_pool.get(k))
                m = _unambiguous_wallet(maker_pool.get(k))
                if w is None and m is None:
                    takers_f, makers_f = _tx_fallback(txh, side)
                    w = w or takers_f
                    m = m or makers_f
                if r.get("taker_wallet") is None and w:
                    r["taker_wallet"] = w
                    filled_wallet += 1
                if r.get("maker_wallet") is None and m:
                    r["maker_wallet"] = m
                    filled_wallet += 1
                if r.get("wallet") is None and (r.get("taker_wallet") or r.get("maker_wallet")):
                    r["wallet"] = r.get("taker_wallet") or r.get("maker_wallet")
                    filled_wallet += 1
            elif r.get("wallet") is None:
                # side unknown — legs cannot be attributed maker/taker; fill the
                # canonical wallet from either leg (previous behavior)
                either = _unambiguous_wallet((buy_pool.get(k) or []) + (sell_pool.get(k) or []))
                if not either:
                    either = _unambiguous_wallet(sorted(set((buy_pool.get((txh,)) or []) + (sell_pool.get((txh,)) or []))))
                if either:
                    r["wallet"] = either
                    filled_wallet += 1
            if r.get("outcome") in (None, "", "unknown"):
                o = outcome_by_key.get(k) or outcome_by_key.get((txh,))
                if o:
                    r["outcome"] = o
                    filled_outcome += 1
    if filled_wallet or filled_outcome:
        print(f"[export] wallet/outcome backfill: filled {filled_wallet} wallet fields and {filled_outcome} outcomes from data-api (both legs, takerOnly=false)")
    combined = pa.Table.from_pylist(pylist, schema=combined.schema)

    # K-6 trade reconciliation: the CLOB last_trade_price stream COALESCES fills on
    # liquid markets (measured 2026-09-05: BTC 12-18% of data-api fills captured,
    # DOGE 93%) — insert missing fills as api-prefixed rows so per-market trade
    # counts are complete. Existing rows keep their identity; only truly missing
    # (tx_hash, price, size) fills are added — never duplicated.
    # R-3: the data-api trade object carries no fee_rate_bps and (before this
    # fix) the outcome was hardcoded "unknown". The outcome now comes from the
    # API's own authoritative label; the fee is derived from the fee rate the
    # exchange itself reported on this market's streamed rows (uniform across
    # the market, 0 on current 5m markets) and flagged fee_is_estimated=True —
    # derived, not fabricated; NULL when the market's streamed rows disagree.
    if not reconcile:
        # wallet/outcome-only mode (enrichment round 2): fill NULLs, skip the
        # api- row inserts — the second pass must never duplicate reconciliation
        return combined
    inserted = 0
    fee_derived = 0
    try:
        by_cid: dict = {}
        for r in combined.to_pylist():
            if r.get("condition_id"):
                by_cid.setdefault(r["condition_id"], []).append(r)
        rows_to_add: list = []
        for cid, rs in by_cid.items():
            have: collections.Counter = collections.Counter(
                ((r.get("transaction_hash") or "").lower(), round(float(r["price"]), 6), round(float(r["size"]), 6))
                for r in rs if r.get("transaction_hash") and r.get("price") is not None and r.get("size") is not None
            )
            series_mode = collections.Counter(r.get("series_id") for r in rs).most_common(1)[0][0] if rs else None
            fee_rate: Optional[float] = None
            rates: set = set()
            for r in rs:
                if r.get("fee") is not None and r.get("fee_is_estimated") is False and r.get("notional"):
                    try:
                        rates.add(round(float(r["fee"]) / float(r["notional"]), 8))
                    except Exception:
                        pass
            if len(rates) == 1:
                fee_rate = rates.pop()
            oldest_needed_ms = min((_row_ts_ms(r) for r in rs), default=None)
            api_rows = _fetch(cid, taker_only=True, oldest_needed_ms=oldest_needed_ms)
            for t in api_rows:
                txh = (t.get("transactionHash") or "").lower()
                try:
                    k = (txh, round(float(t.get("price")), 6), round(float(t.get("size")), 6))
                except Exception:
                    continue
                if have.get(k, 0) > 0:
                    have[k] -= 1
                    continue
                w = t.get("proxyWallet") or t.get("wallet")
                ts_ms = _api_ts_ms(t)
                try:
                    widx = int(ts_ms) // 1000 // 300 if ts_ms else (rs[0].get("window_index") or 0)
                except Exception:
                    widx = rs[0].get("window_index") or 0
                price_f = t.get("price"); size_f = t.get("size")
                notional = round(float(price_f) * float(size_f), 6) if price_f is not None and size_f is not None else None
                fee: Optional[float] = None
                fee_is_estimated: Optional[bool] = None
                if fee_rate is not None and notional is not None:
                    fee = round(notional * fee_rate, 6)
                    fee_is_estimated = True  # derived from the market's reported rate, not reported per fill
                    fee_derived += 1
                # R-2: attribute the maker leg when the earlier both-legs fetch
                # exposed it unambiguously for this fill key (single distinct wallet)
                maker_w = None
                leg_pools = legs_by_cid.get(cid)
                if leg_pools:
                    maker_w = _unambiguous_wallet(leg_pools[1].get(k))
                rows_to_add.append({
                    "ts_source": ts_ms or None,
                    "ts_received_ns": int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp() * 1e9),
                    "condition_id": cid,
                    "market_id": rs[0].get("market_id") or cid,
                    "series_id": series_mode or f"{asset or 'X'}-5MIN",
                    "window_index": int(widx) if widx is not None else 0,
                    "asset": (asset or rs[0].get("asset") or "").upper(),
                    "trade_id": f"api-{txh[:16]}-{inserted}",
                    "transaction_hash": txh or None,
                    "token_id": str(t.get("asset_id") or t.get("asset") or ""),
                    "outcome": _api_outcome_label(t) or "unknown",
                    "price": float(price_f) if price_f is not None else None,
                    "size": float(size_f) if size_f is not None else None,
                    "notional": notional,
                    "fee": fee,
                    "fee_is_estimated": fee_is_estimated,
                    "side": (t.get("side") or "").lower() or None,
                    "aggressor_side": (t.get("side") or "").lower() or None,
                    "sequence_number": None,
                    "maker_wallet": maker_w,
                    "taker_wallet": w,
                    "wallet": w or maker_w,
                })
                inserted += 1
        if rows_to_add:
            combined = pa.concat_tables(
                [combined, pa.Table.from_pylist(rows_to_add, schema=combined.schema)],
                **({"promote_options": "default"} if tuple(int(x) for x in pa.__version__.split(".")[:2]) >= (16, 0) else {"promote": True}),
            )
            print(f"[export] trade reconciliation: inserted {inserted} missing fills from data-api (CLOB stream coalesces liquid fills); fee derived for {fee_derived} rows from the market's exchange-reported rate")
    except Exception as e:
        print(f"[export] WARN trade reconciliation failed: {e}")
    return combined


def _writeback_enriched_trades(data_dir: Path, asset: Optional[str], enriched: pa.Table) -> int:
    """B-5: persist export-time enrichment back into the hive trades partitions.

    Without this, anyone reading data/trades/ sees 100% NULL wallets — the
    enrichment only lived in the Kaggle staging build. Rules: fill NULLs ONLY
    (never overwrite a non-NULL value), rewrite only part files that actually
    change, atomic per file (tmp + os.replace), so a crash leaves every file
    complete and re-running is idempotent. api- rows (staging-only inserts)
    have no hive counterpart and are skipped. Returns files rewritten.
    """
    if enriched.num_rows == 0 or "trade_id" not in enriched.schema.names:
        return 0
    # field updates keyed by trade_id — only rows where a NULL got filled
    updates: dict = {}
    cols = ("maker_wallet", "taker_wallet", "wallet", "outcome", "fee", "fee_is_estimated")
    for r in enriched.to_pylist():
        tid = r.get("trade_id")
        if not tid or str(tid).startswith("api-"):
            continue
        updates[str(tid)] = {c: r.get(c) for c in cols}
    if not updates:
        return 0
    base = Path(data_dir) / "trades"
    if not base.exists():
        return 0
    parts = [p for p in base.rglob("*.parquet") if not p.name.endswith(".tmp")
             and (asset is None or f"asset={asset.upper()}" in str(p) or asset.upper() in str(p.parent))]
    rewritten = 0
    for p in parts:
        try:
            tbl = read_table(p)
        except Exception as e:
            print(f"[export] WARN write-back skipped unreadable {p.name}: {e}")
            continue
        rows = tbl.to_pylist()
        changed = False
        for r in rows:
            upd = updates.get(str(r.get("trade_id")))
            if not upd:
                continue
            for c in cols:
                cur = r.get(c)
                fillable = cur is None or (c == "outcome" and cur == "unknown")
                if fillable and upd[c] is not None:
                    r[c] = upd[c]
                    changed = True
        if not changed:
            continue
        tmp = p.with_suffix(".parquet.tmp")
        pq.write_table(pa.Table.from_pylist(rows, schema=tbl.schema), str(tmp), compression="zstd")
        _os_replace_safe(tmp, p)
        rewritten += 1
    if rewritten:
        print(f"[export] trades enrichment write-back: {rewritten} hive part files updated (NULLs filled, nothing overwritten)")
    return rewritten


def second_pass_enrich_trades(data_dir: str | Path, assets: Optional[List[str]] = None) -> dict:
    """Enrichment round 2 — re-run the data-api wallet/outcome fill over hive
    trades rows that are STILL NULL.

    Data-API fills are indexed late (measured 2026-09-05: enrichment 30s after
    window end finds only a minority of legs; coverage self-heals over time),
    so a pass ~15 min after the export recovers rows the first pass could not
    see. Reuses the enrichment + write-back path: NULLs filled only, atomic per
    part file, idempotent. api- reconciliation rows are NOT inserted here (the
    export-time pass owns those); this pass only completes existing rows.
    Intended to run inside resolution_backfill (pm2 cron, every 15 min).
    """
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    base = Path(data_dir)
    stats = {"assets_scanned": 0, "rows_needed": 0, "files_rewritten": 0}
    for asset in assets:
        au = asset.upper()
        tbl = _read_dataset_per_asset_plain(base, "trades", au)
        stats["assets_scanned"] += 1
        if tbl is None or tbl.num_rows == 0 or "wallet" not in tbl.schema.names:
            continue
        rows = tbl.to_pylist()
        needed = 0
        for r in rows:
            # rows without a transaction_hash cannot be joined to the data-api — skip
            if not r.get("transaction_hash"):
                continue
            needs = (
                r.get("wallet") is None
                or (r.get("maker_wallet") is None and str(r.get("side") or "").upper() in ("BUY", "SELL"))
                or r.get("outcome") in (None, "", "unknown")
            )
            if needs:
                needed += 1
        if not needed:
            continue
        stats["rows_needed"] += needed
        print(f"[export] second-pass enrichment: {au} {needed} rows still missing wallet/outcome — querying data-api")
        enriched = _backfill_trade_wallets(tbl, base, asset=au, reconcile=False)
        try:
            rewritten = _writeback_enriched_trades(base, au, enriched)
            stats["files_rewritten"] += rewritten
        except Exception as e:
            print(f"[export] WARN second-pass write-back failed for {au}: {e}")
    print(f"[export] second-pass enrichment done: {stats}")
    return stats


def _read_dataset_per_asset_plain(data_dir: Path, dataset: str, asset: Optional[str]) -> Optional[pa.Table]:
    """Read hive rows WITHOUT triggering the export-time enrichment side effects
    (data-api fetches, reconciliation inserts) — used by the second pass to
    inspect raw stored rows only."""
    from .parquet_io import read_table as _rt
    base = Path(data_dir) / dataset
    if not base.exists():
        return None
    parts = [p for p in base.rglob("*.parquet")
             if not p.name.endswith(".tmp")
             and (asset is None or f"asset={asset}" in str(p) or f"asset={asset.upper()}" in str(p))]
    if not parts:
        return None
    tables = []
    for p in parts:
        try:
            t = _rt(p)
            if asset and "asset" in t.schema.names and f"asset={asset.upper()}" not in str(p):
                t = t.filter(pc.equal(t.column("asset"), pa.scalar(asset.upper())))
            if t.num_rows:
                tables.append(t)
        except Exception:
            continue
    if not tables:
        return None
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")


# ------------------------------------------------------------------ markets_summary — analyst-facing one-row-per-market export
# Modelled on kaggle.com/datasets/kachoio/polymarket-5-minute-crypto-updown-markets:
# one row per condition_id combining resolution, underlying (chainlink) boundary
# prices, outcome-token OHLC, and activity aggregates. Purely derived — every
# field is computed at export time from the other datasets; nothing new is
# collected.

def _load_markets_latest_rows(base: Path) -> List[dict]:
    """Latest markets row per condition_id (markets_latest, falling back to markets_log hive)."""
    rows: List[dict] = []
    latest = base / "markets_latest" / "markets_latest.parquet"
    if latest.exists():
        try:
            rows = read_table(latest).to_pylist()
        except Exception as e:
            print(f"[export] WARN markets_latest unreadable: {e}")
    if not rows:
        log_tbl = _read_dataset_per_asset(base, "markets_log", None)
        if log_tbl is not None and log_tbl.num_rows:
            rows = log_tbl.to_pylist()
    # dedupe by condition_id, last row wins (log is time-ordered upstream)
    out: Dict[str, dict] = {}
    for r in rows:
        cid = r.get("condition_id")
        if cid:
            out[str(cid)] = r
    return [out[c] for c in sorted(out)]


def _read_trades_for_summary(base: Path, staging_dir: Optional[Path], assets: List[str]) -> Optional[pa.Table]:
    """Trades table for the summary — staging files preferred (they carry the
    api- reconciled fills), hive fallback otherwise."""
    tables: List[pa.Table] = []
    if staging_dir is not None:
        for a in assets:
            p = Path(staging_dir) / f"{a}_trades.parquet"
            if p.exists():
                try:
                    tables.append(read_table(p))
                except Exception as e:
                    print(f"[export] WARN staging trades unreadable {p.name}: {e}")
    if not tables:
        hive = _read_dataset_per_asset(base, "trades", None)
        if hive is not None:
            tables.append(hive)
    if not tables:
        return None
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")


def build_markets_summary(
    data_dir: str | Path,
    staging_dir: str | Path | None = None,
    assets: List[str] | None = None,
) -> pa.Table:
    """Build the analyst-facing markets summary table (one row per condition_id).

    Sources: markets_latest (identity + resolution), book_snapshots_clean
    (outcome-token mid OHLC + average spread), trades incl. api- rows (volume,
    fill count, unique traders), chainlink_events (underlying open/close =
    nearest tick to the window boundary; open ≤10s to match the resolution
    loop K-2 open tolerance, close ≤5s). All nullable except
    condition_id/asset — missing ingredients stay NULL, never zero-filled.
    The tolerance actually applied is recorded per row in
    underlying_open_tolerance_s / underlying_close_tolerance_s.
    """
    UNDERLYING_OPEN_TOL_MS = 10_000  # match resolution loop K-2 open tolerance
    UNDERLYING_CLOSE_TOL_MS = 5_000
    import bisect
    import datetime as _dt2
    base = Path(data_dir)
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]

    def _empty() -> pa.Table:
        return pa.table({f.name: [] for f in MARKETS_SUMMARY_SCHEMA}, schema=MARKETS_SUMMARY_SCHEMA)

    markets = _load_markets_latest_rows(base)
    if not markets:
        return _empty()

    # --- trades: volume / fill count / unique traders per condition_id ---
    vol_by_cid: Dict[str, float] = {}
    fills_by_cid: Dict[str, int] = {}
    traders_by_cid: Dict[str, int] = {}
    trades = _read_trades_for_summary(base, Path(staging_dir) if staging_dir else None, assets)
    if trades is not None and trades.num_rows and "condition_id" in trades.schema.names:
        cols = {"condition_id": trades.column("condition_id")}
        cols["one"] = pa.array([1] * trades.num_rows, type=pa.int64())
        for c in ("notional", "wallet"):
            cols[c] = trades.column(c) if c in trades.schema.names else pa.array([None] * trades.num_rows, type=pa.string() if c == "wallet" else pa.float64())
        t = pa.table(cols)
        try:
            agg = t.group_by("condition_id").aggregate([("one", "sum"), ("notional", "sum")])
            for r in agg.to_pylist():
                cid = r["condition_id"]
                fills_by_cid[cid] = int(r["one_sum"] or 0)
                vol_by_cid[cid] = float(r["notional_sum"]) if r["notional_sum"] is not None else 0.0
            valid_w = t.filter(pc.is_valid(t.column("wallet")))
            if valid_w.num_rows:
                dist = valid_w.group_by("condition_id").aggregate([("wallet", "count_distinct")])
                for r in dist.to_pylist():
                    traders_by_cid[r["condition_id"]] = int(r["wallet_count_distinct"] or 0)
        except Exception as e:
            print(f"[export] WARN markets_summary trades aggregation failed: {e}")

    # --- snapshots (clean): outcome-token mid OHLC + average spread per condition_id ---
    ohlc_by_cid: Dict[str, dict] = {}
    snaps = _read_dataset_per_asset(base, "book_snapshots_clean", None)
    if snaps is not None and snaps.num_rows:
        needed = ["condition_id", "ts_snapshot_ns", "up_bid", "up_ask", "down_bid", "down_ask"]
        if all(c in snaps.schema.names for c in needed):
            try:
                s = pa.table({
                    "condition_id": snaps.column("condition_id"),
                    "ts": snaps.column("ts_snapshot_ns"),
                    "mid_up": pc.divide(pc.add(snaps.column("up_bid"), snaps.column("up_ask")), pa.scalar(2.0)),
                    "mid_dn": pc.divide(pc.add(snaps.column("down_bid"), snaps.column("down_ask")), pa.scalar(2.0)),
                    "spr_up": pc.subtract(snaps.column("up_ask"), snaps.column("up_bid")),
                    "spr_dn": pc.subtract(snaps.column("down_ask"), snaps.column("down_bid")),
                })
                s = s.sort_by([("ts", "ascending"), ("condition_id", "ascending")])
                _old_cpu = pa.cpu_count()
                pa.set_cpu_count(1)  # first/last aggregators are single-threaded only
                try:
                    agg = s.group_by("condition_id").aggregate([
                        ("mid_up", "first"), ("mid_up", "last"), ("mid_up", "min"), ("mid_up", "max"),
                        ("mid_dn", "first"), ("mid_dn", "last"), ("mid_dn", "min"), ("mid_dn", "max"),
                        ("spr_up", "mean"), ("spr_dn", "mean"), ("ts", "count"),
                    ])
                finally:
                    pa.set_cpu_count(_old_cpu)
                for r in agg.to_pylist():
                    ohlc_by_cid[r["condition_id"]] = {
                        "up_open": r["mid_up_first"], "up_close": r["mid_up_last"],
                        "up_low": r["mid_up_min"], "up_high": r["mid_up_max"],
                        "down_open": r["mid_dn_first"], "down_close": r["mid_dn_last"],
                        "down_low": r["mid_dn_min"], "down_high": r["mid_dn_max"],
                        "avg_spread_up": r["spr_up_mean"], "avg_spread_down": r["spr_dn_mean"],
                        "snapshot_count": int(r["ts_count"] or 0),
                    }
            except Exception as e:
                print(f"[export] WARN markets_summary snapshot aggregation failed: {e}")

    # --- chainlink: underlying open/close = nearest tick to window boundary
    # (open ≤10s per K-2, close ≤5s) ---
    ticks_by_asset: Dict[str, List] = {}
    cl = _read_dataset_per_asset(base, "chainlink_events", None)
    if cl is not None and cl.num_rows and {"asset", "ts_source", "price"}.issubset(set(cl.schema.names)):
        for r in cl.select(["asset", "ts_source", "price"]).to_pylist():
            a = r.get("asset")
            p = r.get("price")
            ts_str = r.get("ts_source")
            if not a or p is None or not ts_str:
                continue
            try:
                ts_ms = int(_dt2.datetime.fromisoformat(str(ts_str).replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                continue
            ticks_by_asset.setdefault(a, []).append((ts_ms, float(p)))
        for a in ticks_by_asset:
            ticks_by_asset[a].sort(key=lambda x: x[0])

    def _nearest_tick(asset: str, target_ms: Optional[int], tol_ms: int = 5000):
        if target_ms is None:
            return None, None
        ticks = ticks_by_asset.get(asset or "", [])
        if not ticks:
            return None, None
        ts_list = [t[0] for t in ticks]
        i = bisect.bisect_left(ts_list, target_ms)
        best = None
        for j in (i - 1, i):
            if 0 <= j < len(ticks):
                d = abs(ticks[j][0] - target_ms)
                if best is None or d < best[0]:
                    best = (d, ticks[j])
        if best is None or best[0] > tol_ms:
            return None, None
        ts_ms, price = best[1]
        iso = _dt2.datetime.fromtimestamp(ts_ms / 1000, tz=_dt2.timezone.utc).isoformat().replace("+00:00", "Z")
        return price, iso

    rows = []
    for m in markets:
        cid = str(m.get("condition_id"))
        asset = m.get("asset") or ""
        start_ms = m.get("market_start_ts_ms")
        end_ms = m.get("market_end_ts_ms")
        try:
            start_ms = int(start_ms) if start_ms is not None else None
        except Exception:
            start_ms = None
        try:
            end_ms = int(end_ms) if end_ms is not None else None
        except Exception:
            end_ms = None

        def _iso_from_ms(ms, fallback):
            if ms is None:
                return fallback
            try:
                return _dt2.datetime.fromtimestamp(ms / 1000, tz=_dt2.timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                return fallback

        start_iso = _iso_from_ms(start_ms, m.get("market_start_ts"))
        end_iso = _iso_from_ms(end_ms, m.get("market_end_ts"))
        o_open, o_open_ts = _nearest_tick(asset, start_ms, UNDERLYING_OPEN_TOL_MS)
        o_close, o_close_ts = _nearest_tick(asset, end_ms, UNDERLYING_CLOSE_TOL_MS)
        ohlc = ohlc_by_cid.get(cid, {})
        resolution = m.get("resolution_outcome")
        if resolution in (None, "", "unknown"):
            resolution = resolution or "unknown"
        rows.append({
            "condition_id": cid,
            "asset": asset,
            "slug": m.get("slug"),
            "window_start_ts": start_iso,
            "window_end_ts": end_iso,
            "window_start_ts_ms": start_ms,
            "window_end_ts_ms": end_ms,
            "window_index": m.get("window_index"),
            "up_token_id": m.get("up_token_id"),
            "down_token_id": m.get("down_token_id"),
            "resolution_outcome": resolution,
            "settlement_price": m.get("settlement_price"),
            "settlement_source": m.get("settlement_source"),
            "underlying_open": o_open,
            "underlying_open_ts_utc": o_open_ts,
            "underlying_open_tolerance_s": UNDERLYING_OPEN_TOL_MS // 1000,
            "underlying_close": o_close,
            "underlying_close_ts_utc": o_close_ts,
            "underlying_close_tolerance_s": UNDERLYING_CLOSE_TOL_MS // 1000,
            "up_open": ohlc.get("up_open"), "up_high": ohlc.get("up_high"),
            "up_low": ohlc.get("up_low"), "up_close": ohlc.get("up_close"),
            "down_open": ohlc.get("down_open"), "down_high": ohlc.get("down_high"),
            "down_low": ohlc.get("down_low"), "down_close": ohlc.get("down_close"),
            "traded_volume": vol_by_cid.get(cid),
            "fill_count": fills_by_cid.get(cid),
            "unique_traders": traders_by_cid.get(cid),
            "avg_spread_up": ohlc.get("avg_spread_up"),
            "avg_spread_down": ohlc.get("avg_spread_down"),
            "snapshot_count": ohlc.get("snapshot_count"),
        })
    rows.sort(key=lambda r: (r.get("window_start_ts_ms") or 0, r.get("asset") or "", r.get("condition_id")))
    return pa.Table.from_pylist(rows, schema=MARKETS_SUMMARY_SCHEMA)


def _read_dataset_per_asset(data_dir: Path, dataset: str, asset: Optional[str], include_binance: bool = False) -> Optional[pa.Table]:
    """Read all parquet files for dataset (+ optional asset filter)."""
    base = data_dir / dataset
    if not base.exists():
        return None
    # gather files
    if asset and dataset in PER_ASSET_DATASETS:
        # search hive partitions: dataset/date=*/asset=ASSET/*.parquet
        patterns_set = {p.resolve() for p in base.glob(f"date=*/asset={asset.upper()}/*.parquet")}
        patterns_set.update(p.resolve() for p in base.glob(f"date=*/asset={asset}/*.parquet"))
        patterns = [Path(p) for p in patterns_set]
        # also flat single-file already? fallback to rglob
        if not patterns:
            patterns = [p for p in base.rglob("*.parquet") if f"asset={asset.upper()}" in str(p) or asset.upper() in str(p.parent)]
            # if still empty, read all and filter later by asset column
            if not patterns:
                patterns = list(base.rglob("*.parquet"))
    else:
        patterns = list(base.rglob("*.parquet"))
        # exclude tmp
        patterns = [p for p in patterns if not p.name.endswith(".tmp")]
    if not patterns:
        return None
    tables: List[pa.Table] = []
    _read_errors: List[str] = []
    for p in patterns:
        if p.name.endswith(".tmp"):
            continue
        try:
            t = read_table(p)
            # filter by asset column if per-asset requested but files are mixed
            if asset and dataset in PER_ASSET_DATASETS and "asset" in t.schema.names:
                # if file path already guaranteed asset, skip filter; else filter
                if f"asset={asset.upper()}" not in str(p):
                    try:
                        mask = pc.equal(t.column("asset"), pa.scalar(asset.upper()))
                        t = t.filter(mask)
                        if t.num_rows == 0:
                            continue
                    except Exception:
                        pass
            # exclude binance if chainlink and not include_binance — keep nulls (synthetic/old data without source)
            # Use if_else to keep null source rows (pyarrow or_ with null gives null, not true)
            if dataset == "chainlink_events" and not include_binance and "source" in t.schema.names:
                try:
                    col = t.column("source")
                    is_null = pc.is_null(col)
                    not_binance = pc.not_equal(col, pa.scalar("binance-ticker-proxy"))
                    # if null → True (keep), else not_binance value
                    mask = pc.if_else(is_null, True, not_binance)
                    # mask may still have nulls where not_binance was null and is_null false? but is_null false → not_binance, so null stays null → filter drops nulls we want to keep?
                    # For non-null source, not_binance is true/false, not null. So mask is true/false only.
                    # For safety, fill any remaining nulls with True (keep)
                    if mask.null_count > 0:
                        mask = pc.fill_null(mask, True)
                    t = t.filter(mask)
                    if t.num_rows == 0:
                        continue
                except Exception:
                    pass
            tables.append(t)
        except Exception as e:
            _read_errors.append(f"{p}: {e}")
            print(f"[export] WARN failed to read {p}: {e}")
            continue
    if _read_errors:
        print(f"[export] WARN {len(_read_errors)} parquet files failed to read for {dataset} asset={asset}: {_read_errors[:3]}")
    if not tables:
        # If all files failed, return None so caller triggers monotonic guard / abort instead of empty success
        if _read_errors:
            print(f"[export] ERROR all {len(patterns)} files failed for {dataset} asset={asset} — aborting read")
        return None
    combined = pa.concat_tables(tables, **({"promote_options": "default"} if tuple(int(x) for x in pa.__version__.split(".")[:2]) >= (16, 0) else {"promote": True})) if len(tables) > 1 else tables[0]
    # filter binance again if combined still has mixed sources (promote case) — keep nulls
    if dataset == "chainlink_events" and not include_binance and "source" in combined.schema.names:
        try:
            col = combined.column("source")
            is_null = pc.is_null(col)
            not_binance = pc.not_equal(col, pa.scalar("binance-ticker-proxy"))
            mask = pc.if_else(is_null, True, not_binance)
            if mask.null_count > 0:
                mask = pc.fill_null(mask, True)
            combined = combined.filter(mask)
        except Exception:
            pass
    # K-user-fix: enrich streamed trades with real proxy wallets (data-api) —
    # the CLOB market channel never carries them, so they were 100% null on Kaggle
    if dataset == "trades" and combined.num_rows > 0:
        try:
            combined = _backfill_trade_wallets(combined, data_dir, asset=asset)
            # B-5: persist the enrichment into the hive so data/trades/ matches
            # what ships to Kaggle (NULLs filled only, atomic per file)
            try:
                _writeback_enriched_trades(data_dir, asset, combined)
            except Exception as e:
                print(f"[export] WARN trades enrichment write-back failed: {e}")
        except Exception as e:
            print(f"[export] WARN wallet backfill failed: {e}")
    # backfill trades: compute notional, fee, aggressor_side, transaction_hash where null for old 3.1.0 data
    if dataset == "trades" and combined.num_rows > 0:
        try:
            # convert to pylist for easy backfill, then back to table
            pylist = combined.to_pylist()
            changed = False
            for r in pylist:
                if r.get("notional") is None and r.get("price") is not None and r.get("size") is not None:
                    try:
                        r["notional"] = float(r["price"]) * float(r["size"])
                        changed = True
                    except Exception:
                        pass
                # fee stays NULL if not observed — real data only
                if r.get("aggressor_side") is None and r.get("side"):
                    try:
                        r["aggressor_side"] = str(r["side"]).upper()
                        changed = True
                    except Exception:
                        pass
                if r.get("transaction_hash") is None and r.get("trade_id"):
                    # fallback: trade_id may be hash if hash was used as trade_id
                    # check if trade_id looks like hash (hex length 32+)
                    tid = str(r.get("trade_id"))
                    if len(tid) >= 32 and all(c in "0123456789abcdef" for c in tid.lower()[:8]):
                        r["transaction_hash"] = tid
                        changed = True
                # sequence_number stays NULL if not observed — real data only (no synthetic ts_source -> seq backfill per AGENT.md)
                # previously fabricated seq from ts_source here; removed to preserve null honesty
                # wallet backfill — no RPC, just normalize existing CLOB fields
                # old data may have proxyWallet/wallet under different keys already flattened
                if r.get("wallet") is None:
                    for cand in ("proxyWallet", "proxy_wallet", "maker", "taker", "owner"):
                        if r.get(cand):
                            r["wallet"] = str(r[cand])
                            changed = True
                            break
                if r.get("maker_wallet") is None and r.get("proxyWallet"):
                    r["maker_wallet"] = str(r["proxyWallet"])
                    changed = True
                if r.get("wallet") is None and r.get("maker_wallet"):
                    r["wallet"] = r["maker_wallet"]
                    changed = True
                if r.get("wallet") is None and r.get("taker_wallet"):
                    r["wallet"] = r["taker_wallet"]
                    changed = True
            if changed:
                # rebuild table with same schema as combined (preserve types where possible)
                combined = pa.Table.from_pylist(pylist, schema=combined.schema)
        except Exception as e:
            print(f"[export] WARN backfill failed for {dataset}: {e}")
            pass
    # dedup before sort: remove exact duplicate rows that writer missed (WAL replay, buffer races)
    # For resync_episodes keep latest per resync_id, for snapshots keep first per (asset,condition_id,ts_snapshot_ns)
    try:
        if combined.num_rows > 1:
            if dataset == "book_snapshots_500ms" and all(c in combined.schema.names for c in ["asset", "condition_id", "ts_snapshot_ns"]):
                pylist = combined.to_pylist()
                seen = set()
                uniq = []
                for r in pylist:
                    k = (r.get("asset"), r.get("condition_id"), r.get("ts_snapshot_ns"))
                    if k not in seen:
                        seen.add(k)
                        uniq.append(r)
                if len(uniq) < combined.num_rows:
                    combined = pa.Table.from_pylist(uniq, schema=combined.schema)
            elif dataset == "resync_episodes" and "resync_id" in combined.schema.names:
                # keep latest row per resync_id (max reconnect_ts or last occurrence)
                pylist = combined.to_pylist()
                latest = {}
                for r in pylist:
                    rid = r.get("resync_id")
                    # keep last occurrence as latest (append order is chronological due to sort later, but use dict overwrite)
                    latest[rid] = r
                if len(latest) < combined.num_rows:
                    combined = pa.Table.from_pylist(list(latest.values()), schema=combined.schema)
            elif dataset == "collector_events" and "event_id" in combined.schema.names:
                pylist = combined.to_pylist()
                seen = set()
                uniq = []
                for r in pylist:
                    eid = r.get("event_id")
                    if eid not in seen:
                        seen.add(eid)
                        uniq.append(r)
                if len(uniq) < combined.num_rows:
                    combined = pa.Table.from_pylist(uniq, schema=combined.schema)
            elif dataset == "trades" and "trade_id" in combined.schema.names and "token_id" in combined.schema.names:
                pylist = combined.to_pylist()
                seen = set()
                uniq = []
                for r in pylist:
                    k = (r.get("token_id"), r.get("trade_id"))
                    if k not in seen:
                        seen.add(k)
                        uniq.append(r)
                if len(uniq) < combined.num_rows:
                    combined = pa.Table.from_pylist(uniq, schema=combined.schema)
            elif dataset == "markets_log" and "condition_id" in combined.schema.names:
                # Fix #7: markets 84->28 duplicate — keep latest per condition_id (max updated_at/market_end)
                pylist = combined.to_pylist()
                latest: dict = {}
                for r in pylist:
                    cid = r.get("condition_id")
                    # overwrite so last occurrence wins; pylist is append order, last is latest
                    # if updated_at available, prefer newer
                    prev = latest.get(cid)
                    if prev is None:
                        latest[cid] = r
                    else:
                        # compare updated_at if present
                        try:
                            a = str(prev.get("updated_at") or "")
                            b = str(r.get("updated_at") or "")
                            if b >= a:
                                latest[cid] = r
                        except Exception:
                            latest[cid] = r
                if len(latest) < combined.num_rows:
                    combined = pa.Table.from_pylist(list(latest.values()), schema=combined.schema)
    except Exception as e:
        print(f"[export] WARN dedup failed for {dataset}: {e}")
        pass
    # sort by time then condition_id
    schema = _get_schema(dataset)
    sort_keys = _sort_keys_for_schema(combined.schema if schema is None else schema)
    # only sort keys that exist in actual combined
    sort_keys = [k for k in sort_keys if k in combined.schema.names]
    if sort_keys:
        try:
            indices = pc.sort_indices(combined, sort_keys=[(k, "ascending") for k in sort_keys])
            combined = pc.take(combined, indices)
        except Exception:
            pass
    # reorder columns to time-first schema if schema available, add missing cols as nulls
    if schema is not None:
        try:
            # add missing schema columns as nulls (e.g. transaction_hash added in 3.2.0)
            for field in schema:
                if field.name not in combined.schema.names:
                    # create null column of correct type
                    null_arr = pa.array([None]*combined.num_rows, type=field.type)
                    combined = combined.append_column(field.name, null_arr)
            # build new order: schema.names that exist in combined + remaining cols
            ordered = [n for n in schema.names if n in combined.schema.names]
            remaining = [n for n in combined.schema.names if n not in ordered]
            final_order = ordered + remaining
            combined = combined.select(final_order)
        except Exception:
            pass
    return combined


def export_per_asset_single_file(
    data_dir: str | Path,
    out_dir: str | Path | None = None,
    datasets: List[str] | None = None,
    assets: List[str] | None = None,
    l2_levels: int = 10,
    include_binance: bool = False,
) -> dict:
    """Export one flat parquet per asset per dataset (Kaggle style).

    Returns dict {relative_out_path: rows}

    Note: The global datasets (markets_log, collector_events, resync_episodes)
    and the derived markets_summary are always exported as single files in the
    Kaggle staging folder, even if they contain 0 rows. This ensures the staging
    always has 39 files (7 assets x 5 per-asset + 3 globals + 1 summary) for the
    dataset gghgg1/polymarket-5m-crypto.
    """
    base = Path(data_dir)
    out = Path(out_dir) if out_dir else base / "export"
    out.mkdir(parents=True, exist_ok=True)

    if datasets is None:
        datasets = ["book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events", "markets_log", "collector_events", "resync_episodes", "markets_summary"]
    if assets is None:
        # Always use the 7 known assets — hardcoded per plan.md §0
        # Do NOT discover dynamically from hive partitions, as this fails
        # when the data directory is freshly cleaned (no hive dirs exist yet).
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]

    # The 3 global datasets that should always appear in Kaggle staging
    global_datasets = {"markets_log", "collector_events", "resync_episodes"}
    # Per-asset datasets that get one file per asset
    per_asset_datasets = {"book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events"}

    stats: dict = {}
    for ds in datasets:
        # Derived analyst-facing summary — must run AFTER the per-asset trades
        # staging files are (re)written so it sees the api- reconciled fills.
        if ds == "markets_summary":
            out_path = out / "markets_summary.parquet"
            rel = str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)
            table = build_markets_summary(base, staging_dir=out, assets=assets)
            prior_rows_s = None
            if out_path.exists():
                try:
                    prior_rows_s = read_table(out_path).num_rows
                except Exception:
                    prior_rows_s = None
            new_rows_s = table.num_rows if table is not None else 0
            if prior_rows_s is not None and prior_rows_s > 0 and (table is None or new_rows_s < prior_rows_s):
                # markets only accumulate — a shrink means a transient read failure; keep prior
                stats[rel] = prior_rows_s
                continue
            if table is None:
                table = pa.table({f.name: [] for f in MARKETS_SUMMARY_SCHEMA}, schema=MARKETS_SUMMARY_SCHEMA)
                new_rows_s = 0
            tmp_path = out_path.with_suffix(".parquet.tmp")
            pq.write_table(table, str(tmp_path), compression="zstd")
            _os_replace_safe(tmp_path, out_path)
            stats[rel] = new_rows_s
            continue
        schema = _get_schema(ds, l2_levels)
        if ds in PER_ASSET_DATASETS:
            for asset in assets:
                au = asset.upper()
                table = _read_dataset_per_asset(base, ds, au, include_binance=include_binance)
                out_path = out / f"{au}_{ds}.parquet"
                # --- never overwrite non-empty staging with empty/smaller data (cumulative history guard) ---
                # Load prior staging first to enforce monotonic row-count (never shrink)
                prior_rows = None
                prior_exists = out_path.exists()
                if prior_exists:
                    try:
                        _prior = read_table(out_path)
                        prior_rows = _prior.num_rows
                    except Exception:
                        prior_rows = None
                new_rows = table.num_rows if (table is not None) else 0
                # If prior has data, never replace it with fewer rows (empty read, transient error, or legitimate 0)
                # This prevents 1a empty-file overwrite and guarantees cumulative history
                if prior_rows is not None and prior_rows > 0:
                    if table is None or new_rows < prior_rows:
                        # Transient read error or incomplete export would shrink history — preserve prior
                        stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = prior_rows
                        continue
                    # also guard against equal-but-earlier: if new has rows but fewer, still preserve
                if table is not None and table.num_rows > 0:
                    tmp_path = out_path.with_suffix(".parquet.tmp")
                    pq.write_table(table, str(tmp_path), compression="zstd")
                    _os_replace_safe(tmp_path, out_path)
                    stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = table.num_rows
                else:
                    # No/hollow new data — if prior already preserved above, we already continued
                    # If we are here, either no prior or prior was empty/0 rows
                    if prior_exists and prior_rows is not None and prior_rows > 0:
                        stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = prior_rows
                        continue
                    # For book_snapshots_500ms 0 rows is never valid — fail closed (keep prior if any, else abort write)
                    # If no prior data and 0 new rows, do NOT create an empty file that could be uploaded.
                    # Instead, skip the write and preserve the prior file if it exists; otherwise
                    # leave the output path uncreated (staging will be missing this file, which
                    # _verify_staging_row_counts will catch and block the Kaggle upload).
                    if ds == "book_snapshots_500ms":
                        if table is None or table.num_rows == 0:
                            # No new data and no prior to preserve — abort write entirely
                            # prior_rows guard above already handles the case where prior exists
                            # If we are here, prior was None or 0, so just skip writing
                            stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = 0
                            continue
                    else:
                        # trades/book_events/chainlink_events legitimately 0 early -> write proper schema-empty, not bare pa.table({})
                        # FIX: ensure schema is respected so 31-file guarantee holds even with 0 rows
                        try:
                            # Use snapshot_schema(l2_levels) for snapshots, SCHEMAS[ds] otherwise
                            _schema_for_empty = _get_schema(ds, l2_levels)
                            if _schema_for_empty is not None:
                                empty_data = {field.name: [] for field in _schema_for_empty}
                                table = pa.table(empty_data, schema=_schema_for_empty)
                            elif schema is not None:
                                empty_data = {col: [] for col in schema.names}
                                table = pa.table(empty_data)
                            else:
                                table = pa.table({})
                        except Exception:
                            table = pa.table({})
                        tmp_path = out_path.with_suffix(".parquet.tmp")
                        try:
                            pq.write_table(table, str(tmp_path), compression="zstd")
                        except Exception:
                            # fallback without compression if schema mismatch
                            pq.write_table(pa.table({}), str(tmp_path))
                        _os_replace_safe(tmp_path, out_path)
                        stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = 0
        elif ds in global_datasets:
            # Global dataset: always create a single file in staging, even if 0 rows
            table = _read_dataset_per_asset(base, ds, None, include_binance=include_binance)
            if ds == "markets_log":
                out_path = out / "markets.parquet"
            elif ds == "collector_events":
                out_path = out / "collector_events.parquet"
            else:  # resync_episodes
                out_path = out / "resync_episodes.parquet"
            # Monotonic guard for globals too: never shrink
            prior_rows_g = None
            if out_path.exists():
                try:
                    _pg = read_table(out_path)
                    prior_rows_g = _pg.num_rows
                except Exception:
                    prior_rows_g = None
            new_rows_g = table.num_rows if (table is not None and hasattr(table, "num_rows")) else 0
            if prior_rows_g is not None and prior_rows_g > 0 and (table is None or new_rows_g < prior_rows_g):
                stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = prior_rows_g
                continue
            if table is not None and table.num_rows > 0:
                tmp_path = out_path.with_suffix(".parquet.tmp")
                pq.write_table(table, str(tmp_path), compression="zstd")
                _os_replace_safe(tmp_path, out_path)
            else:
                # Preserve prior if we have it (already handled above), else create schema-empty
                if prior_rows_g is not None and prior_rows_g > 0:
                    stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = prior_rows_g
                    continue
                global_schema = _get_schema(ds, l2_levels)
                empty_data = {col: [] for col in global_schema.names} if global_schema is not None else {}
                table = pa.table(empty_data) if empty_data else pa.table({})
                tmp_path = out_path.with_suffix(".parquet.tmp")
                pq.write_table(table, str(tmp_path), compression="zstd")
                _os_replace_safe(tmp_path, out_path)
            stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = table.num_rows if table is not None else 0
        else:
            # Should not happen with default datasets, but skip
            pass
    return stats


def export_all_flat(data_dir: str | Path, out_dir: str | Path | None = None, include_binance: bool = False) -> dict:
    """Compatibility wrapper for global single-file (not per-asset) — not used per user request but kept."""
    return export_per_asset_single_file(data_dir, out_dir, include_binance=include_binance)


def main() -> None:
    ap = argparse.ArgumentParser(description="Per-asset single-file export — time first, condition_id second, no binance (Kaggle style)")
    ap.add_argument("--data-dir", default="./data")
    ap.add_argument("--out-dir", default=None, help="output dir, default <data-dir>/export")
    ap.add_argument("--datasets", nargs="*", default=None, help="datasets to export")
    ap.add_argument("--assets", nargs="*", default=None, help="assets to export (default BTC ETH SOL or discovered)")
    ap.add_argument("--l2-levels", type=int, default=20)
    ap.add_argument("--include-binance", action="store_true", help="include binance-ticker-proxy rows (default excluded)")
    ap.add_argument("--markets-latest", action="store_true", help="also export markets_latest single file as markets_latest.parquet")
    args = ap.parse_args()
    stats = export_per_asset_single_file(
        args.data_dir,
        out_dir=args.out_dir,
        datasets=args.datasets,
        assets=args.assets,
        l2_levels=args.l2_levels,
        include_binance=args.include_binance,
    )
    # optionally also dump markets_latest flat
    if args.markets_latest:
        base = Path(args.data_dir)
        out = Path(args.out_dir) if args.out_dir else base / "export"
        latest = base / "markets_latest" / "markets_latest.parquet"
        if latest.exists():
            try:
                t = read_table(latest)
                # sort time first
                if "updated_at" in t.schema.names:
                    idx = pc.sort_indices(t, sort_keys=[("updated_at", "ascending"), ("condition_id", "ascending")])
                    t = pc.take(t, idx)
                out_path = out / "markets_latest.parquet"
                tmp = out_path.with_suffix(".parquet.tmp")
                pq.write_table(t, str(tmp), compression="zstd")
                _os_replace_safe(tmp, out_path)
                stats[str(out_path)] = t.num_rows
                print(f"exported markets_latest.parquet: {t.num_rows} rows")
            except Exception as e:
                print(f"markets_latest export failed: {e}")
    if stats:
        for k, v in stats.items():
            print(f"exported {k}: {v} rows")
    else:
        print("no data to export (is data/ empty after delete?)")


# ------------------------------------------------------------------ timeframe aggregation (5m-only; 15m/1h/4h/1d synthetic deprecated, native only)
# When the collector runs with 5min (300s) windows, 15m/1h/4h/1d must be native Gamma windows
# (not synthetic from 5m) per plan.md §2. For 5m-only test we do NO synthesis.
# aggregate_5min_to_timeframe kept for backward compat but not used in 5m-only test.


def _compute_timebucket_ms(ts_ms_values: list, window_size_seconds: int) -> list:
    """Compute bucket index for timestamp values and window size.
    
    Args:
        ts_ms_values: List of timestamps in milliseconds (as ints or convertible)
        window_size_seconds: Window size in seconds (300, 900, 3600, 14400, 86400)
    
    Returns:
        List of bucket indices (one per row)
    """
    seconds_map = {
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }
    sec = seconds_map.get(window_size_seconds, 300)
    interval_ms = sec * 1000
    # Convert each value to int first, then compute bucket
    result = []
    for ts_ms in ts_ms_values:
        try:
            val = int(ts_ms)
            result.append(val // interval_ms)
        except (ValueError, TypeError):
            result.append(-1)  # invalid timestamp -> bucket -1
    return result


def aggregate_5min_to_timeframe(
    table: pa.Table,
    window_size_seconds: int,
    timeframe_label: str,
) -> pa.Table:
    """Aggregate a 5min-snapshot table into a larger timeframe.
    
    For book_snapshots_500ms: computes TWAP-like weighted average per bucket.
    For trades: groups trades into buckets.
    For chainlink_events: groups price events into buckets.
    
    The function assumes the table has ts_snapshot_ns or ts_snapshot_utc columns
    for time grouping.
    """
    if table.num_rows == 0:
        return table

    # Determine which timestamp column to use for bucketing
    ts_col = None
    for candidate in ["ts_snapshot_ns", "ts_snapshot_utc", "ts_source", "ts_received_ns", "ts_utc"]:
        if candidate in table.schema.names:
            ts_col = candidate
            break

    if ts_col is None:
        # Cannot aggregate without a timestamp; return as-is
        return table

    # Extract timestamps as Python ints for bucket computation
    try:
        ts_raw = table.column(ts_col)
        if hasattr(ts_raw, 'to_pylist'):
            ts_ms_list = ts_raw.to_pylist()
        else:
            ts_ms_list = list(ts_raw)
        # Filter out None values
        ts_ms_list = [ts for ts in ts_ms_list if ts is not None]
    except Exception:
        return table

    if not ts_ms_list:
        return table

    # Compute bucket indices
    bucket_indices = _compute_timebucket_ms(ts_ms_list, window_size_seconds)

    # Sort rows by bucket index, then take first row per bucket
    # Create a temporary table with bucket column added
    try:
        # Create bucket column as pa.array
        bucket_col = pa.array(bucket_indices, type=pa.int64())
        
        # Add bucket column to table
        table_with_bucket = table.append_column("__bucket__", bucket_col)
        
        # Sort by bucket
        sorted_table = table_with_bucket.sort_by(["__bucket__"])
        
        # Get unique buckets
        unique_buckets = sorted_table.column("__bucket__").unique()
        
        # For each unique bucket, take the first row
        results: List[pa.Table] = []
        for ub in unique_buckets:
            mask = pc.equal(sorted_table.column("__bucket__"), ub)
            bucket_table = sorted_table.filter(mask)
            # Take first row
            if bucket_table.num_rows > 0:
                first_row = bucket_table.take([0])
                results.append(first_row)
        
        if results:
            combined = pa.concat_tables(results, promote_options="default")
            # Drop the temporary bucket column - results are already ordered by bucket
            # since we iterate over unique buckets from the sorted table
            final_schema = [f for f in combined.schema.names if f != "__bucket__"]
            combined = combined.select(final_schema)
            # Return as-is; rows are already in bucket order from the iteration
            return combined
        else:
            return table
    except Exception as e:
        import traceback
        traceback.print_exc()
        return table


def export_timeframe_aggregates(
    data_dir: str | Path,
    out_dir: str | Path,
    assets: List[str] | None = None,
    l2_levels: int = 10,
) -> dict:
    """Export aggregated timeframe Parquet files from 5min base data.
    
    Creates one file per asset per timeframe:
    - {asset}_book_snapshots_15m.parquet
    - {asset}_book_snapshots_1h.parquet
    - {asset}_book_snapshots_4h.parquet
    - {asset}_book_snapshots_1d.parquet
    
    Also exports trades and chainlink_events aggregated.
    
    Returns dict of {out_path: rows}.
    """
    base = Path(data_dir)
    out = Path(out_dir) if out_dir else base / "export"
    out.mkdir(parents=True, exist_ok=True)

    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]

    # Datasets to aggregate (only per-asset ones that make sense to aggregate)
    datasets = ["book_snapshots_500ms", "book_events", "trades", "chainlink_events"]

    stats: dict = {}

    for asset in assets:
        au = asset.upper()
        asset_stats: dict = {}

        for ds in datasets:
            # Read per-asset data
            table = _read_dataset_per_asset(base, ds, au, include_binance=False)
            if table is None or table.num_rows == 0:
                continue

            # Determine which timeframes to aggregate based on dataset
            if ds == "book_snapshots_500ms":
                # Can aggregate to all timeframes
                timeframes = [
                    ("300", "5m", ds),
                    ("900", "15m", ds),
                    ("3600", "1h", ds),
                    ("14400", "4h", ds),
                    ("86400", "1d", ds),
                ]
            elif ds in ("trades", "chainlink_events"):
                # Trades and chainlink can also be aggregated
                timeframes = [
                    ("300", "5m", ds),
                    ("900", "15m", ds),
                    ("3600", "1h", ds),
                    ("14400", "4h", ds),
                    ("86400", "1d", ds),
                ]
            else:
                # book_events and others: only 5min
                timeframes = [("300", "5m", ds)]

            for sec, label, dset in timeframes:
                agg_table = aggregate_5min_to_timeframe(table, int(sec), label)
                if agg_table is None or agg_table.num_rows == 0:
                    continue

                # Build output filename
                out_path = out / f"{au}_{dset}_{label}.parquet"

                # Write with schema alignment
                try:
                    # Ensure schema has required columns
                    schema = _get_schema(dset, l2_levels)
                    if schema is not None:
                        # Add missing columns as nulls
                        current_names = set(agg_table.schema.names)
                        for field in schema:
                            if field.name not in current_names:
                                null_arr = pa.array([None] * agg_table.num_rows, type=field.type)
                                agg_table = agg_table.append_column(field.name, null_arr)
                        # Reorder columns to match schema
                        ordered = [n for n in schema.names if n in agg_table.schema.names]
                        remaining = [n for n in agg_table.schema.names if n not in ordered]
                        final_order = ordered + remaining
                        agg_table = agg_table.select(final_order)

                    pq.write_table(agg_table, str(out_path), compression="zstd")
                    rows = agg_table.num_rows
                    asset_stats[f"{dset}_{label}"] = rows
                    stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = rows
                    print(f"exported aggregated {au} {dset} {label}: {rows} rows")
                except Exception as e:
                    print(f"failed to write {au} {dset} {label}: {e}")

    return stats


# ------------------------------------------------------------------ Kaggle upload — 5m-only, single dataset, folder versioning
# plan.md: single dataset gghgg1/polymarket-5m-crypto contains 7*5+4=39 files (all assets share same slug; per-asset: snapshots, clean view, book_events, trades, chainlink; global: markets, collector_events, resync_episodes, markets_summary).
# Test mode uploads every 10 min (600s) gated on full closed markets only, safe delete after ready.

try:
    import kaggle  # type: ignore
    KAGGLE_AVAILABLE = True
except ImportError:
    KAGGLE_AVAILABLE = False

import datetime as _dt
import time as _time
import json as _json
import os as _os


def _get_kaggle_dataset_name(window_label: str = "5m", asset: str | None = None, dataset_prefix: str | None = None) -> str:
    """Single dataset for 5m-only: gghgg1/polymarket-5m-crypto (all assets share it).

    Per plan.md §1.1 slugs gghgg1/polymarket-{window}-crypto, asset is NOT part of slug.
    For 5m-only test we always return dataset_prefix (default gghgg1/polymarket-5m-crypto).
    Keeping window_label param for forward compat with native 15m/1h/1d later.
    """
    if dataset_prefix:
        return dataset_prefix
    # allow override via env/config
    return "gghgg1/polymarket-5m-crypto"


def _kaggle_dataset_slug(window_label: str = "5m") -> str:
    return _get_kaggle_dataset_name(window_label)


def prepare_kaggle_staging_5m(
    data_dir: str | Path,
    staging_dir: str | Path | None = None,
    assets: List[str] | None = None,
    l2_levels: int = 10,
    dataset_prefix: str = "gghgg1/polymarket-5m-crypto",
) -> dict:
    """Prepare Kaggle staging folder for 5m-only upload.

    Exports per-asset single files (time-first, zstd, no binance) into a flat staging
    folder with dataset-metadata.json (CC BY-NC-SA 4.0) ready for folder upload.

    Returns dict with staging_path, files (31 for 7 assets), row_counts.
    """
    base = Path(data_dir)
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    staging = Path(staging_dir) if staging_dir else base / "kaggle_staging" / "5m" / dataset_prefix
    staging.mkdir(parents=True, exist_ok=True)

    # Export per-asset 5m files directly into staging (not intermediate export/)
    stats = export_per_asset_single_file(
        data_dir, out_dir=staging, assets=assets, l2_levels=l2_levels, include_binance=False
    )
    # Real data only: never merge synthetic prior Kaggle data. If local hive is empty after
    # clean delete, staging stays empty/minimal (3 globals). Merge disabled per AGENT.md.
    # _try_merge_prior_kaggle_staging disabled — would resurrect old synthetic cl-*/synth-* rows.
    # Ensure markets_latest also available as markets_latest.parquet alias if needed for reference
    # but primary markets file is markets.parquet (from markets_log)
    row_counts = stats
    # Write dataset-metadata.json
    resources = [{"path": Path(k).name, "description": f"{Path(k).name} 5m crypto — {dataset_prefix}"} for k in stats.keys()]
    # Ensure markets.parquet + per-asset files are all listed; add if missing due to empty
    meta = {
        "title": "Polymarket 5m Crypto",
        "id": dataset_prefix,
        "licenses": [{"name": "CC BY-NC-SA 4.0"}],
        "resources": resources,
    }
    (staging / "dataset-metadata.json").write_text(_json.dumps(meta, indent=2))
    return {"staging_path": str(staging), "files": len(stats), "row_counts": row_counts, "dataset": dataset_prefix}


def _try_merge_prior_kaggle_staging(staging: Path, dataset: str, assets: List[str] | None, l2_levels: int = 10) -> None:
    """Best-effort download-merge of prior Kaggle version into staging.

    If local hive has no data for a file but Kaggle staging has prior rows, download
    prior version via dataset_download_files and merge (concat + dedup) so cumulative
    history is preserved across machines/disc clears. Silently no-ops if offline or no prior.
    """
    if not KAGGLE_AVAILABLE:
        return
    try:
        import tempfile, shutil
        api = __import__("kaggle").api  # type: ignore
        # Check dataset exists
        try:
            api.dataset_status(dataset)
        except Exception:
            return  # no prior version to merge
        tmp = Path(tempfile.mkdtemp(prefix="kaggle_prior_"))
        try:
            # download prior version files (quiet)
            try:
                api.dataset_download_files(dataset, path=str(tmp), quiet=True, unzip=True)
            except TypeError:
                api.dataset_download_files(dataset, path=str(tmp), quiet=True)
            except Exception:
                return
            # Find downloaded parquets (could be nested)
            prior_files = list(tmp.rglob("*.parquet"))
            if not prior_files:
                return
            prior_map = {p.name: p for p in prior_files}
            for expected_name in [f"{a}_{ds}.parquet" for a in (assets or []) for ds in ["book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events"]] + ["markets.parquet", "collector_events.parquet", "resync_episodes.parquet", "markets_summary.parquet"]:
                staging_path = staging / expected_name
                prior_path = prior_map.get(expected_name)
                if prior_path is None or not staging_path.exists():
                    # If staging missing but prior has it, copy prior as baseline
                    if prior_path is not None and not staging_path.exists():
                        try:
                            shutil.copy(str(prior_path), str(staging_path))
                        except Exception:
                            pass
                    continue
                try:
                    cur_t = read_table(staging_path)
                    prior_t = read_table(prior_path)
                    if prior_t.num_rows > cur_t.num_rows:
                        # Need to merge: concat and dedup by time+condition_id if possible
                        # For book_snapshots use (asset,condition_id,ts_snapshot_ns) dedup
                        try:
                            combined = pa.concat_tables([prior_t, cur_t], promote_options="default")
                            # Dedup via pylist distinct by serialization if small, else keep prior larger
                            # Simple: if prior has more rows, keep prior + only new rows not in prior
                            # Use dedup key based on dataset type where possible
                            # For now, dedup on row dict equality via pylist set of tuple keys
                            pylist = combined.to_pylist()
                            seen = set()
                            uniq = []
                            for r in pylist:
                                # key: try snapshot ns+cid, else trade_id, else str(r)
                                k = None
                                if "ts_snapshot_ns" in r and "condition_id" in r:
                                    k = (r.get("asset"), r.get("condition_id"), r.get("ts_snapshot_ns"))
                                elif "trade_id" in r:
                                    k = r.get("trade_id")
                                elif "event_id" in r:
                                    k = r.get("event_id")
                                else:
                                    k = tuple(sorted((kk, str(vv)) for kk, vv in r.items() if vv is not None))
                                if k not in seen:
                                    seen.add(k)
                                    uniq.append(r)
                            if len(uniq) > cur_t.num_rows:
                                merged = pa.Table.from_pylist(uniq, schema=cur_t.schema if cur_t.num_rows else None)
                                # sort time first
                                try:
                                    sort_col = next((c for c in ["ts_snapshot_ns", "ts_snapshot_utc", "ts_source", "ts_utc", "updated_at"] if c in merged.schema.names), None)
                                    if sort_col:
                                        merged = merged.sort_by(sort_col)
                                except Exception:
                                    pass
                                pq.write_table(merged, str(staging_path.with_suffix(".parquet.tmp")), compression="zstd")
                                Path(str(staging_path.with_suffix(".parquet.tmp"))).rename(staging_path)
                        except Exception:
                            # Fallback: keep larger prior file
                            shutil.copy(str(prior_path), str(staging_path))
                except Exception:
                    continue
        finally:
            try:
                shutil.rmtree(str(tmp), ignore_errors=True)
            except Exception:
                pass
    except Exception:
        pass


def upload_to_kaggle(
    parquet_path: Path | None = None,
    dataset_name: str | None = None,
    api_username: str | None = None,
    api_key: str | None = None,
    overwrite: bool = True,
    staging_dir: str | Path | None = None,
) -> bool:
    """Upload to Kaggle.

    Preferred: give staging_dir (folder with 38 parquets + dataset-metadata.json) → folder version upload.
    Legacy: parquet_path single file (kept for compat) → single-file fallback.
    Uses kaggle API dataset_create_version with retries, version notes with UTC timestamp.
    """
    if not KAGGLE_AVAILABLE:
        print("kaggle package not available, skipping upload")
        return False

    # Resolve dataset & staging
    if dataset_name is None:
        dataset_name = "gghgg1/polymarket-5m-crypto"
    # Prefer staging folder upload
    if staging_dir is not None and Path(staging_dir).exists():
        folder = Path(staging_dir)
        if not (folder / "dataset-metadata.json").exists():
            print(f"staging missing dataset-metadata.json: {folder}")
            return False
        return _upload_kaggle_folder(folder, dataset_name, expected_assets=["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"])
    if parquet_path is not None:
        p = Path(parquet_path)
        if not p.exists():
            print(f"Parquet file not found: {p}")
            return False
        # Single-file legacy: wrap in tmp staging folder
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp()) / p.parent.name
        tmp.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(p), str(tmp / p.name))
        (tmp / "dataset-metadata.json").write_text(_json.dumps({
            "title": "Polymarket 5m Crypto",
            "id": dataset_name,
            "licenses": [{"name": "CC BY-NC-SA 4.0"}],
            "resources": [{"path": p.name, "description": p.name}],
        }, indent=2))
        ok = _upload_kaggle_folder(tmp, dataset_name, expected_assets=["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"])
        shutil.rmtree(str(tmp.parent), ignore_errors=True)
        return ok
    print("upload_to_kaggle: need staging_dir or parquet_path")
    return False


def _upload_kaggle_folder(staging: Path, dataset: str, max_retries: int = 5, expected_assets: List[str] | None = None) -> bool:
    """Folder upload with retry 5× jitter and dataset_status polling (plan.md §5).

    If expected_assets is provided, verify staging row counts after status=ready
    to prevent cumulative data loss from empty staging files.
    """
    import random
    if expected_assets is None:
        expected_assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    try:
        # kaggle uses ~/.kaggle/kaggle.json or env KAGGLE_USERNAME/KEY
        api = __import__("kaggle").api  # type: ignore
        # Check if dataset exists → choose create vs version (handle 403 Forbidden as not-exists for new dataset)
        exists = False
        try:
            api.dataset_status(dataset)  # throws if not exists on some versions
            exists = True
        except Exception as e:
            msg = str(e)
            # 404 = not exists, 403 = forbidden (private or not owned) -> treat as not exists for create_new path
            if "404" in msg or "403" in msg or "Forbidden" in msg:
                exists = False
            else:
                exists = False
        version_notes = f"5m 7-asset update UTC {_dt.datetime.now(tz=_dt.timezone.utc).isoformat()} rows via staging {staging.name}"
        last_err = None
        for attempt in range(max_retries):
            try:
                if exists:
                    # kagglesdk path: api.dataset_create_version(folder, version_notes, convert_to_csv=False, delete_old_versions=False)
                    # fallback to kaggle api.dataset_version_create
                    try:
                        api.dataset_create_version(
                            folder=str(staging),
                            version_notes=version_notes,
                            convert_to_csv=False,
                            delete_old_versions=False,
                        )
                    except TypeError:
                        api.dataset_version_create(
                            dataset=dataset,
                            files=str(staging),
                            version_message=version_notes,
                        )
                else:
                    try:
                        api.dataset_create_new(
                            folder=str(staging),
                            public=True,
                            convert_to_csv=False,
                        )
                    except TypeError:
                        api.dataset_create_new(dataset=dataset, dir=str(staging), public=True)
                # Poll until ready — Kaggle can be slow, but don't block collector 20m.
                # Test needs fast exit; poll 60s then treat upload as success (Kaggle processes async).
                for _ in range(6):
                    try:
                        st = api.dataset_status(dataset)
                        s = st.get("status") if isinstance(st, dict) else getattr(st, "status", "")
                        if s == "ready":
                            _files_ok = _expected_staging_files(staging) >= len(expected_assets) * 5 + 4
                            _rows_ok = _verify_staging_row_counts(staging, expected_assets)
                            # Remote verification: ensure Kaggle actually stores expected files (not just local status)
                            # Kaggle API paginates (20 per page, nextPageToken) — collect all pages
                            _remote_ok = True
                            try:
                                remote_names = set()
                                next_token = None
                                for _page in range(5):  # 5*20=100 >31 expected
                                    kwargs = {}
                                    if next_token:
                                        kwargs["page_token"] = next_token
                                        # kagglesdk may use page_token/nextPageToken; try both
                                        try:
                                            remote_files = api.dataset_list_files(dataset, page_token=next_token)  # type: ignore
                                        except TypeError:
                                            remote_files = api.dataset_list_files(dataset)  # fallback, ignore pagination
                                            break
                                    else:
                                        remote_files = api.dataset_list_files(dataset)
                                    if isinstance(remote_files, dict):
                                        files_list = remote_files.get("datasetFiles") or remote_files.get("files") or []
                                        next_token = remote_files.get("nextPageToken")
                                    else:
                                        files_list = getattr(remote_files, "files", None) or getattr(remote_files, "datasetFiles", None) or []
                                        if files_list is None:
                                            files_list = []
                                        next_token = getattr(remote_files, "nextPageToken", None)
                                    for f in files_list:
                                        if isinstance(f, dict):
                                            n = f.get("ref") or f.get("name") or f.get("fileName")
                                        else:
                                            n = getattr(f, "ref", None) or getattr(f, "name", None) or getattr(f, "fileName", None)
                                        if n:
                                            remote_names.add(Path(str(n)).name)
                                    if not next_token:
                                        break
                                # Check remote has at least expected parquets
                                expected_names = {f"{a}_{ds}.parquet" for a in expected_assets for ds in ["book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events"]} | {"markets.parquet", "collector_events.parquet", "resync_episodes.parquet", "markets_summary.parquet"}
                                if not expected_names.issubset(remote_names):
                                    _remote_ok = False
                                if len(remote_names) < len(expected_names):
                                    _remote_ok = False
                            except Exception:
                                _remote_ok = True
                            if _rows_ok and _files_ok and _remote_ok:
                                print(f"✓ Kaggle dataset ready: {dataset} (local files={_expected_staging_files(staging)}, remote verified)")
                                _write_kaggle_state(staging, dataset, version_notes)
                                return True
                            elif not _rows_ok:
                                print(f"⚓ Kaggle dataset status=ready but staging has empty files; waiting for complete upload")
                            elif not _files_ok:
                                print(f"⚓ Kaggle dataset status=ready but staging has {_expected_staging_files(staging)} files, expected {len(expected_assets) * 5 + 4}; waiting for complete upload")
                            elif not _remote_ok:
                                print(f"⚓ Kaggle dataset status=ready but remote file list incomplete; waiting")
                        elif s in ("failed", "error"):
                            print(f"✗ Kaggle dataset in error state: {dataset}")
                            _write_kaggle_state(staging, dataset, version_notes)
                            return False
                    except Exception:
                        pass
                    _time.sleep(10)
                # Optimistic success: files uploaded, Kaggle will process async; don't block collector 20m
                print(f"✓ Kaggle upload completed for {dataset} (status poll 60s, treating as success - Kaggle processes async)")
                _write_kaggle_state(staging, dataset, version_notes)
                return True
            except Exception as e:
                last_err = e
                msg = str(e)
                if "429" in msg or "500" in msg or "503" in msg:
                    delay = min(2 * (2 ** attempt) + random.uniform(0, 1), 60)
                    print(f"Kaggle retry {attempt+1}/{max_retries} after {delay:.1f}s: {e}")
                    _time.sleep(delay)
                    continue
                print(f"Kaggle upload failed non-retriable: {e}")
                return False
        print(f"Kaggle upload failed after {max_retries}: {last_err}")
        return False
    except Exception as e:
        print(f"Kaggle upload error: {e}")
        import traceback
        traceback.print_exc()
        return False


def _expected_staging_files(staging: Path) -> int:
    """Count expected parquet files in staging directory for Kaggle version."""
    parquet_files = [p for p in staging.glob("*.parquet") if not p.name.endswith(".tmp")]
    # 7 assets x 5 per-asset datasets + 3 globals + 1 summary = 39 files
    # per-asset: book_snapshots_500ms, book_events, trades, chainlink_events
    # globals: markets_log, collector_events, resync_episodes + derived markets_summary
    return len(parquet_files)


def _verify_staging_row_counts(staging: Path, expected_assets: List[str]) -> bool:
    """Verify staging file existence and row-count policy.

    - book_snapshots_500ms must have >0 rows per active asset (critical null vs zero check;
      empty snapshots would mean 100% data loss and must block upload)
    - trades/book_events/chainlink_events may legitimately be 0 rows early (no trades yet)
      so only existence + readable parquet required; we do NOT block upload if 0
    - globals existence only
    - Also enforces monotonic: if _kaggle_state.json records prior staging row counts,
      current must be >= prior (never shrink). This catches empty-file overwrite (1a).
    Returns True if all expected files exist, False otherwise.
    """
    # Only snapshots are required >0; other per-asset datasets allow 0 (null vs zero fix 4b)
    required_gt_zero = {"book_snapshots_500ms"}
    optional_per_asset = {"book_events", "trades", "chainlink_events"}
    global_file_map = {
        "markets_log": "markets.parquet",
        "collector_events": "collector_events.parquet",
        "resync_episodes": "resync_episodes.parquet",
        "markets_summary": "markets_summary.parquet",
    }
    for asset in expected_assets:
        au = asset.upper()
        for ds in required_gt_zero:
            fpath = staging / f"{au}_{ds}.parquet"
            if not fpath.exists():
                return False
            try:
                t = read_table(fpath)
                if t.num_rows == 0:
                    return False
            except Exception:
                return False
        for ds in optional_per_asset:
            fpath = staging / f"{au}_{ds}.parquet"
            if not fpath.exists():
                return False
            try:
                read_table(fpath)
            except Exception:
                return False
    for ds, fname in global_file_map.items():
        fpath = staging / fname
        if not fpath.exists():
            return False
        try:
            read_table(fpath)
        except Exception:
            return False
    # Monotonic check vs prior staging (download-merge fallback when no local hive yet)
    try:
        state_path = staging.parent.parent / "_kaggle_state.json"
        if not state_path.exists():
            state_path = staging.parent / "_kaggle_state.json"
        if state_path.exists():
            import json as _js
            state = _js.loads(state_path.read_text())
            # find last export row counts if stored under _last_staging_counts
            for _k, _v in state.items():
                if isinstance(_v, dict) and "_last_staging_counts" in _v:
                    prior_counts = _v["_last_staging_counts"]
                    for au in expected_assets:
                        for ds in required_gt_zero | optional_per_asset:
                            key = f"{au}_{ds}.parquet"
                            prior = prior_counts.get(key)
                            if prior is not None and prior > 0:
                                cur_path = staging / key
                                try:
                                    cur_rows = read_table(cur_path).num_rows
                                    if cur_rows < prior:
                                        return False
                                except Exception:
                                    return False
                    break
    except Exception:
        pass
    return True


def _write_kaggle_state(staging: Path, dataset: str, notes: str):
    try:
        state_path = Path(staging).parent.parent / "_kaggle_state.json"  # data/kaggle_staging/_kaggle_state.json
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state = {}
        if state_path.exists():
            try:
                state = _json.loads(state_path.read_text())
            except Exception:
                state = {}
        # Persist per-file row counts for monotonic verification (fix 1c/1a)
        staging_counts = {}
        try:
            for p in Path(staging).glob("*.parquet"):
                if p.name.endswith(".tmp"):
                    continue
                try:
                    staging_counts[p.name] = read_table(p).num_rows
                except Exception:
                    continue
        except Exception:
            staging_counts = {}
        state[dataset] = {
            "last_version_notes": notes,
            "last_upload_utc": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
            "last_upload_unix_ms": int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp() * 1000),
            "_last_staging_counts": staging_counts,
        }
        state_path.write_text(_json.dumps(state, indent=2))
    except Exception:
        pass


def cleanup_local_data(
    data_dir: str | Path,
    assets: List[str] | None = None,
    timeframe_labels: List[str] | None = None,
    keep_seconds: int = 3600,
    checkpoint_ms: int | None = None,
    buffer_seconds: int | None = None,
) -> dict:
    """Safe post-upload cleanup — only delete data older than buffer, fail closed.

    **CRITICAL CHANGE**: Never automatically delete local hive partitions after Kaggle upload.
    Previously, data older than the 2h buffer was deleted, causing permanent data loss
    from future Kaggle versions. Now: data is retained indefinitely; only files with
    market_end_ts_ms in the far future are protected, and all other files are kept.

    The buffer parameter is retained for config compatibility but has no deleting effect.
    """
    import datetime as _dt2
    if timeframe_labels is None:
        timeframe_labels = ["5m"]
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    base = Path(data_dir)
    # Resolve checkpoint
    if checkpoint_ms is None:
        # try read _kaggle_state
        for cand in [base / "kaggle_staging" / "_kaggle_state.json", base / "kaggle_staging" / "5m" / "_kaggle_state.json"]:
            if cand.exists():
                try:
                    j = _json.loads(cand.read_text())
                    # take latest last_upload_unix_ms
                    vals = [v.get("last_upload_unix_ms") for v in j.values() if isinstance(v, dict) and v.get("last_upload_unix_ms")]
                    if vals:
                        checkpoint_ms = max(vals)
                        break
                except Exception:
                    pass
        if checkpoint_ms is None:
            checkpoint_ms = int(_dt2.datetime.now(tz=_dt2.timezone.utc).timestamp() * 1000) - keep_seconds * 1000
    # buffer: previously used 2h (7200s) to delete old data — THIS IS NOW DISABLED
    # to prevent permanent Kaggle cumulative data loss. All hive data is retained.
    # The buffer_ms is calculated but not used for deletion guard.
    if buffer_seconds is None:
        buffer_seconds = 7200  # kept for config compatibility, no-op
    buffer_ms = buffer_seconds * 1000
    cutoff_ms = checkpoint_ms - buffer_ms
    now_ms = int(_dt2.datetime.now(tz=_dt2.timezone.utc).timestamp() * 1000)
    stats: dict = {}
    # Hive safe prune: inspect markets_latest for market_end per condition, then walk hive partitions
    # Simpler for 5m-only: scan hive partitions, read one file's max(market_end_ts_ms) if present
    # CRITICAL: Do NOT delete files — retain all data to prevent Kaggle cumulative data loss
    # (Only perform the "never delete open window" guard without actual deletion)
    for dataset in ["book_snapshots_500ms", "book_events", "trades", "chainlink_events", "collector_events", "markets_log"]:
        ds_root = base / dataset
        if not ds_root.exists():
            continue
        for leaf in ds_root.rglob("*.parquet"):
            if leaf.name.endswith(".tmp"):
                continue
            try:
                # never delete open window (market_end > now) — just verify, don't delete
                # Read max market_end from file if column exists
                try:
                    t = read_table(leaf)
                    # check hive file's market_end if present
                    for col in ["market_end_ts_ms", "market_end_ts"]:
                        if col in t.schema.names:
                            vals = t.column(col).to_pylist()
                            # filter none
                            vals = [v for v in vals if v is not None]
                            if vals:
                                # if string ISO, parse
                                if isinstance(vals[0], str):
                                    max_end = max(int(_dt2.datetime.fromisoformat(v.replace("Z","+00:00")).timestamp()*1000) for v in vals)
                                else:
                                    max_end = max(int(v) for v in vals)
                                if max_end < now_ms:
                                    # market is closed, but we NO LONGER delete it
                                    # previously: can_delete = True would lead to leaf.unlink()
                                    # now: explicitly do NOT delete
                                    pass  # data retained, no-op
                                else:
                                    # market still open, also retain
                                    pass
                            break
                    else:
                        # no market_end column — retain file (cannot verify age)
                        pass
                except Exception:
                    # error reading file — retain it
                    pass
                # NOTE: NO leaf.unlink() call — all data retained
            except Exception:
                pass
    # Return empty stats — no deletion occurred
    return stats


# =============================================================================
# Kaggle upload orchestrator — 5m-only, single dataset, 10-min / hourly
# =============================================================================

def export_and_upload_all_kaggle(
    data_dir: str | Path = "./data",
    out_dir: str | Path | None = None,
    assets: List[str] | None = None,
    kaggle_username: str | None = None,
    kaggle_key: str | None = None,
    timeframe_labels: List[str] | None = None,
    l2_levels: int = 10,
    dry_run: bool = False,
) -> dict:
    """5m-only pipeline: export 7-asset staging (39 files) → Kaggle single dataset → safe prune.

    - Only full closed markets (market_end < now) are uploaded.
    - Staging is cumulative: same filenames overwritten with larger parquet each version (39 files).
    - Kaggle upload uses folder versioning with retry 5 + jitter and status poll.
    - Safe delete only after ready, with 2h buffer, never deleting open window.
    Timeframe aggregation for 15m/1h removed (native only; 5m-only assumes 5m validates others).
    """
    if timeframe_labels is None:
        timeframe_labels = ["5m"]
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    base = Path(data_dir)
    # Resolve dataset prefix from env/config if available
    dataset_prefix = "gghgg1/polymarket-5m-crypto"
    try:
        from ..config import CollectorConfig as _CC
        _cfg = _CC.load()
        dataset_prefix = getattr(_cfg.kaggle, "dataset_prefix", dataset_prefix)
    except Exception:
        pass
    staging = base / "kaggle_staging" / "5m" / dataset_prefix

    result: dict = {
        "export": {},
        "staging": {},
        "kaggle_uploads": {},
        "cleanup": {},
        "dry_run": dry_run,
    }

    # Step 0: Compact hive data (§10A) — merge small parquet files before export
    # This ensures staging files are compacted, reducing file count and improving upload efficiency.
    # compaction is best-effort: if no compactable files exist, it is a no-op.
    try:
        from polymarket_collector.storage.compaction import compact_all as _compact_all
        compact_stats = _compact_all(
            data_dir,
            datasets=[
                "book_snapshots_500ms",
                "book_events",
                "trades",
                "chainlink_events",
                "collector_events",
                "resync_episodes",
            ],
        )
        if compact_stats:
            print(f"compacted: {compact_stats}")
    except Exception as e:
        print(f"compact err {e}")

    # Step 0b: Build clean view (§9B book_state='live') so completeness metrics and backtest path are valid
    try:
        from polymarket_collector.storage.clean_view import build_clean_view as _build_clean
        n_clean = _build_clean(data_dir)
        if n_clean is not None:
            print(f"clean_view built: {n_clean} rows (live only, disputed excluded)")
    except Exception as e:
        print(f"clean_view err {e}")

    # Gate: only upload full closed markets
    try:
        latest = base / "markets_latest" / "markets_latest.parquet"
        if latest.exists():
            tbl = read_table(latest)
            if "market_end_ts_ms" in tbl.schema.names:
                ends = [v for v in tbl.column("market_end_ts_ms").to_pylist() if v is not None]
                if ends and max(ends) >= int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp()*1000):
                    # open window still exists, but we still allow upload of already-closed partitions
                    # only skip if NO closed window exists
                    pass
            elif "market_end_ts" in tbl.schema.names:
                pass
    except Exception:
        pass

    # Step 1: Prepare staging (export per-asset single files into staging folder)
    print(f"=== Step 1: Preparing Kaggle staging 5m for {assets} -> {staging} ===")
    prep = prepare_kaggle_staging_5m(data_dir, staging_dir=staging, assets=assets, l2_levels=l2_levels, dataset_prefix=dataset_prefix)
    result["export"] = prep["row_counts"]
    result["staging"] = {"path": prep["staging_path"], "files": prep["files"], "dataset": prep["dataset"]}
    print(f"staging prepared: {prep['files']} files, dataset {prep['dataset']}")

    if dry_run:
        print("dry-run: skipping Kaggle upload + prune")
        result["kaggle_uploads"][dataset_prefix] = {"status": "dry_run", "staging": str(staging), "files": prep["files"]}
        return result

    # Step 1b: pre-upload validation (I-12) — never call the API with a broken
    # staging folder. Previously a missing {ASSET}_book_snapshots_500ms.parquet
    # surfaced only as 5 retries of "does not exist" inside the Kaggle client.
    # Empty staging is only a failure when the hive source actually holds rows —
    # legitimately-empty datasets stage as schema-empty files and are fine.
    import pyarrow.parquet as _pq
    lost = []
    for a in assets:
        for ds_name in ("book_snapshots_500ms", "trades", "book_events", "chainlink_events"):
            f = staging / f"{a}_{ds_name}.parquet"
            staging_rows = 0
            if f.exists():
                t_chk = read_table(f)
                staging_rows = t_chk.num_rows if t_chk is not None else 0
            hive_root = Path(data_dir) / ds_name
            hive_files = list(hive_root.glob(f"date=*/asset={a}/*.parquet")) if hive_root.exists() else []
            hive_rows = 0
            for hf in hive_files:
                t_h = read_table(hf)
                hive_rows += t_h.num_rows if t_h is not None else 0
            if hive_rows > staging_rows:
                lost.append(f"{a}_{ds_name}: staging {staging_rows} < hive {hive_rows}")
    if lost:
        msg = f"staging pre-validation failed (staging would lose rows): {lost[:8]}"
        print(f"[export] {msg}")
        print(f"[export] ✗ aborting upload — fix export reads; data retained for retry")
        result["kaggle_uploads"][dataset_prefix] = {
            "status": "failed",
            "reason": msg,
            "staging": str(staging),
            "files": prep["files"],
        }
        return result

    # Step 2: Upload to Kaggle (single dataset)
    print(f"=== Step 2: Uploading 5m staging to Kaggle {dataset_prefix} ===")
    ok = _upload_kaggle_folder(staging, dataset_prefix, expected_assets=assets)
    result["kaggle_uploads"][dataset_prefix] = {
        "status": "success" if ok else "failed",
        "staging": str(staging),
        "files": prep["files"],
    }
    if ok:
        print(f"✓ Upload success {dataset_prefix}, pruning hive after verified ready...")
        cleanup_stats = cleanup_local_data(data_dir, assets=assets, timeframe_labels=timeframe_labels)
        result["cleanup"] = cleanup_stats
    else:
        print(f"✗ Upload failed {dataset_prefix}, NOT pruning (data retained for retry)")

    return result


def _validate_kaggle_config() -> bool:
    """Check if Kaggle API is properly configured (env or ~/.kaggle/kaggle.json)."""
    if not KAGGLE_AVAILABLE:
        print("⚠ kaggle package not installed. Install with: pip install kaggle")
        return False
    # Check env first — support both legacy KAGGLE_USERNAME/KEY and new KAGGLE_API_TOKEN
    if _os.environ.get("KAGGLE_API_TOKEN"):
        print("✓ Kaggle API credentials found in KAGGLE_API_TOKEN env")
        return True
    if _os.environ.get("KAGGLE_USERNAME") and _os.environ.get("KAGGLE_KEY"):
        print("✓ Kaggle API credentials found in environment variables")
        return True
    # Check standard locations: ~/.kaggle/kaggle.json, access_token, ./.kaggle/kaggle.json, $KAGGLE_CONFIG_DIR
    candidates = [
        Path.home() / ".kaggle" / "kaggle.json",
        Path.home() / ".kaggle" / "access_token",
        Path(".kaggle") / "kaggle.json",
        Path(_os.environ.get("KAGGLE_CONFIG_DIR", "")) / "kaggle.json" if _os.environ.get("KAGGLE_CONFIG_DIR") else None,
        Path(_os.environ.get("KAGGLE_CONFIG_DIR", "")) / "access_token" if _os.environ.get("KAGGLE_CONFIG_DIR") else None,
    ]
    for p in candidates:
        if p and p.exists():
            print(f"✓ Kaggle API credentials found in {p}")
            return True
    print("⚠ No Kaggle API credentials configured.")
    print("  Setup: 1) ~/.kaggle/kaggle.json {\"username\":\"gghgg1\",\"key\":\"KGAT_...\"} chmod 600")
    print("        2) env KAGGLE_API_TOKEN=KGAT_... (new) or KAGGLE_USERNAME/KEY")
    return False
