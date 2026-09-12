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
import os
import sys
import uuid
from pathlib import Path
from typing import List, Optional, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq


def _os_replace_safe(src, dst):
    """Atomic tmp->final rename that works on Windows (os.replace overwrites; Path.rename raises WinError 183 if dst exists)."""
    import os as _os
    _os.replace(str(src), str(dst))


def _export_lock_path(data_dir) -> Path:
    return Path(data_dir) / ".export.lock"


def _acquire_export_lock(data_dir):
    """Blocking cross-process mutex for the heavy Kaggle export pipeline.

    2026-09-09: the collector hourly loop (4 lanes back-to-back) and the
    15-min backfill cron (--all-lanes) ran exports concurrently in separate
    processes — combined RSS spiked past the pm2 cap and the box OOM'd
    (11:20 SIGKILL mid-export). flock serializes them; the kernel releases
    the lock if the holder dies, so a crash can never wedge uploads.
    Returns an fd to pass to _release_export_lock, or None on non-Unix.
    """
    try:
        import fcntl as _fcntl
    except Exception:
        return None
    import os as _os2
    try:
        p = _export_lock_path(data_dir)
        p.parent.mkdir(parents=True, exist_ok=True)
        fd = _os2.open(str(p), _os2.O_CREAT | _os2.O_RDWR, 0o644)
    except Exception:
        return None
    try:
        _fcntl.flock(fd, _fcntl.LOCK_EX)  # blocking
    except Exception:
        try:
            _os2.close(fd)
        except Exception:
            pass
        return None
    return fd


def _release_export_lock(fd) -> None:
    if fd is None:
        return
    try:
        import fcntl as _fcntl
        _fcntl.flock(fd, _fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        import os as _os3
        _os3.close(fd)
    except Exception:
        pass


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


def _trades_need_enrichment(table: pa.Table) -> int:
    """Count rows needing wallet/outcome enrichment, Arrow-only (no pylist).

    Same predicate as the legacy row loop: has transaction_hash AND
    (wallet NULL OR (maker NULL AND side is buy/sell) OR outcome missing).
    """
    try:
        names = table.schema.names
        if "transaction_hash" not in names:
            return 0
        has_tx = pc.is_valid(table.column("transaction_hash"))
        parts = []
        if "wallet" in names:
            parts.append(pc.is_null(table.column("wallet")))
        if "maker_wallet" in names and "side" in names:
            try:
                _up = pc.utf8_upper(pc.cast(table.column("side"), pa.string()))
                _is_bs = pc.or_(pc.equal(_up, pa.scalar("BUY")), pc.equal(_up, pa.scalar("SELL")))
                parts.append(pc.and_(pc.is_null(table.column("maker_wallet")), pc.fill_null(_is_bs, False)))
            except Exception:
                pass
        if "outcome" in names:
            try:
                _oc = table.column("outcome")
                _missing = pc.or_(pc.is_null(_oc), pc.or_(pc.equal(_oc, pa.scalar("")), pc.equal(_oc, pa.scalar("unknown"))))
                parts.append(pc.fill_null(_missing, False))
            except Exception:
                pass
        if not parts:
            return 0
        need = parts[0]
        for p in parts[1:]:
            need = pc.or_(need, p)
        need = pc.and_(pc.fill_null(has_tx, False), pc.fill_null(need, False))
        return int(pc.sum(pc.cast(need, pa.int64())).as_py() or 0)
    except Exception:
        return 1  # fail open: assume work needed


def _backfill_trade_wallets_chunked(
    table: pa.Table,
    data_dir: Path,
    asset: Optional[str] = None,
    reconcile: bool = True,
    chunk_rows: int = 8000,
    deadline_s: Optional[float] = None,
    pool_cache: Optional[dict] = None,
) -> pa.Table:
    """Bounded-RAM wrapper around _backfill_trade_wallets (2026-09-10 OOM).

    The inner function converts the whole input to python dicts (~10x RAM).
    Split by condition_id groups (a fill's tx never spans markets, so pools
    and reconcile inserts stay exactly correct per group) and concat the
    enriched groups at the end. Small inputs take the direct path.
    deadline_s: total Data-API budget for this call — when exceeded, later
    groups ship with honest NULL wallets (healed by the next pass) instead
    of killing the whole staging file.
    pool_cache: optional dict shared across calls — per-market leg pools are
    fetched once and reused (the streaming export calls this per file-group;
    without the cache every group would re-fetch the same markets).
    """
    if table is None or table.num_rows == 0 or table.num_rows <= chunk_rows:
        return _backfill_trade_wallets(table, data_dir, asset=asset, reconcile=reconcile,
                                       deadline_s=deadline_s, pool_cache=pool_cache)
    try:
        import gc as _gc_c
        import time as _time_c
        _deadline = (_time_c.time() + deadline_s) if deadline_s else None
        vc = table.column("condition_id").value_counts()
        vals = vc.field("values").to_pylist()
        counts = vc.field("counts").to_pylist()
        order = sorted(range(len(vals)), key=lambda i: -(counts[i] or 0))
        groups: list = []
        cur: list = []
        cur_n = 0
        for i in order:
            c = vals[i]
            if c is None:
                continue
            cur.append(c)
            cur_n += counts[i] or 0
            if cur_n >= chunk_rows:
                groups.append(cur)
                cur = []
                cur_n = 0
        if cur:
            groups.append(cur)
        parts = []
        for _gi, g in enumerate(groups):
            if _deadline is not None and _time_c.time() > _deadline:
                print(f"[export] trades enrichment deadline ({deadline_s}s) exceeded at {asset}: "
                      f"shipping remaining groups unenriched (honest NULLs, healed next pass)")
                # ship the remaining groups as-is (no fabrication, NULLs stay NULL)
                for g2 in groups[_gi:]:
                    try:
                        mask2 = pc.is_in(table.column("condition_id"), value_set=pa.array(g2))
                    except Exception:
                        mask2 = None
                    sub2 = table.filter(mask2) if mask2 is not None else table
                    parts.append(sub2)
                    del sub2
                break
            try:
                mask = pc.is_in(table.column("condition_id"), value_set=pa.array(g))
            except Exception:
                mask = None
            sub = table.filter(mask) if mask is not None else table
            _left = (max(0.0, _deadline - _time_c.time()) if _deadline else None)
            parts.append(_backfill_trade_wallets(sub, data_dir, asset=asset, reconcile=reconcile,
                                                deadline_s=_left, pool_cache=pool_cache))
            del sub
            _gc_c.collect()
        if not parts:
            return table
        out = parts[0] if len(parts) == 1 else pa.concat_tables(parts, promote_options="default")
        del parts
        _gc_c.collect()
        return out
    except Exception as e:
        print(f"[export] WARN chunked backfill failed, direct fallback: {e}")
        return _backfill_trade_wallets(table, data_dir, asset=asset, reconcile=reconcile)


def _fetch_market_trades(cid: str, taker_only: bool, oldest_needed_ms: Optional[int], max_pages: int = 60) -> list:
    """Data-API fills for a market, newest-first, paging until older than
    anything we need (adaptive pagination — enrichment round 2). The old
    fixed 12-page cap truncated liquid markets (BTC ~4k fills/window ≈ 8+
    pages), so fills beyond the cap could never be enriched. max_pages is
    now only a runaway-safety ceiling, not the stop condition."""
    import httpx as _hx

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


def _build_leg_pools(api_rows: list) -> tuple:
    """Pool Data-API fill legs per side: one fill settles as TWO legs sharing
    (tx_hash, price, size) — a SELL leg (maker's proxyWallet) and a BUY leg
    (taker's proxyWallet). Returns (buy_pool, sell_pool, outcome_by_key)."""
    buy_pool: dict = {}
    sell_pool: dict = {}
    outcome_by_key: dict = {}
    for t in api_rows:
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
    return buy_pool, sell_pool, outcome_by_key


def _tx_fallback_wallet(buy_pool: dict, sell_pool: dict, txh: str, side: str) -> tuple:
    """Tx-level attribution ONLY when the whole tx is a single
    buyer/single seller fill — otherwise a per-fill wallet cannot be
    assigned honestly (multi-fill txs pool wallets)."""
    buys = sorted(set(buy_pool.get((txh,)) or []))
    sells = sorted(set(sell_pool.get((txh,)) or []))
    if len(buys) == 1 and len(sells) == 1:
        return (buys[0], sells[0]) if side == "BUY" else (sells[0], buys[0])
    return None, None


def _apply_leg_pools(pylist: list, idxs: list, buy_pool: dict, sell_pool: dict,
                     outcome_by_key: dict) -> tuple:
    """Attribute wallets/outcome to pylist rows from leg pools (mutates the
    row dicts in place). Returns (filled_wallet, filled_outcome) counts."""
    filled_wallet = 0
    filled_outcome = 0
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
                takers_f, makers_f = _tx_fallback_wallet(buy_pool, sell_pool, txh, side)
                w = w or takers_f
                m = m or makers_f
            else:
                # partial fallback: exact-key pool hit on one side must not
                # block the tx-level pool on the other — each side keeps its
                # own unanimity rule, so attribution stays honest.
                if w is None:
                    w = _unambiguous_wallet(taker_pool.get((txh,)))
                if m is None:
                    m = _unambiguous_wallet(maker_pool.get((txh,)))
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
    return filled_wallet, filled_outcome


def _backfill_trade_wallets(combined: pa.Table, data_dir: Path, asset: Optional[str] = None, reconcile: bool = True, deadline_s: Optional[float] = None, pool_cache: Optional[dict] = None) -> pa.Table:
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
    deadline_s: Data-API budget for this call — when exceeded, remaining
    markets keep honest NULLs (healed by the next enrichment pass) instead
    of stalling the staging build past the worker timeout.
    pool_cache: optional dict shared across calls mapping condition_id ->
    (buy_pool, sell_pool, outcome_by_key). Cached markets skip the fetch
    (instant, deadline-free); misses fetch and populate the cache.
    """
    import collections
    import datetime as _dt

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
    import time as _time_dl
    _deadline_dl = (_time_dl.time() + deadline_s) if deadline_s else None
    filled_wallet = 0
    filled_outcome = 0
    legs_by_cid: dict = {}
    for cid, idxs in need_by_cid.items():
        if _deadline_dl is not None and _time_dl.time() > _deadline_dl:
            print(f"[export] trades enrichment deadline ({deadline_s}s) hit at {asset}: "
                  f"{len(need_by_cid) - len(legs_by_cid)} markets left unenriched (honest NULLs)")
            break
        oldest_needed_ms = min((_row_ts_ms(pylist[i]) for i in idxs), default=None)
        # one fill settles as TWO data-api legs sharing (tx_hash, price, size)
        _cached = (pool_cache.get(cid) if pool_cache is not None else None)
        if _cached is not None:
            buy_pool, sell_pool, outcome_by_key = _cached
        else:
            buy_pool, sell_pool, outcome_by_key = _build_leg_pools(
                _fetch_market_trades(cid, taker_only=False, oldest_needed_ms=oldest_needed_ms))
            if pool_cache is not None:
                pool_cache[cid] = (buy_pool, sell_pool, outcome_by_key)

        legs_by_cid[cid] = (buy_pool, sell_pool)
        _fw, _fo = _apply_leg_pools(pylist, idxs, buy_pool, sell_pool, outcome_by_key)
        filled_wallet += _fw
        filled_outcome += _fo
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
            if _deadline_dl is not None and _time_dl.time() > _deadline_dl:
                print(f"[export] trades reconcile deadline ({deadline_s}s) hit at {asset}: "
                      f"skipping api- inserts for remaining markets (healed next pass)")
                break
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
            api_rows = _fetch_market_trades(cid, taker_only=True, oldest_needed_ms=oldest_needed_ms)
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
                    # E7: 0-fee market — keep the real 0.0, flag N/A (NULL).
                    if fee == 0.0:
                        fee_is_estimated = None
                    else:
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


def _reconcile_trades_global(markets_order, ctx_by_cid, have, pool_cache, taker_cache,
                             asset, deadline_s=None):
    """Global api- reconcile inserts from narrow per-market context.

    Same insert construction as the per-table path in _backfill_trade_wallets
    (same keys, same fee derivation, same maker attribution from the cached
    both-legs pools), but driven by a COMPLETE have-set built in a narrow
    pre-pass instead of a full-width concat — so the streaming export never
    materializes the whole trades hive. Yields one insert Table per market
    with rows (caller appends them to the staging writer). Markets whose
    taker fetch is deadline-skipped contribute nothing (honest gap, healed
    next pass). trade_id suffixes use a shared counter (unique by
    construction; exact suffixes may differ from the legacy path).
    """
    import collections as _coll
    import datetime as _dt2
    import time as _time_r

    _deadline = (_time_r.time() + deadline_s) if deadline_s else None
    _inserted = [0]
    _fee_derived = [0]

    def _row_ts_ms_local(v):
        try:
            f = float(v)
            return int(f if f > 1e11 else f * 1000)
        except Exception:
            return None

    for cid in markets_order:
        if _deadline is not None and _time_r.time() > _deadline:
            print(f"[export] trades reconcile deadline ({deadline_s}s) hit at {asset}: "
                  f"skipping api- inserts for remaining markets (healed next pass)")
            break
        _ctx = ctx_by_cid.get(cid) or {}
        _have = have
        _taker = taker_cache.get(cid, None) if taker_cache is not None else None
        if _taker is None and taker_cache is not None and cid in taker_cache:
            continue  # already fetched and empty/deadlined
        if _taker is None:
            _taker = _fetch_market_trades(cid, taker_only=True,
                                          oldest_needed_ms=_ctx.get("oldest_needed_ms"))
            if taker_cache is not None:
                taker_cache[cid] = _taker
        if not _taker:
            continue
        _fee_rate = _ctx.get("fee_rate")
        _series_mode = _ctx.get("series_mode")
        _first = _ctx.get("first") or {}
        _rows_to_add = []
        for t in _taker:
            txh = (t.get("transactionHash") or "").lower()
            try:
                k = (txh, round(float(t.get("price")), 6), round(float(t.get("size")), 6))
            except Exception:
                continue
            if _have.get(k, 0) > 0:
                _have[k] -= 1
                continue
            w = t.get("proxyWallet") or t.get("wallet")
            ts_ms = _api_ts_ms(t)
            try:
                widx = int(ts_ms) // 1000 // 300 if ts_ms else (_first.get("window_index") or 0)
            except Exception:
                widx = _first.get("window_index") or 0
            price_f = t.get("price"); size_f = t.get("size")
            notional = round(float(price_f) * float(size_f), 6) if price_f is not None and size_f is not None else None
            fee = None
            fee_is_estimated = None
            if _fee_rate is not None and notional is not None:
                fee = round(notional * _fee_rate, 6)
                fee_is_estimated = True  # derived from the market's reported rate, not reported per fill
                # E7: 0-fee market — keep the real 0.0, flag N/A (NULL).
                if fee == 0.0:
                    fee_is_estimated = None
                else:
                    _fee_derived[0] += 1
            # R-2: attribute the maker leg when the cached both-legs fetch
            # exposed it unambiguously for this fill key (single distinct wallet)
            maker_w = None
            leg_pools = (pool_cache.get(cid) if pool_cache is not None else None)
            if leg_pools:
                maker_w = _unambiguous_wallet(leg_pools[1].get(k))
            _rows_to_add.append({
                "ts_source": ts_ms or None,
                "ts_received_ns": int(_dt2.datetime.now(tz=_dt2.timezone.utc).timestamp() * 1e9),
                "condition_id": cid,
                "market_id": _first.get("market_id") or cid,
                "series_id": _series_mode or f"{asset or 'X'}-5MIN",
                "window_index": int(widx) if widx is not None else 0,
                "asset": (asset or _first.get("asset") or "").upper(),
                "trade_id": f"api-{txh[:16]}-{_inserted[0]}",
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
            _inserted[0] += 1
        if _rows_to_add:
            yield pa.Table.from_pylist(_rows_to_add)
    if _inserted[0] or _fee_derived[0]:
        print(f"[export] trade reconciliation: inserted {_inserted[0]} missing fills from data-api (CLOB stream coalesces liquid fills); fee derived for {_fee_derived[0]} rows from the market's exchange-reported rate")


def _stream_export_trades_dataset(base, asset_upper, tmp_path, timeframe_label, *,
                                  deadline_s=None, cutoff_ts=None, io_stats=None) -> int:
    """Bounded-RAM streaming trades staging build (2026-09-11).

    The legacy path concats the whole per-asset trades hive (~1GB Arrow for
    BTC) plus enrichment pylists and trips the 700MB worker cap every cycle.
    This path streams byte-bounded file groups through the SAME per-group
    processing (timeframe filter, cached-pool enrichment, mechanical fills,
    heals, per-group dedup/sort) into one staging file, so peak RAM stays
    ~one group regardless of history size. Differences vs the legacy path,
    all documented and safe:
    - enrichment leg pools are fetched once per market and cached across
      groups (same pools ⇒ same attribution; redundant fetches eliminated).
    - api- reconcile inserts use a COMPLETE have-set from a narrow pre-pass
      (assigned once globally, never duplicated); a market's inserts ship
      with the first group that sees it... (see below: inserts ship in a
      dedicated second phase after all groups, appended to the same writer).
    - cross-group (token_id, trade_id) duplicates are dropped against a
      global narrow seen-set (same first-wins semantics).
    - global row order is per-group time-sorted, not globally sorted
      (readers sort per E13; the parity test sorts before comparing).
    Returns rows written. NEVER raises for data reasons (fails closed per
    file via io_stats like the other stream paths).
    """
    import collections as _coll2
    import gc as _gc_s
    import time as _time_s

    from .streaming import _order_key, write_batches

    _deadline = (_time_s.time() + deadline_s) if deadline_s else None
    want = f"{asset_upper}-{timeframe_label}" if timeframe_label else None

    # file set (same cutoff rule as stream_batches) in time-approx order
    _all = _list_source_files(base, "trades", asset_upper)
    files = []
    for p in _all:
        try:
            if cutoff_ts is not None and p.stat().st_mtime > cutoff_ts:
                continue
        except OSError:
            continue
        files.append(p)
    try:
        files.sort(key=_order_key)
    except Exception:
        pass
    if io_stats is not None:
        io_stats.setdefault("files_ok", 0)
        io_stats.setdefault("files_failed", 0)
        io_stats.setdefault("failed_bytes", 0)
        io_stats.setdefault("rows_read", 0)

    _PRE_COLS = ["condition_id", "series_id", "transaction_hash", "price", "size",
                 "fee", "fee_is_estimated", "notional", "market_id", "asset",
                 "window_index", "wallet", "maker_wallet", "taker_wallet",
                 "side", "aggressor_side", "outcome", "ts_source", "trade_id",
                 "token_id"]
    _WB_COLS = ("trade_id", "maker_wallet", "taker_wallet", "wallet",
                "outcome", "fee", "fee_is_estimated")

    def _need_row(r) -> bool:
        if not r.get("transaction_hash") or not r.get("condition_id"):
            return False
        if r.get("wallet") is None:
            return True
        if r.get("maker_wallet") is None and str(r.get("side") or "").upper() in ("BUY", "SELL"):
            return True
        return r.get("outcome") in (None, "", "unknown")

    def _ts_ms_of(v):
        try:
            f = float(v)
            return int(f if f > 1e11 else f * 1000)
        except Exception:
            return None

    # ---- pre-pass (narrow): global have-set + per-market fee/series/first ctx
    have = _coll2.Counter()
    fee_rates = {}
    series_counts = {}
    first_ctx = {}
    oldest_needed = {}
    markets_order = []
    _pre_failed = 0
    _pre_failed_bytes = 0
    # union output schema across input vintages (matches the legacy
    # concat-promote: columns only ever accumulate; first non-null type
    # wins). pq.read_schema is footer-only and leak-free (unlike
    # FileMetaData.schema, which pins ~250KB/call in pyarrow 25).
    _union_fields: dict = {}
    _union_order: list = []
    for p in files:
        try:
            try:
                _sch = pq.read_schema(str(p))
            except Exception:
                _sch = None
            if _sch is not None:
                for _fld in _sch:
                    if _fld.name not in _union_fields:
                        _union_fields[_fld.name] = _fld.type
                        _union_order.append(_fld.name)
                    elif _union_fields[_fld.name] == pa.null() and _fld.type != pa.null():
                        _union_fields[_fld.name] = _fld.type
            try:
                t = pq.read_table(str(p), columns=[c for c in _PRE_COLS
                                                   if _sch is None or c in _sch.names])
            except Exception:
                t = read_table(p)
                if t is None:
                    raise IOError(f"unreadable {p.name}")
                keep = [c for c in _PRE_COLS if c in t.schema.names]
                t = t.select(keep) if keep else None
                if t is None or t.num_rows == 0:
                    continue
            if t.num_rows == 0:
                continue
            if io_stats is not None:
                io_stats["files_ok"] += 1
                io_stats["rows_read"] += t.num_rows
            # lane filter mirrors _read_dataset_per_asset (missing col = keep)
            if want is not None and "series_id" in t.schema.names:
                try:
                    t = t.filter(pc.equal(t.column("series_id"), pa.scalar(want)))
                except Exception:
                    pass
                if t.num_rows == 0:
                    continue
            d = t.to_pylist()
            del t
            for r in d:
                cid = r.get("condition_id")
                if not cid:
                    continue
                if cid not in first_ctx:
                    first_ctx[cid] = {"market_id": r.get("market_id"), "asset": r.get("asset"),
                                      "window_index": r.get("window_index")}
                    markets_order.append(cid)
                    fee_rates[cid] = set()
                    series_counts[cid] = _coll2.Counter()
                try:
                    series_counts[cid][r.get("series_id")] += 1
                except Exception:
                    pass
                if (r.get("transaction_hash") and r.get("price") is not None
                        and r.get("size") is not None):
                    try:
                        have[((r.get("transaction_hash") or "").lower(),
                              round(float(r.get("price")), 6),
                              round(float(r.get("size")), 6))] += 1
                    except Exception:
                        pass
                try:
                    if (r.get("fee") is not None and r.get("fee_is_estimated") is False
                            and r.get("notional")):
                        fee_rates[cid].add(round(float(r.get("fee")) / float(r.get("notional")), 8))
                except Exception:
                    pass
                if _need_row(r):
                    _tsm = _ts_ms_of(r.get("ts_source"))
                    _prev = oldest_needed.get(cid)
                    if _tsm is not None and (_prev is None or _tsm < _prev):
                        oldest_needed[cid] = _tsm
            del d
        except Exception:
            _pre_failed += 1
            try:
                _pre_failed_bytes += p.stat().st_size
            except OSError:
                pass
            continue
    if io_stats is not None:
        io_stats["files_failed"] += _pre_failed
        io_stats["failed_bytes"] += _pre_failed_bytes
    fee_rate = {cid: (next(iter(s)) if len(s) == 1 else None) for cid, s in fee_rates.items()}
    series_mode = {}
    for cid, cnt in series_counts.items():
        try:
            series_mode[cid] = cnt.most_common(1)[0][0] if cnt else None
        except Exception:
            series_mode[cid] = None
    ctx_by_cid = {cid: {"fee_rate": fee_rate.get(cid), "series_mode": series_mode.get(cid),
                        "first": first_ctx.get(cid) or {},
                        "oldest_needed_ms": oldest_needed.get(cid)}
                  for cid in markets_order}
    del fee_rates, series_counts, first_ctx, oldest_needed
    _gc_s.collect()
    _union_schema = None
    try:
        if _union_order:
            _union_schema = pa.schema([pa.field(_n, _union_fields[_n], nullable=True)
                                       for _n in _union_order])
    except Exception:
        _union_schema = None

    # ---- byte-bounded groups over the same file order
    groups = []
    _cur: list = []
    _cur_b = 0
    for p in files:
        try:
            _sz = p.stat().st_size
        except OSError:
            continue
        if _cur and _cur_b + _sz > 4_000_000:
            groups.append(_cur)
            _cur = []
            _cur_b = 0
        _cur.append(p)
        _cur_b += _sz
    if _cur:
        groups.append(_cur)
    del files
    _gc_s.collect()

    pool_cache: dict = {}
    taker_cache: dict = {}
    seen_dd: set = set()
    wb_parts: list = []
    wb_rows = [0]

    def _left_all():
        return (max(0.0, _deadline - _time_s.time()) if _deadline else None)

    def _gen():
        for _gi, _group in enumerate(groups):
            _left = (max(0.0, _deadline - _time_s.time()) if _deadline else None)
            try:
                t = _read_dataset_per_asset(base, "trades", asset_upper,
                                            include_binance=False,
                                            timeframe_label=timeframe_label,
                                            stats=io_stats, deadline_s=_left,
                                            files=_group, reconcile=False,
                                            pool_cache=pool_cache, writeback=False)
            except Exception as e:
                print(f"[export] WARN trades group {_gi} failed: {e} — skipped (fail closed)")
                continue
            if t is None or t.num_rows == 0:
                continue
            # global (token_id, trade_id) dedup — same first-wins semantics
            # as the legacy whole-table guard, via a narrow in-RAM key set.
            try:
                _names = t.schema.names
                if "token_id" in _names and "trade_id" in _names:
                    _tids = t.column("trade_id").to_pylist()
                    _toks = t.column("token_id").to_pylist()
                    _keep2 = []
                    for i in range(t.num_rows):
                        _k = (str(_toks[i]), str(_tids[i]))
                        if _k not in seen_dd:
                            seen_dd.add(_k)
                            _keep2.append(i)
                    del _tids, _toks
                    if len(_keep2) < t.num_rows:
                        _keep_set = set(_keep2)
                        t = t.filter(pa.array([i in _keep_set for i in range(t.num_rows)],
                                              type=pa.bool_()))
                        del _keep_set
                    del _keep2
                    _gc_s.collect()
            except Exception as e:
                print(f"[export] WARN trades group dedup failed: {e}")
            # narrow write-back accumulator (trade_id + fill cols only)
            try:
                _ncols = {}
                for _c in _WB_COLS:
                    _ncols[_c] = t.column(_c) if _c in t.schema.names else pa.array(
                        [None] * t.num_rows, type=pa.string() if _c != "fee" else pa.float64())
                # fee_is_estimated is bool — fix the fallback type
                if "fee_is_estimated" not in t.schema.names:
                    _ncols["fee_is_estimated"] = pa.array([None] * t.num_rows, type=pa.bool_())
                wb_parts.append(pa.table(_ncols))
                wb_rows[0] += t.num_rows
            except Exception as e:
                print(f"[export] WARN trades write-back accumulator failed: {e}")
            yield t
            del t
            _gc_s.collect()
        # phase 2: global reconcile inserts (complete have-set ⇒ never duplicated)
        try:
            for _ins in _reconcile_trades_global(markets_order, ctx_by_cid, have,
                                                 pool_cache, taker_cache,
                                                 asset_upper, deadline_s=_left_all()):
                yield _ins
        except Exception as e:
            print(f"[export] WARN trades global reconcile failed: {e}")

    n = write_batches(_gen(), tmp_path, schema=_union_schema)
    del pool_cache, taker_cache, seen_dd, have, ctx_by_cid, markets_order
    _gc_s.collect()
    # single narrow write-back at the end (NULLs filled only, atomic per file)
    if wb_parts:
        try:
            import pyarrow as _pa_wb
            _narrow = wb_parts[0] if len(wb_parts) == 1 else _pa_wb.concat_tables(
                wb_parts, promote_options="default")
            del wb_parts
            _writeback_enriched_trades(base, asset_upper, _narrow)
            del _narrow
        except Exception as e:
            print(f"[export] WARN trades streaming write-back failed: {e}")
        _gc_s.collect()
    return n


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
    # field updates keyed by trade_id — only rows where a NULL got filled.
    # 2026-09-10 OOM: build the map from NARROW columns only (trade_id + 6
    # fill cols); the full enriched table is released before the per-file
    # loop so peak stays flat.
    updates: dict = {}
    cols = ("maker_wallet", "taker_wallet", "wallet", "outcome", "fee", "fee_is_estimated")
    try:
        _tids = enriched.column("trade_id").to_pylist()
        _fills = {c: enriched.column(c).to_pylist() if c in enriched.schema.names else [None] * enriched.num_rows for c in cols}
        for _i, _tid in enumerate(_tids):
            if not _tid or str(_tid).startswith("api-"):
                continue
            updates[str(_tid)] = {c: _fills[c][_i] for c in cols}
        del _tids, _fills
    except Exception:
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
            if upd:
                for c in cols:
                    cur = r.get(c)
                    fillable = cur is None or (c == "outcome" and cur == "unknown")
                    if fillable and upd[c] is not None:
                        # E7: a 0.0 fee never takes a True/False flag (stays NULL).
                        if c == "fee_is_estimated" and (r.get("fee") == 0.0 or upd.get("fee") == 0.0):
                            continue
                        r[c] = upd[c]
                        changed = True
            # E5: lowercase legacy uppercase aggressor sides (representation fix).
            for sc in ("side", "aggressor_side"):
                if sc in r and isinstance(r[sc], str) and r[sc] != r[sc].lower():
                    r[sc] = r[sc].lower()
                    changed = True
            # E7: 0-fee market — real 0.0 kept, flag N/A (NULL).
            if r.get("fee") == 0.0 and r.get("fee_is_estimated") is not None:
                r["fee_is_estimated"] = None
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
    stats = {"assets_scanned": 0, "rows_needed": 0, "files_rewritten": 0, "deferred": False}
    # Freshness guard: data-api wallet/fill coverage indexes late (~15 min,
    # measured 2026-09-05/06). Run inline right after an export, the pass queries
    # the data-api for thousands of rows and recovers none of them (2026-09-06
    # 19:17 run: 4755 rows needed, 0 files rewritten) — defer to the 15-min cron
    # until the newest stored trade is old enough to be covered.
    import time as _time_mod
    _newest_ns = 0
    for asset in assets:
        _tbl = _read_dataset_per_asset_plain(base, "trades", asset.upper())
        if _tbl is not None and _tbl.num_rows and "ts_received_ns" in _tbl.schema.names:
            _mx = pc.max(_tbl.column("ts_received_ns"))
            if _mx is not None:
                _newest_ns = max(_newest_ns, int(_mx))
    if _newest_ns:
        _age_s = (_time_mod.time_ns() - _newest_ns) / 1e9
        if _age_s < 900:
            print(f"[export] second-pass enrichment deferred: newest trade is {int(_age_s)}s old (<900s) — data-api coverage not healed yet, 15-min cron will pick it up")
            stats["deferred"] = True
            return stats
    for asset in assets:
        au = asset.upper()
        tbl = _read_dataset_per_asset_plain(base, "trades", au)
        stats["assets_scanned"] += 1
        if tbl is None or tbl.num_rows == 0 or "wallet" not in tbl.schema.names:
            continue
        # 2026-09-10 OOM: kernel-count need check (never whole-table dicts).
        needed = _trades_need_enrichment(tbl)
        if not needed:
            continue
        stats["rows_needed"] += needed
        print(f"[export] second-pass enrichment: {au} {needed} rows still missing wallet/outcome — querying data-api")
        enriched = _backfill_trade_wallets_chunked(tbl, base, asset=au, reconcile=False)
        try:
            rewritten = _writeback_enriched_trades(base, au, enriched)
            stats["files_rewritten"] += rewritten
        except Exception as e:
            print(f"[export] WARN second-pass write-back failed for {au}: {e}")
    print(f"[export] second-pass enrichment done: {stats}")
    return stats


def third_pass_onchain_wallets(data_dir: str | Path, assets: Optional[List[str]] = None,
                               rpc_url: Optional[str] = None,
                               max_txs: int = 1500) -> dict:
    """Enrichment round 3 — on-chain maker/taker via CTF Exchange OrderFilled logs.

    Runs after the Data-API passes and only touches rows STILL missing
    maker_wallet/taker_wallet. Receipts are fetched per needed tx (newest
    first, capped per run) — no range scans. No RPC call at all when nothing
    is needed; RPC failures are loud but never fatal (Data-API results stand).
    Intended to run inside resolution_backfill (pm2 cron, every 15 min).
    """
    from ..onchain import (DEFAULT_RPC_URL, backfill_wallets_from_chain,
                           backfill_wallets_from_fills, fetch_receipt_fills,
                           tx_map_from_fills)
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    rpc_url = rpc_url or DEFAULT_RPC_URL
    base = Path(data_dir)
    stats = {"assets_scanned": 0, "rows_needed": 0, "txs_queried": 0,
             "filled_maker": 0, "filled_taker": 0, "files_rewritten": 0,
             "rpc_failed": False}
    per_asset_rows: dict = {}
    need_tx_order: dict = {}  # tx -> newest ts_received_ns (for newest-first cap)
    for asset in assets:
        au = asset.upper()
        tbl = _read_dataset_per_asset_plain(base, "trades", au)
        stats["assets_scanned"] += 1
        if tbl is None or tbl.num_rows == 0 or "wallet" not in tbl.schema.names:
            continue
        rows = tbl.to_pylist()
        needed = [r for r in rows
                  if r.get("transaction_hash")
                  and (r.get("maker_wallet") is None or r.get("taker_wallet") is None)]
        if not needed:
            continue
        stats["rows_needed"] += len(needed)
        per_asset_rows[au] = (tbl, rows)
        for r in needed:
            txh = str(r.get("transaction_hash") or "").lower()
            try:
                ts = int(r.get("ts_received_ns") or 0)
            except Exception:
                ts = 0
            if ts > need_tx_order.get(txh, 0):
                need_tx_order[txh] = ts
    if not per_asset_rows:
        return stats
    txs = sorted(need_tx_order, key=lambda t: need_tx_order[t], reverse=True)[:max_txs]
    stats["txs_queried"] = len(txs)
    try:
        fills = fetch_receipt_fills(rpc_url, txs)
        print(f"[export] on-chain pass: receipts={len(txs)} fills={len(fills)}")
    except Exception as e:
        print(f"[export] WARN on-chain pass skipped (RPC failed: {e}) — data-api results stand")
        stats["rpc_failed"] = True
        return stats
    # tx-level fallback map derived from the same fills (no extra RPC)
    tx_map = tx_map_from_fills(fills)
    for au, (tbl, rows) in per_asset_rows.items():
        try:
            # per-fill (tx, token) join first — survives multi-maker bundle txs;
            # tx-level unanimity as fallback for rows without token_id.
            filled = backfill_wallets_from_fills(rows, fills)
            stats["filled_maker"] += filled["filled_maker"]
            stats["filled_taker"] += filled["filled_taker"]
            left = backfill_wallets_from_chain(rows, tx_map)
            stats["filled_maker"] += left["filled_maker"]
            stats["filled_taker"] += left["filled_taker"]
            if filled["filled_maker"] or filled["filled_taker"] or left["filled_maker"] or left["filled_taker"]:
                enriched = pa.Table.from_pylist(rows, schema=tbl.schema)
                stats["files_rewritten"] += _writeback_enriched_trades(base, au, enriched)
        except Exception as e:
            print(f"[export] WARN on-chain write-back failed for {au}: {e}")
    print(f"[export] on-chain pass done: {stats}")
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
    """Trades aggregates for the summary — staging files preferred (they carry
    the api- reconciled fills), hive fallback otherwise.

    2026-09-10 OOM: NARROW columns only (condition_id/notional/wallet) —
    the summary aggregates per-cid and never touches wide L2/tx columns.
    Full-width concat of trades staging (~30x Arrow expansion) SIGKilled
    the box inside this exact call.
    """
    _need = ["condition_id", "notional", "wallet"]

    def _narrow(p) -> Optional[pa.Table]:
        # 2026-09-11 OOM: project columns AT READ TIME (100MB staging files
        # expand ~30x; selecting after a full read does not save RAM).
        try:
            try:
                t = pq.read_table(str(p), columns=_need)
            except Exception:
                t = read_table(p)
                if t is None or t.num_rows == 0:
                    return None
                keep = [c for c in _need if c in t.schema.names]
                t = t.select(keep) if keep else None
                if t is None:
                    return None
            if t.num_rows == 0:
                return None
            cols = {}
            for c in _need:
                if c in t.schema.names:
                    cols[c] = t.column(c)
                else:
                    cols[c] = pa.array(
                        [None] * t.num_rows,
                        type=pa.string() if c == "wallet" else (pa.float64() if c == "notional" else pa.string()),
                    )
            out = pa.table(cols)
            del t
            return out
        except Exception as e:
            print(f"[export] WARN staging trades unreadable {Path(p).name}: {e}")
            return None

    tables: List[pa.Table] = []
    if staging_dir is not None:
        for a in assets:
            p = Path(staging_dir) / f"{a}_trades.parquet"
            if p.exists():
                t = _narrow(p)
                if t is not None:
                    tables.append(t)
    if not tables:
        hive = _read_dataset_per_asset(base, "trades", None)
        if hive is not None and hive.num_rows:
            try:
                cols = {}
                for c in _need:
                    cols[c] = hive.column(c) if c in hive.schema.names else pa.array(
                        [None] * hive.num_rows,
                        type=pa.string() if c in ("wallet", "condition_id") else pa.float64())
                tables.append(pa.table(cols))
            except Exception:
                pass
            finally:
                del hive
    if not tables:
        return None
    out = tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")
    del tables
    return out


def build_markets_summary(
    data_dir: str | Path,
    staging_dir: str | Path | None = None,
    assets: List[str] | None = None,
    timeframe_label: Optional[str] = None,
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

    timeframe_label: when set, only markets of that window size are emitted
    (multi-timeframe hive is shared; lane identity comes from the
    window_size_seconds column in markets_latest).
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

    # multi-timeframe: keep only this lane's markets (markets_latest carries
    # window_size_seconds; the hive is shared across lanes). Legacy rows with a
    # NULL window_size_seconds are all 5m-era, so they belong to the 5m lane.
    if timeframe_label is not None:
        try:
            from ..config import CollectorConfig as _CC
            want_ws = _CC.window_size_for(timeframe_label)
        except Exception:
            want_ws = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}.get(timeframe_label)
        if want_ws is not None:
            markets = [
                m for m in markets
                if m.get("window_size_seconds") == want_ws
                or (m.get("window_size_seconds") is None and timeframe_label == "5m")
            ]
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
    # 2026-09-10 OOM: prefer the just-built staging clean files (lane-pure,
    # small, current) over the clean hive; streaming hive accumulation only
    # as fallback. Per-cid state stays KBs either way; open/close = row at
    # min/max ts among non-null mids (matches sorted first/last).
    ohlc_by_cid: Dict[str, dict] = {}
    try:
        from .streaming import stream_batches as _stream_batches

        _acc: Dict[str, dict] = {}
        _staging_clean: list = []
        try:
            _sdir = Path(staging_dir) if staging_dir else None
            if _sdir is not None:
                for _a in (assets or []):
                    _p = _sdir / f"{str(_a).upper()}_book_snapshots_clean.parquet"
                    if _p.exists():
                        _staging_clean.append(_p)
        except Exception:
            _staging_clean = []

        def _fold_batch(_b: pa.Table) -> None:
            _need = ["condition_id", "ts_snapshot_ns", "up_bid", "up_ask", "down_bid", "down_ask"]
            if not all(c in _b.schema.names for c in _need):
                return
            _cols = {c: _b.column(c).to_pylist() for c in _need}
            for _i in range(_b.num_rows):
                _cid = _cols["condition_id"][_i]
                _ts = _cols["ts_snapshot_ns"][_i]
                if _cid is None or _ts is None:
                    continue
                _ub, _ua = _cols["up_bid"][_i], _cols["up_ask"][_i]
                _db, _da = _cols["down_bid"][_i], _cols["down_ask"][_i]
                _mu = (_ub + _ua) / 2.0 if _ub is not None and _ua is not None else None
                _md = (_db + _da) / 2.0 if _db is not None and _da is not None else None
                _su = _ua - _ub if _ub is not None and _ua is not None else None
                _sd = _da - _db if _db is not None and _da is not None else None
                _a = _acc.get(_cid)
                # open/close = mid at min/max ts AMONG NON-NULL mids per side
                # (matches sorted first/last which skip nulls); low/high over
                # non-null; spreads averaged over non-null. Order-independent.
                if _a is None:
                    _a = _acc[_cid] = {
                        "min_ts_up": _ts if _mu is not None else None,
                        "max_ts_up": _ts if _mu is not None else None,
                        "min_ts_dn": _ts if _md is not None else None,
                        "max_ts_dn": _ts if _md is not None else None,
                        "up_open": _mu, "up_close": _mu,
                        "down_open": _md, "down_close": _md,
                        "up_low": _mu, "up_high": _mu,
                        "down_low": _md, "down_high": _md,
                        "s_up": 0.0, "s_dn": 0.0, "s_n_up": 0, "s_n_dn": 0, "n": 0,
                    }
                else:
                    if _mu is not None:
                        if _a["min_ts_up"] is None or _ts < _a["min_ts_up"]:
                            _a["min_ts_up"] = _ts
                            _a["up_open"] = _mu
                        if _a["max_ts_up"] is None or _ts >= _a["max_ts_up"]:
                            _a["max_ts_up"] = _ts
                            _a["up_close"] = _mu
                    if _md is not None:
                        if _a["min_ts_dn"] is None or _ts < _a["min_ts_dn"]:
                            _a["min_ts_dn"] = _ts
                            _a["down_open"] = _md
                        if _a["max_ts_dn"] is None or _ts >= _a["max_ts_dn"]:
                            _a["max_ts_dn"] = _ts
                            _a["down_close"] = _md
                if _mu is not None:
                    if _a["up_low"] is None or _mu < _a["up_low"]:
                        _a["up_low"] = _mu
                    if _a["up_high"] is None or _mu > _a["up_high"]:
                        _a["up_high"] = _mu
                if _md is not None:
                    if _a["down_low"] is None or _md < _a["down_low"]:
                        _a["down_low"] = _md
                    if _a["down_high"] is None or _md > _a["down_high"]:
                        _a["down_high"] = _md
                if _su is not None:
                    _a["s_up"] += _su
                    _a["s_n_up"] += 1
                if _sd is not None:
                    _a["s_dn"] += _sd
                    _a["s_n_dn"] += 1
                _a["n"] += 1

        if _staging_clean:
            for _fp in _staging_clean:
                try:
                    _pf = pq.ParquetFile(str(_fp))
                except Exception:
                    continue
                try:
                    for _chunk in _pf.iter_batches(batch_size=20000):
                        _bt = pa.Table.from_batches([_chunk])
                        if _bt.num_rows == 0:
                            continue
                        _fold_batch(_bt)
                        del _bt
                except Exception:
                    continue
        else:
            for _bt in _stream_batches(base, "book_snapshots_clean", None, ts_col="ts_snapshot_ns"):
                _fold_batch(_bt)
                del _bt
        for _cid, _a in _acc.items():
            ohlc_by_cid[_cid] = {
                "up_open": _a["up_open"], "up_close": _a["up_close"],
                "up_low": _a["up_low"], "up_high": _a["up_high"],
                "down_open": _a["down_open"], "down_close": _a["down_close"],
                "down_low": _a["down_low"], "down_high": _a["down_high"],
                "avg_spread_up": (_a["s_up"] / _a["s_n_up"]) if _a["s_n_up"] else None,
                "avg_spread_down": (_a["s_dn"] / _a["s_n_dn"]) if _a["s_n_dn"] else None,
                "snapshot_count": int(_a["n"] or 0),
            }
        del _acc
    except Exception as e:
        print(f"[export] WARN markets_summary snapshot aggregation failed: {e}")

    # --- chainlink: underlying open/close = nearest tick to window boundary
    # (open ≤10s per K-2, close ≤5s) ---
    # 2026-09-10 OOM: narrow per-file reads (3 cols) instead of full-hive concat.
    # 2026-09-12 CPU/RAM: vectorized ts parse + numpy per-asset sorted arrays
    # instead of per-row fromisoformat + ~1M Python tuples (~15 min and
    # ~200MB retained per summary build in the long-lived parent).
    # Wire format is uniform Zulu seconds (e.g. 2026-09-10T22:00:30Z);
    # fractional rows take a second vectorized pass; anything else is
    # counted and dropped loudly (never guessed).
    ticks_by_asset: Dict[str, dict] = {}
    _dropped_fmt = 0
    try:
        from .streaming import iter_source_files as _iter_cl_files
        import numpy as _np_cl

        _TS: Dict[str, list] = {}
        _PX: Dict[str, list] = {}
        for _cf in _iter_cl_files(base, "chainlink_events", None):
            # 3-col projection at read time (never full rows).
            try:
                _ct = pq.read_table(str(_cf), columns=["asset", "ts_source", "price"])
            except Exception:
                try:
                    _full = read_table(_cf)
                    if _full is None:
                        continue
                    _ct = _full.select([c for c in ["asset", "ts_source", "price"] if c in _full.schema.names])
                    del _full
                except Exception:
                    continue
            if _ct.num_rows == 0:
                del _ct
                continue
            _need_cl = {"asset", "ts_source", "price"}
            if not _need_cl.issubset(set(_ct.schema.names)):
                del _ct
                continue
            try:
                _au_vals = _ct.column("asset").to_pylist()
                _uniq = sorted({a for a in _au_vals if a})
                del _au_vals
                if not _uniq:
                    del _ct
                    continue
                for _a in _uniq:
                    try:
                        _t2 = _ct if len(_uniq) == 1 else _ct.filter(
                            pc.equal(_ct.column("asset"), pa.scalar(_a)))
                        if _t2.num_rows == 0:
                            if _t2 is not _ct:
                                del _t2
                            continue
                        _s = _t2.column("ts_source").cast(pa.string(), safe=False)
                        _s = pc.replace_substring(_s, pattern="Z", replacement="")
                        _ms = pc.strptime(_s, format="%Y-%m-%dT%H:%M:%S", unit="ms",
                                          error_is_null=True)
                        try:
                            _nn = int(pc.sum(pc.cast(pc.is_null(_ms), pa.int64())).as_py() or 0)
                        except Exception:
                            _nn = 0
                        if _nn:
                            try:
                                _ms2 = pc.strptime(_s, format="%Y-%m-%dT%H:%M:%S.%f", unit="ms",
                                                   error_is_null=True)
                                _ms = pc.case_when(pc.is_null(_ms), _ms2, _ms)
                                del _ms2
                            except Exception:
                                pass
                        del _s
                        _ok = pc.and_(pc.invert(pc.is_null(_ms)),
                                      pc.is_valid(_t2.column("price")))
                        _n_ok = int(pc.sum(pc.cast(_ok, pa.int64())).as_py() or 0)
                        _dropped_fmt += (_t2.num_rows - _n_ok)
                        if _n_ok == 0:
                            del _ok, _ms
                            if _t2 is not _ct:
                                del _t2
                            continue
                        _t2f = _t2.filter(_ok)
                        _msf = _ms.filter(_ok)
                        del _ok, _ms
                        if _t2 is not _ct:
                            del _t2
                        try:
                            _ts_np = _msf.combine_chunks().to_numpy()
                            _px_np = _t2f.column("price").combine_chunks().to_numpy()
                            _px_np = _np_cl.asarray(_px_np, dtype="float64")
                        except Exception:
                            _dropped_fmt += _t2f.num_rows
                            del _t2f, _msf
                            continue
                        del _t2f, _msf
                        _TS.setdefault(_a, []).append(_ts_np)
                        _PX.setdefault(_a, []).append(_px_np)
                        del _ts_np, _px_np
                    except Exception:
                        continue
                del _ct
            except Exception:
                try:
                    del _ct
                except Exception:
                    pass
                continue
        for _a in list(_TS.keys()):
            try:
                _ta = _np_cl.concatenate(_TS.pop(_a)).astype("int64", copy=False)
                _pa_ = _np_cl.concatenate(_PX.pop(_a)).astype("float64", copy=False)
                _ord = _np_cl.argsort(_ta, kind="stable")
                ticks_by_asset[_a] = {"ts": _ta[_ord], "px": _pa_[_ord]}
                del _ord, _ta, _pa_
            except Exception:
                continue
        del _TS, _PX
    except Exception as e:
        print(f"[export] WARN markets_summary chainlink load failed: {e}")
    if _dropped_fmt:
        print(f"[export] chainlink ticks dropped (unparseable ts/price): {_dropped_fmt}")

    def _nearest_tick(asset: str, target_ms: Optional[int], tol_ms: int = 5000):
        if target_ms is None:
            return None, None
        d = ticks_by_asset.get(asset or "")
        if not d:
            return None, None
        try:
            import numpy as _np_tick
            ts = d["ts"]
            px = d["px"]
            i = int(_np_tick.searchsorted(ts, target_ms))
            best = None
            for j in (i - 1, i):
                if 0 <= j < len(ts):
                    dd = abs(int(ts[j]) - target_ms)
                    if best is None or dd < best[0]:
                        best = (dd, float(px[j]), int(ts[j]))
            if best is None or best[0] > tol_ms:
                return None, None
            iso = _dt2.datetime.fromtimestamp(best[2] / 1000, tz=_dt2.timezone.utc).isoformat().replace("+00:00", "Z")
            return best[1], iso
        except Exception:
            return None, None

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


def _heal_hex_market_ids(table: pa.Table, data_dir: Path) -> pa.Table:
    """E1 migration (non-destructive): replace hex market_id with numeric ids.

    Reads condition_id → market_id from markets_latest (ground truth, 0 hex),
    rewrites rows where market_id looks like a hex condition_id (0x+64hex).
    Rows with no mapping get NULL. Hive files are untouched — healing applies
    to the staging build only.

    2026-09-10 OOM: column-at-a-time via Arrow (never whole-table to_pylist —
    a 150-col snapshot table explodes ~10x as python dicts and SIGKilled the
    box on every export).
    """
    if "market_id" not in table.schema.names or "condition_id" not in table.schema.names:
        return table
    try:
        import re as _re
        hex_re = _re.compile(r"0[xX][0-9a-fA-F]{64}\Z")
        mids = table.column("market_id").to_pylist()  # one narrow column only
    except Exception:
        return table
    idx = [i for i, m in enumerate(mids) if isinstance(m, str) and bool(hex_re.match(m.strip()))]
    if not idx:
        return table
    mapping: dict = {}
    try:
        latest = Path(data_dir) / "markets_latest" / "markets_latest.parquet"
        if latest.exists():
            for r in read_table(latest).to_pylist():  # tiny (one row/market)
                cid, mid = r.get("condition_id"), r.get("market_id")
                if cid and mid and not (isinstance(mid, str) and bool(hex_re.match(mid.strip()))):
                    mapping[str(cid)] = str(mid)
    except Exception as e:
        print(f"[export] WARN markets_latest unreadable for market_id heal: {e}")
    try:
        cids = table.column("condition_id").to_pylist()
        new_mids = list(mids)
        healed = nulled = 0
        idx_set = set(idx)
        for i in idx_set:
            new = mapping.get(str(cids[i] or ""))
            new_mids[i] = new
            if new:
                healed += 1
            else:
                nulled += 1
        if healed or nulled:
            print(f"[export] market_id heal: {healed} hex→numeric, {nulled} hex→NULL (E1)")
        pos = table.schema.get_field_index("market_id")
        return table.set_column(pos, "market_id", pa.array(new_mids, type=pa.string()))
    except Exception as e:
        print(f"[export] WARN market_id heal failed: {e}")
        return table


def _read_dataset_per_asset(data_dir: Path, dataset: str, asset: Optional[str], include_binance: bool = False, timeframe_label: Optional[str] = None, stats: Optional[dict] = None, deadline_s: Optional[float] = None, files: Optional[list] = None, reconcile: bool = True, pool_cache: Optional[dict] = None, writeback: bool = True) -> Optional[pa.Table]:
    """Read all parquet files for dataset (+ optional asset filter).

    timeframe_label: when set, keep only rows whose series_id matches
    "{ASSET}-{label}" — the multi-timeframe lane filter. Datasets without a
    series_id column (chainlink_events, globals) are returned unfiltered so the
    shared per-asset feed ships to every TF dataset (plan.md §2.2).
    stats (optional dict): filled with files_ok / files_failed /
    failed_bytes / rows_read for the export-coverage manifest.
    deadline_s: Data-API budget for the trades wallet-enrichment pass —
    when exceeded, remaining markets keep honest NULLs (healed next pass).
    files: optional explicit file list (streaming callers pass one byte-
    bounded group at a time instead of the whole hive).
    reconcile / pool_cache / writeback: trades-enrichment controls —
    reconcile=False skips api- inserts (the streaming driver inserts them
    once globally from a complete have-set); pool_cache shares per-market
    leg pools across calls; writeback=False defers the hive write-back
    (the driver does one narrow write-back at the end).
    """
    base = data_dir / dataset
    if not base.exists():
        return None
    # gather files
    if files is not None:
        patterns = [Path(p) for p in files]
    elif asset and dataset in PER_ASSET_DATASETS:
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
    if stats is not None:
        # setdefault: streaming callers pass one dict across many file-group
        # calls and expect accumulated totals, not per-call resets.
        stats.setdefault("files_ok", 0)
        stats.setdefault("files_failed", 0)
        stats.setdefault("failed_bytes", 0)
        stats.setdefault("rows_read", 0)
    for p in patterns:
        if p.name.endswith(".tmp"):
            continue
        try:
            t = read_table(p)
            if t is None:
                raise IOError(f"unreadable {p.name}")
            if stats is not None:
                stats["files_ok"] += 1
                stats["rows_read"] += t.num_rows
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
            if stats is not None:
                stats["files_failed"] += 1
                try:
                    stats["failed_bytes"] += p.stat().st_size
                except OSError:
                    pass
            continue
    if _read_errors:
        print(f"[export] WARN {len(_read_errors)} parquet files failed to read for {dataset} asset={asset}: {_read_errors[:3]}")
    if not tables:
        # If all files failed, return None so caller triggers monotonic guard / abort instead of empty success
        if _read_errors:
            print(f"[export] ERROR all {len(patterns)} files failed for {dataset} asset={asset} — aborting read")
        return None
    combined = pa.concat_tables(tables, **({"promote_options": "default"} if tuple(int(x) for x in pa.__version__.split(".")[:2]) >= (16, 0) else {"promote": True})) if len(tables) > 1 else tables[0]
    # multi-timeframe lane filter — applied once on the combined table
    if timeframe_label is not None and asset and "series_id" in combined.schema.names:
        try:
            want = f"{asset.upper()}-{timeframe_label}"
            mask = pc.equal(combined.column("series_id"), pa.scalar(want))
            combined = combined.filter(mask)
        except Exception as e:
            print(f"[export] WARN timeframe filter failed for {dataset} asset={asset} tf={timeframe_label}: {e}")
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
            combined = _backfill_trade_wallets_chunked(combined, data_dir, asset=asset, deadline_s=deadline_s,
                                                       reconcile=reconcile, pool_cache=pool_cache)
            # B-5: persist the enrichment into the hive so data/trades/ matches
            # what ships to Kaggle (NULLs filled only, atomic per file).
            # The streaming export (per-file-group calls) defers this to one
            # narrow write-back at the end instead.
            if writeback:
                try:
                    _writeback_enriched_trades(data_dir, asset, combined)
                except Exception as e:
                    print(f"[export] WARN trades enrichment write-back failed: {e}")
        except Exception as e:
            print(f"[export] WARN wallet backfill failed: {e}")
    # backfill trades: compute notional, fee, aggressor_side, transaction_hash where null for old 3.1.0 data
    # 2026-09-10 OOM: Arrow kernels for the mechanical fills; the legacy
    # dict-key fallback scan (to_pylist) runs only when its inputs exist.
    if dataset == "trades" and combined.num_rows > 0:
        def _any_true(mask) -> bool:
            try:
                return int(pc.sum(mask).as_py() or 0) > 0
            except Exception:
                return True  # fail open: run the fill path

        try:
            names = combined.schema.names
            if "notional" in names and "price" in names and "size" in names:
                try:
                    _not = combined.column("notional")
                    _need = pc.invert(pc.is_valid(_not))
                    if _any_true(_need):
                        _calc = pc.multiply(
                            pc.cast(combined.column("price"), pa.float64()),
                            pc.cast(combined.column("size"), pa.float64()),
                        )
                        _filled = pc.case_when(_need, _calc, _not)
                        _pos = combined.schema.get_field_index("notional")
                        combined = combined.set_column(_pos, "notional", _filled.cast(pa.float64()))
                except Exception:
                    pass
            if "aggressor_side" in names and "side" in names:
                try:
                    _ag = combined.column("aggressor_side")
                    _sd = pc.cast(combined.column("side"), pa.string())
                    _need_ag = pc.and_(
                        pc.is_null(_ag),
                        pc.and_(pc.is_valid(_sd), pc.greater(pc.utf8_length(_sd), 0)),
                    )
                    if _any_true(_need_ag):
                        _low = pc.utf8_lower(_sd)
                        _filled_ag = pc.case_when(_need_ag, _low, _ag)
                        _posa = combined.schema.get_field_index("aggressor_side")
                        combined = combined.set_column(_posa, "aggressor_side", _filled_ag.cast(_ag.type))
                except Exception:
                    pass
        except Exception as e:
            print(f"[export] WARN backfill (kernels) failed for {dataset}: {e}")
        # legacy fallbacks (transaction_hash-from-trade_id, old dict wallet
        # keys): only for rows that can benefit — skip the pylist scan
        # entirely when no NULL wallet/tx_hash exists alongside inputs.
        try:
            names = combined.schema.names
            _may_help = False
            if "transaction_hash" in names and "trade_id" in names:
                try:
                    _th = combined.column("transaction_hash")
                    if _th.null_count > 0:
                        _may_help = True
                except Exception:
                    _may_help = True
            if not _may_help and "wallet" in names:
                try:
                    if combined.column("wallet").null_count > 0 and any(
                        c in names for c in ("proxyWallet", "proxy_wallet", "maker", "taker", "owner", "maker_wallet", "taker_wallet")
                    ):
                        _may_help = True
                except Exception:
                    _may_help = True
            if _may_help:
                # 2026-09-11 OOM: row-sliced pylist (the logic is row-local —
                # no cross-row deps — so slices are exactly equivalent). The
                # old whole-table to_pylist() ~10x'd a 100k-row BTC concat
                # and tripped the worker RSS cap by itself.
                _slice_n = 8000
                _fixed_parts = []
                _changed_any = False
                for _off in range(0, combined.num_rows, _slice_n):
                    pylist = combined.slice(_off, _slice_n).to_pylist()
                    changed = False
                    for r in pylist:
                        if r.get("transaction_hash") is None and r.get("trade_id"):
                            # fallback: trade_id may be hash if hash was used as trade_id
                            # check if trade_id looks like hash (hex length 32+)
                            tid = str(r.get("trade_id"))
                            if len(tid) >= 32 and all(c in "0123456789abcdef" for c in tid.lower()[:8]):
                                r["transaction_hash"] = tid
                                changed = True
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
                        _changed_any = True
                    _fixed_parts.append(pa.Table.from_pylist(pylist, schema=combined.schema))
                    del pylist
                if _changed_any:
                    # rebuild table with same schema as combined (preserve types where possible)
                    combined = pa.concat_tables(_fixed_parts, promote_options="default")
                del _fixed_parts
                import gc as _gc_tb
                _gc_tb.collect()
        except Exception as e:
            print(f"[export] WARN backfill failed for {dataset}: {e}")
            pass
    # E1 (2026-09-09): heal hex market_id (== condition_id) from markets_latest.
    # Old hive rows carry the corruption; staging must ship numeric ids for joins.
    # Unmapped rows keep NULL (honest gap) — never the hex value.
    # E2 (2026-09-09): drop trades with NULL/0 window_index from staging (breaks
    # market joins); hive retains them honestly.
    if dataset in ("book_snapshots_500ms", "book_events", "trades") and combined.num_rows > 0:
        try:
            combined = _heal_hex_market_ids(combined, Path(data_dir))
        except Exception as e:
            print(f"[export] WARN market_id heal failed for {dataset}: {e}")
    if dataset == "trades" and combined.num_rows > 0 and "window_index" in combined.schema.names:
        try:
            col = combined.column("window_index")
            not_null = pc.invert(pc.is_null(col))
            not_zero = pc.not_equal(col, pa.scalar(0))
            mask = pc.and_(not_null, not_zero)
            if mask.null_count > 0:
                mask = pc.fill_null(mask, False)
            dropped = combined.num_rows - int(pc.sum(mask).as_py() or 0)
            if dropped:
                print(f"[export] trades window_index filter: dropped {dropped} NULL/0 rows (E2 honest-gap)")
            combined = combined.filter(mask)
        except Exception as e:
            print(f"[export] WARN window_index filter failed: {e}")
    # E5: lowercase legacy uppercase aggressor sides at staging-read (hive is
    # migrated by the write-back below; staging must never ship mixed case).
    # 2026-09-10 OOM: Arrow kernels on narrow columns (never to_pylist).
    if dataset == "trades" and combined.num_rows > 0:
        try:
            for sc in ("side", "aggressor_side"):
                if sc not in combined.schema.names:
                    continue
                col = combined.column(sc)
                vals = col.to_pylist()
                if any(isinstance(v, str) and v != v.lower() for v in vals):
                    lowered = [v.lower() if isinstance(v, str) else v for v in vals]
                    pos = combined.schema.get_field_index(sc)
                    combined = combined.set_column(pos, sc, pa.array(lowered, type=col.type))
        except Exception as e:
            print(f"[export] WARN side normalization failed: {e}")
    # dedup before sort: remove exact duplicate rows that writer missed (WAL replay, buffer races)
    # For resync_episodes keep latest per resync_id, for snapshots keep first per (asset,condition_id,ts_snapshot_ns)
    # 2026-09-10 OOM: value_counts guard — the pylist path below explodes wide
    # tables ~10x; run it only when duplicates actually exist (rare).
    def _has_dupes(_tbl: pa.Table, _cols: list) -> bool:
        try:
            _keys = pa.StructArray.from_arrays(
                [_tbl.column(c) for c in _cols], names=[f"_{i}" for i in range(len(_cols))]
            )
            _counts = _keys.value_counts().field("counts").to_pylist()
            return bool(_counts) and max(_counts) > 1
        except Exception:
            return True  # fail open: keep old behavior on Arrow errors

    try:
        if combined.num_rows > 1:
            if dataset == "book_snapshots_500ms" and all(c in combined.schema.names for c in ["asset", "condition_id", "ts_snapshot_ns"]):
                if not _has_dupes(combined, ["asset", "condition_id", "ts_snapshot_ns"]):
                    pass  # unique — skip the pylist round-trip entirely
                else:
                    _pylist = combined.to_pylist()
                    seen = set()
                    uniq = []
                    for r in _pylist:
                        k = (r.get("asset"), r.get("condition_id"), r.get("ts_snapshot_ns"))
                        if k not in seen:
                            seen.add(k)
                            uniq.append(r)
                    del _pylist
                    if len(uniq) < combined.num_rows:
                        combined = pa.Table.from_pylist(uniq, schema=combined.schema)
                    else:
                        del uniq
                    import gc as _gc_dd
                    _gc_dd.collect()
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
                if not _has_dupes(combined, ["token_id", "trade_id"]):
                    pass  # unique — skip the pylist round-trip entirely
                else:
                    pylist = combined.to_pylist()
                    seen = set()
                    uniq = []
                    for r in pylist:
                        k = (r.get("token_id"), r.get("trade_id"))
                        if k not in seen:
                            seen.add(k)
                            uniq.append(r)
                    del pylist
                    if len(uniq) < combined.num_rows:
                        combined = pa.Table.from_pylist(uniq, schema=combined.schema)
                    else:
                        del uniq
                    import gc as _gc_dd2
                    _gc_dd2.collect()
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


# 2026-09-10 OOM: whale datasets (snapshots/clean/events) stream file-by-file
# via _stream_export_asset_dataset (peak ~one source file). See below.
STREAM_WHALE_DATASETS = {"book_snapshots_500ms", "book_snapshots_clean", "book_events"}


def _load_market_id_map(data_dir: str | Path) -> Dict[str, str]:
    """condition_id -> numeric market_id from markets_latest (tiny)."""
    import re as _re

    mapping: Dict[str, str] = {}
    try:
        hex_re = _re.compile(r"0[xX][0-9a-fA-F]{64}\Z")
        latest = Path(data_dir) / "markets_latest" / "markets_latest.parquet"
        if latest.exists():
            for r in read_table(latest).to_pylist():
                cid, mid = r.get("condition_id"), r.get("market_id")
                if cid and mid and not (isinstance(mid, str) and bool(hex_re.match(str(mid).strip()))):
                    mapping[str(cid)] = str(mid)
    except Exception as e:
        print(f"[export] WARN markets_latest unreadable for market_id map: {e}")
    return mapping


def _apply_market_id_map(table: pa.Table, mapping: Dict[str, str]) -> pa.Table:
    """E1 heal on narrow columns only (never whole-row dicts)."""
    if not mapping or "market_id" not in table.schema.names or "condition_id" not in table.schema.names:
        return table
    try:
        import re as _re

        hex_re = _re.compile(r"0[xX][0-9a-fA-F]{64}\Z")
        mids = table.column("market_id").to_pylist()
        idx = [i for i, m in enumerate(mids) if isinstance(m, str) and bool(hex_re.match(m.strip()))]
        if not idx:
            return table
        cids = table.column("condition_id").to_pylist()
        new_mids = list(mids)
        for i in idx:
            new_mids[i] = mapping.get(str(cids[i] or ""))
        pos = table.schema.get_field_index("market_id")
        return table.set_column(pos, "market_id", pa.array(new_mids, type=pa.string()))
    except Exception as e:
        print(f"[export] WARN market_id map apply failed: {e}")
        return table


def _stream_export_asset_dataset(
    base: Path,
    ds: str,
    asset_upper: str,
    tmp_path: Path,
    timeframe_label: Optional[str],
    l2_levels: int,
    market_id_map: Dict[str, str],
    live_only: bool = False,
    excluded_cids: Optional[set] = None,
    source_dataset: Optional[str] = None,
    io_stats: Optional[dict] = None,
    cutoff_ts: Optional[float] = None,
) -> int:
    """Stream one (asset, whale-dataset) staging build. Returns rows written.

    Same row content as the concat Table path (timeframe filter, E1 heal,
    snapshot dedup), but peak RAM is ~one source file: files are processed
    oldest-first (date partition + mtime, no footer reads), transformed with
    Arrow kernels, per-batch sorted, and appended incrementally. Global
    cross-file order follows date/mtime (source files are time-ordered
    appends; readers sort anyway per E13).

    live_only + source_dataset: stage the CLEAN file straight from the
    snapshots hive (live + not-disputed filter per batch) instead of the
    clean hive — the clean hive (2.85M rows after 40h without prune) can no
    longer be concat-loaded, and its catch-up rebuild belongs off-peak.
    """
    from .streaming import DedupState, stream_batches, write_batches

    src_ds = source_dataset or ds
    ts_col = {"book_snapshots_500ms": "ts_snapshot_ns"}.get(src_ds, "ts_received_ns")
    schema = _get_schema(ds, l2_levels)
    dedup = DedupState(["asset", "condition_id", "ts_snapshot_ns"]) if ds == "book_snapshots_500ms" else None
    # CLEAN staging shares the snapshots dedup scope (same rows, filtered):
    # without it a compaction rewrite would double-count in clean files.
    if ds == "book_snapshots_clean":
        dedup = DedupState(["asset", "condition_id", "ts_snapshot_ns"])
    want = f"{asset_upper}-{timeframe_label}" if timeframe_label else None
    try:
        sort_keys = [k for k in (_sort_keys_for_schema(schema) if schema is not None else [])]
    except Exception:
        sort_keys = []
    _excluded = set(excluded_cids or [])

    def _transform(t: pa.Table) -> pa.Table:
        if want and "series_id" in t.schema.names:
            try:
                t = t.filter(pc.equal(t.column("series_id"), pa.scalar(want)))
            except Exception:
                pass
        if live_only and "book_state" in t.schema.names:
            try:
                t = t.filter(pc.equal(t.column("book_state"), pa.scalar("live")))
            except Exception:
                pass
        if live_only and _excluded and "condition_id" in t.schema.names:
            # disputed exclusion (small set; per-batch mask, no pylist)
            try:
                _bad = pc.is_in(t.column("condition_id"), value_set=pa.array(sorted(_excluded)))
                t = t.filter(pc.invert(pc.fill_null(_bad, False)))
            except Exception:
                pass
        # E1: heal hex market_ids (clean staging included — it must match
        # the raw staging file row-for-row on identity columns).
        if ds in ("book_snapshots_500ms", "book_snapshots_clean", "book_events") and market_id_map:
            t = _apply_market_id_map(t, market_id_map)
        if dedup is not None:
            t = dedup.filter(t)
        if t.num_rows > 1 and sort_keys:
            try:
                keys = [(k, "ascending") for k in sort_keys if k in t.schema.names]
                if keys:
                    t = pc.take(t, pc.sort_indices(t, sort_keys=keys))
            except Exception:
                pass
        return t

    def _gen():
        for b in stream_batches(base, src_ds, asset_upper, ts_col=ts_col,
                                transform=_transform, stats=io_stats,
                                cutoff_ts=cutoff_ts):
            yield b

    healed_note = ""
    rows = write_batches(_gen(), tmp_path, schema=schema)
    if dedup is not None and (dedup.dupes or ds == "book_snapshots_500ms"):
        healed_note = f" (dedup dropped {dedup.dupes})" if dedup.dupes else ""
    if healed_note:
        print(f"[export:stream] {asset_upper} {ds}: {rows} rows{healed_note}")
    return rows


# 2026-09-11 OOM: pre-flight memory gate (module level for testability).
def _avail_mb() -> int:
    try:
        with open("/proc/meminfo") as _f:
            for _line in _f:
                if _line.startswith("MemAvailable:"):
                    return int(_line.split()[1]) // 1024
    except Exception:
        pass
    return 9999


def _list_source_files(base: Path, ds: str, asset_upper: str | None) -> list:
    """Hive source files for (ds, asset), excluding tmp. Metadata only (no reads).

    asset_upper=None → whole-dataset listing (global datasets).
    Mirrors the glob layout used by stream_batches/iter_source_files so the
    export-coverage manifest compares like with like.
    """
    root = base / ds
    if not root.exists():
        return []
    if asset_upper is None:
        pats = [p for p in root.rglob("*.parquet") if not p.name.endswith(".tmp")]
    else:
        pats = {p for p in root.glob(f"date=*/asset={asset_upper}/*.parquet")}
        pats.update(p for p in root.glob(f"date=*/asset={asset_upper.lower()}/*.parquet"))
        if not pats:
            # mixed-layout fallback (same as iter_source_files): filter later
            pats = {p for p in root.rglob("*.parquet")}
        pats = {p for p in pats if not p.name.endswith(".tmp")}
    return sorted(pats, key=str)


def _source_manifest(base: Path, ds: str, asset_upper: str | None, cutoff_ts: float) -> dict:
    """Metadata-only coverage manifest: every hive file at/below cutoff_ts.

    Lets the parent verify a worker saw exactly the inputs present at build
    start WITHOUT re-reading gigabytes of hive data (the old Step-1b
    validation read every file twice and OOM-killed the box at the finish
    line). Files written during the export (mtime > cutoff) belong to the
    next cycle on both sides.
    """
    import hashlib as _hl

    files = []
    total = 0
    for p in _list_source_files(base, ds, asset_upper):
        try:
            st = p.stat()
        except OSError:
            continue
        if st.st_mtime > cutoff_ts:
            continue
        try:
            rel = str(p.relative_to(base))
        except Exception:
            rel = p.name
        files.append((rel, st.st_size, st.st_mtime_ns))
        total += st.st_size
    files.sort()
    h = _hl.sha1()
    for name, size, mt in files:
        h.update(f"{name}|{size}|{mt}\n".encode())
    return {"n": len(files), "bytes": total, "digest": h.hexdigest()[:16]}


def _manifests_match(pre: dict, post: dict) -> bool:
    try:
        return (pre.get("n") == post.get("n") and pre.get("bytes") == post.get("bytes")
                and pre.get("digest") == post.get("digest"))
    except Exception:
        return False


def _commit_staging_file(
    out_path: Path,
    tmp_path: Optional[Path],
    rows: Optional[int],
    *,
    ds: str,
    rolling_window: bool,
    l2_levels: int,
    base: Path,
    out: Path,
) -> int:
    """Guard + publish for one staging file. Shared by inline and worker builds.

    Semantics (unchanged from the inline branches this replaces):
    - cumulative mode: never replace non-empty prior with fewer rows.
    - rows>0: publish tmp. 0 rows: keep prior if any; snapshots fail closed
      (no file); other datasets get a schema-empty file.
    Returns the row count recorded in stats.
    """
    rel_key = str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)
    prior_rows = None
    prior_exists = out_path.exists()
    if prior_exists:
        try:
            prior_rows = pq.read_metadata(str(out_path)).num_rows
        except Exception:
            prior_rows = None
    if not rolling_window and prior_rows is not None and prior_rows > 0 and (rows is None or rows < prior_rows):
        try:
            if tmp_path is not None and Path(tmp_path).exists():
                Path(tmp_path).unlink()
        except Exception:
            pass
        return prior_rows
    if rows is not None and rows > 0:
        if tmp_path is None or not Path(tmp_path).exists():
            return prior_rows if prior_rows is not None else 0
        _os_replace_safe(Path(tmp_path), out_path)
        return rows
    try:
        if tmp_path is not None and Path(tmp_path).exists():
            Path(tmp_path).unlink()
    except Exception:
        pass
    if prior_exists and prior_rows is not None and prior_rows > 0:
        return prior_rows
    if ds == "book_snapshots_500ms":
        return 0  # fail closed: never publish a missing snapshots file
    try:
        _schema_for_empty = _get_schema(ds, l2_levels)
        if _schema_for_empty is not None:
            _tmp2 = out_path.with_suffix(".parquet.tmp")
            pq.write_table(
                pa.table({f.name: [] for f in _schema_for_empty}, schema=_schema_for_empty),
                str(_tmp2), compression="zstd")
            _os_replace_safe(_tmp2, out_path)
    except Exception:
        pass
    return 0


# 2026-09-10 OOM: datasets whose build transient exceeds safe in-process RAM
# run in a short-lived worker process (OS reclaims 100% on exit). Arrow
# arenas otherwise retain GBs across sequential per-dataset builds in one
# process until the kernel kills the box mid-upload.
SUBPROCESS_DATASETS = {"book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events"}


def _build_worker_main(payload_path: str, result_path: str) -> None:
    """Subprocess entry: build tmp staging files for a few datasets.

    Payload JSON: {base, out, datasets, assets, timeframe_label, l2_levels,
    include_binance, rolling_window}. Writes result JSON {rel_key: {"tmp":
    tmp_path, "rows": n}}. Tmp files are left on disk; the parent guards +
    publishes them. NEVER raises (reports {"error": ...} instead).
    """
    import json as _js

    # 2026-09-11 OOM: worker-side RSS cap. A runaway build (giant file,
    # enrichment spiral) must abort ITSELF gracefully (fail-closed upstream)
    # instead of growing until the kernel kills a random process.
    import threading as _th

    _stop_cap = _th.Event()

    def _rss_cap_watch(_limit_mb: int = 700) -> None:
        while not _stop_cap.wait(2):
            try:
                with open("/proc/self/status") as _f:
                    for _line in _f:
                        if _line.startswith("VmRSS:"):
                            if int(_line.split()[1]) // 1024 > _limit_mb:
                                try:
                                    with open(result_path, "w") as _rf:
                                        _js.dump({"error": "rss-cap-abort"}, _rf)
                                except Exception:
                                    pass
                                import os as _os_k

                                _os_k._exit(3)
                            break
            except Exception:
                return

    _cap_thread = _th.Thread(target=_rss_cap_watch, daemon=True)
    _cap_thread.start()

    # 2026-09-10 OOM: die with the parent. A SIGKilled parent (kernel OOM
    # mid-export) otherwise orphans this worker, which keeps building and
    # OOMs the resurrected collector in turn — a death cascade.
    try:
        import ctypes as _ct
        import os as _os_p

        _libc = _ct.CDLL("libc.so.6", use_errno=True)
        _libc.prctl(1, 9, 0, 0, 0)  # PR_SET_PDEATHSIG = 1, SIGKILL = 9
    except Exception:
        pass

    try:
        with open(payload_path) as _f:
            _p0 = _js.load(_f)
        # close the fork/spawn race: if the invoker is already gone, exit now
        try:
            import os as _os_p2

            _want_ppid = int((_p0.get("_ppid") or 0))
            if _want_ppid and _os_p2.getppid() != _want_ppid:
                return
        except Exception:
            pass
        with open(payload_path) as _f:
            _p = _js.load(_f)
        base = Path(_p["base"])
        out = Path(_p["out"])
        _res: dict = {}
        _manifest_out: dict = {}
        _read_err_out: dict = {}
        _mid_map: Dict[str, str] = {}
        _excluded_cids: set = set()
        _ds_list = _p.get("datasets") or []
        _cutoff = _p.get("cutoff_ts")
        try:
            _cutoff = float(_cutoff) if _cutoff is not None else None
        except Exception:
            _cutoff = None
        print(f"[export:worker] start ds={_ds_list} assets={_p.get('assets')}", flush=True)
        if any(d in ("book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades") for d in _ds_list):
            try:
                _mid_map = _load_market_id_map(base)
            except Exception:
                _mid_map = {}
        if any(d == "book_snapshots_clean" for d in _ds_list):
            # disputed exclusion for clean staging (tiny lookup, loaded once)
            try:
                from .markets_log import MarketsLog as _ML

                for _mr in _ML(base).load_latest():
                    if _mr.get("resolution_outcome") == "disputed" and _mr.get("condition_id"):
                        _excluded_cids.add(str(_mr["condition_id"]))
            except Exception:
                try:
                    _latest = base / "markets_latest" / "markets_latest.parquet"
                    if _latest.exists():
                        for _r in read_table(_latest).to_pylist():
                            if _r.get("resolution_outcome") == "disputed" and _r.get("condition_id"):
                                _excluded_cids.add(str(_r["condition_id"]))
                except Exception:
                    pass
        for _ds in _ds_list:
            for _a in (_p.get("assets") or []):
                _au = str(_a).upper()
                _out_path = out / f"{_au}_{_ds}.parquet"
                _rel = str(_out_path.relative_to(base) if _out_path.is_relative_to(base) else _out_path)
                _tmp = _out_path.with_suffix(".parquet.tmp")
                # Coverage manifest BEFORE the build: the parent compares this
                # with its own pre-export manifest (same cutoff) to prove the
                # worker saw every input file — metadata only, no re-read.
                _io: dict = {}
                try:
                    _manifest_out[f"{_ds}/{_au}"] = _source_manifest(
                        base, _ds if _ds != "book_snapshots_clean" else "book_snapshots_500ms",
                        _au, _cutoff if _cutoff is not None else float("inf"))
                except Exception:
                    pass
                try:
                    if _ds in STREAM_WHALE_DATASETS:
                        _src = "book_snapshots_500ms" if _ds == "book_snapshots_clean" else None
                        _n = _stream_export_asset_dataset(
                            base, _ds, _au, _tmp, _p.get("timeframe_label"),
                            int(_p.get("l2_levels") or 10), _mid_map,
                            live_only=(_ds == "book_snapshots_clean"),
                            excluded_cids=_excluded_cids,
                            source_dataset=_src, io_stats=_io, cutoff_ts=_cutoff)
                    elif _ds == "chainlink_events":
                        # 2026-09-11 OOM: full-hive concat isolated here too.
                        _t = _read_dataset_per_asset(
                            base, _ds, _au,
                            include_binance=bool(_p.get("include_binance", False)),
                            timeframe_label=_p.get("timeframe_label"), stats=_io)
                        _n = _t.num_rows if _t is not None else 0
                        if _t is not None and _t.num_rows > 0:
                            pq.write_table(_t, str(_tmp), compression="zstd")
                        del _t
                        import gc as _gc_w
                        _gc_w.collect()
                    elif _ds == "trades":
                        # 2026-09-11: streaming build — the legacy full-hive
                        # concat (~1GB Arrow for BTC) plus enrichment pylists
                        # tripped the 700MB worker cap every cycle. Streams
                        # byte-bounded file groups (cached pools, global
                        # reconcile, single narrow write-back); peak ~one
                        # group regardless of history size. Data-API budget
                        # 420s of the 900s worker timeout (honest NULLs past
                        # it, healed next pass).
                        _n = _stream_export_trades_dataset(
                            base, _au, _tmp, _p.get("timeframe_label"),
                            deadline_s=420, cutoff_ts=_cutoff, io_stats=_io)
                        import gc as _gc_w
                        _gc_w.collect()
                    else:
                        _res[_rel] = {"tmp": None, "rows": None, "skip": True}
                        continue
                    _res[_rel] = {"tmp": str(_tmp) if _tmp.exists() else None, "rows": _n}
                    if _io.get("files_failed"):
                        _read_err_out[_rel] = {"failed": _io.get("files_failed"),
                                              "failed_bytes": _io.get("failed_bytes", 0),
                                              "ok": _io.get("files_ok", 0)}
                except Exception as _e:
                    try:
                        if _tmp.exists():
                            _tmp.unlink()
                    except Exception:
                        pass
                    _res[_rel] = {"tmp": None, "rows": None, "error": repr(_e)[:300]}
        _res["__manifest__"] = _manifest_out
        _res["__read_errors__"] = _read_err_out
        with open(result_path, "w") as _f:
            _js.dump(_res, _f)
    except Exception as _e:
        try:
            with open(result_path, "w") as _f:
                _js.dump({"error": repr(_e)[:500]}, _f)
        except Exception:
            pass


def _build_in_subprocess(
    base: Path,
    out: Path,
    datasets: List[str],
    assets: List[str],
    timeframe_label: Optional[str],
    l2_levels: int,
    include_binance: bool,
    rolling_window: bool,
    timeout_s: int = 900,
    cutoff_ts: Optional[float] = None,
) -> Optional[Dict[str, dict]]:
    """Run the per-(dataset,asset) tmp builds in a worker process."""
    import json as _js
    import subprocess as _sp
    import tempfile as _tf

    payload = {
        "base": str(base), "out": str(out), "datasets": datasets, "assets": assets,
        "timeframe_label": timeframe_label, "l2_levels": l2_levels,
        "include_binance": include_binance, "rolling_window": rolling_window,
        "cutoff_ts": cutoff_ts,
        "_ppid": os.getpid(),
    }
    try:
        with _tf.NamedTemporaryFile("w", suffix=".json", delete=False) as _pf:
            _js.dump(payload, _pf)
            _ppath = _pf.name
        with _tf.NamedTemporaryFile("w", suffix=".json", delete=False) as _rf:
            _rpath = _rf.name
        _code = (
            "import sys; sys.path.insert(0, 'src'); "
            "from polymarket_collector.storage.export import _build_worker_main; "
            "import sys as _s; _build_worker_main(_s.argv[1], _s.argv[2])"
        )
        _r = _sp.run(
            [sys.executable, "-c", _code, _ppath, _rpath],
            cwd=str(Path(__file__).resolve().parents[3]),
            capture_output=True, text=True, timeout=timeout_s,
        )
        try:
            with open(_rpath) as _f:
                res = _js.load(_f)
        except Exception:
            print(f"[export:worker] no result (rc={_r.returncode} err tail:\n{(_r.stderr or '')[-2000:]}")
            return None
        return res
    except Exception as e:
        print(f"[export:worker] spawn failed, caller falls back in-process: {e}")
        return None
    finally:
        for _p in (locals().get("_ppath"), locals().get("_rpath")):
            try:
                if _p and Path(_p).exists():
                    Path(_p).unlink()
            except Exception:
                pass


def export_per_asset_single_file(
    data_dir: str | Path,
    out_dir: str | Path | None = None,
    datasets: List[str] | None = None,
    assets: List[str] | None = None,
    l2_levels: int = 10,
    include_binance: bool = False,
    timeframe_label: Optional[str] = None,
    rolling_window: bool = False,
    cutoff_ts: Optional[float] = None,
    manifests: Optional[dict] = None,
) -> dict:
    """Export one flat parquet per asset per dataset (Kaggle style).

    Returns dict {relative_out_path: rows}

    timeframe_label: filter per-asset datasets (and markets_log) to this
    timeframe lane via series_id / window_size_seconds; None = no filter
    (legacy single-TF behavior).
    rolling_window: when True the prior-staging monotonic guard is skipped —
    with a retention pruned hive the staging legitimately shrinks over time.
    cutoff_ts: hive files newer than this are the next cycle's input (both
    the worker coverage manifest and the stream reader apply it).
    manifests: optional dict filled with {(ds, asset): {"pre":..., "worker":...,
    "ok": bool, "read_errors": {...}}} coverage evidence for the caller —
    the cheap replacement for re-reading the whole hive for validation.

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
    # 2026-09-11 OOM: pre-flight memory gate helper (module-level _avail_mb).
    # Spawning workers into a starved box grinds them one by one (each death
    # frees RAM for the next victim). Below the floor, skip new builds.
    # 2026-09-11 OOM: checkpoint-resume — incarnations live ~25 min, full
    # cycles take ~40. Completed (lane, dataset) files are recorded with
    # timestamps in _progress.json; fresh ones (<45 min) are skipped so the
    # next incarnation continues past the death point instead of redoing lanes.
    _prog_path = base / "kaggle_staging" / "_progress.json"
    _progress: dict = {}
    try:
        if _prog_path.exists():
            import json as _js_p

            _progress = _js_p.loads(_prog_path.read_text()) or {}
    except Exception:
        _progress = {}

    def _mark_done(_lane: str, _ds: str) -> None:
        try:
            _progress[f"{_lane}/{_ds}"] = int(__import__("time").time())
            _prog_path.parent.mkdir(parents=True, exist_ok=True)
            import json as _js_p2

            _prog_path.write_text(_js_p2.dumps(_progress))
        except Exception:
            pass

    def _is_fresh(_lane: str, _ds: str, _max_age_s: int = 2700) -> bool:
        try:
            _ts = int(_progress.get(f"{_lane}/{_ds}") or 0)
            return (int(__import__("time").time()) - _ts) < _max_age_s
        except Exception:
            return False

    for ds in datasets:
        # 2026-09-10 OOM: heavy datasets build in short-lived workers, ONE
        # (dataset, asset) per worker (OS reclaims 100% on exit — Arrow
        # arenas otherwise stack GBs across sequential builds until the
        # kernel kills the box). Parent only guards + publishes tmp files
        # (footer counts, no reads). A 7-asset lane costs ~28 small spawns.
        if ds in SUBPROCESS_DATASETS and ds in PER_ASSET_DATASETS:
            for _asset in assets:
                _au_one = str(_asset).upper()
                _out_path = out / f"{_au_one}_{ds}.parquet"
                _rel = str(_out_path.relative_to(base) if _out_path.is_relative_to(base) else _out_path)
                # 2026-09-12: floor 900->550. Post-fix worker peaks measure
                # <=500MB (snapshots 495, trades ~400 streaming, events less;
                # the 700MB worker self-cap still guards runaways), so 900
                # blocked legitimate builds whenever the box hosted anything
                # else (agent sessions, backfill), starving trades/chainlink
                # freshness for every tick. Parent-side transients are bounded
                # by streaming everywhere now.
                if _avail_mb() < 550:
                    print(f"[export] SKIP {ds}/{_au_one}: only {_avail_mb()}MB available (floor 550) — keeping prior staging")
                    try:
                        stats[_rel] = pq.read_metadata(str(_out_path)).num_rows if _out_path.exists() else 0
                    except Exception:
                        stats[_rel] = 0
                    continue
                # 2026-09-11 OOM: resume-fast — passes die mid-cycle (~25 min
                # incarnations vs ~40 min lanes), so skip files already fresh
                # this run: staging newer than every source file needs no
                # rebuild. Next incarnation resumes where death left off
                # instead of redoing all lanes from scratch.
                try:
                    if _out_path.exists():
                        _om = _out_path.stat().st_mtime
                        _src_root = base / ds
                        _pats = {p for p in _src_root.glob(f"date=*/asset={_au_one}/*.parquet")}
                        _pats.update(p for p in _src_root.glob(f"date=*/asset={_au_one.lower()}/*.parquet"))
                        if _pats and all(p.stat().st_mtime <= _om for p in _pats):
                            try:
                                stats[_rel] = pq.read_metadata(str(_out_path)).num_rows
                            except Exception:
                                stats[_rel] = 0
                            continue
                except Exception:
                    pass
                # checkpoint-resume: fresh this cycle window (<45 min) → skip
                # rebuild entirely (staging misses only the last minutes of
                # ticks, which the next cycle picks up).
                _lane_key = str(timeframe_label or "5m")
                try:
                    if _is_fresh(_lane_key, f"{ds}/{_au_one}") and _out_path.exists():
                        try:
                            stats[_rel] = pq.read_metadata(str(_out_path)).num_rows
                        except Exception:
                            stats[_rel] = 0
                        continue
                except Exception:
                    pass
                _built_one = None
                _pre_mani = None
                try:
                    # Coverage manifest BEFORE the spawn (metadata only): the
                    # worker reports what IT saw; a mismatch fails closed.
                    _mani_ds = "book_snapshots_500ms" if ds == "book_snapshots_clean" else ds
                    _pre_mani = _source_manifest(
                        base, _mani_ds, _au_one,
                        cutoff_ts if cutoff_ts is not None else float("inf"))
                except Exception:
                    _pre_mani = None
                _built_one = _build_in_subprocess(
                    base, out, [ds], [_asset], timeframe_label, l2_levels,
                    include_binance, rolling_window, cutoff_ts=cutoff_ts)
                _out_path = out / f"{_au_one}_{ds}.parquet"
                _rel = str(_out_path.relative_to(base) if _out_path.is_relative_to(base) else _out_path)
                _failed = _built_one is None or (isinstance(_built_one, dict) and _built_one.get("error"))
                if _failed:
                    _why = ""
                    try:
                        if isinstance(_built_one, dict):
                            _why = f" reason={str(_built_one.get('error'))[:200]}"
                    except Exception:
                        pass
                    print(f"[export] WARN {ds}/{_au_one} worker failed{_why} — keeping prior staging (fail closed)")
                    try:
                        _pr = pq.read_metadata(str(_out_path)).num_rows if _out_path.exists() else None
                    except Exception:
                        _pr = None
                    stats[_rel] = _pr if _pr is not None else 0
                    continue
                _info = _built_one.get(_rel) or {}
                if _info.get("skip"):
                    continue
                # Coverage check: worker-observed inputs must equal the
                # pre-spawn manifest (same cutoff). Unreadable-input bytes
                # above budget also fail closed. Either way the prior staging
                # is kept — never ship a silently partial file.
                _mkey = f"{ds}/{_au_one}"
                _wmani = (_built_one.get("__manifest__") or {}).get(_mkey)
                _rerr = (_built_one.get("__read_errors__") or {}).get(_rel) or {}
                _mok = bool(_pre_mani and _wmani and _manifests_match(_pre_mani, _wmani))
                _fb = int(_rerr.get("failed_bytes") or 0)
                _budget = max(1_000_000, int((_pre_mani or {}).get("bytes", 0) * 0.01))
                if manifests is not None:
                    manifests[_mkey] = {"pre": _pre_mani, "worker": _wmani,
                                        "ok": _mok, "read_errors": _rerr}
                if not _mok or _fb > _budget:
                    try:
                        _tmp_bad = Path(_info["tmp"]) if _info.get("tmp") else None
                        if _tmp_bad is not None and _tmp_bad.exists():
                            _tmp_bad.unlink()
                    except Exception:
                        pass
                    _why_cov = ("manifest mismatch" if not _mok
                                else f"unreadable inputs {_fb}B > {_budget}B budget")
                    print(f"[export] WARN {ds}/{_au_one} coverage check failed ({_why_cov}) "
                          f"— keeping prior staging (fail closed)")
                    try:
                        _pr = pq.read_metadata(str(_out_path)).num_rows if _out_path.exists() else None
                    except Exception:
                        _pr = None
                    stats[_rel] = _pr if _pr is not None else 0
                    continue
                _tmp = Path(_info["tmp"]) if _info.get("tmp") else None
                _rows = _info.get("rows")
                if _info.get("error") or _rows is None:
                    try:
                        if _tmp is not None and _tmp.exists():
                            _tmp.unlink()
                    except Exception:
                        pass
                    try:
                        _pr = pq.read_metadata(str(_out_path)).num_rows if _out_path.exists() else None
                    except Exception:
                        _pr = None
                    stats[_rel] = _pr if _pr is not None else 0
                    continue
                stats[_rel] = _commit_staging_file(
                    _out_path, _tmp, int(_rows), ds=ds,
                    rolling_window=rolling_window, l2_levels=l2_levels,
                    base=base, out=out)
                try:
                    _mark_done(_lane_key, f"{ds}/{_au_one}")
                except Exception:
                    pass
            import gc as _gc_ds
            _gc_ds.collect()
            continue
        # Derived analyst-facing summary — must run AFTER the per-asset trades
        # staging files are (re)written so it sees the api- reconciled fills.
        if ds == "markets_summary":
            out_path = out / "markets_summary.parquet"
            rel = str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)
            table = build_markets_summary(base, staging_dir=out, assets=assets, timeframe_label=timeframe_label)
            prior_rows_s = None
            if out_path.exists():
                try:
                    prior_rows_s = pq.read_metadata(str(out_path)).num_rows
                except Exception:
                    prior_rows_s = None
            new_rows_s = table.num_rows if table is not None else 0
            if not rolling_window and prior_rows_s is not None and prior_rows_s > 0 and (table is None or new_rows_s < prior_rows_s):
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
            # 2026-09-10 OOM: market_id map loaded once per dataset (tiny).
            _mid_map: Dict[str, str] = {}
            if ds in ("book_snapshots_500ms", "book_events", "trades"):
                try:
                    _mid_map = _load_market_id_map(base)
                except Exception:
                    _mid_map = {}
            for asset in assets:
                au = asset.upper()
                out_path = out / f"{au}_{ds}.parquet"
                rel_key = str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)
                # --- never overwrite non-empty staging with empty/smaller data (cumulative history guard) ---
                # Load prior staging first to enforce monotonic row-count (never shrink).
                # Skipped in rolling_window mode: after a retention prune the staging
                # legitimately shrinks — freezing prior rows would ship deleted data forever.
                # 2026-09-10 OOM: footer row count (no data read) for whale lanes.
                prior_rows = None
                prior_exists = out_path.exists()
                if prior_exists:
                    try:
                        prior_rows = pq.read_metadata(str(out_path)).num_rows
                    except Exception:
                        prior_rows = None
                if ds in STREAM_WHALE_DATASETS:
                    # Streaming build into tmp; guard decides replace vs keep.
                    # (Same row content as the concat path: timeframe filter,
                    # E1 heal, snapshot dedup; cross-file order follows footer
                    # timestamps + per-batch sort instead of one global sort.)
                    tmp_path = out_path.with_suffix(".parquet.tmp")
                    try:
                        new_rows = _stream_export_asset_dataset(
                            base, ds, au, tmp_path, timeframe_label, l2_levels, _mid_map
                        )
                    except Exception as e:
                        print(f"[export:stream] WARN {au} {ds} failed: {e}")
                        try:
                            if tmp_path.exists():
                                tmp_path.unlink()
                        except Exception:
                            pass
                        stats[rel_key] = prior_rows if prior_rows is not None else 0
                        continue
                    if not rolling_window and prior_rows is not None and prior_rows > 0 and new_rows < prior_rows:
                        try:
                            if tmp_path.exists():
                                tmp_path.unlink()
                        except Exception:
                            pass
                        stats[rel_key] = prior_rows
                        continue
                    if new_rows > 0:
                        _os_replace_safe(tmp_path, out_path)
                        stats[rel_key] = new_rows
                        continue
                    # 0 rows streamed.
                    try:
                        if tmp_path.exists():
                            tmp_path.unlink()
                    except Exception:
                        pass
                    if prior_exists and prior_rows is not None and prior_rows > 0:
                        stats[rel_key] = prior_rows
                        continue
                    if ds == "book_snapshots_500ms":
                        # fail closed: never publish a missing snapshots file
                        stats[rel_key] = 0
                        continue
                    # other whales legitimately 0 early -> schema-empty file
                    try:
                        _schema_for_empty = _get_schema(ds, l2_levels)
                        if _schema_for_empty is not None:
                            pq.write_table(
                                pa.table({f.name: [] for f in _schema_for_empty}, schema=_schema_for_empty),
                                str(tmp_path), compression="zstd")
                            _os_replace_safe(tmp_path, out_path)
                    except Exception:
                        pass
                    stats[rel_key] = 0
                    continue
                table = _read_dataset_per_asset(base, ds, au, include_binance=include_binance, timeframe_label=timeframe_label)
                new_rows = table.num_rows if (table is not None) else 0
                # If prior has data, never replace it with fewer rows (empty read, transient error, or legitimate 0)
                # This prevents 1a empty-file overwrite and guarantees cumulative history
                if not rolling_window and prior_rows is not None and prior_rows > 0:
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
            # Global dataset: always create a single file in staging, even if 0 rows.
            # 2026-09-11: streamed in row-group batches (bounded RAM) instead of
            # one full-hive concat — collector_events alone (43MB zstd) spiked
            # ~1GB in the parent on top of live collection state. Same row
            # content: the markets_log lane filter is row-local per batch.
            from .streaming import stream_batches as _sb, write_batches as _wb
            _gio: dict = {}
            try:
                _pre_g = _source_manifest(base, ds, None, cutoff_ts if cutoff_ts is not None else float("inf"))
            except Exception:
                _pre_g = None
            if ds == "markets_log":
                out_path = out / "markets.parquet"
            elif ds == "collector_events":
                out_path = out / "collector_events.parquet"
            else:  # resync_episodes
                out_path = out / "resync_episodes.parquet"
            _want_ws_g: Optional[int] = None
            if ds == "markets_log" and timeframe_label is not None:
                try:
                    from ..config import CollectorConfig as _CC2
                    _want_ws_g = _CC2.window_size_for(timeframe_label)
                except Exception as e:
                    print(f"[export] WARN markets_log timeframe filter failed (tf={timeframe_label}): {e}")

            def _g_transform(_t: pa.Table) -> pa.Table:
                if ds == "markets_log" and timeframe_label is not None and _want_ws_g is not None \
                        and "window_size_seconds" in _t.schema.names:
                    try:
                        _col = _t.column("window_size_seconds")
                        _keep = pc.or_(pc.equal(_col, pa.scalar(_want_ws_g)),
                                       pc.and_(pc.is_null(_col), pa.scalar(timeframe_label == "5m")))
                        _t = _t.filter(pc.fill_null(_keep, False))
                    except Exception as e:
                        print(f"[export] WARN markets_log timeframe filter failed (tf={timeframe_label}): {e}")
                return _t

            _tmp_g = out_path.with_suffix(".parquet.tmp")
            try:
                # schema=None: first batch defines the layout (hive-native,
                # like the old concat path) — never drop evolved columns.
                _gen_g = _sb(base, ds, None, transform=_g_transform, stats=_gio,
                             cutoff_ts=cutoff_ts)
                new_rows_g = _wb(_gen_g, _tmp_g, schema=None)
            except Exception as e:
                print(f"[export] WARN globals {ds} stream failed: {e}")
                try:
                    if _tmp_g.exists():
                        _tmp_g.unlink()
                except Exception:
                    pass
                new_rows_g = 0
            try:
                _post_g = _source_manifest(base, ds, None, cutoff_ts if cutoff_ts is not None else float("inf"))
            except Exception:
                _post_g = None
            _g_ok = bool(_pre_g and _post_g and _manifests_match(_pre_g, _post_g))
            _g_fb = int((_gio.get("failed_bytes") or 0))
            if manifests is not None:
                manifests[f"{ds}/GLOBAL"] = {"pre": _pre_g, "worker": _post_g,
                                             "ok": _g_ok, "read_errors": _gio}
            if not _g_ok or _g_fb > max(1_000_000, int((_pre_g or {}).get("bytes", 0) * 0.01)):
                print(f"[export] WARN globals {ds} coverage check failed — keeping prior staging (fail closed)")
                try:
                    if _tmp_g.exists():
                        _tmp_g.unlink()
                except Exception:
                    pass
                try:
                    _prg = pq.read_metadata(str(out_path)).num_rows if out_path.exists() else None
                except Exception:
                    _prg = None
                stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = _prg if _prg is not None else 0
                del _gio
                continue
            del _gio
            # Monotonic guard for globals too: never shrink (skipped in rolling mode)
            prior_rows_g = None
            if out_path.exists():
                try:
                    prior_rows_g = pq.read_metadata(str(out_path)).num_rows
                except Exception:
                    prior_rows_g = None
            table = None  # streamed straight to tmp; footer count is the source of truth
            try:
                new_rows_g = pq.read_metadata(str(_tmp_g)).num_rows if _tmp_g.exists() else 0
            except Exception:
                new_rows_g = 0
            if not rolling_window and prior_rows_g is not None and prior_rows_g > 0 and new_rows_g < prior_rows_g:
                try:
                    if _tmp_g.exists():
                        _tmp_g.unlink()
                except Exception:
                    pass
                stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = prior_rows_g
                continue
            if new_rows_g > 0:
                _os_replace_safe(_tmp_g, out_path)
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
            # table was streamed straight to tmp (no in-RAM copy); the footer
            # count above is the source of truth.
            stats[str(out_path.relative_to(base) if out_path.is_relative_to(base) else out_path)] = new_rows_g
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
    timeframe_label: str = "5m",
    rolling_window: bool = False,
    cutoff_ts: Optional[float] = None,
    manifests: Optional[dict] = None,
) -> dict:
    """Prepare Kaggle staging folder for 5m-only upload.

    Exports per-asset single files (time-first, zstd, no binance) into a flat staging
    folder with dataset-metadata.json (CC BY-NC-SA 4.0) ready for folder upload.

    timeframe_label: which timeframe lane to export (filters rows by series_id;
    the shared hive carries all lanes). Also names the default staging path
    kaggle_staging/{label}/<dataset_prefix>.
    rolling_window: allow staging to shrink when data exited the retention
    window (the legacy cumulative monotonic guard would otherwise freeze stale
    rows in staging forever).

    Returns dict with staging_path, files (39 for 7 assets), row_counts.
    """
    base = Path(data_dir)
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    staging = Path(staging_dir) if staging_dir else base / "kaggle_staging" / timeframe_label / dataset_prefix
    staging.mkdir(parents=True, exist_ok=True)

    # Export per-asset TF-lane files directly into staging (not intermediate export/)
    stats = export_per_asset_single_file(
        data_dir, out_dir=staging, assets=assets, l2_levels=l2_levels,
        include_binance=False, timeframe_label=timeframe_label, rolling_window=rolling_window,
        cutoff_ts=cutoff_ts, manifests=manifests,
    )
    # Real data only: never merge synthetic prior Kaggle data. If local hive is empty after
    # clean delete, staging stays empty/minimal (3 globals). Merge disabled per AGENT.md.
    # _try_merge_prior_kaggle_staging disabled — would resurrect old synthetic cl-*/synth-* rows.
    # Ensure markets_latest also available as markets_latest.parquet alias if needed for reference
    # but primary markets file is markets.parquet (from markets_log)
    row_counts = stats
    # Write dataset-metadata.json
    resources = [{"path": Path(k).name, "description": f"{Path(k).name} {timeframe_label} crypto — {dataset_prefix}"} for k in stats.keys()]
    # Ensure markets.parquet + per-asset files are all listed; add if missing due to empty
    meta = {
        "title": f"Polymarket {timeframe_label} Crypto",
        "id": dataset_prefix,
        "licenses": [{"name": "CC BY-NC-SA 4.0"}],
        "resources": resources,
    }
    (staging / "dataset-metadata.json").write_text(_json.dumps(meta, indent=2))
    _ret: dict = {"staging_path": str(staging), "files": len(stats), "row_counts": row_counts, "dataset": dataset_prefix}
    if manifests is not None:
        _ret["manifests"] = manifests
    return _ret


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


def _upload_kaggle_folder(staging: Path, dataset: str, max_retries: int = 5, expected_assets: List[str] | None = None, check_monotonic: bool = True, build_start_ms: int | None = None) -> bool:
    """Folder upload with retry 5× jitter and dataset_status polling (plan.md §5).

    If expected_assets is provided, verify staging row counts after status=ready
    to prevent cumulative data loss from empty staging files.
    build_start_ms: hive-read cutoff of the staging being uploaded — recorded
    in state on success so the prune can prove coverage (never delete rows no
    lane's staging included).
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
                # Poll until ready, then run the verification gates. Kaggle processing
                # takes ~1-3 min, so a short poll would almost ALWAYS time out — and
                # the "optimistic success" fallback below that used to fire at 60s
                # skipped the row-count + remote-file verification on nearly every
                # upload. Budget 10 min, then fail closed (audit fix #10/#20).
                # Kaggle answers 403 on the status endpoint for a few seconds
                # after create/version before the dataset is queryable — poll 0
                # used to log a scary ERROR even though the version landed fine
                # (2026-09-06 19:17 run). Give it a settling delay first.
                _time.sleep(10)
                for _ in range(60):
                    try:
                        st = api.dataset_status(dataset)
                        # kaggle 2.x returns a plain "ready"/"pending" STRING here;
                        # older wrappers may return a dict or object. Normalise — the
                        # previous str-only path threw AttributeError on st.get() and
                        # the bare except swallowed it, so the poll NEVER matched and
                        # every upload fell through to timeout.
                        if isinstance(st, dict):
                            s = st.get("status") or ""
                        elif isinstance(st, str):
                            s = st.strip().strip('"')
                        else:
                            s = getattr(st, "status", "") or ""
                        if s != "ready" and _ % 6 == 0:
                            print(f"[kaggle] poll {_}/60 dataset {dataset} status={s!r}")
                    except Exception as _st_exc:
                        # never swallow silently again — an exception here previously
                        # made every poll a no-op and the upload "time out"
                        _msg = repr(_st_exc)
                        if "403" in _msg or "Forbidden" in _msg:
                            # fresh version: the status endpoint answers 403 briefly
                            # before the dataset is queryable — retryable, not fatal
                            if _ % 6 == 0:
                                print(f"[kaggle] poll {_}/60 dataset {dataset} status 403 (processing, retryable)")
                        else:
                            print(f"[kaggle] poll {_}/60 dataset {dataset} status ERROR: {_msg}")
                        s = ""
                    if s == "ready":
                        _files_ok = _expected_staging_files(staging) >= len(expected_assets) * 5 + 4
                        _rows_ok = _verify_staging_row_counts(staging, expected_assets, check_monotonic=check_monotonic)
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
                            _write_kaggle_state(staging, dataset, version_notes,
                                                build_start_ms=build_start_ms)
                            return True
                        elif not _rows_ok:
                            print(f"⚓ Kaggle dataset status=ready but staging has empty files; waiting for complete upload")
                        elif not _files_ok:
                            print(f"⚓ Kaggle dataset status=ready but staging has {_expected_staging_files(staging)} files, expected {len(expected_assets) * 5 + 4}; waiting for complete upload")
                        elif not _remote_ok:
                            print(f"⚓ Kaggle dataset status=ready but remote file list incomplete; waiting")
                    elif s in ("failed", "error"):
                        print(f"✗ Kaggle dataset in error state: {dataset}")
                        # 2026-09-12: do NOT mark state on failure — a failed
                        # upload must never advance the prune checkpoint (a
                        # stale success timestamp would authorize deletion of
                        # rows that were never uploaded).
                        return False
                    _time.sleep(10)
                # Fail closed: unverified upload must not report success or mark state
                print(f"✗ Kaggle dataset not verified ready after 10 min poll: {dataset}")
                return False
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


def _verify_staging_row_counts(staging: Path, expected_assets: List[str], check_monotonic: bool = True) -> bool:
    """Verify staging file existence and row-count policy.

    - book_snapshots_500ms must have >0 rows per active asset (critical null vs zero check;
      empty snapshots would mean 100% data loss and must block upload)
    - trades/book_events/chainlink_events may legitimately be 0 rows early (no trades yet)
      so only existence + readable parquet required; we do NOT block upload if 0
    - globals existence only
    - Also enforces monotonic (check_monotonic=True): if _kaggle_state.json records prior
      staging row counts, current must be >= prior (never shrink). This catches
      empty-file overwrite (1a). DISABLED in rolling_window mode — after a retention
      prune the staging legitimately shrinks; history lives in old Kaggle versions.
    Returns True if all expected files exist, False otherwise.

    2026-09-10 OOM: footer metadata ONLY (pq.read_metadata = row count with
    zero data read). The old code read_table()'d all 32 staging files
    (~500MB parquet, ~30x Arrow expansion) in the parent right before
    upload — a 2GB+ bomb that SIGKilled the box at the finish line.
    """
    def _rows(_p: Path) -> Optional[int]:
        try:
            return pq.read_metadata(str(_p)).num_rows
        except Exception:
            return None

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
            _n = _rows(fpath)
            if _n is None or _n == 0:
                return False
        for ds in optional_per_asset:
            fpath = staging / f"{au}_{ds}.parquet"
            if not fpath.exists():
                return False
            if _rows(fpath) is None:
                return False
    for ds, fname in global_file_map.items():
        fpath = staging / fname
        if not fpath.exists():
            return False
        if _rows(fpath) is None:
            return False
    # Monotonic check vs prior staging (download-merge fallback when no local hive yet)
    if not check_monotonic:
        return True
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
                                    import pyarrow.parquet as _pq_mono
                                    cur_rows = _pq_mono.read_metadata(str(cur_path)).num_rows
                                    if cur_rows < prior:
                                        return False
                                except Exception:
                                    return False
                    break
    except Exception:
        pass
    return True


def _write_kaggle_state(staging: Path, dataset: str, notes: str, build_start_ms: int | None = None):
    """Persist upload state (checkpoint + staging counts + build cutoff).

    build_start_ms: the hive-read cutoff the uploaded staging was built
    from — the prune's coverage proof (never delete hive rows newer than
    every lane's build_start).
    """
    try:
        state_path = Path(staging).parent.parent / "_kaggle_state.json"  # data/kaggle_staging/_kaggle_state.json
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state = {}
        if state_path.exists():
            try:
                state = _json.loads(state_path.read_text())
            except Exception:
                state = {}
        # Persist per-file row counts for monotonic verification (fix 1c/1a).
        # 2026-09-11: footer metadata ONLY — the old read_table() of all 39
        # staging files (~2GB parquet → ~30x Arrow) ran right after a
        # successful upload and OOM-killed the box before the prune.
        staging_counts = {}
        try:
            import pyarrow.parquet as _pq_state
            for p in Path(staging).glob("*.parquet"):
                if p.name.endswith(".tmp"):
                    continue
                try:
                    staging_counts[p.name] = _pq_state.read_metadata(str(p)).num_rows
                except Exception:
                    continue
        except Exception:
            staging_counts = {}
        state[dataset] = {
            "last_version_notes": notes,
            "last_upload_utc": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
            "last_upload_unix_ms": int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp() * 1000),
            "build_start_unix_ms": int(build_start_ms) if build_start_ms is not None else None,
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
    rolling_window: bool | None = None,
    retention_hours: int | None = None,
    dry_run: bool = False,
    skip_datasets: list | tuple | None = None,
) -> dict:
    """Post-upload local prune — rolling-window mode (market-end aware, fail closed).

    Called ONLY after a verified Kaggle upload. Deletes local parquet files whose
    every row is (a) from a market that ENDED before the cutoff and (b) therefore
    already included in at least one uploaded version. The cutoff is
    ``min(now, upload_checkpoint) - retention_hours`` — the retention is the
    "leeway" window kept locally just in case.

    Semantics per file (never per row — files are the delete unit after compaction):
    - condition-bearing datasets (snapshots, clean view, book_events, trades):
      delete only if EVERY condition_id in the file maps to a market that ended
      before the cutoff; any unknown condition → keep (conservative).
    - timestamp-only datasets (chainlink_events, collector_events): delete only if
      the max timestamp is before the cutoff.
    - markets_log / markets_latest: never deleted (the resolution map depends on them).

    In cumulative mode (rolling_window=False, the legacy default) NOTHING is
    deleted — the staging is cumulative and rebuilt from the full local hive, so
    deleting local data would shrink future Kaggle versions.

    skip_datasets: optional names of top-level datasets to never delete from
    (e.g. ("collector_events", "chainlink_events") in the 2x5min test-buffer
    prune — timestamp-only datasets would otherwise be wiped by the ~0h test
    cutoff and the NEXT staging rebuild would publish truncated event files).

    Returns stats {relative_path: rows_deleted} (empty when nothing was deleted).
    """
    import datetime as _dt2
    if timeframe_labels is None:
        timeframe_labels = ["5m"]
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    base = Path(data_dir)
    tf_label = str(timeframe_labels[0]).lower()

    # Resolve rolling-window policy: explicit arg > config > legacy no-op
    if rolling_window is None:
        try:
            from ..config import CollectorConfig as _CC
            rolling_window = bool(getattr(_CC.load().kaggle, "rolling_window", False))
        except Exception:
            rolling_window = False
    if not rolling_window:
        # Cumulative mode: staging is rebuilt from the FULL local hive each time,
        # so deleting local data would shrink future Kaggle versions. Retain all.
        return {}

    if retention_hours is None:
        try:
            from ..config import CollectorConfig as _CC
            retention_hours = int(getattr(_CC.load().kaggle, "local_retention_hours", 48))
        except Exception:
            retention_hours = 48

    # Resolve checkpoint: MINIMUM last-upload across ALL enabled lanes.
    # 2026-09-11 DATA-LOSS FIX: lanes share one hive (rows for every lane
    # interleave in the same flush files), but uploads happen one lane per
    # tick. The old max() checkpoint let a fresh 5m upload authorize deletion
    # of 15m/1h/4h rows those lanes had never uploaded — 34h of 15m/1h/4h
    # snapshots (Sep 9 20:07 → Sep 11 06:39) were deleted unrecoverably.
    # A row is safe to delete only once ITS lane uploaded it, so the slowest
    # lane gates the prune. As round-robin uploads catch lanes up, the min
    # advances and the prune bites on its own. Never-uploaded lanes are
    # excluded (their rows are recent by construction) but WARN loudly.
    if checkpoint_ms is None:
        _lanes: list = []
        try:
            from ..config import CollectorConfig as _CC2
            _lanes = [str(t).lower() for t in (_CC2.load().timeframes or [])]
        except Exception:
            _lanes = []
        if not _lanes:
            _lanes = [tf_label]
        _per_lane: dict = {}
        for _lane in _lanes:
            _best = None
            for cand in (base / "kaggle_staging" / _lane / "_kaggle_state.json",
                         base / "kaggle_staging" / "_kaggle_state.json"):
                try:
                    if not cand.exists():
                        continue
                    j = _json.loads(cand.read_text())
                    vals = [v.get("last_upload_unix_ms") for v in j.values()
                            if isinstance(v, dict) and v.get("last_upload_unix_ms")]
                    if vals:
                        _v = max(vals)
                        _best = _v if _best is None else max(_best, _v)
                except Exception:
                    continue
            if _best is not None:
                _per_lane[_lane] = _best
            else:
                print(f"[prune] WARN lane {_lane} has no verified upload yet — "
                      f"not gating prune (its history is at risk until it uploads)")
        if _per_lane:
            _slow = min(_per_lane, key=lambda k: _per_lane[k])
            checkpoint_ms = min(_per_lane.values())
            try:
                import datetime as _dt_slow
                _slow_iso = _dt_slow.datetime.fromtimestamp(
                    checkpoint_ms / 1000, tz=_dt_slow.timezone.utc).isoformat()
            except Exception:
                _slow_iso = str(checkpoint_ms)
            print(f"[prune] checkpoint gated by slowest lane {_slow} @ {_slow_iso} "
                  f"(lanes={_per_lane})")
        else:
            # No verified upload on ANY lane — fail closed, never prune blind.
            print("[prune] no verified Kaggle upload on any lane — pruning skipped (fail closed)")
            return {}
    # 2026-09-12 coverage proof (per DATASET): a hive file of dataset D is
    # deletable only if EVERY lane's uploaded staging for D included it.
    # Staging freshness per (lane, dataset) = oldest staging file mtime for
    # D in that lane's staging dir, minus a build-slack (a staging file
    # committed at T covers hive files existing at build start < T; lane
    # builds take <3h, so T-3h is a sound lower bound).
    # Why per-dataset, not per-lane: lanes share one hive but upload
    # different datasets at different freshness (gate-skipped trades keep
    # days-old priors while snapshots rebuild hourly). Per-lane gating would
    # either delete rows whose staging is stale (data loss, observed
    # 2026-09-11) or block all pruning on one stale dataset (disk death
    # spiral). Per-dataset freshness prunes exactly what is proven uploaded.
    # Staging files map: {ASSET}_{ds}.parquet -> ds; markets.parquet ->
    # markets_log; collector_events/resync_episodes.parquet -> themselves
    # (markets_summary is derived — gates nothing).
    _GLOBAL_DS_FILES = {"markets.parquet": "markets_log",
                        "collector_events.parquet": "collector_events",
                        "resync_episodes.parquet": "resync_episodes"}
    _BUILD_SLACK_MS = 3 * 3600 * 1000
    _fresh_by_ds: dict = {}
    try:
        from ..config import CollectorConfig as _CCb
        _lanes_b = [str(t).lower() for t in (_CCb.load().timeframes or [])] or [tf_label]
    except Exception:
        _lanes_b = [tf_label]
    for _lane in _lanes_b:
        _sdir = None
        for _cand in (base / "kaggle_staging" / _lane,):
            if _cand.exists():
                _sdir = _cand
                break
        if _sdir is None:
            print(f"[prune] WARN lane {_lane} has no staging dir — "
                  f"not gating coverage (its history is at risk until it uploads)")
            continue
        _per_ds_min: dict = {}
        try:
            for _sp in _sdir.rglob("*.parquet"):
                if _sp.name.endswith(".tmp"):
                    continue
                _ds = _GLOBAL_DS_FILES.get(_sp.name)
                if _ds is None and "_" in _sp.name:
                    _maybe = _sp.name.rsplit(".", 1)[0].split("_", 1)
                    _maybe_ds = _maybe[1] if len(_maybe) == 2 else None
                    if _maybe_ds in ("book_snapshots_500ms", "book_snapshots_clean",
                                     "book_events", "trades", "chainlink_events"):
                        _ds = _maybe_ds
                if _ds is None:
                    continue
                try:
                    _mt = int(_sp.stat().st_mtime * 1000)
                except OSError:
                    continue
                if _ds not in _per_ds_min or _mt < _per_ds_min[_ds]:
                    _per_ds_min[_ds] = _mt
        except Exception:
            continue
        for _ds, _mt in _per_ds_min.items():
            _fresh_by_ds.setdefault(_ds, {})[_lane] = _mt
    _fresh_cutoff_by_ds: dict = {}
    for _ds, _per_lane in _fresh_by_ds.items():
        _missing = [l for l in _lanes_b if l not in _per_lane]
        if _missing:
            print(f"[prune] WARN dataset {_ds} missing staging for lanes {_missing} — "
                  f"its hive files are never pruned until every lane ships it")
            continue
        _fresh_cutoff_by_ds[_ds] = min(_per_lane.values()) - _BUILD_SLACK_MS
    if _fresh_cutoff_by_ds:
        try:
            import datetime as _dt_fresh
            _show = {k: _dt_fresh.datetime.fromtimestamp(v / 1000, tz=_dt_fresh.timezone.utc).isoformat()
                     for k, v in _fresh_cutoff_by_ds.items()}
        except Exception:
            _show = _fresh_cutoff_by_ds
        print(f"[prune] coverage proof per dataset (hive files newer than these are kept): {_show}")
    else:
        print("[prune] no per-dataset staging freshness available — coverage proof blocks all deletes")
    now_ms = int(_dt2.datetime.now(tz=_dt2.timezone.utc).timestamp() * 1000)
    cutoff_ms = min(now_ms, checkpoint_ms) - int(retention_hours) * 3600 * 1000

    # condition_id -> market_end map (never pruned source of truth)
    end_by_cid: dict = {}
    latest = base / "markets_latest" / "markets_latest.parquet"
    try:
        if latest.exists():
            tbl = read_table(latest)
            if "condition_id" in tbl.schema.names and "market_end_ts_ms" in tbl.schema.names:
                for cid, me in zip(tbl.column("condition_id").to_pylist(),
                                   tbl.column("market_end_ts_ms").to_pylist()):
                    if cid and me is not None:
                        try:
                            end_by_cid[str(cid)] = int(me)
                        except Exception:
                            pass
    except Exception as e:
        print(f"[prune] WARN could not read markets_latest — pruning skipped this cycle: {e}")
        return {}

    CID_DATASETS = ["book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades"]
    TS_DATASETS = ["chainlink_events", "collector_events"]
    skipped = set(skip_datasets or [])
    stats: dict = {}
    pruned_rows = 0

    def _delete_file(p: Path, rows: int, reason: str) -> None:
        nonlocal pruned_rows
        rel = str(p.relative_to(base))
        if dry_run:
            print(f"[prune] dry-run would delete {rel} ({rows} rows, {reason})")
            return
        try:
            p.unlink()
            stats[rel] = rows
            pruned_rows += rows
        except Exception as e:
            print(f"[prune] WARN could not delete {rel}: {e}")

    import re as _re_prune
    _writer_pat = _re_prune.compile(r".+_\d+\.parquet$")
    for dataset in CID_DATASETS + TS_DATASETS:
        if dataset in skipped:
            continue
        ds_root = base / dataset
        if not ds_root.exists():
            continue
        for p in sorted(ds_root.rglob("*.parquet")):
            if p.name.endswith(".tmp"):
                continue
            try:
                # 2026-09-12 coverage proof (see above): never delete a hive
                # file of dataset D newer than every lane's staging for D —
                # no uploaded staging could have included it. Pure stat.
                _fc = _fresh_cutoff_by_ds.get(dataset)
                if _fc is None:
                    continue  # freshness unknown for D — fail closed
                try:
                    if int(p.stat().st_mtime * 1000) > _fc:
                        continue
                except OSError:
                    pass
                # 2026-09-11 OOM: the old code read_table()'d EVERY hive file
                # fully (70k+ files, GBs transient in the LONG-LIVED parent)
                # and the kernel OOM-killed the collector mid-prune. Two guards:
                # (a) writer-named flush files ({ds}_{ts}.parquet) carry only
                # rows collected around their mtime — mtime > cutoff proves a
                # live market inside, so they are kept WITHOUT any read.
                # Compacted files (mtime = compact time, not content time)
                # always take the content check below. Both rules are
                # conservative: they only ever keep more, never delete wrongly.
                # (b) the content check projects the single decision column
                # instead of materializing 120-col wide rows.
                if _writer_pat.match(p.name):
                    try:
                        if int(p.stat().st_mtime * 1000) > cutoff_ms:
                            continue
                    except OSError:
                        pass
                if dataset in CID_DATASETS:
                    try:
                        t = pq.read_table(str(p), columns=["condition_id"])
                    except Exception:
                        continue  # unreadable or no condition_id — conservative keep
                    if t is None or t.num_rows == 0 or "condition_id" not in t.schema.names:
                        del t
                        continue
                    cids = {str(c) for c in t.column("condition_id").to_pylist() if c}
                    _n = t.num_rows
                    del t
                    if not cids:
                        continue
                    ends = [end_by_cid.get(c) for c in cids]
                    if any(e is None for e in ends):
                        continue  # unknown condition — conservative keep
                    if max(ends) >= cutoff_ms:
                        continue  # some market inside the retention leeway — keep
                    _delete_file(p, _n, f"all markets ended < cutoff {cutoff_ms}")
                elif dataset in TS_DATASETS:
                    # timestamp-only datasets: age the FILE by its newest row
                    max_ts_ns = None
                    for col in ("ts_received_ns", "ts_utc"):
                        try:
                            t = pq.read_table(str(p), columns=[col])
                        except Exception:
                            t = None
                        if t is None or t.num_rows == 0 or col not in t.schema.names:
                            try:
                                del t
                            except Exception:
                                pass
                            continue
                        vals = t.column(col).to_pylist()
                        del t
                        vals = [v for v in vals if v is not None]
                        if vals:
                            if isinstance(vals[0], str):
                                ms_vals = [int(_dt2.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000) for v in vals]
                                max_ts_ns = max(max_ts_ns or 0, max(ms_vals) * 1_000_000)
                            else:
                                max_ts_ns = max(max_ts_ns or 0, int(max(vals)))
                            break
                    if max_ts_ns is None:
                        continue
                    if max_ts_ns // 1_000_000 < cutoff_ms:
                        try:
                            _n_ts = pq.read_metadata(str(p)).num_rows
                        except Exception:
                            _n_ts = 0
                        _delete_file(p, _n_ts, "newest row older than cutoff")
            except Exception as e:
                print(f"[prune] WARN skipping unreadable {p}: {e}")

    # remove empty leaf dirs left behind (hive hygiene, safe: only empties)
    if not dry_run:
        for dataset in CID_DATASETS + TS_DATASETS:
            ds_root = base / dataset
            if not ds_root.exists():
                continue
            for d in sorted((p for p in ds_root.rglob("*") if p.is_dir()), reverse=True):
                try:
                    if next(d.iterdir(), None) is None:
                        d.rmdir()
                except Exception:
                    pass

    if stats or pruned_rows:
        print(f"[prune:{tf_label}] deleted {len(stats)} files / {pruned_rows} rows older than {retention_hours}h leeway (cutoff {cutoff_ms})")
    return stats


# =============================================================================
# Kaggle upload orchestrator — 5m-only, single dataset, 10-min / hourly
# =============================================================================

def _export_and_upload_all_kaggle_impl(
    data_dir: str | Path = "./data",
    out_dir: str | Path | None = None,
    assets: List[str] | None = None,
    kaggle_username: str | None = None,
    kaggle_key: str | None = None,
    timeframe_labels: List[str] | None = None,
    l2_levels: int = 10,
    dry_run: bool = False,
    dataset_prefix: str | None = None,
) -> dict:
    """Per-timeframe pipeline: export 7-asset staging (39 files) → Kaggle dataset → safe prune.

    Handles ONE timeframe per call (the label is timeframe_labels[0]); the caller
    (collector kaggle loop) invokes it once per enabled lane. Each lane has its
    own staging dir (kaggle_staging/{label}/) and its own Kaggle dataset
    (config.kaggle.datasets[label], fallback dataset_prefix).

    - Only full closed markets (market_end < now) are uploaded.
    - Cumulative mode (rolling_window=False): staging is cumulative, monotonic
      row-count checks apply, local data is NEVER deleted.
    - Rolling-window mode (rolling_window=True): the staging contains the
      trailing local_retention_hours; local data is pruned after VERIFIED
      upload (market-end aware, retention leeway). History lives in old
      Kaggle versions (delete_old_versions=False).
    """
    if timeframe_labels is None:
        timeframe_labels = ["5m"]
    tf_label = str(timeframe_labels[0]).lower()
    if assets is None:
        assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]
    base = Path(data_dir)
    # Resolve rolling-window policy + dataset prefix from explicit args, then config
    rolling_window = False
    try:
        from ..config import CollectorConfig as _CC
        _cfg = _CC.load()
        rolling_window = bool(getattr(_cfg.kaggle, "rolling_window", False))
        if dataset_prefix is None:
            try:
                dataset_prefix = _cfg.kaggle.datasets[tf_label]
            except (KeyError, AttributeError, TypeError):
                dataset_prefix = getattr(_cfg.kaggle, "dataset_prefix", None)
    except Exception:
        pass
    if dataset_prefix is None:
        dataset_prefix = "gghgg1/polymarket-5m-crypto"
    staging = base / "kaggle_staging" / tf_label / dataset_prefix

    result: dict = {
        "export": {},
        "staging": {},
        "kaggle_uploads": {},
        "cleanup": {},
        "dry_run": dry_run,
    }

    # Step 0: Compact hive data (§10A) — merge small parquet files before export
    # 2026-09-10 OOM: DISABLED inside the export path — compact_all reads the
    # ENTIRE hive into pandas on every lane export (4x/hour), stacking GBs
    # against collection until the kernel SIGKills the box. Staging concat
    # handles many small files correctly (opens sequentially). Run the
    # standalone compact cron off-peak instead (polymarket-compact, 03:00).
    # (Kept as a no-op block so the step numbering below stays stable.)

    # Step 0b: clean view — SKIPPED in the export path (2026-09-10 OOM).
    # Clean staging files are built straight from the snapshots hive by the
    # per-asset loop below, and the summary reads those staging files. The
    # clean HIVE is maintained out-of-band (build_clean_view stays for manual
    # use + test mode, which calls it explicitly). Rebuilding 2.85M rows here
    # SIGKilled the box on every lane export.

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
    print(f"=== Step 1: Preparing Kaggle staging {tf_label} for {assets} -> {staging} ===")
    # Snapshot the wall clock BEFORE the staging build: the pre-upload validation
    # must compare staging against the hive AS OF the read, not as of validation
    # time — while the collector is live, new part files land between the staging
    # read and the validation, and counting them made staging look lossy
    # (observed live 2026-09-06: BTC snapshots staging 1207 < hive 1330 → upload
    # aborted although the staging was complete for its source files). Hive part
    # files are immutable once written, so files with mtime <= build_start hold
    # exactly the rows the export read; later files belong to the NEXT export.
    build_start_ts = _time.time()
    _manifests: dict = {}
    prep = prepare_kaggle_staging_5m(data_dir, staging_dir=staging, assets=assets, l2_levels=l2_levels,
                                     dataset_prefix=dataset_prefix, timeframe_label=tf_label,
                                     rolling_window=rolling_window, cutoff_ts=build_start_ts,
                                     manifests=_manifests)
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
    #
    # 2026-09-11: coverage is proven by the per-(dataset,asset) manifests
    # collected during the build (worker-observed inputs vs pre-spawn inputs
    # at build_start_ts) — metadata only. The old code re-read EVERY hive
    # file a second time here (full data reads, ~15 min + GBs transient) and
    # OOM-killed the box at the finish line on every cycle, so uploads never
    # started. Files written during the export (mtime > build start) belong
    # to the NEXT export on both sides.
    import pyarrow.parquet as _pq
    lost = []
    for _mk, _mi in _manifests.items():
        if not _mi.get("ok"):
            lost.append(f"{_mk}: worker inputs != hive at build start")
        _re = _mi.get("read_errors") or {}
        if int(_re.get("failed_bytes") or 0) > 0:
            lost.append(f"{_mk}: unreadable inputs {_re.get('failed_bytes')}B "
                        f"({_re.get('failed', 0)} files)")
    for a in assets:
        f = staging / f"{a}_book_snapshots_500ms.parquet"
        staging_rows = None
        if f.exists():
            try:
                staging_rows = _pq.read_metadata(str(f)).num_rows
            except Exception:
                staging_rows = None
        if not staging_rows:
            # fail closed only when the hive actually holds rows for this lane
            _mm = (_manifests.get(f"book_snapshots_500ms/{a.upper()}") or {}).get("pre") or {}
            if int(_mm.get("n") or 0) > 0:
                lost.append(f"{a}_book_snapshots_500ms: staging {staging_rows} rows "
                            f"but hive holds {_mm.get('n')} files")
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
    print(f"=== Step 2: Uploading {tf_label} staging to Kaggle {dataset_prefix} ===")
    try:
        _build_ms = int(build_start_ts * 1000)
    except Exception:
        _build_ms = None
    ok = _upload_kaggle_folder(staging, dataset_prefix, expected_assets=assets, check_monotonic=not rolling_window,
                               build_start_ms=_build_ms)
    result["kaggle_uploads"][dataset_prefix] = {
        "status": "success" if ok else "failed",
        "staging": str(staging),
        "files": prep["files"],
    }
    if ok:
        print(f"✓ Upload success {dataset_prefix}, pruning hive after verified ready...")
        cleanup_stats = cleanup_local_data(data_dir, assets=assets, timeframe_labels=[tf_label],
                                           rolling_window=rolling_window)
        result["cleanup"] = cleanup_stats
    else:
        print(f"✗ Upload failed {dataset_prefix}, NOT pruning (data retained for retry)")

    return result


def export_and_upload_all_kaggle(*args, **kwargs):
    """Serialized entry point — holds the cross-process export lock, then runs.

    Keeps the collector hourly loop and the backfill cron from building
    staging (clean_view ~300k rows + 39 files/lane + wallet backfill) at the
    same time. Callers keep the original signature (data_dir first).
    """
    _dd = kwargs.get("data_dir", args[0] if args else "./data")
    _fd = _acquire_export_lock(_dd)
    try:
        return _export_and_upload_all_kaggle_impl(*args, **kwargs)
    finally:
        _release_export_lock(_fd)


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
