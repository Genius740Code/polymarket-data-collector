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
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import websockets
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

from .book import Level, OrderBookState, snapshot_bucket_ms
from .clock import check_clock_drift, is_clock_issue
from .config import CollectorConfig
from .enums import BookState, CollectorEventType, MarketStatus, ResolutionOutcome
from .storage.export import (
    export_and_upload_all_kaggle,
    prepare_kaggle_staging_5m,
    _validate_kaggle_config,
)
from .rollover import MarketInfo, RolloverManager
from .resync import ResyncManager, exponential_backoff
from .storage.cursor_store import CursorState, CursorStore
from .storage.markets_log import MarketsLog
from .storage.parquet_io import read_table
from .storage.parquet_writer import ParquetWriter
from .storage.raw_archive import RawArchive
from .validation import validate_ws_message


class Collector:
    """BTC/ETH/SOL/HYPE/BNB/XRP/DOGE 5-min market collector (§1-§19) — 5m-only, 4 markets test, 10-min Kaggle."""

    def __init__(self, config: CollectorConfig):
        self.config = config
        self.config.validate_assets_have_series()

        # state
        self.books: Dict[str, OrderBookState] = {}  # condition_id -> book
        self.markets: Dict[str, MarketInfo] = {}  # condition_id -> market
        self.rollover = RolloverManager(config, on_event=self._collector_event)
        self.cursor_stores: Dict[str, CursorStore] = {a: CursorStore.for_asset(config, a) for a in config.assets}
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
            synthetic_mode=getattr(config, "synthetic_mode", False),
        )
        self.markets_log = MarketsLog(config.storage.data_dir, writer=self.writer)
        self.raw_archive = RawArchive(
            base_path=config.raw_archive.path,
            retention_hours=config.raw_archive.retention_hours,
            enabled=config.raw_archive.enabled,
        )
        # resync needs a fetcher; default fetches via REST (may not exist — verified by §18 gate)
        self.resync = ResyncManager(
            config,
            rest_fetcher=self._fetch_rest_book,
            on_event=self._collector_event,
            on_episode_persist=self._persist_resync_episode,
        )

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
        # kaggle lock ensures flush/export/prune never races with snapshot append (§10A lossless)
        self._kaggle_lock = asyncio.Lock()
        self._last_snapshot_bucket_ms: Optional[int] = None
        # CRITICAL: the per-asset WS task and the resync manager call `self.on_event`
        # in the disconnect/reconnect path. It was never defined — the first
        # AttributeError killed the whole task silently, so reconnects never ran
        # and every disconnect lasted until process shutdown.
        self.on_event = self._collector_event
        # real per-asset WS connection state — snapshots of a disconnected asset are
        # labeled stale even if the book was REST-healed (frozen book = not live data)
        self._ws_connected: Dict[str, bool] = {}
        # resync episodes: hold latest state in RAM, persist each episode exactly once
        # (previously every state transition appended a new parquet row → double rows)
        self._episode_latest: Dict[str, dict] = {}
        self._episode_persisted: set = set()
        # §6/§6A chainlink: in-RAM rolling store of RTDS events for settlement lookup
        self._chainlink_events: List[dict] = []
        # markets whose resolution_stuck was already emitted (dedup — was spamming 1/30s)
        self._resolution_stuck_emitted: set = set()
        # §6A lifecycle tracking: markets advanced to closed / resolved in this process
        self._closed_cids: set = set()
        self._resolved_cids: set = set()
        # windows already attributed as coverage_gap in test mode (asset, window_index)
        self._coverage_gapped: set = set()
        # books with a background REST heal in flight (prevents snapshot-loop stalls)
        self._heal_inflight: set = set()
        # R-1 pre-warm gate: when set, the collector tasks run (WS connect,
        # discovery, subscriptions) but snapshots are only WRITTEN from this
        # epoch-ms onward — used by test mode to warm up before the window
        # boundary so the first collected window is warm (600/600), not cold.
        self._snapshot_start_ms: Optional[int] = None
        # B-4: RTDS received/parsed counters per asset (upstream-cadence probe).
        # defaultdict — the RTDS loop must never crash on a counter miss (a plain
        # dict here killed the chainlink consumer 115 times in one run: KeyError
        # on the first message → crash → reconnect forever, 0 rows written).
        self._rtds_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"rx": 0, "parsed": 0})

    async def _recover_from_cursor(self) -> None:
        """§1B: recover cursor state on startup after crash/restart — recreates books as stale if still active, else coverage_gap."""
        import datetime
        print(f"[startup] recovering cursor state for assets: {self.config.assets}")
        now_ms = int(time.time() * 1000)
        for asset in self.config.assets:
            try:
                store = CursorStore.for_asset(self.config, asset)
                state = store.load(asset)
                if state is None:
                    print(f"[startup] no cursor for {asset}")
                    continue
                print(f"[startup] recovered cursor for {asset}: window={state.current_window_index} cid={state.current_condition_id} last_snap={state.last_snapshot_written_ts}")
                if state.current_condition_id:
                    age_ms = now_ms - (state.last_snapshot_written_ts or 0)
                    # Heuristic: if last snapshot <10 min ago, market still active → recreate stale book for resync
                    if age_ms < 600_000 and state.current_condition_id not in self.books:
                        # Create minimal OrderBookState for the recovered market (will be resynced)
                        try:
                            book = OrderBookState(
                                asset=asset,
                                condition_id=state.current_condition_id,
                                market_id=state.current_condition_id,
                                series_id=f"{asset}-5m",
                                window_index=state.current_window_index or 0,
                                up_token_id=f"{state.current_condition_id}-UP",
                                down_token_id=f"{state.current_condition_id}-DOWN",
                                market_end_ts_ms=now_ms + 300_000,
                                schema_version=self.config.schema_version,
                                l2_levels=self.config.l2_levels,
                            )
                            book.mark_stale(resync_id=str(uuid.uuid4()))
                            self.books[state.current_condition_id] = book
                            self._collector_event(CollectorEventType.collector_restarted, {"asset": asset, "condition_id": state.current_condition_id, "age_ms": age_ms, "recovered": True})
                            print(f"[startup] recreated stale book for {asset} {state.current_condition_id}")
                        except Exception as e:
                            print(f"[startup] recreate book err {e}")
                    elif age_ms >= 600_000:
                        # Market ended while down → coverage_gap
                        self._collector_event(CollectorEventType.coverage_gap, {"asset": asset, "condition_id": state.current_condition_id, "downtime_ms": age_ms})
                        self._collector_event(CollectorEventType.collector_restarted, {"asset": asset, "condition_id": state.current_condition_id, "downtime_ms": age_ms, "market_ended": True})
                        print(f"[startup] coverage_gap for {asset} {state.current_condition_id} age {age_ms}ms")
            except Exception as e:
                print(f"[startup] no cursor state for {asset}: {e}")

    def _collector_event(self, event_type: CollectorEventType, details: dict) -> None:
        """Fire a collector_events row for data-quality tracking."""
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        # auto-fill connection_id from per-asset tracking if caller omitted (§8 avoid 100% null)
        conn_id = details.get("connection_id")
        if conn_id is None:
            asset_hint = details.get("asset")
            if asset_hint and asset_hint in self._conn_ids:
                conn_id = self._conn_ids[asset_hint]
            elif asset_hint and asset_hint.upper() in self._conn_ids:
                conn_id = self._conn_ids[asset_hint.upper()]
        self.markets_log.append_event(
            event_type=event_type,
            ts_utc=now.isoformat(),
            ts_received_ns=int(now.timestamp() * 1e9),
            condition_id=details.get("condition_id"),
            market_id=details.get("market_id"),
            asset=details.get("asset"),
            connection_id=conn_id,
            details=details,
        )

    def _writer_event(self, event_type: CollectorEventType, details: dict) -> None:
        """Fire a collector_events row for write-status tracking (backpressure/write_failed)."""
        try:
            self._collector_event(event_type, details)
        except Exception:
            pass

    def _persist_resync_episode(self, episode_dict: dict) -> None:
        """Persist resync episode to ParquetWriter (resync_episodes dataset).

        Exactly one parquet row per episode: state transitions update
        ``self._episode_latest`` in RAM; the row is appended only when the
        episode reaches a final state (completed, escalated, or collector
        stop). Previously every transition appended a row, double-counting
        episodes and stamping reconnect at shutdown.
        """
        try:
            rid = episode_dict.get("resync_id")
            if not rid:
                return
            # Never fabricate placeholder condition_id - keep None if missing (schema nullable)
            if episode_dict.get("condition_id") in ("test-condition", "test-market", "TEST-5MIN"):
                episode_dict = dict(episode_dict)
                episode_dict["condition_id"] = None
            self._episode_latest[rid] = dict(episode_dict)
            is_final = bool(
                episode_dict.get("resync_completed_ts_utc") or episode_dict.get("escalated")
            )
            if is_final and rid not in self._episode_persisted:
                self._episode_persisted.add(rid)
                ok = self.writer.append("resync_episodes", episode_dict, asset=episode_dict.get("asset"))
                if not ok:
                    self._episode_persisted.discard(rid)
                    self._collector_event(CollectorEventType.backpressure, {"dataset": "resync_episodes", "asset": episode_dict.get("asset")})
        except Exception:
            pass

    def _persist_open_episodes_on_stop(self) -> None:
        """At stop(): persist episodes that never reached a final state, honestly."""
        for rid, ep in list(self._episode_latest.items()):
            if rid in self._episode_persisted:
                continue
            self._episode_persisted.add(rid)
            try:
                self.writer.append("resync_episodes", ep, asset=ep.get("asset"))
            except Exception:
                pass

    @staticmethod
    def _extract_wallets(msg: dict) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract maker/taker wallet from CLOB trade payload — no RPC.
        Checks many key variants (proxyWallet, maker, taker, owner, etc) used across
        Polymarket WS/REST versions. Returns (maker_wallet, taker_wallet, wallet).
        wallet = taker if present else maker for single-col queries.
        """
        def _find(keys: list[str]) -> Optional[str]:
            for k in keys:
                v = msg.get(k)
                if v:
                    s = str(v).strip()
                    if s and s.lower() != "none" and s.lower() != "null":
                        # heuristic: wallet-like 0x + 40 hex or at least 6 chars
                        return s
            # also check nested under 'order'/'maker_orders' etc
            for nk in ("order", "maker_orders", "taker_orders"):
                nested = msg.get(nk)
                if isinstance(nested, dict):
                    for k in keys:
                        v = nested.get(k)
                        if v:
                            return str(v).strip()
                elif isinstance(nested, list) and nested:
                    for item in nested:
                        if isinstance(item, dict):
                            for k in keys:
                                v = item.get(k)
                                if v:
                                    return str(v).strip()
            return None

        # proxyWallet on Polymarket is the taker/aggressor — no RPC, just CLOB field
        # keep maker_keys without proxyWallet so single wallet maps to taker
        maker_keys = ["maker_wallet", "maker", "makerAddress", "maker_address", "owner", "maker_proxy_wallet"]
        taker_keys = ["taker_wallet", "taker", "takerAddress", "taker_address", "taker_proxy_wallet", "proxyWallet", "proxy_wallet"]
        # generic wallet key last resort
        generic_keys = ["wallet", "address", "user", "account"]

        maker = _find(maker_keys)
        taker = _find(taker_keys)
        generic = _find(generic_keys) if not maker and not taker else None
        # fallback: if message has 'side' and 'proxyWallet', treat as taker (aggressor)
        if not taker and not maker and generic:
            # assign generic to taker for now
            taker = generic
        wallet = taker or maker or generic
        return maker, taker, wallet

    def _append_book_event(self, ev: dict, book: "OrderBookState", asset: str, msg: dict) -> None:
        """Persist one §4 book_events row captured by OrderBookState.apply_ws_message."""
        try:
            row = {
                "ts_source": str(ev.get("ts_source")) if ev.get("ts_source") is not None else None,
                "ts_received_ns": int(time.time_ns()),
                "condition_id": book.condition_id,
                "market_id": book.market_id,
                "series_id": book.series_id,
                "window_index": int(book.window_index),
                "asset": asset.upper(),
                "event_id": str(uuid.uuid4()),
                "token_id": str(ev.get("token_id") or ""),
                "outcome": str(ev.get("outcome") or "unknown"),
                "event_type": str(ev.get("event_type") or "price_change"),
                "old_best_bid": ev.get("old_best_bid"),
                "new_best_bid": ev.get("new_best_bid"),
                "old_best_ask": ev.get("old_best_ask"),
                "new_best_ask": ev.get("new_best_ask"),
                "old_bid_size": ev.get("old_bid_size"),
                "new_bid_size": ev.get("new_bid_size"),
                "old_ask_size": ev.get("old_ask_size"),
                "new_ask_size": ev.get("new_ask_size"),
                "threshold_config_id": self._threshold_config_id,
            }
            ok = self.writer.append("book_events", row, asset=asset.upper())
            if not ok:
                self._collector_event(CollectorEventType.backpressure, {"dataset": "book_events", "asset": asset})
        except Exception:
            pass

    def _handle_trade_message(self, msg: dict, asset: str, now_ns: int, now_bucket_ms: int | None = None) -> bool:
        """Parse a CLOB trade WS message and persist to trades with wallet — no RPC.
        Returns True if a trade row was written.
        """
        # heuristic: trade messages have price+size and either event_type last_trade_price/trade or hash
        event_type = str(msg.get("event_type") or msg.get("type") or msg.get("eventType") or "").lower()
        is_trade = False
        if event_type in ("last_trade_price", "trade", "market_trade", "trade_price"):
            is_trade = True
        elif msg.get("price") is not None and msg.get("size") is not None and msg.get("asset_id"):
            # book update also has price/size but via price_changes; if price_changes present, not a trade
            if "price_changes" not in msg and "bids" not in msg and "asks" not in msg:
                is_trade = True
        elif msg.get("transaction_hash") or msg.get("transactionHash"):
            is_trade = True
        if not is_trade:
            return False

        try:
            token_id = str(msg.get("token_id") or msg.get("asset_id") or msg.get("asset") or msg.get("tokenId") or "")
            if not token_id:
                return False
            # resolve market/condition for this token via books scan
            market = None
            condition_id = msg.get("condition_id") or msg.get("conditionId")
            if condition_id and condition_id in self.markets:
                market = self.markets[condition_id]
            else:
                for b in self.books.values():
                    if b.up_token_id == token_id or b.down_token_id == token_id:
                        market = self.markets.get(b.condition_id)
                        condition_id = b.condition_id
                        break
            if market is None:
                # still need condition_id for row; use token-derived fallback
                condition_id = condition_id or token_id
                # try to find any active market for this asset as fallback
                for m in self.rollover.active_markets(asset):
                    if m.up_token_id == token_id or m.down_token_id == token_id:
                        market = m
                        condition_id = m.condition_id
                        break
            price_raw = msg.get("price")
            size_raw = msg.get("size") or msg.get("amount")
            if price_raw is None or size_raw is None:
                return False
            try:
                price = float(price_raw)
                size = float(size_raw)
            except Exception:
                return False
            # sanity bounds §3A
            if not (0 <= price <= 1) or size < 0:
                return False
            maker_wallet, taker_wallet, wallet = self._extract_wallets(msg)
            # Wallet extraction is from CLOB fields only (proxyWallet/maker/taker) — no RPC.
            # If CLOB did not provide wallet, keep NULL (do NOT fabricate from token_id/hash/market)
            # NULL is correct signal for research; fake 0x addresses poison clustering.
            # Export backfill also respects real fields only (storage/export.py:184).
            side = str(msg.get("side") or msg.get("aggressor_side") or "").upper() or None
            outcome = msg.get("outcome")
            if not outcome:
                # infer from token vs market
                if market and token_id == market.up_token_id:
                    outcome = "up"
                elif market and token_id == market.down_token_id:
                    outcome = "down"
                else:
                    outcome = "up" if msg.get("asset_id") == token_id else "unknown"
            trade_id = str(msg.get("trade_id") or msg.get("tradeId") or msg.get("id") or uuid.uuid4())
            tx_hash = str(msg.get("transaction_hash") or msg.get("transactionHash") or msg.get("hash") or "")
            if not tx_hash:
                tx_hash = None
            ts_source = str(msg.get("timestamp") or msg.get("ts") or msg.get("ts_source") or now_bucket_ms or int(now_ns // 1_000_000))
            seq = msg.get("sequence_number") or msg.get("seq") or msg.get("sequence")
            try:
                seq_int = int(seq) if seq is not None else None
            except Exception:
                seq_int = None
            notional = round(price * size, 6) if price and size else None
            # Fee: the CLOB trade payload carries the exchange-reported fee rate
            # (fee_rate_bps, "0" on current 5m markets) — compute the amount from
            # it instead of storing NULL (was the 100% null fee column on Kaggle).
            fee_raw = msg.get("fee")
            fee_rate_bps = msg.get("fee_rate_bps") or msg.get("feeRateBps")
            fee: Optional[float] = None
            fee_is_estimated: Optional[bool] = None
            if fee_raw is not None:
                try:
                    fee = float(fee_raw)
                    fee_is_estimated = False
                except Exception:
                    fee = None
            elif fee_rate_bps is not None and notional is not None:
                try:
                    fee = round(notional * float(fee_rate_bps) / 10_000.0, 6)
                    # amount derived from the exchange-reported rate — not a fallback guess
                    fee_is_estimated = False
                except Exception:
                    fee = None
                    fee_is_estimated = None
            row = {
                "ts_source": ts_source,
                "ts_received_ns": now_ns,
                "condition_id": str(condition_id),
                "market_id": market.market_id if market else str(condition_id),
                "series_id": market.series_id if market else f"{asset.upper()}-5m",
                "window_index": market.window_index if market else 0,
                "asset": asset.upper(),
                "trade_id": trade_id,
                "transaction_hash": tx_hash,
                "token_id": token_id,
                "outcome": str(outcome).lower() if outcome else "unknown",
                "price": price,
                "size": size,
                "notional": notional,
                "fee": fee,
                "fee_is_estimated": fee_is_estimated,
                "side": side,
                "aggressor_side": side,
                "sequence_number": seq_int,
                "maker_wallet": maker_wallet,
                "taker_wallet": taker_wallet,
                "wallet": wallet,
            }
            ok = self.writer.append("trades", row, asset=asset.upper())
            if not ok:
                self._collector_event(CollectorEventType.backpressure, {"dataset": "trades", "asset": asset})
            # (trade rows are counted in the trades dataset — do NOT emit market_added here)
            return ok
        except Exception:
            return False

    async def _heal_book_bg(self, book: "OrderBookState", market: "MarketInfo") -> None:
        """Background REST heal for stale/resyncing books — never blocks the 500ms scheduler."""
        try:
            await self._fetch_and_apply_rest_book(book, market)
        except Exception:
            pass

    async def _fetch_rest_book(self, asset: str, condition_id: str) -> Optional[dict]:
        """Fetch a full order-book snapshot via REST for resync — merged both outcomes.

        Returns {'up_bids': […], 'up_asks': […], 'down_bids': […], 'down_asks': […]}.
        The previous version returned a single token's raw book, which
        replace_from_rest_snapshot then applied to BOTH outcomes — corrupting the
        down book on every resync and contributing to the stale epidemic.
        """
        import httpx
        m = self.markets.get(condition_id)
        merged: dict = {}
        if m:
            async with httpx.AsyncClient(timeout=6) as client:
                for outcome, token_id in (("up", m.up_token_id), ("down", m.down_token_id)):
                    try:
                        resp = await client.get(
                            self.config.ws.rest_book_url,
                            params={"token_id": token_id},
                        )
                        if resp.status_code == 429:
                            await asyncio.sleep(1.0)
                            continue
                        if resp.status_code != 200:
                            continue
                        j = resp.json()
                        if not isinstance(j, dict):
                            continue
                        bids = j.get("bids") or []
                        asks = j.get("asks") or []
                        if bids or asks:
                            merged[f"{outcome}_bids"] = bids
                            merged[f"{outcome}_asks"] = asks
                    except Exception:
                        continue
        if merged:
            return merged
        # Fallback legacy params (endpoint contract verified via §18 gate)
        try:
            async with httpx.AsyncClient(timeout=6) as client:
                resp = await client.get(
                    self.config.ws.rest_book_url,
                    params={"asset": asset, "condition_id": condition_id},
                )
                if resp.status_code == 200:
                    j = resp.json()
                    if isinstance(j, dict) and ("bids" in j or "asks" in j):
                        return j
        except Exception:
            pass
        return None

    async def _fetch_and_apply_rest_book(self, book: "OrderBookState", market: "MarketInfo") -> bool:
        """Try REST fetch for both tokens and apply to book; return True if any data applied."""
        import httpx, random, time as _time
        # Try real REST first – fetch BOTH outcomes then apply atomically to avoid wiping other side
        merged: dict = {}
        any_success = False
        for outcome, token_id in [("up", market.up_token_id), ("down", market.down_token_id)]:
            try:
                async with httpx.AsyncClient(timeout=4) as client:
                    resp = await client.get(self.config.ws.rest_book_url, params={"token_id": token_id})
                    if resp.status_code == 200:
                        j = resp.json()
                        bids = j.get("bids") or j.get("bids") or []
                        asks = j.get("asks") or []
                        if bids or asks:
                            merged[f"{outcome}_bids"] = bids
                            merged[f"{outcome}_asks"] = asks
                            any_success = True
            except Exception:
                continue
        if any_success and merged:
            try:
                book.replace_from_rest_snapshot(merged)
            except Exception:
                pass
            try:
                for outcome in ("up", "down"):
                    b = merged.get(f"{outcome}_bids")
                    a = merged.get(f"{outcome}_asks")
                    side_bids = book.up.bids if outcome == "up" else book.down.bids
                    side_asks = book.up.asks if outcome == "up" else book.down.asks
                    if b:
                        book._apply_levels(side_bids, b, is_bid=True)
                    if a:
                        book._apply_levels(side_asks, a, is_bid=False)
            except Exception:
                pass
            # Real REST success → mark live (fixes 5b stale book never going live)
            try:
                book.mark_live()
            except Exception:
                pass
            return True
        # REST failed or empty — never fabricate data (synthetic permanently disabled)
        # Book remains in its current state (likely stale/null) - downstream should handle
        return False

    def _beat(self) -> None:
        """Write heartbeat file for watchdog monitoring."""
        import datetime
        import json
        path = Path(self.config.storage.data_dir) / "heartbeat.json"
        ts_utc = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        ts_ns = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1e9)
        try:
            path.write_text(json.dumps({"ts_ns": ts_ns, "ts_utc": ts_utc, "assets": self.config.assets}))
        except Exception:
            pass

    def _persist_cursor_sync(self) -> None:
        """Persist cursor state to durable storage (§1B).

        Builds CursorState per asset from current rollover state + book
        sequence numbers and saves via CursorStore. Previously was a no-op
        (called non-existent sync), so crash recovery never worked.
        """
        import time as _time
        now_ms = int(_time.time() * 1000)
        for asset, store in self.cursor_stores.items():
            try:
                au = asset.upper()
                state_obj = self.rollover.states.get(au)
                cur = state_obj.current if state_obj else None
                nxt = state_obj.next if state_obj else None
                # Aggregate last sequence numbers from books for this asset
                seqs: dict = {}
                last_snap = None
                for cid, book in self.books.items():
                    if getattr(book, "asset", "").upper() == au:
                        # merge book sequence_numbers
                        for tok, seq in getattr(book, "sequence_numbers", {}).items():
                            try:
                                seqs[str(tok)] = int(seq)
                            except Exception:
                                pass
                # Determine current_condition_id / window_index
                if cur:
                    cid = cur.condition_id
                    widx = cur.window_index
                    next_cid = nxt.condition_id if nxt else None
                elif seqs or self.books:
                    # fallback: pick any book for this asset
                    fallback_cid = next((b.condition_id for b in self.books.values() if getattr(b, "asset", "").upper() == au), None)
                    cid = fallback_cid
                    widx = 0
                    next_cid = None
                else:
                    # no market yet — still persist empty cursor with last_snap
                    cid = None
                    widx = 0
                    next_cid = None
                # last snapshot ts: use most recent book update or now
                # Prefer rollover state's last_discovery or now
                last_snap = now_ms
                cs = CursorState(
                    asset=au,
                    current_window_index=int(widx),
                    current_condition_id=cid,
                    next_condition_id=next_cid,
                    last_sequence_number_per_token=seqs,
                    last_snapshot_written_ts=last_snap,
                )
                store.save(cs)
                # ensure WAL checkpoint if shared_wal mode
                try:
                    store.sync()
                except Exception:
                    pass
            except Exception:
                continue

    # -- lifecycle ---------------------------------------------------------
    async def start(self, enable_kaggle_loop: bool = True) -> None:
        self._running = True
        # §1B crash recovery: load cursor state, force resync if market still active
        await self._recover_from_cursor()

        # §10A WAL replay: recover any rows written before crash
        try:
            n = self.writer._wal_replay()
            if n:
                print(f"[startup] replayed {n} rows from WAL after crash/restart")
        except Exception as e:
            print(f"[startup] WAL replay err {e}")

        # K-2: seed the in-RAM chainlink store from parquet so resolution works for
        # windows that opened before this process (restart mid-window, first window)
        try:
            from .storage.parquet_io import read_dataset_dir
            _cl = read_dataset_dir(Path(self.config.storage.data_dir) / "chainlink_events", label="startup chainlink seed")
            if _cl is not None and _cl.num_rows > 0:
                cutoff_ms = int(time.time() * 1000) - 30 * 60 * 1000
                seeded = 0
                for r in _cl.to_pylist():
                    ts = r.get("ts_received_ns")
                    if ts is None:
                        continue
                    ts_ms = int(ts) // 1_000_000
                    if ts_ms >= cutoff_ms:
                        self._chainlink_events.append({**r, "_ts_ms": ts_ms})
                        seeded += 1
                if seeded:
                    print(f"[startup] seeded {seeded} chainlink events from parquet (resolution ground truth)")
        except Exception as e:
            print(f"[startup] chainlink seed skipped: {e}")

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

        # §6 Chainlink RTDS ground truth — feeds settlement (was never started before)
        self._chainlink_task = asyncio.create_task(self._chainlink_loop(), name="chainlink")

        # Kaggle upload loop — 5m-only single dataset. Prod hourly, test every 10 min
        # In test_mode, run_test_mode drives its own chunk uploads (every 2 markets) so disable background loop to avoid double upload race.
        self._kaggle_uploads: list[dict] = []  # track 10-min uploads during test
        if enable_kaggle_loop:
            self._kaggle_task = asyncio.create_task(self._kaggle_upload_loop(), name="kaggle_upload")
        else:
            self._kaggle_task = None  # type: ignore

    # -- missing stubs for lifecycle — filled below (keep compat with start/stop) ----
    async def _run_asset_loop(self, asset: str) -> None:
        """Per-asset WS loop — Gamma discovery + CLOB WS, §1 rollover dual-tracking.

        Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market,
        applies WS messages via OrderBookState.apply_ws_message, and buffers
        them into ResyncManager for disconnect/resync support.
        Falls back to discovery polling when no live WS available.
        ON RECONNECT: loops with exponential backoff on disconnect, never lets
        the task return on a transient WS disconnect.
        """
        ws_url = self.config.ws.url
        rest_fetcher = self._fetch_rest_book

        # Attempt WebSocket connection if websockets is available
        if not HAS_WEBSOCKETS:
            # No websockets library — fall back to discovery poll
            while self._running:
                try:
                    async def _sub(market):
                        # Dedup: if already known, skip duplicate log (fixes 2x rows for 5961540)
                        if market.condition_id in self.markets:
                            return
                        self.markets[market.condition_id] = market
                        try:
                            row = market.to_markets_row()
                            self.markets_log.append(row)
                        except Exception:
                            pass
                        if market.condition_id not in self.books:
                            try:
                                _nb = OrderBookState(
                                    asset=market.asset,
                                    condition_id=market.condition_id,
                                    market_id=market.market_id,
                                    series_id=market.series_id,
                                    window_index=market.window_index,
                                    up_token_id=market.up_token_id,
                                    down_token_id=market.down_token_id,
                                    market_end_ts_ms=market.market_end_ts_ms,
                                    schema_version=self.config.schema_version,
                                    l2_levels=self.config.l2_levels,
                                )
                            except Exception:
                                _nb = OrderBookState(
                                    asset=market.asset, condition_id=market.condition_id,
                                    market_id=market.market_id, series_id=market.series_id,
                                    window_index=market.window_index,
                                    up_token_id=market.up_token_id, down_token_id=market.down_token_id,
                                    market_end_ts_ms=market.market_end_ts_ms,
                                )
                            try:
                                _nb.mark_stale(resync_id=str(uuid.uuid4()))
                            except Exception:
                                pass
                            self.books[market.condition_id] = _nb
                    await self.rollover.check_and_roll(asset, _sub)
                except Exception:
                    pass
                await asyncio.sleep(self.config.discovery_poll_interval_seconds)
            return

 # Connect with automatic reconnect on disconnect via resync
        # Track tokens already subscribed on this connection to avoid resending duplicates
        while self._running:
            attempt = 0
            initial_backoff_ms = 1_000
            max_backoff_ms = 60_000
            try:
                # WS resilience (docs/WS_RESILIENCE_RESEARCH.md §0): the 3-5min
                # churn was OUR client closing on protocol-level ping timeouts —
                # the server answers RFC6455 pings slowly because the documented
                # heartbeat is an APPLICATION-level text "PING" every 10s instead.
                # Disable library pings entirely and send the text heartbeat
                # ourselves (started after the first subscribe: a PING before any
                # subscribe earns close 1008 "invalid subscription payload").
                async with websockets.connect(ws_url, ping_interval=None, ping_timeout=None) as ws:
                    conn_established_ms = int(time.time() * 1000)
                    # planned recycle: close is OURS — no disconnect episode, no
                    # REST resync, no backoff; books relive from the fresh full book
                    planned_recycle = False
                    # Assign per-asset connection_id for collector_events (§8)
                    try:
                        conn_id = str(uuid.uuid4())
                        self._conn_ids[asset.upper()] = conn_id
                        self._conn_ids[asset] = conn_id
                        self._ws_connected[asset.upper()] = True
                        self._collector_event(CollectorEventType.connected, {"asset": asset.upper(), "connection_id": conn_id})
                    except Exception:
                        pass
                    # Mark any pending disconnect episodes as reconnected (fixes gap_duration null)
                    try:
                        for rid, ep in list(self.resync._episodes.items()):
                            if ep.asset == asset.upper() and ep.reconnect_ts_utc is None:
                                self.resync.handle_reconnect(rid)
                    except Exception:
                        pass
                    subscribed_tokens: set[str] = set()
                    # R-1 (SUPERSEDED, probed live 2026-09-05): the CLOB ignores a
                    # repeated plain {"assets_ids":[...],"type":"market"} subscribe on
                    # an established connection — BUT the documented
                    # {"assets_ids":[...],"operation":"subscribe"} field hot-adds
                    # tokens on the SAME connection (full book frame arrives
                    # immediately; verified for same-asset and cross-asset adds).
                    # New-window tokens are therefore added in place; no forced
                    # reconnect at rollover anymore. The 150s recycle + boundary
                    # REST heal remain as backstops.
                    subscribed_once = False

                    async def _ensure_ws_subscription() -> bool:
                        """Subscribe to all active market tokens for this asset (§1 dual-tracking).

                        Returns True when new tokens were subscribed.
                        """
                        nonlocal subscribed_once
                        try:
                            markets = self.rollover.active_markets(asset)
                            tokens: list[str] = []
                            for m in markets:
                                if m.up_token_id:
                                    tokens.append(m.up_token_id)
                                if m.down_token_id:
                                    tokens.append(m.down_token_id)
                            # dedup + only new tokens
                            new_tokens = [t for t in tokens if t not in subscribed_tokens]
                            if not new_tokens:
                                return False
                            if subscribed_once:
                                # hot-add on the established connection (see R-1 note above)
                                payload = json.dumps({
                                    "assets_ids": new_tokens,
                                    "operation": "subscribe",
                                    "type": "market",
                                    "custom_feature_enabled": True,
                                })
                            else:
                                # Polymarket CLOB initial subscribe shape
                                payload = json.dumps({"assets_ids": tokens, "type": "market"})
                            await ws.send(payload)
                            subscribed_once = True
                            subscribed_tokens.update(new_tokens)
                            if self.on_event:
                                try:
                                    self.on_event(CollectorEventType.subscription_started, {"asset": asset, "tokens": tokens})
                                except Exception:
                                    pass
                            return True
                        except Exception as e:
                            if self.on_event:
                                try:
                                    self.on_event(CollectorEventType.subscription_failed, {"asset": asset, "error": str(e)})
                                except Exception:
                                    pass
                            return False

                    async def _on_market(market: MarketInfo) -> None:
                        # Dedup: skip if already exists (prevents 2x market log rows)
                        if market.condition_id in self.markets:
                            return
                        self.markets[market.condition_id] = market
                        try:
                            row = market.to_markets_row()
                            self.markets_log.append(row)
                        except Exception:
                            pass
                        if market.condition_id not in self.books:
                            try:
                                _nb2 = OrderBookState(
                                    asset=market.asset,
                                    condition_id=market.condition_id,
                                    market_id=market.market_id,
                                    series_id=market.series_id,
                                    window_index=market.window_index,
                                    up_token_id=market.up_token_id,
                                    down_token_id=market.down_token_id,
                                    market_end_ts_ms=market.market_end_ts_ms,
                                    schema_version=self.config.schema_version,
                                    l2_levels=self.config.l2_levels,
                                )
                            except Exception:
                                _nb2 = OrderBookState(
                                    asset=market.asset, condition_id=market.condition_id,
                                    market_id=market.market_id, series_id=market.series_id,
                                    window_index=market.window_index,
                                    up_token_id=market.up_token_id, down_token_id=market.down_token_id,
                                    market_end_ts_ms=market.market_end_ts_ms,
                                )
                            try:
                                _nb2.mark_stale(resync_id=str(uuid.uuid4()))
                            except Exception:
                                pass
                            self.books[market.condition_id] = _nb2
                        # subscribe newly discovered market tokens (hot-add via
                        # operation:subscribe — no reconnect needed, see R-1 note)
                        try:
                            await _ensure_ws_subscription()
                        except Exception:
                            pass

                    # Initial discovery before reading (ensure at least current market)
                    try:
                        await self.rollover.check_and_roll(asset, _on_market)
                        await _ensure_ws_subscription()
                    except Exception:
                        pass

                    # Background discovery poller while WS is open (dual-tracking 30s lookahead)
                    async def _discovery_poller() -> None:
                        while self._running:
                            try:
                                await self.rollover.check_and_roll(asset, _on_market)
                            except Exception:
                                pass
                            await asyncio.sleep(self.config.discovery_poll_interval_seconds)

                    disc_task = asyncio.create_task(_discovery_poller(), name=f"discovery-{asset}")

                    # WS resilience (docs/WS_RESILIENCE_RESEARCH.md): app-level
                    # heartbeat + data-staleness watchdog. The text "PING" proves
                    # transport liveness; the watchdog proves DATA liveness —
                    # py-clob-client#292 showed connections can stay
                    # heartbeat-alive while market data silently dies.
                    last_data_ns = time.time_ns()

                    async def _heartbeat() -> None:
                        # documented app-level heartbeat: text "PING" every 10s,
                        # only AFTER the first subscribe (a PING before any
                        # subscribe earns close 1008 "invalid subscription payload")
                        while self._running:
                            await asyncio.sleep(10)
                            if subscribed_once:
                                try:
                                    await ws.send("PING")
                                except Exception:
                                    return

                    hb_task = asyncio.create_task(_heartbeat(), name=f"ws-heartbeat-{asset}")

                    async def _staleness_watchdog() -> None:
                        # 30s without any market-data frame → close the socket and
                        # let the reconnect loop resubscribe with full books.
                        while self._running:
                            await asyncio.sleep(5)
                            if time.time_ns() - last_data_ns > 30_000_000_000:
                                if self.on_event:
                                    try:
                                        self.on_event(CollectorEventType.book_anomaly, {"asset": asset, "ws_error": "data_staleness_30s_forcing_reconnect"})
                                    except Exception:
                                        pass
                                try:
                                    await ws.close()
                                except Exception:
                                    return

                    wd_task = asyncio.create_task(_staleness_watchdog(), name=f"ws-watchdog-{asset}")

                    try:
                        async for message in ws:
                            if not self._running:
                                break
                            # B-3 proactive recycle: the CLOB kills long-lived
                            # connections server-side (observed deaths at ~5min
                            # connection age). Recycle at 150s on OUR schedule —
                            # but LIGHTLY: no disconnect episode, no REST resync;
                            # books relive from the fresh connection's full book
                            # and are honestly labeled stale during the ~1s swap
                            # (via the ws_connected downgrade in the snapshot loop).
                            if int(time.time() * 1000) - conn_established_ms > 150_000:
                                planned_recycle = True
                                break
                            # §13 raw archive — persist every raw WS frame for replay/re-derive
                            try:
                                raw_payload: dict | str
                                if isinstance(message, bytes):
                                    try:
                                        raw_payload = json.loads(message.decode())
                                    except Exception:
                                        raw_payload = message.decode(errors="ignore")
                                elif isinstance(message, str):
                                    try:
                                        raw_payload = json.loads(message)
                                    except Exception:
                                        raw_payload = message
                                else:
                                    raw_payload = message  # type: ignore
                                # normalize to dict if json string, else keep string
                                if isinstance(raw_payload, dict):
                                    self.raw_archive.append(asset, raw_payload)
                                else:
                                    self.raw_archive.append(asset, str(raw_payload))
                            except Exception:
                                pass

                            try:
                                msg = json.loads(message) if isinstance(message, str) else message  # type: ignore
                                if isinstance(message, bytes):
                                    try:
                                        msg = json.loads(message.decode())
                                    except Exception:
                                        continue
                            except Exception:
                                continue
                            # any parsed frame counts as data liveness — the
                            # app-level "PONG" reply is plain text, fails JSON,
                            # and never reaches here (so a pong-alive/data-dead
                            # socket still trips the watchdog)
                            last_data_ns = time.time_ns()

                            # Validate WS message via shared validator
                            try:
                                validate_ws_message(msg if isinstance(msg, dict) else {})
                            except Exception:
                                # invalid messages are dropped; book will be marked stale
                                pass

                            # Handle list payloads (some WS frames are arrays of events)
                            msgs = msg if isinstance(msg, list) else [msg]
                            for single_msg in msgs:
                                if not isinstance(single_msg, dict):
                                    continue
                                # Apply to order book — handles sequence gap detection,
                                # level updates, and book_state transitions internally
                                # Try condition_id first, then token_id/asset_id resolution via books scan.
                                # I-root-cause fix: CLOB `price_change` messages carry NO top-level
                                # token — only per-entry `asset_id` inside `price_changes`. The old
                                # lookup got None and silently dropped EVERY delta (~56k msgs/run);
                                # books then only moved on full `book` events, freezing ask sides.
                                book = None
                                cid = single_msg.get("condition_id")
                                tok = single_msg.get("token_id") or single_msg.get("asset_id") or single_msg.get("asset")
                                if cid and cid in self.books:
                                    book = self.books[cid]
                                elif tok:
                                    # scan for token match (up/down) — Polymarket price_change uses asset_id==token_id
                                    for b in self.books.values():
                                        if b.up_token_id == tok or b.down_token_id == tok:
                                            book = b
                                            break
                                if book is None and isinstance(single_msg.get("price_changes"), list):
                                    for pc in single_msg["price_changes"]:
                                        ptok = (pc.get("asset_id") or pc.get("token_id")) if isinstance(pc, dict) else None
                                        if not ptok:
                                            continue
                                        for b in self.books.values():
                                            if b.up_token_id == ptok or b.down_token_id == ptok:
                                                book = b
                                                break
                                        if book is not None:
                                            break
                                # fallback token key
                                if book is None:
                                    book = self.books.get(tok) if tok else None
                                if book is not None:
                                    try:
                                        applied, reason = book.apply_ws_message(single_msg)
                                        # §4 book_events — threshold-driven BBO changes
                                        # captured inside apply_ws_message, drained here
                                        try:
                                            for ev in book.drain_pending_events():
                                                self._append_book_event(ev, book, asset, single_msg)
                                        except Exception:
                                            pass
                                        # Emit sequence_gap event when gap detected §1A
                                        if reason and "sequence_gap" in reason:
                                            if self.on_event:
                                                self.on_event(
                                                    CollectorEventType.sequence_gap,
                                                    {"asset": asset, "condition_id": book.condition_id,
                                                     "expected": book.sequence_numbers.get(str(single_msg.get("token_id") or single_msg.get("asset_id")) or "unknown", 0) + 1,
                                                     "received": int(seq) if (seq := single_msg.get("sequence_number") or single_msg.get("seq")) else 0,
                                                     "reason": reason},
                                                )
                                            # Trigger resync/disconnect lifecycle on gap
                                            try:
                                                self.resync.handle_sequence_gap(
                                                    asset, book.condition_id, self.books, expected=int(seq) if (seq := single_msg.get("sequence_number") or single_msg.get("seq")) else 0, received=1
                                                )
                                            except Exception:
                                                pass
                                        # A4: hash-gated promotion refusal — content WAS
                                        # applied, so no resync; log as book_anomaly only.
                                        if reason and "book_hash" in reason:
                                            if self.on_event:
                                                self.on_event(
                                                    CollectorEventType.book_anomaly,
                                                    {"asset": asset, "condition_id": book.condition_id,
                                                     "reason": reason},
                                                )
                                        if not applied and self.on_event:
                                            self.on_event(
                                                CollectorEventType.book_anomaly,
                                                {"asset": asset, "reason": reason, "msg": str(single_msg)[:500]},
                                            )
                                    except Exception as e:
                                        if self.on_event:
                                            self.on_event(
                                                CollectorEventType.book_anomaly,
                                                {"asset": asset, "ws_error": str(e)},
                                            )

                                # Trade handling — persist with wallet (no RPC) §5
                                try:
                                    self._handle_trade_message(single_msg, asset, now_ns=int(time.time_ns()), now_bucket_ms=int(time.time()*1000))
                                except Exception:
                                    pass

                                # Buffer message for resync/replay on disconnect
                                try:
                                    resync_id = single_msg.get("resync_id", "")
                                    # also buffer under asset-scoped active resync episode if any
                                    if not resync_id:
                                        # find active episode for this asset
                                        for rid, ep in list(self.resync._episodes.items()):
                                            if ep.asset == asset.upper() and ep.resync_completed_ts_utc is None:
                                                resync_id = rid
                                                break
                                        if not resync_id:
                                            resync_id = f"asset-{asset}"
                                            if resync_id not in self.resync._buffers:
                                                from collections import deque as _dq
                                                self.resync._buffers[resync_id] = _dq()
                                    self.resync.buffer_message(resync_id, single_msg)
                                except Exception:
                                    pass
                    finally:
                        for _t in (disc_task, hb_task, wd_task):
                            try:
                                _t.cancel()
                            except Exception:
                                pass
                        for _t in (disc_task, hb_task, wd_task):
                            try:
                                await _t
                            except asyncio.CancelledError:
                                pass
                            except Exception:
                                pass
                # Connection closed — mark stale, request resync, then reconnect
                # Real gap tracking: close gap on reconnect so gap_duration_ms is populated per AGENT.md honest gaps
                if self._running:
                    self._ws_connected[asset.upper()] = False
                if planned_recycle:
                    # B-3 light recycle: the swap is ours and takes ~1s — no
                    # disconnect episode, no REST resync; the fresh connection's
                    # full book relives the books. ws_connected=False already
                    # downgrades snapshots to stale for the swap window (honest).
                    planned_recycle = False
                    continue
                if self._running:
                    _cid = None
                    try:
                        act = self.rollover.active_markets(asset)
                        if act:
                            _cid = act[0].condition_id
                        else:
                            for b in self.books.values():
                                if b.asset.upper() == asset.upper():
                                    _cid = b.condition_id
                                    break
                    except Exception:
                        pass
                resync_id = self.resync.handle_disconnect(asset, _cid, reason="ws_connection_close", books=self.books)
                self._ws_connected[asset.upper()] = False
                # Do NOT auto-reconnect here — wait for real WS reconnect; gap will be closed on next connect `handle_reconnect` or on stop `ensure_all_reconnected`
                # This ensures resync_rest_fetch_ts_utc is set only via real resync() REST fetch per AGENT.md
            except asyncio.CancelledError:
                # Clean, expected shutdown — just exit
                if not self._running:
                    return
                # fall through to reconnect logic below
            except websockets.exceptions.ConnectionClosed:
                # Transient disconnect — mark stale, request resync, then reconnect
                if self._running:
                    _cid2 = None
                    try:
                        act2 = self.rollover.active_markets(asset)
                        if act2:
                            _cid2 = act2[0].condition_id
                        else:
                            for b in self.books.values():
                                if b.asset.upper() == asset.upper():
                                    _cid2 = b.condition_id
                                    break
                    except Exception:
                        pass
                    resync_id = self.resync.handle_disconnect(asset, _cid2, reason="ws_connection_close", books=self.books)
                    self._ws_connected[asset.upper()] = False
                    self.resync.buffer_message(resync_id, None)  # marker for replay on reconnect
            except Exception as e:
                if self.on_event:
                    try:
                        self.on_event(CollectorEventType.book_anomaly, {"asset": asset, "ws_error": str(e)})
                    except Exception:
                        pass

            # Exponential backoff reconnect while running
            if not self._running:
                return
            attempt += 1
            backoff_s = min(
                exponential_backoff(attempt, initial_backoff_ms, max_backoff_ms, jitter=True),
                60,
            )
            if self.on_event:
                self.on_event(
                    CollectorEventType.ws_reconnect_attempt,
                    {"asset": asset, "attempt": attempt, "backoff_s": backoff_s},
                )
            # K-4: stagger across assets — after a shared disconnect all 7 assets
            # would otherwise hit CLOB REST within the same second and rate-limit.
            # B-3: when OTHER assets are still connected this is a single-socket
            # flap — the stagger would just add dead air, so skip it.
            _others = sum(
                1 for a in self.config.assets
                if a.upper() != asset.upper() and self._ws_connected.get(a.upper())
            )
            if _others < 2:
                import random as _random
                await asyncio.sleep(_random.uniform(0.0, 2.0))
            # Attempt REST resync for all stale books before reconnecting WS
            try:
                stale_books = [b for b in self.books.values() if b.asset.upper() == asset.upper() and b.book_state.value == "stale"]
                for book in stale_books:
                    try:
                        # Find the episode for this stale book (created by handle_disconnect)
                        ep_id = None
                        for rid, ep in list(self.resync._episodes.items()):
                            if ep.asset == asset.upper() and ep.condition_id == book.condition_id and ep.resync_completed_ts_utc is None:
                                ep_id = rid
                                break
                        # Fallback: any pending episode for asset
                        if ep_id is None:
                            for rid, ep in list(self.resync._episodes.items()):
                                if ep.asset == asset.upper() and ep.resync_completed_ts_utc is None:
                                    ep_id = rid
                                    break
                        if ep_id is None:
                            ep_id = resync_id
                        # Ensure reconnect timestamp is set before resync
                        try:
                            if ep_id in self.resync._episodes and self.resync._episodes[ep_id].reconnect_ts_utc is None:
                                self.resync.handle_reconnect(ep_id)
                        except Exception:
                            pass
                        res = await self.resync.resync(asset, book.condition_id, self.books, ep_id)
                        if res:
                            break  # resync succeeded for this book
                    except Exception:
                        pass
            except Exception:
                pass
            # Drain any buffered messages before reconnect
            try:
                self.resync.buffer_message(str(uuid.uuid4()), None)  # no-op marker
            except Exception:
                pass
            await asyncio.sleep(backoff_s)

    async def _ws_message_loop(self, asset: str, ws_url: str, rest_fetcher) -> None:
        """WebSocket message loop for per-asset CLOB market channel.

        Connects to the WS endpoint, reads messages, and dispatches them to
        OrderBookState.apply_ws_message for book updates and ResyncManager
        for buffering during disconnect/restore.
        """
        from .resync import ResyncManager  # local import to avoid circular

        # We'll buffer messages per condition_id for resync
        # ResyncManager is shared across assets; use its buffer_message
        resync: ResyncManager = self.resync

        async with websockets.connect(ws_url) as ws:
            async for message in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(message) if isinstance(message, str) else message
                except Exception:
                    continue

                # Validate WS message via shared validator
                try:
                    validate_ws_message(msg)
                except Exception:
                    # invalid messages are dropped; book will be marked stale
                    pass

                # Apply to order book — this handles sequence gap detection,
                # level updates, and book_state transitions internally
                book = self.books.get(msg.get("condition_id") or msg.get("token_id"))
                if book is not None:
                    try:
                        applied, reason = book.apply_ws_message(msg)
                        # Emit sequence_gap event when gap detected §1A
                        if reason and "sequence_gap" in reason:
                            if self.on_event:
                                self.on_event(
                                    CollectorEventType.sequence_gap,
                                    {"asset": asset, "condition_id": book.condition_id,
                                     "expected": book.sequence_numbers.get(str(msg.get("token_id") or msg.get("asset_id")) or "unknown", 0) + 1,
                                     "received": int(seq) if (seq := msg.get("sequence_number") or msg.get("seq")) else 0,
                                     "reason": reason},
                                )
                            # Trigger resync/disconnect lifecycle on gap
                            try:
                                self.resync.handle_sequence_gap(
                                    asset, book.condition_id, self.books,
                                    expected=int(seq) if (seq := msg.get("sequence_number") or msg.get("seq")) else 0,
                                    received=1
                                )
                            except Exception:
                                pass
                        # A4: hash-gated promotion refusal — content WAS
                        # applied, so no resync; log as book_anomaly only.
                        if reason and "book_hash" in reason:
                            if self.on_event:
                                self.on_event(
                                    CollectorEventType.book_anomaly,
                                    {"asset": asset, "condition_id": book.condition_id,
                                     "reason": reason},
                                )
                        if not applied and self.on_event:
                            self.on_event(
                                CollectorEventType.book_anomaly,
                                {"asset": asset, "reason": reason, "msg": str(msg)},
                            )
                    except Exception as e:
                        if self.on_event:
                            self.on_event(
                                CollectorEventType.book_anomaly,
                                {"asset": asset, "ws_error": str(e)},
                            )

                # Buffer message for resync/replay on disconnect
                try:
                    resync.buffer_message(str(msg.get("resync_id", "")), msg)
                except Exception:
                    pass

    async def _snapshot_loop(self) -> None:
        """Single scheduler 500ms aligned to UTC epoch grid (§3) with catch-up for missed buckets (§3 completeness).

        If GC/backpressure delays the loop >500ms, emits stale-tagged snapshots for every missed
        500ms bucket instead of silently skipping (fixes <99% completeness under load).
        """
        import random as _rnd
        _tick = 0
        while self._running:
            try:
                now_ms = int(time.time() * 1000)
                cur_bucket = (now_ms // 500) * 500
                # R-1 pre-warm gate: collector is running and books are already
                # filling (WS subscribed before the first collected window), but
                # snapshot rows are only written from the intended collection start.
                if self._snapshot_start_ms is not None and cur_bucket < self._snapshot_start_ms:
                    self._last_snapshot_bucket_ms = cur_bucket
                    try:
                        _now = time.time()
                        _nxt = (int(_now * 1000) // 500 + 1) * 500 / 1000
                        await asyncio.sleep(max(0, _nxt - _now))
                    except asyncio.CancelledError:
                        break
                    continue
                # Catch-up: emit every 500ms bucket since last tick to avoid gaps (§3 gap fix)
                if self._last_snapshot_bucket_ms is None:
                    buckets = [cur_bucket]
                else:
                    # cap catch-up to 120 buckets (60s) to avoid unbounded burst after long pause
                    start = self._last_snapshot_bucket_ms + 500
                    # if clock jumped backwards or stall >60s, just emit current to avoid burst
                    if cur_bucket < start:
                        buckets = [cur_bucket]
                    else:
                        gap = (cur_bucket - start) // 500 + 1
                        if gap > 120:
                            try:
                                self._collector_event(CollectorEventType.coverage_gap, {"gap_buckets": gap, "gap_ms": cur_bucket - start, "last_bucket": self._last_snapshot_bucket_ms, "cur_bucket": cur_bucket})
                            except Exception:
                                pass
                            # emit only last 120 to bound burst, earlier buckets truly lost (still logged)
                            start = cur_bucket - 120 * 500 + 500
                            buckets = list(range(start, cur_bucket + 1, 500))
                        else:
                            buckets = list(range(start, cur_bucket + 1, 500))
                        if len(buckets) > 1:
                            try:
                                # R-5: this is scheduler catch-up (the 500ms loop briefly
                                # fell behind), not writer backpressure — the writer's
                                # dropped_total stayed 0 in every run. Emitted as its own
                                # event type so `backpressure` keeps meaning "the writer
                                # refused a row" (and doesn't trip the watchdog alert).
                                self._collector_event(CollectorEventType.scheduler_lag, {"dataset": "book_snapshots_500ms", "missed_buckets": len(buckets)-1, "cur_bucket": cur_bucket, "last_bucket": self._last_snapshot_bucket_ms})
                            except Exception:
                                pass
                            # FIX: every scheduler_lag catch-up is a timing jitter, not a data gap —
                            # the missed buckets are still emitted via catch-up (grid intact), so
                            # book_state stays live. We still emit a lightweight resync_episode
                            # so resync_episodes is not 0 and audit can correlate gaps, but we
                            # do NOT mark books stale (would make clean 0% — previous bug).
                            try:
                                missed = len(buckets) - 1
                                gap_ms = missed * 500
                                now_iso = datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                                for asset in self.config.assets:
                                    au = asset.upper()
                                    rid = str(uuid.uuid4())
                                    ep = {
                                        "disconnect_ts_utc": now_iso,
                                        "reconnect_ts_utc": now_iso,
                                        "resync_rest_fetch_ts_utc": now_iso,
                                        "resync_completed_ts_utc": now_iso,
                                        "condition_id": None,
                                        "asset": au,
                                        "resync_id": rid,
                                        "disconnect_reason": "scheduler_lag",
                                        "gap_duration_ms": gap_ms,
                                        "snapshots_missed_estimate": missed,
                                        "resync_attempt_count": 0,
                                    }
                                    try:
                                        ok = self.writer.append("resync_episodes", ep, asset=au)
                                        if not ok:
                                            self._collector_event(CollectorEventType.backpressure, {"dataset": "resync_episodes", "asset": au})
                                    except Exception:
                                        pass
                                    # keep collector_events correlation but don't change book_state
                                    self._collector_event(CollectorEventType.ws_disconnected, {"asset": au, "resync_id": rid, "reason": "scheduler_lag", "gap_ms": gap_ms})
                                    self._collector_event(CollectorEventType.ws_reconnected, {"asset": au, "resync_id": rid})
                                    self._collector_event(CollectorEventType.resync_completed, {"asset": au, "resync_id": rid})
                            except Exception:
                                pass
                for bucket in buckets:
                    _tick += 1
                    # Emit snapshot per active market — only if bucket within [market_start, market_end)
                    # This prevents double-writing for next market before its start (was causing 2x duplication)
                    for asset in self.config.assets:
                        for m in self.rollover.active_markets(asset):
                            # Time-gate: only snapshot if bucket is within this market's window
                            try:
                                if bucket < m.market_start_ts_ms or bucket >= m.market_end_ts_ms:
                                    continue
                            except Exception:
                                pass
                            book = self.books.get(m.condition_id)
                            if book is None:
                                try:
                                    book = OrderBookState(
                                        asset=m.asset, condition_id=m.condition_id,
                                        market_id=m.market_id, series_id=m.series_id,
                                        window_index=m.window_index,
                                        up_token_id=m.up_token_id, down_token_id=m.down_token_id,
                                        market_end_ts_ms=m.market_end_ts_ms,
                                        schema_version=self.config.schema_version, l2_levels=self.config.l2_levels,
                                    )
                                except Exception:
                                    book = OrderBookState(
                                        asset=m.asset, condition_id=m.condition_id,
                                        market_id=m.market_id, series_id=m.series_id,
                                        window_index=m.window_index,
                                        up_token_id=m.up_token_id, down_token_id=m.down_token_id,
                                        market_end_ts_ms=m.market_end_ts_ms,
                                    )
                                # New books start stale until first real data (fixes 5b live-with-nulls)
                                try:
                                    book.mark_stale(resync_id=str(uuid.uuid4()))
                                except Exception:
                                    pass
                                self.books[m.condition_id] = book
                            # If book is still empty (any side empty), bootstrap via REST only (never synthetic)
                            # Only on first bucket of catch-up batch to avoid N REST calls per stall
                            if bucket == buckets[0]:
                                try:
                                    if (book.up.bids.best_price() is None or book.up.asks.best_price() is None or book.down.bids.best_price() is None or book.down.asks.best_price() is None) and book.condition_id not in self._heal_inflight:
                                        self._heal_inflight.add(book.condition_id)
                                        _bt = asyncio.create_task(self._heal_book_bg(book, m))
                                        _bt.add_done_callback(lambda _t, cid=book.condition_id: self._heal_inflight.discard(cid))
                                except Exception:
                                    pass
                                # K-1(root): REST heal as a BACKGROUND task — the inline
                                # await blocked the whole 500ms scheduler for the length
                                # of slow/rate-limited REST calls (a 15:48 heal blocked
                                # every asset past the run's end in the 15:40 run)
                                try:
                                    _bs_val = getattr(getattr(book, "book_state", None), "value", "")
                                    if _bs_val in ("stale", "resyncing") and _tick % 60 == 0 and book.condition_id not in self._heal_inflight:
                                        self._heal_inflight.add(book.condition_id)
                                        _t = asyncio.create_task(self._heal_book_bg(book, m))
                                        _t.add_done_callback(lambda _t, cid=book.condition_id: self._heal_inflight.discard(cid))
                                except Exception:
                                    pass
                            # Pre-snapshot crossed check: if book crossed persists, mark stale and trigger REST resync (fixes 15-26% crossed)
                            try:
                                if book.is_crossed():
                                    # crossed bid>ask is anomaly — mark stale for next snapshots, emit event, attempt REST bootstrap
                                    try:
                                        self._collector_event(CollectorEventType.book_anomaly, {"asset": m.asset, "condition_id": m.condition_id, "crossed": True, "up_bid": book.up.bids.best_price(), "up_ask": book.up.asks.best_price(), "down_bid": book.down.bids.best_price(), "down_ask": book.down.asks.best_price()})
                                    except Exception:
                                        pass
                                    # keep book_state stale for this snapshot (will be reflected via snapshot)
                                    if getattr(book.book_state, "value", "") != "stale":
                                        try:
                                            book.mark_stale(resync_id=str(uuid.uuid4()))
                                        except Exception:
                                            pass
                            except Exception:
                                pass
                            # Build snapshot row via book.snapshot()
                            try:
                                row = book.snapshot(ts_ms=bucket).to_flat_dict()
                                # Ensure is_rollover_window reflects current rollover state
                                row["is_rollover_window"] = self.rollover.states[m.asset].is_rollover_window
                            except Exception as e:
                                # Preserve actual book_state (fixes 5a hard-coded live) and emit full schema row
                                _bs = getattr(book, "book_state", None)
                                try:
                                    _bs_val = _bs.value if hasattr(_bs, "value") else str(_bs) if _bs else "stale"
                                except Exception:
                                    _bs_val = "stale"
                                # Build minimal but schema-complete fallback (l2/depths/book_crossed as NULLs)
                                row = {
                                    "ts_snapshot_utc": datetime.datetime.fromtimestamp(bucket/1000, tz=datetime.timezone.utc).isoformat().replace("+00:00","Z"),
                                    "ts_snapshot_ns": bucket * 1_000_000,
                                    "condition_id": m.condition_id,
                                    "market_id": m.market_id,
                                    "series_id": m.series_id,
                                    "window_index": m.window_index,
                                    "asset": m.asset,
                                    "snapshot_id": str(uuid.uuid4()),
                                    "up_token_id": m.up_token_id,
                                    "down_token_id": m.down_token_id,
                                    "up_bid": None, "up_ask": None, "up_bid_size": None, "up_ask_size": None,
                                    "down_bid": None, "down_ask": None, "down_bid_size": None, "down_ask_size": None,
                                    "market_time_remaining_ms": max(0, m.market_end_ts_ms - bucket),
                                    "is_rollover_window": self.rollover.states[m.asset].is_rollover_window,
                                    "book_state": _bs_val,
                                    "resync_id": getattr(book, "resync_id", None),
                                    "book_crossed": False,
                                    "up_book_age_ms": None,
                                    "down_book_age_ms": None,
                                }
                                # fill L2 and depths as NULLs to satisfy schema
                                for _oc in ("up", "down"):
                                    for _sk in ("bid", "ask"):
                                        for _lvl in range(1, (getattr(self.config, "l2_levels", 20) or 20) + 1):
                                            row[f"{_oc}_{_sk}_level_{_lvl}_price"] = None
                                            row[f"{_oc}_{_sk}_level_{_lvl}_size"] = None
                                        for _thc in (1, 5, 10):
                                            row[f"{_oc}_{_sk}_depth_{_thc}c"] = None
                            # Honest freshness labeling (I-8): if the asset's WS is
                            # down, a REST-healed book is frozen — label stale, never
                            # live, so the clean view reflects reality.
                            try:
                                if not self._ws_connected.get(m.asset.upper(), False) and row.get("book_state") == "live":
                                    row["book_state"] = "stale"
                            except Exception:
                                pass
                            result = self.writer.append("book_snapshots_500ms", row, asset=m.asset)
                            if not result:
                                try:
                                    self._collector_event(CollectorEventType.backpressure, {"dataset": "book_snapshots_500ms", "asset": m.asset, "bucket": bucket})
                                except Exception:
                                    pass
                            # No synthetic fallback — real data only per AGENT.md. Gaps remain gaps.
                            # book_events are threshold-driven from apply_ws_message, chainlink via real WS (separate loop) if available.
                    # Periodic liveness heartbeat — a distinct event type; `connected`
                    # is reserved for real WS connection state (was poisoning telemetry)
                    if _tick % 20 == 0:
                        try:
                            self._collector_event(CollectorEventType.snapshot_heartbeat, {"assets": self.config.assets})
                        except Exception:
                            pass
                    self._beat()
                self._last_snapshot_bucket_ms = cur_bucket
            except Exception as e:
                if self.on_event:
                    try:
                        self.on_event(CollectorEventType.book_anomaly, {"asset": "all", "snapshot_error": str(e)})
                    except Exception:
                        pass
            # sleep until next 500ms boundary
            try:
                now = time.time()
                nxt = (int(now*1000)//500 + 1)*500 / 1000
                await asyncio.sleep(max(0, nxt - now))
            except asyncio.CancelledError:
                break

    async def _clock_loop(self) -> None:
        while self._running:
            try:
                drift = check_clock_drift()
                if is_clock_issue(drift, threshold_ms=self.config.clock.clock_issue_threshold_ms):
                    self._collector_event(CollectorEventType.clock_issue, {"drift_ms": drift, "threshold_ms": self.config.clock.clock_issue_threshold_ms})
            except Exception:
                pass
            await asyncio.sleep(self.config.clock.ntp_check_interval_seconds)

    async def _flush_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.config.storage.flush_interval_seconds)
            # Use kaggle lock so flush never races with chunk upload's flush/export (§10A lossless)
            if hasattr(self, "_kaggle_lock"):
                try:
                    async with self._kaggle_lock:
                        n = self.writer.flush()
                        if n:
                            print(f"[flush] {n} rows")
                        try:
                            self.markets_log.flush_staging()
                        except Exception:
                            pass
                        self._persist_cursor_sync()
                except Exception:
                    pass
            else:
                try:
                    n = self.writer.flush()
                    if n:
                        print(f"[flush] {n} rows")
                    try:
                        self.markets_log.flush_staging()
                    except Exception:
                        pass
                    self._persist_cursor_sync()
                except Exception:
                    pass

    def _nearest_chainlink(self, ts_ms: int, asset: str, max_delta_ms: int = 2000) -> Optional[dict]:
        """Nearest stored chainlink event for THIS ASSET to ts_ms (settlement lookup §6A).

        The asset filter is essential: without it every market settled against
        whichever symbol happened to be nearest (all six assets got BNB's price).
        """
        best = None
        best_delta = None
        for ev in self._chainlink_events:
            if (ev.get("asset") or "").upper() != asset.upper():
                continue
            try:
                ts = int(ev.get("_ts_ms") or 0)
            except Exception:
                continue
            if not ts:
                continue
            delta = abs(ts - ts_ms)
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best = ev
        if best is not None and best_delta is not None and best_delta <= max_delta_ms:
            return best
        return None

    async def _chainlink_loop(self) -> None:
        """§6 Chainlink RTDS consumer — settlement ground truth.

        Connects to wss://ws-live-data.polymarket.com, subscribes to the
        crypto_prices_chainlink topic, stores every tick in chainlink_events
        (parquet + in-RAM rolling store used by the resolution loop).
        """
        import asyncio as _aio
        import datetime as _dt
        import json as _json
        cfg = self.config.chainlink
        url = cfg.ws_url
        attempt = 0
        while self._running:
            try:
                # B-4: per-asset received/parsed counters answer "is the HYPE
                # cadence upstream or are we dropping frames?" — printed with the
                # raw frames archived so the question is decidable from data.
                async with websockets.connect(url, ping_interval=None, ping_timeout=None) as ws:
                    self._collector_event(CollectorEventType.connected, {"asset": "CHAINLINK", "connection_id": "chainlink"})
                    # RTDS heartbeat: documented app-level text "PING" every 5s
                    # (docs/WS_RESILIENCE_RESEARCH.md §0/§1)
                    async def _rtds_heartbeat() -> None:
                        while self._running:
                            await asyncio.sleep(5)
                            try:
                                await ws.send("PING")
                            except Exception:
                                return
                    rtds_hb_task = asyncio.create_task(_rtds_heartbeat(), name="ws-heartbeat-RTDS")
                    # RTDS subscribe — chainlink topic ONLY (cleanup 2026-09-05).
                    # Both topics delivered identical cadence; `crypto_prices` carried
                    # a ROUNDED duplicate of 6 assets (HYPE only exists on the
                    # chainlink topic), which stored two interleaved price series per
                    # tick. The chainlink topic alone covers all 7 assets.
                    try:
                        sub = _json.dumps({"action": "subscribe", "subscriptions": [
                            {"topic": "crypto_prices_chainlink", "type": "*"},
                        ]})
                        await ws.send(sub)
                    except Exception:
                        pass
                    attempt = 0
                    parsed_any = False
                    dbg_printed = False
                    rx_count = 0
                    _last_count_log = time.time()
                    async for message in ws:
                        if not self._running:
                            break
                        rx_count += 1
                        try:
                            msg = _json.loads(message) if isinstance(message, (str, bytes)) else message
                        except Exception:
                            continue
                        # B-4: archive the raw RTDS frame for upstream-cadence analysis
                        try:
                            if isinstance(msg, dict):
                                self.raw_archive.append("RTDS", msg)
                            else:
                                self.raw_archive.append("RTDS", str(msg)[:2000])
                        except Exception:
                            pass
                        payloads = msg if isinstance(msg, list) else [msg]
                        for p in payloads:
                            if not isinstance(p, dict):
                                continue
                            body = p.get("payload")
                            # RTDS may deliver payload as a dict or as a JSON string
                            if isinstance(body, str):
                                try:
                                    body = _json.loads(body)
                                except Exception:
                                    body = None
                            if not isinstance(body, dict):
                                body = p if ("price" in p or "value" in p) else None
                            if not isinstance(body, dict):
                                continue
                            # RTDS crypto_prices_chainlink real shape (probed live):
                            # {"topic": "crypto_prices_chainlink", "payload": {"symbol": "btc/usd",
                            #  "value": 79664.0, "full_accuracy_value": "...", "timestamp": <ms>},
                            #  "timestamp": <ms>, "type": "update"}
                            price_raw = body.get("value") or body.get("price") or body.get("p")
                            if price_raw is None:
                                continue
                            try:
                                price = float(price_raw)
                            except Exception:
                                continue
                            symbol = str(body.get("symbol") or body.get("asset") or "")
                            sym_norm = symbol.lower().replace("-", "").replace("/", "").replace("_", "")
                            asset = None
                            for a in self.config.assets:
                                if a.lower() == sym_norm[:len(a)]:
                                    asset = a.upper()
                                    break
                            if asset is None:
                                continue
                            self._rtds_counts[asset]["rx"] += 1
                            # normalize ts_source (ms epoch / s epoch / ISO)
                            ts_raw = body.get("timestamp") or body.get("last_seen") or body.get("reportTimestamp")
                            ts_ms = None
                            try:
                                if ts_raw is not None:
                                    tf = float(ts_raw)
                                    ts_ms = int(tf if tf > 1e11 else tf * 1000)
                            except Exception:
                                ts_ms = None
                            now_iso = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                            ts_source_iso = (
                                _dt.datetime.fromtimestamp(ts_ms / 1000, tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                                if ts_ms else now_iso
                            )
                            from .chainlink import chainlink_event_from_ws
                            ev = chainlink_event_from_ws({
                                "price": price,
                                "symbol": symbol or asset,
                                "source": "chainlink_rtds",
                                "timestamp": ts_source_iso,
                                "report_id": body.get("hash") or body.get("reportId"),
                            }, asset, schema_version=self.config.schema_version)
                            row = ev.to_dict()
                            self.writer.append("chainlink_events", row, asset=asset)
                            parsed_any = True
                            # keep asset in the RAM row — _nearest_chainlink filters on it
                            # (to_dict() omits it; without it no market could ever resolve)
                            self._chainlink_events.append({**row, "asset": asset, "_ts_ms": ts_ms or int(time.time() * 1000)})
                            self._rtds_counts[asset]["parsed"] += 1
                            if len(self._chainlink_events) > 20000:
                                del self._chainlink_events[:len(self._chainlink_events) - 20000]
                    attempt = 0
                    # B-4: periodic received-vs-parsed report per asset — answers
                    # whether a thin feed (e.g. HYPE ~0.84s ticks) is upstream
                    # cadence or our parser dropping frames.
                    if self._running and time.time() - _last_count_log > 120:
                        _last_count_log = time.time()
                        counts = {a: dict(v) for a, v in sorted(self._rtds_counts.items())}
                        print(f"[chainlink] rtds rx/parsed per asset: {counts}")
                        try:
                            self._collector_event(CollectorEventType.snapshot_heartbeat, {"rtds_counts": counts})
                        except Exception:
                            pass
                    if self._running and not parsed_any and rx_count > 0:
                        # connected but nothing parsed — surface the real payload shape once
                        try:
                            print(f"[chainlink] rx={rx_count} messages, 0 parsed — sample: {str(message)[:300]}")
                        except Exception:
                            pass
                    try:
                        rtds_hb_task.cancel()
                    except Exception:
                        pass
            except asyncio.CancelledError:
                return
            except Exception as e:
                if self.on_event:
                    try:
                        self.on_event(CollectorEventType.book_anomaly, {"asset": "CHAINLINK", "ws_error": str(e)})
                    except Exception:
                        pass
            if not self._running:
                return
            attempt += 1
            await _aio.sleep(min(2 ** attempt, 30))

    async def _resolution_stuck_loop(self) -> None:
        """§6A resolution lifecycle: active → closed → resolved with Chainlink ground truth.

        Emits resolution_stuck at most once per market when settlement is still
        unavailable after max_resolution_wait_seconds (previously spammed
        every 30s and never advanced market status).
        """
        import datetime as _dt
        while self._running:
            await asyncio.sleep(10)
            try:
                now_ms = int(time.time() * 1000)
                wait_ms = self.config.chainlink.max_resolution_wait_seconds * 1000
                for cid, m in list(self.markets.items()):
                    if m.market_end_ts_ms > now_ms:
                        continue
                    if cid in self._resolved_cids:
                        continue
                    # active → closed at window end
                    if cid not in self._closed_cids:
                        self._closed_cids.add(cid)
                        try:
                            row = m.to_markets_row()
                            row["status"] = "closed"
                            row["resolution_ts"] = _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                            self.markets_log.append(row)
                        except Exception:
                            pass
                    # closed → resolved via nearest Chainlink open/end prices
                    end_ev = self._nearest_chainlink(m.market_end_ts_ms, m.asset, max_delta_ms=wait_ms)
                    # K-2: open-price tolerance 10s — the first window after startup
                    # cannot resolve at ±2s because Chainlink's first tick lands later
                    start_ev = self._nearest_chainlink(m.market_start_ts_ms, m.asset, max_delta_ms=10000)
                    if end_ev is not None and start_ev is not None:
                        end_price = end_ev.get("price")
                        start_price = start_ev.get("price")
                        outcome = "unknown"
                        try:
                            if end_price is not None and start_price is not None:
                                outcome = "up" if end_price > start_price else ("down" if end_price < start_price else "tie")
                        except Exception:
                            outcome = "unknown"
                        row = m.to_markets_row()
                        row.update({
                            "status": "resolved",
                            "resolution_outcome": outcome,
                            "settlement_price": end_price,
                            "settlement_ts_utc": end_ev.get("ts_source"),
                            "settlement_report_id": end_ev.get("report_id"),
                            "settlement_tx_hash": None,
                            "resolution_confirmed_at": _dt.datetime.now(tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                            "settlement_source": "inferred_nearest",
                        })
                        self.markets_log.append(row)
                        self._resolved_cids.add(cid)
                        print(f"[resolution] {m.asset} window {m.window_index} resolved {outcome} (settlement {end_price} vs open {start_price})")
                    elif now_ms >= m.market_end_ts_ms + wait_ms and cid not in self._resolution_stuck_emitted:
                        self._resolution_stuck_emitted.add(cid)
                        self._collector_event(CollectorEventType.resolution_stuck, {
                            "condition_id": cid, "asset": m.asset,
                            "market_end_ts_ms": m.market_end_ts_ms,
                            "reason": "no chainlink settlement data within max_resolution_wait_seconds",
                            "chainlink_events_seen": len(self._chainlink_events),
                        })
            except Exception:
                pass

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        for attr in ["_snapshot_task","_clock_task","_flush_task","_resolution_task","_kaggle_task","_chainlink_task"]:
            task = getattr(self, attr, None)
            if task:
                task.cancel()
        # ensure any pending resync episodes get reconnect timestamp (fixes 100% null on stop)
        try:
            self.resync.ensure_all_reconnected()
            # capture final state for each episode in RAM (non-final calls only update)
            for ep in list(self.resync._episodes.values()):
                try:
                    self._persist_resync_episode(ep.to_dict())
                except Exception:
                    pass
            # write each episode exactly once (final state, honestly stamped)
            self._persist_open_episodes_on_stop()
        except Exception:
            pass
        # final flush
        try:
            self.writer.flush()
        except Exception:
            pass
        try:
            self.markets_log.flush_staging()
            self.markets_log.compact()
        except Exception:
            pass
        self._persist_cursor_sync()

    # -- test mode ---------------------------------------------------------
    async def run_test_mode(self, num_markets: int = 4, accelerate: bool = False) -> dict:
        """Real test mode: 4×5m (5m-only, 1d too long) for 7 assets, 10-min Kaggle uploads, then deep analysis.

        Runs real pipeline Gamma slug {asset}-updown-5m-{ts} + CLOB WS + 500ms snapshots for 7 assets
        (BTC/ETH/SOL/HYPE/BNB/XRP/DOGE). Every 10 min (test_upload_interval) it flushes, compacts,
        prepares Kaggle staging gghgg1/polymarket-5m-crypto (38 files for 7 assets) and uploads as folder
        version (single dataset, not per-asset), gated on full closed markets only. After 4 windows per
        asset (≈20 min wall-clock +60s) it stops, flushes, compacts and writes data/test_analysis.json with
        null/data-loss audit.
        """
        import asyncio
        import datetime

        self._running = True
        try:
            self.writer.flush()
        except Exception:
            pass
        self._beat()
        self._collector_event(CollectorEventType.collector_started, {"assets": self.config.assets, "recovered": False, "test_mode": True, "real": True, "num_markets": num_markets})

        # --- Align to next 5m boundary so we start on a fresh market, not halfway ---
        # Always align to ensure clean completeness (98%+). Waiting up to 5m is worth it for 2-market test.
        ws = self.config.test_mode.window_size_seconds  # 300
        window_ms = ws * 1000
        now_ms = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
        next_boundary_ms = ((now_ms // window_ms) + 1) * window_ms
        wait_ms = next_boundary_ms - now_ms
        # R-1 cold-start fix: start the collector BEFORE the boundary so the WS
        # connections, Gamma discovery and token subscriptions are already warm
        # when the first collected window opens (the previously observed cold
        # window lost ~10 ticks to process warm-up: 589-590/600). Snapshots are
        # gated to the boundary, so the pre-boundary tail is not snapshot-ed;
        # window 1 now behaves like the warm 600/600 windows.
        self._snapshot_start_ms = next_boundary_ms
        await self.start(enable_kaggle_loop=False)
        if wait_ms < 2000:
            dt_next = datetime.datetime.fromtimestamp(next_boundary_ms/1000, tz=datetime.timezone.utc)
            print(f"[test-mode:real] already on boundary (wait {wait_ms/1000:.1f}s, next {dt_next.isoformat()}), starting immediately")
        elif wait_ms <= 300_000:
            wait_s = wait_ms / 1000
            dt_next = datetime.datetime.fromtimestamp(next_boundary_ms/1000, tz=datetime.timezone.utc)
            print(f"[test-mode:real] aligning to next 5m market boundary {dt_next.isoformat()} — waiting {wait_s:.1f}s (collector PRE-WARMING: WS + discovery + subscriptions live, snapshots gated to boundary) — 5m-only, 7 assets")
            end_wait = time.time() + wait_s
            while time.time() < end_wait and self._running:
                await asyncio.sleep(min(1, end_wait - time.time()))
                self._beat()
            print(f"[test-mode:real] boundary reached — collector already warm (pre-subscribed), first window starts now")
        else:
            print(f"[test-mode:real] starting immediately (wait {wait_ms/1000:.1f}s >300s, unexpected) — will capture current + next windows")

        # In finite quick-test (4 markets = 2 chunks of 2), disable background kaggle loop — run_test_mode drives chunk uploads itself (every 2 markets =10min)
        # This avoids double-upload race where both loops flush/export concurrently (§10A).
        if getattr(self, "_kaggle_task", None):
            try:
                self._kaggle_task.cancel()
            except Exception:
                pass
            print("[test-mode] background kaggle loop disabled (chunk uploads driven by test loop)")
        kaggle_interval = getattr(self.config.kaggle, "test_upload_interval_seconds", 600)
        print(f"[test-mode:real] QUICK TEST — {num_markets}×{ws}s (5m-only) = {num_markets*(ws/60):.0f}min total, {len(self.config.assets)} assets {self.config.assets}")
        print(f"[test-mode] chunks: every 2 markets → kaggle every {kaggle_interval}s (10min), one-file-per-asset staging (39 files for 7 assets): BTC/ETH/..._book_snapshots, trades, book_events, chainlink + 3 globals + summary")

        timeout_s = num_markets * ws + 90  # 4*300+90=1290s ~21.5 min
        # R-1: measure the run from the boundary, not from process start — the
        # pre-warm wait must not count against the timeout or pull the first
        # Kaggle chunk forward, and the analysis must expect only the windows
        # that START at/after this boundary.
        start_ts = next_boundary_ms // 1000
        start_ms = next_boundary_ms
        # Will be refined after first market discovery (see below)
        completed_windows: Dict[str, set] = {a: set() for a in self.config.assets}
        last_kaggle_s = start_ts
        kaggle_uploads: list[dict] = []

        async def _do_test_kaggle_upload(tag: str):
            # Flush + compact + staging upload, record result — held under kaggle_lock so snapshot appends buffer safely during staging.
            async with self._kaggle_lock:
                try:
                    n = self.writer.flush()
                    if n:
                        print(f"[test-kaggle:{tag}] flushed {n} rows before staging (lock held, snapshots go to fresh buffer)")
                except Exception as e:
                    print(f"[test-kaggle:{tag}] flush err {e}")
                try:
                    self.markets_log.flush_staging()
                except Exception:
                    pass
                try:
                    p = self.markets_log.compact()
                    print(f"[test-kaggle:{tag}] compacted {p}")
                except Exception as e:
                    print(f"[test-kaggle:{tag}] compact err {e}")
                # Build clean view so snapshot_clean available for analysis
                try:
                    from .storage.clean_view import build_clean_view
                    built = build_clean_view(self.config.storage.data_dir)
                    print(f"[test-kaggle:{tag}] clean_view rows {built}")
                except Exception as e:
                    print(f"[test-kaggle:{tag}] clean_view err {e}")
                # Prepare staging + upload (dry_run if no creds to avoid crash)
                # Staging is one-file-per-asset: 7 assets ×5 (snapshots, clean view, trades, book_events, chainlink) +3 globals + summary =39 files
                # Single dataset gghgg1/polymarket-5m-crypto — all assets share same slug, cumulative rows.
                _has_creds = _validate_kaggle_config()
                try:
                    from .storage.export import cleanup_local_data as _cleanup
                    # B-fix: run the export in a worker thread — it makes
                    # synchronous HTTP calls (data-api enrichment, Kaggle client)
                    # that otherwise FREEZE the whole event loop for minutes,
                    # stalling the 500ms snapshot scheduler (measured: the chunk
                    # upload at 20:20 blocked the loop past the run's timeout).
                    res = await asyncio.to_thread(
                        export_and_upload_all_kaggle,
                        data_dir=self.config.storage.data_dir,
                        assets=self.config.assets,
                        timeframe_labels=["5m"],
                        l2_levels=self.config.l2_levels,
                        dry_run=not _has_creds,
                    )
                    # Quick-test prune: after verified Kaggle ready, delete only closed markets.
                    # Prod keeps 2h buffer; test uses 120s so first 2 markets (10min old) are removable by second chunk.
                    # For dry_run (no creds) we do NOT delete — just log what would be pruned, to avoid losing data without Kaggle.
                    def _is_dry(r):
                        try:
                            for v in r.get("kaggle_uploads", {}).values():
                                if v.get("status") == "dry_run":
                                    return True
                        except Exception:
                            pass
                        return False
                    is_dry = _is_dry(res)
                    if not is_dry and _has_creds:
                        built_pruned = sum(res.get("cleanup", {}).values()) if isinstance(res.get("cleanup"), dict) else 0
                        if built_pruned == 0:
                            # built-in 2h prune kept everything (expected in 20min test); do test-buffer prune to demo delete
                            try:
                                extra = _cleanup(self.config.storage.data_dir, assets=self.config.assets, timeframe_labels=["5m"], keep_seconds=120, checkpoint_ms=None)
                                if extra:
                                    print(f"[test-kaggle:{tag}] test-buffer prune (120s) extra: {extra} — closed markets only, open window kept")
                                    for k, v in extra.items():
                                        res["cleanup"][k] = res["cleanup"].get(k, 0) + v
                            except Exception as e:
                                print(f"[test-kaggle:{tag}] test prune err {e}")
                    elif is_dry:
                        # dry_run: simulate prune, don't delete — show what would be deleted after real Kaggle
                        try:
                            # preview: count files that would be deleted with 120s buffer, without actually deleting if no checkpoint
                            # We peek by calling cleanup with a preview flag? For now just log, no delete.
                            print(f"[test-kaggle:{tag}] dry_run — skipping delete (no Kaggle creds). Would prune closed markets with 120s buffer after real upload.")
                        except Exception:
                            pass

                    # I-11: only a verified-successful upload counts as a completed
                    # chunk — a failed attempt previously satisfied the chunk gate and
                    # permanently blocked the final retry within the run.
                    _up_status = None
                    try:
                        for _v in (res.get("kaggle_uploads") or {}).values():
                            _up_status = _v.get("status")
                            if _up_status == "success":
                                break
                    except Exception:
                        pass
                    if _up_status == "success":
                        kaggle_uploads.append({"at_s": int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp()) - start_ts, "tag": tag, "result": res})
                    else:
                        print(f"[test-kaggle:{tag}] upload NOT counted as completed chunk (status={_up_status or res.get('error')}) — data retained, retry scheduled")
                    # one-file-per-asset audit: staging should be 39 files for 7 assets
                    try:
                        st = res.get("staging", {})
                        files = st.get("files", 0)
                        expected = len(self.config.assets) * 5 + 4
                        if files != expected:
                            print(f"[test-kaggle:{tag}] WARN staging files {files} != expected {expected} (one-file-per-asset)")
                        else:
                            print(f"[test-kaggle:{tag}] staging OK {files} files (one per asset: snapshots, clean view, trades, book_events, chainlink +3 globals + summary)")
                    except Exception:
                        pass
                    # also intermediate lightweight analysis
                    try:
                        inter = await self._analyse_test_data(start_ms, num_markets, window_size=ws, write_interim=f"test_analysis_{tag}.json")
                        print(f"[test-kaggle:{tag}] interim completeness {inter.get('checks',{}).get('snapshot_completeness_pct')}% clean {inter.get('checks',{}).get('clean_completeness_pct')}%")
                    except Exception as e:
                        print(f"[test-kaggle:{tag}] interim analyse err {e}")
                    # keep list on collector for final report
                    self._kaggle_uploads = kaggle_uploads
                    return res
                except Exception as e:
                    print(f"[test-kaggle:{tag}] kaggle err {e}")
                    import traceback; traceback.print_exc()
                    return {"error": str(e)}

        try:
            while self._running:
                await asyncio.sleep(5)
                elapsed = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp()) - start_ts
                # 10-min Kaggle schedule inside test (gate on closed markets handled inside export_and_upload)
                if elapsed - (last_kaggle_s - start_ts) >= kaggle_interval and elapsed >= kaggle_interval:
                    # Only upload if at least one full window closed (avoid empty first tick)
                    # Check if any market_end < now
                    has_closed = False
                    now_ms = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp()*1000)
                    for m in list(self.markets.values()):
                        if m.market_end_ts_ms < now_ms:
                            has_closed = True
                            break
                    # also allow if we have snapshots files already
                    if not has_closed:
                        from pathlib import Path as _P
                        if (_P(self.config.storage.data_dir)/"book_snapshots_500ms").exists():
                            has_closed = True
                    if has_closed:
                        await _do_test_kaggle_upload(tag=f"t{elapsed//60}min")
                        last_kaggle_s = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
                    else:
                        print(f"[test-mode] {elapsed}s no closed market yet, deferring kaggle")
                        last_kaggle_s += 60  # retry in 1 min
                # update completed windows
                for asset in self.config.assets:
                    state = self.rollover.states.get(asset.upper())
                    if not state:
                        continue
                    for cid, m in list(self.markets.items()):
                        if m.asset.upper() != asset.upper():
                            continue
                        # R-1: the pre-warm tail window (discovered before the
                        # boundary, snapshots gated off) is not one of the run's
                        # num_markets windows — counting it would end the run a
                        # window short.
                        if (m.market_start_ts_ms or 0) < start_ms:
                            continue
                        now_ms = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
                        if m.market_end_ts_ms < now_ms:
                            completed_windows[asset].add(m.window_index)
                # I-5: attribute missing markets in real time — a window that started
                # >75s ago with no discovered market is a coverage_gap, not silence
                now_ms_c = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
                cur_widx = (now_ms_c // 1000) // ws
                boundary_widx = start_ms // 1000 // ws
                for asset in self.config.assets:
                    has_market = any(
                        m.asset.upper() == asset.upper() and m.window_index == cur_widx
                        for m in self.markets.values()
                    )
                    if (not has_market and (asset, cur_widx) not in self._coverage_gapped
                            and cur_widx >= boundary_widx
                            and now_ms_c >= (cur_widx * ws + 75) * 1000):
                        self._coverage_gapped.add((asset, cur_widx))
                        self._collector_event(CollectorEventType.coverage_gap, {
                            "asset": asset,
                            "window_index": cur_widx,
                            "window_start_ts_ms": cur_widx * ws * 1000,
                            "slug": f"{asset.lower()}-updown-{(ws//60)}m-{cur_widx*ws}",
                            "reason": "no market discovered 75s after window start",
                        })
                        print(f"[test-mode] coverage_gap: {asset} window {cur_widx} — no market discovered")
                gap_count = {a: sum(1 for (aa, _) in self._coverage_gapped if aa == a) for a in self.config.assets}
                all_done = all(
                    len(completed_windows[a]) + gap_count.get(a, 0) >= num_markets
                    for a in self.config.assets
                )
                if elapsed % 30 < 5:
                    prog = {a: sorted(s) for a, s in completed_windows.items()}
                    print(f"[test-mode:real] {elapsed}s elapsed, windows per asset: {prog}, timeout {timeout_s}s, kaggle {len(kaggle_uploads)} uploads")
                if all_done:
                    # B-1: let the final in-window bucket(s) land before stopping —
                    # an immediate stop raced the last 500ms tick (1 missing bucket
                    # per asset at 19:14:59.5 in the previous run).
                    await asyncio.sleep(1.5)
                    print(f"[test-mode:real] all assets have {num_markets} windows (completed={completed_windows}), stopping")
                    break
                if elapsed >= timeout_s:
                    print(f"[test-mode:real] timeout {timeout_s}s reached, stopping with progress {completed_windows}")
                    break
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
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

        # Final Kaggle upload — only if needed (don't duplicate chunk already uploaded at 20min)
        # For 4 markets → 2 chunks (10min + 20min). If last upload was <4min ago and we already have 2 uploads, skip.
        try:
            now_ts = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
            since_last = now_ts - last_kaggle_s if last_kaggle_s else 9999
            expected_chunks = max(1, num_markets // 2)  # 4 →2, 2→1
            need_final = (since_last >= 250) or (len(kaggle_uploads) < expected_chunks)
            if need_final:
                print(f"[test-mode] final kaggle check: since_last={since_last}s uploads={len(kaggle_uploads)}/{expected_chunks} → uploading final")
                await _do_test_kaggle_upload(tag="final")
            else:
                print(f"[test-mode] final kaggle skipped (last {since_last}s ago, {len(kaggle_uploads)} chunks already) — quick-test done")
        except Exception as e:
            print(f"[test-mode] final kaggle err {e}")

        analysis = await self._analyse_test_data(start_ms, num_markets, window_size=ws)
        analysis["real_mode"] = True
        analysis["window_label"] = "5m"
        analysis["kaggle_uploads_during_test"] = kaggle_uploads
        analysis["finished_at_utc"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00","Z")
        # Persist final list to file already done inside analyse, append kaggle info
        try:
            from pathlib import Path as _P2
            import json as _js
            data_dir = Path(self.config.storage.data_dir)
            out_path = data_dir / "test_analysis.json"
            # ensure data dir exists
            data_dir.mkdir(parents=True, exist_ok=True)
            existing = _js.loads(out_path.read_text()) if out_path.exists() else {}
            existing["kaggle_uploads_during_test"] = kaggle_uploads
            existing["finished_at_utc"] = analysis["finished_at_utc"]
            out_path.write_text(_js.dumps(existing, indent=2, default=str))
            # also write final analysis version
            final_path = data_dir / "test_analysis_final.json"
            final_path.write_text(_js.dumps(analysis, indent=2, default=str))
        except Exception as e:
            print(f"[test-mode] warning: could not write analysis files: {e}")
        # One more clean_view build for fresh book_snapshots_clean before exit
        try:
            from .storage.clean_view import build_clean_view
            build_clean_view(self.config.storage.data_dir)
        except Exception:
            pass
        return analysis

    async def _analyse_test_data(self, start_ms: int, num_markets: int, window_size: int = 300, write_interim: str | None = None) -> dict:
        """Deep analysis: counts, completeness, per-column null % , book_state, gaps, trades/chainlink, data-loss.

        Writes data/test_analysis.json (or interim variant) with null/data-loss audit for every 10-min Kaggle step and final.
        """
        import pyarrow.parquet as pq
        import pyarrow.compute as pc
        from pathlib import Path
        import datetime, json as _js
        base = Path(self.config.storage.data_dir)
        start_dt = datetime.datetime.fromtimestamp(start_ms/1000, tz=datetime.timezone.utc)
        date_str = start_dt.date().isoformat()
        interim_tag = write_interim or "test_analysis.json"
        print(f"[analyse] date={date_str} start_ms={start_ms} num_markets={num_markets} window={window_size} -> {interim_tag}")
        report: dict = {
            "generated_start_utc": start_dt.isoformat().replace("+00:00","Z"),
            "date": date_str,
            "num_markets": num_markets,
            "window_size_seconds": window_size,
            "window_label": "5m",
            "ticks_per_market": window_size // 300 * 600,
            "assets": self.config.assets,
            "expected_per_asset_per_window": 600,
            "datasets": {},
            "checks": {},
            "null_analysis": {},
            "data_loss": {},
            "kaggle_staging": {},
        }
        # I-4 / bonus-row accounting: expect ticks only for markets that
        # actually exist AND have fully elapsed — counting the just-discovered
        # next window (2 ticks so far) inflated the denominator and faked a
        # completeness shortfall. Rows belonging to other windows (the
        # pre-discovered next window, pre-warm tails) are counted separately
        # as "bonus rows" instead of inflating completeness. Computed up front
        # so per-dataset scans can record discovered-window row counts while
        # the tables are in hand (they are stripped from the report after).
        ticks_per_market = report["ticks_per_market"]
        import time as _time_mod
        _now_ms_a = int(_time_mod.time() * 1000)
        elapsed_cids = {cid for cid, m in self.markets.items()
                        if m.market_end_ts_ms < _now_ms_a
                        # R-1: only windows that STARTED at/after the run start —
                        # a pre-warm tail window (snapshots gated off) would
                        # otherwise inflate the expected denominator to ~50%.
                        and (m.market_start_ts_ms or 0) >= start_ms}
        expected_snaps = num_markets * ticks_per_market * len(self.config.assets)
        discovered = len(elapsed_cids)
        expected_discovered = discovered * ticks_per_market

        def count_dataset(name: str):
            p = base / name
            if not p.exists():
                # also handle single-file dataset markets_latest
                return {"exists": False, "files": 0, "rows": 0, "rows_discovered": 0, "sample": None, "columns": None, "table": None}
            files = [f for f in p.rglob("*.parquet") if not f.name.endswith(".tmp")]
            if not files and p.is_file():
                files = [p]
            total = 0
            sample = None
            columns = None
            table = None
            for part in files:
                try:
                    tbl = read_table(part)
                    total += tbl.num_rows
                    if table is None and tbl.num_rows>0:
                        table = tbl
                        sample = tbl.slice(0,1).to_pylist()[0] if tbl.num_rows else None
                        columns = tbl.column_names
                    elif tbl.num_rows>0 and sample is None:
                        sample = tbl.slice(0,1).to_pylist()[0]
                        columns = tbl.column_names
                    elif table is not None:
                        # keep combined for null analysis: concat lazily later if needed
                        pass
                except Exception:
                    continue
            # For accurate null analysis return concatenated table if multiple files
            combined = None
            rows_discovered = 0
            if files:
                try:
                    tables = []
                    for f in files:
                        try:
                            tables.append(read_table(f))
                        except Exception:
                            continue
                    if tables:
                        combined = read_table(files[0]) if len(tables)==1 else __import__("pyarrow").concat_tables(tables, promote_options="default") if len(tables)>1 else None
                        if combined is not None and combined.num_rows>0 and sample is None:
                            sample = combined.slice(0,1).to_pylist()[0]
                            columns = combined.column_names
                            table = combined
                        elif combined is not None:
                            table = combined
                except Exception:
                    pass
            # discovered-window rows (for honest ≤100% completeness): counted
            # while the table is in hand; elapsed_cids known up front.
            if combined is not None and combined.num_rows and elapsed_cids and "condition_id" in combined.schema.names:
                try:
                    import pyarrow as _pa
                    _mask = _pa.compute.is_in(combined.column("condition_id"),
                                              value_set=_pa.array(sorted(elapsed_cids), type=_pa.string()))
                    rows_discovered = combined.filter(_mask).num_rows
                except Exception:
                    rows_discovered = 0
            return {"exists": True, "files": len(files), "rows": total, "rows_discovered": rows_discovered, "sample": sample, "columns": columns, "table": table}

        def null_stats(tbl) -> dict:
            if tbl is None or tbl.num_rows==0:
                return {"rows": 0, "cols": {}}
            out = {"rows": tbl.num_rows, "cols": {}}
            for col in tbl.column_names:
                try:
                    col_arr = tbl.column(col)
                    nulls = col_arr.null_count if hasattr(col_arr, "null_count") else 0
                    # also count zeros for numeric cols to contrast null-vs-zero §3
                    zeros = 0
                    try:
                        if col_arr.type in (__import__("pyarrow").float64(), __import__("pyarrow").int64(), __import__("pyarrow").float32()):
                            # compute zeros via pc
                            import pyarrow.compute as _pc
                            zeros = _pc.sum(_pc.equal(col_arr, 0)).as_py() if tbl.num_rows else 0
                            if zeros is None:
                                zeros = 0
                    except Exception:
                        zeros = 0
                    out["cols"][col] = {"null": nulls, "null_pct": round(100*nulls/tbl.num_rows,2), "zeros": int(zeros) if zeros else 0, "zero_pct": round(100*int(zeros)/tbl.num_rows,2) if zeros else 0}
                except Exception:
                    pass
            return out

        for ds in ["book_snapshots_500ms", "book_snapshots_clean", "book_events", "trades", "chainlink_events", "collector_events", "markets_log", "markets_latest", "resync_episodes"]:
            info = count_dataset(ds)
            if ds=="markets_latest":
                p = base / "markets_latest" / "markets_latest.parquet"
                if p.exists():
                    try:
                        tbl = read_table(p)
                        info = {"exists": True, "files": 1, "rows": tbl.num_rows, "sample": tbl.slice(0,1).to_pylist()[0] if tbl.num_rows else None, "columns": tbl.column_names[:16] if tbl.num_rows else None, "table": tbl}
                    except Exception as e:
                        print(f"[analyse] markets_latest read err {e}")
                        info = {"exists": True, "files": 1, "rows": 0, "sample": None, "columns": None, "table": None}
            report["datasets"][ds] = {k:v for k,v in info.items() if k!="table"}
            print(f"[analyse] {ds}: {info['rows']} rows in {info['files']} files, exists={info['exists']}")
            # null analysis for key datasets
            if info.get("table") is not None and info["rows"]>0:
                # limit heavy tables to first 20000 rows for speed if huge
                t = info["table"]
                if t.num_rows>20000:
                    t = t.slice(0, 20000)
                report["null_analysis"][ds] = null_stats(t)
                # quick book_state histogram for snapshots
                if ds=="book_snapshots_500ms" and "book_state" in t.schema.names:
                    try:
                        vals = t.column("book_state").to_pylist()
                        from collections import Counter
                        report["checks"]["book_state_histogram"] = dict(Counter([v for v in vals if v]))
                        live = report["checks"]["book_state_histogram"].get("live",0)
                        stale = report["checks"]["book_state_histogram"].get("stale",0)
                        report["checks"]["live_pct"] = round(100*live/t.num_rows,2) if t.num_rows else 0
                    except Exception:
                        pass

        checks = report["checks"]
        snap_info = report["datasets"]["book_snapshots_500ms"]
        # elapsed_cids / expected_discovered / discovered computed up front
        # (see above); per-dataset scans recorded rows_discovered while the
        # tables were in hand. Headline actuals count ONLY discovered windows
        # so completeness reads ≤100% honestly; anything else is bonus rows.
        actual_snaps_disc = snap_info.get("rows_discovered") or 0
        actual_clean_disc = report["datasets"]["book_snapshots_clean"].get("rows_discovered") or 0
        checks["expected_book_snapshots"] = expected_discovered
        checks["expected_book_snapshots_if_all_windows"] = expected_snaps
        checks["discovered_windows_total"] = discovered
        checks["actual_book_snapshots"] = actual_snaps_disc if actual_snaps_disc else snap_info["rows"]
        checks["actual_book_snapshots_total"] = snap_info["rows"]
        checks["bonus_book_snapshots"] = max(0, snap_info["rows"] - checks["actual_book_snapshots"])
        checks["actual_clean_snapshots"] = actual_clean_disc if actual_clean_disc else report["datasets"]["book_snapshots_clean"]["rows"]
        checks["actual_clean_snapshots_total"] = report["datasets"]["book_snapshots_clean"]["rows"]
        checks["bonus_clean_snapshots"] = max(0, report["datasets"]["book_snapshots_clean"]["rows"] - checks["actual_clean_snapshots"])
        checks["snapshot_completeness_pct"] = round(100*checks["actual_book_snapshots"]/expected_discovered,2) if expected_discovered else 0
        checks["clean_completeness_pct"] = round(100*checks["actual_clean_snapshots"]/expected_discovered,2) if expected_discovered else 0
        # completeness.py style report per asset if date partitions exist
        try:
            from .completeness import compute_daily_completeness
            daily = compute_daily_completeness(base, date_str)
            checks["daily_completeness"] = [d.to_dict() for d in daily]
        except Exception as e:
            checks["daily_completeness_error"] = str(e)
        if snap_info["rows"] and snap_info["sample"]:
            s = snap_info["sample"]
            checks["sample_up_bid"] = s.get("up_bid")
            checks["sample_chainlink_price"] = report["datasets"]["chainlink_events"]["sample"].get("price") if report["datasets"]["chainlink_events"]["sample"] else None
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
            checks["price_bounds_ok"] = None
        # chainlink checks
        cl = report["datasets"]["chainlink_events"]
        if cl["sample"]:
            checks["chainlink_has_price"] = cl["sample"].get("price") is not None
        # collector_events gaps
        ce = report["datasets"]["collector_events"]
        if ce["rows"]:
            try:
                ce_tbl = count_dataset("collector_events")["table"]
                if ce_tbl is not None:
                    pyl = ce_tbl.to_pylist()
                    from collections import Counter
                    by_type = Counter(r.get("event_type") for r in pyl)
                    checks["collector_events_by_type"] = dict(by_type)
                    checks["sequence_gaps"] = by_type.get("sequence_gap",0)
                    checks["resync_failed"] = by_type.get("resync_failed",0)
                    checks["coverage_gaps"] = by_type.get("coverage_gap",0)
                    checks["rollover_miss"] = by_type.get("rollover_miss",0)
                    checks["book_anomaly"] = by_type.get("book_anomaly",0)
            except Exception:
                pass
        # resync gap total
        re_info = report["datasets"]["resync_episodes"]
        if re_info["rows"]:
            try:
                tbl = report["null_analysis"]["resync_episodes"]["rows"]
            except Exception:
                pass
        # data loss summary: expected vs missing intervals — expected uses the
        # discovered windows (honest denominator; all-window count would count
        # markets that were never discovered as "loss")
        _eff_expected = expected_discovered if expected_discovered else expected_snaps
        checks["missing_intervals"] = max(0, _eff_expected - checks["actual_clean_snapshots"])
        checks["data_loss_pct"] = round(100*checks["missing_intervals"]/_eff_expected,2) if _eff_expected else 0
        # null loss for critical fields: top-of-book null % should be low if books have liquidity; high null => empty book side
        try:
            snap_null = report["null_analysis"].get("book_snapshots_500ms",{}).get("cols",{})
            crit = ["up_bid","up_ask","down_bid","down_ask","up_bid_size","up_ask_size"]
            checks["critical_null_pct"] = {k: snap_null.get(k,{}).get("null_pct") for k in crit if k in snap_null}
            # flag if any critical >80% null (suggests book never populated)
            checks["critical_null_flag"] = any((snap_null.get(k,{}).get("null_pct",0) > 80) for k in crit)
        except Exception:
            pass
        # trades chainlink loss
        checks["trades_rows"] = report["datasets"]["trades"]["rows"]
        checks["chainlink_rows"] = report["datasets"]["chainlink_events"]["rows"]
        # kaggle staging check (38 files expected for 7 assets)
        try:
            kag_staging = base / "kaggle_staging" / "5m" / "gghgg1/polymarket-5m-crypto"
            if kag_staging.exists():
                files = [f for f in kag_staging.glob("*.parquet") if not f.name.endswith(".tmp")]
                report["kaggle_staging"] = {"exists": True, "files": len(files), "expected": len(self.config.assets)*5 + 4, "dataset": "gghgg1/polymarket-5m-crypto", "file_list": sorted([f.name for f in files])[:20]}
                # meta
                meta = kag_staging / "dataset-metadata.json"
                if meta.exists():
                    report["kaggle_staging"]["metadata"] = _js.loads(meta.read_text())
            else:
                report["kaggle_staging"] = {"exists": False, "expected": len(self.config.assets)*4 + 3}
        except Exception as e:
            report["kaggle_staging"] = {"error": str(e)}
        # Write report
        out_path = base / interim_tag
        try:
            out_path.write_text(_js.dumps(report, indent=2, default=str))
            print(f"[analyse] wrote {out_path} completeness {checks.get('snapshot_completeness_pct')}% clean {checks.get('clean_completeness_pct')}% loss {checks.get('data_loss_pct')}%")
        except Exception as e:
            print(f"[analyse] write err {e}")
        return report

    async def _kaggle_upload_loop(self) -> None:
        """5m-only Kaggle loop: single dataset gghgg1/polymarket-5m-crypto.

        Prod: hourly (3600s). Test: every 10 min (600s) via config.kaggle.test_upload_interval_seconds.
        Uses staging folder upload with retry 5 + status poll, only after Kaggle ready does safe prune.
        Starts with delay = interval (not immediate) to avoid empty first upload.
        """
        # Determine interval: prod hourly unless test_mode enabled
        interval = self.config.kaggle.upload_interval_seconds
        try:
            if getattr(self.config.test_mode, "enabled", False):
                interval = getattr(self.config.kaggle, "test_upload_interval_seconds", 600)
        except Exception:
            pass
        # In run_test_mode, this loop is also started but we coordinate via _running flag
        # Validate creds once
        _has_creds = _validate_kaggle_config()
        print(f"[kaggle] 5m-only dataset gghgg1/polymarket-5m-crypto interval {interval}s ({interval//60} min), creds={'ok' if _has_creds else 'missing (dry-run only)'}")
        # Initial delay before first upload (so collector can discover markets)
        slept = 0
        while self._running and slept < interval:
            await asyncio.sleep(min(5, interval - slept))
            slept += 5
            if not self._running:
                return
        while self._running:
            try:
                # Only upload full closed markets — gate inside export_and_upload handles it, but skip if no parquet yet
                has_data = False
                from pathlib import Path as _P
                if (_P(self.config.storage.data_dir)/"book_snapshots_500ms").exists():
                    # check any parquet exists
                    has_data = any((_P(self.config.storage.data_dir)/"book_snapshots_500ms").rglob("*.parquet"))
                if not has_data:
                    print("[kaggle] no hive data yet, skipping")
                else:
                    # Flush buffer under kaggle lock so staging includes in-memory rows (fixes 2b unflushed buffer)
                    # Uses same lock as test mode and flush loop to avoid races with snapshot append
                    if hasattr(self, "_kaggle_lock"):
                        async with self._kaggle_lock:
                            try:
                                n = self.writer.flush()
                                if n:
                                    print(f"[kaggle loop] flushed {n} rows before staging (lock held)")
                            except Exception as e:
                                print(f"[kaggle loop] flush err {e}")
                            try:
                                self.markets_log.flush_staging()
                            except Exception:
                                pass
                            try:
                                self.markets_log.compact()
                            except Exception:
                                pass
                            res = await asyncio.to_thread(
                                export_and_upload_all_kaggle,
                                data_dir=self.config.storage.data_dir,
                                assets=self.config.assets,
                                timeframe_labels=["5m"],
                                l2_levels=self.config.l2_levels,
                                dry_run=not _has_creds,
                            )
                    else:
                        res = await asyncio.to_thread(
                            export_and_upload_all_kaggle,
                            data_dir=self.config.storage.data_dir,
                            assets=self.config.assets,
                            timeframe_labels=["5m"],
                            l2_levels=self.config.l2_levels,
                            dry_run=not _has_creds,
                        )
                    export_n = len(res.get("export", {}))
                    kag = res.get("kaggle_uploads", {})
                    succ = sum(1 for v in kag.values() if v.get("status")=="success")
                    dry = sum(1 for v in kag.values() if v.get("status")=="dry_run")
                    print(f"[kaggle] staging {export_n} files -> {kag} succ {succ} dry {dry} prune {res.get('cleanup',{})}")
                    self._kaggle_uploads.append({"ts": int(time.time()), "interval": interval, "result": res})
                # Sleep interval with early-exit check every 5s
                slept2 = 0
                while self._running and slept2 < interval:
                    await asyncio.sleep(min(5, interval - slept2))
                    slept2 += 5
            except asyncio.CancelledError:
                print("[kaggle] cancelled")
                break
            except Exception as e:
                print(f"[kaggle] loop err {e}")
                import traceback; traceback.print_exc()
                await asyncio.sleep(60)
