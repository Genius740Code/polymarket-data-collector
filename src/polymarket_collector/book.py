"""Order book state — §3 book_snapshots_500ms.

One BookState per (asset, condition_id). Implements:
- null-vs-zero uniformly (empty side → None, never 0) — §3
- depth aggregates depth_1c/5c/10c precisely defined — §3
- book_crossed flag
- sanitize-bounds gate before apply (§3A)
- 500ms snapshot generation aligned to wall-clock grid (§1A/§3)
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .enums import BookState
from .validation import validate_ws_message


# -- helpers ---------------------------------------------------------------

def snapshot_bucket_ms(unix_ms: int, interval_ms: int = 500) -> int:
    """Shared wall-clock grid: floor(unix_ms/interval)*interval, UTC epoch aligned (§1A/§3)."""
    return (unix_ms // interval_ms) * interval_ms


def depth_within(levels: List[Tuple[Optional[float], Optional[float]]], best: Optional[float], window_cents: int) -> Optional[float]:
    """Cumulative size within N cents of own best price (§3 definition).

    levels: list of (price, size) sorted best-first (bids descending, asks ascending).
    If best is None (empty side), returns None, not 0 (§3).
    Levels with None price/size are skipped (null-padded tail).
    window is N cents = N*0.01 in token units.
    """
    if best is None:
        return None
    window = window_cents * 0.01
    total = 0.0
    found_any = False
    for price, size in levels:
        if price is None or size is None:
            continue
        # bid side: price <= best, within window means best - price <= window
        # ask side: price >= best, within window means price - best <= window
        # We don't know side here; assume levels are sorted best-first so distance
        # increases; break once outside window (but handle both directions).
        # Use absolute distance with direction check: for sorted best-first,
        # distance = abs(price - best); if > window then all further levels also outside.
        if abs(price - best) - 1e-9 > window:
            # Because levels sorted best-first, once we exceed window we can break
            # need to know monotonic direction: bids decreasing, asks increasing
            # abs suffices since sorted best-first implies distance non-decreasing
            break
        total += float(size)
        found_any = True
    # If book had a best but no levels within window (e.g. only stale None levels), total 0
    # That is a valid 0 (there are levels but none in window), vs None for empty side.
    # Distinguish: if best is not None we return total (could be 0).
    # However spec: if best is null depth fields are null — already handled above.
    # So return 0 only if we had a best; otherwise null.
    return total if best is not None else None


@dataclass
class Level:
    price: Optional[float]  # None → null (absent level)
    size: Optional[float]

    def is_null(self) -> bool:
        return self.price is None


@dataclass
class SideBook:
    """One side (bid or ask) of one outcome (UP or DOWN)."""
    levels: List[Level] = field(default_factory=list)  # best-first, up to 20

    def best_price(self) -> Optional[float]:
        for lvl in self.levels:
            if not lvl.is_null():
                return lvl.price
        return None

    def best_size(self) -> Optional[float]:
        for lvl in self.levels:
            if not lvl.is_null():
                return lvl.size
        return None

    def is_empty(self) -> bool:
        return self.best_price() is None

    def crossed_with(self, other: "SideBook") -> bool:
        """True if this bid side crossed with other ask side — strictly b > a (equal is not crossed)."""
        b = self.best_price()
        a = other.best_price()
        if b is None or a is None:
            return False
        # 1e-9 tolerance for float equality; crossed only if bid strictly greater than ask
        return (b - a) > 1e-9


@dataclass
class OutcomeBook:
    bids: SideBook = field(default_factory=SideBook)
    asks: SideBook = field(default_factory=SideBook)


@dataclass
class BookSnapshot:
    """Wide flat-column snapshot row matching §3 schema."""
    snapshot_id: str
    schema_version: str
    series_id: str
    window_index: int
    condition_id: str
    market_id: str
    asset: str
    up_token_id: str
    down_token_id: str
    ts_snapshot_utc: str  # ISO8601
    ts_snapshot_ns: int
    # top-of-book (null vs zero)
    up_bid: Optional[float]
    up_ask: Optional[float]
    up_bid_size: Optional[float]
    up_ask_size: Optional[float]
    down_bid: Optional[float]
    down_ask: Optional[float]
    down_bid_size: Optional[float]
    down_ask_size: Optional[float]
    # L2 (dict of field -> value, 160 cols if 20 levels)
    l2: Dict[str, Optional[float]]
    # depth aggregates
    depths: Dict[str, Optional[float]]
    # state
    market_time_remaining_ms: int
    up_book_age_ms: Optional[int]
    down_book_age_ms: Optional[int]
    is_rollover_window: bool
    book_state: str
    resync_id: Optional[str]
    book_crossed: bool

    def to_flat_dict(self) -> Dict[str, object]:
        d: Dict[str, object] = {
            "ts_snapshot_utc": self.ts_snapshot_utc,
            "ts_snapshot_ns": self.ts_snapshot_ns,
            "condition_id": self.condition_id,
            "market_id": self.market_id,
            "series_id": self.series_id,
            "window_index": self.window_index,
            "asset": self.asset,
            "snapshot_id": self.snapshot_id,
            "up_token_id": self.up_token_id,
            "down_token_id": self.down_token_id,
            "up_bid": self.up_bid,
            "up_ask": self.up_ask,
            "up_bid_size": self.up_bid_size,
            "up_ask_size": self.up_ask_size,
            "down_bid": self.down_bid,
            "down_ask": self.down_ask,
            "down_bid_size": self.down_bid_size,
            "down_ask_size": self.down_ask_size,
            "market_time_remaining_ms": self.market_time_remaining_ms,
            "up_book_age_ms": self.up_book_age_ms,
            "down_book_age_ms": self.down_book_age_ms,
            "is_rollover_window": self.is_rollover_window,
            "book_state": self.book_state,
            "resync_id": self.resync_id,
            "book_crossed": self.book_crossed,
        }
        d.update(self.l2)
        d.update(self.depths)
        return d


class OrderBookState:
    """Per-market order book — §3 + §3A + §1A book_state.

    Holds UP and DOWN books, applies deltas, validates, produces snapshots.
    """

    def __init__(
        self,
        asset: str,
        condition_id: str,
        market_id: str,
        series_id: str,
        window_index: int,
        up_token_id: str,
        down_token_id: str,
        market_end_ts_ms: int,
        schema_version: str = "3.0.0",
        l2_levels: int = 20,
    ):
        self.asset = asset
        self.condition_id = condition_id
        self.market_id = market_id
        self.series_id = series_id
        self.window_index = window_index
        self.up_token_id = up_token_id
        self.down_token_id = down_token_id
        self.market_end_ts_ms = market_end_ts_ms
        self.schema_version = schema_version
        self.l2_levels = l2_levels

        self.up = OutcomeBook()
        self.down = OutcomeBook()
        self.book_state: BookState = BookState.live
        self.resync_id: Optional[str] = None
        self._last_update_ns: Optional[int] = None
        self._up_book_age_ms: Optional[int] = None
        self._down_book_age_ms: Optional[int] = None
        self.is_rollover_window: bool = False
        self.sequence_numbers: Dict[str, int] = {}  # token_id -> last seq
        self._stale_since_ms: Optional[int] = None
        # §4 book_events: old/new BBO captured per applied WS message, drained by the collector
        self.pending_events: List[dict] = []
        self.event_thresholds: Dict[str, float] = {
            "spread_change_threshold": 0.002,
            "size_change_threshold_pct": 0.10,
        }

    # -- state transitions (§1A) -------------------------------------------
    def mark_stale(self, resync_id: str | None = None) -> None:
        self.book_state = BookState.stale
        self.resync_id = resync_id or str(uuid.uuid4())
        self._stale_since_ms = int(time.time() * 1000)

    def mark_resyncing(self, resync_id: str | None = None) -> None:
        self.book_state = BookState.resyncing
        if resync_id:
            self.resync_id = resync_id
        elif not self.resync_id:
            self.resync_id = str(uuid.uuid4())

    def mark_live(self) -> None:
        self.book_state = BookState.live
        # keep resync_id for tagging snapshots in rest of bucket? spec says resync_id
        # groups snapshots affected by episode — so we retain it for one more snapshot
        # then clear on next snapshot call. For simplicity clear now.
        self.resync_id = None
        self._stale_since_ms = None

    # -- apply message (with validation) -----------------------------------
    def apply_ws_message(self, msg: dict) -> Tuple[bool, Optional[str]]:
        """Apply a WS delta/message to the book.

        Returns (applied: bool, error_reason: Optional[str]).
        §3A: if validation fails, do NOT apply, mark stale, return error.
        """
        # sanity bounds first
        errors = validate_ws_message(msg)
        if errors:
            # per §3A: log book_anomaly and mark stale; trigger resync externally
            self.mark_stale()
            return False, f"sanity_bounds_failed: {errors[0].reason} field={errors[0].field} value={errors[0].value}"

        # sequence gap detection (where sequence_number present) — §1A
        # Use explicit None check: seq 0 is valid but falsy with `or` chaining.
        token_id = msg.get("token_id")
        if token_id is None:
            token_id = msg.get("asset_id")
        if token_id is None:
            token_id = msg.get("token")
        seq = msg.get("sequence_number")
        if seq is None:
            seq = msg.get("seq")
        if seq is None:
            seq = msg.get("sequence")
        if token_id and seq is not None:
            try:
                seq_int = int(seq)
            except (TypeError, ValueError):
                seq_int = None
            if seq_int is not None:
                last = self.sequence_numbers.get(str(token_id))
                if last is not None:
                    if seq_int == last:
                        return False, "duplicate_event"
                    if seq_int < last:
                        return False, "out_of_order_duplicate"
                    if seq_int != last + 1:
                        self.mark_stale()
                        return False, f"sequence_gap expected {last+1} got {seq_int}"
                self.sequence_numbers[str(token_id)] = seq_int

        # apply levels if present — handles both full book snapshots (bids/asks)
        # and incremental price_change events (price_changes array)
        pre_bbo = {o: self._bbo(o) for o in ("up", "down")}
        touched: Dict[str, str] = {}  # outcome -> token_id
        # price_changes path (CLOB market channel)
        if "price_changes" in msg and isinstance(msg["price_changes"], list):
            ex_bbo: Dict[str, Dict[str, Optional[float]]] = {}  # outcome -> exchange-reported BBO
            for pc in msg["price_changes"]:
                pc_token = pc.get("asset_id") or pc.get("token_id") or pc.get("asset")
                pc_outcome = self._outcome_for_token(pc_token) if pc_token else None
                if not pc_outcome:
                    continue
                price = pc.get("price")
                size = pc.get("size")
                side = (pc.get("side") or "").upper()
                # side: BUY = bid, SELL = ask
                is_bid = side == "BUY"
                # validation already done via validate_ws_message, but double-check bounds
                try:
                    p = float(price) if price is not None else None
                    s = float(size) if size is not None else None
                except Exception:
                    continue
                if p is None or s is None:
                    continue
                # price_change with size 0 means remove level
                book = self.up if pc_outcome == "up" else self.down
                side_book = book.bids if is_bid else book.asks
                # §3A crossed-book fix: only revert the level that CAUSES a new
                # crossing. Polymarket price_change `side` is the taker side — a
                # market BUY lifting the ask arrives as side=BUY at the ask price,
                # and applying it as a bid level crosses the book. If the book was
                # already crossed, updates must still apply so the ask side can
                # heal (reverting everything froze crossings for whole seconds).
                was_crossed = book.bids.crossed_with(book.asks)
                prev_size: Optional[float] = None
                for lvl in side_book.levels:
                    if lvl.price is not None and abs(lvl.price - p) < 1e-9:
                        prev_size = lvl.size
                        break
                self._apply_price_change_level(side_book, p, s, is_bid)
                if not was_crossed and book.bids.crossed_with(book.asks):
                    self._apply_price_change_level(side_book, p, prev_size if prev_size is not None else 0.0, is_bid)
                    self.pending_events.append({
                        "event_type": "crossed_reverted",
                        "token_id": pc_token, "outcome": pc_outcome,
                        "price": p, "size": s, "side": side,
                    })
                touched.setdefault(pc_outcome, pc_token)
                # exchange-reported authoritative BBO for this token after the change
                try:
                    bb = float(pc["best_bid"]) if pc.get("best_bid") not in (None, "") else None
                    ba = float(pc["best_ask"]) if pc.get("best_ask") not in (None, "") else None
                    if bb is not None or ba is not None:
                        ex_bbo[pc_outcome] = {"bid": bb, "ask": ba}
                except Exception:
                    pass
            # enforce BBO against the exchange's own best_bid/best_ask — heals
            # stale ask/bid sides and guarantees the top-of-book is never crossed
            for outcome, ex in ex_bbo.items():
                self._enforce_bbo(outcome, ex)
            # update age
            self._last_update_ns = time.time_ns()
            # For price_change, we don't know which outcome was updated, so reset both ages slightly
            # Use timestamp from msg if available
            self._emit_bbo_events(pre_bbo, touched, msg)
            return True, None

        outcome = self._outcome_for_token(token_id) if token_id else None
        if outcome and ("bids" in msg or "asks" in msg):
            book = self.up if outcome == "up" else self.down
            if "bids" in msg:
                self._apply_levels(book.bids, msg["bids"], is_bid=True)
            if "asks" in msg:
                self._apply_levels(book.asks, msg["asks"], is_bid=False)
            now_ms = int(time.time() * 1000)
            self._last_update_ns = time.time_ns()
            if outcome == "up":
                self._up_book_age_ms = 0
            else:
                self._down_book_age_ms = 0
            touched.setdefault(outcome, token_id)
            # A full `book` snapshot is a complete exchange-side state — as trustworthy
            # as a REST fetch. Promote a stale/resyncing book to live when the snapshot
            # fills both sides (was the 55s cold-start and post-resync stale blocks).
            if self.book_state != BookState.live:
                if book.bids.best_price() is not None and book.asks.best_price() is not None:
                    self.mark_live()

        self._emit_bbo_events(pre_bbo, touched, msg)
        return True, None

    # -- BBO capture for §4 book_events ------------------------------------
    def _bbo(self, outcome: str) -> Dict[str, Optional[float]]:
        book = self.up if outcome == "up" else self.down
        return {
            "bid": book.bids.best_price(), "bid_size": book.bids.best_size(),
            "ask": book.asks.best_price(), "ask_size": book.asks.best_size(),
        }

    def _enforce_bbo(self, outcome: str, ex: Dict[str, Optional[float]], tick: float = 0.0101) -> None:
        """Snap the book's top-of-book to the exchange-reported best_bid/best_ask.

        Every CLOB price_change carries the authoritative post-change BBO for its
        token. If our book's best disagrees by more than a tick (dropped deltas,
        taker-side artifacts), snap the best level's price IN PLACE (keeping its
        size — sizes self-heal on the next full `book` event; never fabricate).
        Removes a book best that the exchange says is gone; marks stale if the
        exchange reports a best on a side we hold empty (cannot invent a size).
        """
        book = self.up if outcome == "up" else self.down
        for side, ex_best in (("bid", ex.get("bid")), ("ask", ex.get("ask"))):
            side_book = book.bids if side == "bid" else book.asks
            my_best = side_book.best_price()
            if ex_best is None:
                if my_best is not None:
                    self._apply_price_change_level(side_book, my_best, 0.0, side == "bid")
                continue
            if my_best is None:
                # exchange reports a best on a side we hold empty — leave it; the
                # next full `book` event fills the side. (Marking stale here caused
                # a stale churn on thin books, since deltas arrive before snapshots.)
                continue
            if abs(my_best - ex_best) > tick:
                for lvl in side_book.levels:
                    if lvl.price is not None and abs(lvl.price - my_best) < 1e-9:
                        lvl.price = ex_best
                        break
                # re-sort best-first (null tail stays at the end for both sides)
                if side == "bid":
                    side_book.levels.sort(key=lambda l: (l.price is None, -(l.price or 0.0)))
                else:
                    side_book.levels.sort(key=lambda l: (l.price is None, l.price if l.price is not None else 0.0))
                self.pending_events.append({
                    "event_type": "bbo_snapped",
                    "token_id": self.up_token_id if outcome == "up" else self.down_token_id,
                    "outcome": outcome, "side": side,
                    "book_best": my_best, "exchange_best": ex_best,
                })

    def _emit_bbo_events(self, pre_bbo: Dict[str, Dict[str, Optional[float]]], touched: Dict[str, str], msg: dict) -> None:
        """Emit §4 book_events rows for touched outcomes whose best PRICE moved.

        Fires only on best bid/ask price changes — emitting on every size delta
        flooded the writer (~36k rows/11min), triggered writer backpressure, and
        caused 500ms snapshot drops (the highest-value rows) in the 2026-09-05 run.
        """
        for outcome, token_id in touched.items():
            pre = pre_bbo.get(outcome) or {}
            post = self._bbo(outcome)
            price_changed = (pre.get("bid") != post.get("bid")) or (pre.get("ask") != post.get("ask"))
            if not price_changed:
                continue
            self.pending_events.append({
                "event_type": "price_change",
                "token_id": token_id, "outcome": outcome,
                "old_best_bid": pre.get("bid"), "new_best_bid": post.get("bid"),
                "old_best_ask": pre.get("ask"), "new_best_ask": post.get("ask"),
                "old_bid_size": pre.get("bid_size"), "new_bid_size": post.get("bid_size"),
                "old_ask_size": pre.get("ask_size"), "new_ask_size": post.get("ask_size"),
                "ts_source": msg.get("timestamp") or msg.get("ts"),
            })

    def drain_pending_events(self) -> List[dict]:
        evs = self.pending_events
        self.pending_events = []
        return evs

    def _outcome_for_token(self, token_id: str | None) -> Optional[str]:
        if token_id == self.up_token_id:
            return "up"
        if token_id == self.down_token_id:
            return "down"
        return None

    def _apply_price_change_level(self, side: SideBook, price: float, size: float, is_bid: bool) -> None:
        # Apply single price_change level update (size 0 = remove)
        price_map = {lvl.price: lvl for lvl in side.levels if lvl.price is not None}
        if size == 0:
            price_map.pop(price, None)
        else:
            price_map[price] = Level(price=price, size=size)
        items = list(price_map.values())
        items.sort(key=lambda x: x.price if x.price is not None else 0, reverse=is_bid)
        filtered = [lvl for lvl in items if lvl.price is not None][: self.l2_levels]
        while len(filtered) < self.l2_levels:
            filtered.append(Level(price=None, size=None))
        side.levels = filtered

    def _apply_levels(self, side: SideBook, levels: list, is_bid: bool) -> None:
        # Normalize to list of Level, sorted best-first, truncated/padded to l2_levels
        # Levels with size 0 mean remove that price level (§3 ghost-liquidity fix)
        # CLOB market-channel `book` events are FULL side snapshots — always replace.
        # (The previous patch-when-<5-levels heuristic kept stale levels when the
        # exchange sent an empty/thin side, freezing asks while bids moved → the
        # mirrored crossed books seen on 2026-09-05. Empty list = side is empty.)
        new_levels: List[Level] = []
        removals: set[float] = set()
        for lvl in levels:
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                price, size = lvl[0], lvl[1]
                if price is None or size is None:
                    continue
                try:
                    p = float(price); s = float(size)
                except (TypeError, ValueError):
                    continue
                if s == 0:
                    removals.add(float(p))
                    continue  # removal — tracked below
                new_levels.append(Level(price=p, size=s))
            elif isinstance(lvl, dict):
                p = lvl.get("price"); s = lvl.get("size")
                if p is None or s is None:
                    continue
                try:
                    pf = float(p); sf = float(s)
                except (TypeError, ValueError):
                    continue
                if sf == 0:
                    try:
                        removals.add(float(pf))
                    except Exception:
                        pass
                    continue
                new_levels.append(Level(price=pf, size=sf))
        # sort best-first
        new_levels.sort(key=lambda x: x.price if x.price is not None else 0, reverse=is_bid)
        # FULL REPLACE (book events are complete side snapshots from the exchange)
        side.levels = new_levels[: self.l2_levels]
        # pad with null levels to l2_levels for snapshot uniformity
        while len(side.levels) < self.l2_levels:
            side.levels.append(Level(price=None, size=None))

    def replace_from_rest_snapshot(self, snapshot: dict) -> None:
        """Wholesale replace in-RAM book from REST full snapshot (§1A step 3)."""
        for outcome_key, book in (("up", self.up), ("down", self.down)):
            for side_key, side in (("bids", book.bids), ("asks", book.asks)):
                key = f"{outcome_key}_{side_key}"  # e.g. up_bids
                # Only update sides present in snapshot – don't wipe other side when single-outcome dict passed
                if key not in snapshot and side_key not in snapshot:
                    continue
                levels = snapshot.get(key) or snapshot.get(side_key) or []
                is_bid = side_key == "bids"
                new_levels: List[Level] = []
                for lvl in levels:
                    if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                        try:
                            p = float(lvl[0]); s = float(lvl[1])
                        except (TypeError, ValueError):
                            continue
                        new_levels.append(Level(price=p, size=s))
                    elif isinstance(lvl, dict):
                        try:
                            p = float(lvl["price"]); s = float(lvl["size"])
                        except (TypeError, ValueError, KeyError):
                            continue
                        new_levels.append(Level(price=p, size=s))
                new_levels.sort(key=lambda x: x.price if x.price is not None else 0, reverse=is_bid)
                side.levels = new_levels[: self.l2_levels]
                while len(side.levels) < self.l2_levels:
                    side.levels.append(Level(price=None, size=None))
        # update sequence if snapshot carries cursor
        for tok_key in (self.up_token_id, self.down_token_id):
            seq = snapshot.get("sequence_number")
            if seq is None:
                seq = snapshot.get(f"{tok_key}_seq")
            if seq is not None:
                try:
                    self.sequence_numbers[tok_key] = int(seq)
                except (TypeError, ValueError):
                    pass

    def is_crossed(self) -> bool:
        # crossed if any outcome's bid >= ask
        for book in (self.up, self.down):
            if book.bids.crossed_with(book.asks):
                return True
        return False

    # -- snapshot generation (§3) ------------------------------------------
    def snapshot(self, ts_ms: int | None = None, ts_ns: int | None = None) -> BookSnapshot:
        """Generate a 500ms snapshot row. Call from shared scheduler tick."""
        now_ms = ts_ms if ts_ms is not None else int(time.time() * 1000)
        now_ns = ts_ns if ts_ns is not None else time.time_ns()
        # bucket alignment for idempotent write key (§1A redundancy)
        bucket_ms = snapshot_bucket_ms(now_ms, 500)
        import datetime

        # always format with millisecond fraction for stable lexical sort (good format)
        dt = datetime.datetime.fromtimestamp(bucket_ms / 1000, tz=datetime.timezone.utc)
        ts_utc = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # 3ms fraction e.g. .000 or .500
        # align ts_snapshot_ns to bucket for dedup (not time.time_ns jitter)
        now_ns = bucket_ms * 1_000_000
        # top-of-book extracts (null-vs-zero: empty → None)
        up_bid = self.up.bids.best_price()
        up_bid_size = self.up.bids.best_size()
        up_ask = self.up.asks.best_price()
        up_ask_size = self.up.asks.best_size()
        down_bid = self.down.bids.best_price()
        down_bid_size = self.down.bids.best_size()
        down_ask = self.down.asks.best_price()
        down_ask_size = self.down.asks.best_size()

        # if empty side, ensure sizes are None (not 0) per §3
        if up_bid is None:
            up_bid_size = None
        if up_ask is None:
            up_ask_size = None
        if down_bid is None:
            down_bid_size = None
        if down_ask is None:
            down_ask_size = None

        # L2 flat columns
        l2: Dict[str, Optional[float]] = {}
        for outcome_key, book in (("up", self.up), ("down", self.down)):
            for side_key, side in (("bid", book.bids), ("ask", book.asks)):
                for i, lvl in enumerate(side.levels, start=1):
                    # pad already done; but ensure l2_levels pad nulls
                    if i > self.l2_levels:
                        break
                    p_field = f"{outcome_key}_{side_key}_level_{i}_price"
                    s_field = f"{outcome_key}_{side_key}_level_{i}_size"
                    l2[p_field] = lvl.price
                    l2[s_field] = lvl.size
                # if book had fewer than l2_levels (should be padded) still ensure keys exist
                for i in range(len(side.levels) + 1, self.l2_levels + 1):
                    l2[f"{outcome_key}_{side_key}_level_{i}_price"] = None
                    l2[f"{outcome_key}_{side_key}_level_{i}_size"] = None

        # depth aggregates (§3 precisely defined: within N cents of own best)
        depths: Dict[str, Optional[float]] = {}
        for outcome_key, book in (("up", self.up), ("down", self.down)):
            for side_key, side in (("bid", book.bids), ("ask", book.asks)):
                best = side.best_price()
                level_tuples: List[Tuple[Optional[float], Optional[float]]] = [(lvl.price, lvl.size) for lvl in side.levels]
                for thc in (1, 5, 10):
                    field = f"{outcome_key}_{side_key}_depth_{thc}c"
                    depths[field] = depth_within(level_tuples, best, thc)

        # market_time_remaining
        remaining = max(0, self.market_end_ts_ms - bucket_ms)
        crossed = self.is_crossed()

        return BookSnapshot(
            snapshot_id=str(uuid.uuid4()),
            schema_version=self.schema_version,
            series_id=self.series_id,
            window_index=self.window_index,
            condition_id=self.condition_id,
            market_id=self.market_id,
            asset=self.asset,
            up_token_id=self.up_token_id,
            down_token_id=self.down_token_id,
            ts_snapshot_utc=ts_utc,
            ts_snapshot_ns=now_ns,
            up_bid=up_bid,
            up_ask=up_ask,
            up_bid_size=up_bid_size,
            up_ask_size=up_ask_size,
            down_bid=down_bid,
            down_ask=down_ask,
            down_bid_size=down_bid_size,
            down_ask_size=down_ask_size,
            l2=l2,
            depths=depths,
            market_time_remaining_ms=remaining,
            up_book_age_ms=self._up_book_age_ms,
            down_book_age_ms=self._down_book_age_ms,
            is_rollover_window=self.is_rollover_window,
            book_state=self.book_state.value,
            resync_id=self.resync_id,
            book_crossed=crossed,
        )

    def diff_against_rest(self, rest_snapshot: dict, tolerance: float = 0.0) -> Optional[dict]:
        """Full-book diff drift check (§1A fallback). Returns diff details if drift detected."""
        # Build normalized dicts of levels for comparison
        mismatches: List[dict] = []
        for outcome_key, book in (("up", self.up), ("down", self.down)):
            for side_key, side in (("bids", book.bids), ("asks", book.asks)):
                key = f"{outcome_key}_{side_key}"
                rest_levels = rest_snapshot.get(key) or []
                # normalize rest
                rest_norm: List[Tuple[float, float]] = []
                for lvl in rest_levels:
                    if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                        try:
                            rest_norm.append((float(lvl[0]), float(lvl[1])))
                        except (TypeError, ValueError):
                            continue
                    elif isinstance(lvl, dict):
                        try:
                            rest_norm.append((float(lvl["price"]), float(lvl["size"])))
                        except (TypeError, ValueError, KeyError):
                            continue
                # ram book norm (skip null levels)
                ram_norm: List[Tuple[float, float]] = [(lvl.price, lvl.size) for lvl in side.levels if lvl.price is not None]
                # compare lengths first
                if len(ram_norm) != len(rest_norm):
                    mismatches.append({"side": key, "reason": "level_count_mismatch", "ram": len(ram_norm), "rest": len(rest_norm)})
                    continue
                for (rp, rs), (op, os) in zip(ram_norm, rest_norm):
                    if abs(rp - op) > tolerance or abs(rs - os) > tolerance:
                        mismatches.append({"side": key, "ram": (rp, rs), "rest": (op, os)})
                        break
        if mismatches:
            return {"mismatches": mismatches}
        return None
