"""Order book state — §3 book_snapshots_500ms.

One BookState per (asset, condition_id). Implements:
- null-vs-zero uniformly (empty side → None, never 0) — §3
- depth aggregates depth_1c/5c/10c precisely defined — §3
- book_crossed flag
- sanitize-bounds gate before apply (§3A)
- 500ms snapshot generation aligned to wall-clock grid (§1A/§3)
"""
from __future__ import annotations

import datetime
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .enums import BookState
from .validation import coerce_ts_source_ms, validate_ws_message


# -- helpers ---------------------------------------------------------------

def snapshot_bucket_ms(unix_ms: int, interval_ms: int = 500) -> int:
    """Shared wall-clock grid: floor(unix_ms/interval)*interval, UTC epoch aligned (§1A/§3)."""
    return (unix_ms // interval_ms) * interval_ms


# PERF 2026-09-12 (#19): bucket_ms -> ISO string cache. snapshot() runs per
# book per tick (28 books × 2Hz); the bucket string is identical for every
# book in the same tick, so format once and reuse. snapshot_id stays uuid4
# (must remain unique per row — never cached).
_BUCKET_TS_CACHE: Dict[int, str] = {}


def bucket_ts_utc(bucket_ms: int) -> str:
    """ISO8601 ms-fraction UTC string for a 500ms bucket, cached per bucket."""
    cached = _BUCKET_TS_CACHE.get(bucket_ms)
    if cached is not None:
        return cached
    import datetime
    dt = datetime.datetime.fromtimestamp(bucket_ms / 1000, tz=datetime.timezone.utc)
    ts_utc = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    # Bounded: buckets advance monotonically; keep only the latest few so a
    # long-lived process cannot grow this dict (2 entries cover a tick edge).
    _BUCKET_TS_CACHE.clear()
    _BUCKET_TS_CACHE[bucket_ms] = ts_utc
    return ts_utc


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


def depth_all(levels: List[Tuple[Optional[float], Optional[float]]], best: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Single-pass 1c/5c/10c depths — bit-identical to 3x depth_within calls.

    Same None-vs-0, same 1e-9 tolerance, same best-first break assumption.
    Levels sorted best-first ⇒ distance non-decreasing, so a prefix sum per
    cutoff equals the per-cutoff loop with its own break.
    """
    if best is None:
        return (None, None, None)
    t1 = 0.0
    t5 = 0.0
    t10 = 0.0
    for price, size in levels:
        if price is None or size is None:
            continue
        if abs(price - best) - 1e-9 > 0.10:
            break
        s = float(size)
        d = abs(price - best)
        if d - 1e-9 <= 0.01:
            t1 += s
        if d - 1e-9 <= 0.05:
            t5 += s
        t10 += s
    return (t1, t5, t10)


def depth_all_levels(levels: List["Level"], best: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Single-pass depths directly over Level objects — identical to depth_all.

    Avoids per-snapshot [(lvl.price, lvl.size)] tuple-list allocs (28 books x
    2Hz). Same None-skip, same 1e-9 tolerance/break, same float(size) sum.
    """
    if best is None:
        return (None, None, None)
    t1 = 0.0
    t5 = 0.0
    t10 = 0.0
    for lvl in levels:
        price = lvl.price
        size = lvl.size
        if price is None or size is None:
            continue
        if abs(price - best) - 1e-9 > 0.10:
            break
        s = float(size)
        d = abs(price - best)
        if d - 1e-9 <= 0.01:
            t1 += s
        if d - 1e-9 <= 0.05:
            t5 += s
        t10 += s
    return (t1, t5, t10)


def _nz(v: Optional[float]) -> Optional[float]:
    """Null-vs-zero: None or 0/0.0 -> None, else v (§3 E4). Module-level to avoid per-snapshot closure alloc."""
    return None if v is None or v == 0 else v


@dataclass(slots=True)
class Level:
    price: Optional[float]  # None → null (absent level)
    size: Optional[float]

    def is_null(self) -> bool:
        return self.price is None


@dataclass(slots=True)
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

    def best_top(self) -> Tuple[Optional[float], Optional[float]]:
        """Single-scan (price, size) — identical to (best_price(), best_size())."""
        for lvl in self.levels:
            if lvl.price is not None:
                return (lvl.price, lvl.size)
        return (None, None)

    def is_empty(self) -> bool:
        for lvl in self.levels:
            if lvl.price is not None:
                return False
        return True

    def crossed_with(self, other: "SideBook") -> bool:
        """True if this bid side crossed with other ask side — strictly b > a (equal is not crossed)."""
        b, _ = self.best_top()
        a, _ = other.best_top()
        if b is None or a is None:
            return False
        # 1e-9 tolerance for float equality; crossed only if bid strictly greater than ask
        return (b - a) > 1e-9


@dataclass(slots=True)
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
    market_id: Optional[str]
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
    # A4 integrity attestation from the last accepted frame per outcome (None = not yet seen)
    up_book_hash: Optional[str] = None
    down_book_hash: Optional[str] = None

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
            "up_book_hash": self.up_book_hash,
            "down_book_hash": self.down_book_hash,
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
        market_id: Optional[str],
        series_id: str,
        window_index: int,
        up_token_id: str,
        down_token_id: str,
        market_end_ts_ms: int,
        schema_version: str = "3.0.0",
        l2_levels: int = 10,
    ):
        self.asset = asset
        self.condition_id = condition_id
        # E1: never store a hex condition_id as market_id — NULL is honest.
        try:
            from .rollover import clean_market_id as _clean_mid
            self.market_id = _clean_mid(market_id)
        except Exception:
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
        # E6: per-outcome last-exchange-update clock — snapshot() derives
        # book_age_ms as now - last update (was: always 0, never aged).
        self._up_last_update_ns: Optional[int] = None
        self._down_last_update_ns: Optional[int] = None
        self.is_rollover_window: bool = False
        self.sequence_numbers: Dict[str, int] = {}  # token_id -> last seq
        self._stale_since_ms: Optional[int] = None
        # §4 book_events: old/new BBO captured per applied WS message, drained by the collector
        self.pending_events: List[dict] = []
        # A1: last exchange frame timestamp seen on THIS connection (ms epoch).
        # Used ONLY as a carry-forward source-time fallback for frames that
        # genuinely lack a top-level timestamp (batched deltas share a clock).
        # Never stamped from receive-time: a missing value stays NULL.
        self._last_frame_ts_ms: Optional[int] = None
        self._last_frame_rx_ms: Optional[int] = None
        # A4: exchange book-integrity hashes, per outcome (latest seen).
        # `book` frames carry a top-level `hash` (hash of the orderbook
        # content); `price_change` entries each carry a per-order `hash`.
        # Both are REQUIRED per the AsyncAPI spec and were 100% present in
        # the 2026-09-05 live probe — raw frames are also in raw_ws_archive.
        self.book_hash: Dict[str, Optional[str]] = {"up": None, "down": None}
        self.event_thresholds: Dict[str, float] = {
            "spread_change_threshold": 0.002,
            "size_change_threshold_pct": 0.10,
        }

    # -- A1 frame-timestamp helpers --------------------------------------
    def heal_market_id(self, market_id: Optional[str]) -> bool:
        """Fill a missing market_id from later discovery (audit 2026-09-16 M1).

        Long-lived books (esp. *-1d, resurrected from cursor with
        market_id=None) froze NULL forever because snapshot rows come from
        the book, not the market — even after markets_latest learned the
        numeric id. Only fills when currently empty; never overwrites a
        known id and never stores hex (E1). Returns True if filled.
        """
        if self.market_id:
            return False
        try:
            from .rollover import clean_market_id as _clean_mid
            cleaned = _clean_mid(market_id)
        except Exception:
            cleaned = None
        if cleaned:
            self.market_id = cleaned
            return True
        return False

    def heal_tokens(self, up_token_id: Optional[str], down_token_id: Optional[str]) -> bool:
        """Replace placeholder/wrong token IDs from later discovery (audit 2026-09-18 C1).

        Cursor recovery (`collector._recover_from_cursor`) creates books with
        synthetic `"<cid>-UP"/"<cid>-DOWN"` token IDs. Real WS frames carry the
        numeric CLOB token IDs, so without healing those frames never route to
        the book (silent delta loss) and snapshots ship fabricated token IDs.
        Only replaces when the incoming IDs differ and look real (non-empty,
        not `<cid>-UP` style placeholders); never blanks a known good pair.
        Returns True if replaced.
        """
        try:
            new_up = str(up_token_id).strip() if up_token_id else ""
            new_down = str(down_token_id).strip() if down_token_id else ""
        except Exception:
            return False
        if not new_up or not new_down:
            return False
        try:
            cur_up = str(getattr(self, "up_token_id", "") or "")
            cur_down = str(getattr(self, "down_token_id", "") or "")
        except Exception:
            return False
        if cur_up == new_up and cur_down == new_down:
            return False

        def _placeholder(tok: str) -> bool:
            try:
                return tok.endswith("-UP") or tok.endswith("-DOWN")
            except Exception:
                return False

        # Heal placeholders always; heal mismatched real IDs too (token reuse
        # across windows is impossible — token IDs are unique per market — so a
        # mismatch means this book holds the wrong pair).
        if _placeholder(cur_up) or _placeholder(cur_down) or cur_up != new_up or cur_down != new_down:
            self.up_token_id = new_up
            self.down_token_id = new_down
            # sequence state keyed by old tokens is meaningless for the new pair
            try:
                self.sequence_numbers.pop(cur_up, None)
                self.sequence_numbers.pop(cur_down, None)
            except Exception:
                pass
            return True
        return False

    @staticmethod
    def _parse_frame_ts_ms(msg: dict) -> Optional[int]:
        """Extract the exchange frame timestamp as ms epoch, or None.

        Live probe (2026-09-05, 37k frames): top-level `timestamp` is always
        present (ms-epoch string); `ts` never appears (kept as dead fallback).
        """
        raw = msg.get("timestamp")
        if raw is None:
            raw = msg.get("ts")
        if raw is None or raw == "":
            return None
        try:
            v = int(str(raw).strip())
        except (TypeError, ValueError):
            try:
                v = int(float(str(raw).strip()))
            except (TypeError, ValueError):
                return None
        if v < 10**12:
            v *= 1000  # tolerate seconds-epoch senders
        return v

    def _note_frame_ts(self, msg: dict) -> None:
        """Record this frame's exchange timestamp for carry-forward."""
        ts = self._parse_frame_ts_ms(msg)
        if ts is not None:
            self._last_frame_ts_ms = ts
            self._last_frame_rx_ms = int(time.time() * 1000)

    def _resolve_ts_source(self, msg: dict) -> Optional[int]:
        """Source timestamp for an emitted book_event (int ms epoch).

        Preference: the frame's own top-level timestamp → carry-forward of
        the previous frame's timestamp from the SAME connection when fresh
        (received within ~1.5s; batched deltas share a clock) → NULL.
        Receive-time is used only as a freshness gate, never as the value.
        """
        raw = msg.get("timestamp")
        if raw is None:
            raw = msg.get("ts")
        coerced = coerce_ts_source_ms(raw)
        if coerced is not None:
            return coerced
        if self._last_frame_ts_ms is not None and self._last_frame_rx_ms is not None:
            try:
                if int(time.time() * 1000) - self._last_frame_rx_ms <= 1500:
                    return int(self._last_frame_ts_ms)
            except Exception:
                pass
        return None

    # -- A4 book-hash integrity primitive ----------------------------------
    @staticmethod
    def _well_formed_hash(h) -> bool:
        """A usable exchange integrity attestation: non-empty string, hash-like."""
        return isinstance(h, str) and len(h.strip()) >= 8

    def _note_frame_hash(self, msg: dict, outcome: Optional[str],
                         entry_hash: object = None) -> None:
        """Capture the exchange hash for an outcome (top-level or per-entry)."""
        if outcome not in ("up", "down"):
            return
        h = msg.get("hash") if isinstance(msg, dict) else None
        if not self._well_formed_hash(h):
            h = entry_hash
        if self._well_formed_hash(h):
            self.book_hash[outcome] = str(h).strip()

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

        # A1: record the frame's exchange timestamp (carry-forward source)
        self._note_frame_ts(msg)
        # A4: non-fatal integrity note (e.g. hash-gated promotion refusal).
        # Returned as the reason with applied=True so the collector logs it
        # as book_anomaly telemetry without triggering a resync.
        promo_note: Optional[str] = None

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
                        "price": p, "size": s, "side": side.lower(),
                        "ts_source": self._resolve_ts_source(msg),
                    })
                touched.setdefault(pc_outcome, pc_token)
                # A4: capture the per-order exchange hash for this outcome
                self._note_frame_hash(msg, pc_outcome, pc.get("hash"))
                # exchange-reported authoritative BBO for this token after the change
                try:
                    bb_raw = pc.get("best_bid")
                    ba_raw = pc.get("best_ask")
                    bb = float(bb_raw) if bb_raw is not None and bb_raw != "" else None
                    ba = float(ba_raw) if ba_raw is not None and ba_raw != "" else None
                    if bb is not None or ba is not None:
                        ex_bbo[pc_outcome] = {"bid": bb, "ask": ba}
                except Exception:
                    pass
            # enforce BBO against the exchange's own best_bid/best_ask — heals
            # stale ask/bid sides and guarantees the top-of-book is never crossed
            for outcome, ex in ex_bbo.items():
                self._enforce_bbo(outcome, ex, msg)
            # update age
            self._last_update_ns = time.time_ns()
            # E6: stamp per-outcome update clocks for price_change touches.
            for _toc in touched:
                if _toc == "up":
                    self._up_last_update_ns = self._last_update_ns
                elif _toc == "down":
                    self._down_last_update_ns = self._last_update_ns
            # E6: per-outcome update clocks stamped above; snapshot() ages from them.
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
                self._up_last_update_ns = self._last_update_ns
            else:
                self._down_book_age_ms = 0
                self._down_last_update_ns = self._last_update_ns
            touched.setdefault(outcome, token_id)
            # A4: capture the full-book hash (hash of the orderbook content)
            self._note_frame_hash(msg, outcome)
            # A full `book` snapshot is a complete exchange-side state — as trustworthy
            # as a REST fetch. Promote a stale/resyncing book to live when the snapshot
            # fills both sides (was the 55s cold-start and post-resync stale blocks).
            # A4: promotion is hash-gated — the exchange attests every `book` frame
            # (AsyncAPI REQUIRED, 100% present in the 2026-09-05 probe). A snapshot
            # without a well-formed hash is still APPLIED (levels are real data)
            # but must not promote: the book stays stale and the REST-heal path
            # covers it. The refusal is returned as the reason so the collector
            # emits a book_anomaly (no resync storm — content was applied).
            if self.book_state != BookState.live:
                if book.bids.best_top()[0] is not None and book.asks.best_top()[0] is not None:
                    if self._well_formed_hash(msg.get("hash")):
                        self.mark_live()
                    else:
                        promo_note = (f"book_hash_missing_on_promotion outcome={outcome} "
                                      f"hash={msg.get('hash')!r} — levels applied, promotion refused; REST heal covers")

        self._emit_bbo_events(pre_bbo, touched, msg)
        return True, promo_note

    # -- BBO capture for §4 book_events ------------------------------------
    def _bbo(self, outcome: str) -> Dict[str, Optional[float]]:
        book = self.up if outcome == "up" else self.down
        bid, bid_size = book.bids.best_top()
        ask, ask_size = book.asks.best_top()
        return {
            "bid": bid, "bid_size": bid_size,
            "ask": ask, "ask_size": ask_size,
        }

    def _enforce_bbo(self, outcome: str, ex: Dict[str, Optional[float]], msg: dict | None = None, tick: float = 0.0101) -> None:
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
                    "ts_source": self._resolve_ts_source(msg) if isinstance(msg, dict) else None,
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
                "ts_source": self._resolve_ts_source(msg),
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
        # Apply single price_change level update (size 0 = remove).
        # E4: price 0 is an empty-side sentinel, never a resting quote — drop it
        # like a removal so 0.0 never ships as a BBO/L1 price (null-vs-zero).
        # PERF: in-place insert/update + insertion position for n<=10.
        # Bit-identical to the old dict+sort+pad: exact-float key match (NOT
        # 1e-9 tolerance — verified 0.1+0.2 != 0.3 stays), same 0-sentinel,
        # same best-first order, same truncate/pad to l2_levels.
        # Safety fallback: if the side somehow holds duplicate prices or is
        # unsorted (never from our writers, only hand-fed tests), use the old
        # dict+sort path once so the result still matches exactly.
        try:
            _reals = [lvl.price for lvl in side.levels if lvl.price is not None]
            if len(_reals) != len(set(_reals)):
                raise ValueError("dupes")
            # sorted check best-first (null tail ignored — pad region is None)
            _prev = None
            for _p in _reals:
                if _prev is not None:
                    if is_bid and _p > _prev + 1e-12:
                        raise ValueError("unsorted")
                    if not is_bid and _p < _prev - 1e-12:
                        raise ValueError("unsorted")
                _prev = _p
        except ValueError:
            price_map = {lvl.price: lvl for lvl in side.levels if lvl.price is not None}
            if size == 0 or price == 0:
                price_map.pop(price, None)
            else:
                price_map[price] = Level(price=price, size=size)
            items = list(price_map.values())
            items.sort(key=lambda x: x.price if x.price is not None else 0, reverse=is_bid)
            filtered = [lvl for lvl in items if lvl.price is not None][: self.l2_levels]
            while len(filtered) < self.l2_levels:
                filtered.append(Level(price=None, size=None))
            side.levels = filtered
            return
        if size == 0 or price == 0:
            # Removal: drop ALL exact matches (dict.pop collapsed dupes).
            # Order preserved (already sorted), then re-pad null tail.
            kept = [lvl for lvl in side.levels if not (lvl.price is not None and lvl.price == price)]
            while len(kept) < self.l2_levels:
                kept.append(Level(price=None, size=None))
            side.levels = kept[: self.l2_levels]
            while len(side.levels) < self.l2_levels:
                side.levels.append(Level(price=None, size=None))
            return
        # Update in place when the exact price rests (no reorder needed).
        # Dupes impossible here (checked above), so first match == only match.
        for lvl in side.levels:
            if lvl.price is not None and lvl.price == price:
                lvl.size = size
                return
        # Insert new level best-first (bids desc, asks asc), n<=10 linear.
        new_lvl = Level(price=price, size=size)
        idx = 0
        n = len(side.levels)
        for i, lvl in enumerate(side.levels):
            if lvl.price is None:
                idx = i
                break
            if is_bid:
                if price > lvl.price:  # type: ignore[operator]
                    idx = i
                    break
            else:
                if price < lvl.price:  # type: ignore[operator]
                    idx = i
                    break
            idx = i + 1
        else:
            idx = n
        side.levels.insert(idx, new_lvl)
        del side.levels[self.l2_levels :]
        while len(side.levels) < self.l2_levels:
            side.levels.append(Level(price=None, size=None))

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
                if p == 0:
                    continue  # E4: 0-price is an empty-side sentinel, not a quote
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
                if pf == 0:
                    continue  # E4: 0-price is an empty-side sentinel, not a quote
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
        """Wholesale replace in-RAM book from REST full snapshot (§1A step 3).

        M7 (audit 2026-09-18): REST levels get the same §3A bounds discipline as
        WS frames (price in [0,1], size >= 0). Out-of-range levels are skipped
        — a malformed REST response must not poison the book and every
        subsequent snapshot. (The WS path enforces this in apply_ws_message.)"""
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
                        if p == 0:
                            continue  # E4: 0-price sentinel, not a quote
                        if not (0.0 <= p <= 1.0) or not (s >= 0):
                            continue  # M7: §3A bounds, same as WS path
                        new_levels.append(Level(price=p, size=s))
                    elif isinstance(lvl, dict):
                        try:
                            p = float(lvl["price"]); s = float(lvl["size"])
                        except (TypeError, ValueError, KeyError):
                            continue
                        if p == 0:
                            continue  # E4: 0-price sentinel, not a quote
                        if not (0.0 <= p <= 1.0) or not (s >= 0):
                            continue  # M7: §3A bounds, same as WS path
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

    def tops(self) -> Tuple[Tuple[Optional[float], Optional[float]], Tuple[Optional[float], Optional[float]], Tuple[Optional[float], Optional[float]], Tuple[Optional[float], Optional[float]], bool]:
        """Single-scan BBO + crossed for the snapshot tick heal/anomaly checks.

        Returns ((up_bid,up_bid_size),(up_ask,up_ask_size),(down_bid,down_bid_size),(down_ask,down_ask_size),crossed).
        Values are RAW best_top (no _nz); caller applies _nz/size-None exactly
        like snapshot() so heal/anomaly decisions match snapshot row values.
        Order preserved: caller must mark_stale BEFORE snapshot() for the
        crossed bucket to stay stale (gap honesty).
        """
        up_bid, up_bid_size = self.up.bids.best_top()
        up_ask, up_ask_size = self.up.asks.best_top()
        down_bid, down_bid_size = self.down.bids.best_top()
        down_ask, down_ask_size = self.down.asks.best_top()
        crossed = False
        if up_bid is not None and up_ask is not None and (up_bid - up_ask) > 1e-9:
            crossed = True
        elif down_bid is not None and down_ask is not None and (down_bid - down_ask) > 1e-9:
            crossed = True
        return ((up_bid, up_bid_size), (up_ask, up_ask_size), (down_bid, down_bid_size), (down_ask, down_ask_size), crossed)

    # -- snapshot generation (§3) ------------------------------------------
    def snapshot(self, ts_ms: int | None = None, ts_ns: int | None = None) -> BookSnapshot:
        """Generate a 500ms snapshot row. Call from shared scheduler tick."""
        now_ms = ts_ms if ts_ms is not None else int(time.time() * 1000)
        # bucket alignment for idempotent write key (§1A redundancy)
        bucket_ms = snapshot_bucket_ms(now_ms, 500)
        # PERF #19: reuse the cached bucket string (same for all books/tick).
        # 3ms fraction e.g. .000 or .500 for stable lexical sort.
        ts_utc = bucket_ts_utc(bucket_ms)
        # align ts_snapshot_ns to bucket for dedup (not time.time_ns jitter)
        now_ns = bucket_ms * 1_000_000
        # E6: age each side from its last exchange update (None = never updated).
        if self._up_last_update_ns is not None:
            self._up_book_age_ms = max(0, (now_ns - self._up_last_update_ns) // 1_000_000)
        if self._down_last_update_ns is not None:
            self._down_book_age_ms = max(0, (now_ns - self._down_last_update_ns) // 1_000_000)
        # top-of-book extracts (null-vs-zero: empty → None)
        # E4: belt-and-braces — a 0.0 best is an empty-side sentinel (ingest
        # paths above already drop 0-price levels; this covers legacy RAM).
        # PERF: single scan per side via best_top (was best_price+best_size x8
        # + depth best x4 + crossed x2 ≈ 15 rescans of the same 10-level lists).
        # _nz hoisted to module level (was per-snapshot closure).
        (up_bid_raw, up_bid_size_raw) = self.up.bids.best_top()
        (up_ask_raw, up_ask_size_raw) = self.up.asks.best_top()
        (down_bid_raw, down_bid_size_raw) = self.down.bids.best_top()
        (down_ask_raw, down_ask_size_raw) = self.down.asks.best_top()
        up_bid = _nz(up_bid_raw)
        up_bid_size = up_bid_size_raw
        up_ask = _nz(up_ask_raw)
        up_ask_size = up_ask_size_raw
        down_bid = _nz(down_bid_raw)
        down_bid_size = down_bid_size_raw
        down_ask = _nz(down_ask_raw)
        down_ask_size = down_ask_size_raw

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
                # PERF: guard avoids range+f-strings on the normal padded path.
                if len(side.levels) < self.l2_levels:
                    for i in range(len(side.levels) + 1, self.l2_levels + 1):
                        l2[f"{outcome_key}_{side_key}_level_{i}_price"] = None
                        l2[f"{outcome_key}_{side_key}_level_{i}_size"] = None

        # depth aggregates (§3 precisely defined: within N cents of own best)
        # PERF: one sorted pass per side via depth_all_levels directly over
        # Level objects (was 2 dicts + 4 tuple-lists per snapshot). Same
        # math/tolerance/break as depth_within/depth_all.
        # NOTE: raw best (pre-_nz) drives depth, matching old code which
        # used side.best_price() directly (0.0 counted, None→None).
        depths: Dict[str, Optional[float]] = {}
        # Single-pass per side with cached raws (no rescans beyond the 4 tops above).
        for outcome_key, side_key, side, best in (
            ("up", "bid", self.up.bids, up_bid_raw),
            ("up", "ask", self.up.asks, up_ask_raw),
            ("down", "bid", self.down.bids, down_bid_raw),
            ("down", "ask", self.down.asks, down_ask_raw),
        ):
            d1, d5, d10 = depth_all_levels(side.levels, best)
            depths[f"{outcome_key}_{side_key}_depth_1c"] = d1
            depths[f"{outcome_key}_{side_key}_depth_5c"] = d5
            depths[f"{outcome_key}_{side_key}_depth_10c"] = d10

        # market_time_remaining
        remaining = max(0, self.market_end_ts_ms - bucket_ms)
        # crossed from cached raws (identical to is_crossed()).
        crossed = False
        if up_bid_raw is not None and up_ask_raw is not None and (up_bid_raw - up_ask_raw) > 1e-9:
            crossed = True
        elif down_bid_raw is not None and down_ask_raw is not None and (down_bid_raw - down_ask_raw) > 1e-9:
            crossed = True

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
            up_book_hash=self.book_hash.get("up"),
            down_book_hash=self.book_hash.get("down"),
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
