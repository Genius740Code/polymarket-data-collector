"""Dual-WS pool helpers (perfect-collector checkbox 2, spec §3.2).

Pure logic + unit-testable dedup — no sockets here, no network in tests.
The asyncio transport lives in ``collector.py``; this module only builds
payloads, tracks per-shard subscriptions, dedupes redelivered frames across
the A/B pair, and answers recycle/silence-watchdog questions.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Proactive recycle: preempt the ~5min server-side kill on OUR schedule.
# Max 280s (never past the server kill — an OUR-close recycle is a light
# relive, a server kill is an unplanned disconnect + REST storm).
RECYCLE_TARGET_S = 270
RECYCLE_MAX_S = 280

# Per-book silence watchdog: a token with no frame this long is presumed
# data-dead (the shard-level socket can stay heartbeat-alive while market
# data silently dies — py-clob-client#292).
SILENCE_WATCHDOG_S = 120


def _dedupe_tokens(tokens: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for t in tokens or []:
        try:
            s = str(t)
        except Exception:
            continue
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def build_initial_subscribe(tokens: List[str]) -> Dict[str, Any]:
    """Shared subscribe for a fresh connection: ``{"assets_ids": tokens, "type": "market"}``."""
    return {"assets_ids": _dedupe_tokens(tokens), "type": "market"}


def build_hot_add(tokens: List[str]) -> Dict[str, Any]:
    """Hot-add on an established connection: ``{"operation": "subscribe"}`` variant."""
    return {
        "assets_ids": _dedupe_tokens(tokens),
        "operation": "subscribe",
        "type": "market",
        "custom_feature_enabled": True,
    }


def _norm_seq(v: Any) -> Optional[Any]:
    if v is None or v == "":
        return None
    try:
        if isinstance(v, bool):
            return None
        if isinstance(v, int):
            return v
        s = str(v).strip()
        if not s:
            return None
        try:
            return int(s)
        except Exception:
            try:
                return int(float(s))
            except Exception:
                return s
    except Exception:
        return None


def _norm_ts(v: Any) -> Optional[int]:
    if v is None or v == "" or isinstance(v, bool):
        return None
    try:
        if isinstance(v, (int, float)):
            f = float(v)
            return int(f if f > 1e11 else f * 1000)
        s = str(v).strip()
        if not s:
            return None
        try:
            f = float(s)
            return int(f if f > 1e11 else f * 1000)
        except Exception:
            pass
        try:
            import datetime as _dt

            dt = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    except Exception:
        return None


def _frame_seq_ts(msg: Dict[str, Any]) -> Tuple[Optional[Any], Optional[int]]:
    seq = None
    for k in ("sequence_number", "seq", "sequence"):
        try:
            v = msg.get(k)
        except Exception:
            continue
        seq = _norm_seq(v)
        if seq is not None:
            break
    ts = None
    for k in ("timestamp", "ts", "ts_source"):
        try:
            v = msg.get(k)
        except Exception:
            continue
        ts = _norm_ts(v)
        if ts is not None:
            break
    return seq, ts


def dedup_keys_for_message(msg: Dict[str, Any]) -> List[Optional[Tuple]]:
    """One dedup key per token carried by a frame (pure).

    Multi-entry ``price_changes`` frames fan out to one key per entry token
    (rollover dual-tracking: one frame can carry several markets). Entries
    with no token yield None (never dedupe on nothing).
    """
    keys: List[Optional[Tuple]] = []
    try:
        pcs = msg.get("price_changes")
    except Exception:
        pcs = None
    if isinstance(pcs, list):
        top_seq, top_ts = _frame_seq_ts(msg if isinstance(msg, dict) else {})
        for pc in pcs:
            if not isinstance(pc, dict):
                keys.append(None)
                continue
            tok = pc.get("asset_id") or pc.get("token_id") or pc.get("asset")
            tok = str(tok) if tok else None
            seq, ts = top_seq, top_ts
            for k in ("sequence_number", "seq", "sequence"):
                s = _norm_seq(pc.get(k))
                if s is not None:
                    seq = s
                    break
            for k in ("timestamp", "ts", "ts_source"):
                t = _norm_ts(pc.get(k))
                if t is not None:
                    ts = t
                    break
            keys.append(_dedup_key(tok, seq, ts))
        return keys
    tok = None
    try:
        for k in ("token_id", "asset_id", "asset", "token"):
            v = msg.get(k)
            if v:
                tok = str(v)
                break
    except Exception:
        tok = None
    seq, ts = _frame_seq_ts(msg)
    return [_dedup_key(tok, seq, ts)]


def _dedup_key(token: Optional[str], seq: Optional[Any], ts: Optional[int]) -> Optional[Tuple]:
    if not token:
        return None
    if seq is not None:
        return (token, seq)
    if ts is not None:
        return (token, ts)
    return None


class FrameDedup:
    """Redelivery dedup across the A/B pair, keyed (token, seq/ts).

    Both conns carry the same frames; the second delivery must not double
    downstream work. Frames with neither seq nor ts have no meaningful key
    and are never deduped (stored/delivered) rather than risk false-duping
    distinct events. Bounded FIFO — a redelivery older than the window is
    re-accepted (dupe bloat, tolerated downstream), never OOM.
    """

    def __init__(self, max_keys: int = 100_000):
        self.max_keys = max(1, int(max_keys))
        self._seen: "OrderedDict[Tuple, None]" = OrderedDict()
        self.duplicates = 0
        self.accepted = 0

    def __len__(self) -> int:
        return len(self._seen)

    def check(self, token: Optional[str], seq: Any = None, ts: Any = None) -> bool:
        """True when this (token, seq/ts) was already seen (duplicate).

        Records unseen keys. A key of None (no token, or neither seq nor ts)
        always returns False and stores nothing.
        """
        key = _dedup_key(token, _norm_seq(seq), _norm_ts(ts))
        if key is None:
            self.accepted += 1
            return False
        if key in self._seen:
            self.duplicates += 1
            return True
        self._seen[key] = None
        if len(self._seen) > self.max_keys:
            try:
                self._seen.popitem(last=False)
            except Exception:
                pass
        self.accepted += 1
        return False

    def check_message(self, msg: Dict[str, Any]) -> bool:
        """True when EVERY token key in the frame is a duplicate (whole-frame redelivery).

        Partially-new frames return False and record the new keys, so the
        next identical delivery dedupes. Frames with no usable key return
        False (deliver, never drop on nothing).
        """
        if not isinstance(msg, dict):
            return False
        keys = [k for k in dedup_keys_for_message(msg) if k is not None]
        if not keys:
            self.accepted += 1
            return False
        if all(k in self._seen for k in keys):
            self.duplicates += 1
            return True
        for k in keys:
            if k not in self._seen:
                try:
                    self._seen[k] = None
                except Exception:
                    pass
        while len(self._seen) > self.max_keys:
            try:
                self._seen.popitem(last=False)
            except Exception:
                break
        self.accepted += 1
        return False


def should_recycle(conn_age_s: float, max_s: float = RECYCLE_MAX_S) -> bool:
    """True when a connection lived past the recycle ceiling (OUR-close relive)."""
    try:
        return float(conn_age_s) >= float(max_s)
    except Exception:
        return False


def silence_exceeded(
    last_data_ns: Optional[int], now_ns: int, timeout_s: float = SILENCE_WATCHDOG_S
) -> bool:
    """True when no data frame arrived within the silence watchdog window."""
    if last_data_ns is None:
        return False
    try:
        return (int(now_ns) - int(last_data_ns)) > float(timeout_s) * 1e9
    except Exception:
        return False


class ShardSubscriptions:
    """Tracks which tokens are already subscribed on a live connection (pure).

    The first payload is the shared subscribe; later additions hot-add only
    the delta via ``{"operation": "subscribe"}``. Safe to drive while the
    socket is down (payloads are just built, never sent here).
    """

    def __init__(self):
        self._subscribed: Set[str] = set()
        self.subscribed_once = False

    @property
    def subscribed_tokens(self) -> Set[str]:
        return set(self._subscribed)

    def initial_payload(self, tokens: List[str]) -> Dict[str, Any]:
        toks = _dedupe_tokens(tokens)
        self._subscribed.update(toks)
        self.subscribed_once = True
        return {"assets_ids": toks, "type": "market"}

    def hot_add_payload(self, tokens: List[str]) -> Optional[Dict[str, Any]]:
        new = [t for t in _dedupe_tokens(tokens) if t not in self._subscribed]
        if not new:
            return None
        self._subscribed.update(new)
        self.subscribed_once = True
        return build_hot_add(new)

    def mark_subscribed(self, tokens: List[str]) -> None:
        self._subscribed.update(_dedupe_tokens(tokens))
        if self._subscribed:
            self.subscribed_once = True

    def reset(self) -> None:
        """Fresh connection: subscribe state restarts (server holds none)."""
        self._subscribed.clear()
        self.subscribed_once = False


@dataclass
class ConnectionState:
    """Per-conn (A/B) liveness clocks for one shard socket (pure, ns clocks)."""

    name: str
    established_ns: int = 0
    last_data_ns: Optional[int] = None

    def note_frame(self, now_ns: int) -> None:
        self.last_data_ns = int(now_ns)

    def age_s(self, now_ns: int) -> float:
        try:
            return max(0.0, (int(now_ns) - int(self.established_ns)) / 1e9)
        except Exception:
            return 0.0

    def needs_recycle(self, now_ns: int, max_s: float = RECYCLE_MAX_S) -> bool:
        return should_recycle(self.age_s(now_ns), max_s)

    def is_silent(self, now_ns: int, timeout_s: float = SILENCE_WATCHDOG_S) -> bool:
        return silence_exceeded(self.last_data_ns, now_ns, timeout_s)


@dataclass
class ShardPool:
    """Two-connection (A/B) bookkeeping for one shard (pure state only).

    Transports attach outside; the pool answers which conn needs work.
    """

    shard: List[str] = field(default_factory=list)
    conn_a: ConnectionState = field(default_factory=lambda: ConnectionState(name="A"))
    conn_b: ConnectionState = field(default_factory=lambda: ConnectionState(name="B"))
    subs: ShardSubscriptions = field(default_factory=ShardSubscriptions)
    dedup: FrameDedup = field(default_factory=FrameDedup)

    def conns_needing_recycle(self, now_ns: int) -> List[str]:
        out = []
        for c in (self.conn_a, self.conn_b):
            try:
                if c.needs_recycle(now_ns):
                    out.append(c.name)
            except Exception:
                continue
        return out

    def conns_gone_silent(self, now_ns: int) -> List[str]:
        out = []
        for c in (self.conn_a, self.conn_b):
            try:
                if c.is_silent(now_ns):
                    out.append(c.name)
            except Exception:
                continue
        return out
