"""WebSocket disconnect & resync handling — §1A (core fix).

Rule: never resume a book from stale state after disconnect.

On every disconnect: mark dirty, backoff reconnect, full REST resync before
trusting deltas, buffer-and-replay in-flight deltas, sequence-gap treated as
disconnect, full-book diff drift fallback.
"""
from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .book import OrderBookState
from .enums import BookState, CollectorEventType


@dataclass
class ResyncEpisode:
    resync_id: str
    asset: str
    condition_id: Optional[str]
    disconnect_ts_utc: str
    disconnect_reason: str
    reconnect_ts_utc: Optional[str] = None
    resync_rest_fetch_ts_utc: Optional[str] = None
    resync_completed_ts_utc: Optional[str] = None
    gap_duration_ms: Optional[int] = None
    snapshots_missed_estimate: Optional[int] = None
    resync_attempt_count: int = 0
    # H3: detection time vs last-frame time (disconnect_ts is backdated to last frame)
    detected_ts_utc: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "resync_id": self.resync_id,
            "asset": self.asset,
            "condition_id": self.condition_id,
            "disconnect_ts_utc": self.disconnect_ts_utc,
            "disconnect_reason": self.disconnect_reason,
            "reconnect_ts_utc": self.reconnect_ts_utc,
            "resync_rest_fetch_ts_utc": self.resync_rest_fetch_ts_utc,
            "resync_completed_ts_utc": self.resync_completed_ts_utc,
            "gap_duration_ms": self.gap_duration_ms,
            "snapshots_missed_estimate": self.snapshots_missed_estimate,
            "resync_attempt_count": self.resync_attempt_count,
            "detected_ts_utc": self.detected_ts_utc,
        }


def _now_iso() -> str:
    import datetime
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def exponential_backoff(attempt: int, initial_ms: int, max_ms: int, jitter: bool = True) -> float:
    """Return backoff in seconds for attempt (0-indexed)."""
    delay_ms = min(initial_ms * (2 ** attempt), max_ms)
    if jitter:
        delay_ms = delay_ms * (0.5 + random.random() * 0.5)  # 0.5x–1.0x jitter
    return delay_ms / 1000.0


class ResyncManager:
    """Manages disconnect → resync lifecycle per (asset, condition_id)."""

    # P0 leak hunt session 2 (2026-09-08): every container here must be bounded.
    # Episodes are persisted to parquet at every transition via on_episode_persist,
    # so evicting the finished ones from RAM drops no data. Open episodes are
    # never evicted — they are the replay source of truth.
    MAX_EPISODES = 500
    # Hard cap per replay buffer. Overflow drops the OLDEST buffered deltas and
    # counts them (honest accounting) — the REST snapshot supplies the base book,
    # so replay correctness is preserved for the retained tail.
    # 2026-09-24 11:20 triage: 50k->5k. Under WS churn (150s recycles x7 assets
    # + thrashed-box slow heals), concurrent episodes buffered the full firehose
    # for minutes each; ~100MB/min RSS growth with bounded writer buf/ntasks
    # points here (500 eps x 50k multi-KB msgs = GBs worst case). Overflow path
    # already counts + emits book_anomaly, so the tighter bound only converts
    # would-be-OOM into honest, counted drops. Revisit after RSS is flat.
    MAX_BUFFERED_MSGS_PER_EPISODE = 5_000
    # C4 (audit 2026-09-16): max age of an episode's replay buffer. A resync
    # that never completes (expired-window 404 loop) kept every live WS
    # message feeding a dead deque for hours — 1.36M drops in one XRP
    # episode. Past this age the buffer is retired: new messages are no
    # longer appended (one `resync_buffer_retired` event, then silence) so
    # live traffic flows to books directly instead of loss-theater.
    # Multiple of the REST-escalation horizon so healthy slow resyncs fit.
    BUFFER_AGE_FACTOR = 5

    def __init__(
        self,
        config,
        rest_fetcher: Callable[..., Any],  # async (asset, condition_id) -> rest snapshot dict
        on_event=None,
        on_book_state_change=None,
        on_episode_persist=None,
    ):
        self.config = config
        self.rest_fetcher = rest_fetcher
        self.on_event = on_event
        self.on_book_state_change = on_book_state_change
        self.on_episode_persist = on_episode_persist
        self._episodes: Dict[str, ResyncEpisode] = {}  # resync_id -> episode
        self._buffers: Dict[str, deque] = {}  # resync_id -> buffered WS messages
        self._rest_attempt_counts: Dict[str, int] = {}
        # episodes whose resync escalated (final state, buffer dead) — RAM-only
        # bookkeeping so finished-episode eviction can classify them
        self._escalated: set = set()
        self._buffer_dropped_total: Dict[str, int] = {}
        # C4: monotonic deadlines per buffer (retire zombie feeds) + retired set
        self._buffer_deadline: Dict[str, float] = {}
        self._buffer_retired: set = set()
        # P1 fix: episode-persist failures must never be silent (AGENT.md:
        # "high-stale-day with zero resync_episodes = P0 silent failure").
        # Counts every on_episode_persist exception; each also fires a
        # write_failed event so the blind spot is operationally visible.
        self._persist_fail_total: int = 0
        # P1 fetch_none quiet (weather audit 2026-09-22): some tokens expose
        # no full L2 (AMSTERDAM/ANKARA/ATLANTA) — REST returns None forever.
        # Track consecutive fetch_none per condition_id; after threshold the
        # book stays honestly stale with terminal backoff instead of hot churn.
        self._fetch_none_streak: Dict[str, int] = {}
        self._fetch_none_quiet_until: Dict[str, float] = {}
        # 2026-09-25 stale-epidemic fix: per-asset newest LIVE buffer id.
        # newest_open_buffer_id() runs on EVERY WS message; the old
        # reversed(list(_episodes.keys())) scan allocated an O(N) list per
        # message and, once the asset's buffers were all retired (books stale
        # past the 300s buffer deadline), walked ALL of _episodes (~2.3k open
        # in prod, 22.7k minted in 3.1h) per message — the event loop
        # starved, the 500ms scheduler fell 30-90s behind, and every live
        # snapshot row was honestly-but-misleadingly downgraded to stale by
        # the catch-up rule while the CLOB killed sockets with 1013 "slow
        # consumer". The cache is set at mint (the only episode-creation
        # point) and revalidated by buffer_live() per message; any
        # retirement/reap/completion/supersede/escalation flips buffer_live()
        # and the scan fallback refreshes it, so the returned id is always
        # identical to the scan's.
        self._newest_buf_by_asset: Dict[str, str] = {}
        # Stale-healing fix (2026-09-26): optional market-status resolver,
        # wired by the collector to its live markets registry:
        #   resolver(condition_id) -> (market_end_ts_ms|None, status|None)
        #   or None when the condition is unknown to discovery.
        # Lets resync() refuse dead markets up front (their REST 404s
        # forever — prod 2026-09-26: 77% of attempted conditions ended, 648
        # unknown) instead of burning a full escalation per ghost per walk.
        # None = standalone/test use: every market is treated as open.
        self.market_status_resolver = None

    def _safe_persist(self, payload: dict, where: str) -> None:
        """Persist an episode via on_episode_persist with loud failure accounting.

        Success path is unchanged. On exception: increment
        _persist_fail_total and fire write_failed (never silent). Used at
        every episode transition (disconnect/reconnect/attempt/completed/
        escalated/superseded).
        """
        cb = self.on_episode_persist
        if cb is None:
            return
        try:
            cb(payload)
        except Exception as e:
            try:
                self._persist_fail_total += 1
            except Exception:
                pass
            if self.on_event:
                try:
                    _rid = payload.get("resync_id") if isinstance(payload, dict) else None
                    self.on_event(CollectorEventType.write_failed, {
                        "dataset": "resync_episodes",
                        "reason": "episode_persist_failed",
                        "where": where,
                        "resync_id": _rid,
                        "fail_total": self._persist_fail_total,
                        "error": f"{type(e).__name__}: {e}"[:200],
                    })
                except Exception:
                    pass

    def fetch_none_quiet(self, condition_id: str) -> bool:
        """True if this condition is in terminal fetch_none backoff (honest stale, no churn)."""
        try:
            return time.monotonic() < float(self._fetch_none_quiet_until.get(str(condition_id), 0))
        except Exception:
            return False

    def note_fetch_none(self, condition_id: str) -> int:
        """Record a fetch_none; returns streak. >=5 enters 1h terminal quiet."""
        try:
            k = str(condition_id)
            n = int(self._fetch_none_streak.get(k, 0) or 0) + 1
            self._fetch_none_streak[k] = n
            if n >= 5:
                self._fetch_none_quiet_until[k] = time.monotonic() + 3600.0
                if self.on_event:
                    try:
                        self.on_event(CollectorEventType.resync_failed, {"condition_id": k, "fail_branch": "fetch_none_terminal", "attempts": n, "quiet_s": 3600, "reason": "REST exposes no full L2 for this token — honest stale, backing off"})
                    except Exception:
                        pass
            return n
        except Exception:
            return 0

    def note_fetch_ok(self, condition_id: str) -> None:
        try:
            self._fetch_none_streak.pop(str(condition_id), None)
            self._fetch_none_quiet_until.pop(str(condition_id), None)
        except Exception:
            pass

    def market_ended(self, condition_id: str):
        """True if the condition's window ended, False if open, None if unknown.

        Stale-healing fix (2026-09-26): consults market_status_resolver when
        wired (collector's live registry). None resolver or unknown condition
        returns None — discovery may lag, so unknown markets always proceed.
        """
        try:
            _resolver = getattr(self, "market_status_resolver", None)
            if not callable(_resolver):
                return None
            _mstat = _resolver(condition_id)
            if _mstat is None:
                return None
            try:
                _end_ms, _status = _mstat
            except Exception:
                return None
            if _end_ms is None:
                return None
            try:
                import time as _t_mod
                return int(_end_ms) < int(_t_mod.time() * 1000)
            except Exception:
                return None
        except Exception:
            return None

    def _buffer_age_limit_s(self) -> float:
        try:
            base = float(getattr(getattr(self.config, "ws", self.config), "max_resync_duration_seconds", 60))
        except Exception:
            base = 60.0
        return max(60.0, base * self.BUFFER_AGE_FACTOR)

    def buffer_live(self, resync_id: str) -> bool:
        """True if resync_id may still receive buffered WS messages."""
        try:
            if resync_id in self._escalated or resync_id in self._buffer_retired:
                return False
            if resync_id not in self._buffers:
                return False
            ep = self._episodes.get(resync_id)
            if ep is not None and ep.resync_completed_ts_utc is not None:
                return False
            dl = self._buffer_deadline.get(resync_id)
            if dl is not None and time.monotonic() > dl:
                return False
            return True
        except Exception:
            return False

    def newest_open_buffer_id(self, asset_upper: str) -> str:
        """Newest live buffer id for an asset (audit 2026-09-21 HIGH).

        The old first-match scan routed live messages to the OLDEST open
        episode — a zombie from the first recycle whose buffer was retired —
        so buffer-and-replay never worked after the first recycle. Newest
        first; never a retired/escalated/completed buffer.

        2026-09-25 fix: this runs per WS MESSAGE, and the O(N) list alloc +
        scan (N = all open episodes, thousands under episode churn) starved
        the event loop (see _newest_buf_by_asset). Fast path: the cached id
        for the asset, revalidated with the exact same gates as the scan.
        The cache is refreshed at mint (handle_disconnect) and by the scan
        fallback, so it can never return anything the scan would not.
        """
        try:
            au = str(asset_upper).upper()
        except Exception:
            return ""
        try:
            if not self._episodes or not self._buffers:
                self._newest_buf_by_asset.pop(au, None)
                return ""
            cached = self._newest_buf_by_asset.get(au, "")
            if cached:
                try:
                    ep = self._episodes.get(cached)
                    if (ep is not None and ep.asset == au
                            and ep.resync_completed_ts_utc is None
                            and cached in self._buffers
                            and self.buffer_live(cached)):
                        return cached
                except Exception:
                    _cache_validate_note = "cache entry failed revalidation; falling through to scan"
            for rid in reversed(list(self._episodes.keys())):
                try:
                    ep = self._episodes.get(rid)
                    if ep is None or ep.asset != au:
                        continue
                    if ep.resync_completed_ts_utc is not None:
                        continue
                    if rid not in self._buffers:
                        continue
                    if not self.buffer_live(rid):
                        continue
                    try:
                        self._newest_buf_by_asset[au] = rid
                    except Exception:
                        _cache_store_note = "cache store failed; scan remains source of truth"
                    return rid
                except Exception:
                    continue
            self._newest_buf_by_asset.pop(au, None)
            return ""
        except Exception:
            return ""

    def reap_expired_buffers(self) -> int:
        """Actively retire replay buffers past their age deadline (leak hunt 2026-09-25).

        The lazy retirement in buffer_message() only fires when ANOTHER message
        arrives for the same episode — but the dominant leak shape is a feed
        that went quiet (window rolled while the book was stale): no further
        buffer_message() call ever comes, the deadline passes unobserved, and
        the deque (up to MAX_BUFFERED_MSGS_PER_EPISODE parsed WS frames; a
        full-ladder `book` frame is multi-KB) stays pinned in RAM forever.
        The episode can never reach a final state in that shape — resync() is
        only driven from the reconnect path, and close_healed_episodes only
        closes episodes whose book went live — so neither the escalation pop
        nor the finished-episode FIFO eviction applies. Measured prod
        2026-09-25 (fresh reseed, 2.9h): 1494/2538 episodes never-final with
        ZERO resync attempts, each pinning its buffer (20-min 7-asset repro:
        tracemalloc 36.9MB / 518k retained orjson dicts at the loads() site,
        RSS ~22MB/min). Same contract as the lazy path: retired once, one
        honest resync_buffer_retired event, buffer + deadline popped, never
        replayed (buffer_live() gates on _buffer_retired).
        """
        now = time.monotonic()
        reaped = 0
        errs: list = []
        retired_events: list = []
        for rid in list(self._buffers.keys()):
            try:
                ep = self._episodes.get(rid)
                if rid in self._escalated or (ep is not None and ep.resync_completed_ts_utc is not None):
                    # Dead episode: drop any leftover buffer the state
                    # transitions missed (same pops the escalation/completion
                    # paths already do).
                    self._buffers.pop(rid, None)
                    self._buffer_deadline.pop(rid, None)
                    reaped += 1
                    continue
                dl = self._buffer_deadline.get(rid)
                if dl is None or now <= dl:
                    continue  # still live — nothing to do
                if rid not in self._buffer_retired and rid in self._episodes:
                    self._buffer_retired.add(rid)
                    retired_events.append((rid, ep))
                # RAM cleanup happens BEFORE the event so an on_event failure
                # can never skip freeing the buffer.
                self._buffers.pop(rid, None)
                self._buffer_deadline.pop(rid, None)
                reaped += 1
            except Exception as e:
                errs.append((rid, e))
                continue
        for rid, ep in retired_events:
            # One honest book_anomaly per retired buffer — the same event the
            # lazy path emits (buffer_message). A failure here is counted
            # loudly, never silent, and never fails the whole sweep.
            try:
                if self.on_event:
                    self.on_event(CollectorEventType.book_anomaly, {
                        "resync_id": rid,
                        "asset": ep.asset if ep is not None else None,
                        "reason": "resync_buffer_retired",
                        "dropped_total": self._buffer_dropped_total.get(rid, 0),
                        "cap": self.MAX_BUFFERED_MSGS_PER_EPISODE,
                    })
            except Exception as e:
                errs.append((rid, e))
        if errs:
            print(f"[resync] reap_expired_buffers: {len(errs)} error(s), "
                  f"buffers left in place (last: {errs[-1][1]!r})")
        return reaped

    def close_healed_episodes(self, books: Dict[str, OrderBookState]) -> int:
        """Close open episodes whose books are live again (audit 2026-09-21 HIGH).

        book_stalled/crossed/sanity episodes are healed by background REST
        (_heal_book_bg) and planned_recycle episodes by the fresh connection's
        full-book promotion — neither path touched the episode, so
        resync_completed_ts_utc stayed NULL forever, _episodes/_buffers grew
        without bound, and gap metrics were backdated to the next connect.
        The sweep closes an open episode once every book it covers reads live:
        reconnect stamped (if missing), completed stamped now, gap measured to
        the real heal time, buffer popped. Returns episodes closed.
        """
        closed = 0
        try:
            live_by_asset: Dict[str, list] = {}
            for b in books.values():
                try:
                    if getattr(getattr(b, "book_state", None), "value", "") == "live":
                        live_by_asset.setdefault(str(getattr(b, "asset", "")).upper(), []).append(
                            getattr(b, "condition_id", None))
                except Exception:
                    continue
        except Exception:
            return 0
        for rid, ep in list(self._episodes.items()):
            try:
                if self.is_finished(rid):
                    continue
                try:
                    au = str(ep.asset or "").upper()
                except Exception:
                    continue
                live_cids = live_by_asset.get(au, [])
                if not live_cids:
                    continue
                # Episode covers one book (condition_id set) or the whole asset
                # (condition_id None, e.g. asset-wide disconnect): close when
                # the covered book(s) are all live.
                if ep.condition_id is not None and ep.condition_id not in live_cids:
                    continue
                if ep.reconnect_ts_utc is None:
                    try:
                        self.handle_reconnect(rid)
                        ep = self._episodes.get(rid) or ep
                    except Exception:
                        pass
                ep.resync_completed_ts_utc = _now_iso()
                if self.on_event:
                    try:
                        self.on_event(CollectorEventType.resync_completed, ep.to_dict())
                    except Exception:
                        pass
                self._safe_persist(ep.to_dict(), "close_healed_episodes")
                self._buffers.pop(rid, None)
                self._buffer_deadline.pop(rid, None)
                self._buffer_retired.discard(rid)
                closed += 1
            except Exception:
                continue
        return closed

    def supersede_ended_market_episodes(self, books: Dict[str, OrderBookState]) -> int:
        """Supersede open episodes whose book's market window has ended (2026-09-25).

        The stale-epidemic tail: books of markets that ended <6h ago stay in
        RAM (books=777 measured in prod), their episodes mint per recycle /
        book_stalled and can never reach a final state — REST resync of an
        ended condition 404s forever, close_healed_episodes only closes
        books-live episodes, and the 6h memory eviction is the only exit
        (prod 2026-09-25: 29,678 never-final episodes in one day, 29,678 with
        ZERO resync attempts; supersede_episode had no callers at all).
        Each open episode also pins its replay buffer and keeps
        newest_open_buffer_id routing live WS messages into a dead deque.

        The sweep closes such an episode once via supersede_episode (honest
        resync_failed with superseded=True + persisted, buffer freed). Books
        whose window is still open or unknown are left to the normal paths.
        Returns episodes superseded.
        """
        superseded = 0
        try:
            now_ms = int(time.time() * 1000)
        except Exception:
            return 0
        for rid, ep in list(self._episodes.items()):
            try:
                if self.is_finished(rid):
                    continue
                if ep.condition_id is None:
                    continue  # asset-wide episode: closed by close_healed_episodes
                book = books.get(ep.condition_id)
                if book is None:
                    continue  # book evicted: the 6h memory tick owns it
                end_ms = getattr(book, "market_end_ts_ms", None)
                if end_ms is None or end_ms >= now_ms:
                    continue  # window open or unknown — normal healing applies
                if self.supersede_episode(rid, "market_window_ended_sweep"):
                    superseded += 1
            except Exception:
                continue
        return superseded

    def is_finished(self, resync_id: str) -> bool:
        ep = self._episodes.get(resync_id)
        if ep is None:
            return True
        return ep.resync_completed_ts_utc is not None or resync_id in self._escalated

    def _evict_finished_episodes(self) -> None:
        """Bound _episodes/_buffers RAM: evict oldest FINISHED episodes when over cap.

        Never evicts open episodes; every episode was already handed to
        on_episode_persist (parquet) at its transitions, so this is RAM hygiene
        only, not data loss.
        """
        if len(self._episodes) <= self.MAX_EPISODES:
            return
        excess = len(self._episodes) - self.MAX_EPISODES
        evicted = 0
        for rid in list(self._episodes.keys()):
            if evicted >= excess:
                break
            if self.is_finished(rid):
                self._episodes.pop(rid, None)
                self._buffers.pop(rid, None)
                self._escalated.discard(rid)
                self._buffer_dropped_total.pop(rid, None)
                self._buffer_deadline.pop(rid, None)
                self._buffer_retired.discard(rid)
                evicted += 1

    # -- disconnect --------------------------------------------------------
    def handle_disconnect(self, asset: str, condition_id: Optional[str], reason: str, books: Dict[str, OrderBookState], last_frame_ms: Optional[int] = None) -> str:
        """Mark books stale, create episode, return resync_id.

        H3: disconnect_ts_utc is backdated to the last frame time when known
        (gap_duration honest); detected_ts_utc carries wall-clock detection.
        """
        resync_id = str(uuid.uuid4())
        now_iso = _now_iso()
        try:
            _disc_iso = now_iso
            if last_frame_ms is not None:
                import datetime as _dtm
                _disc_iso = _dtm.datetime.fromtimestamp(last_frame_ms / 1000, tz=_dtm.timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            _disc_iso = now_iso
        ep = ResyncEpisode(
            resync_id=resync_id,
            asset=asset.upper(),
            condition_id=condition_id,
            disconnect_ts_utc=_disc_iso,
            disconnect_reason=reason,
            resync_attempt_count=0,
            detected_ts_utc=now_iso,
        )
        self._episodes[resync_id] = ep
        self._buffers[resync_id] = deque()
        self._buffer_deadline[resync_id] = time.monotonic() + self._buffer_age_limit_s()
        # 2026-09-25: mint is the only episode-creation point — a fresh
        # buffer is by definition the newest live one for the asset, so the
        # per-message routing cache (newest_open_buffer_id) can serve O(1)
        # until this buffer retires/reaps/completes and the scan refreshes.
        try:
            self._newest_buf_by_asset[ep.asset] = resync_id
        except Exception:
            _mint_cache_note = "cache store failed at mint; scan covers"
        self._evict_finished_episodes()  # after insert: guarantees len(_episodes) <= cap
        # mark each affected book
        for key, book in books.items():
            if book.asset.upper() == asset.upper() and (condition_id is None or book.condition_id == condition_id):
                book.mark_stale(resync_id=resync_id)
                if self.on_book_state_change:
                    self.on_book_state_change(book, BookState.stale)
        if self.on_event:
            self.on_event(CollectorEventType.ws_disconnected, ep.to_dict())
        self._safe_persist(ep.to_dict(), "handle_disconnect")
        return resync_id

    def handle_reconnect(self, resync_id: str) -> None:
        ep = self._episodes.get(resync_id)
        if not ep:
            return
        now_iso = _now_iso()
        ep.reconnect_ts_utc = now_iso
        # compute gap_duration_ms
        try:
            import datetime
            disc = datetime.datetime.fromisoformat(ep.disconnect_ts_utc.replace("Z", "+00:00"))
            recon = datetime.datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
            gap_ms = int((recon - disc).total_seconds() * 1000)
            ep.gap_duration_ms = gap_ms
            ep.snapshots_missed_estimate = gap_ms // 500
        except Exception:
            pass
        # M2: rest_fetch stays NULL until a real REST fetch is attempted in
        # resync() (was set here at reconnect even when no fetch happened).
        # Do NOT auto-mark completed for quick gaps — require real REST resync via resync() for honest gap per AGENT.md
        if self.on_event:
            self.on_event(CollectorEventType.ws_reconnected, ep.to_dict())
        self._safe_persist(ep.to_dict(), "handle_reconnect")

    # -- buffering during REST fetch ---------------------------------------
    def buffer_message(self, resync_id: str, msg: dict) -> None:
        if resync_id in self._buffers:
            # C4: retired/dead buffers refuse silently after one honest event.
            if not self.buffer_live(resync_id):
                if resync_id not in self._buffer_retired and resync_id in self._episodes:
                    self._buffer_retired.add(resync_id)
                    if self.on_event:
                        try:
                            _ep = self._episodes.get(resync_id)
                            self.on_event(CollectorEventType.book_anomaly, {
                                "resync_id": resync_id,
                                "asset": _ep.asset if _ep is not None else None,
                                "reason": "resync_buffer_retired",
                                "dropped_total": self._buffer_dropped_total.get(resync_id, 0),
                                "cap": self.MAX_BUFFERED_MSGS_PER_EPISODE,
                            })
                        except Exception:
                            pass
                    # HIGH fix (audit 2026-09-21): a retired buffer is never
                    # replayed — pop it so RAM is freed and no router can feed
                    # the zombie (routing now requires buffer_live anyway).
                    try:
                        self._buffers.pop(resync_id, None)
                        self._buffer_deadline.pop(resync_id, None)
                    except Exception:
                        pass
                return
            q = self._buffers[resync_id]
            if len(q) >= self.MAX_BUFFERED_MSGS_PER_EPISODE:
                # honest overflow: count every drop, emit an event (throttled) —
                # never silently lose data (AGENT.md)
                self._buffer_dropped_total[resync_id] = self._buffer_dropped_total.get(resync_id, 0) + 1
                dropped = self._buffer_dropped_total[resync_id]
                if dropped == 1 or dropped % 1000 == 0:
                    if self.on_event:
                        try:
                            _ep = self._episodes.get(resync_id)
                            self.on_event(CollectorEventType.book_anomaly, {
                                "resync_id": resync_id,
                                "asset": _ep.asset if _ep is not None else None,
                                "reason": "resync_buffer_overflow",
                                "dropped_total": dropped,
                                "cap": self.MAX_BUFFERED_MSGS_PER_EPISODE,
                            })
                        except Exception:
                            pass
                q.popleft()
            q.append(msg)

    # -- full resync -------------------------------------------------------
    async def resync(self, asset: str, condition_id: str, books: Dict[str, OrderBookState], resync_id: str) -> bool:
        """Perform REST resync with buffer-and-replay (§1A step 3).

        Returns True on success, False if escalation needed after max duration.
        """
        ep = self._episodes.get(resync_id)
        if not ep:
            return False
        # Audit 2026-09-16: an escalated episode is final (buffer dead, REST
        # target gone/refused). Re-driving it burns 60s of retries per call
        # and re-escalates forever — no-op so books stay honestly stale until
        # a genuinely new disconnect opens a fresh episode.
        if resync_id in self._escalated:
            return False

        # Ensure reconnect timestamp is set before first REST fetch (fixes 100% null gap_duration)
        if ep.reconnect_ts_utc is None:
            try:
                self.handle_reconnect(resync_id)
                ep = self._episodes.get(resync_id) or ep
            except Exception:
                pass

        cfg = self.config.ws
        max_duration_s = cfg.max_resync_duration_seconds
        start_ts = time.monotonic()
        attempt = 0
        # F9 (Sep 2026 forensics): per-attempt failures were fully silent —
        # 19/19 run-3 resyncs failed with no evidence of which phase broke.
        # Track the failing phase per attempt and report the last one on
        # escalation so one capture run answers it definitively.
        last_fail_branch: Optional[str] = None
        last_fail_error: Optional[str] = None

        if self.fetch_none_quiet(condition_id):
            return False

        # Stale-healing fix (2026-09-26): a market whose window already ended
        # never reaches the retry loop — its REST 404s forever and each drive
        # burns up to max_duration. Supersede immediately (honest final
        # state, zero attempts) so live books behind it still get driven.
        try:
            if self.market_ended(condition_id) is True:
                self.supersede_episode(resync_id, "market_window_ended_resync_precheck")
                return False
        except Exception:
            _precheck_note = "resolver failed; proceeding with normal resync"

        # mark resyncing
        # PERF: single targets list (was 4 full books.values() scans + BxM nest).
        # Same set/order (dict order), same mark_live/on_book_state_change sequence.
        targets = [b for b in books.values() if b.condition_id == condition_id]
        for book in targets:
            book.mark_resyncing(resync_id=resync_id)

        if self.on_event:
            self.on_event(CollectorEventType.resync_started, {"resync_id": resync_id, "asset": asset, "condition_id": condition_id})

        while True:
            ep.resync_attempt_count += 1
            ep.resync_rest_fetch_ts_utc = _now_iso()
            # persist attempt timestamp even on failure (ensures not 100% null)
            self._safe_persist(ep.to_dict(), "resync_attempt")
            phase = "fetch"
            attempt_branch: Optional[str] = None
            attempt_error: Optional[str] = None
            try:
                snapshot = await self.rest_fetcher(asset, condition_id)
                if snapshot is None:
                    attempt_branch = "fetch_none"
                    attempt_error = "REST fetch returned None (endpoint may not expose full L2 — §18 gate)"
                    try:
                        self.note_fetch_none(condition_id)
                    except Exception:
                        pass
                    raise RuntimeError(attempt_error)
                try:
                    self.note_fetch_ok(condition_id)
                except Exception:
                    pass

                # wholesale replace
                phase = "replace"
                for book in targets:
                    book.replace_from_rest_snapshot(snapshot)

                # buffer replay: apply buffered deltas in order, discarding those <= snapshot cursor
                snapshot_seq = snapshot.get("sequence_number")
                if snapshot_seq is None:
                    snapshot_seq = snapshot.get("seq")
                try:
                    snapshot_seq_int = int(snapshot_seq) if snapshot_seq is not None else None
                except (TypeError, ValueError):
                    snapshot_seq_int = None

                buffered = list(self._buffers.get(resync_id, []))
                phase = "replay"
                # MEDIUM fix (audit 2026-09-21): when the wire carries no
                # sequence numbers (the normal case per DATA_CARD), a stale
                # buffered frame used to overwrite the fresher REST snapshot.
                # Drop frames provably older than the REST fetch (frame clock
                # vs resync_rest_fetch_ts_utc, 1s skew); frames with no clock
                # fail open (applied) — reach is limited since the WS is
                # usually down during resync().
                try:
                    import datetime as _dtf
                    _fetch_ms = int(_dtf.datetime.fromisoformat(
                        (ep.resync_rest_fetch_ts_utc or "").replace("Z", "+00:00")).timestamp() * 1000)
                except Exception:
                    _fetch_ms = None
                for msg in buffered:
                    if not isinstance(msg, dict):
                        continue  # connection markers (None) — nothing to replay
                    msg_seq = msg.get("sequence_number")
                    if msg_seq is None:
                        msg_seq = msg.get("seq")
                    try:
                        msg_seq_int = int(msg_seq) if msg_seq is not None else None
                    except (TypeError, ValueError):
                        msg_seq_int = None
                    # discard if provably older/equal to snapshot cursor
                    if snapshot_seq_int is not None and msg_seq_int is not None and msg_seq_int <= snapshot_seq_int:
                        continue
                    if msg_seq_int is None and snapshot_seq_int is None and _fetch_ms is not None:
                        try:
                            from .book import OrderBookState as _OBS
                            _fts = _OBS._parse_frame_ts_ms(msg)
                        except Exception:
                            _fts = None
                        if _fts is not None and _fts < _fetch_ms - 1000:
                            continue  # stale pre-fetch frame — REST snapshot is newer
                    for book in targets:
                        book.apply_ws_message(msg)

                # clear stale flag
                # NOTE (audit 2026-09-21): promotion is NOT gated on both
                # outcomes here by design — partial-REST success is the pinned
                # contract (test_resync/chaos: up-only snapshots heal to live;
                # missing sides ship as honest NULLs). The one-sided-REST hazard
                # (429 on one token) is fixed at the source: _fetch_rest_book
                # refuses to return a partial merge (returns None → retry)
                # instead of letting resync() promote a half book.
                for book in targets:
                    book.mark_live()
                    if self.on_book_state_change:
                        self.on_book_state_change(book, BookState.live)

                ep.resync_completed_ts_utc = _now_iso()
                if self.on_event:
                    self.on_event(CollectorEventType.resync_completed, ep.to_dict())
                self._safe_persist(ep.to_dict(), "resync_completed")
                # cleanup buffer
                self._buffers.pop(resync_id, None)
                self._buffer_deadline.pop(resync_id, None)
                self._buffer_retired.discard(resync_id)
                return True

            except Exception as e:
                # K-4: per-attempt failures are recorded on the episode
                # (resync_attempt_count) — emitting a resync_failed event per
                # attempt turned rate-limited recoveries into alert noise.
                # Only the escalation path (below) raises resync_failed.
                # F9: one stdout line per attempt (attempts are seconds apart
                # via backoff) + last-failure branch carried to escalation.
                if attempt_branch is None:
                    attempt_branch = {"fetch": "fetch_err", "replace": "replace_err",
                                      "replay": "replay_err"}.get(phase, "unknown")
                    attempt_error = f"{type(e).__name__}: {e}"[:200]
                last_fail_branch, last_fail_error = attempt_branch, attempt_error
                # Stale-healing fix (2026-09-26): an ended market must not ride
                # out the backoff/escalation burn after a fetch_none — abandon
                # immediately (honest final state). Unknown markets keep the
                # normal retry path (discovery may lag).
                if attempt_branch == "fetch_none":
                    try:
                        if self.market_ended(condition_id) is True:
                            self.supersede_episode(resync_id, "market_window_ended_fetch_none")
                            return False
                    except Exception:
                        _abandon_note = "resolver failed; continuing normal retry"
                print(f"[resync] attempt {ep.resync_attempt_count} {asset} {condition_id} "
                      f"failed at {attempt_branch}: {attempt_error}")
                # persist attempt state for honest episode bookkeeping
                self._safe_persist(ep.to_dict(), "resync_attempt_failed")
                # check escalation timeout
                elapsed = time.monotonic() - start_ts
                if elapsed >= max_duration_s:
                    # escalate — treat as page operator (§1A retry policy)
                    # Mark gap metrics even on escalation so episode is not 100% null
                    if ep.gap_duration_ms is None and ep.reconnect_ts_utc is not None:
                        try:
                            import datetime as _dt2
                            disc = _dt2.datetime.fromisoformat(ep.disconnect_ts_utc.replace("Z", "+00:00"))
                            recon = _dt2.datetime.fromisoformat(ep.reconnect_ts_utc.replace("Z", "+00:00"))
                            ep.gap_duration_ms = int((recon - disc).total_seconds() * 1000)
                            ep.snapshots_missed_estimate = ep.gap_duration_ms // 500
                        except Exception:
                            ep.gap_duration_ms = int(elapsed * 1000)
                            ep.snapshots_missed_estimate = ep.gap_duration_ms // 500
                    if self.on_episode_persist:
                        d = ep.to_dict()
                        d["escalated"] = True
                        self._safe_persist(d, "resync_escalated")
                    if self.on_event:
                        self.on_event(CollectorEventType.resync_failed, {"resync_id": resync_id, "escalation": True, "elapsed_s": elapsed, "asset": ep.asset, "condition_id": ep.condition_id, "fail_branch": last_fail_branch, "fail_error": last_fail_error, "attempts": ep.resync_attempt_count})
                    # P0 leak hunt session 2: an escalated episode must stop
                    # consuming buffers. It can never complete (its REST target is
                    # gone or refused), so leaving it open + buffered meant
                    # _replay_buffer_id kept feeding every live WS message for the
                    # asset into a never-consumed deque — unbounded (leak #2).
                    # The episode record itself is kept (honest, already persisted).
                    self._buffers.pop(resync_id, None)
                    self._buffer_deadline.pop(resync_id, None)
                    self._buffer_retired.discard(resync_id)
                    self._escalated.add(resync_id)
                    return False
                # backoff before retry (independent of WS reconnect backoff)
                delay = exponential_backoff(attempt, cfg.resync_rest_backoff_initial_ms, cfg.resync_rest_backoff_max_ms, jitter=True)
                attempt += 1
                await asyncio.sleep(delay)

    # -- sequence gap / drift helpers --------------------------------------
    def handle_sequence_gap(self, asset: str, condition_id: str, books: Dict[str, OrderBookState], expected: int, received: int) -> str:
        """Treat sequence gap as disconnect (§1A). Returns new resync_id."""
        if self.on_event:
            self.on_event(CollectorEventType.sequence_gap, {"asset": asset, "condition_id": condition_id, "expected": expected, "received": received})
        return self.handle_disconnect(asset, condition_id, reason="sequence_gap", books=books)

    async def periodic_drift_check(self, asset: str, condition_id: str, books: Dict[str, OrderBookState]) -> Optional[str]:
        """Full-book diff drift check (§1A fallback, interval 30-60s). Returns resync_id if drift detected."""
        try:
            snapshot = await self.rest_fetcher(asset, condition_id)
            if snapshot is None:
                return None
            for book in books.values():
                if book.condition_id == condition_id:
                    diff = book.diff_against_rest(snapshot, tolerance=self.config.ws.full_book_diff_tolerance)
                    if diff:
                        if self.on_event:
                            self.on_event(CollectorEventType.book_anomaly, {"asset": asset, "condition_id": condition_id, "diff": diff})
                        return self.handle_disconnect(asset, condition_id, reason="drift_detected", books=books)
        except Exception as e:
            if self.on_event:
                self.on_event(CollectorEventType.book_anomaly, {"asset": asset, "error": str(e)})
        return None

    def ensure_all_reconnected(self) -> None:
        """Set reconnect timestamp for any episode still pending (e.g., on collector stop)."""
        for ep in list(self._episodes.values()):
            if ep.reconnect_ts_utc is None:
                try:
                    self.handle_reconnect(ep.resync_id)
                except Exception:
                    pass

    def get_episode(self, resync_id: str) -> Optional[ResyncEpisode]:
        return self._episodes.get(resync_id)

    def supersede_episode(self, resync_id: str, reason: str, extra: Optional[dict] = None) -> bool:
        """Close an open episode that can never complete (audit 2026-09-16 C2).

        Used when the episode's market window already ended: REST resync of
        an expired condition 404s forever, so re-driving it every recycle
        burns a full max_duration retry loop for nothing. Closes the gap
        honestly (reconnect timestamp set, resync_failed with
        superseded=True) and frees the buffer. Returns False if there was
        no open episode to close.
        """
        ep = self._episodes.get(resync_id)
        if ep is None or self.is_finished(resync_id):
            return False
        try:
            if ep.reconnect_ts_utc is None:
                self.handle_reconnect(resync_id)
        except Exception:
            pass
        if self.on_event:
            try:
                payload = {"resync_id": resync_id, "superseded": True, "reason": reason,
                           "asset": ep.asset, "condition_id": ep.condition_id}
                if extra:
                    payload.update(extra)
                self.on_event(CollectorEventType.resync_failed, payload)
            except Exception:
                pass
        if self.on_episode_persist:
            d = ep.to_dict()
            d["superseded"] = True
            d["supersede_reason"] = reason
            self._safe_persist(d, "supersede_episode")
        self._buffers.pop(resync_id, None)
        self._buffer_deadline.pop(resync_id, None)
        self._buffer_retired.discard(resync_id)
        self._escalated.add(resync_id)
        return True

    def all_episodes(self) -> List[ResyncEpisode]:
        return list(self._episodes.values())
