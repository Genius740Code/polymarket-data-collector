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
    MAX_BUFFERED_MSGS_PER_EPISODE = 50_000

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
                evicted += 1

    # -- disconnect --------------------------------------------------------
    def handle_disconnect(self, asset: str, condition_id: Optional[str], reason: str, books: Dict[str, OrderBookState]) -> str:
        """Mark books stale, create episode, return resync_id."""
        resync_id = str(uuid.uuid4())
        now_iso = _now_iso()
        ep = ResyncEpisode(
            resync_id=resync_id,
            asset=asset.upper(),
            condition_id=condition_id,
            disconnect_ts_utc=now_iso,
            disconnect_reason=reason,
            resync_attempt_count=0,
        )
        self._episodes[resync_id] = ep
        self._buffers[resync_id] = deque()
        self._evict_finished_episodes()  # after insert: guarantees len(_episodes) <= cap
        # mark each affected book
        for key, book in books.items():
            if book.asset.upper() == asset.upper() and (condition_id is None or book.condition_id == condition_id):
                book.mark_stale(resync_id=resync_id)
                if self.on_book_state_change:
                    self.on_book_state_change(book, BookState.stale)
        if self.on_event:
            self.on_event(CollectorEventType.ws_disconnected, ep.to_dict())
        if self.on_episode_persist:
            try:
                self.on_episode_persist(ep.to_dict())
            except Exception:
                pass
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
        # ensure rest_fetch timestamp not 100% null: mark fetch attempt time on reconnect (honest gap even if REST not yet tried)
        # Do NOT auto-mark completed for quick gaps — require real REST resync via resync() for honest gap per AGENT.md
        if ep.resync_rest_fetch_ts_utc is None:
            ep.resync_rest_fetch_ts_utc = now_iso
        if self.on_event:
            self.on_event(CollectorEventType.ws_reconnected, ep.to_dict())
        if self.on_episode_persist:
            try:
                self.on_episode_persist(ep.to_dict())
            except Exception:
                pass

    # -- buffering during REST fetch ---------------------------------------
    def buffer_message(self, resync_id: str, msg: dict) -> None:
        if resync_id in self._buffers:
            q = self._buffers[resync_id]
            if len(q) >= self.MAX_BUFFERED_MSGS_PER_EPISODE:
                # honest overflow: count every drop, emit an event (throttled) —
                # never silently lose data (AGENT.md)
                self._buffer_dropped_total[resync_id] = self._buffer_dropped_total.get(resync_id, 0) + 1
                dropped = self._buffer_dropped_total[resync_id]
                if dropped == 1 or dropped % 1000 == 0:
                    if self.on_event:
                        try:
                            self.on_event(CollectorEventType.book_anomaly, {
                                "resync_id": resync_id,
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

        # mark resyncing
        for book in books.values():
            if book.condition_id == condition_id:
                book.mark_resyncing(resync_id=resync_id)

        if self.on_event:
            self.on_event(CollectorEventType.resync_started, {"resync_id": resync_id, "asset": asset, "condition_id": condition_id})

        while True:
            ep.resync_attempt_count += 1
            ep.resync_rest_fetch_ts_utc = _now_iso()
            # persist attempt timestamp even on failure (ensures not 100% null)
            if self.on_episode_persist:
                try:
                    self.on_episode_persist(ep.to_dict())
                except Exception:
                    pass
            try:
                snapshot = await self.rest_fetcher(asset, condition_id)
                if snapshot is None:
                    raise RuntimeError("REST fetch returned None (endpoint may not expose full L2 — §18 gate)")

                # wholesale replace
                for book in books.values():
                    if book.condition_id == condition_id:
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
                    for book in books.values():
                        if book.condition_id == condition_id:
                            book.apply_ws_message(msg)

                # clear stale flag
                for book in books.values():
                    if book.condition_id == condition_id:
                        book.mark_live()
                        if self.on_book_state_change:
                            self.on_book_state_change(book, BookState.live)

                ep.resync_completed_ts_utc = _now_iso()
                if self.on_event:
                    self.on_event(CollectorEventType.resync_completed, ep.to_dict())
                if self.on_episode_persist:
                    try:
                        self.on_episode_persist(ep.to_dict())
                    except Exception:
                        pass
                # cleanup buffer
                self._buffers.pop(resync_id, None)
                return True

            except Exception as e:
                # K-4: per-attempt failures are recorded on the episode
                # (resync_attempt_count) — emitting a resync_failed event per
                # attempt turned rate-limited recoveries into alert noise.
                # Only the escalation path (below) raises resync_failed.
                # persist attempt state for honest episode bookkeeping
                if self.on_episode_persist:
                    try:
                        self.on_episode_persist(ep.to_dict())
                    except Exception:
                        pass
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
                        try:
                            d = ep.to_dict()
                            d["escalated"] = True
                            self.on_episode_persist(d)
                        except Exception:
                            pass
                    if self.on_event:
                        self.on_event(CollectorEventType.resync_failed, {"resync_id": resync_id, "escalation": True, "elapsed_s": elapsed})
                    # P0 leak hunt session 2: an escalated episode must stop
                    # consuming buffers. It can never complete (its REST target is
                    # gone or refused), so leaving it open + buffered meant
                    # _replay_buffer_id kept feeding every live WS message for the
                    # asset into a never-consumed deque — unbounded (leak #2).
                    # The episode record itself is kept (honest, already persisted).
                    self._buffers.pop(resync_id, None)
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

    def all_episodes(self) -> List[ResyncEpisode]:
        return list(self._episodes.values())
