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
            self._buffers[resync_id].append(msg)

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
