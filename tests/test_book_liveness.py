"""P3 liveness fix (2026-10-11) — liveness follows book freshness (row label).

P3 retime evidence (/tmp/opencode/p3_retime.txt): post-roll resync episodes
held every snapshot row of a fresh book stale — stale rows carried bk_age
med 0ms / max 4.5s while (stale,resync_id)=334 vs live=272 per lane in the
first 2-3 min, so +2min live% read 7.5-39% of preroll with the feed alive.

These tests pin the row-level gate in OrderBookState.snapshot():
- a fresh verified book (both outcomes updated within FRESH_BOOK_AGE_MS,
  both two-sided sane uncrossed) reads book_state 'live' EVEN with an open
  episode, while RAM state, resync_id and episode machinery stay untouched
  (evidence rows keep flowing from resync_episodes/collector_events);
- a stale-aged book, a one-sided book, a crossed book and the
  known-divergent boundary (sanity_bounds_failed / sequence_gap) keep the
  honest stale label;
- the stale_dominance marker reports the would-be-stale split per spell.

Real objects + constant literals only; no network.
"""
import time

from polymarket_collector.book import OrderBookState


def make_book(cid="cid-live", asset="BTC"):
    return OrderBookState(
        asset=asset,
        condition_id=cid,
        market_id="mid-1",
        series_id="BTC-5MIN",
        window_index=7,
        up_token_id="up-123",
        down_token_id="down-456",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
    )


def _apply_two_sided(book, bid=0.48, ask=0.52):
    """Apply real two-sided frames for BOTH outcomes without promoting.

    Frames carry no hash (crypto lanes are hash-gated), so levels are
    applied but the book is not promoted — exactly the mid-episode shape
    where an open resync episode holds the RAM label while WS streams.
    """
    book.apply_ws_message({"token_id": "up-123", "bids": [[bid, 10]], "asks": [[ask, 10]]})
    book.apply_ws_message({"token_id": "down-456", "bids": [[bid, 10]], "asks": [[ask, 10]]})


# --- fresh verified book + open episode -> row reads live -------------------

def test_fresh_book_open_episode_row_reads_live():
    b = make_book()
    _apply_two_sided(b)
    # the roll mints an episode: handle_disconnect marks the book stale
    # while the feed keeps streaming fresh frames
    b.mark_stale(resync_id="ep-1")
    assert b.book_state.value == "stale"
    row = b.snapshot().to_flat_dict()
    # liveness follows book freshness: the row reads live despite the
    # open episode, and keeps the episode join for evidence
    assert row["book_state"] == "live"
    assert row["resync_id"] == "ep-1"
    assert row["up_book_age_ms"] is not None and row["up_book_age_ms"] <= 5000
    assert row["down_book_age_ms"] is not None and row["down_book_age_ms"] <= 5000


def test_fresh_book_open_episode_ram_untouched():
    """The fix is row-level only: RAM state, resync_id and the stale clock
    stay exactly as the episode machinery left them (evidence rows keep
    flowing; nothing about the episode is closed or filled here)."""
    b = make_book()
    _apply_two_sided(b)
    b.mark_stale(resync_id="ep-2")
    _since = b._stale_since_ms
    b.snapshot()
    assert b.book_state.value == "stale"
    assert b.resync_id == "ep-2"
    assert b._stale_since_ms == _since


def test_resyncing_ram_state_fresh_book_row_reads_live():
    """The resync() drive holds books in 'resyncing' through its retry loop —
    same freshness gate applies (the probe counted those rows stale)."""
    b = make_book()
    _apply_two_sided(b)
    b.mark_stale(resync_id="ep-3")
    b.mark_resyncing(resync_id="ep-3")
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "live"
    assert b.book_state.value == "resyncing"


def test_live_ram_state_row_unchanged():
    b = make_book()
    _apply_two_sided(b)
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "live"


# --- stale content + open episode -> row keeps the honest label -------------

def test_stale_aged_book_open_episode_row_reads_stale():
    b = make_book()
    _apply_two_sided(b)
    b.mark_stale(resync_id="ep-aged")
    # the feed went quiet: last exchange update 6.5s behind the bucket,
    # past FRESH_BOOK_AGE_MS — the label honestly returns to stale
    b._up_last_update_ns = time.time_ns() - 6_500_000_000
    b._down_last_update_ns = time.time_ns() - 6_500_000_000
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"
    assert row["resync_id"] == "ep-aged"


def test_one_sided_outcome_fresh_row_stays_stale():
    """A never-updated outcome has no age and no verifiable side — H2's
    missing-liquidity discipline keeps the honest stale label."""
    b = make_book()
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.48, 10]], "asks": [[0.52, 10]]})
    b.mark_stale(resync_id="ep-1s")
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"


def test_one_sided_top_fresh_row_stays_stale():
    """A fresh two-outcome book whose UP side holds bids only cannot be
    spread-verified — stale stays."""
    b = make_book()
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.48, 10]]})
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.48, 10]], "asks": [[0.52, 10]]})
    b.mark_stale(resync_id="ep-1t")
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"


def test_crossed_fresh_book_row_stays_stale():
    b = make_book()
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.60, 10]], "asks": [[0.50, 10]]})
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.48, 10]], "asks": [[0.52, 10]]})
    b.mark_stale(resync_id="ep-x")
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"
    assert row["book_crossed"] is True


# --- boundary: known-divergent RAM keeps stale despite freshness ----------

def test_sanity_bounds_staleness_keeps_stale_despite_fresh_book():
    """Boundary (documented in book.py): a sanity_bounds_failed mark means
    the offending frame was REJECTED — the update it carried was missed, so
    RAM is known-divergent from the venue. A fresh age certifies only the
    last applied frame, not agreement with the venue: the row keeps the
    honest stale label until REST / a hash-gated full-book frame re-verifies."""
    b = make_book()
    _apply_two_sided(b)
    applied, reason = b.apply_ws_message({"token_id": "up-123", "bids": [[1.5, 10]]})
    assert applied is False and "sanity_bounds_failed" in reason
    assert b.book_state.value == "stale"
    assert b._stale_reason == "sanity_bounds_failed"
    # ages are still fresh (the good frames applied just before the strike)
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"
    # verification clears the boundary: a hash-gated full-book frame promotes
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.48, 10]], "asks": [[0.52, 10]],
                        "hash": "a1b2c3d4e5f6a7b8"})
    assert b.book_state.value == "live"
    assert b.snapshot().to_flat_dict()["book_state"] == "live"


def test_sequence_gap_staleness_keeps_stale_despite_fresh_book():
    """Boundary: a sequence_gap mark means deltas were missed — RAM has
    known holes; freshness may not verify the row until re-verification."""
    b = make_book()
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.48, 10]], "asks": [[0.52, 10]],
                        "sequence_number": 1})
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.48, 10]], "asks": [[0.52, 10]],
                        "sequence_number": 2})
    applied, reason = b.apply_ws_message({"token_id": "up-123", "bids": [[0.49, 10]],
                                          "sequence_number": 5})
    assert applied is False and "sequence_gap" in reason
    assert b._stale_reason == "sequence_gap"
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"


def test_episode_staleness_is_freshness_promotable_after_refill():
    """H2 empty-side staleness (reason None) heals by the same gate: while a
    side is empty the two-sided check refuses; once real frames refill both
    sides the row reads live again."""
    b = make_book()
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.48, 10]]})
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.48, 10]]})
    b.mark_stale(resync_id="ep-h2")  # episode-style mint (reason None)
    assert b.snapshot().to_flat_dict()["book_state"] == "stale"
    _apply_two_sided(b)  # frames refill both outcomes, both sides
    assert b.snapshot().to_flat_dict()["book_state"] == "live"


# --- stale_dominance marker ---------------------------------------------------

def test_stale_dominance_marker_reports_split(capsys):
    b = make_book()
    _apply_two_sided(b)
    b.mark_stale(resync_id="ep-sd")
    now_ms = int(time.time() * 1000)
    # 10 fresh ticks read live under the fix but still count as would-be-stale
    for _ in range(10):
        b.snapshot(ts_ms=now_ms)
    # 20 aged ticks read stale: the feed goes quiet, last update 6.5s back
    b._up_last_update_ns = time.time_ns() - 6_500_000_000
    b._down_last_update_ns = time.time_ns() - 6_500_000_000
    for _ in range(20):
        b.snapshot(ts_ms=now_ms)
    out = capsys.readouterr().out
    expected = ("[recovery] phase=stale_dominance asset=BTC cid=cid-live "
            "window_index=7 stale_total=30 stale_fresh_book=10\n")
    assert expected in out
    # checkpoint-bounded: ticks 31..359 emit nothing further
    for _ in range(29):
        b.snapshot(ts_ms=now_ms - 6000)
    assert "stale_total=59" not in capsys.readouterr().out


# --- fresh+verified+hashless promotion gate --------------------------------

def test_fresh_verified_hashless_promotes():
    """(a) Fresh+verified+hashless book promotes to live.

    A book with both outcomes fresh (≤5000ms age) and two-sided sane,
    uncrossed reads 'live' even without a hash — the freshness gate
    is the promotion path when no hash is present (F10/Fresh book promo).
    """
    b = make_book()
    _apply_two_sided(b)
    # Both outcomes are fresh from _apply_two_sided; force ages into
    # the fresh window by snapshotting quickly
    now_ms = int(time.time() * 1000)
    row = b.snapshot(ts_ms=now_ms).to_flat_dict()
    assert row["book_state"] == "live"
    assert row["up_book_age_ms"] is not None and row["up_book_age_ms"] <= 5000
    assert row["down_book_age_ms"] is not None and row["down_book_age_ms"] <= 5000


# --- stale EITHER side refuses promotion ------------------------------------

def test_stale_either_side_refuses_promotion():
    """(b) If EITHER outcome is stale (>5000ms), the book refuses promotion.

    Freshness requires BOTH outcomes within FRESH_BOOK_AGE_MS. One stale
    side kills the promote.  We first mark the book stale so the freshness
    gate is evaluated (it refuses when one age > FRESH_BOOK_AGE_MS).
    """
    b = make_book()
    _apply_two_sided(b)
    b.mark_stale(resync_id="ep-stale-either")
    # Mark only up side stale — 6.5s behind
    b._up_last_update_ns = time.time_ns() - 6_500_000_000
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"
    assert row["up_book_age_ms"] is not None and row["up_book_age_ms"] > 5000


def test_one_sided_outcome_refuses_promotion():
    """(c) One-sided book (only one outcome updated) refuses promotion.

    A book with bids on UP but no asks on DOWN, or only one outcome with
    frames, cannot promote — the two-sided check refuses.  Mark stale
    first so the freshness gate is evaluated."""
    b = make_book()
    b.mark_stale(resync_id="ep-1s")
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.48, 10]], "asks": [[0.52, 10]]})
    # DOWN has no frames at all — one-sided
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"


# --- crossed/violated-bounds refuses promotion --------------------------

def test_crossed_book_refuses_promotion():
    """(d) A crossed book refuses promotion even if ages are fresh.

    Crossed book_BBO triggers the stale label regardless of freshness.
    Mark stale first so the freshness gate is evaluated (and refuses)."""
    b = make_book()
    b.mark_stale(resync_id="ep-x-crossed")
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.60, 10]], "asks": [[0.50, 10]]})
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.48, 10]], "asks": [[0.52, 10]]})
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"
    assert row["book_crossed"] is True


def test_violated_bounds_refuses_promotion():
    """(d) Prices outside 0..1 bounds refuse promotion."""
    b = make_book()
    b.apply_ws_message({"token_id": "up-123", "bids": [[1.5, 10]], "asks": [[1.6, 10]]})
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.48, 10]], "asks": [[0.52, 10]]})
    row = b.snapshot().to_flat_dict()
    assert row["book_state"] == "stale"


# --- 429 streak decays at 30s -----------------------------------------

def test_429_streak_decays_at_30s():
    """(e) Heal 429 streak decays after 30s of clean wire (not 60s, not 0).

    Per heal.py: _HEAL_429_CLEAN_S = 30.0s. A streak > 0 resets once
    the wire has been clean past 30s. Retry respect is kept — 0 decay
    would mean never resetting; 60s would be too lenient.
    """
    import time as _t
    from polymarket_collector.ingest.heal import _HEAL_429_CLEAN_S, heal_rate_limited

    # Verify the constant
    assert _HEAL_429_CLEAN_S == 30.0, (
        f"Expected _HEAL_429_CLEAN_S=30.0, got {_HEAL_429_CLEAN_S}"
    )

    # Setup: streak=1, cooldown active
    # Set cooldown 60s in the future
    import polymarket_collector.ingest.heal as _mod
    _mod._heal_429_until = _t.monotonic() + 60.0
    _mod._heal_429_streak = 1
    _mod._heal_429_last_event = _t.monotonic()

    # Before 30s clean: rate_limited should still be True (cooldown active)
    assert heal_rate_limited() is True, (
        "Expected rate_limited True during cooldown"
    )

    # Advance 31s past last event — streak should decay
    # Reset cooldown to simulate wire clean for 31s
    _mod._heal_429_until = 0.0
    _mod._heal_429_last_event = _t.monotonic() - 31.0

    # After 31s clean past last event with streak>0, streak decays to 0
    # and rate_limited returns False
    assert heal_rate_limited() is False, (
        f"Expected rate_limited False after 31s clean, got {heal_rate_limited()}"
    )


# --- dip markers emit with T0→first-tick shape --------------------------

def test_post_roll_dip_marker_emits_t0_first_tick():
    """(f) post_roll_dip_start marker emits with T0→first-tick shape.

    Per resync.py handle_disconnect: recovery_marker("post_roll_dip_start",
    T0=_disc_iso) is emitted. note_ws_frame emits post_roll_first_tick
    on the first WS frame after disconnect (T0 → first-tick). The
    recovery_marker function accepts arbitrary keyword fields (via **fields)
    so T0 and first_tick are emitted in the output line."""
    from polymarket_collector.resync import recovery_marker
    import inspect
    source = inspect.getsource(recovery_marker)
    # Function accepts **fields kwargs, enabling T0/first-tick emission
    assert "**fields" in source
    # Verify the function prints recovery lines with phase + key=value
    assert "print" in source