"""Pre-warm tail extension (backlog #4a) — post-roll discovery tail coverage.

Measured last-24h: 330/2443 5m discoveries happen post-open (p50 lag 43.3s,
p90 220.6s, max 299s); 82 windows ran >120s late, 77 of them via=poll,
because the pre-warm slug path had already given up at T+60s while Gamma
serves a fresh 5m slug anywhere in ~30-180s after open. Both bounded tails
(gap-tail and pre-window tail) now poll to T+180s on a 2s cadence: at most
one Gamma GET per 2s per lane, hard stop at T+180s — never an unbounded
spinner.

Real data only: test fixtures injecting canned Gamma responses are normal
test practice — a local recording responder stands in for discovery here
(no network, no generated prices or books; every adopted market is an
inline MarketInfo literal).
"""
import pytest

from polymarket_collector.config import CollectorConfig
from polymarket_collector.rollover import MarketInfo, RolloverManager

# Fixed wall-clock anchor: the fresh 5m window opens at T0 (the unix-ts
# window start used by the 5m slug family, in ms).
T0 = 1_791_642_600_000
WINDOW_MS = 300_000
STEP_MS = 100  # driver cadence: finer than the 2s gate so the gate bounds load


def _market(cid, start_ms, end_ms, window_index):
    return MarketInfo(
        condition_id=cid,
        market_id=f"mid-{cid}",
        asset="BTC",
        up_token_id=f"{cid}-UP",
        down_token_id=f"{cid}-DOWN",
        market_start_ts_ms=start_ms,
        market_end_ts_ms=end_ms,
        window_index=window_index,
        series_id="BTC-5MIN",
    )


def _prev():
    """The ended window [T0-300s, T0) — current at a gapped promote."""
    return _market("prev", T0 - WINDOW_MS, T0, (T0 // 1000 // 300) - 1)


def _fresh():
    """The fresh window [T0, T0+300s) — the slug Gamma indexes late."""
    return _market("fresh", T0, T0 + WINDOW_MS, T0 // 1000 // 300)


def _mgr(events):
    cfg = CollectorConfig()
    cfg.rollover_lead_seconds = 30
    return RolloverManager(cfg, on_event=lambda t, d: events.append((t, d)))


def _canned_discovery(mgr, state, attempts, serve_at_ms, market):
    """Recording responder standing in for the Gamma GET: appends each
    attempt's clock reading (prewarm sets prewarm_last_attempt_ms = now just
    before the call) and serves `market` once the attempt clock reaches
    serve_at_ms (None serves nothing, ever)."""

    async def canned_fetch(asset, after_ts_ms, strict_adjacent=False):
        at = state.prewarm_last_attempt_ms
        attempts.append(at)
        if serve_at_ms is not None and at is not None and at >= serve_at_ms:
            return market
        return None

    mgr.discoveries["5m"].fetch_next_market = canned_fetch  # type: ignore


async def _drive(mgr, subscribe, from_ms, to_ms, stop_on_event=False):
    """Call prewarm_next_window every STEP_MS of simulated wall clock."""
    t = from_ms
    res = None
    while t <= to_ms and not (stop_on_event and res == "market_added"):
        res = await mgr.prewarm_next_window("BTC", subscribe, now_ms=t)
        t += STEP_MS
    return res


def _spacings(attempts):
    return [b - a for a, b in zip(attempts, attempts[1:])]


# --- gap-tail: current=None, prewarm_gap_after_ms armed ---------------------


@pytest.mark.asyncio
async def test_gap_tail_polls_past_old_60s_stop_with_2s_cadence():
    """The gap-tail must keep polling the fresh slug past the old T+60s
    give-up (measured tail: Gamma serves 60-180s after open) on the 2s
    cadence — at most one Gamma GET per 2s per lane."""
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    state.current = None  # gapped promote
    state.prewarm_gap_after_ms = T0

    attempts = []
    _canned_discovery(mgr, state, attempts, serve_at_ms=None, market=None)

    async def subscribe(market):
        raise AssertionError("nothing served — must not subscribe")

    assert await _drive(mgr, subscribe, T0, T0 + 152_000) is None
    assert attempts[0] == T0  # polling starts at window open
    assert T0 + 150_000 in attempts  # still polling at T+150s, past the old stop
    assert state.prewarm_gap_after_ms == T0  # bookkeeping not retired early
    diffs = _spacings(attempts)
    assert diffs and all(2_000 <= d <= 2_100 for d in diffs)


@pytest.mark.asyncio
async def test_gap_tail_adopts_market_served_at_90s(capsys):
    """A slug served at T+90s (inside the old give-up window) must be
    adopted by the gap-tail with the prewarm_gap provenance markers intact."""
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    state.current = None  # gapped promote
    state.prewarm_gap_after_ms = T0
    fresh = _fresh()

    attempts = []
    _canned_discovery(mgr, state, attempts, serve_at_ms=T0 + 90_000, market=fresh)

    subscribed = []

    async def subscribe(market):
        subscribed.append(market.condition_id)

    res = await _drive(mgr, subscribe, T0, T0 + 120_000, stop_on_event=True)
    assert res == "market_added"
    # adopted on the first 2s-grid attempt at/after the slug appears
    assert attempts[-1] == T0 + 90_000
    diffs = _spacings(attempts)
    assert diffs and all(2_000 <= d <= 2_100 for d in diffs)
    assert state.current is not None and state.current.condition_id == "fresh"
    assert state.prewarm_gap_after_ms is None  # bookkeeping retired on adopt
    assert subscribed == ["fresh"]  # subscribed exactly once
    assert ("market_added", "prewarm_gap") in [
        (t, d.get("via")) for t, d in events if t == "market_added"]
    # settle path routed: throttle dropped so the NEXT lookahead starts at once
    assert "settlement_discovery" in [t for t, _ in events]
    out = capsys.readouterr().out
    assert "[recovery] phase=window_discovered asset=BTC tf=5m" in out
    assert "via=prewarm_gap" in out


@pytest.mark.asyncio
async def test_gap_tail_bounded_stop_no_attempts_after_180s():
    """The extension stays bounded: last attempt lands on the T+180s grid
    point and nothing fires after it."""
    events = []
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    state.current = None  # gapped promote
    state.prewarm_gap_after_ms = T0

    attempts = []
    _canned_discovery(mgr, state, attempts, serve_at_ms=None, market=None)

    async def subscribe(market):
        raise AssertionError("must not subscribe past the stop bound")

    assert await _drive(mgr, subscribe, T0, T0 + 240_000) is None
    assert attempts and max(attempts) == T0 + 180_000  # the stop is real, not vacuous
    assert all(a - T0 <= 180_000 for a in attempts)  # nothing after T+180s
    assert state.prewarm_gap_after_ms is None  # bookkeeping retired at the stop


# --- symmetric pre-window tail: current set, next undiscovered --------------


def _prewindow_setup(events, attempts, serve_at_ms, market):
    mgr = _mgr(events)
    state = mgr.states[("BTC", "5m")]
    state.current = _prev()  # ends at T0; lead is 30s
    _canned_discovery(mgr, state, attempts, serve_at_ms=serve_at_ms, market=market)
    return mgr, state


@pytest.mark.asyncio
async def test_prewindow_tail_polls_past_old_60s_stop_with_2s_cadence():
    """The pre-window tail gets the same extension: it keeps polling the
    adjacent slug past end+60s (attempt at end+150s) on the 2s cadence."""
    events = []
    attempts = []
    mgr, state = _prewindow_setup(events, attempts, serve_at_ms=None, market=None)

    async def subscribe(market):
        raise AssertionError("nothing served — must not subscribe")

    assert await _drive(mgr, subscribe, T0 - 30_000, T0 + 152_000) is None
    assert attempts[0] == T0 - 30_000  # polling starts at T-lead
    assert T0 + 150_000 in attempts  # still polling at end+150s
    assert state.prewarm_done_for_ts is None  # no early give-up marker
    diffs = _spacings(attempts)
    assert diffs and all(2_000 <= d <= 2_100 for d in diffs)


@pytest.mark.asyncio
async def test_prewindow_tail_bounded_stop_no_attempts_after_180s():
    """Pre-window tail bound: last attempt at end+180s, nothing after."""
    events = []
    attempts = []
    mgr, state = _prewindow_setup(events, attempts, serve_at_ms=None, market=None)

    async def subscribe(market):
        raise AssertionError("must not subscribe past the stop bound")

    assert await _drive(mgr, subscribe, T0 - 30_000, T0 + 240_000) is None
    assert attempts and max(attempts) == T0 + 180_000  # the stop is real
    assert all(a <= T0 + 180_000 for a in attempts)  # nothing after end+180s
    assert state.prewarm_done_for_ts == T0  # give-up marker set at the stop


@pytest.mark.asyncio
async def test_prewindow_tail_adopts_market_served_at_end_plus_90s(capsys):
    """Slug served at end+90s adopted into state.next with the prewarm
    provenance, once-per-window rollover_started, no double subscribe."""
    events = []
    attempts = []
    mgr, state = _prewindow_setup(
        events, attempts, serve_at_ms=T0 + 90_000, market=_fresh())

    subscribed = []

    async def subscribe(market):
        subscribed.append(market.condition_id)

    res = await _drive(mgr, subscribe, T0 - 30_000, T0 + 120_000, stop_on_event=True)
    assert res == "market_added"
    assert attempts[-1] == T0 + 90_000
    assert state.next is not None and state.next.condition_id == "fresh"
    assert state.is_rollover_window is True
    assert state.prewarm_done_for_ts == T0
    assert subscribed == ["fresh"]
    assert ("market_added", "prewarm") in [
        (t, d.get("via")) for t, d in events if t == "market_added"]
    assert [t for t, _ in events].count("rollover_started") == 1
    out = capsys.readouterr().out
    assert "via=prewarm" in out
