"""Recycle-stagger tests (2026-10-05 dual-down defect fix).

Defect: [ws:ETH:A] and [ws:ETH:B] planned-recycled the SAME second —
same-age legs hit the 270s ceiling together, so the swap ran with zero
peer cover (the exact episode fan-out dual-WS exists to prevent).

Fix under test: leg-name-derived phase offset (B short-first 135s, then
270s each, 280s absolute ceiling every cycle). Pure ws_pool state +
collector wrappers only — no network, no sockets, no sleeps (downtime
paths run with mocked sleeps/REST walk).
"""
import asyncio
import time
import types

import polymarket_collector.collector as collector_mod
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.ingest import ws_pool
from polymarket_collector.ingest.ws_pool import (
    RECYCLE_MAX_S,
    RECYCLE_STAGGER_S,
    RECYCLE_TARGET_S,
    ConnectionState,
    ShardPool,
    recycle_due_for_leg,
    recycle_target_for_leg,
    should_recycle,
)


def make_collector(tmp_path, **overrides):
    kwargs = {
        "assets": ["BTC"],
        "storage": {"data_dir": str(tmp_path)},
        "cursor_store": {"path": str(tmp_path / "cursor_state")},
        "timeframes": ["5m"],
    }
    kwargs.update(overrides)
    return Collector(CollectorConfig(**kwargs))


def stub_market(cid="cid-1", asset="BTC", up="tok-up-1", down="tok-down-1"):
    return types.SimpleNamespace(
        condition_id=cid,
        asset=asset,
        market_id="mid-1",
        series_id="BTC-5MIN",
        window_index=1,
        up_token_id=up,
        down_token_id=down,
        market_end_ts_ms=9_999_999_999_999,
        to_markets_row=lambda: {"condition_id": cid, "asset": asset},
    )


def _simulate_events(n_events=16):
    """Mirror of the transport contract: simultaneous (re)start, per-leg
    target from (name, recycles), phase advance (+1) on every OUR-close.
    Returns [(t_s, leg, age_at_fire)] in wall-clock seconds from start."""
    est = {"A": 0.0, "B": 0.0}
    cyc = {"A": 0, "B": 0}
    out = []
    for _ in range(n_events):
        due = {leg: est[leg] + recycle_target_for_leg(leg, cyc[leg]) for leg in ("A", "B")}
        leg = "A" if due["A"] <= due["B"] else "B"
        t = due[leg]
        out.append((t, leg, t - est[leg]))
        est[leg] = t
        cyc[leg] += 1
    return out


# -- separation across cycles, incl. restart (phase from identity) ----------

def test_stagger_constants():
    assert RECYCLE_TARGET_S == 270
    assert RECYCLE_MAX_S == 280
    assert RECYCLE_STAGGER_S == 135 == RECYCLE_TARGET_S // 2


def test_legs_never_recycle_within_window_across_cycles():
    events = _simulate_events(16)
    assert [leg for _, leg, _ in events[:4]] == ["B", "A", "B", "A"]
    assert events[0][0] == 135.0  # B short-first establishes the phase
    assert events[1][0] == 270.0  # A on its target
    for i in range(1, len(events)):
        gap = events[i][0] - events[i - 1][0]
        assert gap >= 120.0, f"legs {gap:.1f}s apart at {events[i]}"
    # Per-leg steady cadence is 270 each (B only short on its first cycle).
    for t, leg, age in events:
        if leg == "A":
            assert age == 270.0
        else:
            assert age in (135.0, 270.0)
        assert age <= RECYCLE_MAX_S


def test_restart_reestablishes_phase_from_identity():
    """A restart rebuilds fresh counters with both legs reconnecting
    together — the same interleave must emerge (no wall-clock memory)."""
    first_run = _simulate_events(6)
    second_run = _simulate_events(6)  # fresh state == restart
    assert [(t, leg) for t, leg, _ in first_run] == [(t, leg) for t, leg, _ in second_run]
    for i in range(1, len(first_run)):
        assert first_run[i][0] - first_run[i - 1][0] >= 120.0


def test_pool_state_needs_recycle_matches_simulation():
    now_ns = time.time_ns()
    pool = ShardPool(shard=["ETH"])
    for c in (pool.conn_a, pool.conn_b):
        c.established_ns = now_ns
    assert pool.conns_needing_recycle(now_ns) == []
    # B hits its short-first target while A still has 135s of cover.
    at_b = now_ns + int(135 * 1e9)
    assert pool.conns_needing_recycle(at_b) == ["B"]
    pool.conn_b.established_ns = at_b
    pool.conn_b.recycles += 1  # transport phase advance on OUR-close
    at_a = now_ns + int(270 * 1e9)
    assert pool.conns_needing_recycle(at_a) == ["A"]
    # Steady state: both on 270s cadence, never due together.
    pool.conn_a.established_ns = at_a
    pool.conn_a.recycles += 1
    at_b2 = at_b + int(270 * 1e9)
    assert pool.conns_needing_recycle(at_b2) == ["B"]
    pool.conn_b.established_ns = at_b2
    pool.conn_b.recycles += 1
    assert pool.conns_needing_recycle(at_b2 + int(1 * 1e9)) == []


# -- per-leg ceiling: never past the server kill ----------------------------

def test_per_leg_ceiling_never_past_280():
    for leg in ("A", "B"):
        for cycle in (0, 1, 2, 5):
            target = recycle_target_for_leg(leg, cycle)
            assert target <= RECYCLE_MAX_S, (leg, cycle, target)
            assert recycle_due_for_leg(target - 0.1, leg, cycle) is False
            assert recycle_due_for_leg(target, leg, cycle) is True
            assert recycle_due_for_leg(280.0, leg, cycle) is True
            assert recycle_due_for_leg(600.0, leg, cycle) is True
    # Absolute ceiling holds even for unknown legs/cycles (preemption first).
    assert recycle_due_for_leg(281.0, "Z", 99) is True
    assert recycle_due_for_leg(10.0, "Z", 99) is False
    # Helpers never raise on garbage (fail safe: no false due below target).
    assert recycle_due_for_leg("garbage", "B", 0) is False
    assert recycle_due_for_leg(None, None, None) is False
    assert recycle_target_for_leg(None, "xx") == 270.0
    c = ConnectionState(name="B", established_ns=0, recycles="xx")
    assert c.recycle_target_s() == 135.0 or c.recycle_target_s() == 270.0


def test_connection_state_ceiling_and_silence_independent(tmp_path):
    now_ns = time.time_ns()
    assert should_recycle(279.9) is False  # absolute ceiling untouched
    assert should_recycle(280.0) is True
    a = ConnectionState(name="A", established_ns=now_ns - int(281 * 1e9))
    assert a.needs_recycle(now_ns) is True  # ceiling still forces
    fresh = ConnectionState(name="B", established_ns=now_ns)
    assert fresh.needs_recycle(now_ns) is False
    assert fresh.is_silent(now_ns) is False  # no frames yet: never trips
    stale_ns = now_ns - int(600 * 1e9)
    stale = ConnectionState(name="B", established_ns=stale_ns, last_data_ns=stale_ns)
    assert stale.is_silent(now_ns) is True  # watchdog independent of recycle
    assert ws_pool.SILENCE_WATCHDOG_S == 120


# -- single-socket path untouched --------------------------------------------

def test_single_leg_mode_unaffected(tmp_path):
    col = make_collector(tmp_path)
    assert col._ws_mode() == "single"
    # Untagged wrapper keeps the legacy ceiling-only answer.
    assert col._ws_dual_recycle_due(279.9) is False
    assert col._ws_dual_recycle_due(280.0) is True
    assert col._ws_dual_recycle_due("garbage") is False
    # Single-socket keeps its own config-driven interval (240s), not ws_pool.
    assert col.config.ws.recycle_interval_seconds == 240
    # Named legs get the stagger; A default cycle matches legacy at 280.
    assert col._ws_dual_recycle_due(135.0, leg="B", cycle=0) is True
    assert col._ws_dual_recycle_due(134.9, leg="B", cycle=0) is False
    assert col._ws_dual_recycle_due(269.9, leg="A") is False
    assert col._ws_dual_recycle_due(270.0, leg="A") is True
    assert col._ws_dual_recycle_due(269.9, leg="B", cycle=3) is False
    # No pair in single mode: every frame flows (dedup inactive).
    frame = {"asset_id": "tok-up-1", "sequence_number": 7, "price": 0.5}
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], frame) is False
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], frame) is False


# -- flap rule intact: quiet flap vs honest dual-down episodes ---------------

def _running_collector(tmp_path, monkeypatch):
    col = make_collector(tmp_path)
    col._register_market(stub_market())
    col._running = True

    async def _no_walk(shard_set, now_ms):
        return None

    monkeypatch.setattr(col, "_reconnect_resync_walk", _no_walk)

    async def _no_sleep(delay):
        return None

    monkeypatch.setattr(collector_mod.asyncio, "sleep", _no_sleep)
    return col


# -- stagger-collapse re-arm on joint unplanned reconnect (2026-10-07) ------

def _steady_joint_pool(now_ns, a_recycles=2, b_recycles=3):
    """Steady-state pool after a joint drop: both ages ~0, both counters ≥1."""
    pool = ShardPool(shard=["ETH"])
    pool.conn_a.established_ns = now_ns
    pool.conn_a.recycles = a_recycles
    pool.conn_b.established_ns = now_ns
    pool.conn_b.recycles = b_recycles
    return pool


def _next_dues(pool, now_ns):
    """Seconds from now until each leg's next planned recycle fires."""
    return {
        "A": pool.conn_a.established_ns / 1e9 + recycle_target_for_leg("A", pool.conn_a.recycles) - now_ns / 1e9,
        "B": pool.conn_b.established_ns / 1e9 + recycle_target_for_leg("B", pool.conn_b.recycles) - now_ns / 1e9,
    }


def test_joint_reconnect_rearms_stagger_b_second(tmp_path):
    """Joint drop, B reheals second: B resets to short-first, dues split."""
    col = make_collector(tmp_path)
    now_ns = time.time_ns()
    pool = _steady_joint_pool(now_ns)
    # Pre-fix state: equal targets → same-second dues (the collapse).
    assert _next_dues(pool, now_ns)["A"] == _next_dues(pool, now_ns)["B"] == 270.0
    # Transport order on B's connect: stamp own clock, then re-arm check.
    pool.conn_b.established_ns = int(time.time_ns())
    assert col._dual_rearm_stagger_on_connect(pool, "B") is True
    assert pool.conn_b.recycles == 0
    assert pool.conn_a.recycles == 2  # A untouched
    dues = _next_dues(pool, pool.conn_b.established_ns)
    assert dues["B"] == 135.0
    assert dues["A"] >= 265.0  # stamped ~together, A still on 270
    assert abs(dues["A"] - dues["B"]) >= 120.0


def test_joint_reconnect_rearms_stagger_a_second(tmp_path):
    """Joint drop, A reheals second: peer B still reset (order-independent)."""
    col = make_collector(tmp_path)
    now_ns = time.time_ns()
    pool = ShardPool(shard=["ETH"])
    # B rehealed first (~1s ago, as rolling 1006s land): peer A stale then.
    pool.conn_b.established_ns = now_ns - int(1 * 1e9)
    pool.conn_b.recycles = 3
    pool.conn_a.established_ns = now_ns - int(300 * 1e9)  # pre-drop clock
    pool.conn_a.recycles = 2
    assert col._dual_rearm_stagger_on_connect(pool, "B") is False
    assert pool.conn_b.recycles == 3  # first leg: no re-arm (peer stale)
    # A reheals second: stamps own clock, peer B fresh → B rearmed.
    pool.conn_a.established_ns = int(time.time_ns())
    assert col._dual_rearm_stagger_on_connect(pool, "A") is True
    assert pool.conn_b.recycles == 0
    assert pool.conn_a.recycles == 2
    dues = _next_dues(pool, pool.conn_a.established_ns)
    assert abs(dues["A"] - dues["B"]) >= 120.0


def test_single_leg_flap_rearms_nothing(tmp_path):
    """Lone flap under peer cover: fresh leg, stale peer → counters kept."""
    col = make_collector(tmp_path)
    now_ns = time.time_ns()
    pool = ShardPool(shard=["ETH"])
    pool.conn_a.established_ns = now_ns - int(200 * 1e9)  # peer covering
    pool.conn_a.recycles = 2
    pool.conn_b.established_ns = now_ns  # B just flapped + rehealed
    pool.conn_b.recycles = 2
    assert col._dual_rearm_stagger_on_connect(pool, "B") is False
    assert pool.conn_b.recycles == 2
    assert pool.conn_a.recycles == 2
    # Mirror image: A flaps while B covers.
    pool.conn_b.established_ns = now_ns - int(200 * 1e9)
    pool.conn_a.established_ns = now_ns
    assert col._dual_rearm_stagger_on_connect(pool, "A") is False
    assert pool.conn_a.recycles == 2
    assert pool.conn_b.recycles == 2


def test_rearm_helpers_never_raise(tmp_path):
    col = make_collector(tmp_path)
    now_ns = time.time_ns()
    assert ws_pool.peer_fresh_for_rearm(now_ns, now_ns) is True
    assert ws_pool.peer_fresh_for_rearm(now_ns - int(31 * 1e9), now_ns) is False
    assert ws_pool.peer_fresh_for_rearm(0, now_ns) is False
    assert ws_pool.peer_fresh_for_rearm("garbage", now_ns) is False
    assert ws_pool.peer_fresh_for_rearm(None, None) is False
    assert ws_pool.peer_fresh_for_rearm(now_ns, "garbage") is False
    assert ws_pool.STAGGER_REARM_WINDOW_S == 30.0
    assert col._dual_rearm_stagger_on_connect(None, "B") is False
    assert col._dual_rearm_stagger_on_connect(object(), "B") is False
    pool = ShardPool(shard=["ETH"])  # never established: no re-arm, no raise
    assert col._dual_rearm_stagger_on_connect(pool, "B") is False
    assert col._dual_rearm_stagger_on_connect(pool, "garbage") is False


def test_flap_under_cover_still_quiet(tmp_path, monkeypatch):
    col = _running_collector(tmp_path, monkeypatch)
    before = len(col.resync._episodes)
    alive = {"A": False, "B": True}  # peer B still streams
    asyncio.run(col._dual_leg_downtime(
        ["BTC"], {"BTC"}, "BTC", "A", alive, asyncio.Lock(),
        {"epoch": False}, "ws_connection_close", 1, 0.0))
    assert alive["A"] is False
    assert len(col.resync._episodes) == before  # quiet flap: no episodes


def test_dual_down_still_mints_honest_episodes(tmp_path, monkeypatch):
    # Grace contract (2026-10-07 rolling-1006 fix): the first dual-down
    # observer marks books stale immediately but HOLDS the episode row for
    # DUAL_DOWN_EPISODE_GRACE_S; a still-dark pair past the grace mints the
    # same honest per-book episode, backdated to the second-leg drop.
    col = _running_collector(tmp_path, monkeypatch)
    lock = asyncio.Lock()
    alive = {"A": False, "B": False}  # both dark
    down = {"epoch": False}
    before = set(col.resync._episodes)
    asyncio.run(col._dual_leg_downtime(
        ["BTC"], {"BTC"}, "BTC", "A", alive, lock,
        down, "ws_connection_close:1006:", 1, 0.0))
    assert down["epoch"] is True  # first observer opens the epoch
    assert set(col.resync._episodes) - before == set()  # grace holds the mint
    assert col._dual_down_pending.get("BTC") is not None  # ...but arms it
    drop_ms = col._dual_down_pending["BTC"]["drop_ms"]
    # Second observer in the same epoch mints nothing more (no fan-out).
    asyncio.run(col._dual_leg_downtime(
        ["BTC"], {"BTC"}, "BTC", "B", alive, lock,
        down, "ws_connection_close:1006:", 1, 0.0))
    assert set(col.resync._episodes) - before == set()
    # Still dark past the grace: one honest episode, backdated to the drop.
    col._dual_down_pending["BTC"]["deadline_mono"] -= 1000
    asyncio.run(col._dual_leg_downtime(
        ["BTC"], {"BTC"}, "BTC", "A", alive, lock,
        down, "ws_connection_close:1006:", 2, 0.0))
    new = set(col.resync._episodes) - before
    assert len(new) == 1  # one book -> one honest episode (no fan-out)
    rid = next(iter(new))
    ep = col.resync._episodes[rid]
    assert "1006" in (ep.disconnect_reason or "")
    import datetime as _dt
    assert ep.disconnect_ts_utc == _dt.datetime.fromtimestamp(
        drop_ms / 1000, tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")
    assert col.resync.is_finished(rid) is False
    # Further dark observations mint nothing more (no fan-out).
    asyncio.run(col._dual_leg_downtime(
        ["BTC"], {"BTC"}, "BTC", "B", alive, lock,
        down, "ws_connection_close:1006:", 2, 0.0))
    assert set(col.resync._episodes) - before == new
