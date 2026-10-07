"""Dual-down episode-mint grace tests (2026-10-07 rolling-1006 fix).

Real-data-only: no network, no sockets — the dual-leg downtime helper is
driven directly with recording stand-ins; sleeps/backoff/REST are stubbed.
Nothing here writes to data/ (tmp_path only, episode persists captured via
a fake _safe_persist seam, never the FS writer).

Contract (branch perfect/pmdata-parity):
  1. Blink (~9s dual-down inside the grace) mints NOTHING — books still go
     stale immediately (stale snapshots flow) and collector_events still fire
     (complete evidence, no episode row).
  2. Genuine outage (dark past the grace) mints exactly one episode per book,
     BACKDATED to the second-leg drop (disconnect_ts == drop, not mint), and
     the REST walk runs only after the mint (never inside the grace — the
     walk's find-or-create would defeat it).
  3. Flap under peer cover is untouched: no pending, no stale, no episodes.
  4. The missing chaos case: kill B ~1s after A (rolling 1006 shape — the old
     suite covered single-flap and simultaneous-kill only) must not fan out
     while the pair reheals inside the grace.
"""
import asyncio
import datetime
import random
import time
import types

import polymarket_collector.collector as collector_mod
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig

GRACE = collector_mod.DUAL_DOWN_EPISODE_GRACE_S
assert GRACE == 12.0, "grace constant moved — update rationale + tests together"

REASON_1006 = "ws_connection_close:1006:"


def make_collector(tmp_path, **overrides):
    kwargs = {
        "assets": ["BTC"],
        "storage": {"data_dir": str(tmp_path)},
        "cursor_store": {"path": str(tmp_path / "cursor_state")},
        "timeframes": ["5m"],
    }
    kwargs.update(overrides)
    return Collector(CollectorConfig(**kwargs))


def stub_market(cid, asset="BTC", up="tok-up-1", down="tok-down-1"):
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


def two_book_collector(tmp_path):
    """Collector with two live BTC books (per-book fanout counting)."""
    col = make_collector(tmp_path)
    col._register_market(stub_market("cid-1", up="tok-up-1", down="tok-down-1"))
    col._register_market(stub_market("cid-2", up="tok-up-2", down="tok-down-2"))
    assert set(col.books) == {"cid-1", "cid-2"}
    return col


class DualPair:
    """Shared per-shard dual-leg state, mirroring _run_shard_loop_dual."""

    def __init__(self, col, label="BTC", shard=None):
        self.col = col
        self.label = label
        self.shard = shard or ["BTC"]
        self.shard_set = set(self.shard)
        self.alive = {"A": True, "B": True}
        self.lock = asyncio.Lock()
        self.epoch = {"epoch": False}

    def down(self, leg, reason=REASON_1006, attempt=1):
        return asyncio.run(self.col._dual_leg_downtime(
            self.shard, self.shard_set, self.label, leg,
            self.alive, self.lock, self.epoch, reason, attempt, 0.0))


def tap_events(col):
    """Capture collector_events from BOTH emit paths (col + resync manager)."""
    events = []
    orig_col = col.on_event
    orig_resync = col.resync.on_event

    def _tap(event_type, details):
        events.append((event_type, details))

    def _col_tap(event_type, details):
        _tap(event_type, details)
        return orig_col(event_type, details)

    def _resync_tap(event_type, details):
        _tap(event_type, details)
        return orig_resync(event_type, details)

    col.on_event = _col_tap
    col.resync.on_event = _resync_tap
    return events


def tap_persists(col, monkeypatch):
    """Fake writer seam: record every episode-row persist (payload, where)."""
    persisted = []
    orig = col.resync._safe_persist

    def _rec(payload, where):
        persisted.append((dict(payload), where))
        return orig(payload, where)

    monkeypatch.setattr(col.resync, "_safe_persist", _rec)
    return persisted


def run_fast(col, monkeypatch):
    """Neutralise backoff/REST sleeps so a RUNNING collector stays in-test fast."""
    col._running = True
    walks = []

    async def _fake_walk(shard_set, now_ms):
        walks.append((set(shard_set), now_ms))
        return None

    monkeypatch.setattr(col, "_reconnect_resync_walk", _fake_walk)
    monkeypatch.setattr(collector_mod, "exponential_backoff", lambda *a, **k: 0.0)
    monkeypatch.setattr(random, "uniform", lambda a, b: 0.0)
    return walks


def _disc_ms(ep):
    return int(datetime.datetime.fromisoformat(
        ep.disconnect_ts_utc.replace("Z", "+00:00")).timestamp() * 1000)


def _baseline(col):
    """Episode ids predating the scenario (registration mints market_added)."""
    return set(col.resync._episodes)


# -- flap-quiet path untouched -------------------------------------------------

def test_flap_quiet_unchanged(tmp_path, monkeypatch):
    """A drops while B streams: no pending, no stale, no episodes, no rows."""
    col = two_book_collector(tmp_path)
    persisted = tap_persists(col, monkeypatch)
    events = tap_events(col)
    base = _baseline(col)
    before_states = {cid: b.book_state.value for cid, b in col.books.items()}
    pair = DualPair(col)
    pair.down("A")
    assert col._dual_down_pending == {}
    assert set(col.resync._episodes) == base
    assert persisted == []
    for cid, book in col.books.items():
        assert book.book_state.value == before_states[cid], "flap marks nothing stale"
    kinds = [str(t) for t, _ in events]
    assert any("ws_reconnect_attempt" in k for k in kinds)
    assert not any("ws_disconnected" in k for k in kinds)


# -- blink: stale-now, mint-never ----------------------------------------------

def test_blink_9s_mints_nothing_but_stale_snapshots(tmp_path, monkeypatch):
    """Dual-down rehealed inside the grace: stale books + events, zero rows."""
    col = two_book_collector(tmp_path)
    persisted = tap_persists(col, monkeypatch)
    events = tap_events(col)
    walks = run_fast(col, monkeypatch)
    base = _baseline(col)
    pair = DualPair(col)
    pair.down("A")  # flap under cover — nothing
    assert set(col.resync._episodes) == base
    pair.down("B", attempt=1)  # second leg drops -> dual-down, grace armed
    assert walks == []
    rec = col._dual_down_pending.get("BTC")
    assert rec is not None and rec["minted"] is False
    assert set(col.resync._episodes) == base
    assert persisted == []
    # Snapshot honesty is immediate: stale books flow stale rows next tick.
    for book in col.books.values():
        assert book.book_state.value == "stale"
        # Orphan rid by design — joins nothing until a genuine outage mints
        # (stale_no_episode trail, same as the H2/cursor-recovery precedent).
        assert book.resync_id not in col.resync._episodes
    # Collector_events evidence still fires for the blink.
    assert any("backoff_dual_down" in str(d) for _, d in events)
    assert not any("ws_disconnected" in str(t) for t, _ in events)
    # Reheal inside the grace (the _run_dual_conn any-up path calls clear):
    # closes silently with NO episode row.
    col._dual_down_grace_clear("BTC")
    assert col._dual_down_pending == {}
    assert set(col.resync._episodes) == base
    assert persisted == []
    col._running = False


# -- the missing chaos case: kill B ~1s after A ---------------------------------

def test_kill_b_1s_after_a_no_fanout(tmp_path, monkeypatch):
    """Rolling-1006 shape (prod 13:13 BTC): A peer_covering, B dual-down ~1s
    later, pair reheals ~9s after — must mint nothing (old suite covered
    single-flap and simultaneous-kill only, never this cascade)."""
    col = two_book_collector(tmp_path)
    persisted = tap_persists(col, monkeypatch)
    run_fast(col, monkeypatch)
    base = _baseline(col)
    pair = DualPair(col)
    t_a = int(time.time() * 1000)
    pair.down("A")  # 13:13:11.792 A peer_covering 1006
    assert set(col.resync._episodes) == base
    time.sleep(1.1)  # kills roll ~1s apart, server-side, never simultaneous
    t_pre_b = int(time.time() * 1000)
    pair.down("B", attempt=1)  # 13:13:12.766 B backoff_dual_down 1006
    # Second-leg drop arms the grace (one pending record, shared epoch — the
    # trailing leg must NOT re-arm or reset the window).
    rec = col._dual_down_pending.get("BTC")
    assert rec is not None
    assert t_pre_b <= rec["drop_ms"] <= int(time.time() * 1000)
    assert rec["drop_ms"] > t_a, "grace anchors on the SECOND-leg drop"
    assert set(col.resync._episodes) == base, "no fanout while dark inside the grace"
    assert persisted == []
    # Reheal ~9s later (inside the 12s grace) — 13:13:20/21 reconnects.
    col._dual_down_grace_clear("BTC")
    assert set(col.resync._episodes) == base
    assert persisted == [], "a 9s dual-blink leaves no episode row"
    col._running = False


# -- genuine outage: one backdated episode per book, walk deferred --------------

def test_outage_20s_mints_exactly_one_backdated_episode_per_book(tmp_path, monkeypatch):
    """Dark past the grace: exactly one episode per book, backdated to the
    second-leg drop, and the REST walk runs only after the mint."""
    col = two_book_collector(tmp_path)
    persisted = tap_persists(col, monkeypatch)
    walks = run_fast(col, monkeypatch)
    base = _baseline(col)
    pair = DualPair(col)
    pair.down("A")  # flap under cover — nothing
    assert set(col.resync._episodes) == base
    pair.down("B", attempt=1)  # dual-down: stale-now, mint + walk deferred
    drop_ms = col._dual_down_pending["BTC"]["drop_ms"]
    assert set(col.resync._episodes) == base
    assert walks == [], "REST walk must wait out the grace (its find-or-create would mint)"
    # Still dark 20s later: the next downtime observation fires the mint.
    col._dual_down_pending["BTC"]["deadline_mono"] -= 1000
    pair.down("A", attempt=2)
    new = set(col.resync._episodes) - base
    assert len(new) == 2, "exactly one episode per book, no fanout growth"
    by_cid = {col.resync._episodes[r].condition_id: col.resync._episodes[r] for r in new}
    assert set(by_cid) == {"cid-1", "cid-2"}
    for ep in by_cid.values():
        assert "1006" in ep.disconnect_reason
        assert _disc_ms(ep) == drop_ms, "disconnect_ts == second-leg drop, not mint"
        assert col.books[ep.condition_id].book_state.value == "stale"
    disc_rows = [p for p, w in persisted if w == "handle_disconnect"]
    assert len(disc_rows) == 2
    assert len(walks) == 1, "walk runs once, after the mint"
    # Idempotent: further dark observations mint nothing new.
    pair.down("B", attempt=2)
    assert set(col.resync._episodes) - base == new
    col._running = False


# -- backdate arithmetic --------------------------------------------------------

def test_backdate_arithmetic_disconnect_is_drop_not_mint(tmp_path, monkeypatch):
    """disconnect_ts_utc == drop instant exactly; detected carries mint time;
    gap_duration derives from the honest (backdated) start."""
    col = two_book_collector(tmp_path)
    run_fast(col, monkeypatch)
    base = _baseline(col)
    drop_ms = col._dual_down_grace_arm("BTC", ["BTC"], REASON_1006)
    col._dual_down_pending["BTC"]["deadline_mono"] -= 1000  # 20s later, still dark
    before_fire_ms = int(time.time() * 1000)
    assert col._dual_down_grace_fire_if_due("BTC", ["BTC"]) is True
    assert drop_ms <= before_fire_ms
    expected_iso = datetime.datetime.fromtimestamp(
        drop_ms / 1000, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    new = set(col.resync._episodes) - base
    assert len(new) == 2
    for rid in new:
        ep = col.resync._episodes[rid]
        assert ep.disconnect_ts_utc == expected_iso
        _det = datetime.datetime.fromisoformat(ep.detected_ts_utc.replace("Z", "+00:00"))
        _disc = datetime.datetime.fromisoformat(ep.disconnect_ts_utc.replace("Z", "+00:00"))
        assert _det >= _disc, "detected (mint) never predates the backdated drop"
    # Gap honesty: reconnect - backdated drop, and the missed-snapshot estimate.
    for rid in list(new):
        col.resync.handle_reconnect(rid)
    for rid in new:
        ep = col.resync._episodes[rid]
        _recon = datetime.datetime.fromisoformat(ep.reconnect_ts_utc.replace("Z", "+00:00"))
        _disc = datetime.datetime.fromisoformat(ep.disconnect_ts_utc.replace("Z", "+00:00"))
        assert ep.gap_duration_ms == int((_recon - _disc).total_seconds() * 1000)
        assert ep.snapshots_missed_estimate == ep.gap_duration_ms // 500
    col._running = False
