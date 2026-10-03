"""Dual-WS wiring tests (perfect-collector §3.2 workstream 1A).

Covers the collector-side wiring of ingest.ws_pool into the shard loop:
config-flag fallback selection, wire-identical payload builders, pair-wide
redelivery dedup, OUR-schedule recycle/silence decisions, per-leg fan-out,
and the flap-vs-dual-down episode rule. No network, no sockets — transports
are recording stand-ins; sleeps are avoided (downtime tests run with the
collector stopped so the helper returns before any backoff sleep).
"""
import asyncio
import json
import types

import polymarket_collector.collector as collector_mod
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig
from polymarket_collector.ingest import ws_pool
from polymarket_collector.ingest.ws_pool import ShardSubscriptions


def make_collector(tmp_path, **overrides):
    kwargs = {
        "assets": ["BTC"],
        "storage": {"data_dir": str(tmp_path)},
        "cursor_store": {"path": str(tmp_path / "cursor_state")},
        "timeframes": ["5m"],
    }
    kwargs.update(overrides)
    return Collector(CollectorConfig(**kwargs))


class RecordingWS:
    """Recording stand-in for a websocket connection (no network)."""

    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)


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


# -- fallback selection ------------------------------------------------------

def test_dual_flag_defaults_off_single_mode(tmp_path):
    col = make_collector(tmp_path)
    assert col.config.ws.dual_enabled is False
    assert col._ws_dual_enabled() is False
    assert col._ws_mode() == "single"


def test_dual_mode_needs_flag_and_library(tmp_path, monkeypatch):
    col = make_collector(tmp_path, ws={"dual_enabled": True})
    assert col._ws_dual_enabled() is True
    assert col._ws_mode() == "dual"
    # No websockets library -> single-socket fallback even with the flag on.
    monkeypatch.setattr(collector_mod, "HAS_WEBSOCKETS", False)
    assert col._ws_mode() == "single"


def test_helpers_never_raise_on_broken_config(tmp_path):
    col = make_collector(tmp_path)
    col.config = None  # type: ignore
    assert col._ws_dual_enabled() is False
    assert col._ws_mode() == "single"
    assert col._ws_dual_recycle_due("garbage") is False
    assert col._ws_dual_silence_due(None, 0) is False
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], None) is False
    assert col._ws_initial_payload(None) == {"assets_ids": [], "type": "market"}


# -- payload builders (wire-identical to the legacy loop) --------------------

def test_initial_payload_shape_and_dedup(tmp_path):
    col = make_collector(tmp_path)
    payload = col._ws_initial_payload(["t1", "t1", "t2"])
    assert payload == {"assets_ids": ["t1", "t2"], "type": "market"}
    assert payload == ws_pool.build_initial_subscribe(["t1", "t1", "t2"])
    # Byte-identical to what the single-socket loop sent before wiring.
    assert json.dumps(payload) == json.dumps({"assets_ids": ["t1", "t2"], "type": "market"})


def test_hot_add_payload_shape(tmp_path):
    col = make_collector(tmp_path)
    payload = col._ws_hot_add_payload(["t9"])
    assert payload["assets_ids"] == ["t9"]
    assert payload["operation"] == "subscribe"
    assert payload["type"] == "market"
    assert payload["custom_feature_enabled"] is True
    legacy = {"assets_ids": ["t9"], "operation": "subscribe",
              "type": "market", "custom_feature_enabled": True}
    assert json.dumps(payload) == json.dumps(legacy)


def test_hot_add_delta_only_no_resend(tmp_path):
    col = make_collector(tmp_path)
    pool = col._ws_pool_for_shard("BTC", ["BTC"])
    pool.subs.initial_payload(["t1"])
    assert pool.subs.hot_add_payload(["t1"]) is None  # nothing new, no resend
    hot = pool.subs.hot_add_payload(["t1", "t2"])
    assert hot is not None and hot["assets_ids"] == ["t2"]  # delta only
    assert hot["operation"] == "subscribe"


# -- dedup across the pair ----------------------------------------------------

def test_frame_dedup_only_in_dual_mode(tmp_path):
    col = make_collector(tmp_path)
    frame = {"asset_id": "tok-up-1", "sequence_number": 7, "price": 0.5}
    # Single-socket default: no pair exists, every frame flows (helper False).
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], frame) is False
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], frame) is False
    col.config.ws.dual_enabled = True
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], frame) is False  # first delivery
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], frame) is True  # B-leg redelivery
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], dict(frame, sequence_number=8)) is False
    # Keyless frames always deliver (never deduped on nothing).
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], {"event_type": "book"}) is False
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], {"event_type": "book"}) is False
    # Non-frames never dedupe.
    assert col._ws_frame_is_duplicate("BTC", ["BTC"], ["not", "a", "dict"]) is False


def test_pool_identity_and_isolation(tmp_path):
    col = make_collector(tmp_path)
    col.config.ws.dual_enabled = True
    p1 = col._ws_pool_for_shard("BTC", ["BTC"])
    assert col._ws_pool_for_shard("BTC", ["BTC"]) is p1  # stable per label
    p2 = col._ws_pool_for_shard("BTC+ETH", ["BTC", "ETH"])
    assert p2 is not p1  # shards never share a dedup window
    frame = {"asset_id": "tok-x", "sequence_number": 1}
    assert p1.dedup.check_message(frame) is False
    assert p1.dedup.check_message(frame) is True
    assert p2.dedup.check_message(frame) is False  # isolated window


# -- recycle / silence decisions ----------------------------------------------

def test_dual_recycle_ceiling_preempts_server_kill(tmp_path):
    col = make_collector(tmp_path)
    assert ws_pool.RECYCLE_MAX_S == 280
    assert col._ws_dual_recycle_due(279.9) is False
    assert col._ws_dual_recycle_due(280.0) is True
    assert col._ws_dual_recycle_due(400.0) is True
    # The single-socket path keeps its own config-driven interval (240s),
    # untouched by the ws_pool ceiling.
    assert col.config.ws.recycle_interval_seconds == 240


def test_dual_silence_watchdog(tmp_path):
    col = make_collector(tmp_path)
    assert ws_pool.SILENCE_WATCHDOG_S == 120
    now = 1_000_000_000_000_000_000
    assert col._ws_dual_silence_due(now - 119_000_000_000, now) is False
    assert col._ws_dual_silence_due(now - 121_000_000_000, now) is True
    assert col._ws_dual_silence_due(None, now) is False


# -- per-leg fan-out (recording stand-ins, no network) -------------------------

def _fanout_collector(tmp_path):
    col = make_collector(tmp_path)
    m1 = stub_market()
    col.rollover = types.SimpleNamespace(active_markets=lambda a: [m1] if a == "BTC" else [])
    return col, m1


def test_fanout_initial_on_both_legs_then_quiet(tmp_path):
    col, _m1 = _fanout_collector(tmp_path)
    ws_a, ws_b = RecordingWS(), RecordingWS()
    holder = {"A": ws_a, "B": ws_b,
              "subs": {"A": ShardSubscriptions(), "B": ShardSubscriptions()}}
    assert asyncio.run(col._dual_fanout_subscription(holder, ["BTC"], "BTC")) is True
    assert len(ws_a.sent) == 1 and len(ws_b.sent) == 1
    for raw in (ws_a.sent[0], ws_b.sent[0]):
        payload = json.loads(raw)
        assert payload == {"assets_ids": ["tok-up-1", "tok-down-1"], "type": "market"}
    # Nothing new -> no resend on either leg.
    assert asyncio.run(col._dual_fanout_subscription(holder, ["BTC"], "BTC")) is False
    assert len(ws_a.sent) == 1 and len(ws_b.sent) == 1


def test_fanout_hot_add_delta_on_live_legs_only(tmp_path):
    col, m1 = _fanout_collector(tmp_path)
    ws_a, ws_b = RecordingWS(), RecordingWS()
    holder = {"A": ws_a, "B": ws_b,
              "subs": {"A": ShardSubscriptions(), "B": ShardSubscriptions()}}
    assert asyncio.run(col._dual_fanout_subscription(holder, ["BTC"], "BTC")) is True
    # A new window adds one token; leg A is down -> only B hot-adds the delta.
    m2 = stub_market(cid="cid-new", up="tok-up-2", down="tok-down-2")
    col.rollover = types.SimpleNamespace(
        active_markets=lambda a: [m1, m2] if a == "BTC" else [])
    holder["A"] = None
    assert asyncio.run(col._dual_fanout_subscription(holder, ["BTC"], "BTC")) is True
    assert len(ws_a.sent) == 1  # down leg untouched
    assert len(ws_b.sent) == 2
    hot = json.loads(ws_b.sent[1])
    assert hot["operation"] == "subscribe"
    assert hot["assets_ids"] == ["tok-up-2", "tok-down-2"]  # delta only


# -- registration + flap-vs-dual-down ------------------------------------------

def test_register_market_new_and_duplicate(tmp_path):
    col = make_collector(tmp_path)
    m = stub_market()
    assert col._register_market(m) is True
    assert m.condition_id in col.markets
    assert m.condition_id in col.books
    assert col._register_market(m) is False  # duplicate, no double row


def test_flap_under_cover_opens_no_episode(tmp_path):
    col = make_collector(tmp_path)
    m = stub_market()
    col._register_market(m)
    before = len(col.resync._episodes)
    alive = {"A": False, "B": True}  # peer B still streams
    lock = asyncio.Lock()
    asyncio.run(col._dual_leg_downtime(
        ["BTC"], {"BTC"}, "BTC", "A", alive, lock,
        {"epoch": False}, "ws_connection_close", 1, 0.0))
    assert alive["A"] is False
    assert len(col.resync._episodes) == before  # quiet flap: no episodes
