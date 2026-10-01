"""Tests for perfect-collector checkboxes (1)-(3): l2_raw + ws_pool + heal.

Real-data-only: no synthetic rows — frame_json must round-trip to the input
frame exactly; gaps stay NULL, never filled.
"""
import asyncio
import copy
import json
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_collector.storage.l2_raw import (
    L2_RAW_EVENT_TYPES,
    L2_RAW_SCHEMA,
    append_row,
    build_l2_raw_row,
    dedup_key_for_row,
)
from polymarket_collector.storage.schemas import SCHEMAS
from polymarket_collector.storage.parquet_writer import ParquetWriter
from polymarket_collector.ingest.heal import (
    build_books_request,
    chunk_tokens,
    heal_books_batched,
    parse_books_response,
    plan_heal,
)
from polymarket_collector.ingest.ws_pool import (
    RECYCLE_MAX_S,
    SILENCE_WATCHDOG_S,
    WS_MARKET_URL,
    ConnectionState,
    FrameDedup,
    ShardPool,
    ShardSubscriptions,
    build_hot_add,
    build_initial_subscribe,
    dedup_keys_for_message,
    should_recycle,
    silence_exceeded,
)

CID = "0x" + "ab" * 32
TOK = "1234567890123456789012345678901234567890123456789012345678901234"


def _book_frame():
    return {
        "event_type": "book",
        "asset_id": TOK,
        "condition_id": CID,
        "timestamp": 1759300000000,
        "bids": [{"price": "0.55", "size": "100"}],
        "asks": [{"price": "0.56", "size": "120"}],
    }


# -- (1) l2_raw schema -------------------------------------------------------

def test_l2_raw_schema_conformance():
    assert SCHEMAS["l2_raw"] is L2_RAW_SCHEMA
    fields = {f.name: f for f in L2_RAW_SCHEMA}
    assert set(fields) == {
        "ts_source", "ts_received_ns", "asset", "condition_id",
        "token_id", "event_type", "frame_json", "source_conn",
    }
    assert fields["ts_source"].type == pa.int64() and fields["ts_source"].nullable
    assert fields["ts_received_ns"].type == pa.int64() and not fields["ts_received_ns"].nullable
    assert fields["asset"].type == pa.string() and not fields["asset"].nullable
    assert fields["condition_id"].nullable and fields["token_id"].nullable
    assert not fields["event_type"].nullable and not fields["frame_json"].nullable
    assert fields["source_conn"].nullable


def test_l2_raw_tmp_rename_atomic():
    with tempfile.TemporaryDirectory() as tmp:
        w = ParquetWriter(data_dir=tmp, wal_enabled=False)
        # Partition date derives from ts_received_ns (receive-time hive);
        # pin it to the frame's day for a deterministic leaf.
        assert append_row(w, _book_frame(), asset="btc", source_conn="A",
                          ts_received_ns=1759300000000 * 1_000_000) is True
        w.flush()
        leaf = Path(tmp) / "l2_raw" / ("date=" + time.strftime("%Y-%m-%d", time.gmtime(1759300000))) / "asset=BTC"
        assert leaf.exists(), f"hive leaf missing: {leaf}"
        parts = list(leaf.glob("*.parquet"))
        assert len(parts) >= 1
        assert list(leaf.glob("*.tmp")) == []  # tmp+rename published, no orphans
        t = pq.read_table(str(parts[0]))
        assert set(t.column_names) == set(L2_RAW_SCHEMA.names)
        row = t.to_pylist()[0]
        assert json.loads(row["frame_json"]) == _book_frame()
        assert row["asset"] == "BTC" and row["source_conn"] == "A"


def test_l2_raw_dedup_redelivery_across_conns():
    with tempfile.TemporaryDirectory() as tmp:
        w = ParquetWriter(data_dir=tmp, wal_enabled=False)
        f = _book_frame()
        assert append_row(w, f, asset="BTC", source_conn="A", ts_received_ns=111) is True
        # Same frame redelivered on conn B with a fresh receive stamp dedupes.
        assert append_row(w, f, asset="BTC", source_conn="B", ts_received_ns=222) is True
        assert len([b for b in w._buffer if b.dataset == "l2_raw"]) == 1
        # A genuinely different frame (new exchange ts) is kept.
        f2 = dict(f)
        f2["timestamp"] = 1759300000001
        assert append_row(w, f2, asset="BTC", source_conn="B", ts_received_ns=333) is True
        assert len([b for b in w._buffer if b.dataset == "l2_raw"]) == 2
        # Frames with no frame content never collapse to one key.
        assert dedup_key_for_row({}) is None
        assert dedup_key_for_row({"frame_json": ""}) is None


def test_l2_raw_tick_size_and_resolved_passthrough():
    tick = {"event_type": "tick_size_change", "asset_id": TOK, "condition_id": CID,
            "old_tick_size": "0.001", "new_tick_size": "0.01", "timestamp": 1759300001000}
    resolved = {"event_type": "market_resolved", "condition_id": CID,
                "outcome": "up", "timestamp": 1759300002000}
    for frame in (tick, resolved):
        row = build_l2_raw_row(frame, asset="ETH")
        assert row["event_type"] == frame["event_type"]
        assert json.loads(row["frame_json"]) == frame
    assert build_l2_raw_row(tick, asset="ETH")["token_id"] == TOK
    assert build_l2_raw_row(resolved, asset="ETH")["condition_id"] == CID
    assert build_l2_raw_row(resolved, asset="ETH")["token_id"] is None  # honest NULL
    assert set(L2_RAW_EVENT_TYPES) >= {"book", "price_change", "last_trade_price",
                                       "tick_size_change", "market_resolved"}


def test_l2_raw_no_synthesis():
    frame = {"event_type": "price_change",
             "price_changes": [{"asset_id": TOK, "price": "0.55", "size": "10",
                                "best_bid": "0.54", "best_ask": "0.56"}],
             "timestamp": 1759300003000}
    snapshot = copy.deepcopy(frame)
    row = build_l2_raw_row(frame, asset="SOL", source_conn="B")
    assert frame == snapshot  # input never mutated
    assert json.loads(row["frame_json"]) == snapshot  # verbatim, nothing added
    assert row["ts_source"] == 1759300003000
    assert isinstance(row["ts_received_ns"], int)
    # Unknown source time stays NULL (never fabricated).
    row2 = build_l2_raw_row({"event_type": "book", "asset_id": TOK}, asset="SOL")
    assert row2["ts_source"] is None


# -- (2) ws_pool -------------------------------------------------------------

def test_ws_pool_subscribe_shapes():
    assert WS_MARKET_URL == "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    init = build_initial_subscribe([TOK, TOK, "tok2"])
    assert init == {"assets_ids": [TOK, "tok2"], "type": "market"}
    hot = build_hot_add(["tok2"])
    assert hot["assets_ids"] == ["tok2"] and hot["operation"] == "subscribe"


def test_ws_pool_dedup_token_seq_ts():
    d = FrameDedup()
    assert d.check(TOK, seq=7) is False
    assert d.check(TOK, seq=7) is True  # redelivery on conn B
    assert d.check(TOK, seq=8) is False
    assert d.check(TOK, ts=1759300000000) is False
    assert d.check(TOK, ts=1759300000000) is True
    assert d.check("other", seq=7) is False  # per-token keys
    assert d.check(None, seq=7) is False  # no token: never dedupe
    assert d.check(TOK) is False  # neither seq nor ts: never dedupe
    assert d.check(TOK) is False


def test_ws_pool_dedup_message_fanout():
    d = FrameDedup()
    msg = {"event_type": "price_change", "timestamp": 1759300000000,
           "price_changes": [{"asset_id": "t1", "price": "0.5"},
                             {"asset_id": "t2", "price": "0.6"}]}
    assert len(dedup_keys_for_message(msg)) == 2
    assert d.check_message(msg) is False
    assert d.check_message(msg) is True
    # Partial overlap (one new token) delivers once, then dedupes.
    msg2 = {"event_type": "price_change", "timestamp": 1759300000000,
            "price_changes": [{"asset_id": "t1", "price": "0.5"},
                              {"asset_id": "t3", "price": "0.6"}]}
    assert d.check_message(msg2) is False
    assert d.check_message(msg2) is True
    assert d.check_message({"event_type": "book"}) is False  # keyless: deliver


def test_ws_pool_recycle_and_silence():
    assert RECYCLE_MAX_S == 280
    assert SILENCE_WATCHDOG_S == 120
    assert should_recycle(279.9) is False
    assert should_recycle(280.0) is True
    now = time.time_ns()
    assert silence_exceeded(now - 119_000_000_000, now) is False
    assert silence_exceeded(now - 121_000_000_000, now) is True
    assert silence_exceeded(None, now) is False
    c = ConnectionState(name="A", established_ns=now - 281 * 10**9)
    assert c.needs_recycle(now) is True
    pool = ShardPool(shard=["BTC"])
    pool.conn_a.established_ns = now
    pool.conn_b.established_ns = now
    assert pool.conns_needing_recycle(now) == []
    pool.conn_a.established_ns = now - 281 * 10**9
    assert pool.conns_needing_recycle(now) == ["A"]
    subs = ShardSubscriptions()
    subs.initial_payload([TOK])
    assert subs.hot_add_payload([TOK]) is None  # nothing new, no resend
    hot = subs.hot_add_payload(["tok-new"])
    assert hot is not None and hot["operation"] == "subscribe"
    assert hot["assets_ids"] == ["tok-new"]


# -- (3) heal ----------------------------------------------------------------

def test_heal_batched_single_round_trip():
    calls = []

    async def fake_post(url, payload):
        calls.append((url, payload))
        return _Resp(200, [{"token_id": p["token_id"],
                            "bids": [{"price": "0.5", "size": "1"}],
                            "asks": []} for p in payload])

    tokens = [f"tok{i}" for i in range(250)]
    res = asyncio.run(heal_books_batched(tokens, fake_post))
    assert res.errors == [] and len(res.books) == 250 and res.missing == []
    assert len(calls) == 3  # 250 tokens in 3 POSTs, not 500 GETs
    assert calls[0][0] == "https://clob.polymarket.com/books"
    assert len(calls[0][1]) == 100
    url, payload = build_books_request(tokens[:3])
    assert payload == [{"token_id": "tok0"}, {"token_id": "tok1"}, {"token_id": "tok2"}]
    assert [c for c in chunk_tokens(tokens)] and sum(len(c) for c in chunk_tokens(tokens)) == 250


def test_heal_ended_window_precheck_supersede():
    live_cid = "0x" + "11" * 32
    dead_cid = "0x" + "22" * 32

    def resolver(cid):
        if cid == dead_cid:
            return (1759300000000 - 60_000, "active")  # window already ended
        if cid == "closed-cid":
            return (1759300000000 + 60_000, "closed")
        if cid == live_cid:
            return (1759300000000 + 60_000, "active")
        return None  # unknown: discovery may lag

    plan = plan_heal(["t-live", "t-dead", "t-closed", "t-unknown"],
                     condition_by_token={"t-live": live_cid, "t-dead": dead_cid,
                                         "t-closed": "closed-cid", "t-unknown": "nope"},
                     resolver=resolver, now_ms=1759300000000)
    assert plan.live_token_ids == ["t-live", "t-unknown"]  # unknown stays live
    assert {s.token_id: s.reason for s in plan.superseded} == {
        "t-dead": "ended_window", "t-closed": "closed_status"}

    async def fail_if_called(url, payload):
        raise AssertionError("superseded tokens must burn no request")

    res = asyncio.run(heal_books_batched(
        ["t-dead"], fail_if_called,
        condition_by_token={"t-dead": dead_cid}, resolver=resolver, now_ms=1759300000000))
    assert res.books == {} and len(res.superseded) == 1


def test_heal_parse_and_errors():
    books = parse_books_response([
        {"token_id": "a", "bids": [], "asks": []},  # genuine empty: kept
        {"token_id": "b", "bids": [{"price": "0.5", "size": "2"}]},  # half: skipped
        {"nope": 1},
    ])
    assert set(books) == {"a"}
    books2 = parse_books_response({"c": {"bids": [{"price": "0.4", "size": "1"}], "asks": []}})
    assert set(books2) == {"c"}

    async def post_429(url, payload):
        return _Resp(429, [])

    res = asyncio.run(heal_books_batched(["x"], post_429))
    assert res.books == {} and res.missing == ["x"] and res.errors != []

    async def post_boom(url, payload):
        raise TimeoutError("down")

    res2 = asyncio.run(heal_books_batched(["y"], post_boom))
    assert res2.missing == ["y"] and res2.errors != []


class _Resp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body
