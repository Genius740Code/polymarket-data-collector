"""WS event-type coverage at the shard-loop dispatch (spec §4).

Dispatch rule (``Collector._handle_shard_frame``): l2_raw is fail-OPEN — every
dict frame (book, price_change, last_trade_price, tick_size_change,
market_resolved, and unknown future types) is stored verbatim with no
thresholds and a source_conn leg tag. Books and episodes are fail-CLOSED:
only book-shaped frames (``price_changes`` list, or ``bids``/``asks`` keys)
may reach ``apply_ws_message``; nothing else mutates books or mints episodes.

Real-data-only: no network, no fabricated rows — frames are fixed literals
and ``frame_json`` must round-trip to the input exactly.
"""
import copy
import json
import tempfile
from collections import defaultdict
from types import SimpleNamespace

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.storage.parquet_writer import ParquetWriter

CID = "0x" + "ab" * 32
TOK_UP = "1" * 64
TOK_DOWN = "2" * 64
TS = 1759300000000


def _frames():
    return {
        "book": {
            "event_type": "book", "asset_id": TOK_UP, "condition_id": CID,
            "timestamp": TS, "bids": [["0.55", "100"]], "asks": [["0.56", "120"]],
            "hash": "abcdef1234567890",
        },
        "price_change": {
            "event_type": "price_change", "timestamp": TS + 1000,
            "price_changes": [{"asset_id": TOK_UP, "price": "0.551", "size": "50",
                               "side": "BUY", "best_bid": "0.551", "best_ask": "0.56"}],
        },
        "last_trade_price": {
            "event_type": "last_trade_price", "asset_id": TOK_UP,
            "condition_id": CID, "price": "0.55", "size": "10",
            "side": "BUY", "timestamp": TS + 2000,
        },
        "tick_size_change": {
            "event_type": "tick_size_change", "asset_id": TOK_UP,
            "condition_id": CID, "old_tick_size": "0.01",
            "new_tick_size": "0.001", "timestamp": TS + 3000,
        },
        "market_resolved": {
            "event_type": "market_resolved", "condition_id": CID,
            "outcome": "up", "timestamp": TS + 4000,
        },
        "mystery_v2": {
            "event_type": "mystery_v2", "asset_id": TOK_UP,
            "condition_id": CID, "timestamp": TS + 5000,
        },
    }


def _make_collector():
    """Live dispatch methods on stub transport state (no sockets, no I/O)."""
    tmp = tempfile.mkdtemp()
    c = Collector.__new__(Collector)
    c.books = {}
    c._books_by_token = {}
    c._ws_noise_throttle = defaultdict(int)
    c._last_frame_ns_per_book = {}
    c._threshold_config_id = "test-ws-coverage"
    c.writer = ParquetWriter(data_dir=tmp, wal_enabled=False)
    market = SimpleNamespace(
        asset="BTC", condition_id=CID, market_id="111", series_id="BTC-5m",
        window_index=0, up_token_id=TOK_UP, down_token_id=TOK_DOWN,
    )
    c.markets = {CID: market}

    class _Rollover:
        def active_markets(self, _a):
            return [market]

    c.rollover = _Rollover()
    resync_calls: list = []

    class _Resync:
        def buffer_message(self, *a):
            resync_calls.append(("buffer", a))

        def newest_open_buffer_id(self, _au):
            return ""

        def handle_sequence_gap(self, *a, **k):
            resync_calls.append(("gap", a, k))

    c.resync = _Resync()
    events: list = []
    c.on_event = lambda et, d: events.append((et, d))  # noqa: E731
    episode_calls: list = []
    c._ensure_episode_for_stale_book = lambda *a, **k: episode_calls.append((a, k))  # noqa: E731
    book = OrderBookState(
        asset="BTC", condition_id=CID, market_id="111", series_id="BTC-5m",
        window_index=0, up_token_id=TOK_UP, down_token_id=TOK_DOWN,
        market_end_ts_ms=9999999999999,
    )
    c.books[CID] = book
    c._books_by_token[TOK_UP] = book
    c._books_by_token[TOK_DOWN] = book
    return c, book, events, resync_calls, episode_calls


def _l2_rows(c):
    return [br.row for br in list(c.writer._l2_buffer) if br.dataset == "l2_raw"]


def _book_fingerprint(book):
    return {
        "state": str(book.book_state),
        "up_bids": [(l.price, l.size) for l in book.up.bids.levels if l.price is not None],
        "up_asks": [(l.price, l.size) for l in book.up.asks.levels if l.price is not None],
        "seq": dict(book.sequence_numbers),
        "pending": copy.deepcopy(book.pending_events),
    }


def test_all_event_types_reach_l2_raw_verbatim():
    c, _book, _events, _rc, _ec = _make_collector()
    frames = _frames()
    for name, f in frames.items():
        snapshot = copy.deepcopy(f)
        c._handle_shard_frame(dict(f), ["BTC"], source_conn="A")
        assert f == snapshot, f"dispatch mutated input frame {name}"
    rows = _l2_rows(c)
    assert len(rows) == len(frames)
    by_type = {r["event_type"]: r for r in rows}
    assert set(by_type) == set(frames)
    for name, f in frames.items():
        row = by_type[name]
        assert json.loads(row["frame_json"]) == f  # verbatim, nothing added
        assert row["source_conn"] == "A"
        assert row["asset"] == "BTC"
    assert by_type["tick_size_change"]["token_id"] == TOK_UP
    assert by_type["market_resolved"]["condition_id"] == CID
    assert by_type["market_resolved"]["token_id"] is None  # honest NULL
    assert by_type["mystery_v2"]["token_id"] == TOK_UP  # unknown: still stored


def test_market_keyed_resolved_frame_attributed():
    c, _book, _events, _rc, _ec = _make_collector()
    frame = {"event_type": "market_resolved", "market": CID,
             "outcome": "up", "timestamp": TS + 6000}
    c._handle_shard_frame(dict(frame), ["BTC"], source_conn="B")
    rows = _l2_rows(c)
    assert len(rows) == 1
    assert json.loads(rows[0]["frame_json"]) == frame
    assert rows[0]["condition_id"] == CID
    assert rows[0]["asset"] == "BTC"
    assert rows[0]["source_conn"] == "B"


def test_source_conn_tags_legs():
    c, _book, _events, _rc, _ec = _make_collector()
    for i, conn in enumerate(("single", "A", "B")):
        f = {"event_type": "tick_size_change", "asset_id": TOK_UP,
             "condition_id": CID, "new_tick_size": "0.001",
             "timestamp": TS + 7000 + i}  # distinct ts: not a redelivery
        c._handle_shard_frame(f, ["BTC"], source_conn=conn)
    assert [r["source_conn"] for r in _l2_rows(c)] == ["single", "A", "B"]


def test_non_book_frames_leave_book_untouched():
    c, book, events, resync_calls, episode_calls = _make_collector()
    c._handle_shard_frame(dict(_frames()["book"]), ["BTC"], source_conn="A")
    before = _book_fingerprint(book)
    assert before["up_bids"] and before["up_asks"]  # seeded BBO to protect
    hostile = [
        # seq-bearing non-book frame: must not advance book sequence state.
        {"event_type": "tick_size_change", "asset_id": TOK_UP,
         "condition_id": CID, "old_tick_size": "0.01", "new_tick_size": "0.001",
         "sequence_number": 41, "timestamp": TS + 8000},
        # out-of-range price-ish key: must not mark stale / mint an episode.
        {"event_type": "market_resolved", "asset_id": TOK_UP,
         "condition_id": CID, "result_price": "99.5",
         "timestamp": TS + 8100},
        # unknown future type with seq: fail-open to l2_raw, fail-closed here.
        {"event_type": "mystery_v2", "asset_id": TOK_UP,
         "condition_id": CID, "sequence_number": 42,
         "timestamp": TS + 8200},
    ]
    for f in hostile:
        c._handle_shard_frame(dict(f), ["BTC"], source_conn="A")
    assert _book_fingerprint(book) == before
    assert events == []  # no anomaly / sequence_gap / unroutable noise
    assert episode_calls == []  # no episode minted
    assert [rc[0] for rc in resync_calls] == [] or all(
        rc[0] == "buffer" for rc in resync_calls)
    assert not any(rc[0] == "gap" for rc in resync_calls)
    rows = _l2_rows(c)
    assert {r["event_type"] for r in rows} >= {
        "book", "tick_size_change", "market_resolved", "mystery_v2"}
    for f in hostile:
        match = [r for r in rows if json.loads(r["frame_json"]) == f]
        assert len(match) == 1  # every hostile frame still logged verbatim


def test_book_frames_still_apply():
    c, book, _events, _rc, _ec = _make_collector()
    c._handle_shard_frame(dict(_frames()["book"]), ["BTC"], source_conn="A")
    assert book.up.bids.best_price() == 0.55
    c._handle_shard_frame(dict(_frames()["price_change"]), ["BTC"], source_conn="A")
    assert book.up.bids.best_price() == 0.551
    assert {r["event_type"] for r in _l2_rows(c)} == {"book", "price_change"}


def test_last_trade_price_feeds_trades_not_books():
    c, book, _events, _rc, _ec = _make_collector()
    c._handle_shard_frame(dict(_frames()["book"]), ["BTC"], source_conn="A")
    before = _book_fingerprint(book)
    c._handle_shard_frame(dict(_frames()["last_trade_price"]), ["BTC"], source_conn="A")
    assert _book_fingerprint(book) == before  # fills never touch the book
    trades = [br.row for br in list(c.writer._buffer) if br.dataset == "trades"]
    assert len(trades) == 1
    assert trades[0]["price"] == 0.55 and trades[0]["token_id"] == TOK_UP
    assert trades[0]["condition_id"] == CID
    assert {r["event_type"] for r in _l2_rows(c)} == {"book", "last_trade_price"}
