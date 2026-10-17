"""Regression tests for the 2026-09-18 data-integrity audit fixes.

Covers: C1 token healing, C3 honest NULL condition_id/outcome, M9 deterministic
trade_id, H1 unsent retry, M7 REST bounds, H5 write-batch stats, L1 cursor key
normalization, M6 honest chainlink ts_source, H2 fan-out routing.
"""
import time
from types import SimpleNamespace

import pyarrow as pa

from polymarket_collector.book import OrderBookState


def _book(**kw):
    base = dict(asset="BTC", condition_id="0xabc", market_id=None,
                series_id="BTC-5m", window_index=1, up_token_id="U1",
                down_token_id="D1", market_end_ts_ms=int(time.time() * 1000) + 300_000)
    base.update(kw)
    return OrderBookState(**base)


# ---------------------------------------------------------------- C1
def test_heal_tokens_replaces_placeholders():
    b = _book(up_token_id="0xabc-UP", down_token_id="0xabc-DOWN")
    assert b.heal_tokens("111", "222") is True
    assert (b.up_token_id, b.down_token_id) == ("111", "222")


def test_heal_tokens_noop_and_never_blanks():
    b = _book()
    assert b.heal_tokens("U1", "D1") is False  # identical
    assert b.heal_tokens("", "") is False and b.heal_tokens(None, None) is False
    assert (b.up_token_id, b.down_token_id) == ("U1", "D1")  # untouched


# ---------------------------------------------------------------- C3 + M9 (collector trade path)
def _collector(tmp_path):
    from polymarket_collector.collector import Collector
    from polymarket_collector.config import CollectorConfig

    cfg = CollectorConfig()
    cfg.storage.data_dir = str(tmp_path / "data")
    cfg.storage.wal_dir = str(tmp_path / "_wal")
    cfg.raw_archive.path = str(tmp_path / "raw")
    cfg.raw_archive.enabled = False
    cfg.cursor_store.path = str(tmp_path / "cursor")
    cfg.assets = ["BTC"]
    c = Collector(cfg)
    captured = []
    c.writer.append = lambda dataset, row, asset=None, **kw: captured.append((dataset, row)) or True
    c.rollover = SimpleNamespace(active_markets=lambda a: [])
    return c, captured


def _trade_msg(**kw):
    m = {"type": "last_trade_price", "asset_id": "tok-xyz",
         "price": 0.55, "size": 10.0, "side": "BUY",
         "timestamp": str(int(time.time() * 1000))}
    m.update(kw)
    return m


def test_unresolvable_trade_keeps_null_condition_and_unknown_outcome(tmp_path):
    c, captured = _collector(tmp_path)
    assert c._handle_trade_message(_trade_msg(), "BTC", time.time_ns()) is True
    row = captured[0][1]
    assert row["condition_id"] is None  # C3: never token_id
    # N10/M6: honest NULL (was "unknown" sentinel that defeated IS NOT NULL
    # filters and polluted group-bys with a fake third outcome).
    assert row["outcome"] is None  # C3: no tautological "up"
    assert row["ts_source"] is not None  # wire ts present here


def test_trade_id_deterministic_across_retries(tmp_path):
    c, captured = _collector(tmp_path)
    # A retry redelivers the SAME frame — reuse one msg object so the test
    # asserts determinism given identical input (not same-millisecond luck).
    m = _trade_msg()
    assert c._handle_trade_message(m, "BTC", time.time_ns()) is True
    assert c._handle_trade_message(m, "BTC", time.time_ns()) is True
    assert captured[0][1]["trade_id"] == captured[1][1]["trade_id"]
    assert captured[0][1]["trade_id"].startswith("ws-")


def test_missing_wire_ts_stays_null(tmp_path):
    c, captured = _collector(tmp_path)
    m = _trade_msg()
    del m["timestamp"]  # M6: no fallback to receive time
    assert c._handle_trade_message(m, "BTC", time.time_ns()) is True
    assert captured[0][1]["ts_source"] is None


# ------- 2026-10-17: asset=UNKNOWN trades partition (roster asset repair)
def test_trade_roster_asset_repairs_unknown_partition(tmp_path):
    """A trade whose caller asset is UNKNOWN but whose condition_id resolves
    EXACTLY in the live roster must carry the roster asset — series_id
    confirmed this resolution existed (series_id='BTC-5m' rows inside
    asset=UNKNOWN partitions, ~200/day, unjoinable). Not invented from
    token shape: the roster is discovery state this collector resolved."""
    from types import SimpleNamespace
    c, captured = _collector(tmp_path)
    cid = "0xrosterhit"
    c.markets[cid] = SimpleNamespace(
        asset="BTC", series_id="BTC-5m", window_index=5971971,
        market_id="5771729", up_token_id="tok-xyz", down_token_id="tok-down",
        condition_id=cid)
    m = _trade_msg(condition_id=cid)
    assert c._handle_trade_message(m, "UNKNOWN", time.time_ns()) is True
    row = captured[0][1]
    assert row["asset"] == "BTC"
    assert row["series_id"] == "BTC-5m"
    assert row["condition_id"] == cid


def test_trade_book_asset_repairs_unknown_partition(tmp_path):
    """market=None but the O(1) book index resolves the token — same repair."""
    from types import SimpleNamespace
    c, captured = _collector(tmp_path)
    cid = "0xbookhit"
    # _lookup_book: O(1) by condition_id in books? Provide the monkeypatched
    # lookup the trade path actually uses (books dict keyed by condition_id).
    c._lookup_book = lambda _cid, _tok: SimpleNamespace(
        condition_id=cid, asset="ETH")
    m = _trade_msg(token_id="tok-eth")
    assert c._handle_trade_message(m, "UNKNOWN", time.time_ns()) is True
    row = captured[0][1]
    assert row["asset"] == "ETH"
    assert row["condition_id"] == cid  # NOT the token: C3 held


def test_trade_unresolvable_stays_unknown(tmp_path):
    """NO market resolution anywhere → still UNKNOWN (honest gap, event-
    sourced): the repair never invents an asset from token shape."""
    c, captured = _collector(tmp_path)
    c._lookup_book = lambda _cid, _tok: None
    m = _trade_msg()
    assert c._handle_trade_message(m, "UNKNOWN", time.time_ns()) is True
    row = captured[0][1]
    assert row["asset"] == "UNKNOWN"
    assert row["condition_id"] is None  # C3: never the token_id


def test_trade_resolved_caller_asset_not_overridden(tmp_path):
    """A caller-resolved asset is authoritative — repair never overrides."""
    from types import SimpleNamespace
    c, captured = _collector(tmp_path)
    cid = "0xresolved"
    c.markets[cid] = SimpleNamespace(
        asset="BTC", series_id="BTC-5m", window_index=5971971,
        market_id="5771729", up_token_id="tok-xyz", down_token_id="tok-down",
        condition_id=cid)
    m = _trade_msg(condition_id=cid)
    assert c._handle_trade_message(m, "DOGE", time.time_ns()) is True
    # caller labeled it DOGE (e.g. shard truth) — repair must NOT touch it
    assert captured[0][1]["asset"] == "DOGE"


# ---------------------------------------------------------------- H1
def test_markets_log_unsent_retry(tmp_path):
    from polymarket_collector.storage.markets_log import MarketsLog

    state = {"fail": True}
    rows = []

    class Flaky:
        def append(self, dataset, row, asset=None, date_str=None):
            if state["fail"]:
                return False
            rows.append((dataset, row))
            return True

    ml = MarketsLog(str(tmp_path), writer=Flaky())
    ml.append_event("connected", "2026-09-18T00:00:00Z", 123, asset="BTC", details={})
    assert len(ml._unsent) == 1
    assert ml.flush_staging() == 1  # staging cleared...
    assert len(ml._unsent) == 1  # ...but the refused row is retained
    state["fail"] = False
    ml.flush_staging()  # retry succeeds
    assert len(ml._unsent) == 0
    assert len(rows) == 1


# ---------------------------------------------------------------- M7
def test_rest_snapshot_skips_out_of_range_levels():
    b = _book()
    b.replace_from_rest_snapshot({
        "up_bids": [["1.5", "10"], ["0.5", "10"], ["-0.1", "3"]],
        "up_asks": [["0.6", "-2"], ["0.6", "5"]],
    })
    bids = [lv.price for lv in b.up.bids.levels if lv.price is not None]
    asks = [(lv.price, lv.size) for lv in b.up.asks.levels if lv.price is not None]
    assert bids == [0.5]
    assert asks == [(0.6, 5.0)]


# ---------------------------------------------------------------- H5
def test_write_batches_counts_dropped_batch(tmp_path):
    from polymarket_collector.storage.streaming import write_batches

    schema = pa.schema([pa.field("a", pa.int64(), nullable=True),
                        pa.field("b", pa.string(), nullable=True)])
    good = pa.table({"a": [1, 2], "b": ["x", "y"]}, schema=schema)
    bad = pa.table({"a": ["not-an-int"], "b": ["z"]})  # uncastable
    stats: dict = {}
    n = write_batches(iter([good, bad]), tmp_path / "out.parquet", schema=schema, stats=stats)
    assert n == 2
    assert stats.get("write_dropped_batches") == 1
    assert stats.get("write_dropped_rows") == 1


# ---------------------------------------------------------------- L1
def test_cursor_to_row_normalizes_keys():
    from polymarket_collector.storage.cursor_store import CursorState

    cs = CursorState(asset="btc", current_window_index=3, window_label="5M")
    row = cs.to_row()
    assert row[0] == "BTC" and row[1] == "5m"


# ---------------------------------------------------------------- M6
def test_chainlink_event_missing_ts_stays_null():
    from polymarket_collector.chainlink import chainlink_event_from_ws

    ev = chainlink_event_from_ws({"price": 90000.0, "symbol": "btc/usd"}, "BTC")
    assert ev.ts_source is None
    assert ev.report_id is None


# ---------------------------------------------------------------- H2
def test_fanout_books_resolves_each_market(tmp_path):
    c, _ = _collector(tmp_path)
    b1 = _book(condition_id="c1", up_token_id="U1", down_token_id="D1")
    b2 = _book(condition_id="c2", up_token_id="U2", down_token_id="D2")
    c.books = {"c1": b1, "c2": b2}
    c._index_book(b1)
    c._index_book(b2)
    frame = {"price_changes": [
        {"asset_id": "U1", "price": 0.5, "size": 10, "side": "BUY"},
        {"asset_id": "D2", "price": 0.5, "size": 10, "side": "SELL"},
    ]}
    got = c._fanout_books(frame)
    assert {b.condition_id for b in got} == {"c1", "c2"}
    assert c._fanout_books({"price_changes": [{"asset_id": "nope", "price": 1, "size": 1}]}) == []
    assert c._fanout_books({"type": "book"}) == []
