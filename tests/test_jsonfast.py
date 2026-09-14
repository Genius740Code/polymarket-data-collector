"""jsonfast shim — byte-identical parsed data vs stdlib json.

Guards the orjson swap: every representative CLOB payload must parse to
exactly what ``json.loads`` produces, and every row shape we WAL/archive
must round-trip identically. NaN/non-finite edge behavior is stricter by
design (see jsonfast docstring) and covered separately.
"""
import json as stdlib

import pytest

from polymarket_collector import jsonfast


CLOB_FRAMES = [
    # full book snapshot
    '{"market":"x","asset_id":"1","timestamp":"1757243400123","hash":"abcdef0123456789",'
    '"bids":[{"price":"0.55","size":"120.5"}],"asks":[{"price":"0.57","size":"80.0"}]}',
    # price_change batch with two assets
    '{"price_changes":[{"asset_id":"1","price":"0.55","size":"10","side":"BUY",'
    '"best_bid":"0.55","best_ask":"0.57","hash":"abcdef0123456789"},'
    '{"asset_id":"2","price":"0.45","size":"5","side":"SELL",'
    '"best_bid":"0.43","best_ask":"0.45","hash":"1234567890abcdef"}],'
    '"timestamp":"1757243400456"}',
    # trade with wallets
    '{"event_type":"trade","asset_id":"1","price":"0.55","size":"25",'
    '"side":"buy","proxyWallet":"0xabc123","transaction_hash":"0xdead",'
    '"timestamp":"1757243400789"}',
    # empty/thin book sides, unicode question text
    '{"bids":[],"asks":[],"question":"Höchsttemperatur München?","timestamp":"1757243400000"}',
    # nested Gamma-style payload
    '{"events":[{"id":"1","ticker":"highest-temperature-in-london-on-2026-09-14",'
    '"markets":[{"conditionId":"0xabc","clobTokenIds":"[\\"11\\",\\"22\\"]",'
    '"outcomes":"[\\"Yes\\",\\"No\\"]","volumeNum":"1234.5","liquidityNum":"600.0"}]}]}',
]

WAL_ROWS = [
    {"dataset": "book_snapshots_500ms", "asset": "HONG-KONG", "date_str": "2026-09-13",
     "row": {"condition_id": "0xabc", "market_id": None, "ts_snapshot_ns": 1757243400500000000,
             "up_bid": 0.55, "up_ask": 0.57, "up_bid_size": 120.5, "book_state": "live",
             "resync_id": None, "book_crossed": False}, "ts": 1757243400.5},
    {"dataset": "trades", "asset": "LONDON", "date_str": "2026-09-13",
     "row": {"token_id": "11", "price": 0.1 + 0.2, "size": 25, "notional": 7.5,
             "fee": 0.0, "fee_is_estimated": None, "side": "buy"}, "ts": 1757243400.0},
]


@pytest.mark.parametrize("frame", CLOB_FRAMES)
def test_loads_matches_stdlib_str_and_bytes(frame):
    assert jsonfast.loads(frame) == stdlib.loads(frame)
    assert jsonfast.loads(frame.encode("utf-8")) == stdlib.loads(frame)


@pytest.mark.parametrize("entry", WAL_ROWS)
def test_dumps_roundtrip_matches_stdlib(entry):
    fast_line = jsonfast.dumps(entry)
    std_line = stdlib.dumps(entry, separators=(",", ":"))
    assert isinstance(fast_line, str)
    # Same parsed content (whitespace/compact form may differ textually).
    assert stdlib.loads(fast_line) == stdlib.loads(std_line)
    # WAL replay path parses with either parser identically.
    assert jsonfast.loads(fast_line) == stdlib.loads(std_line)


def test_dumps_default_str_matches_stdlib():
    obj = {"payload": {"a", "b"}, "n": None}  # set is non-serializable
    assert stdlib.loads(jsonfast.dumps(obj, default=str)) == stdlib.loads(
        stdlib.dumps(obj, default=str)
    )


def test_indent_delegates_to_stdlib_exactly():
    obj = {"b": [1, 2], "a": "x"}
    assert jsonfast.dumps(obj, indent=2) == stdlib.dumps(obj, indent=2)


def test_malformed_raises_like_stdlib():
    for bad in ["{not json", b"\xff\xfe binary", "", "   "]:
        with pytest.raises(Exception):
            stdlib.loads(bad)
        with pytest.raises(Exception):
            jsonfast.loads(bad)
