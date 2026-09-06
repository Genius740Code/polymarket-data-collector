"""Tests for §3A sanity-bounds validation."""
import pytest
from polymarket_collector.validation import validate_price, validate_size, validate_ws_message, validate_snapshot_fields


def test_price_within_bounds():
    assert validate_price("price", 0.0) is None
    assert validate_price("price", 0.5) is None
    assert validate_price("price", 1.0) is None
    assert validate_price("price", None) is None  # null allowed


def test_price_out_of_bounds():
    assert validate_price("price", 1.4) is not None
    assert validate_price("price", -0.02) is not None
    assert validate_price("price", 1.0001) is not None
    e = validate_price("up_bid", 1.5)
    assert "outside" in e.reason


def test_size_negative():
    assert validate_size("size", 0) is None
    assert validate_size("size", 10) is None
    assert validate_size("size", None) is None
    assert validate_size("size", -1) is not None
    assert validate_size("size", -0.001) is not None


def test_ws_message_valid():
    msg = {"price": 0.55, "size": 100, "bids": [[0.5, 10], [0.4, 5]]}
    errors = validate_ws_message(msg)
    assert errors == []


def test_ws_message_price_out_of_range():
    msg = {"price": 1.4, "size": 10}
    errors = validate_ws_message(msg)
    assert len(errors) == 1
    assert errors[0].field == "price"


def test_ws_message_negative_size():
    msg = {"bids": [[0.5, -10]]}
    errors = validate_ws_message(msg)
    assert any("size" in e.field for e in errors)


def test_ws_message_level_arrays():
    msg = {"bids": [[0.55, 100], [1.2, 50]]}  # second price out of range
    errors = validate_ws_message(msg)
    assert len(errors) == 1
    assert errors[0].value == 1.2


def test_snapshot_fields_validation():
    snap = {
        "up_bid": 0.6, "up_ask": 0.7,
        "down_bid": 0.3, "down_ask": 0.4,
        "up_bid_size": 100, "up_ask_size": 50,
        "down_bid_size": 80, "down_ask_size": 60,
        "up_bid_level_1_price": 0.6, "up_bid_level_1_size": 100,
        "up_ask_level_1_price": 0.7, "up_ask_level_1_size": 50,
    }
    assert validate_snapshot_fields(snap) == []
    snap_bad = dict(snap)
    snap_bad["up_bid_level_1_price"] = 1.5
    errors = validate_snapshot_fields(snap_bad)
    assert len(errors) == 1

def test_snapshot_depth_validation():
    snap = {"up_bid": 0.5, "up_bid_depth_1c": -5}  # depth negative size
    errors = validate_snapshot_fields(snap)
    assert any("depth" in e.field for e in errors)


def test_ws_book_frame_empty_last_trade_price_ok():
    # live 2026-09-05: `book` frames carry last_trade_price:'' when the
    # snapshot was not trade-triggered — must not reject the whole frame
    # (rejection dropped full snapshots and marked books stale ×14).
    msg = {"event_type": "book", "asset_id": "up-1", "market": "0xm",
           "bids": [{"price": "0.50", "size": "10"}],
           "asks": [{"price": "0.52", "size": "10"}],
           "timestamp": "1788649334527", "hash": "0df9ed199b15f73",
           "tick_size": "0.01", "last_trade_price": ""}
    assert validate_ws_message(msg) == []
    # numeric context still validated when present
    msg2 = dict(msg, last_trade_price="0.49")
    assert validate_ws_message(msg2) == []
    msg3 = dict(msg, last_trade_price="1.5")
    assert any(e.field == "last_trade_price" for e in validate_ws_message(msg3))
