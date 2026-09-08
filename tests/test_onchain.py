"""Tests for C2 on-chain maker/taker backfill (OrderFilled logs).

All network-free: log fixtures mirror the real topic layout
(topic0 = event sig, topic1 = orderHash, topic2 = maker, topic3 = taker).
The V2 topic0 constant was verified live 2026-09-08 against 174 real logs.
"""
import pytest

from polymarket_collector.onchain import (
    ORDERFILLED_V1_TOPIC,
    ORDERFILLED_V2_TOPIC,
    backfill_wallets_from_chain,
    parse_order_filled_logs,
)


def _log(txh, maker, taker, topic0=ORDERFILLED_V2_TOPIC):
    def addr(a):
        return "0x" + "00" * 12 + a.lower().removeprefix("0x")
    return {
        "transactionHash": txh,
        "topics": [topic0, "0x" + "11" * 32, addr(maker), addr(taker)],
        "data": "0x" + "00" * 192,
    }


def test_parse_v1_and_v2_topics():
    logs = [
        _log("0xaaa", "0x" + "01" * 20, "0x" + "02" * 20, ORDERFILLED_V1_TOPIC),
        _log("0xbbb", "0x" + "03" * 20, "0x" + "04" * 20, ORDERFILLED_V2_TOPIC),
    ]
    out = parse_order_filled_logs(logs)
    assert out["0xaaa"] == ("0x" + "01" * 20, "0x" + "02" * 20)
    assert out["0xbbb"] == ("0x" + "03" * 20, "0x" + "04" * 20)


def test_parse_ignores_foreign_and_short_logs():
    logs = [
        _log("0xccc", "0x" + "01" * 20, "0x" + "02" * 20,
             "0x" + "ff" * 32),  # wrong topic0
        {"transactionHash": "0xddd", "topics": ["0x1234"], "data": "0x"},  # short
        {"transactionHash": "0xeee", "topics": [], "data": "0x"},
    ]
    assert parse_order_filled_logs(logs) == {}


def test_parse_multi_fill_tx_stays_null():
    """Two DISTINCT makers in one tx -> maker None (never guessed); taker fills."""
    logs = [
        _log("0xtx", "0x" + "01" * 20, "0x" + "09" * 20),
        _log("0xtx", "0x" + "02" * 20, "0x" + "09" * 20),
    ]
    out = parse_order_filled_logs(logs)
    assert out["0xtx"] == (None, "0x" + "09" * 20)


def test_parse_consistent_multi_log_agrees():
    logs = [
        _log("0xtx", "0x" + "01" * 20, "0x" + "09" * 20),
        _log("0xtx", "0x" + "01" * 20, "0x" + "09" * 20),
    ]
    out = parse_order_filled_logs(logs)
    assert out["0xtx"] == ("0x" + "01" * 20, "0x" + "09" * 20)


def test_writeback_fills_nulls_only():
    rows = [
        {"trade_id": "t1", "transaction_hash": "0xAAA",
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
        {"trade_id": "t2", "transaction_hash": "0xaaa",  # case-insensitive join
         "maker_wallet": "0xkeep", "taker_wallet": None, "wallet": None},
        {"trade_id": "t3", "transaction_hash": None,  # no tx -> skipped
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
        {"trade_id": "t4", "transaction_hash": "0xmissing",  # no chain log
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
    ]
    tx_map = {"0xaaa": ("0x" + "01" * 20, "0x" + "02" * 20)}
    stats = backfill_wallets_from_chain(rows, tx_map)
    assert stats == {"filled_maker": 1, "filled_taker": 2, "filled_wallet": 2}
    assert rows[0]["maker_wallet"] == "0x" + "01" * 20
    assert rows[0]["taker_wallet"] == "0x" + "02" * 20
    assert rows[0]["wallet"] == "0x" + "02" * 20  # taker preferred
    assert rows[1]["maker_wallet"] == "0xkeep", "non-NULL never overwritten"
    assert rows[2]["wallet"] is None and rows[3]["wallet"] is None


def _v2_log(txh, maker, taker, token_id, side=0):
    def addr(a):
        return "0x" + "00" * 12 + a.lower().removeprefix("0x")
    def word(v):
        return format(v, "064x")
    from polymarket_collector.onchain import ORDERFILLED_V2_TOPIC
    return {
        "transactionHash": txh,
        "topics": [ORDERFILLED_V2_TOPIC, "0x" + "11" * 32, addr(maker), addr(taker)],
        "data": "0x" + word(side) + word(token_id) + word(100) + word(50) + word(1) + word(0) + word(0),
    }


def test_fills_decode_v2_token_id():
    from polymarket_collector.onchain import parse_order_filled_fills
    logs = [_v2_log("0xaaa", "0x" + "01" * 20, "0x" + "02" * 20, 12345, side=1)]
    fills = parse_order_filled_fills(logs)
    assert len(fills) == 1
    f = fills[0]
    assert f["tx_hash"] == "0xaaa" and f["token_id"] == "12345"
    assert f["maker"] == "0x" + "01" * 20 and f["taker"] == "0x" + "02" * 20
    assert f["side"] == 1


def test_fills_join_per_fill_survives_bundle():
    """Same tx, two fills, different makers/tokens: each row gets ITS maker."""
    from polymarket_collector.onchain import backfill_wallets_from_fills
    fills = [
        {"tx_hash": "0xtx", "token_id": "111", "maker_asset_id": None,
         "taker_asset_id": None, "maker": "0x" + "01" * 20,
         "taker": "0x" + "09" * 20, "side": 0},
        {"tx_hash": "0xtx", "token_id": "222", "maker_asset_id": None,
         "taker_asset_id": None, "maker": "0x" + "02" * 20,
         "taker": "0x" + "09" * 20, "side": 0},
    ]
    rows = [
        {"trade_id": "a", "transaction_hash": "0xTX", "token_id": "111",
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
        {"trade_id": "b", "transaction_hash": "0xtx", "token_id": "222",
         "maker_wallet": None, "taker_wallet": None, "wallet": None},
    ]
    stats = backfill_wallets_from_fills(rows, fills)
    assert stats == {"filled_maker": 2, "filled_taker": 2, "filled_wallet": 2}
    assert rows[0]["maker_wallet"] == "0x" + "01" * 20
    assert rows[1]["maker_wallet"] == "0x" + "02" * 20


def test_fills_join_same_token_multi_maker_stays_null():
    from polymarket_collector.onchain import backfill_wallets_from_fills
    fills = [
        {"tx_hash": "0xtx", "token_id": "111", "maker_asset_id": None,
         "taker_asset_id": None, "maker": "0x" + "01" * 20,
         "taker": "0x" + "09" * 20, "side": 0},
        {"tx_hash": "0xtx", "token_id": "111", "maker_asset_id": None,
         "taker_asset_id": None, "maker": "0x" + "02" * 20,
         "taker": "0x" + "09" * 20, "side": 0},
    ]
    rows = [{"trade_id": "a", "transaction_hash": "0xtx", "token_id": "111",
             "maker_wallet": None, "taker_wallet": "0xkeep", "wallet": "0xkeep"}]
    stats = backfill_wallets_from_fills(rows, fills)
    assert rows[0]["maker_wallet"] is None, "same-token multi-maker must stay NULL"
    assert stats["filled_taker"] == 0 and rows[0]["taker_wallet"] == "0xkeep", "non-NULL never overwritten"


def test_tx_map_from_fills_unanimity():
    from polymarket_collector.onchain import tx_map_from_fills
    fills = [
        {"tx_hash": "0xs", "token_id": "1", "maker_asset_id": None,
         "taker_asset_id": None, "maker": "0x" + "01" * 20,
         "taker": "0x" + "09" * 20, "side": 0},
        {"tx_hash": "0xm", "token_id": "1", "maker_asset_id": None,
         "taker_asset_id": None, "maker": "0x" + "01" * 20,
         "taker": "0x" + "09" * 20, "side": 0},
        {"tx_hash": "0xm", "token_id": "2", "maker_asset_id": None,
         "taker_asset_id": None, "maker": "0x" + "02" * 20,
         "taker": "0x" + "09" * 20, "side": 0},
    ]
    m = tx_map_from_fills(fills)
    assert m["0xs"] == ("0x" + "01" * 20, "0x" + "09" * 20)
    assert m["0xm"] == (None, "0x" + "09" * 20)
