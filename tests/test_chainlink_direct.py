"""Direct Chainlink streams stub: parse, NULL policy, symbol map. No network."""
from polymarket_collector.chainlink import ChainlinkEvent
from polymarket_collector.ingest import chainlink_direct as cd


def test_direct_symbols():
    assert tuple(cd.DIRECT_SYMBOLS) == (
        "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "DOGEUSD", "BNBUSD", "HYPEUSD",
    )
    assert len(cd.DIRECT_SYMBOLS) == 7


def test_symbol_map_roundtrip():
    for asset, symbol in (("BTC", "BTCUSD"), ("ETH", "ETHUSD"), ("SOL", "SOLUSD"),
                          ("XRP", "XRPUSD"), ("DOGE", "DOGEUSD"), ("BNB", "BNBUSD"),
                          ("HYPE", "HYPEUSD")):
        assert cd.symbol_for_asset(asset) == symbol
        assert cd.asset_for_symbol(symbol) == asset
        assert cd.symbol_for_asset(asset.lower()) == symbol
    assert cd.symbol_for_asset("FAKE") is None
    assert cd.asset_for_symbol("FAKEUSD") is None
    assert cd.symbol_for_asset(None) is None
    assert cd.asset_for_symbol("") is None


def _streams_msg(**kw):
    m = {"type": "streams", "symbol": "BTCUSD", "price": "67234.5",
         "timestamp": 1727740800000, "reportId": "0xabc123"}
    m.update(kw)
    return m


def test_parse_streams_row():
    ev = cd.parse_direct_message(_streams_msg())
    assert isinstance(ev, ChainlinkEvent)
    assert ev.asset == "BTC"
    assert ev.symbol == "BTCUSD"
    assert ev.price == 67234.5
    assert ev.report_id == "0xabc123"
    assert ev.ts_source == 1727740800000
    assert ev.source == "chainlink-direct"
    d = ev.to_dict()
    assert d["report_id"] == "0xabc123"
    assert d["price"] == 67234.5


def test_parse_twap_passthrough():
    for stream, source in (("streams_twap30s", "chainlink-direct-twap30s"),
                           ("streams_twap60s", "chainlink-direct-twap60s")):
        ev = cd.parse_direct_message({"type": stream, "symbol": "ETHUSD",
                                      "price": 3500.25, "timestamp": 1727740800000,
                                      "roundId": 987})
        assert ev is not None
        assert ev.asset == "ETH"
        assert ev.price == 3500.25
        assert ev.report_id == "987"
        assert ev.source == source


def test_null_policy_report_round_id():
    assert cd.parse_direct_message(_streams_msg(reportId=None)).report_id is None
    m = _streams_msg()
    del m["reportId"]
    assert cd.parse_direct_message(m).report_id is None
    assert cd.parse_direct_message(_streams_msg(reportId="", roundId=42)).report_id == "42"


def test_null_policy_price_and_ts():
    assert cd.parse_direct_message(_streams_msg(price="abc")).price is None
    assert cd.parse_direct_message(_streams_msg(price=None)).price is None
    assert cd.parse_direct_message(_streams_msg(timestamp="not-a-time")).ts_source is None
    m = _streams_msg()
    del m["timestamp"]
    assert cd.parse_direct_message(m).ts_source is None


def test_parse_rejects_without_fabrication():
    assert cd.parse_direct_message({"type": "streams", "symbol": "FAKEUSD", "price": 1.0}) is None
    assert cd.parse_direct_message({"type": "nope", "symbol": "BTCUSD", "price": 1.0}) is None
    assert cd.parse_direct_message({}) is None
    assert cd.parse_direct_message(None) is None
    assert cd.parse_direct_message("streams") is None
    assert cd.parse_direct_message([]) is None


def test_subscribe_payload_covers_all_symbols():
    p = cd.build_subscribe_message()
    assert p["type"] == "subscribe"
    assert sorted(p["symbols"]) == sorted(cd.DIRECT_SYMBOLS)
    custom = cd.build_subscribe_message(["BTCUSD", "btcusd"])
    assert custom["symbols"] == ["BTCUSD"]
    client = cd.ChainlinkDirectClient()
    assert sorted(client.subscribe_payload()["symbols"]) == sorted(cd.DIRECT_SYMBOLS)


def test_client_handle_message_counts_no_network():
    seen = []
    client = cd.ChainlinkDirectClient(connect=None, on_event=lambda t, d: seen.append((t, d)))
    assert client.connect is None
    ev = client.handle_message(_streams_msg())
    assert ev is not None and ev.asset == "BTC"
    assert client.handle_message({"type": "nope"}) is None
    assert (client.received, client.parsed, client.dropped) == (2, 1, 1)
    note = client.note_unreachable("dns outage")
    assert note["fallback"] == "rtds_primary"
    assert seen and seen[0][0] == "chainlink_direct_unreachable"


def test_unreachable_note_rtds_stays_primary():
    note = cd.unreachable_note("connection refused", detail="tcp timeout")
    assert note["event_type"] == "chainlink_direct_unreachable"
    assert note["reason"] == "connection refused"
    assert note["fallback"] == "rtds_primary"
    assert cd.unreachable_note("")["reason"] == "unreachable"
