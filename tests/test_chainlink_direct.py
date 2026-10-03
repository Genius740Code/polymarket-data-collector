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


# -- WS4 wiring: stream families, dataset shape, TWAP agreement, gaps ---------

def test_parse_channel_stream_and_feed_keys():
    for fam_key in ("channel", "stream"):
        ev = cd.parse_direct_message({fam_key: "streams", "feed": "SOLUSD",
                                      "benchmarkPrice": "171.2",
                                      "timestamp": 1727740800000})
        assert ev is not None and ev.asset == "SOL" and ev.price == 171.2
    ev = cd.parse_direct_message({"type": "streams", "feedID": "dogeusd",
                                  "twap": 0.22, "timestamp": 1727740800000})
    assert ev is not None and ev.asset == "DOGE" and ev.price == 0.22
    ev = cd.parse_direct_message({"type": "STREAMS_TWAP30S", "symbol": "BNBUSD",
                                  "twap_price": 600.5, "timestamp": 1727740800000})
    assert ev is not None and ev.source == "chainlink-direct-twap30s"
    assert ev.price == 600.5


def test_event_to_stream_row_matches_schema():
    from polymarket_collector.storage.schemas import CHAINLINK_STREAMS_SCHEMA, SCHEMAS
    assert SCHEMAS["chainlink_streams"] is CHAINLINK_STREAMS_SCHEMA
    assert CHAINLINK_STREAMS_SCHEMA.names == [
        "ts_source", "ts_received_ns", "ts_received_ns_estimated", "asset",
        "event_id", "symbol", "source", "price", "report_id",
    ]
    ev = cd.parse_direct_message({"type": "streams", "symbol": "BTCUSD",
                                  "price": "67234.5", "timestamp": 1727740800000,
                                  "reportId": "0xabc123"})
    row = cd.event_to_stream_row(ev)
    assert row is not None
    assert set(row) == set(CHAINLINK_STREAMS_SCHEMA.names)
    assert row["asset"] == "BTC" and row["report_id"] == "0xabc123"
    assert row["source"] == "chainlink-direct"
    assert cd.event_to_stream_row(None) is None
    assert cd.event_to_stream_row({"not": "an event"}) is None
    assert cd.STREAM_DATASET == "chainlink_streams"


def test_twap_passthrough_rows_shape_and_source():
    from polymarket_collector.storage.schemas import CHAINLINK_TWAP_SCHEMA
    ev30 = cd.parse_direct_message({"type": "streams_twap30s", "symbol": "BTCUSD",
                                    "price": 67200.0, "timestamp": 1727740800000})
    ev60 = cd.parse_direct_message({"type": "streams_twap60s", "symbol": "BTCUSD",
                                    "price": 67210.0, "timestamp": 1727740800000})
    bench = cd.parse_direct_message({"type": "streams", "symbol": "BTCUSD",
                                     "price": 67205.0, "timestamp": 1727740800000})
    rows = cd.twap_passthrough_rows([ev30, ev60, bench], asset="BTC")
    assert len(rows) == 2, "benchmark ticks are not TWAP passthrough"
    assert {r["source"] for r in rows} == {"chainlink-direct-twap30s",
                                           "chainlink-direct-twap60s"}
    assert set(rows[0]) == set(CHAINLINK_TWAP_SCHEMA.names)
    r30 = next(r for r in rows if r["source"] == "chainlink-direct-twap30s")
    assert r30["twap_30s"] == 67200.0 and r30["twap_60s"] is None
    assert r30["n_ticks_30s"] == 1 and r30["n_ticks_60s"] is None
    assert r30["gap_max_ms_60s"] is None
    assert r30["ts_window_end_ms"] == 1727740800000
    # asset filter + unplaceable messages drop out, never raise.
    assert cd.twap_passthrough_rows([ev30], asset="ETH") == []
    assert cd.twap_passthrough_rows(None) == []
    assert cd.twap_passthrough_rows("nope") == []


def test_select_twap_direct_wins_else_derived_labelled():
    derived = [{"ts_window_end_ms": 1, "asset": "BTC", "twap_30s": 5.0,
                "source": "derived_chainlink_rtds"}]
    direct = [{"ts_window_end_ms": 1, "asset": "BTC", "twap_30s": 5.1,
               "source": "chainlink-direct-twap30s"}]
    assert cd.select_twap_rows(direct, derived) == direct
    assert cd.select_twap_rows([], derived) == derived
    assert cd.select_twap_rows(None, derived) == derived
    assert cd.select_twap_rows(None, None) == []


def test_twap_agreement_derived_vs_direct_within_tick():
    from polymarket_collector.chainlink_twap import compute_grid
    base = 1727740800000
    ticks = [(base + i * 1000, 67200.0) for i in range(70)]
    derived = compute_grid(ticks, "BTC")
    assert derived, "dense 1s ticks must yield a derived grid"
    assert all(r["twap_30s"] == 67200.0 for r in derived if r["twap_30s"] is not None)
    direct = [{"ts_window_end_ms": r["ts_window_end_ms"], "asset": "BTC",
               "twap_30s": 67200.0, "twap_60s": r["twap_60s"],
               "source": "chainlink-direct-twap30s"} for r in derived]
    stats = cd.twap_agreement(direct, derived, tick=1.0)
    assert stats["compared_30s"] > 0
    assert stats["agreed_30s"] == stats["compared_30s"]
    assert stats["max_abs_diff"] == 0.0
    # outside-tick drift disagrees but never raises.
    drifted = [dict(r, twap_30s=67299.0) for r in direct]
    stats = cd.twap_agreement(drifted, derived, tick=1.0)
    assert stats["compared_30s"] > 0 and stats["agreed_30s"] == 0
    assert stats["max_abs_diff"] == 99.0
    assert cd.twap_agreement(None, derived, "bad-tick")["compared_30s"] == 0


def test_null_on_gap_never_filled():
    from polymarket_collector.chainlink_twap import compute_grid
    base = 1727740800000
    ticks = [(base, 100.0), (base + 60_000, 101.0)]  # 60s hole > 10s max gap
    derived = compute_grid(ticks, "BTC")
    null_rows = [r for r in derived if r["twap_60s"] is None]
    assert null_rows, "gapped windows must stay NULL"
    assert any(r["gap_max_ms_60s"] is not None and r["gap_max_ms_60s"] > 10_000
               for r in derived), "the 60s hole must surface as gap_max>10s"
    # no direct rows reachable -> derived stands in, NULLs intact (never filled).
    selected = cd.select_twap_rows([], derived)
    assert any(r["twap_60s"] is None for r in selected)
    assert all(r["source"] == "derived_chainlink_rtds" for r in selected)
    # agreement skips NULL pairs (gaps are not disagreements).
    stats = cd.twap_agreement(selected, derived, tick=1.0)
    assert stats["compared_60s"] == 0 and stats["max_abs_diff"] is None


def test_onchain_unanimity_join_multi_maker_null():
    from polymarket_collector.onchain import collapse_onchain_unanimity
    m1, m2, t = "0x" + "01" * 20, "0x" + "02" * 20, "0x" + "09" * 20
    rows = [
        {"tx_hash": "0xabc", "token_id": "111", "condition_id": "0xc",
         "maker": m1, "taker": t, "price": None, "size": None, "fee": None,
         "side": "buy", "exchange_version": "v2", "builder": None},
        {"tx_hash": "0xabc", "token_id": "111", "condition_id": "0xc",
         "maker": m2, "taker": t, "price": None, "size": None, "fee": None,
         "side": "buy", "exchange_version": "v2", "builder": None},
        {"tx_hash": "0xabc", "token_id": "222", "condition_id": "0xc",
         "maker": m2, "taker": t, "price": None, "size": None, "fee": None,
         "side": "sell", "exchange_version": "v2", "builder": None},
    ]
    out = collapse_onchain_unanimity(rows)
    assert len(out) == 2, "one row per (tx_hash, token_id)"
    same = next(r for r in out if r["token_id"] == "111")
    assert same["maker"] is None, "multi-maker fill must stay NULL"
    assert same["taker"] == t and same["side"] == "buy"
    other = next(r for r in out if r["token_id"] == "222")
    assert other["maker"] == m2 and other["side"] == "sell"
    # unanimous duplicates collapse to one identical row; junk never raises.
    dup = collapse_onchain_unanimity([rows[2], dict(rows[2])])
    assert dup == [rows[2]]
    assert collapse_onchain_unanimity(None) == []
    assert collapse_onchain_unanimity([{"no": "tx"}]) == []
