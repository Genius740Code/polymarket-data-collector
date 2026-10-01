"""Tests for backtest_exec: pure tick-replay execution simulator.

All ticks/orders are hand-built (no network, no synthesis from the
collector). NULL = gap: missing sides never fill and never interpolate.
"""

import pandas as pd
import pytest

from polymarket_collector.backtest_exec import normalize_settlement, run_backtest

T0 = 1_700_000_000_000


def _tick(ts, bid=None, bid_sz=None, ask=None, ask_sz=None):
    return {"timestamp": ts, "bid_price": bid, "bid_size": bid_sz,
            "ask_price": ask, "ask_size": ask_sz}


def _order(oid, ts, side, price, size, **kw):
    return {"order_id": oid, "timestamp": ts, "side": side,
            "price": price, "size": size, **kw}


def test_taker_fill_at_resting_price():
    ticks = [_tick(T0, bid=0.55, bid_sz=100.0, ask=0.60, ask_sz=100.0)]
    orders = [_order("o1", T0, "buy", 0.65, 10.0)]
    res = run_backtest(ticks, orders, latency_ms=0)
    assert len(res.fills) == 1
    f = res.fills.iloc[0]
    assert f["fill_price"] == pytest.approx(0.60)
    assert f["fill_qty"] == pytest.approx(10.0)
    assert bool(f["taker"]) is True
    assert f["fee"] == pytest.approx(0.0)
    assert res.metrics["fill_rate"] == pytest.approx(1.0)
    t = res.trades.iloc[0]
    assert t["status"] == "filled" and t["avg_fill_price"] == pytest.approx(0.60)


def test_latency_delays_activation_no_lookahead():
    ticks = [
        _tick(T0 + 50, bid=0.55, bid_sz=50.0, ask=0.56, ask_sz=50.0),
        _tick(T0 + 150, bid=0.60, bid_sz=50.0, ask=0.62, ask_sz=50.0),
    ]
    # Signal at T0, latency 100ms -> effective T0+100: first tick invisible.
    orders = [_order("o1", T0, "buy", 0.70, 5.0)]
    res = run_backtest(ticks, orders, latency_ms=100)
    assert len(res.fills) == 1
    assert res.fills.iloc[0]["fill_ts"] == T0 + 150
    assert res.fills.iloc[0]["fill_price"] == pytest.approx(0.62)


def test_queue_size_ahead_partial_then_trade_through():
    ticks = [
        _tick(T0, bid=0.55, bid_sz=10.0, ask=0.65, ask_sz=10.0),      # resting, no touch
        _tick(T0 + 10, bid=0.58, bid_sz=10.0, ask=0.60, ask_sz=80.0),  # touch: 80 ahead
        _tick(T0 + 20, bid=0.58, bid_sz=10.0, ask=0.60, ask_sz=30.0),  # depletion: fill 50
        _tick(T0 + 30, bid=0.58, bid_sz=10.0, ask=0.59, ask_sz=5.0),   # through: rest at limit
    ]
    orders = [_order("o1", T0, "buy", 0.60, 100.0)]
    res = run_backtest(ticks, orders, latency_ms=0)
    fills = res.fills
    assert res.metrics["total_filled"] == pytest.approx(100.0)
    assert res.trades.iloc[0]["status"] == "filled"
    # Maker fills at the limit (queue depletion), remainder at limit (through).
    assert set(fills["taker"]) == {False}
    assert ((fills["fill_price"] - 0.60).abs().max()) < 1e-9
    assert sorted(fills["fill_qty"]) == pytest.approx([50.0, 50.0])


def test_gap_side_never_fills_never_interpolates():
    ticks = [
        _tick(T0, bid=0.55, bid_sz=10.0, ask=None, ask_sz=None),
        _tick(T0 + 10, bid=None, bid_sz=None, ask=None, ask_sz=None),
    ]
    orders = [_order("o1", T0, "buy", 0.90, 5.0)]
    res = run_backtest(ticks, orders, latency_ms=0)
    assert len(res.fills) == 0
    assert res.metrics["fill_rate"] == pytest.approx(0.0)
    assert res.trades.iloc[0]["status"] == "unfilled"
    # Mid/equity stay NULL on gaps — no interpolation.
    assert res.equity["mid"].isna().all()
    assert res.equity["equity"].isna().all()
    assert res.metrics["slippage_bps"] is None


def test_fee_math_and_official_only_settlement():
    ticks = [_tick(T0, bid=0.55, bid_sz=100.0, ask=0.60, ask_sz=100.0)]
    orders = [_order("o1", T0, "buy", 0.65, 10.0,
                     condition_id="0xaaa", outcome="up")]
    official = {"0xaaa": {"resolution_outcome": "up",
                          "settlement_source": "polymarket_official"}}
    res = run_backtest(ticks, orders, latency_ms=0, fee_bps=100.0, settlement=official)
    f = res.fills.iloc[0]
    assert f["fee"] == pytest.approx(0.60 * 10.0 * 0.01)
    t = res.trades.iloc[0]
    assert bool(t["settled"]) is True
    assert t["pnl"] == pytest.approx((1.0 - 0.60) * 10.0 - f["fee"])
    assert res.metrics["pnl"] == pytest.approx(t["pnl"])


def test_inferred_settlement_never_settles():
    ticks = [_tick(T0, bid=0.55, bid_sz=100.0, ask=0.60, ask_sz=100.0)]
    orders = [_order("o1", T0, "buy", 0.65, 10.0,
                     condition_id="0xbbb", outcome="up")]
    inferred = {"0xbbb": {"resolution_outcome": "up",
                          "settlement_source": "inferred_nearest"}}
    res = run_backtest(ticks, orders, latency_ms=0, settlement=inferred)
    assert bool(res.trades.iloc[0]["settled"]) is False
    assert res.trades.iloc[0]["pnl"] is None
    assert res.metrics["pnl"] is None
    assert res.metrics["n_unsettled"] == 1


def test_sell_short_settles_on_down_win():
    ticks = [_tick(T0, bid=0.60, bid_sz=100.0, ask=0.65, ask_sz=100.0)]
    orders = [_order("s1", T0, "sell", 0.55, 10.0,
                     condition_id="0xccc", outcome="up")]
    official = {"0xccc": {"resolution_outcome": "down",
                          "settlement_source": "polymarket_official"}}
    res = run_backtest(ticks, orders, latency_ms=0, settlement=official)
    assert res.fills.iloc[0]["fill_price"] == pytest.approx(0.60)
    assert res.trades.iloc[0]["pnl"] == pytest.approx((0.60 - 0.0) * 10.0)


def test_slippage_vs_effective_mid_and_equity_marks():
    ticks = [
        _tick(T0, bid=0.58, bid_sz=100.0, ask=0.62, ask_sz=100.0),
        _tick(T0 + 10, bid=0.59, bid_sz=100.0, ask=0.63, ask_sz=100.0),
    ]
    orders = [_order("o1", T0, "buy", 0.70, 4.0)]
    res = run_backtest(ticks, orders, latency_ms=0)
    mid0 = (0.58 + 0.62) / 2.0
    assert res.fills.iloc[0]["slippage_bps"] == pytest.approx((0.62 - mid0) / mid0 * 1e4)
    assert list(res.equity.columns) == ["timestamp", "position", "cash", "mid", "equity"]
    assert res.equity.iloc[0]["position"] == pytest.approx(4.0)
    assert res.equity.iloc[0]["mid"] == pytest.approx(mid0)
    assert res.equity.iloc[0]["equity"] == pytest.approx(res.equity.iloc[0]["cash"] + 4.0 * mid0)


def test_dataframe_inputs_and_expired_order():
    ticks = pd.DataFrame([{"timestamp": T0, "bid_price": 0.5, "bid_size": 5.0,
                           "ask_price": 0.9, "ask_size": 5.0}])
    orders = pd.DataFrame([
        {"order_id": "late", "timestamp": T0 + 10_000, "side": "buy",
         "price": 0.95, "size": 1.0},
    ])
    res = run_backtest(ticks, orders)
    assert res.trades.iloc[0]["status"] == "expired_no_tick"
    assert res.metrics["fill_rate"] == pytest.approx(0.0)


def test_invalid_order_rejected():
    with pytest.raises(ValueError):
        run_backtest([_tick(T0, 0.5, 1.0, 0.6, 1.0)],
                     [_order("bad", T0, "buy", 1.50, 1.0)])
    with pytest.raises(ValueError):
        run_backtest([_tick(T0, 0.5, 1.0, 0.6, 1.0)],
                     [_order("bad", T0, "buy", 0.5, 0.0)])
    with pytest.raises(ValueError):
        run_backtest([_tick(T0, 0.5, 1.0, 0.6, 1.0)],
                     [{"order_id": "bad", "timestamp": T0, "side": "hold",
                       "price": 0.5, "size": 1.0}])


def test_normalize_settlement_filters_unofficial_df():
    df = pd.DataFrame([
        {"condition_id": "0xa", "resolution_outcome": "up",
         "settlement_source": "polymarket_official", "settlement_price": None},
        {"condition_id": "0xb", "resolution_outcome": "up",
         "settlement_source": "inferred_nearest", "settlement_price": None},
    ])
    out = normalize_settlement(df)
    assert set(out) == {"0xa"}
