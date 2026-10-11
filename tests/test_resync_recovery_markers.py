"""P2 recovery markers (2026-10-11) — one stdout line per recovery phase.

The P2 timing run proved recovery instrumentation silent: zero
provisional/prewarm/notify_settlement markers across every window, so phases
had to be inferred from live% samples. These tests pin the marker lines the
next timing run greps — book.py (book_live), resync.py (fetch_none_strike,
provisional_promote), rollover.py (gap_promote, window_discovered,
notify_settlement) — all through the shared book.recovery_marker format:

    [recovery] phase=<name> key=value ...

Real objects + constant literals only; no network, no generated data.
"""
import time

from polymarket_collector.book import OrderBookState, recovery_marker
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager


def _book(cid="cid-marker", asset="BTC"):
    return OrderBookState(
        asset=asset,
        condition_id=cid,
        market_id="mid-1",
        series_id="BTC-5MIN",
        window_index=1,
        up_token_id="up-123",
        down_token_id="down-456",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
    )


def _mgr(events):
    cfg = CollectorConfig()
    return ResyncManager(
        cfg,
        rest_fetcher=None,  # unused on the marker paths
        on_event=lambda t, d: events.append((t, d)),
    )


# --- shared format -----------------------------------------------------------

def test_marker_format_stable(capsys):
    recovery_marker("gap_promote", asset="BTC", tf="5m", window_ts=1791731700)
    out = capsys.readouterr().out
    assert out.startswith("[recovery] phase=gap_promote asset=BTC tf=5m window_ts=1791731700\n")


def test_marker_none_fields_omitted_never_raises(capsys):
    recovery_marker("fetch_none_strike", cid="c1", n=1, quiet_s=None)
    out = capsys.readouterr().out
    assert out == "[recovery] phase=fetch_none_strike cid=c1 n=1\n"
    recovery_marker("boom")  # empty payload stays well-formed
    assert "[recovery] phase=boom" in capsys.readouterr().out


# --- book_live: the per-revival outage duration ------------------------------

def test_book_live_marker_on_stale_to_live(capsys):
    b = _book()
    t0 = time.time()
    b.mark_stale(resync_id="r1")
    b.mark_live()
    out = capsys.readouterr().out
    assert "[recovery] phase=book_live asset=BTC cid=cid-marker stale_ms=" in out
    stale_ms = int(out.rsplit("stale_ms=", 1)[1].strip())
    # outage span is honest: bounded by the real wall-clock between the marks
    assert 0 <= stale_ms <= int((time.time() - t0) * 1000) + 50


def test_fresh_book_mark_live_is_silent(capsys):
    """A book that was never stale emits nothing (fresh-born revivals are not
    recovery events)."""
    b = _book()
    b.mark_live()
    assert capsys.readouterr().out == ""


# --- fetch_none_strike: the REST-blind timeline ------------------------------

def test_fetch_none_strike_markers_at_1_3_5(capsys):
    events = []
    mgr = _mgr(events)
    for n in (1, 2, 3, 4, 5):
        mgr.note_fetch_none("c-strike")
    out = capsys.readouterr().out
    assert out.count("[recovery] phase=fetch_none_strike cid=c-strike") == 3
    assert "n=1\n" in out
    assert "n=3\n" in out
    assert "n=5 quiet_s=3600" in out
    assert "n=2" not in out and "n=4" not in out


# --- provisional_promote: the unblocked fast lane firing ---------------------

def test_provisional_promote_marker_on_success(capsys):
    events = []
    mgr = _mgr(events)
    books = {"c1": _book("c1")}
    rid = mgr.handle_disconnect("BTC", "c1", reason="market_added", books=books)
    mgr.note_fetch_none("c1")  # open the cluster
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47, rid) is False
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47, rid) is False
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47, rid) is True
    out = capsys.readouterr().out
    assert "[recovery] phase=provisional_promote asset=BTC cid=c1" in out
    assert "frames=3" in out
    # book_live fired too — the revival is attributed with its outage span
    assert "[recovery] phase=book_live asset=BTC cid=c1 stale_ms=" in out


def test_no_provisional_marker_below_threshold(capsys):
    events = []
    mgr = _mgr(events)
    books = {"c1": _book("c1")}
    mgr.handle_disconnect("BTC", "c1", reason="market_added", books=books)
    assert mgr.try_ws_provisional_promote("c1", books, 0.45, 0.47) is False
    assert "phase=provisional_promote" not in capsys.readouterr().out
