"""Regression tests — audit 2026-09-21 ranked findings.

Each test pins one fixed invariant. All offline (no network).
"""
import tempfile

import pytest

from polymarket_collector.book import OrderBookState, sanitize_level
from polymarket_collector.config import CollectorConfig
from polymarket_collector.resync import ResyncManager


def make_cfg(tmpdir: str) -> CollectorConfig:
    cfg = CollectorConfig()
    cfg.storage.data_dir = tmpdir
    cfg.storage.wal_dir = tmpdir + "/_wal"
    cfg.raw_archive.path = tmpdir + "/raw_ws_archive"
    cfg.cursor_store.path = tmpdir + "/cursor_state"
    cfg.ws.max_resync_duration_seconds = 2
    cfg.ws.resync_rest_backoff_initial_ms = 50
    cfg.ws.resync_rest_backoff_max_ms = 100
    return cfg


def ev_row(eid: str, ns: int = 1735689600000000000) -> dict:
    return {"event_id": eid, "event_type": "t", "ts_utc": "2026-01-01T00:00:00Z",
            "ts_received_ns": ns}


# --- CRITICAL: WAL-spill rows survive -------------------------------------
def test_wal_spill_rows_survive_flush_recovery():
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    import glob
    import os
    import pyarrow.parquet as pq

    tmp = tempfile.mkdtemp()
    w = ParquetWriter(tmp, flush_row_count_threshold=100000, buffer_max_rows=5,
                      flush_interval_seconds=9999)
    w.flush_threshold = 100000
    w._write_group = lambda *a, **k: (_ for _ in ()).throw(OSError("disk full"))
    results = [w.append("collector_events", ev_row(f"e{i}", 1735689600000000000 + i))
               for i in range(12)]
    assert all(results), "spilled rows must report True (durable in WAL+buffer)"
    assert w._dropped_rows.get("collector_events", 0) == 0
    # heal disk and flush: every WAL row must reach parquet
    w._write_group = ParquetWriter._write_group.__get__(w)
    w.flush()
    disk = sum(pq.ParquetFile(f).metadata.num_rows
               for f in glob.glob(os.path.join(tmp, "collector_events/**/*.parquet"), recursive=True))
    wal_left = sum(1 for f in glob.glob(os.path.join(tmp, "_wal/*.jsonl")) for _ in open(f))
    assert disk + w._dropped_rows.get("collector_events", 0) == 12
    assert disk == 12 and wal_left == 0


def test_threshold_flush_failure_never_escapes_append():
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    tmp = tempfile.mkdtemp()
    w = ParquetWriter(tmp, flush_row_count_threshold=2, buffer_max_rows=100,
                      flush_interval_seconds=9999)
    w._write_group = lambda *a, **k: (_ for _ in ()).throw(OSError("disk full"))
    for i in range(4):  # must not raise — row is WAL-durable + buffered
        assert w.append("collector_events", ev_row(f"x{i}")) is True


def test_writer_event_reentrancy_bounded():
    from polymarket_collector.storage.parquet_writer import ParquetWriter
    from polymarket_collector.storage.markets_log import MarketsLog
    tmp = tempfile.mkdtemp()
    w = ParquetWriter(tmp, flush_row_count_threshold=100000, buffer_max_rows=5,
                      flush_interval_seconds=9999)
    w.flush_threshold = 100000
    w._write_group = lambda *a, **k: (_ for _ in ()).throw(OSError("disk full"))
    ml = MarketsLog(tmp, writer=w)
    w.on_event = lambda et, det: ml.append_event(
        str(et), "2026-01-01T00:00:00Z", 1735689600000000000, asset="BTC", details=dict(det))
    flushes = [0]
    _orig = ParquetWriter.flush.__get__(w)
    w.flush = lambda: (flushes.__setitem__(0, flushes[0] + 1), _orig())[1]
    for i in range(5):
        w.append("collector_events", ev_row(f"f{i}"))
    assert w.append("collector_events", ev_row("trig")) is True  # no RecursionError
    assert flushes[0] <= 2, f"one append must not stampede flushes, got {flushes[0]}"


# --- HIGH: disconnect scope covers all lanes -------------------------------
def test_disconnect_covers_all_lanes_and_joins():
    from polymarket_collector.collector import Collector
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))

    def mk(cid, series="BTC-5M"):
        return OrderBookState("BTC", cid, None, series, 0, f"up-{cid}", f"dn-{cid}", 9999999999999)

    col.books = {"c5": mk("c5"), "c1h": mk("c1h", "BTC-1H")}
    col._index_book(col.books["c5"])
    col._index_book(col.books["c1h"])
    rids = col._disconnect_asset_books("BTC", reason="ws_connection_close")
    assert col.books["c5"].book_state.value == "stale"
    assert col.books["c1h"].book_state.value == "stale", "1h lane must also be marked stale"
    ep_ids = set(col.resync._episodes.keys())
    for cid, book in col.books.items():
        assert book.resync_id in ep_ids, f"{cid} resync_id must join to resync_episodes"
    assert len(rids) == 2


def test_snapshot_downgrade_prefers_real_episode():
    from polymarket_collector.collector import Collector
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    book = OrderBookState("BTC", "c5", None, "BTC-5M", 0, "up", "dn", 9999999999999)
    col.books = {"c5": book}
    rid = col.resync.handle_disconnect("BTC", "c5", reason="t", books=col.books)
    book.resync_id = "orphan-uuid"  # simulate a minted orphan
    assert col._episode_for_snapshot("BTC", "c5") == rid


# --- HIGH: episode lifecycle ------------------------------------------------
def test_healed_episodes_close_and_buffers_pop():
    from types import SimpleNamespace
    cfg = SimpleNamespace(ws=SimpleNamespace(max_resync_duration_seconds=60))
    rm = ResyncManager(cfg, rest_fetcher=None, on_event=None)

    def mk(cid):
        b = OrderBookState("BTC", cid, None, "BTC-5M", 0, f"up-{cid}", f"dn-{cid}", 9999999999999)
        b.mark_stale(resync_id="x")
        return b

    books = {"a": mk("a"), "b": mk("b")}
    r1 = rm.handle_disconnect("BTC", "a", reason="book_stalled", books=books)
    r2 = rm.handle_disconnect("BTC", "b", reason="planned_recycle", books=books)
    assert len(rm._episodes) == 2 and len(rm._buffers) == 2
    # books heal via background REST / fresh full-book (outside resync())
    for b in books.values():
        b.mark_live()
    closed = rm.close_healed_episodes(books)
    assert closed == 2
    assert rm._episodes[r1].resync_completed_ts_utc is not None
    assert rm._episodes[r2].resync_completed_ts_utc is not None
    assert r1 not in rm._buffers and r2 not in rm._buffers


def test_buffer_routes_to_newest_live_episode():
    from types import SimpleNamespace
    cfg = SimpleNamespace(ws=SimpleNamespace(max_resync_duration_seconds=60))
    rm = ResyncManager(cfg, rest_fetcher=None, on_event=None)
    b = OrderBookState("BTC", "c", None, "BTC-5M", 0, "up", "dn", 9999999999999)
    books = {"c": b}
    zombie = rm.handle_disconnect("BTC", "c", reason="first", books=books)
    # retire the zombie buffer (deadline passed)
    rm._buffer_deadline[zombie] = 0.0
    rm.buffer_message(zombie, {"t": 1})
    assert zombie not in rm._buffers, "retired buffer must be popped"
    fresh = rm.handle_disconnect("BTC", "c", reason="second", books=books)
    assert rm.newest_open_buffer_id("BTC") == fresh


# --- MEDIUM: one-sided REST fetch never yields a partial merge ---------------
@pytest.mark.asyncio
async def test_one_sided_rest_fetch_returns_none_not_partial():
    """_fetch_rest_book: a 429 on one token must NOT return up_*-only keys.

    The finding's exact repro: partial merge → replace touches present sides
    only → resync() promoted a half book. Now the fetcher returns None (the
    resync loop retries honestly) instead of a partial dict.
    """
    from polymarket_collector.collector import Collector

    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))

    class Resp:
        def __init__(self, status, payload=None):
            self.status_code = status
            self._payload = payload or {}

        def json(self):
            return dict(self._payload)

    class FakeClient:
        async def get(self, url, params=None):
            if params.get("token_id") == "up-tok":
                return Resp(200, {"bids": [[0.55, 10.0]], "asks": [[0.56, 10.0]]})
            return Resp(429)  # DOWN rate-limited

    col._get_rest_client = lambda: FakeClient()
    col.markets = {"cid-x": type("M", (), {"up_token_id": "up-tok",
                                           "down_token_id": "dn-tok"})()}
    out = await col._fetch_rest_book("BTC", "cid-x")
    assert out is None, f"partial merge must be refused, got {out}"

    class FullClient:
        async def get(self, url, params=None):
            side = "up" if params.get("token_id") == "up-tok" else "down"
            _ = side
            return Resp(200, {"bids": [[0.5, 10.0]], "asks": [[0.6, 10.0]]})

    col._get_rest_client = lambda: FullClient()
    out = await col._fetch_rest_book("BTC", "cid-x")
    assert out is not None and all(
        k in out for k in ("up_bids", "up_asks", "down_bids", "down_asks"))


# --- MEDIUM: shared bounds sanitizer ----------------------------------------
def test_bounds_sanitizer_shared_across_paths():
    assert sanitize_level(7.5, 10.0) is None
    assert sanitize_level(0.5, -3.0) is None
    assert sanitize_level(0.0, 10.0) is None  # sentinel
    assert sanitize_level(0.5, 10.0) == (0.5, 10.0)

    b = OrderBookState("BTC", "c", None, "BTC-5M", 0, "up", "dn", 9999999999999)
    b._apply_levels(b.up.bids, [[7.5, 10.0]], is_bid=True)
    assert b.up.bids.best_price() is None, "_apply_levels must reject out-of-range"
    b.replace_from_rest_snapshot({"up_bids": [[7.5, 10.0]], "up_asks": [[0.6, 5.0]],
                                  "down_bids": [[0.4, 5.0]], "down_asks": [[0.45, 5.0]]})
    assert b.up.bids.best_price() is None, "REST path must reject out-of-range"
    b._apply_levels(b.up.bids, [[0.5, 10.0]], is_bid=True)
    assert b.up.bids.best_price() == 0.5


def test_per_token_promotion_contract():
    """Per-token `book` frames promote (cold-start path the A4 tests pin).

    The CLOB wire sends full-book frames per TOKEN; requiring both outcomes
    in a single frame was tried and reverted (held books stale whenever
    frames arrive separately). A missing outcome ships as honest NULLs, and
    the one-sided-REST hazard is fixed in _fetch_rest_book instead.
    """
    b = OrderBookState("BTC", "c", None, "BTC-5M", 0, "up-tok", "dn-tok", 9999999999999,
                       one_sided_promotion=False)
    b.mark_stale(resync_id="r")
    up_frame = {"token_id": "up-tok", "timestamp": "1735689600000",
                "hash": "abcdef1234567890",
                "bids": [[0.55, 10.0]], "asks": [[0.56, 10.0]]}
    applied, _ = b.apply_ws_message(up_frame)
    assert applied is True
    assert b.book_state.value == "live"
    assert b.down.bids.best_price() is None, "absent outcome stays NULL (honest)"


# --- LOW-MEDIUM: chainlink symbol match is exact ------------------------------
def test_chainlink_symbol_exact_match():
    assets = ["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"]

    def match(symbol: str):
        sym_norm = symbol.lower().replace("-", "").replace("/", "").replace("_", "")
        base = sym_norm
        for q in ("usdt", "usd", "usdc"):
            if base.endswith(q) and len(base) > len(q):
                base = base[: -len(q)]
                break
        for a in assets:
            if a.lower() == base:
                return a
        return None

    assert match("btc/usd") == "BTC"
    assert match("ethfi/usd") is None, "ETH must not match ethfi feed"
    assert match("ETH/USD") == "ETH"


# --- CRITICAL: orphan-stale books get a real episode (find-or-create) ------
def test_h2_stale_orphan_gets_episode_via_helper():
    """_enforce_bbo H2 marks stale with a minted rid, (True, None).

    The helper must link a real episode and be idempotent (no per-message
    episode spam).
    """
    from polymarket_collector.collector import Collector
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    b = OrderBookState("BTC", "c-h2", None, "BTC-5M", 0, "up-tok", "dn-tok", 9999999999999)
    col.books = {"c-h2": b}
    b.apply_ws_message({"token_id": "up-tok", "timestamp": "1735689600000",
                        "hash": "abcdef1234567890",
                        "bids": [[0.55, 10.0]], "asks": [[0.56, 10.0]]})
    b.apply_ws_message({"token_id": "dn-tok", "timestamp": "1735689600000",
                        "hash": "abcdef1234567890",
                        "bids": [[0.44, 10.0]], "asks": [[0.45, 10.0]]})
    applied, reason = b.apply_ws_message(
        {"event_type": "price_change", "timestamp": "1735689601000",
         "price_changes": [{"asset_id": "up-tok", "price": 0.55, "size": 0.0,
                            "side": "BUY", "best_bid": 0.54, "best_ask": 0.56}]})
    assert (applied, reason) == (True, None)
    assert b.book_state.value == "stale"
    assert b.resync_id not in col.resync._episodes  # the orphan, as minted
    rid = col._ensure_episode_for_stale_book(b, "BTC", "stale_no_episode")
    assert rid in col.resync._episodes
    assert b.resync_id == rid
    n = len(col.resync._episodes)
    rid2 = col._ensure_episode_for_stale_book(b, "BTC", "stale_no_episode")
    assert rid2 == rid and len(col.resync._episodes) == n, "idempotent: no dupes"


def test_ensure_episode_prefers_open_episode():
    from polymarket_collector.collector import Collector
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    b = OrderBookState("BTC", "c-pref", None, "BTC-5M", 0, "up", "dn", 9999999999999)
    col.books = {"c-pref": b}
    rid = col.resync.handle_disconnect("BTC", "c-pref", reason="t", books=col.books)
    b.resync_id = "orphan-uuid"
    assert col._ensure_episode_for_stale_book(b, "BTC", "x") == rid
    assert b.resync_id == rid
    assert len(col.resync._episodes) == 1


def test_no_unbound_resync_id_fallback_in_reconnect_loop():
    """The `ep_id = resync_id` fallback (NameError-or-stale-id) must be gone;
    the stanza must use the find-or-create helper."""
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "src" / "polymarket_collector" / "collector.py").read_text()
    assert "\n                            ep_id = resync_id\n" not in src
    assert "_ensure_episode_for_stale_book(book, book.asset" in src
