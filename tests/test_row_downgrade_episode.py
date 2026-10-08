"""Row-downgrade path is ROW-ONLY: snapshot ticks must never kill RAM liveness.

Regression for the promote->disconnect cycle: try_ws_provisional_promote ->
book.mark_live() fired, but the next snapshot tick's row-downgrade
(_row_episode_id -> _ensure_episode_for_stale_book -> handle_disconnect ->
book.mark_stale) killed RAM liveness, so live never survived. All offline
(real objects, no network, no synthetic rows).
"""
import tempfile

from polymarket_collector.book import OrderBookState
from polymarket_collector.collector import Collector
from polymarket_collector.config import CollectorConfig


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


def _col():
    col = Collector(make_cfg(tempfile.mkdtemp()))
    return col


def _book(cid="c-row", asset="BTC"):
    return OrderBookState(asset, cid, None, "BTC-5M", 0, "up-tok", "dn-tok",
                          9999999999999)


def test_row_episode_id_never_marks_stale_or_mints():
    """Live book + no open episode: row path mints nothing, touches nothing."""
    col = _col()
    b = _book()
    col.books = {b.condition_id: b}
    b.mark_live()
    assert b.resync_id is None
    stale_calls = []
    disc_calls = []
    _orig_stale = b.mark_stale
    _orig_disc = col.resync.handle_disconnect
    b.mark_stale = lambda *a, **k: (stale_calls.append((a, k)), _orig_stale(*a, **k))
    col.resync.handle_disconnect = lambda *a, **k: (disc_calls.append((a, k)), _orig_disc(*a, **k))[1]
    try:
        tag = col._row_episode_id(b, "BTC", b.condition_id, "ws_down_downgrade")
    finally:
        b.mark_stale = _orig_stale
        col.resync.handle_disconnect = _orig_disc
    assert tag is None, "no episode, no rid -> honest NULL row tag"
    assert stale_calls == [], "row path must never mark_stale"
    assert disc_calls == [], "row path must never handle_disconnect"
    assert b.book_state.value == "live", "RAM liveness survives the row tick"
    assert len(col.resync._episodes) == 0, "no episode minted by a row tag"


def test_row_episode_id_reuses_open_episode_without_repointing():
    """Live book (rid cleared by mark_live) still tags rows with the open episode."""
    col = _col()
    b = _book("c-open")
    col.books = {b.condition_id: b}
    rid = col.resync.handle_disconnect("BTC", b.condition_id, reason="t", books=col.books)
    assert b.book_state.value == "stale"
    b.mark_live()  # promotion clears the rid, episode stays open
    assert b.resync_id is None
    tag = col._row_episode_id(b, "BTC", b.condition_id, "catchup_downgrade")
    assert tag == rid, "row reuses the open episode id"
    assert b.book_state.value == "live"
    assert b.resync_id is None, "ROW-ONLY: no resync_id re-point onto the book"
    assert len(col.resync._episodes) == 1, "no duplicate episode minted"


def test_row_episode_id_returns_last_known_rid_when_no_episode():
    """Stale book with orphan rid, no open episode: row carries the honest tag."""
    col = _col()
    b = _book("c-orphan")
    col.books = {b.condition_id: b}
    b.mark_stale(resync_id="orphan-uuid-row")
    n = len(col.resync._episodes)
    tag = col._row_episode_id(b, "BTC", b.condition_id, "ws_down_downgrade")
    assert tag == "orphan-uuid-row", "last-known rid, never a fabricated join"
    assert len(col.resync._episodes) == n, "no mint from the row path"


def test_episode_rows_still_emitted_on_real_disconnect():
    """Gap evidence intact: the real disconnect path still mints episode + stale."""
    col = _col()
    b = _book("c-ev")
    col.books = {b.condition_id: b}
    b.mark_live()
    rid = col.resync.handle_disconnect("BTC", b.condition_id, reason="book_stalled",
                                       books=col.books)
    assert rid in col.resync._episodes, "resync_episodes row minted"
    assert b.book_state.value == "stale"
    assert b.resync_id == rid


def test_per_message_backfill_still_mints_for_stale_orphan():
    """The stale-guarded ensure path (per-message backfill) still closes orphans."""
    col = _col()
    b = _book("c-h2row")
    col.books = {b.condition_id: b}
    b.mark_stale(resync_id="orphan-h2")
    assert b.resync_id not in col.resync._episodes
    rid = col._ensure_episode_for_stale_book(b, "BTC", "stale_no_episode")
    assert rid in col.resync._episodes, "evidence minter for already-stale books intact"
    assert b.resync_id == rid


def test_promotion_survives_downgrade_tick():
    """Full cycle: 3 consistent WS frames promote -> row tick leaves book live."""
    col = _col()
    b = _book("c-promote")
    col.books = {b.condition_id: b}
    b.mark_stale(resync_id="pre-promote-orphan")
    for _ in range(3):
        ok = col.resync.try_ws_provisional_promote(
            b.condition_id, col.books, 0.45, 0.47,
            resync_id=getattr(b, "resync_id", None))
    assert ok is True
    assert b.book_state.value == "live", "provisional promotion fires"
    # Promotion completes the linked episode -> no open episode remains,
    # exactly the state where the old row path minted + re-staled.
    n = len(col.resync._episodes)
    tag = col._row_episode_id(b, "BTC", b.condition_id, "ws_down_downgrade")
    assert b.book_state.value == "live", "live survives the downgrade tick"
    assert len(col.resync._episodes) == n, "row tick mints no episode"
    assert tag is None, "no open episode and cleared rid -> honest NULL"
