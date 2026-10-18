"""stale_no_episode closure at emit time: stale snapshot rows offline from the
WS-apply path get a resync_episodes parent minted once per stale transition.

Regression for backfill item #1 (HANDOFF.md §7, measured 2026-10-18): books
marked stale off the message path (cursor recovery, quiet books whose next
real frame is the heal) never reached the per-message
`_ensure_episode_for_stale_book` hook, so every 2Hz snapshot row of the spell
carried a resync_id that joins to no resync_episodes parent. All offline
(real objects, no network, no fabricated rows — evidence minting only).
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
    return Collector(make_cfg(tempfile.mkdtemp()))


def _book(cid, asset="BTC"):
    return OrderBookState(asset, cid, None, "BTC-5M", 0, "up-tok", "dn-tok",
                          9999999999999)


def test_stale_orphan_row_mints_parent_and_row_joins():
    """Orphan stale row: the emit-path hook mints ONE parent and tags the row."""
    col = _col()
    b = _book("c-srp1")
    col.books = {b.condition_id: b}
    b.mark_stale(resync_id="orphan-srp-uuid")
    row = {"book_state": "stale", "resync_id": "orphan-srp-uuid"}
    col._ensure_stale_row_parent(b, "BTC", row)
    assert row["resync_id"] in col.resync._episodes, "row now joins a parent"
    assert row["resync_id"] != "orphan-srp-uuid", "orphan uuid replaced by joined id"
    parents = [e for e in col.resync._episodes.values()
               if e.disconnect_reason == "stale_no_episode"]
    assert len(parents) == 1, "one evidence episode per stale transition"
    assert parents[0].asset == "BTC"
    assert b.resync_id == row["resync_id"], "book re-pointed to the joined parent"


def test_stale_row_parent_idempotent_per_transition():
    """Repeated rows of one spell: one parent, one dict check apart, no storm."""
    col = _col()
    b = _book("c-srp2")
    col.books = {b.condition_id: b}
    b.mark_stale(resync_id="orphan-srp2")
    row = {"book_state": "stale", "resync_id": "orphan-srp2"}
    col._ensure_stale_row_parent(b, "BTC", row)
    rid1 = row["resync_id"]
    n = len(col.resync._episodes)
    disc_calls = []
    _orig = col.resync.handle_disconnect
    col.resync.handle_disconnect = lambda *a, **k: (disc_calls.append(a), _orig(*a, **k))[1]
    try:
        for _ in range(50):
            r2 = {"book_state": "stale", "resync_id": rid1}
            col._ensure_stale_row_parent(b, "BTC", r2)
            assert r2["resync_id"] == rid1, "joined rid short-circuits"
    finally:
        col.resync.handle_disconnect = _orig
    assert disc_calls == [], "no second mint once the parent joins"
    assert len(col.resync._episodes) == n, "episode count stable"


def test_live_row_never_mints():
    """The hook is a stale-only guard: live rows mint nothing."""
    col = _col()
    b = _book("c-srp3")
    col.books = {b.condition_id: b}
    b.mark_live()
    row = {"book_state": "live", "resync_id": None}
    col._ensure_stale_row_parent(b, "BTC", row)
    assert len(col.resync._episodes) == 0
    assert row["resync_id"] is None


def test_null_rid_stale_row_gets_parent():
    """Stale row with honest NULL rid: parent minted, row tagged, never lost."""
    col = _col()
    b = _book("c-srp4")
    col.books = {b.condition_id: b}
    b.mark_stale(resync_id="orphan-srp4")
    b.resync_id = None
    row = {"book_state": "stale", "resync_id": None}
    col._ensure_stale_row_parent(b, "BTC", row)
    assert row["resync_id"] in col.resync._episodes, "NULL rid no longer joins to nothing"


def test_row_content_untouched():
    """Row-only: relabeling never happens; only resync_id may change."""
    col = _col()
    b = _book("c-srp5")
    col.books = {b.condition_id: b}
    b.mark_stale(resync_id="orphan-srp5")
    row = {"book_state": "stale", "resync_id": "orphan-srp5", "up_bid": 0.42}
    col._ensure_stale_row_parent(b, "BTC", row)
    assert row["book_state"] == "stale", "no relabel"
    assert row["up_bid"] == 0.42, "content untouched"
