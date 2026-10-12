"""M5 catch-up downgrade × P3 freshness gate reconciliation (2026-10-12).

Measured failure (2026-10-12 16:22-16:31 UTC, PID 1218164): newest-file
snapshot rows read 0% live while 100% of four-price rows carried
up/down_book_age_ms = 0 (p50=p90=p99=max) — books were quoting at label
time. The scheduler lived in permanent catch-up (20,926 scheduler_lag
events/14h, cur_bucket-last_bucket pinned ~76s), so ~97% of rows were
DEFERRED buckets and the M5 blanket downgrade (2026-09-17) reverted every
gate-decided live label after e974883 had already certified the content.

These tests pin the reconciliation: a deferred bucket keeps the live label
only while the book's last exchange update stayed within FRESH_BOOK_AGE_MS
of the bucket (the same window the row gate trusts); content that
postdates the bucket by more than the window is genuine forward-fill and
keeps the honest stale label; missing clocks fail closed to the old
blanket rule; on-time buckets never reach the downgrade at all.

Real OrderBookState objects + real WS frames; no network, no synthetics.
"""
import time

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


def make_book(cid="cid-defer", asset="BTC") -> OrderBookState:
    b = OrderBookState(
        asset=asset,
        condition_id=cid,
        market_id="mid-1",
        series_id="BTC-5MIN",
        window_index=7,
        up_token_id="up-123",
        down_token_id="down-456",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
    )
    b.apply_ws_message({"token_id": "up-123", "bids": [[0.48, 10]], "asks": [[0.52, 10]]})
    b.apply_ws_message({"token_id": "down-456", "bids": [[0.48, 10]], "asks": [[0.52, 10]]})
    return b


def _deferred_bucket(defer_ms: int = 10_000) -> int:
    """A bucket `defer_ms` behind the scheduler's current bucket."""
    cur = (int(time.time() * 1000) // 500) * 500
    return cur - defer_ms


def _mirror_downgrade(col: Collector, book: OrderBookState, row: dict,
                      bucket: int, cur_bucket: int) -> dict:
    """Mirror of the _snapshot_loop M5 branch (collector.py, 'M5 (2026-09-17)
    + P3 (e974883) reconciliation') — kept verbatim so these tests pin the
    exact production condition, not a re-implementation."""
    if (bucket < cur_bucket and row.get("book_state") == "live"
            and col._deferred_content_diverges(book, bucket)):
        row["book_state"] = "stale"
    return row


# --- deferred bucket, content certifiable at bucket time -> keeps live ------

def test_deferred_bucket_fresh_content_keeps_live_label():
    """THE fix: a deferred bucket whose last exchange update stayed within
    FRESH_BOOK_AGE_MS of the bucket keeps the gate's live label (the old
    blanket downgrade ate it — the 0%-live failure)."""
    import tempfile
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    b = make_book()
    col.books[b.condition_id] = b
    bucket = _deferred_bucket(10_000)
    # content certified at bucket time: last update 2s after the bucket,
    # inside the freshness window the row gate itself trusts
    b._up_last_update_ns = b._down_last_update_ns = bucket * 1_000_000 + 2_000_000_000
    assert col._deferred_content_diverges(b, bucket) is False
    row = b.snapshot(ts_ms=bucket).to_flat_dict()
    assert row["book_state"] == "live"  # e974883 gate certified the content
    cur_bucket = bucket + 10_000
    row = _mirror_downgrade(col, b, row, bucket, cur_bucket)
    assert row["book_state"] == "live", (
        "deferred bucket with in-window content must keep the live label")


def test_deferred_bucket_window_boundary_is_inclusive():
    """Exactly FRESH_BOOK_AGE_MS after the bucket == still certifiable (the
    same inclusive boundary fresh_verified uses: age 5000 is not > 5000)."""
    import tempfile
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    b = make_book()
    bucket = _deferred_bucket(10_000)
    b._up_last_update_ns = b._down_last_update_ns = (
        bucket * 1_000_000 + b.FRESH_BOOK_AGE_MS * 1_000_000)
    assert col._deferred_content_diverges(b, bucket) is False


# --- deferred bucket, forward-filled content -> keeps the honest label -----

def test_deferred_bucket_forward_filled_content_downgrades():
    """M5's honesty preserved: content updated 30s PAST the bucket cannot
    certify the bucket — the row must read stale (with the episode join)."""
    import tempfile
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    b = make_book()
    b.mark_stale(resync_id="ep-ff")  # episode open, feed keeps streaming
    col.books[b.condition_id] = b
    bucket = _deferred_bucket(60_000)
    b._up_last_update_ns = b._down_last_update_ns = bucket * 1_000_000 + 30_000_000_000
    assert col._deferred_content_diverges(b, bucket) is True
    row = b.snapshot(ts_ms=bucket).to_flat_dict()
    assert row["book_state"] == "live"  # gate: fresh vs the clamp hole
    row = _mirror_downgrade(col, b, row, bucket, bucket + 60_000)
    assert row["book_state"] == "stale"
    assert row["resync_id"] == "ep-ff"


def test_deferred_bucket_content_predating_bucket_is_authentic():
    """A book that went quiet BEFORE the bucket: no update landed in the
    deferral window, so RAM holds exactly the bucket-time book — the live
    label survives (same freshness standard as an on-time row)."""
    import tempfile
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    b = make_book()
    bucket = _deferred_bucket(10_000)
    b._up_last_update_ns = b._down_last_update_ns = bucket * 1_000_000 - 300_000_000
    assert col._deferred_content_diverges(b, bucket) is False


# --- fail-closed -----------------------------------------------------------

def test_deferred_bucket_never_updated_clock_fails_closed():
    """A never-updated outcome cannot certify anything — downgrade exactly
    like the old blanket rule."""
    import tempfile
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    b = make_book()
    b._up_last_update_ns = None
    assert col._deferred_content_diverges(b, _deferred_bucket()) is True


def test_deferred_content_diverges_never_raises():
    import tempfile
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    assert col._deferred_content_diverges(None, 0) is True  # total garbage
    assert col._deferred_content_diverges(make_book(), None) is True


# --- on-time buckets never reach the downgrade -----------------------------

def test_ontime_bucket_never_downgraded():
    """bucket == cur_bucket is the normal healthy path: the downgrade branch
    must not fire regardless of content."""
    import tempfile
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    b = make_book()
    bucket = (int(time.time() * 1000) // 500) * 500
    row = b.snapshot(ts_ms=bucket).to_flat_dict()
    assert row["book_state"] == "live"
    row = _mirror_downgrade(col, b, row, bucket, bucket)
    assert row["book_state"] == "live"
