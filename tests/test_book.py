"""Tests for §3 order book — null-vs-zero, depth aggregates, snapshot alignment."""
import time

from polymarket_collector.book import OrderBookState, snapshot_bucket_ms, depth_within
from polymarket_collector.enums import BookState


def make_book(asset="BTC", condition_id="cid-1"):
    return OrderBookState(
        asset=asset,
        condition_id=condition_id,
        market_id="mid-1",
        series_id="BTC-5MIN",
        window_index=42,
        up_token_id="up-123",
        down_token_id="down-456",
        market_end_ts_ms=int(time.time() * 1000) + 300_000,
        l2_levels=20,
    )


def test_null_vs_zero_empty_side():
    book = make_book()
    # empty book → snapshot should have None, not 0
    snap = book.snapshot()
    assert snap.up_bid is None
    assert snap.up_bid_size is None
    assert snap.up_ask is None
    assert snap.depths["up_bid_depth_1c"] is None
    assert snap.depths["up_ask_depth_1c"] is None
    # l2 levels all None
    assert snap.l2["up_bid_level_1_price"] is None
    assert snap.l2["up_bid_level_1_size"] is None

    # apply a valid level
    book.apply_ws_message({"token_id": "up-123", "bids": [[0.55, 100]], "sequence_number": 1})
    snap2 = book.snapshot()
    assert snap2.up_bid == 0.55
    assert snap2.up_bid_size == 100
    # depth within 1c should include that level
    assert snap2.depths["up_bid_depth_1c"] == 100.0


def test_depth_aggregates_precise_definition():
    # depth_Nc = cumulative size within N cents of own best, not mid/opposite
    levels = [(0.55, 100), (0.54, 50), (0.50, 20), (0.44, 10)]  # bids descending
    best = 0.55
    # within 1c (0.01): only 0.55 and 0.54
    assert depth_within(levels, best, 1) == 150.0  # 100+50
    # within 5c: 0.55,0.54,0.50 (0.05 exactly) — inclusive
    assert depth_within(levels, best, 5) == 170.0  # 100+50+20
    # within 10c: all except 0.44 is 0.11 away → 170
    assert depth_within(levels, best, 10) == 170.0
    # empty side → None
    assert depth_within([], None, 1) is None
    assert depth_within(levels, None, 1) is None


def test_depth_aggregates_in_snapshot():
    book = make_book()
    # UP bids: 0.60 (100), 0.595 (50), 0.50 (20)
    book.apply_ws_message({"token_id": "up-123", "bids": [[0.60, 100], [0.595, 50], [0.50, 20]], "asks": [[0.65, 30]], "sequence_number": 1})
    book.apply_ws_message({"token_id": "up-123", "bids": [[0.60, 100], [0.595, 50], [0.50, 20]], "asks": [[0.65, 30]], "sequence_number": 2})
    snap = book.snapshot()
    # up_bid_depth_1c: within 0.01 of 0.60 → 0.60 and 0.595 (0.005 away) → 150
    assert snap.depths["up_bid_depth_1c"] == 150.0
    # up_bid_depth_5c: within 0.05 of 0.60 → still 150 (0.50 is 0.10 away)
    assert snap.depths["up_bid_depth_5c"] == 150.0
    # up_bid_depth_10c: within 0.10 → all 170
    assert snap.depths["up_bid_depth_10c"] == 170.0


def test_snapshot_bucket_alignment():
    # §1A/§3 shared wall-clock grid
    assert snapshot_bucket_ms(0, 500) == 0
    assert snapshot_bucket_ms(499, 500) == 0
    assert snapshot_bucket_ms(500, 500) == 500
    assert snapshot_bucket_ms(1234, 500) == 1000
    assert snapshot_bucket_ms(1001, 500) == 1000
    # same bucket for two collectors at same wall time → dedup works
    ts = 1_700_000_000_123
    assert snapshot_bucket_ms(ts, 500) == snapshot_bucket_ms(ts + 10, 500)


def test_book_crossed_flag():
    book = make_book()
    book.apply_ws_message({"token_id": "up-123", "bids": [[0.70, 10]], "asks": [[0.65, 10]], "sequence_number": 1})
    # need to ensure cross is detected: same outcome bid >= ask
    assert book.is_crossed() is True
    snap = book.snapshot()
    assert snap.book_crossed is True

    book2 = make_book()
    book2.apply_ws_message({"token_id": "up-123", "bids": [[0.60, 10]], "asks": [[0.65, 10]], "sequence_number": 1})
    assert book2.is_crossed() is False


def test_l2_null_padding():
    book = make_book()
    book.apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 1})
    snap = book.snapshot()
    # only 1 level provided, rest should be None padded to 20
    assert snap.l2["up_bid_level_1_price"] == 0.5
    assert snap.l2["up_bid_level_2_price"] is None
    assert snap.l2["up_bid_level_20_price"] is None


def test_book_state_transitions():
    book = make_book()
    assert book.book_state == BookState.live
    resync_id = "test-resync"
    book.mark_stale(resync_id)
    assert book.book_state == BookState.stale
    assert book.resync_id == resync_id
    book.mark_resyncing(resync_id)
    assert book.book_state == BookState.resyncing
    book.mark_live()
    assert book.book_state == BookState.live


def test_sanity_bounds_rejects_and_marks_stale():
    book = make_book()
    # price outside [0,1] → rejected, book marked stale
    ok, reason = book.apply_ws_message({"token_id": "up-123", "bids": [[1.4, 10]], "sequence_number": 1})
    assert ok is False
    assert "outside" in reason or "sanity" in reason
    assert book.book_state == BookState.stale

    book2 = make_book()
    ok, reason = book2.apply_ws_message({"token_id": "up-123", "bids": [[0.5, -5]], "sequence_number": 1})
    assert ok is False
    assert book2.book_state == BookState.stale


def test_sequence_gap_detection():
    book = make_book()
    book.apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 1})
    assert book.sequence_numbers["up-123"] == 1
    # correct next
    ok, _ = book.apply_ws_message({"token_id": "up-123", "bids": [[0.51, 10]], "sequence_number": 2})
    assert ok is True
    # gap: expected 3 but got 5 → stale
    ok, reason = book.apply_ws_message({"token_id": "up-123", "bids": [[0.52, 10]], "sequence_number": 5})
    assert ok is False
    assert "sequence_gap" in reason
    assert book.book_state == BookState.stale

    # duplicate should be dropped not stale (already stale but check duplicate path)
    book3 = make_book()
    book3.apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 1})
    ok, reason = book3.apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 1})
    assert ok is False
    assert reason == "duplicate_event"

    # out of order lower than last
    ok, reason = book3.apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 0})
    assert ok is False
    assert reason == "out_of_order_duplicate"


def test_replace_from_rest_snapshot():
    book = make_book()
    book.apply_ws_message({"token_id": "up-123", "bids": [[0.5, 10]], "sequence_number": 1})
    assert book.up.bids.best_price() == 0.5
    book.replace_from_rest_snapshot({
        "up_bids": [[0.60, 100], [0.59, 50]],
        "up_asks": [[0.65, 30]],
        "down_bids": [[0.40, 20]],
        "down_asks": [[0.45, 25]],
        "sequence_number": 99,
    })
    assert book.up.bids.best_price() == 0.60
    assert book.sequence_numbers.get("up-123") == 99 or book.sequence_numbers.get(book.up_token_id) == 99


def test_full_book_diff_drift():
    book = make_book()
    book.apply_ws_message({"token_id": "up-123", "bids": [[0.60, 100]], "asks": [[0.65, 30]], "sequence_number": 1})
    rest = {
        "up_bids": [[0.60, 100]],
        "up_asks": [[0.65, 30]],
    }
    # no drift
    assert book.diff_against_rest(rest) is None
    # drift
    rest_drift = {
        "up_bids": [[0.61, 100]],  # different price
        "up_asks": [[0.65, 30]],
    }
    diff = book.diff_against_rest(rest_drift)
    assert diff is not None
    assert "mismatches" in diff


def _live_book_frame(ts=None):
    """Realistic CLOB `book` frame (live probe 2026-09-05 shape)."""
    m = {"event_type": "book", "asset_id": "up-123", "market": "0xm",
         "bids": [{"price": "0.50", "size": "10"}], "asks": [{"price": "0.52", "size": "10"}],
         "hash": "0df9ed199b15f73551ec79f2b0d43cb805c0dafa"}
    if ts is not None:
        m["timestamp"] = ts
    return m


def test_a1_ts_source_from_frame_timestamp():
    # A1 probe: top-level `timestamp` always present live → events carry it
    book = make_book()
    book.apply_ws_message(_live_book_frame(ts="1788649334527"))
    evs = [e for e in book.drain_pending_events() if e["event_type"] == "price_change"]
    assert evs, "BBO change must emit a price_change event"
    assert all(e["ts_source"] == "1788649334527" for e in evs)


def test_a1_ts_source_carry_forward_within_window():
    # frame without timestamp reuses the previous frame's ts (same connection, fresh)
    book = make_book()
    book.apply_ws_message(_live_book_frame(ts="1788649334527"))
    book.drain_pending_events()
    f2 = _live_book_frame(ts=None)
    f2["bids"] = [{"price": "0.51", "size": "10"}]  # move BBO so an event fires
    book.apply_ws_message(f2)
    evs = [e for e in book.drain_pending_events() if e["event_type"] == "price_change"]
    assert evs
    assert all(e["ts_source"] == "1788649334527" for e in evs)


def test_a1_ts_source_stays_null_without_source():
    # no frame timestamp ever seen → NULL, never receive-time (no fabrication)
    book = make_book()
    book.apply_ws_message(_live_book_frame(ts=None))
    evs = [e for e in book.drain_pending_events() if e["event_type"] == "price_change"]
    assert evs
    assert all(e["ts_source"] is None for e in evs)


def test_a1_bbo_snapped_carries_ts_source():
    # bbo_snapped (previously always NULL) carries the frame timestamp
    book = make_book()
    book.apply_ws_message(_live_book_frame(ts="1788649334527"))
    book.drain_pending_events()
    pc = {"event_type": "price_change", "market": "0xm", "timestamp": "1788649335000",
          "price_changes": [{"asset_id": "up-123", "price": "0.53", "size": "5",
                             "side": "SELL", "hash": "abc123",
                             "best_bid": "0.50", "best_ask": "0.55"}]}
    book.apply_ws_message(pc)
    evs = book.drain_pending_events()
    snapped = [e for e in evs if e["event_type"] == "bbo_snapped"]
    assert snapped, f"expected bbo_snapped, got {[e['event_type'] for e in evs]}"
    assert all(e["ts_source"] == "1788649335000" for e in snapped)


def test_a4_hash_captured_and_gates_promotion():
    # full `book` frame WITH exchange hash promotes a stale book to live
    book = make_book()
    book.mark_stale("r1")
    assert book.book_state == BookState.stale
    applied, reason = book.apply_ws_message(_live_book_frame(ts="1788649334527"))
    assert applied is True
    assert reason is None
    assert book.book_state == BookState.live
    assert book.book_hash["up"] == "0df9ed199b15f73551ec79f2b0d43cb805c0dafa"


def test_a4_promotion_refused_without_hash():
    # snapshot WITHOUT hash: levels still applied, but NO promotion (REST heal covers)
    book = make_book()
    book.mark_stale("r1")
    f = _live_book_frame(ts="1788649334527")
    del f["hash"]
    applied, reason = book.apply_ws_message(f)
    assert applied is True
    assert book.book_state == BookState.stale
    assert reason is not None and "book_hash_missing_on_promotion" in reason
    # content was still applied — no data loss, only trust refused
    assert book.up.bids.best_price() == 0.50


def test_a4_malformed_hash_refused():
    book = make_book()
    book.mark_stale("r1")
    f = _live_book_frame(ts="1788649334527")
    f["hash"] = "x"  # too short to be an attestation
    applied, reason = book.apply_ws_message(f)
    assert applied is True
    assert book.book_state == BookState.stale
    assert reason is not None and "book_hash_missing_on_promotion" in reason


def test_a4_price_change_entry_hash_captured():
    book = make_book()
    pc = {"event_type": "price_change", "market": "0xm", "timestamp": "1788649335000",
          "price_changes": [{"asset_id": "up-123", "price": "0.51", "size": "5",
                             "side": "BUY", "hash": "entryhash001",
                             "best_bid": "0.51", "best_ask": "0.52"}]}
    applied, _ = book.apply_ws_message(pc)
    assert applied is True
    assert book.book_hash["up"] == "entryhash001"
