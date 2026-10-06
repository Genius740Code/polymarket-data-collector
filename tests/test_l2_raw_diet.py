"""l2_raw dual-leg diet: redelivery widens the conn bitmap, appends no new row.

Real-data-only: every UNIQUE frame is still stored exactly once with a
verbatim frame_json (no threshold, no sampling, no dedup-key change); the
second leg's redelivery only widens the buffered first-seen row's
source_conn ("A" -> "A|B"). Readers split the bitmap on "|"; export passes
it through verbatim. All entry points never raise on malformed frames.
"""
import copy
import glob
import json
import os
import tempfile

import pyarrow.parquet as pq

from polymarket_collector.storage.l2_raw import (
    append_row,
    build_l2_raw_row,
    dedup_key_for_row,
    widen_source_conn,
)
from polymarket_collector.storage.parquet_writer import ParquetWriter

CID = "0x" + "ab" * 32
TOK_A = "1" * 64
TOK_B = "2" * 64
TS = 1759300000000


def _keyed_frame(tok=TOK_A, ts=TS, price="0.55"):
    return {
        "event_type": "price_change",
        "asset_id": tok,
        "condition_id": CID,
        "timestamp": ts,
        "price_changes": [{"asset_id": tok, "price": price, "size": "10"}],
    }


def _l2_rows(w):
    out = [br.row for br in list(w._l2_buffer) if br.dataset == "l2_raw"]
    out += [br.row for br in list(w._buffer) if br.dataset == "l2_raw"]
    return out


def _writer(**kw):
    kw.setdefault("wal_enabled", False)
    return ParquetWriter(data_dir=tempfile.mkdtemp(), **kw)


# -- bitmap pure helper --------------------------------------------------------

def test_widen_bitmap_values_verbatim():
    assert widen_source_conn("A", "B") == "A|B"
    assert widen_source_conn("B", "A") == "A|B"  # arrival-order independent
    assert widen_source_conn("A", "A") == "A"  # same-leg resend: no-op
    assert widen_source_conn("B", "B") == "B"
    assert widen_source_conn("A|B", "A") == "A|B"  # idempotent
    assert widen_source_conn("A|B", "B") == "A|B"
    assert widen_source_conn("single", "A") == "single"  # no pair: verbatim
    assert widen_source_conn("single", "B") == "single"
    assert widen_source_conn(None, "A") == "A"  # untagged history gains witness
    assert widen_source_conn(None, "B") == "B"
    # Unknown/empty legs never fabricate.
    assert widen_source_conn("A", None) == "A"
    assert widen_source_conn("A", "") == "A"
    assert widen_source_conn("A", "C") == "A"
    assert widen_source_conn("A", "single") == "A"
    assert widen_source_conn("A", 123) == "A"
    assert widen_source_conn(None, None) is None


def test_widen_never_raises_on_malformed():
    for existing in (None, "", 123, 4.5, [], {}, {"x": 1}, "A", "single"):
        for leg in (None, "", "A", "B", "C", 123, [], {}, "A|B"):
            widen_source_conn(existing, leg)  # must not raise
    assert widen_source_conn([], "B") == []
    assert widen_source_conn(123, "B") == 123


# -- transport path: redelivery widens without a new row ----------------------

def test_redelivery_widens_bitmap_without_new_row():
    w = _writer()
    frame = _keyed_frame()
    assert append_row(w, frame, asset="BTC", source_conn="A",
                      ts_received_ns=111) is True
    assert len(_l2_rows(w)) == 1
    # Second leg's redelivery: proven duplicate, appends nothing new.
    assert w.note_l2_raw_redelivery(frame, "B") is True
    rows = _l2_rows(w)
    assert len(rows) == 1
    assert rows[0]["source_conn"] == "A|B"
    # Verbatim fidelity: the frame bytes are untouched by the widen.
    assert json.loads(rows[0]["frame_json"]) == frame
    # Re-widening is a no-op (already carries B).
    assert w.note_l2_raw_redelivery(frame, "B") is False
    assert w.note_l2_raw_redelivery(frame, "A") is False
    assert len(_l2_rows(w)) == 1 and _l2_rows(w)[0]["source_conn"] == "A|B"


def test_widened_tag_survives_flush_verbatim():
    tmp = tempfile.mkdtemp()
    w = ParquetWriter(data_dir=tmp, wal_enabled=False)
    frame = _keyed_frame()
    append_row(w, frame, asset="BTC", source_conn="A", ts_received_ns=111)
    assert w.note_l2_raw_redelivery(frame, "B") is True
    w.flush()
    assert _l2_rows(w) == []  # drained
    assert w._l2_conn_index == {}  # flushed rows leave the widen index
    parts = glob.glob(os.path.join(tmp, "l2_raw", "**", "*.parquet"), recursive=True)
    assert parts, "expected a flushed l2_raw file"
    stored = pq.read_table(parts[0]).to_pylist()
    assert len(stored) == 1
    assert stored[0]["source_conn"] == "A|B"
    assert json.loads(stored[0]["frame_json"]) == frame
    # A redelivery after the flush misses (row is on disk, never mutated).
    assert w.note_l2_raw_redelivery(frame, "B") is False


# -- writer path: exact-dupe append widens instead of silently dropping -------

def test_writer_dupe_append_widens_bitmap():
    w = _writer()
    frame = _keyed_frame()
    assert append_row(w, frame, asset="BTC", source_conn="A",
                      ts_received_ns=111) is True
    # Same frame via writer-level dedup (keyless-frame safety net): still one
    # row, but the B witness is kept instead of dropped silently.
    assert append_row(w, frame, asset="BTC", source_conn="B",
                      ts_received_ns=222) is True
    rows = _l2_rows(w)
    assert len(rows) == 1
    assert rows[0]["source_conn"] == "A|B"


def test_single_tag_never_widens_through_writer_dupe():
    w = _writer()
    frame = _keyed_frame()
    assert append_row(w, frame, asset="BTC", source_conn="single") is True
    assert append_row(w, frame, asset="BTC", source_conn="single") is True
    rows = _l2_rows(w)
    assert len(rows) == 1
    assert rows[0]["source_conn"] == "single"


# -- every unique frame still stored exactly once ------------------------------

def test_distinct_frames_all_stored_unique_count_exact():
    w = _writer()
    winners = ["A", "B"]
    n_unique = 40
    for i in range(n_unique):
        f = _keyed_frame(tok=TOK_A if i % 2 else TOK_B, ts=TS + i,
                         price=f"0.{50 + (i % 49)}")
        leg = winners[i % 2]
        other = "B" if leg == "A" else "A"
        assert append_row(w, f, asset="BTC", source_conn=leg) is True
        # Losing leg redelivers every frame: bitmap only, never a new row.
        assert w.note_l2_raw_redelivery(f, other) is True
    rows = _l2_rows(w)
    assert len(rows) == n_unique  # exact vs input: no loss, no dupes
    assert sum(1 for r in rows if r["source_conn"] == "A|B") == n_unique
    # No dedup-key change: keys still cover (token, ts, type, frame-hash).
    keys = [dedup_key_for_row(r) for r in rows]
    assert None not in keys and len(set(keys)) == n_unique


def test_keyless_frames_never_false_dupe():
    w = _writer()
    # No token and no timestamp: distinct bytes must all be stored ...
    frames = [
        {"event_type": "book", "bids": [{"price": f"0.{i}", "size": "1"}]}
        for i in range(1, 11)
    ]
    for f in frames:
        assert append_row(w, f, asset="ETH", source_conn="A") is True
    assert len(_l2_rows(w)) == len(frames)
    # ... while an exact keyless redelivery still widens instead of doubling.
    assert w.note_l2_raw_redelivery(frames[0], "B") is True
    rows = _l2_rows(w)
    assert len(rows) == len(frames)
    by_loaded = {json.dumps(json.loads(r["frame_json"]), sort_keys=True): r for r in rows}
    assert by_loaded[json.dumps(frames[0], sort_keys=True)]["source_conn"] == "A|B"
    for f in frames[1:]:
        assert by_loaded[json.dumps(f, sort_keys=True)]["source_conn"] == "A"


def test_mixed_workload_unique_count_exact_vs_input():
    w = _writer()
    deliveries = []
    unique_jsons = set()
    for i in range(30):
        f = _keyed_frame(tok=TOK_A, ts=TS + i, price=f"0.{10 + i}")
        deliveries.append((f, "A", "B"))
        unique_jsons.add(json.dumps(f, sort_keys=True))
    for i in range(30, 35):  # same ts, different token => distinct frames
        f = _keyed_frame(tok=TOK_B, ts=TS, price=f"0.{10 + i}")
        deliveries.append((f, "B", "A"))
        unique_jsons.add(json.dumps(f, sort_keys=True))
    for (f, first, second) in deliveries:
        append_row(w, copy.deepcopy(f), asset="BTC", source_conn=first)
        assert w.note_l2_raw_redelivery(f, second) is True
    rows = _l2_rows(w)
    assert len(rows) == len(unique_jsons) == 35
    stored = {json.dumps(json.loads(r["frame_json"]), sort_keys=True) for r in rows}
    assert stored == unique_jsons
    assert all(r["source_conn"] == "A|B" for r in rows)


# -- never-raise on malformed frames -------------------------------------------

def test_note_redelivery_never_raises_malformed():
    w = _writer()
    frame = _keyed_frame()
    append_row(w, frame, asset="BTC", source_conn="A")
    before = len(_l2_rows(w))
    bad_msgs = [None, "", "frame", 123, 4.5, [], [frame], {"unserializable": object()}]
    bad_legs = [None, "", "C", "single", "A|B", 123, [], {}]
    for m in bad_msgs:
        for leg in ["A", "B"] + bad_legs:
            assert w.note_l2_raw_redelivery(m, leg) is False
    for leg in bad_legs:
        assert w.note_l2_raw_redelivery(frame, leg) is False
    # A malformed storm widens nothing and stores nothing new.
    assert len(_l2_rows(w)) == before
    assert _l2_rows(w)[0]["source_conn"] == "A"
    # build_l2_raw_row input contract still holds (non-dict raises TypeError).
    try:
        build_l2_raw_row("nope", asset="BTC")
    except TypeError:
        pass
    else:
        raise AssertionError("non-dict frame must raise TypeError")


def test_widen_missing_row_is_noop():
    w = _writer()
    assert w.note_l2_raw_redelivery(_keyed_frame(), "B") is False  # never buffered
    assert w._widen_l2_conn(("l2_raw", "nope"), "B") is False
    assert _l2_rows(w) == []
