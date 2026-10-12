"""TRADES streaming parity fix (2026-10-12) — zero-accumulation + compact keys.

Prod symptom: every trades staging worker rss-cap-aborted at 1000MB
(BTC 05:50, ETH 06:02, SOL 06:17 on 2026-10-12; same on 10-11 19:53-20:41),
stretching the pre-upload phase of the lane export past collector
incarnation lifetimes — zero `✓ Upload success / ✗ Upload failed` lines
since 2026-10-11 15:57 while snapshots staged fine. Measured on the real
BTC hive (137 files / 1.72M rows, frozen cutoff): HEAD cap-aborted at
1001MB; the pre-pass alone held 732MB (tuple have-set ~320MB + whole-file
read_table+to_pylist spike 672MB on the first ~500k-row compacted file);
the group pass then concats each 4MB group full-width and runs a
whole-group to_pylist inside _backfill_trade_wallets (~10x RAM).

The fix mirrors the snapshot-path patterns (718729d/5dcb1e2/9450d7e):
per-file iter_batches chunks (coalesce_rows ceiling handed to the writer,
zero accumulation), 16-byte digests for the two global per-row key stores
(_DigestCounter/_DigestSet, ~16B/key vs ~121-300B/key), per-chunk trim,
write-back accumulation gated on ALLOW_HIVE_WRITEBACK, raw (<=0 = expired)
Data-API budget — the falsy max(0.0,…) used to turn an expired budget into
None = unbounded fetching — and a memory-bounded leg-pool cache
(_BoundedPoolCache) whose honest-NULLs-then-next-pass degradation is the
same as the time deadline's. Benchmark: 1001MB cap-abort -> 439MB peak,
full row/content parity vs HEAD on a frozen real-file subset (1,581,559
rows, 33 columns, zero mismatches). One lane's worker abort now records a
worker_failed manifest marker and never blocks the other lanes' upload
(fail closed per lane; coverage mismatches still abort — fail-closed stays).
"""

import collections
import inspect
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage import export as ex
from polymarket_collector.storage.schemas import SCHEMAS

_TS = SCHEMAS["trades"]


# ---------------------------------------------------------------- digest containers

def _real_fill_keys(asset="BTC", limit=25, cap_rows=40000):
    """Real (tx, price, size) fill keys from the hive (skip when absent)."""
    base = Path(__file__).resolve().parents[1] / "data" / "trades"
    if not base.is_dir():
        pytest.skip("real trades hive absent")
    files = sorted((p for p in base.rglob("*.parquet")
                    if not p.name.endswith(".tmp") and f"asset={asset}" in str(p)),
                   key=lambda p: p.stat().st_size, reverse=True)[:limit]
    keys = []
    for p in files:
        try:
            sch = pq.read_schema(str(p))
            cols = [c for c in ("transaction_hash", "price", "size") if c in sch.names]
            t = pq.read_table(str(p), columns=cols)
        except Exception:
            continue
        if t.num_rows == 0 or len(cols) < 3:
            continue
        for tx, pr, sz in zip(t.column("transaction_hash").to_pylist(),
                              t.column("price").to_pylist(),
                              t.column("size").to_pylist()):
            if tx and pr is not None and sz is not None:
                keys.append((tx, pr, sz))
        if len(keys) >= cap_rows:
            break
    if len(keys) < 1000:
        pytest.skip("too few real fill keys")
    return keys[:cap_rows]


def test_digest_set_membership_matches_plain_set_on_real_keys() -> None:
    keys = _real_fill_keys()
    digests = [ex._trades_fill_key_digest(*k) for k in keys]
    plain = set(digests)
    ds = ex._DigestSet(capacity=1 << 8)  # tiny start: many growth rehashes
    for d in digests:
        ds.add(d)
    assert len(ds) == len(plain)
    assert all((d in ds) == (d in plain) for d in digests)
    # negatives: shuffled bytes are not present in either
    rng = random.Random(11)
    absent = [bytes(rng.getrandbits(8) for _ in range(16)) for _ in range(500)]
    assert all((d in ds) == (d in plain) for d in absent)
    # duplicates: add returns newness exactly once
    ds2 = ex._DigestSet()
    assert ds2.add(digests[0]) is True
    assert ds2.add(digests[0]) is False


def test_digest_counter_take_matches_counter_consume() -> None:
    """bump/take must reproduce Counter get-then-decrement exactly, across
    capacity growth and the >255 overflow spill."""
    keys = _real_fill_keys()
    digests = [ex._trades_fill_key_digest(*k) for k in keys]
    rng = random.Random(5)
    for cap in (1 << 8, 1 << 16):
        dc = ex._DigestCounter(capacity=cap)
        ref = collections.Counter()
        for d in digests:
            dc.bump(d)
            ref[d] += 1
            if rng.random() < 0.25:  # extra duplicates exercise the counts
                dc.bump(d)
                ref[d] += 1
        order = list(digests)
        rng.shuffle(order)
        for d in order + order[: len(order) // 2]:  # over-take past exhaustion
            got = dc.take(d)
            want = ref.get(d, 0) > 0
            if want:
                ref[d] -= 1
            assert got == want
        assert all(dc.take(d) is False for d in order[:50])  # exhausted/absent
    # overflow spill: 300 bumps of one key stays exact (255 in the byte, rest in _over)
    dc3 = ex._DigestCounter(capacity=1 << 8)
    big = digests[0]
    for _ in range(300):
        dc3.bump(big)
    taken = 0
    while dc3.take(big):
        taken += 1
    assert taken == 300


def test_fill_key_digest_equality_matches_legacy_tuple_equality() -> None:
    keys = _real_fill_keys(cap_rows=3000)
    # legacy Counter key construction, verbatim from the pre-fix pre-pass
    legacy = [((tx or "").lower(), round(float(pr), 6), round(float(sz), 6))
              for tx, pr, sz in keys]
    digs = [ex._trades_fill_key_digest(tx, pr, sz) for tx, pr, sz in keys]
    for i in range(0, len(legacy), 3):
        for j in range(i + 3, min(i + 60, len(legacy)), 3):
            assert (digs[i] == digs[j]) == (legacy[i] == legacy[j])
    # malformed price raises like the legacy round(float(None)) — callers skip the row
    with pytest.raises(TypeError):
        ex._trades_fill_key_digest("0xabc", None, 1.0)


# ---------------------------------------------------------------- reconcile plumbing

def _ctx(cid, series="BTC-5m", fee=None):
    return {"fee_rate": fee, "series_mode": series,
            "first": {"market_id": "7", "asset": "BTC", "window_index": 3},
            "oldest_needed_ms": 1790000000000}


def test_reconcile_global_digest_have_consumes_local_and_inserts_missing(monkeypatch) -> None:
    """The api- reconcile must consume local fills via the digest have-set
    (never re-inserting a local fill) and insert each truly missing fill once."""
    fetched = {
        "cidA": [
            {"transactionHash": "0x" + "aa" * 32, "price": 0.5, "size": 2.0,
             "side": "buy", "proxyWallet": "0xw1", "timestamp": 1790000001000},
            {"transactionHash": "0x" + "bb" * 32, "price": 0.6, "size": 3.0,
             "side": "sell", "proxyWallet": "0xw2", "timestamp": 1790000002000},
        ],
        "cidB": [
            {"transactionHash": "0x" + "cc" * 32, "price": 0.7, "size": 4.0,
             "side": "buy", "proxyWallet": "0xw3", "timestamp": 1790000003000},
        ],
    }
    monkeypatch.setattr(ex, "_fetch_market_trades",
                        lambda cid, taker_only=True, oldest_needed_ms=None, max_pages=60:
                        list(fetched.get(cid, [])))
    have = ex._DigestCounter()
    # cidA's first fill is already local (the reconcile must consume it, not insert)
    have.bump(ex._trades_fill_key_digest("0x" + "aa" * 32, 0.5, 2.0))
    ctx_by_cid = {"cidA": _ctx("cidA"), "cidB": _ctx("cidB", series="BTC-5m")}
    outs = list(ex._reconcile_trades_global(["cidA", "cidB"], ctx_by_cid, have,
                                            {}, {}, "BTC", deadline_s=None))
    rows = [r for t in outs for r in t.to_pylist()]
    assert len(rows) == 2  # cidA missing fill + cidB fill (local one consumed)
    assert all(str(r["trade_id"]).startswith("api-") for r in rows)
    assert all(r["source"] == "api_reconciled" for r in rows)
    txs = {r["transaction_hash"] for r in rows}
    assert "0x" + "bb" * 32 in txs and "0x" + "cc" * 32 in txs
    assert "0x" + "aa" * 32 not in txs  # consumed by the digest have-set
    # expired budget (<= 0) inserts nothing — honest gap, healed next pass
    have2 = ex._DigestCounter()
    have2.bump(ex._trades_fill_key_digest("0x" + "aa" * 32, 0.5, 2.0))
    outs2 = list(ex._reconcile_trades_global(["cidA"], {"cidA": _ctx("cidA")}, have2,
                                             {}, {}, "BTC", deadline_s=-1.0))
    assert outs2 == []


def test_chunk_enrichment_deadline_zero_is_expired_not_unbounded(monkeypatch) -> None:
    """The falsy `if deadline_s` used to turn a 0.0 remaining budget into
    None = unbounded data-api time; 0.0 must now mean already-expired."""
    calls: list = []

    def _no_fetch(*a, **k):
        calls.append(a[0])
        return []

    monkeypatch.setattr(ex, "_fetch_market_trades", _no_fetch)
    t = pa.Table.from_pylist([{
        "condition_id": "0x" + "11" * 32, "series_id": "BTC-5m", "asset": "BTC",
        "transaction_hash": "0x" + "22" * 32, "trade_id": "t1", "token_id": "tok1",
        "price": 0.5, "size": 2.0, "wallet": None, "maker_wallet": None,
        "outcome": "up", "side": "buy", "ts_source": 1790000000000,
    }], schema=_TS)
    out = ex._backfill_trade_wallets_chunked(t, Path("."), asset="BTC",
                                             reconcile=False, deadline_s=0.0)
    assert out.num_rows == 1
    assert out.column("wallet").to_pylist() == [None]  # honest NULL, no fetch
    assert calls == []  # zero budget: nothing fetched (was: unbounded pre-fix)


# ---------------------------------------------------------------- chunk-stream build

def _trd(i, ts, cid, token, trade, tx=None, wallet=None, series="BTC-5m",
         market_id=None, window=5, side="BUY", price=0.5, size=2.0,
         notional=1.0, outcome="up"):
    return {
        "ts_source": ts, "condition_id": cid, "series_id": series, "asset": "BTC",
        "trade_id": trade, "token_id": token, "transaction_hash": tx,
        "wallet": wallet, "maker_wallet": wallet, "taker_wallet": wallet,
        "outcome": outcome, "price": price, "size": size, "notional": notional,
        "market_id": market_id, "window_index": window, "side": side,
        "aggressor_side": side, "fee": None, "fee_is_estimated": None,
    }


CID = "0x" + "ab" * 32
HEX_MID = "0x" + CID[2:]  # hex market_id == condition_id -> must heal to 42/NULL


def _mini_hive(tmp_path):
    base = tmp_path / "data"
    d1 = base / "trades" / "date=2026-10-10" / "asset=BTC"
    d1.mkdir(parents=True)
    rows1 = [
        # complete row + hex market_id (E1 heal) + uppercase side (E5)
        _trd(1, 1790000001000, CID, "tokA", "tA", tx="0x" + "11" * 32,
             wallet="0xw", market_id=HEX_MID, side="BUY"),
        # exact duplicate (token_id, trade_id) across rows -> first wins
        _trd(1, 1790000001500, CID, "tokA", "tA", tx="0x" + "11" * 32,
             wallet="0xw", market_id=HEX_MID, side="buy"),
        # NULL series_id resolvable via markets_latest (N7 backfill)
        _trd(2, 1790000002000, CID, "tokB", "tB", tx="0x" + "22" * 32,
             series=None),
        # NULL window_index stays (M4 honest gap, counted loudly)
        _trd(3, 1790000003000, CID, "tokC", "tC", window=None),
    ]
    pq.write_table(pa.Table.from_pylist(rows1, schema=_TS), str(d1 / "f1.parquet"))
    d2 = base / "trades" / "date=2026-10-11" / "asset=BTC"
    d2.mkdir(parents=True)
    rows2 = [
        # missing notional (mechanical fill), NULL market_id with known cid (E1)
        _trd(4, 1790086001000, CID, "tokD", "tD", notional=None,
             market_id=None, side="SELL"),
        # unresolvable NULL series_id: kept as honest NULL (N7 null-keep)
        _trd(5, 1790086002000, "0x" + "99" * 32, "tokE", "tE", series=None),
    ]
    t2 = pa.Table.from_pylist(rows2, schema=_TS)
    t2 = t2.append_column("extra_v1", pa.array(["x"] * t2.num_rows, type=pa.string()))
    pq.write_table(t2, str(d2 / "f2.parquet"))
    # deterministic oldest-first order within the same date stamps
    import os
    os.utime(d1 / "f1.parquet", (1790000100, 1790000100))
    os.utime(d2 / "f2.parquet", (1790086100, 1790086100))
    ml = base / "markets_latest"
    ml.mkdir(parents=True)
    pq.write_table(pa.table({"condition_id": [CID],
                             "series_id": ["BTC-5m"],
                             "market_id": ["42"]}),
                   str(ml / "markets_latest.parquet"))
    return base


def test_chunk_stream_rows_match_expected_transform(monkeypatch, tmp_path) -> None:
    base = _mini_hive(tmp_path)
    monkeypatch.setattr(ex, "_fetch_market_trades", lambda *a, **k: [])
    io: dict = {}
    tmp = tmp_path / "BTC_trades.parquet.tmp"
    n = ex._stream_export_trades_dataset(base, "BTC", tmp, "5m",
                                         deadline_s=60, io_stats=io)
    assert tmp.exists()
    got = pq.read_table(str(tmp))
    assert got.num_rows == n == 5  # 6 rows - 1 duplicate
    rows = {(r["token_id"], r["trade_id"]): r for r in got.to_pylist()}
    # first-wins dedup: the duplicate kept the FIRST occurrence (earlier ts)
    assert rows[("tokA", "tA")]["ts_source"] == 1790000001000
    # E5: staging never ships mixed case
    assert rows[("tokA", "tA")]["side"] == "buy"
    assert rows[("tokD", "tD")]["side"] == "sell"
    # E1 heal: hex market_id -> numeric 42 (cid in markets_latest)
    assert rows[("tokA", "tA")]["market_id"] == "42"
    assert rows[("tokD", "tD")]["market_id"] == "42"
    # mechanical notional fill — NOTE: the kernel branch is a PRE-EXISTING
    # dead path (pc.case_when takes a struct in this pyarrow, the 3-arg form
    # raises and the bare except swallows it; HEAD behaves identically — both
    # paths ship notional NULL here). Kept verbatim for exact HEAD parity;
    # the dead kernel is reported for a deliberate follow-up fix.
    assert rows[("tokD", "tD")]["notional"] is None
    # N7: NULL series backfilled for the known cid, kept NULL for the unknown one
    assert rows[("tokB", "tB")]["series_id"] == "BTC-5m"
    assert rows[("tokE", "tE")]["series_id"] is None
    # M4: NULL window_index row stays in staging (honest gap)
    assert rows[("tokC", "tC")]["window_index"] is None
    # schema union: the second file's evolved column is present, null for old rows
    assert "extra_v1" in got.schema.names
    assert rows[("tokE", "tE")]["extra_v1"] == "x"
    assert rows[("tokA", "tA")]["extra_v1"] is None
    # honest single-pass accounting: every file counted once, none failed
    assert io.get("files_ok") == 2 and io.get("files_failed") == 0


def test_writeback_accumulator_off_by_default(monkeypatch, tmp_path) -> None:
    """The 7-column accumulator for ALL window rows was pure RAM waste when
    the write-back cannot run (H4 default off) — it must not even build."""
    base = _mini_hive(tmp_path)
    monkeypatch.setattr(ex, "_fetch_market_trades", lambda *a, **k: [])
    calls: list = []
    monkeypatch.setattr(ex, "_writeback_enriched_trades",
                        lambda *a, **k: calls.append(a) or 0)
    monkeypatch.delenv("ALLOW_HIVE_WRITEBACK", raising=False)
    tmp = tmp_path / "BTC_trades.parquet.tmp"
    ex._stream_export_trades_dataset(base, "BTC", tmp, "5m", deadline_s=60)
    assert calls == []  # accumulator skipped: write-back disabled by default
    monkeypatch.setenv("ALLOW_HIVE_WRITEBACK", "1")
    ex._stream_export_trades_dataset(base, "BTC", tmp, "5m", deadline_s=60)
    assert len(calls) == 1  # enabled -> accumulation resumes, write-back runs


# ---------------------------------------------------------------- pool cache bound

def test_pool_cache_bounded_refuses_without_evicting() -> None:
    """Memory budget past max legs stores EMPTY pools (honest NULLs, healed
    next pass) — cached markets are never evicted and never change mid-build."""
    def pools(n):
        return ({("k", i): [f"w{i}"] for i in range(n)}, {}, {})
    c = ex._BoundedPoolCache(max_legs=10)
    c["m1"] = pools(5)
    c["m2"] = pools(5)
    assert c.legs == 10 and len(c) == 2 and c.refused == 0
    c["m3"] = pools(5)  # over budget -> refused, empty pools cached
    assert c.refused == 1 and c.legs == 10
    assert c["m3"] == ({}, {}, {})
    assert len(c["m1"][0]) == 5  # earlier caches intact (no eviction)
    c["m3"] = pools(5)  # re-store still refused, no growth
    assert c.refused == 2 and c.legs == 10
    assert ex._TRADES_POOL_CACHE_MAX_LEGS >= 100_000  # sanity vs fat-finger


# ---------------------------------------------------------------- source-shape guards

def test_stream_guards_wired() -> None:
    src = inspect.getsource(ex._stream_export_trades_dataset)
    # snapshot-path patterns mirrored, not reinvented:
    assert "coalesce_rows: int = 20000" in src          # coalesce ceiling param
    assert "row_group_rows=coalesce_rows" in src        # ceiling -> writer
    assert "iter_batches(batch_size=coalesce_rows" in src  # zero-accumulation decode
    assert "_malloc_trim()" in src                      # per-chunk/per-file trim
    assert "use_threads=False" in src                  # no arena-fragmenting decode
    # compact keys (digests), NOT the snapshot class (pinned by the other suites):
    assert "_DigestSet()" in src and "_DigestCounter()" in src
    assert "_CompactDedupState" not in src and "compact_dedup" not in src
    # memory-bounded leg-pool cache (symmetric with the time deadline):
    assert "_BoundedPoolCache()" in src
    # expired budget stays expired (the falsy max(0.0,…) bug):
    assert "if _deadline is not None else None" in src
    assert "max(0.0" not in src
    # write-back accumulation gated on the env:
    assert "ALLOW_HIVE_WRITEBACK" in src and "_wb_enabled" in src
    # phase lines + fail-closed tokens (worker-log monitors parse them):
    assert "_phase_timed_iter(_gen()" in src
    assert "phase {asset_upper}/trades scan:" in src
    assert "fail closed" in src
    assert "1 batches dropped (fail closed)" in src


# ---------------------------------------------------------------- upload independence

def test_worker_abort_marks_lane_fail_closed_in_manifests(monkeypatch, tmp_path) -> None:
    """A worker abort must be VISIBLE to the upload gate (worker_failed
    marker), not silently absent — and must keep prior staging (fail closed
    per lane)."""
    monkeypatch.setattr(ex, "_build_in_subprocess",
                        lambda *a, **k: {"error": "rss-cap-abort"})
    monkeypatch.setattr(ex, "_avail_mb", lambda: 99999)
    base = tmp_path / "data"
    (base / "trades" / "date=2026-10-10" / "asset=BTC").mkdir(parents=True)
    out = tmp_path / "staging"
    out.mkdir()
    mani: dict = {}
    stats = ex.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=["trades"], manifests=mani)
    m = mani.get("trades/BTC")
    assert m is not None, "worker aborts must be recorded in the manifests"
    assert m.get("worker_failed") is True and m.get("ok") is False
    assert m.get("pre") is not None  # coverage evidence kept for the report
    assert stats and next(iter(stats.values())) == 0  # prior staging kept (none here)


def _prep_result(staging, ds_prefix, manifests):
    return {"staging_path": str(staging), "files": 39, "row_counts": {},
            "dataset": ds_prefix, "manifests": manifests}


def test_upload_proceeds_when_one_lane_worker_aborted(monkeypatch, tmp_path, capsys) -> None:
    """Per-lane upload independence: a trades worker abort keeps that lane's
    prior staging (fail closed per lane) and the OTHER lanes' fresh staging
    still uploads — the upload is never silently starved again."""
    base = tmp_path / "data"
    base.mkdir(parents=True)
    staging = base / "kaggle_staging" / "5m" / "gghgg1/polymarket-5m-crypto"
    staging.mkdir(parents=True)
    # snapshots staged fine this cycle (a real 1-row file: the pre-upload
    # gate requires non-empty snapshots staging when the hive holds files)
    from polymarket_collector.storage.schemas import snapshot_schema
    _snap_row = {
        "ts_snapshot_ns": 1790000000000000000, "condition_id": CID,
        "market_id": "42", "series_id": "BTC-5m", "asset": "BTC",
        "window_index": 1, "snapshot_id": "s1", "up_token_id": "u",
        "down_token_id": "d", "up_bid": 0.5, "up_ask": 0.6,
        "up_bid_size": 1.0, "up_ask_size": 1.0, "down_bid": 0.4,
        "down_ask": 0.5, "down_bid_size": 1.0, "down_ask_size": 1.0,
        "book_state": "live",
    }
    _ss = pa.schema([pa.field(f.name, f.type, nullable=True) for f in snapshot_schema(10)])
    pq.write_table(pa.Table.from_pylist([_snap_row], schema=_ss),
                   str(staging / "BTC_book_snapshots_500ms.parquet"))
    uploads: list = []

    def _prep(data_dir, staging_dir=None, assets=None, l2_levels=10,
              dataset_prefix="gghgg1/polymarket-5m-crypto", timeframe_label="5m",
              rolling_window=False, cutoff_ts=None, manifests=None, datasets=None):
        if manifests is not None:
            manifests["trades/BTC"] = {"pre": {"n": 3, "bytes": 1, "digest": "x"},
                                       "worker": None, "ok": False,
                                       "read_errors": {}, "worker_failed": True}
            manifests["book_snapshots_500ms/BTC"] = {
                "pre": {"n": 5, "bytes": 2, "digest": "y"}, "worker":
                {"n": 5, "bytes": 2, "digest": "y"}, "ok": True, "read_errors": {}}
        return _prep_result(staging, dataset_prefix, manifests)

    monkeypatch.setattr(ex, "prepare_kaggle_staging_5m", _prep)
    monkeypatch.setattr(ex, "_upload_kaggle_folder",
                        lambda folder, dataset, **k: uploads.append((folder, dataset)) or True)
    cleaned: list = []
    monkeypatch.setattr(ex, "cleanup_local_data",
                        lambda *a, **k: cleaned.append(a) or {})
    res = ex._export_and_upload_all_kaggle_impl(str(base), timeframe_labels=["5m"])
    assert len(uploads) == 1, "one lane's abort must not block the upload"
    assert res["kaggle_uploads"]["gghgg1/polymarket-5m-crypto"]["status"] == "success"
    logged = capsys.readouterr().out
    assert "kept prior staging after worker abort" in logged
    assert "aborting upload" not in logged
    assert cleaned and cleaned[0][0] == str(base), "verified upload still runs the prune"


def test_upload_still_fails_closed_on_coverage_mismatch(monkeypatch, tmp_path, capsys) -> None:
    """A REAL coverage failure (worker ran, saw different inputs) still aborts
    the whole upload — the per-lane marker only covers worker ABORTS."""
    base = tmp_path / "data"
    base.mkdir(parents=True)
    staging = base / "kaggle_staging" / "5m" / "gghgg1/polymarket-5m-crypto"
    staging.mkdir(parents=True)
    uploads: list = []

    def _prep(data_dir, staging_dir=None, assets=None, l2_levels=10,
              dataset_prefix="gghgg1/polymarket-5m-crypto", timeframe_label="5m",
              rolling_window=False, cutoff_ts=None, manifests=None, datasets=None):
        if manifests is not None:
            manifests["trades/BTC"] = {"pre": {"n": 3, "bytes": 1, "digest": "x"},
                                       "worker": {"n": 2, "bytes": 1, "digest": "z"},
                                       "ok": False, "read_errors": {}}
        return _prep_result(staging, dataset_prefix, manifests)

    monkeypatch.setattr(ex, "prepare_kaggle_staging_5m", _prep)
    monkeypatch.setattr(ex, "_upload_kaggle_folder",
                        lambda folder, dataset, **k: uploads.append((folder, dataset)) or True)
    monkeypatch.setattr(ex, "cleanup_local_data", lambda *a, **k: {})
    res = ex._export_and_upload_all_kaggle_impl(str(base), timeframe_labels=["5m"])
    assert uploads == [], "coverage mismatches must abort the upload (fail closed)"
    assert res["kaggle_uploads"]["gghgg1/polymarket-5m-crypto"]["status"] == "failed"
    logged = capsys.readouterr().out
    assert "aborting upload" in logged
    assert "worker inputs != hive at build start" in logged
