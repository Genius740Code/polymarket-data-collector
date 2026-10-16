"""Staging-FINALIZE RSS regression (2026-10-16): markets_summary fold projection.

The 23:21 Oct-15 pm2 max-memory kill (2.68GB vs 2.2GB cap, parent PID
1307984) peaked in the post-last-worker finalize. Measured on the real 5m
staging (fresh process per phase, /proc/self/statm sampling):

- OHLC fold over {ASSET}_book_snapshots_clean.parquet (export.py:2605 old
  numbering): iter_batches(batch_size=20000) with NO columns= decompressed
  ALL 842 L2 columns of every row group — the fold needs 6. Peak +748MB
  (3.45M clean rows, 7 files), t=121s. THE spike: parent live state
  (~1.1GB) + kaggle SDK (~0.5GB) + this fold + retained trades concat
  crossed the cap mid-prune after a verified upload.
- trades-for-summary concat + group_by (export.py:2361/2493): +350MB, and
  the names (trades/t/cols/agg/valid_w/dist) stayed REFERENCED through the
  fold, chainlink and final-table phases — stacking that peak on the
  fold's instead of releasing at the phase boundary.

Fix (b7b86fd bounded-scanner mirror): columns=_FOLD_NEED projection in
both fold loops (staging + hive fallback; the fallback used
stream_batches which reads whole 842-col rows too), per-ingredient del +
gc at the trades phase boundary. The trades concat + ONE group_by stays
byte-for-byte the same code — per-file aggregation + merge measured
last-ULP notional drift on 1972/20434 real cids (float sum is
order-sensitive), so parity wins over the 350MB (new finalize peak
+391-419MB whole-function, was +1091-1108MB; t=64s was 176s).

Full-summary parity on the real 5m staging: rows 7384=7384, cols 33=33,
schema equal; all deterministic fields equal; traded_volume ULPs within
the OLD code's own run-to-run envelope (pyarrow group_by float sum is
schedule-nondeterministic: old1-vs-old2=2 diffs, old1-vs-new=2,
old2-vs-new=2 on three fresh runs).

These tests pin: the projection, the del/gc boundary, the unchanged
aggregation contract, wide-file fold parity, missing-column skip
semantics, staging-vs-hive-fallback fold parity, and real-staging
rows×cols — the same fail-closed/monotonic/retention guards are
untouched (asserted by source pins).
"""

import datetime as _dt
import inspect
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage import export as ex
from polymarket_collector.storage.schemas import MARKETS_SUMMARY_SCHEMA

DAY = "2026-10-16"
DAY_START_MS = int(_dt.datetime.fromisoformat(f"{DAY}T00:00:00+00:00").timestamp() * 1000)

CID_BTC = "0x" + "b1" * 32
CID_ETH = "0x" + "e2" * 32
CID_ETH_OTHER = "0x" + "e3" * 32  # no clean rows -> all-None OHLC

FOLD_NEED = ["condition_id", "ts_snapshot_ns", "up_bid", "up_ask", "down_bid", "down_ask"]


def _write(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(path), compression="zstd")


def _markets_rows() -> list:
    return [
        {"condition_id": CID_BTC, "slug": "btc-updown-5m-x", "series_id": "BTC-5m",
         "asset": "BTC", "window_size_seconds": 300, "up_token_id": "1001",
         "down_token_id": "1002", "market_start_ts_ms": DAY_START_MS,
         "market_end_ts_ms": DAY_START_MS + 300_000, "window_index": 1,
         "resolution_outcome": "up", "settlement_price": 101.5,
         "settlement_source": "chainlink"},
        {"condition_id": CID_ETH, "slug": "eth-updown-5m-x", "series_id": "ETH-5m",
         "asset": "ETH", "window_size_seconds": 300, "up_token_id": "2001",
         "down_token_id": "2002", "market_start_ts_ms": DAY_START_MS,
         "market_end_ts_ms": DAY_START_MS + 300_000, "window_index": 1,
         "resolution_outcome": "down", "settlement_price": 2500.25,
         "settlement_source": "chainlink"},
        {"condition_id": CID_ETH_OTHER, "slug": "eth-updown-5m-y", "series_id": "ETH-5m",
         "asset": "ETH", "window_size_seconds": 300, "up_token_id": "3001",
         "down_token_id": "3002", "market_start_ts_ms": DAY_START_MS,
         "market_end_ts_ms": DAY_START_MS + 300_000, "window_index": 2},
    ]


def _clean_rows(cid: str, n: int, wide: int = 40, start_ms: int = 0) -> pa.Table:
    """n snapshot rows for cid with the 6 fold columns + `wide` junk columns.

    up mid: 0.40, 0.42, 0.44, ... ; down mid fixed 0.50/0.52 => deterministic
    open/close/low/high and average spreads regardless of batch order.
    """
    cols: dict = {
        "condition_id": pa.array([cid] * n, type=pa.string()),
        "ts_snapshot_ns": pa.array([(start_ms + i) * 1_000_000 for i in range(n)], type=pa.int64()),
        "up_bid": pa.array([0.40 + 0.02 * i for i in range(n)], type=pa.float64()),
        "up_ask": pa.array([0.41 + 0.02 * i for i in range(n)], type=pa.float64()),
        "down_bid": pa.array([0.50 if i % 2 else None for i in range(n)], type=pa.float64()),
        "down_ask": pa.array([0.52 if i % 2 else None for i in range(n)], type=pa.float64()),
    }
    for j in range(wide):
        cols[f"junk_l{j:03d}"] = pa.array([float(j)] * n, type=pa.float64())
    return pa.table(cols)


def _trades_rows(rows: list) -> pa.Table:
    return pa.table({
        "condition_id": pa.array([r[0] for r in rows], type=pa.string()),
        "notional": pa.array([r[1] for r in rows], type=pa.float64()),
        "wallet": pa.array([r[2] for r in rows], type=pa.string()),
    })


@pytest.fixture()
def summary_hive(tmp_path: Path) -> Path:
    data = tmp_path / "data"
    _write(data / "markets_latest" / "markets_latest.parquet",
           pa.Table.from_pylist(_markets_rows()))
    return data


def _by_cid(table: pa.Table) -> dict:
    out = {}
    for r in table.to_pylist():
        out[r["condition_id"]] = r
    return out


# ---------------------------------------------------------------- source pins

def test_fold_projects_needed_columns_at_read_time() -> None:
    src = inspect.getsource(ex.build_markets_summary)
    assert "columns=_FOLD_NEED" in src  # the memory bound itself
    assert src.count("iter_batches(batch_size=20000, columns=_FOLD_NEED)") == 2  # staging loop AND hive fallback
    # no unprojected whole-row batch iteration may remain in the summary
    assert "iter_batches(batch_size=20000)\n" not in src
    assert "_FOLD_NEED = [" in src


def test_fold_hive_fallback_no_longer_reads_whole_rows() -> None:
    src = inspect.getsource(ex.build_markets_summary)
    assert "iter_source_files as _iter_clean_files" in src
    # stream_batches (whole-row 842-col batches) must not feed the fold
    assert "stream_batches as _stream_batches" not in src


def test_trades_intermediates_released_at_phase_boundary() -> None:
    src = inspect.getsource(ex.build_markets_summary)
    assert "del t, cols, trades" in src  # b7b86fd per-dataset del/gc mirror
    assert "del agg, valid_w" in src and "del dist" in src
    assert "_gc_trades.collect()" in src


def test_trades_aggregation_contract_unchanged() -> None:
    """Concat + ONE group_by stays: per-file merge broke float parity
    (last-ULP notional drift on 1972/20434 real cids) — pin the exact calls."""
    src = inspect.getsource(ex.build_markets_summary)
    assert 't.group_by("condition_id").aggregate([("one", "sum"), ("notional", "sum")])' in src
    assert 'valid_w.group_by("condition_id").aggregate([("wallet", "count_distinct")])' in src
    # guards untouched elsewhere
    assert "_read_trades_for_summary" in src  # narrow 3-col staging read kept


def test_guards_untouched_elsewhere() -> None:
    src_commit = inspect.getsource(ex._commit_staging_file)
    assert "fail closed: never publish a missing snapshots file" in src_commit
    src_verify = inspect.getsource(ex._verify_staging_row_counts)
    assert "footer metadata ONLY" in src_verify  # verify stays bounded


# ---------------------------------------------------------------- functional

def test_summary_folds_wide_staging_correctly(summary_hive: Path, tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    _write(staging / "BTC_book_snapshots_clean.parquet", _clean_rows(CID_BTC, 30))
    _write(staging / "ETH_book_snapshots_clean.parquet", _clean_rows(CID_ETH, 25))
    _write(staging / "BTC_trades.parquet",
           _trades_rows([(CID_BTC, 100.0, "w1"), (CID_BTC, 23.0, "w2"),
                         (CID_BTC, 0.5, "w1"), (CID_BTC, 1.5, None)]))
    _write(staging / "ETH_trades.parquet",
           _trades_rows([(CID_ETH, 10.0, "w9")]))

    tbl = ex.build_markets_summary(str(summary_hive), staging_dir=str(staging),
                                   assets=["BTC", "ETH"], timeframe_label="5m")
    assert tbl.num_rows == 3 and tbl.num_columns == len(MARKETS_SUMMARY_SCHEMA)
    assert tbl.schema.equals(pa.schema(MARKETS_SUMMARY_SCHEMA))
    rows = _by_cid(tbl)

    btc = rows[CID_BTC]
    # up mid = (bid+ask)/2 over rows i=0..29: 0.405 + 0.02*i
    assert btc["up_open"] == pytest.approx(0.405)
    assert btc["up_close"] == pytest.approx(0.985)
    assert btc["up_low"] == pytest.approx(0.405)
    assert btc["up_high"] == pytest.approx(0.985)
    # down mid present only on odd i: open/close 0.51, avg spread 0.02
    assert btc["down_open"] == pytest.approx(0.51)
    assert btc["down_close"] == pytest.approx(0.51)
    assert btc["avg_spread_down"] == pytest.approx(0.02)
    assert btc["avg_spread_up"] == pytest.approx(0.01)
    assert btc["snapshot_count"] == 30
    assert btc["traded_volume"] == pytest.approx(125.0)
    assert btc["fill_count"] == 4
    assert btc["unique_traders"] == 2  # w1 twice counts once; None wallet excluded
    assert btc["resolution_outcome"] == "up"
    assert btc["settlement_price"] == 101.5

    eth = rows[CID_ETH]
    assert eth["snapshot_count"] == 25
    assert eth["traded_volume"] == pytest.approx(10.0)
    assert eth["fill_count"] == 1 and eth["unique_traders"] == 1

    other = rows[CID_ETH_OTHER]  # market with no clean rows
    assert other["snapshot_count"] is None and other["up_open"] is None
    assert other["traded_volume"] is None  # never zero-filled (null-vs-zero)


def test_fold_skips_file_missing_needed_columns(summary_hive: Path, tmp_path: Path) -> None:
    """A clean file missing a fold column contributes NOTHING — the old
    all-columns-present guard skipped every batch; the projection now raises
    at read time and the per-file except skips the file. Same net semantics."""
    staging = tmp_path / "staging"
    staging.mkdir()
    bad = _clean_rows(CID_BTC, 10)
    bad = bad.drop_columns(["up_ask"])
    _write(staging / "BTC_book_snapshots_clean.parquet", bad)
    _write(staging / "ETH_book_snapshots_clean.parquet", _clean_rows(CID_ETH, 5))

    tbl = ex.build_markets_summary(str(summary_hive), staging_dir=str(staging),
                                   assets=["BTC", "ETH"], timeframe_label="5m")
    rows = _by_cid(tbl)
    assert rows[CID_BTC]["snapshot_count"] is None  # bad file folded nothing
    assert rows[CID_ETH]["snapshot_count"] == 5    # other lanes unaffected


def test_hive_fallback_folds_with_projection(summary_hive: Path, tmp_path: Path) -> None:
    """No staging clean files -> hive fallback. Must fold the SAME values the
    staging path would (projection parity between the two fold loops)."""
    staging_a = tmp_path / "staging_a"
    staging_a.mkdir()
    _write(staging_a / "BTC_book_snapshots_clean.parquet", _clean_rows(CID_BTC, 20))
    _write(staging_a / "BTC_trades.parquet", _trades_rows([(CID_BTC, 7.0, "w1")]))
    via_staging = ex.build_markets_summary(str(summary_hive), staging_dir=str(staging_a),
                                           assets=["BTC"], timeframe_label="5m")

    _write(summary_hive / "book_snapshots_clean" / "date=2026-10-16" / "part-0.parquet",
           _clean_rows(CID_BTC, 20))
    _write(summary_hive / "trades" / "date=2026-10-16" / "part-0.parquet",
           _trades_rows([(CID_BTC, 7.0, "w1")]))
    via_hive = ex.build_markets_summary(str(summary_hive), staging_dir=str(tmp_path / "empty"),
                                        assets=["BTC"], timeframe_label="5m")

    assert via_staging.num_rows == via_hive.num_rows == 3
    assert via_staging.schema.equals(via_hive.schema)
    a = _by_cid(via_staging)[CID_BTC]
    b = _by_cid(via_hive)[CID_BTC]
    for k in a:
        assert a[k] == b[k], f"staging-vs-hive fold mismatch on {k!r}: {a[k]!r} != {b[k]!r}"
    assert b["snapshot_count"] == 20


def _real_5m_staging() -> Path:
    base = Path(__file__).resolve().parents[1]
    p = base / "data" / "kaggle_staging" / "5m" / "gghgg1" / "polymarket-5m-crypto"
    if not (p / "BTC_book_snapshots_clean.parquet").is_file():
        pytest.skip("real 5m staging absent")
    return p


def test_real_staging_summary_rows_cols_schema() -> None:
    """Real-data smoke: the fixed finalize still produces the exact
    rows×cols×schema contract on the production staging dir."""
    staging = _real_5m_staging()
    data = staging.parents[3]  # .../data (kaggle_staging/{lane}/{owner}/{dataset})
    tbl = ex.build_markets_summary(str(data), staging_dir=str(staging),
                                   assets=["BTC", "ETH", "SOL", "HYPE", "BNB", "XRP", "DOGE"],
                                   timeframe_label="5m")
    assert tbl.num_columns == len(MARKETS_SUMMARY_SCHEMA)
    assert tbl.schema.equals(pa.schema(MARKETS_SUMMARY_SCHEMA))
    assert tbl.num_rows > 0
    rows = tbl.to_pylist()
    assert all(r["condition_id"] for r in rows)  # key never null
    # deterministic fields must exist and stay typed (null-vs-zero policy)
    for r in rows[:50]:
        assert r["traded_volume"] is None or isinstance(r["traded_volume"], float)
        assert r["snapshot_count"] is None or isinstance(r["snapshot_count"], int)
