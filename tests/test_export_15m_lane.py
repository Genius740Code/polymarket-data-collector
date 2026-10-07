"""15m lane staging builder — locked by default, lane-pure, manifest-proven.

The 15m lane reuses the 08ea3d4 bounded-worker path via a thin wrapper
(prepare_kaggle_staging_15m); 4h/1d builders are out of scope. These tests
pin the lock contract on tiny real-parquet fixtures (no rows are ever
invented by the builder — honest gaps stay gaps):

- locked: RuntimeError without enabled=True, nothing built (fail-closed);
- default slug is gghgg1/polymarket-15m-crypto (plan.md §1.1);
- lane purity: 15m staging carries only {ASSET}-15m rows, never 5m rows;
- first-upload proof: staging-manifest.json rows + sha256 verify by read.
"""

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_collector.storage.export import (
    _write_staging_manifest,
    prepare_kaggle_staging_15m,
)

CID_15 = "0x" + "c1" * 32
CID_5 = "0x" + "d2" * 32


def _snap(cid, series, ts_ns):
    return {"asset": "BTC", "condition_id": cid, "series_id": series,
            "ts_snapshot_ns": ts_ns}


def _hive_15m_5m(data: Path) -> None:
    part = data / "book_snapshots_500ms" / "date=2026-10-01" / "asset=BTC"
    part.mkdir(parents=True, exist_ok=True)
    rows = [_snap(CID_15, "BTC-15m", 1_000_000_000 + i) for i in range(3)]
    rows += [_snap(CID_5, "BTC-5m", 2_000_000_000 + i) for i in range(2)]
    pq.write_table(pa.Table.from_pylist(rows), str(part / "snap.parquet"),
                   compression="zstd")


def test_15m_locked_by_default(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    _hive_15m_5m(data)
    with pytest.raises(RuntimeError):
        prepare_kaggle_staging_15m(data, staging_dir=tmp_path / "staging",
                                   assets=["BTC"],
                                   datasets=["book_snapshots_500ms"])
    # fail-closed: nothing built
    assert not (tmp_path / "staging").exists()


def test_15m_default_slug_and_lane_purity(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    _hive_15m_5m(data)
    prep = prepare_kaggle_staging_15m(
        data, assets=["BTC"], datasets=["book_snapshots_500ms"],
        enabled=True)
    assert prep["dataset"] == "gghgg1/polymarket-15m-crypto"
    assert "15m" in prep["staging_path"]
    staged = Path(prep["staging_path"]) / "BTC_book_snapshots_500ms.parquet"
    assert staged.exists()
    got = pq.read_table(str(staged)).to_pylist()
    assert len(got) == 3
    assert {r["series_id"] for r in got} == {"BTC-15m"}
    assert {r["condition_id"] for r in got} == {CID_15}
    # first-upload proof written beside dataset-metadata.json
    for name in ("dataset-metadata.json", "staging-manifest.json"):
        assert (Path(prep["staging_path"]) / name).exists()
    assert prep["staging_manifest"] is not None


def test_staging_manifest_rows_and_sha_verify_by_read(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    rows = [_snap(CID_15, "BTC-15m", 1_000_000_000 + i) for i in range(5)]
    pq.write_table(pa.Table.from_pylist(rows),
                   str(staging / "BTC_book_snapshots_500ms.parquet"),
                   compression="zstd")
    out = _write_staging_manifest(staging, "gghgg1/polymarket-15m-crypto", "15m")
    assert out is not None and out.exists()
    # tmp discipline: no stray .tmp left behind
    assert list(staging.glob("*.tmp")) == []
    manifest = json.loads(out.read_text())
    assert manifest["dataset"] == "gghgg1/polymarket-15m-crypto"
    assert manifest["timeframe"] == "15m"
    assert manifest["total_rows"] == 5
    assert len(manifest["files"]) == 1
    entry = manifest["files"][0]
    assert entry["rows"] == 5
    raw = (staging / entry["name"]).read_bytes()
    assert entry["sha256"] == hashlib.sha256(raw).hexdigest()
    assert entry["bytes"] == len(raw)
