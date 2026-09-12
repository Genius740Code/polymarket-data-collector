"""Checkpoint-resume: fresh (lane, dataset) files skip rebuild."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow.parquet as pq

import polymarket_collector.storage.export as E
from test_streaming_export import _hive  # noqa


def test_checkpoint_skips_fresh_rebuild(monkeypatch, tmp_path):
    base = _hive(tmp_path)
    out = tmp_path / "staging"
    out.mkdir()
    calls = []

    real = E._build_in_subprocess

    def _counting(*a, **k):
        calls.append(a[2] if len(a) > 2 else k.get("datasets"))
        return real(*a, **k)

    monkeypatch.setattr(E, "_build_in_subprocess", _counting)
    # box-RAM gate must not interfere with the unit test
    monkeypatch.setattr(E, "_avail_mb", lambda: 99999)
    stats1 = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=["book_snapshots_500ms"], rolling_window=True)
    assert calls, "first run must spawn a worker"
    assert stats1, stats1
    n_calls_first = len(calls)
    # second run minutes later: checkpoint fresh -> no worker spawn
    stats2 = E.export_per_asset_single_file(
        str(base), out_dir=str(out), assets=["BTC"], timeframe_label="5m",
        datasets=["book_snapshots_500ms"], rolling_window=True)
    assert len(calls) == n_calls_first, f"fresh files must skip spawn, calls={calls}"
    assert stats2 == stats1
    # progress file records the completion
    prog = (base / "kaggle_staging" / "_progress.json")
    assert prog.exists()
