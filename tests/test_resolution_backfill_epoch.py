"""Regression: 2026-10-14 resolution-backfill crash-loop (pm2 id 8, ~4x/hr).

MarketsLog.compact() rebuilds markets_latest from every markets_log fragment
plus the run's staging rows via pa.Table.from_pylist. Collector-written
fragments carry settlement_ts_utc as the canonical STRING (schemas.py), while
markets_latest — and therefore every row this backfill re-appended — carried
it as int64 epoch-ms (the ts_source int-ms leak). One mixed winner set later
pyarrow died with the exact production error:

    pyarrow.lib.ArrowInvalid: Could not convert '1791590700000' with type str:
    tried to convert to int64

...raised out of backfill_resolutions() -> main() at line 227, killing each
15-min cron run. These tests replay the real shapes through the REAL
MarketsLog write paths (offline: the single CLOB GET is answered locally) on
the real incident value 1791590700000 (XRP/BNB/SOL/ETH/DOGE/HYPE window
w5971968, ended 2026-10-10T00:05:00Z).
"""

import time

import pyarrow.parquet as pq

from polymarket_collector.resolution_backfill import backfill_resolutions
from polymarket_collector.storage.markets_log import MarketsLog

# the exact value pyarrow choked on in the production error log
REAL_EPOCH_MS = 1791590700000
REAL_EPOCH_MS_STR = "1791590700000"
REAL_EPOCH_ISO = "2026-10-10T00:05:00Z"


def _ended_row(cid: str, asset: str, widx: int, status: str, outcome: str,
               source, settlement_ts) -> dict:
    """A markets row as the collector/backfill write it (ended 5m window)."""
    # clearly-stale stamp: compact() dedups by max updated_at (string compare)
    # and the backfill's re-append must always win it
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(time.time() - 3600))
    end_ms = (int(time.time()) - 600) * 1000
    return {
        "updated_at": now_iso,
        "recorded_at": now_iso,
        "market_start_ts": None,
        "market_end_ts": None,
        "market_start_ts_ms": end_ms - 300_000,
        "market_end_ts_ms": end_ms,
        "resolution_ts": None,
        "condition_id": cid,
        "market_id": cid,
        "series_id": f"{asset}-5MIN",
        "window_index": widx,
        "window_label": "5m",
        "window_size_seconds": 300,
        "asset": asset,
        "up_token_id": "up",
        "down_token_id": "down",
        "status": status,
        "resolution_outcome": outcome,
        "question": None,
        "resolution_rule": None,
        "resolution_source": None,
        "tick_size": None,
        "minimum_order_size": None,
        "minimum_notional": None,
        "fee_information": None,
        "reported_volume": None,
        "reported_liquidity": None,
        "settlement_price": None,
        "settlement_ts_utc": settlement_ts,
        "resolution_confirmed_at": None,
        "settlement_source": source,
    }


class _ClobResp:
    status_code = 200

    def __init__(self, j):
        self._j = j

    def json(self):
        return self._j


def _patch_clob(monkeypatch, outcome="up", price=1.0):
    """Answer the backfill's single CLOB GET locally (offline test)."""
    import httpx

    def local_get(url, timeout=None, headers=None):
        return _ClobResp({"closed": True, "minimum_order_size": 0.1,
                          "minimum_tick_size": 0.001, "tokens": [
            {"outcome": "Up", "price": price, "winner": outcome == "up"},
            {"outcome": "Down", "price": 1 - price, "winner": outcome != "up"},
        ]})

    monkeypatch.setattr(httpx, "get", local_get)


def _fragment_rows(data_dir):
    rows = []
    for part in (data_dir / "markets_log").rglob("*.parquet"):
        rows.extend(pq.read_table(part).to_pylist())
    return rows


# --------------------------------------------------------------- unit: the cast
def test_settlement_ts_cast_converts_real_epoch_values():
    from polymarket_collector.resolution_backfill import _iso_settlement_ts
    # the string form straight from the production error log
    assert _iso_settlement_ts(REAL_EPOCH_MS_STR) == REAL_EPOCH_ISO
    # the int64 form markets_latest actually carries
    assert _iso_settlement_ts(REAL_EPOCH_MS) == REAL_EPOCH_ISO
    # never raises / never invents: None, already-ISO and unparseable pass through
    assert _iso_settlement_ts(None) is None
    assert _iso_settlement_ts(REAL_EPOCH_ISO) == REAL_EPOCH_ISO
    assert _iso_settlement_ts("not-a-timestamp") == "not-a-timestamp"


# -------------------------------------- integration: mixed winners get compacted
def test_backfill_compacts_str_epoch_fragment_without_crash(tmp_path, monkeypatch):
    """The exact production shape: a collector-written fragment holds the
    digit-STRING settlement row for a market that is NOT in markets_latest
    (never re-appended by the backfill), while the candidate row in latest
    carries int64 epoch-ms. Old code: compact() raised ArrowInvalid out of
    backfill_resolutions. Fixed code: the re-appended row is canonical ISO,
    every winner is a string and the compact succeeds."""
    _patch_clob(monkeypatch)

    # 1) latest carries ONLY the upgrade candidate, settlement_ts_utc int64
    log = MarketsLog(tmp_path)
    log.append(_ended_row("0xcandidate", "XRP", 5971968, "resolved", "up",
                          "inferred_nearest", REAL_EPOCH_MS))
    log.flush_staging()
    log.compact()
    # 2) collector-side fragment: digit-STRING settlement for a market that
    #    never made it into latest (the w5971968 str rows of 2026-10-10)
    log.append(_ended_row("0xstronly", "BNB", 5971968, "resolved", "down",
                          "inferred_nearest", REAL_EPOCH_MS_STR))
    log.flush_staging()

    stats = backfill_resolutions(tmp_path)  # old code: ArrowInvalid here

    assert stats["candidates"] == 1 and stats["upgraded"] == 1
    latest = {r["condition_id"]: r for r in MarketsLog(tmp_path).load_latest()}
    cand = latest["0xcandidate"]
    assert cand["settlement_source"] == "polymarket_official"
    # the cast: re-appended row entered the log as the canonical ISO string
    assert cand["settlement_ts_utc"] == REAL_EPOCH_ISO
    # the historical digit-string row compacted alongside it, value preserved
    assert latest["0xstronly"]["settlement_ts_utc"] == REAL_EPOCH_MS_STR


# ------------------------- fail-closed: a persisting int winner must not kill the run
def test_backfill_survives_unmergeable_mix_fail_closed(tmp_path, monkeypatch, capsys):
    """Production today also holds int64-typed fragments (old backfill
    flushes) whose winner rows no write of ours can reach. compact() still
    raises on the mixed winners — but it must stay INSIDE the backfill
    (WARN, latest untouched, retried next run) instead of crash-looping pm2."""
    _patch_clob(monkeypatch)

    # 1) latest: the candidate (int64 settlement) + an already-official market
    #    whose ONLY row is the int64-typed fragment below (never re-appended)
    log = MarketsLog(tmp_path)
    log.append(_ended_row("0xcandidate", "XRP", 5971968, "resolved", "up",
                          "inferred_nearest", REAL_EPOCH_MS))
    log.append(_ended_row("0xintwinner", "ETH", 5971967, "resolved", "down",
                          "polymarket_official", REAL_EPOCH_MS))
    log.flush_staging()
    log.compact()
    # 2) collector-side digit-STRING fragment row (mixed with 1) -> unmergeable
    log.append(_ended_row("0xstronly", "BNB", 5971968, "resolved", "down",
                          "inferred_nearest", REAL_EPOCH_MS_STR))
    log.flush_staging()

    stats = backfill_resolutions(tmp_path)  # must NOT raise

    assert stats["upgraded"] == 1
    out = capsys.readouterr().out
    assert "WARN markets_latest compact failed" in out
    # fail-closed: latest is exactly as before the run (atomic compact)
    latest = {r["condition_id"]: r for r in MarketsLog(tmp_path).load_latest()}
    assert latest["0xcandidate"]["settlement_source"] == "inferred_nearest"
    # the resolution itself was flushed with the canonical ISO cast applied
    frag = [r for r in _fragment_rows(tmp_path)
            if r.get("condition_id") == "0xcandidate"
            and r.get("settlement_source") == "polymarket_official"]
    assert frag and frag[-1]["settlement_ts_utc"] == REAL_EPOCH_ISO
    assert isinstance(frag[-1]["settlement_ts_utc"], str)
