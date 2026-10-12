"""B-4 adaptive catch-up drain (2026-10-12) — kills the permanent-deficit mode.

Measured failure (2026-10-12, PID 1218164): the fixed 6-bucket catch-up slice
drained 6 buckets/tick while a saturated event loop stretched ticks to ~3s —
drain == accrual, so one 60s stall parked the 500ms scheduler at a
~60-76s steady-state deficit for hours (20,926 scheduler_lag events/14h,
cur_bucket-last_bucket pinned 71-79s, every processed bucket deferred ->
M5 downgrade -> 0% live rows despite fresh books).

Fix: per-tick time budget (floor 6 / cap 60 / 400ms) — a cheap tick
(measured 76-213ms per 6-bucket body) drains the backlog faster than the
2-buckets/s accrual and the deficit strictly shrinks to zero; a slow tick
behaves exactly like the old fixed slice. These tests pin the decision
helper, the cursor commit (the unprocessed tail must never be skipped)
and the deficit-closing arithmetic on the measured body times.
"""
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


# --- budget decision helper -------------------------------------------------

def test_over_budget_respects_floor():
    # below the floor a catch-up tick never breaks — B-4's minimum drain
    assert Collector._catchup_over_budget(0, 999.0) is False
    assert Collector._catchup_over_budget(5, 999.0) is False
    # past the floor, budget spent -> stop handing buckets to this tick
    assert Collector._catchup_over_budget(6, 999.0) is True
    # past the floor but budget not spent -> keep draining
    assert Collector._catchup_over_budget(60, 0.0) is False


def test_over_budget_never_raises_on_garbage():
    assert Collector._catchup_over_budget(None, None) is False


def test_slice_cap_bounds_burst():
    # the slice cap must stay far above the accrual per starved tick (~6
    # buckets per ~3s) so a cheap tick closes the deficit, and bounded well
    # below the 120-bucket burst that 1013-closed the BTC lane (2026-09-17)
    assert Collector._CATCHUP_SLICE_CAP >= 4 * Collector._CATCHUP_SLICE_FLOOR
    assert Collector._CATCHUP_SLICE_CAP <= 60
    assert 0.0 < Collector._CATCHUP_SLICE_BUDGET_S <= 0.5


# --- cursor commit -----------------------------------------------------------

def test_commit_progress_follows_last_processed_not_tail():
    """The adaptive drain may stop mid-batch — advancing to buckets[-1] would
    silently skip the unprocessed tail (rows lost without even a
    coverage_gap trail). The cursor must follow the last PROCESSED bucket."""
    import tempfile
    tmp = tempfile.mkdtemp()
    col = Collector(make_cfg(tmp))
    col._last_snapshot_bucket_ms = None
    col._commit_snapshot_progress(1791823000000, 1791823060000)
    assert col._last_snapshot_bucket_ms == 1791823000000
    # nothing processed this tick -> fall back to cur (pre-commit semantics)
    col._commit_snapshot_progress(None, 1791823060000)
    assert col._last_snapshot_bucket_ms == 1791823060000


# --- the measured failure mode: deficit pinned vs deficit closed -------------

def _drain_one_tick(deficit: int, body_s: float, starved_accrual: int,
                    budgeted: bool) -> int:
    """Mirror of the _snapshot_loop slice + in-loop budget break.

    deficit: backlog buckets at wake; starved_accrual: buckets accrued during
    the ~3s starved tick (measured wake interval in permanent catch-up);
    body_s: per-bucket tick body (measured 76-213ms per 6-bucket body).
    """
    backlog = deficit + starved_accrual
    batch = min(backlog, Collector._CATCHUP_SLICE_CAP)
    processed = 0
    elapsed = 0.0
    for i in range(batch):
        if budgeted and Collector._catchup_over_budget(i, elapsed):
            break
        if not budgeted and i >= Collector._CATCHUP_SLICE_FLOOR:
            break  # the old fixed 6-slice
        elapsed += body_s
        processed += 1
    return processed


def test_fixed_slice_pins_deficit_budgeted_drain_closes_it():
    """Replay of the 2026-10-12 failure: 76s deficit (152 buckets), ~3s
    starved ticks accruing 6 buckets each, 36ms per-bucket body (213ms
    measured worst / 6). The old fixed slice NEVER closes the deficit —
    the budgeted drain must close it and then stay closed."""
    deficit = 152
    accrual = 6
    body = 0.036

    # old behavior: drain == accrual forever (the pinned ~76s steady state)
    d = deficit
    for _ in range(500):
        d = d + accrual - _drain_one_tick(d, body, accrual, budgeted=False)
    assert d >= deficit, "fixed slice must never fall behind the measurement"

    # new behavior: cheap ticks drain past accrual — deficit strictly closes
    d = deficit
    ticks = 0
    while d > 0 and ticks < 500:
        d = d + accrual - _drain_one_tick(d, body, accrual, budgeted=True)
        ticks += 1
    assert d <= 0, f"budgeted drain failed to close a {deficit}-bucket deficit in {ticks} ticks"
    # once closed the loop is on-time: 1-bucket batches, no break (floor gate)
    assert _drain_one_tick(0, body, 1, budgeted=True) == 1
