# Test Plan: >99% Data Completeness for 5-Minute Markets

## Objective
Verify that the polymarket-collector captures ≥99% of expected snapshot intervals for 2 specified 5-minute markets, tracking gaps, resync episodes, and book_state correctness.

## Scope
- Assets: 2 of [BTC, ETH, SOL, HYPE, BNB, XRP, DOGE]
- Metric: `snapshot_completeness_pct` ≥ 99% (actual_snapshots / expected_snapshots × 100)
- Expected snapshots per market per day: 172,800 (2/sec × 86,400s)
- Test duration: Minimum 1 full day (24h) or sufficient pilot run to capture ≥1,000 ticks per market

## Test Metrics
| Metric | Target | Description |
|---|---|---|
| `snapshot_completeness_pct` | ≥ 99% | actual / expected snapshots |
| `missing_intervals` | ≤ 0.5% of expected | Gaps not attributable to resync |
| `resync_episode_count` | Any (tracked) | Disconnect/resync events recorded |
| `book_state = "stale"` % | ≤ 1% of snapshots | Should not be majority of data |
| `coverage_gap_count` | ≤ 2 per asset | §1 coverage gaps |

## Test Scenarios

### 1. Baseline Completeness Test (§15)
**File**: `tests/test_completeness.py` — extend existing
- Write synthetic snapshot data for 2 assets across 1+ day(s)
- Run `compute_daily_completeness()` and verify `snapshot_completeness_pct ≥ 99%`
- Verify `missing_intervals` count is reasonable (≤ 0.5% × expected)

### 2. Resync-Aware Completeness Test
**File**: New test or extend `tests/test_chaos.py`
- Simulate disconnect/resync events per §1A
- After resync completion, verify completeness recovers to ≥99%
- Track `resync_episodes` gap durations and confirm they're accounted for in missing_intervals

### 3. Malformed-Message Resilience Test (§3A)
**File**: `tests/test_chaos.py` — extend
- Feed malformed price/size values outside [0,1] / negative size
- Verify book marks `stale`, does NOT apply malformed data
- Confirm completeness remains ≥99% after rejecting bad messages

### 4. Sequence-Gap Detection Test (§1A)
**File**: `tests/test_chaos.py` — extend or `tests/test_resync.py`
- Synthetically drop/ reorder WS messages
- Verify `sequence_gap` events fire
- After resync, verify completeness recovers to ≥99%

### 5. Backpressure / No-Drop Test (§10A)
**File**: `tests/test_chaos.py` — extend `test_chaos_backpressure`
- Artificially slow disk I/O
- Verify no snapshots silently dropped (backpressure fires instead)
- After recovery, verify completeness ≥99%

### 6. Clock Drift Resilience Test (§14)
- Verify `clock_issue` fires if drift > 50ms
- Ensure timestamps remain aligned to 500ms scheduler grid
- Completeness not degraded by clock issues

### 7. End-to-End Integration Test (smoke)
Run collector in test mode for ≥ 10 minutes with 2 assets
- Collect collector_events, resync_episodes
- Run `compute_daily_completeness()` on output data
- Assert `snapshot_completeness_pct ≥ 99%`

## Success Criteria
A test pass is considered successful if ALL of the following hold:
1. `snapshot_completeness_pct ≥ 99%` for both assets
2. No more than 1% of snapshots have `book_state = "stale"` (excluding intentional resync windows)
3. All gaps are accounted for in `resync_episodes` or `missing_intervals`
4. No data loss from backpressure, malformed messages, or sequence gaps
5. `clock_issue` not triggered (or if triggered, completeness unaffected)

## Implementation Notes
- Use existing `compute_daily_completeness()` from `polymarket_collector.completeness`
- Extend existing test infrastructure (`tests/test_chaos.py`, `tests/test_completeness.py`)
- Tests should run against recorded WS data or mocked collector, not require live API
- Each test should be self-contained using `tempfile.TemporaryDirectory()`
- Use pytest markers to distinguish: `@pytest.mark.completeness_99pct`

## Run Order
1. `pytest tests/test_completeness.py -v` — baseline
2. `pytest tests/test_chaos.py -v` — resilience under failure
3. Custom 99% completeness tests — final verification

## Example Test Structure (to be added)

```python
def test_99pct_completeness_two_assets(tmpdir):
    """Verify ≥99% snapshot completeness for 2 5-min markets."""
    # Setup collector with 2 assets, run for sufficient duration
    # Run compute_daily_completeness()
    # Assert snapshot_completeness_pct >= 99
    pass
```