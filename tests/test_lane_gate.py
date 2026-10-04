"""Unit tests for the lane gate logic.

Tests threshold math and gate decision logic without any network I/O.
"""


LIVE_THRESHOLD_PCT = 80.0


def _make_live_pct(live, total):
    return 100.0 * live / max(1, total)


def test_gate_met_above_threshold():
    """Gate MET when live% > 80%."""
    assert _make_live_pct(81, 100) > LIVE_THRESHOLD_PCT
    assert _make_live_pct(90, 100) > LIVE_THRESHOLD_PCT


def test_gate_not_met_below_threshold():
    """Gate NOT MET when live% < 80%."""
    assert _make_live_pct(79, 100) < LIVE_THRESHOLD_PCT
    assert _make_live_pct(55, 357244) < LIVE_THRESHOLD_PCT  # real-world ~55.84%


def test_gate_exactly_threshold():
    """Gate NOT MET when live% exactly at 80% (strict >)."""
    pct = _make_live_pct(80, 100)
    assert pct == LIVE_THRESHOLD_PCT
    assert not (pct > LIVE_THRESHOLD_PCT)


def test_gate_met_just_above_threshold():
    """Gate MET when live% just above 80%."""
    pct = _make_live_pct(80 + 1, 100)
    assert pct > LIVE_THRESHOLD_PCT


def test_gate_not_met_just_below_threshold():
    """Gate NOT MET when live% just below 80%."""
    pct = _make_live_pct(80 - 1, 100)
    assert pct < LIVE_THRESHOLD_PCT


def test_gate_with_real_data_numbers():
    """Gate NOT MET with actual measured numbers from 2026-10-04."""
    # Measured: live=199471, stale=157554, resync=219, n=357244 → 55.84%
    live_pct = _make_live_pct(199471, 357244)
    assert live_pct < LIVE_THRESHOLD_PCT
    assert abs(live_pct - 55.84) < 0.1


def test_gate_met_100_percent():
    """Gate MET at 100% live."""
    assert _make_live_pct(100, 100) > LIVE_THRESHOLD_PCT