"""Unit tests for paper_momentum_impulse pure decision helpers.

No network. Imports the bot module for skew_agrees / size_shares only.
"""
import importlib.util
import os

import pytest

_here = os.path.dirname(os.path.abspath(__file__))
_mod_path = os.path.join(_here, "..", "paper_momentum_impulse.py")
_spec = importlib.util.spec_from_file_location("paper_momentum_impulse", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


def test_skew_agrees_up():
    assert _mod.skew_agrees("Up", 0.77, 0.23) is True
    assert _mod.skew_agrees("Up", 0.40, 0.60) is False


def test_skew_agrees_down():
    assert _mod.skew_agrees("Down", 0.36, 0.64) is True
    assert _mod.skew_agrees("Down", 0.60, 0.40) is False


def test_skew_agrees_missing_mid():
    assert _mod.skew_agrees("Up", None, 0.23) is False
    assert _mod.skew_agrees("Down", 0.36, None) is False


def test_skew_agrees_boundary_is_not_agreement():
    # exactly 0.5 is no lean — must not count as confirmation
    assert _mod.skew_agrees("Up", 0.5, 0.5) is False


def test_size_shares_risk_cap_binds():
    # 5% risk budget at ~0.30 ask => shares = 50 / cost_ps
    shares = _mod.size_shares(1000.0, 0.30)
    cost_ps = 0.30 + 0.07 * 0.30 * 0.70
    assert shares == pytest.approx(50.0 / cost_ps)


def test_size_shares_never_exceeds_pos_frac():
    # invariant across the tradeable price range: notional <= 50% of equity
    for px in (0.02, 0.10, 0.30, 0.60, 0.94):
        shares = _mod.size_shares(1000.0, px)
        assert shares * px <= 1000.0 * _mod.MAX_POS_FRAC + 1e-9
        assert shares > 0


def test_size_shares_unpriceable():
    assert _mod.size_shares(1000.0, 0.0) == 0.0
    assert _mod.size_shares(0.0, 0.30) == 0.0


def test_impulse_threshold_config_sane():
    # idea range $70-100; default must sit inside it
    assert 70.0 <= _mod.IMPULSE_MIN <= 100.0
    assert 180 <= _mod.ENTRY_AGE_MIN <= _mod.ENTRY_AGE_MAX <= 270
