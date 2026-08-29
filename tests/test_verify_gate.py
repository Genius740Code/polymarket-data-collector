"""Tests for §18 verification gate — offline checklist mode."""
import tempfile
import json
import asyncio

import pytest

from polymarket_collector.verify_gate import GateCheck, GateResult, run_gate


def test_gate_offline():
    # offline mode without --live should have all passed=None and all_passed=False
    result = asyncio.run(run_gate(config_path=None))
    # run_gate with no config will try live probes but may fail to connect; we just check structure
    # In CI without network, checks may be None or False — but should not crash
    assert isinstance(result, GateResult)
    assert len(result.checks) == 4
    names = {c.name for c in result.checks}
    assert names == {"ws_sequence_number", "rest_full_l2", "settlement_report", "rate_limits"}


def test_gate_result_serialization():
    gr = GateResult(checks=[
        GateCheck(name="ws_sequence_number", question="q1", passed=True, details="ok"),
        GateCheck(name="rest_full_l2", question="q2", passed=False, details="fail"),
    ])
    d = gr.to_dict()
    assert "all_passed" in d
    assert len(d["checks"]) == 2


@pytest.mark.asyncio
async def test_verify_gate_cli_offline():
    # run_gate offline check
    from polymarket_collector.verify_gate import check_ws_sequence_number

    # with invalid URL it should handle gracefully (None passed)
    chk = await check_ws_sequence_number("ws://127.0.0.1:1", timeout_s=0.5)
    assert chk.name == "ws_sequence_number"
    assert chk.passed in (None, False)  # either no connect or no messages
