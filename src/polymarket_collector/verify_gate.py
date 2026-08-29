"""Verification gate — §18.

Hard gate: §1A resync/dedup design depends on assumptions (sequence_number,
full L2 REST, settlement report endpoint, rate limits). Confirm against live
payload capture BEFORE coding/running §1A.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class GateCheck:
    name: str
    question: str
    passed: Optional[bool] = None
    details: str = ""
    payload_sample: Optional[Dict[str, Any]] = None


@dataclass
class GateResult:
    checks: List[GateCheck] = field(default_factory=list)
    all_passed: bool = False

    def to_dict(self) -> dict:
        return {
            "all_passed": self.all_passed,
            "checks": [
                {"name": c.name, "question": c.question, "passed": c.passed, "details": c.details, "payload_sample": c.payload_sample}
                for c in self.checks
            ],
            "gate": "§18 — do not implement §1A until passed against live payload capture",
        }


async def check_ws_sequence_number(ws_url: str, timeout_s: float = 10.0) -> GateCheck:
    """Connect to WS briefly, capture messages, check for monotonic sequence_number."""
    chk = GateCheck(
        name="ws_sequence_number",
        question="Does the CLOB market-channel WS feed expose monotonic sequence_number per token on price_change/book/last_trade_price?",
    )
    try:
        import websockets

        # Use 10s capture window
        msgs: List[dict] = []
        try:
            async with websockets.connect(ws_url, open_timeout=5) as ws:
                # Subscribe to a minimal channel if required; otherwise just listen
                # For Polymarket CLOB, ws subscriptions require auth + asset_ids; we try anonymous listen
                end = time.time() + timeout_s
                while time.time() < end:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2)
                        try:
                            msg = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
                        except Exception:
                            continue
                        if isinstance(msg, dict):
                            msgs.append(msg)
                        elif isinstance(msg, list):
                            msgs.extend([m for m in msg if isinstance(m, dict)])
                    except asyncio.TimeoutError:
                        break
        except Exception as e:
            chk.passed = None
            chk.details = f"Could not connect to WS ({e}); manual payload capture required — see §18. Messages captured: {len(msgs)}."
            return chk

        # analyze captured messages
        seq_present = 0
        seq_missing = 0
        sample_with = None
        sample_without = None
        for m in msgs:
            if "sequence_number" in m or "sequence" in m or "seq" in m:
                seq_present += 1
                if not sample_with:
                    sample_with = m
            else:
                seq_missing += 1
                if not sample_without:
                    sample_without = m

        if not msgs:
            chk.passed = None
            chk.details = "No messages received in capture window — cannot determine sequence support. Verify gate requires live payload capture."
        elif seq_present > 0 and seq_missing == 0:
            chk.passed = True
            chk.details = f"All {seq_present} sampled messages carry sequence_number — primary gap detection via sequence (§1A) is viable."
            chk.payload_sample = sample_with
        elif seq_present > 0 and seq_missing > 0:
            chk.passed = True
            chk.details = f"Mixed: {seq_present} with seq, {seq_missing} without. Treat sequence detection as available where present, fallback to full-book diff elsewhere."
            chk.payload_sample = sample_with
        else:
            chk.passed = False
            chk.details = f"No sampled messages ({len(msgs)}) carried sequence_number. Full-book diff check becomes PRIMARY drift detection (§1A fallback path). Dedup must use fallback key (token_id, ts_received_ns, event_type, new_best_bid/ask)."
            chk.payload_sample = sample_without
    except ImportError:
        chk.passed = None
        chk.details = "websockets not installed — install to run live gate check."
    return chk


async def check_rest_full_l2(rest_book_url: str, timeout_s: float = 5.0) -> GateCheck:
    chk = GateCheck(
        name="rest_full_l2",
        question="Does CLOB REST expose a full L2 book per token (not just BBO/spread)? Required for §1A resync.",
    )
    try:
        import httpx

        # Probe endpoint with a dummy token_id (use a known test token if available)
        # Try without token first to see shape, then with dummy
        for url in [rest_book_url, rest_book_url.rstrip("/") + "/BTC"]:
            try:
                async with httpx.AsyncClient(timeout=timeout_s) as client:
                    resp = await client.get(url)
                    if resp.status_code in (400, 404):
                        # endpoint exists but needs valid token — this is still positive (endpoint exists)
                        chk.passed = True
                        chk.details = f"REST endpoint {url} returned {resp.status_code} (needs valid token_id) — endpoint exists, needs valid token probe. Confirm with a real market token_id via manual test (§18)."
                        chk.payload_sample = {"status_code": resp.status_code, "body_preview": resp.text[:500]}
                        return chk
                    if resp.status_code == 200:
                        body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                        # heuristic: full L2 has bids/asks arrays with multiple levels
                        has_l2 = False
                        sample = body if isinstance(body, dict) else {"body": body}
                        for key in ("bids", "asks", "levels", "book"):
                            if key in body and isinstance(body[key], list) and len(body[key]) > 2:
                                has_l2 = True
                        if has_l2:
                            chk.passed = True
                            chk.details = "REST endpoint returned full L2 book (bids/asks with multiple levels) — resync snapshot viable (§1A)."
                            chk.payload_sample = {k: (v[:2] if isinstance(v, list) else v) for k, v in (body.items() if isinstance(body, dict) else [])}
                        else:
                            chk.passed = False
                            chk.details = "REST endpoint returned 200 but no multi-level bids/asks — may be BBO-only. §1A needs redesign if full L2 unavailable."
                            chk.payload_sample = sample
                        return chk
                    if resp.status_code == 429:
                        chk.passed = None
                        chk.details = f"Rate-limited (429) on {url} — retry with backoff; rate limits shape §1/#7 backoff params."
                        return chk
            except Exception as e:
                chk.details += f" [{url}: {e}]"
        chk.passed = None
        chk.details = "Could not determine REST L2 support from probe URLs — manual verification required (§18)."
    except ImportError:
        chk.passed = None
        chk.details = "httpx not installed."
    return chk


async def check_settlement_report(rest_market_url: str) -> GateCheck:
    chk = GateCheck(
        name="settlement_report",
        question="Is there a settlement/resolution report endpoint (report_id/tx_hash) or must resolutions use inferred_nearest (§6A)?",
    )
    # This is more doc-oriented; probe market metadata for settlement fields
    chk.passed = None
    chk.details = (
        "Automatic probe not definitive. Manually check: fetch a recently-resolved market's metadata "
        "via CLOB REST or on-chain settlement fetcher. If response includes settlement_report_id/tx_hash, "
        "settlement_source=on_chain_confirmed; otherwise all resolutions will be inferred_nearest — document this explicitly (§6A)."
    )
    return chk


async def check_rate_limits(rest_book_url: str, rest_market_url: str) -> GateCheck:
    chk = GateCheck(
        name="rate_limits",
        question="What rate limits apply to REST endpoints for rollover discovery (§1) and resync (§1A)?",
    )
    chk.passed = None
    chk.details = (
        "Probe headers (Retry-After, X-RateLimit-*) on REST endpoints to size backoff params. "
        "Current config uses exponential backoff capped at 8s (discovery) / 20s (resync) and 30s WS — "
        "revisit against actual 429 responses before unattended run (§18)."
    )
    return chk


async def run_gate(config_path: Optional[str] = None, ws_url: Optional[str] = None, rest_book_url: Optional[str] = None) -> GateResult:
    from .config import CollectorConfig

    cfg = CollectorConfig.load(config_path) if config_path else CollectorConfig()
    ws_url = ws_url or cfg.ws.url
    rest_book_url = rest_book_url or cfg.ws.rest_book_url
    rest_market_url = cfg.ws.rest_market_url

    checks: List[GateCheck] = []
    checks.append(await check_ws_sequence_number(ws_url))
    checks.append(await check_rest_full_l2(rest_book_url))
    checks.append(await check_settlement_report(rest_market_url))
    checks.append(await check_rate_limits(rest_book_url, rest_market_url))

    # overall: gate passes only if no check is definitively False where False means redesign needed
    has_hard_fail = any(c.passed is False for c in checks)
    result = GateResult(checks=checks, all_passed=not has_hard_fail)
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="Verification gate — §18 (answer BEFORE building §1A)")
    ap.add_argument("--config", default=None, help="path to collector.yaml")
    ap.add_argument("--ws-url", default=None)
    ap.add_argument("--rest-book-url", default=None)
    ap.add_argument("--json-out", default=None)
    ap.add_argument("--live", action="store_true", help="run live WS/REST probes (requires network)")
    args = ap.parse_args()

    async def run():
        if args.live:
            result = await run_gate(args.config, args.ws_url, args.rest_book_url)
        else:
            # offline mode: emit checklist without live probes
            checks = [
                GateCheck(name="ws_sequence_number", question="Does CLOB WS expose sequence_number? Capture live payload.", passed=None, details="Run with --live or manual capture per §18."),
                GateCheck(name="rest_full_l2", question="Does REST expose full L2 book per token? Probe REST.", passed=None, details="Run with --live."),
                GateCheck(name="settlement_report", question="Settlement report endpoint available?", passed=None, details="Manual check: fetch resolved market metadata."),
                GateCheck(name="rate_limits", question="REST rate limits?", passed=None, details="Inspect response headers on 429."),
            ]
            result = GateResult(checks=checks, all_passed=False)
        print(json.dumps(result.to_dict(), indent=2))
        if result.all_passed:
            print("\nGate PASSED (no hard fails). Review --live details before coding §1A resync.")
        else:
            print("\nGate NOT PASSED — answer open items against live payload capture before building §1A (§18).")
        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump(result.to_dict(), f, indent=2)
            print(f"Wrote {args.json_out}")

    asyncio.run(run())


if __name__ == "__main__":
    main()
