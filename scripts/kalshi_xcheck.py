#!/usr/bin/env python3
"""Kalshi KXBTC15M cross-check of our BTC-15m settlement labels (report-only).

External ground truth: Kalshi's public trade-api v2 needs no auth for market
data. KXBTC15M is "BTC price up in next 15 mins?" -- quarter-hour UTC windows,
the same window shape as our Polymarket btc-updown-15m series in
data/markets_log (event-sourced parquet, scanned strictly read-only).

This script NEVER modifies our labels or any data/ file; it only measures.

Passes:
  primary       the 20 most recent settled KXBTC15M markets vs our log
  supplementary our resolved BTC-15m windows vs Kalshi settled history for
                the same window range (windows that would fall inside the
                primary pass are excluded here to avoid double counting).
                As of 2026-10-11 our 15m lane last wrote 2026-10-02, so the
                primary pass has no our-side overlap; this pass supplies the
                measurable overlap.

Alignment rule (strict): a Kalshi market pairs with one of our windows iff
the UTC window boundaries are exactly equal -- kalshi open_time == our
market_start AND kalshi close_time == our market_end (epoch seconds). Both
venues align 15-minute windows to quarter-hour UTC boundaries, so exact
equality is expected. Zero tolerance, no gap filling, no substituted values;
anything else is unmatchable and the reason is recorded.

Agreement rule: (kalshi result == "yes") == (our resolution_outcome == "up").
Note Kalshi resolves ties as yes (strike_type greater_or_equal); a tie on our
side would surface as a disagreement finding, not an error to smooth.

Budget: 10s timeout per request, >=1s sleep between requests, hard cap of 60
HTTP requests per run. Raw API responses are saved verbatim.

Outputs (paperlog/kalshi_xcheck/): raw/*.json verbatim responses, summary.json.
Exit 0 when the measurement completes (disagreements are findings, not
failures); exit 1 only on infrastructure failure (API unreachable, log
unreadable, budget exhausted).
"""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES = "KXBTC15M"
OUR_SERIES = "BTC-15m"
N_RECENT = 20
PAGE_LIMIT = 50
TIMEOUT_S = 10
SLEEP_S = 1.0
MAX_REQUESTS = 60
OUT_DIR = REPO / "paperlog" / "kalshi_xcheck"
RAW_DIR = OUT_DIR / "raw"
LOG_GLOB = "data/markets_log/date=*/"
LOG_COLS = [
    "updated_at",
    "condition_id",
    "slug",
    "series_id",
    "market_start_ts_ms",
    "market_end_ts_ms",
    "status",
    "resolution_outcome",
]
UP_DOWN = ("up", "down")
UTC = dt.timezone.utc


def iso_utc(epoch_seconds: int | None) -> str | None:
    if epoch_seconds is None:
        return None
    return dt.datetime.fromtimestamp(epoch_seconds, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_kalshi_ts(value: str | None) -> int | None:
    if not value:
        return None
    return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


class KalshiClient:
    """Public market-data client with request accounting and raw capture."""

    def __init__(self, raw_dir: Path) -> None:
        self.raw_dir = raw_dir
        self.requests = 0
        self._last = 0.0
        self._seq = 0

    def get(self, path: str, params: dict[str, str] | None = None, name: str = "fetch") -> dict:
        for attempt in (1, 2):
            if self.requests >= MAX_REQUESTS:
                raise RuntimeError(f"request budget exhausted ({MAX_REQUESTS})")
            gap = time.monotonic() - self._last
            if self.requests and gap < SLEEP_S:
                time.sleep(SLEEP_S - gap)
            url = API_BASE + path
            if params:
                url += "?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(
                url, headers={"Accept": "application/json", "User-Agent": "pmdata-parity-kalshi-xcheck/1.0"}
            )
            self.requests += 1
            self._last = time.monotonic()
            try:
                with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
                    body = resp.read()
                break
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt == 2:
                    raise RuntimeError(f"GET {url} failed after retry: {exc}") from exc
                print(f"  retrying after error: {exc}", file=sys.stderr, flush=True)
                time.sleep(2.0)
        self._seq += 1
        (self.raw_dir / f"{self._seq:02d}_{name}.json").write_bytes(body)
        return json.loads(body)


def scan_our_log() -> tuple[dict[str, dict], int, int]:
    """Read-only scan of data/markets_log; returns merged latest state per condition_id.

    Merge rule: latest event per condition_id by updated_at (ISO8601, lexicographically
    sortable) carries status/resolution_outcome; static window bounds come from the first
    event that carries them.
    """
    files = sorted(REPO.glob(LOG_GLOB + "*.parquet"))
    rows: list[dict] = []
    errors = 0

    def read_one(path: Path) -> list[dict] | None:
        try:
            pf = pq.ParquetFile(path)
            cols = [c for c in LOG_COLS if c in pf.schema_arrow.names]
            table = pf.read(columns=cols)
            out = []
            for i, series in enumerate(table.column("series_id").to_pylist()):
                if series == OUR_SERIES:
                    out.append({c: table.column(c)[i].as_py() for c in cols})
            return out
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        for result in pool.map(read_one, files):
            if result is None:
                errors += 1
            else:
                rows.extend(result)

    merged: dict[str, dict] = {}
    for row in rows:
        state = merged.setdefault(row["condition_id"], {"events": 0})
        state["events"] += 1
        state["condition_id"] = row["condition_id"]
        for key in ("status", "resolution_outcome", "slug", "updated_at"):
            if key in row:
                prev = state.get("last_updated_at", "")
                if row["updated_at"] and row["updated_at"] >= prev:
                    state[key] = row[key]
                state["last_updated_at"] = max(prev, row["updated_at"] or "")
        for key in ("market_start_ts_ms", "market_end_ts_ms"):
            if row.get(key) is not None and state.get(key) is None:
                state[key] = row[key]
    return merged, len(files), errors


def kalshi_market_rec(m: dict) -> dict:
    return {
        "ticker": m.get("ticker"),
        "open_s": parse_kalshi_ts(m.get("open_time")),
        "close_s": parse_kalshi_ts(m.get("close_time")),
        "result": m.get("result"),
        "strike": m.get("floor_strike"),
        "expiration_value": m.get("expiration_value"),
        "settlement_ts": m.get("settlement_ts"),
        "status": m.get("status"),
        "title": m.get("title"),
    }


def compare(kalshi: dict, ours: dict) -> dict:
    k_up = kalshi["result"] == "yes"
    o_up = ours["resolution_outcome"] == "up"
    pair = {
        "window_start_utc": iso_utc(kalshi["open_s"]),
        "window_end_utc": iso_utc(kalshi["close_s"]),
        "kalshi_ticker": kalshi["ticker"],
        "kalshi_result": kalshi["result"],
        "kalshi_strike": kalshi["strike"],
        "kalshi_expiration_value": kalshi["expiration_value"],
        "kalshi_settlement_ts": kalshi["settlement_ts"],
        "our_condition_id": ours["condition_id"],
        "our_slug": ours.get("slug"),
        "our_outcome": ours["resolution_outcome"],
        "our_status": ours.get("status"),
        "verdict": "agreement" if k_up == o_up else "disagreement",
    }
    if kalshi["strike"] is not None and kalshi["expiration_value"] is not None:
        try:
            if float(kalshi["strike"]) == float(kalshi["expiration_value"]):
                pair["note"] = "kalshi end price equals strike (>= tie rule); tie-rule differences are findings"
        except (TypeError, ValueError):
            pass
    return pair


def coverage_note(ours_by_window: dict[tuple[int, int], dict]) -> str:
    if not ours_by_window:
        return "our markets_log has no BTC-15m windows at all"
    starts = [k[0] for k in ours_by_window]
    return (
        f"our BTC-15m coverage: {iso_utc(min(starts))}..{iso_utc(max(k[1] for k in ours_by_window))} UTC "
        f"({len(ours_by_window)} windows)"
    )


def main() -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[1/4] scanning data/markets_log for series {OUR_SERIES} (read-only) ...", flush=True)
    merged, files_scanned, read_errors = scan_our_log()
    ours_by_window: dict[tuple[int, int], dict] = {}
    for state in merged.values():
        start_ms = state.get("market_start_ts_ms")
        end_ms = state.get("market_end_ts_ms")
        if start_ms is None or end_ms is None:
            continue
        key = (start_ms // 1000, end_ms // 1000)
        if key in ours_by_window:
            prev = ours_by_window[key]
            prev["_ambiguous"] = True
            state["_ambiguous"] = True
        else:
            ours_by_window[key] = state
    print(
        f"      scanned {files_scanned} files ({read_errors} unreadable), "
        f"{len(merged)} conditions, {len(ours_by_window)} distinct windows",
        flush=True,
    )

    client = KalshiClient(RAW_DIR)
    print(f"[2/4] fetching {N_RECENT} most recent settled Kalshi {SERIES} markets ...", flush=True)
    listing = client.get(
        "/markets", {"series_ticker": SERIES, "status": "settled", "limit": str(PAGE_LIMIT)}, "markets_recent"
    )
    markets = {m["ticker"]: m for m in listing.get("markets", []) if "ticker" in m}
    while listing.get("cursor") and len(markets) < max(N_RECENT, 30):
        listing = client.get(
            "/markets",
            {"series_ticker": SERIES, "status": "settled", "limit": str(PAGE_LIMIT), "cursor": listing["cursor"]},
            "markets_recent_next",
        )
        markets.update({m["ticker"]: m for m in listing.get("markets", []) if "ticker" in m})
    settled = [kalshi_market_rec(m) for m in markets.values() if m.get("result") in ("yes", "no")]
    settled.sort(key=lambda r: (r["close_s"] or 0), reverse=True)
    recent = settled[:N_RECENT]
    print(f"      {len(recent)} settled markets selected (close_time desc, local sort)", flush=True)

    print("[3/4] fetching per-market Kalshi records ...", flush=True)
    for rec in recent:
        detail = client.get(f"/markets/{rec['ticker']}", name="market_" + rec["ticker"])
        got = detail.get("market") or detail
        if got.get("ticker"):
            rec.update(kalshi_market_rec(got))

    primary_pairs: list[dict] = []
    primary_unmatchable: list[dict] = []
    cov = coverage_note(ours_by_window)
    for rec in recent:
        key = (rec["open_s"], rec["close_s"])
        ours = ours_by_window.get(key)
        if ours is None:
            primary_unmatchable.append(
                {
                    "kalshi_ticker": rec["ticker"],
                    "window_start_utc": iso_utc(rec["open_s"]),
                    "window_end_utc": iso_utc(rec["close_s"]),
                    "reason": f"no {OUR_SERIES} window with identical start/end in our markets_log; {cov}",
                }
            )
        elif ours.get("_ambiguous"):
            primary_unmatchable.append(
                {
                    "kalshi_ticker": rec["ticker"],
                    "window_start_utc": iso_utc(rec["open_s"]),
                    "window_end_utc": iso_utc(rec["close_s"]),
                    "reason": "multiple our-side condition_ids map to this window",
                }
            )
        elif ours["resolution_outcome"] not in UP_DOWN:
            primary_unmatchable.append(
                {
                    "kalshi_ticker": rec["ticker"],
                    "window_start_utc": iso_utc(rec["open_s"]),
                    "window_end_utc": iso_utc(rec["close_s"]),
                    "reason": f"our outcome not determined (status={ours.get('status')}, outcome={ours['resolution_outcome']})",
                }
            )
        elif rec["result"] not in ("yes", "no"):
            primary_unmatchable.append(
                {
                    "kalshi_ticker": rec["ticker"],
                    "window_start_utc": iso_utc(rec["open_s"]),
                    "window_end_utc": iso_utc(rec["close_s"]),
                    "reason": f"kalshi result not yes/no (result={rec['result']})",
                }
            )
        else:
            primary_pairs.append(compare(rec, ours))
    primary = {
        "description": f"the {N_RECENT} most recent settled Kalshi {SERIES} markets vs our markets_log",
        "kalshi_markets": len(recent),
        "window_range_utc": [iso_utc(recent[-1]["open_s"]), iso_utc(recent[0]["close_s"])] if recent else None,
        "agreement": sum(p["verdict"] == "agreement" for p in primary_pairs),
        "disagreement": sum(p["verdict"] == "disagreement" for p in primary_pairs),
        "unmatchable": len(primary_unmatchable),
        "pairs": primary_pairs,
        "unmatchable_details": primary_unmatchable,
    }

    print("[4/4] supplementary pass: our resolved windows vs Kalshi history ...", flush=True)
    if recent:
        min_primary_close = min(rec["close_s"] for rec in recent)
    else:
        min_primary_close = None
    supp_sources = {
        key: state
        for key, state in ours_by_window.items()
        if state["resolution_outcome"] in UP_DOWN
        and (min_primary_close is None or key[1] < min_primary_close)
        and not state.get("_ambiguous")
    }
    supp_pairs: list[dict] = []
    supp_unmatchable: list[dict] = []
    kalshi_in_range: dict[tuple[int, int], dict] = {}
    if supp_sources:
        closes = [key[1] for key in supp_sources]
        history = client.get(
            "/markets",
            {
                "series_ticker": SERIES,
                "status": "settled",
                "min_close_ts": str(min(closes)),
                "max_close_ts": str(max(closes)),
                "limit": "200",
            },
            "markets_history",
        )
        for m in history.get("markets", []):
            rec = kalshi_market_rec(m)
            if rec["open_s"] is None or rec["close_s"] is None:
                continue
            key = (rec["open_s"], rec["close_s"])
            if key in kalshi_in_range:
                kalshi_in_range[key].setdefault("ambiguous_tickers", []).append(rec["ticker"])
            else:
                kalshi_in_range[key] = rec
        for key in sorted(supp_sources):
            ours = supp_sources[key]
            rec = kalshi_in_range.get(key)
            if rec is None:
                supp_unmatchable.append(
                    {
                        "our_condition_id": ours["condition_id"],
                        "our_slug": ours.get("slug"),
                        "window_start_utc": iso_utc(key[0]),
                        "window_end_utc": iso_utc(key[1]),
                        "reason": "no settled Kalshi market with identical window boundaries in fetched range",
                    }
                )
            elif rec.get("ambiguous_tickers"):
                supp_unmatchable.append(
                    {
                        "our_condition_id": ours["condition_id"],
                        "our_slug": ours.get("slug"),
                        "window_start_utc": iso_utc(key[0]),
                        "window_end_utc": iso_utc(key[1]),
                        "reason": f"multiple Kalshi markets share this window ({rec['ticker']}, {rec['ambiguous_tickers']})",
                    }
                )
            elif rec["result"] not in ("yes", "no"):
                supp_unmatchable.append(
                    {
                        "our_condition_id": ours["condition_id"],
                        "our_slug": ours.get("slug"),
                        "window_start_utc": iso_utc(key[0]),
                        "window_end_utc": iso_utc(key[1]),
                        "reason": f"kalshi result not yes/no (result={rec['result']})",
                    }
                )
            else:
                supp_pairs.append(compare(rec, ours))
    supplementary = {
        "description": "our resolved BTC-15m windows vs Kalshi settled history for the same window range",
        "our_windows": len(supp_sources),
        "kalshi_windows_fetched_in_range": len(kalshi_in_range),
        "agreement": sum(p["verdict"] == "agreement" for p in supp_pairs),
        "disagreement": sum(p["verdict"] == "disagreement" for p in supp_pairs),
        "unmatchable": len(supp_unmatchable),
        "pairs": supp_pairs,
        "unmatchable_details": supp_unmatchable,
    }

    summary = {
        "script": "scripts/kalshi_xcheck.py",
        "generated_at_utc": dt.datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "purpose": "external ground truth for our BTC-15m settlement labels; report-only, labels never adjusted",
        "kalshi": {
            "base_url": API_BASE,
            "series_ticker": SERIES,
            "status_filter": "settled",
            "requests_made": client.requests,
            "request_budget": MAX_REQUESTS,
        },
        "our_side": {
            "source": "data/markets_log (event-sourced parquet; read-only scan)",
            "series_id": OUR_SERIES,
            "files_scanned": files_scanned,
            "read_errors": read_errors,
            "condition_ids": len(merged),
            "distinct_windows": len(ours_by_window),
            "merge_rule": "latest event per condition_id by updated_at; window bounds from first event carrying them",
            "coverage_note": cov,
        },
        "alignment_rule": (
            "exact UTC window boundary equality: kalshi open_time == our market_start AND "
            "kalshi close_time == our market_end (epoch seconds); both venues align 15m windows "
            "to quarter-hour UTC; zero tolerance, no gap filling, no substituted values"
        ),
        "agreement_rule": "(kalshi result == 'yes') == (our resolution_outcome == 'up')",
        "primary": primary,
        "supplementary": supplementary,
        "notes": [
            "unmatchable pairs are reported with reasons; our labels are never modified",
            "primary pass counts from the Kalshi side; supplementary counts per our-window",
            f"{cov}; the 15m lane was not writing on the dates of the {N_RECENT} most recent Kalshi markets",
            "windows that fall inside the primary range are excluded from the supplementary pass",
        ],
    }
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    print(f"\nprimary (20 most recent settled {SERIES}):")
    print(
        f"  agreement={primary['agreement']} disagreement={primary['disagreement']} "
        f"unmatchable={primary['unmatchable']} (of {len(recent)} kalshi markets)"
    )
    for p in primary_pairs:
        if p["verdict"] == "disagreement":
            print(f"  DISAGREEMENT {p['window_start_utc']}..{p['window_end_utc']} kalshi={p['kalshi_result']} ours={p['our_outcome']}")
    print("supplementary (our windows vs Kalshi history):")
    print(
        f"  our_windows={supplementary['our_windows']} agreement={supplementary['agreement']} "
        f"disagreement={supplementary['disagreement']} unmatchable={supplementary['unmatchable']}"
    )
    for p in supp_pairs:
        if p["verdict"] == "disagreement":
            print(
                f"  DISAGREEMENT {p['window_start_utc']}..{p['window_end_utc']} "
                f"kalshi={p['kalshi_result']} (strike={p['kalshi_strike']}, end={p['kalshi_expiration_value']}) ours={p['our_outcome']} slug={p['our_slug']}"
            )
    print(f"requests: {client.requests}/{MAX_REQUESTS}; summary: {OUT_DIR / 'summary.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
