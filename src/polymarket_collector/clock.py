"""Clock & timestamp handling — §14.

- Preserves ts_source vs ts_received_ns distinctly (never substitutes)
- NTP drift check with configurable threshold (default 50ms) → clock_issue
- Snapshot bucket alignment shared with book.py (§1A/§3)
"""
from __future__ import annotations

import datetime
import time
from typing import Optional

# Try ntplib if available, otherwise fallback to no-op
try:
    import ntplib  # type: ignore
    _HAS_NTPLIB = True
except ImportError:
    _HAS_NTPLIB = False


def now_ns() -> int:
    return time.time_ns()


def now_utc_iso() -> str:
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def ts_bucket_ms(unix_ms: int, interval_ms: int = 500) -> int:
    """Floor to shared wall-clock grid (UTC epoch aligned) — §1A redundancy."""
    return (unix_ms // interval_ms) * interval_ms


def check_clock_drift(ntp_server: str = "pool.ntp.org", timeout: int = 5) -> Optional[float]:
    """Return drift in milliseconds (positive = local ahead) or None if unavailable."""
    if not _HAS_NTPLIB:
        return None
    try:
        client = ntplib.NTPClient()
        resp = client.request(ntp_server, version=3, timeout=timeout)
        # offset = ntp time - local time
        drift_ms = float(resp.offset) * 1000.0
        return drift_ms
    except Exception:
        return None


def is_clock_issue(drift_ms: Optional[float], threshold_ms: int = 50) -> bool:
    if drift_ms is None:
        return False
    return abs(drift_ms) > threshold_ms
