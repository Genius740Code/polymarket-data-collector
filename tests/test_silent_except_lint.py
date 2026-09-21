"""Silent-except lint — Real-Data-Only §7 (audit 2026-09-21).

WS/collect/write paths must log + collector_events, never swallow errors
with bare `except: pass/continue`. This test fails on NEW silent handlers
in storage/ (burn-down: compaction + writer first), not on the historical
baseline — so existing code passes, but additions are blocked.

Baseline (2026-09-21, multiline-aware count of `except:` blocks whose body
is bare pass/continue within 3 lines):
  compaction.py: 19, export.py: 99, parquet_writer.py: 78
(resync.py lives at src/polymarket_collector/resync.py; collector.py WS
loops tracked separately — see test_ws_silent_except below.)
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
STORAGE = REPO / "src" / "polymarket_collector" / "storage"

# Per-file ceilings (current count + 0 headroom: any new silent handler fails).
BASELINE = {
    "compaction.py": 19,
    "export.py": 96,
    "parquet_writer.py": 78,
    "quarantine.py": 2,
}


def _count_silent(path: Path) -> int:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    n = 0
    for i, line in enumerate(lines):
        if re.match(r"\s*except\b.*:", line):
            for j in range(i + 1, min(i + 4, len(lines))):
                s = lines[j].strip()
                if s.startswith("pass") or s.startswith("continue"):
                    n += 1
                    break
                if s == "" or s.startswith("#"):
                    continue
                break
    return n


def test_storage_silent_except_no_growth():
    over = []
    for name, ceiling in BASELINE.items():
        p = STORAGE / name
        if not p.exists():
            continue
        got = _count_silent(p)
        if got > ceiling:
            over.append(f"{name}: {got} > baseline {ceiling} — log + collector_events instead of pass/continue")
    assert not over, "NEW silent except handlers in storage/:\n" + "\n".join(over)


def test_ws_silent_except_no_growth():
    """Collector WS loops: ceiling only, burn-down tracked separately."""
    p = REPO / "src" / "polymarket_collector" / "collector.py"
    if not p.exists():
        return
    got = _count_silent(p)
    assert got <= 194, f"collector.py silent handlers grew: {got} > 194"
