"""Silent-except lint — Real-Data-Only §7 (audit 2026-09-21, expanded 2026-09-22).

WS/collect/write paths must log + collector_events, never swallow errors
with bare `except: pass/continue`. This test fails on NEW silent handlers,
not on the historical baseline — so existing code passes, but additions are blocked.

Baseline (2026-09-22, multiline-aware count of `except:` blocks whose body
is bare pass/continue within 3 lines):
  storage: compaction 19, export 98, parquet_writer 78, quarantine 2,
    clean_view 12, markets_log 14, cursor_store 7, raw_archive 14, streaming 11
  core: collector 213, resync 21, book 11, rollover 12
Burn-down: lower each ceiling over time; new code must log + events.
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
STORAGE = REPO / "src" / "polymarket_collector" / "storage"
CORE = REPO / "src" / "polymarket_collector"

# Per-file ceilings (current count + 0 headroom: any new silent handler fails).
BASELINE = {
    "compaction.py": 19,
    "export.py": 98,
    "parquet_writer.py": 78,
    "quarantine.py": 2,
    "clean_view.py": 12,
    "markets_log.py": 14,
    "cursor_store.py": 7,
    "raw_archive.py": 14,
    "streaming.py": 11,
}

CORE_BASELINE = {
    "collector.py": 213,
    "resync.py": 21,
    "book.py": 11,
    "rollover.py": 12,
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
    assert got <= 213, f"collector.py silent handlers grew: {got} > 213"


def test_core_silent_except_no_growth():
    """Core modules (resync/book/rollover): frozen, burn-down over time."""
    over = []
    for name, ceiling in CORE_BASELINE.items():
        if name == "collector.py":
            continue  # covered above
        p = CORE / name
        if not p.exists():
            continue
        got = _count_silent(p)
        if got > ceiling:
            over.append(f"{name}: {got} > baseline {ceiling} — log + collector_events instead of pass/continue")
    assert not over, "NEW silent except handlers in core/:\n" + "\n".join(over)
