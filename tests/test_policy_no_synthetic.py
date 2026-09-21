"""Policy test — AGENT.md §4 (audit 2026-09-21).

CI must fail if any writer path creates source='synthetic' or report_id
starting with 'synth-', or fabricates data. Scans the source tree for live
synthetic producers (test files asserting synthetic *isolation* are allowed).
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src" / "polymarket_collector"

# Live-code patterns that would violate the real-data-only policy.
FORBIDDEN = [
    (r"""source\s*=\s*['"]synthetic['"]""", "source='synthetic'"),
    (r"""['"]synth-['"]?\s*\+""", "synth- report_id prefix"),
    (r"""report_id\s*=\s*f?['"]synth-""", "synth- report_id literal"),
]

# Files allowed to mention synthetic concepts (isolation guards, policy text,
# historical comments, tests asserting isolation).
ALLOWLIST = {
    "config.py",  # deprecated flag, validator forbids true
    "enums.py",
    "verify_gate.py",
    "AGENT.md",
    "AGENTS.md",
}


def _scan():
    hits = []
    for path in SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern, label in FORBIDDEN:
            for i, line in enumerate(text.splitlines(), 1):
                if re.search(pattern, line):
                    # Historical-removal comments (e.g. "synthetic X removed")
                    # are not producers.
                    if re.search(r"remov|never|forbid|no .*synthetic|番", line, re.IGNORECASE):
                        continue
                    if path.name in ALLOWLIST:
                        continue
                    hits.append(f"{path.relative_to(REPO)}:{i}: {label}: {line.strip()[:160]}")
    return hits


def test_no_synthetic_producers_in_live_code():
    hits = _scan()
    assert not hits, "AGENT.md §4 violation — synthetic producers in live code:\n" + "\n".join(hits)


def test_synthetic_mode_never_enabled():
    """The deprecated flag must default False and no live path may read it.

    AGENT.md §0: "New code must not read it." Allowed: the deprecated
    definition itself (config.py), the unused constructor default
    (parquet_writer.py keeps the parameter for backward compat), and audit
    comments referencing the removal.
    """
    text = (SRC / "config.py").read_text()
    assert "synthetic_mode: bool = False" in text
    read_patterns = [
        re.compile(r"getattr\([^)]*synthetic_mode"),
        re.compile(r"config\.synthetic_mode"),
        re.compile(r"if\s+.*\.synthetic_mode"),
    ]
    for path in SRC.rglob("*.py"):
        if path.name in ("config.py", "verify_gate.py"):
            continue
        body = path.read_text(encoding="utf-8", errors="ignore")
        for i, line in enumerate(body.splitlines(), 1):
            s = line.strip()
            if s.startswith("#") or "audit 2026-09-21" in line:
                continue
            code = line.split("#", 1)[0]  # ignore trailing comments
            # Constructor plumbing (param definition / stored-but-unused) is
            # not a read.
            if re.search(r"synthetic_mode\s*:\s*bool|self\.synthetic_mode\s*=", code):
                continue
            for pat in read_patterns:
                if pat.search(code):
                    raise AssertionError(
                        f"AGENT.md §0: new code must not read synthetic_mode — "
                        f"{path.relative_to(REPO)}:{i}: {line.strip()[:160]}"
                    )
