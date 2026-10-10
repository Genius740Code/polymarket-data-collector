"""Skip-throttle keep-alive discipline for dataless-book ticks.

The BBO-empty skip site must emit ``book_empty_skipped`` ONLY on the
keep-alive tick (``_tick % 60 == 0``, ~1/30s per market) and skip silently
otherwise. Emitting on every non-keep-alive tick (~2/s/market) bloats the
never-pruned collector_events trail.

The tests below execute the REAL skip block, extracted verbatim from
``collector.py`` source and driven tick-by-tick with dataless rows, so any
inversion of the throttle gate fails here. Stubs only — no network, no
writes, no market data; every skipped row stays an honest skip.
"""
import pathlib
import textwrap
import types
from collections import defaultdict


def _load_skip_program():
    src = (
        pathlib.Path(__file__).resolve().parent.parent
        / "src"
        / "polymarket_collector"
        / "collector.py"
    ).read_text(encoding="utf-8").splitlines()
    start = next(
        i for i, line in enumerate(src) if '_bbo_empty = (row.get("up_bid")' in line
    )
    assert src[start - 1].strip() == "try:"
    start -= 1
    end = next(
        i
        for i, line in enumerate(src)
        if 'result = self.writer.append("book_snapshots_500ms"' in line
    )
    block = textwrap.dedent("\n".join(src[start:end]))
    program = (
        "for _tick in _ticks:\n"
        + textwrap.indent(block, "    ")
        + "\n    _appended.append(_tick)\n"
    )
    compile(program, "<skip_block>", "exec")
    return program


class _Self:
    def __init__(self):
        self._ws_noise_throttle = defaultdict(int)
        self.events = []

    def _collector_event(self, event_type, details):
        self.events.append((event_type, dict(details)))


def _run_ticks(program, ticks):
    """Drive the real block tick by tick with a shared throttle (continuity).

    Returns (events_by_tick, appended): ``continue`` skips the row while
    fall-through appends it, exactly as in the snapshot loop.
    """
    self_obj = _Self()
    appended = []
    events_by_tick = {}
    for tick in ticks:
        before = len(self_obj.events)
        namespace = {
            "_ticks": [tick],
            "_appended": appended,
            "row": {
                "up_bid": None,
                "up_ask": None,
                "down_bid": None,
                "down_ask": None,
                "book_state": "stale",
            },
            "m": types.SimpleNamespace(asset="BTC", condition_id="cid-skip"),
            "self": self_obj,
        }
        exec(program, namespace)
        events_by_tick[tick] = self_obj.events[before:]
    return events_by_tick, appended


def test_skip_emits_only_on_keep_alive_tick():
    program = _load_skip_program()
    ticks = list(range(1, 121))  # two full 60-tick windows
    events_by_tick, appended = _run_ticks(program, ticks)

    emitting = [t for t in ticks if events_by_tick[t]]
    assert emitting == [60, 120]  # <=1 per 60 ticks, on the keep-alive only
    for tick in ticks:
        for event_type, details in events_by_tick[tick]:
            assert event_type == "book_empty_skipped"
            assert details["condition_id"] == "cid-skip"
    # Keep-alive rows are still appended (the honest gap trail); every
    # other tick skips silently.
    assert appended == [60, 120]


def test_silent_ticks_emit_nothing():
    program = _load_skip_program()
    events_by_tick, appended = _run_ticks(program, range(1, 60))
    assert all(events == [] for events in events_by_tick.values())
    assert appended == []
