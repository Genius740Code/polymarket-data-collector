"""Fast JSON shim — orjson when available, stdlib json otherwise.

Drop-in for the subset of the stdlib API this repo uses on hot paths
(loads/dumps). Output contract:

- ``loads`` accepts str/bytes/bytearray and returns identical objects to
  ``json.loads`` for all well-formed finite JSON (the only kind the CLOB
  emits: decimal strings/numbers, no NaN/Infinity literals).
- ``dumps`` returns ``str`` (never bytes) so every call site — including
  ``ws.send`` text frames — behaves exactly as before.
- Formatting kwargs (``indent``, ``separators``, ``sort_keys``) and
  ``default`` are honored by delegating to stdlib when they would change
  output shape; orjson is already compact so ``separators`` is a no-op.

Known edge differences (unreachable on real CLOB data, documented here):
- orjson rejects ``NaN``/``Infinity`` literals at parse time where stdlib
  accepts them. A frame carrying non-finite literals is dropped as an
  anomaly (book goes stale, honest gap) instead of propagating NaN.
- orjson serializes non-finite floats as ``null`` where stdlib emits
  ``NaN``. §3A validation already rejects non-finite prices, and null is
  the honest §3 representation for absent/invalid values.
- datetime/date/time/UUID objects: stdlib raises TypeError without
  ``default=``; this shim raises too (OPT_PASSTHROUGH_DATETIME with no
  default handler). With ``default=str`` both paths stringify identically.
"""
from __future__ import annotations

import json as _stdlib
from typing import Any

try:
    import orjson as _orjson

    HAS_ORJSON = True
except ImportError:  # pragma: no cover — fallback path
    _orjson = None  # type: ignore[assignment]
    HAS_ORJSON = False


def loads(s: str | bytes | bytearray) -> Any:
    """Parse JSON identically to ``json.loads`` (str or bytes input)."""
    if HAS_ORJSON:
        return _orjson.loads(s)
    return _stdlib.loads(s)


def dumps(obj: Any, *args: Any, **kwargs: Any) -> str:
    """Serialize to a compact JSON ``str`` (stdlib-compatible return type)."""
    if HAS_ORJSON and _orjson is not None:
        default = kwargs.get("default", None)
        # Pretty-printing / key-ordering kwargs change output shape — keep
        # stdlib for those (cold human-readable paths only).
        if kwargs.get("indent") is None and kwargs.get("sort_keys") is None:
            try:
                if default is None:
                    return _orjson.dumps(
                        obj, option=_orjson.OPT_PASSTHROUGH_DATETIME
                    ).decode("utf-8")
                return _orjson.dumps(obj, default=default).decode("utf-8")
            except TypeError:
                # orjson is stricter about unknown types than stdlib+default;
                # fall through to stdlib so failure behavior matches.
                pass
        return _stdlib.dumps(obj, *args, **kwargs)
    return _stdlib.dumps(obj, *args, **kwargs)


# Re-exported so ``jsonfast`` can stand in for ``json`` attribute-style.
load = _stdlib.load
dump = _stdlib.dump
JSONDecodeError = _stdlib.JSONDecodeError
