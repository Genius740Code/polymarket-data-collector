"""Batched POST /books heal (perfect-collector checkbox 3, spec §3.3).

Replaces the per-token GET storm (2 GETs per book per heal tick — the
``fetch_none``/429 generator under multi-lane load) with one ``POST /books``
round-trip per token batch.

Ended-window precheck: a resolver callback over the live markets registry
answers (market_end_ts_ms, status) per condition_id; ended windows return a
supersede decision (skip the heal — their REST 404s forever) instead of
burning requests.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

BOOKS_BATCH_URL = "https://clob.polymarket.com/books"

# Tokens per POST: one round-trip heals a whole shard's books. Sized so the
# JSON body stays small (~100 × ~70B ids) while collapsing 2N GETs to ~1 POST.
BOOKS_BATCH_SIZE = 100

# Resolver: condition_id -> (market_end_ts_ms | None, status | None).
# None (unknown condition) means "treat as open" — discovery may lag, and an
# unknown market must never read as dead.
EndedMarketResolver = Callable[[str], Optional[Tuple[Optional[int], Optional[str]]]]

# HTTP POST hook: (url, payload) -> response-like with .status_code and .json().
# Injected so tests never touch the network; the collector passes its pooled
# httpx client call.
HttpPostFn = Callable[[str, List[Dict[str, str]]], Awaitable[Any]]

ENDED_STATUSES = frozenset({"closed", "resolved"})


# Shared 429 discipline across heal callers (perfect-collector checkbox 3,
# spec §3.3 task 4). Per-asset walkers must not each retry-blindly: one 429
# parks ALL batched-heal POSTs behind a jittered exponential backoff so N
# concurrent walkers burn 0 requests instead of N blind retries. Process-wide
# (the CLOB rate limit is per-IP, shared by every lane), bounded [0.5, 60]s
# like ResyncManager.note_rate_limited, total by construction (never raises).
_heal_429_until: float = 0.0
_heal_429_streak: int = 0
# Last 429 event (monotonic): the streak decays once the wire has been clean
# for _HEAL_429_CLEAN_S — a handful of lifetime 429s must never park heals
# behind U(30,60)s forever.
_heal_429_last_event: float = 0.0
_HEAL_429_CLEAN_S: float = 60.0

# Heal pacing (pmdata-parity audit): per-book minimum interval between
# first-bucket trigger starts, and a tighter per-book REST-attempt cap so a
# 5-minute fresh window cannot burn its whole REST budget in seconds.
HEAL_TRIGGER_MIN_INTERVAL_S: float = 30.0
HEAL_ATTEMPT_CAP_S: float = 5.0


def heal_backoff_remaining() -> float:
    """Seconds left on the shared batched-heal cooldown (0 when clear)."""
    try:
        return max(0.0, float(_heal_429_until) - time.monotonic())
    except Exception:
        return 0.0


def heal_rate_limited() -> bool:
    """True while the shared batched-heal cooldown is active (no POST burn)."""
    global _heal_429_streak
    try:
        now = time.monotonic()
    except Exception:
        return False
    try:
        if now < _heal_429_until:
            return True
    except Exception:
        return False
    # Decay: the wire has been clean past the cooldown — a stale lifetime
    # streak must not park future heals. Reset once, total.
    try:
        if int(_heal_429_streak or 0) > 0 and (now - float(_heal_429_last_event or 0.0)) > _HEAL_429_CLEAN_S:
            _heal_429_streak = 0
    except Exception:
        pass
    return False


def note_heal_success() -> None:
    """Record a 2xx on the heal path: the limit lifted, drop streak+cooldown.

    Any 2xx proves the shared IP limit is not currently tripped, so the
    exponential streak resets instead of growing forever. Total (never
    raises); worst case is a retained backoff.
    """
    global _heal_429_until, _heal_429_streak
    try:
        _heal_429_until = 0.0
        _heal_429_streak = 0
    except Exception:
        pass


def note_heal_rate_limited(retry_after_s: float = 1.0) -> float:
    """Record a 429 on the batched-heal path; returns the backoff applied.

    Jittered exponential: base = min(60, 2**streak), applied =
    uniform(0.5*base, base) clamped to [0.5, 60]. A huge Retry-After cannot
    park healing forever; jitter spreads the walkers' retry starts.
    """
    global _heal_429_until, _heal_429_streak, _heal_429_last_event
    try:
        _heal_429_streak = int(_heal_429_streak) + 1
    except Exception:
        _heal_429_streak = 1
    try:
        hint = float(retry_after_s if retry_after_s is not None else 1.0)
    except Exception:
        hint = 1.0
    try:
        base = max(1.0, min(60.0, hint if hint > 0 else 1.0))
        base = min(60.0, base * (2.0 ** max(0, _heal_429_streak - 1)))
        applied = random.uniform(0.5 * base, base)
        applied = max(0.5, min(60.0, float(applied)))
    except Exception:
        applied = 1.0
    try:
        _heal_429_until = time.monotonic() + applied
    except Exception:
        pass
    try:
        _heal_429_last_event = time.monotonic()
    except Exception:
        pass
    return applied


def reset_heal_rate_limit() -> None:
    """Clear the shared cooldown (test hook only — never called in prod)."""
    global _heal_429_until, _heal_429_streak, _heal_429_last_event
    try:
        _heal_429_until = 0.0
        _heal_429_streak = 0
        _heal_429_last_event = 0.0
    except Exception:
        pass


def classify_heal_failure(errors: Any) -> str:
    """Classify a failed heal round: rate_limited | genuine_empty | unknown.

    Only a genuine 200-empty/404 (token exposes no book) may feed a
    fetch_none streak. Timeouts, transport errors, bad bodies and non-404
    statuses are *unknown* — the book may be fine, REST just did not
    answer. A 200 whose body omits the token carries no error entry and is
    genuine-empty. Total (never raises); unparseable input is unknown.
    """
    try:
        errs = list(errors or [])
    except Exception:
        return "unknown"
    try:
        for e in errs:
            if str(e).startswith("rate_limited"):
                return "rate_limited"
    except Exception:
        return "unknown"
    try:
        if not errs:
            return "genuine_empty"
        if all(str(e).startswith("bad_status:404") for e in errs):
            return "genuine_empty"
    except Exception:
        return "unknown"
    return "unknown"


def should_heal_trigger(
    book_state_value: Any,
    condition_id: Any,
    last_starts: Any,
    now_monotonic: float,
    min_interval_s: float = HEAL_TRIGGER_MIN_INTERVAL_S,
) -> bool:
    """First-bucket heal gate: stale/resyncing books only, paced per book.

    Live one-sided books (0.999/NaN tops) must not fire a heal every tick —
    a live book is not a heal candidate. Stale/resyncing books re-fire at
    most once per min_interval_s. Pure read over last_starts (the caller
    records the start); total, never raises.
    """
    try:
        if str(book_state_value or "").lower() not in ("stale", "resyncing"):
            return False
    except Exception:
        return False
    try:
        last = float((last_starts or {}).get(str(condition_id), 0.0) or 0.0)
    except Exception:
        last = 0.0
    try:
        return (float(now_monotonic) - last) >= float(min_interval_s)
    except Exception:
        return True


def note_ws_book_content(resync_mgr: Any, condition_id: Any) -> None:
    """A real WS book-content frame clears REST suppression (total).

    Fresh WS data flowing for a book proves the venue exposes it — a
    fetch_none streak/quiet banked earlier is stale evidence and must not
    keep REST parked. Delegates to note_fetch_ok (clears streak + quiet);
    never raises; a missing manager is a no-op.
    """
    try:
        fn = getattr(resync_mgr, "note_fetch_ok", None)
        if callable(fn):
            fn(condition_id)
    except Exception:
        pass


def _retry_after_of(resp: Any, default_s: float = 1.0) -> float:
    """Read Retry-After off a response-like, bounded [0, 60] (never raises)."""
    try:
        headers = getattr(resp, "headers", None)
        if headers is not None:
            v = headers.get("retry-after") or headers.get("Retry-After")
            if v is not None:
                return max(0.0, min(60.0, float(str(v).strip().split(",")[0])))
    except Exception:
        pass
    return default_s


def chunk_tokens(token_ids: Sequence[str], batch_size: int = BOOKS_BATCH_SIZE) -> List[List[str]]:
    """Split token ids into batches, deduped (order-kept), falsy dropped."""
    seen = set()
    uniq: List[str] = []
    for t in token_ids or []:
        try:
            s = str(t).strip()
        except Exception:
            continue
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)
    try:
        n = max(1, int(batch_size))
    except Exception:
        n = BOOKS_BATCH_SIZE
    return [uniq[i:i + n] for i in range(0, len(uniq), n)]


def build_books_request(
    token_ids: Sequence[str], url: str = BOOKS_BATCH_URL
) -> Tuple[str, List[Dict[str, str]]]:
    """One batched request: (url, [{"token_id": t}, ...])."""
    batch = chunk_tokens(token_ids, batch_size=len(list(token_ids or [])) or 1)
    flat = batch[0] if batch else []
    return url, [{"token_id": t} for t in flat]


def parse_books_response(payload: Any) -> Dict[str, Dict[str, list]]:
    """Normalize a POST /books body to token_id -> {"bids", "asks"}.

    Accepts a list of per-token book dicts (``token_id``/``asset_id`` key) or
    a dict keyed by token id. Entries missing both sides are skipped (a
    half book must never promote — same both-outcomes gate as the GET heal
    path). Empty-but-present sides are kept: a genuine 200-empty book is
    data, not a transport error.
    """
    out: Dict[str, Dict[str, list]] = {}
    try:
        items: List[Any]
        if isinstance(payload, dict):
            items = []
            for tok, book in payload.items():
                if isinstance(book, dict):
                    b = dict(book)
                    b.setdefault("token_id", tok)
                    items.append(b)
                # non-dict values are not books — skipped, never synthesized
            if not items:
                return out
        elif isinstance(payload, list):
            items = payload
        else:
            return out
        for entry in items:
            if not isinstance(entry, dict):
                continue
            tok = entry.get("token_id") or entry.get("asset_id") or entry.get("asset")
            if not tok:
                continue
            if "bids" not in entry or "asks" not in entry:
                continue
            bids = entry.get("bids") or []
            asks = entry.get("asks") or []
            if not isinstance(bids, list) or not isinstance(asks, list):
                continue
            out[str(tok)] = {"bids": bids, "asks": asks}
    except Exception:
        pass
    return out


@dataclass(frozen=True)
class SupersededToken:
    token_id: str
    condition_id: Optional[str]
    reason: str  # ended_window | closed_status


@dataclass
class HealPlan:
    live_token_ids: List[str] = field(default_factory=list)
    superseded: List[SupersededToken] = field(default_factory=list)


def plan_heal(
    token_ids: Sequence[str],
    *,
    condition_by_token: Optional[Dict[str, Optional[str]]] = None,
    resolver: Optional[EndedMarketResolver] = None,
    now_ms: Optional[int] = None,
) -> HealPlan:
    """Ended-window precheck: split tokens into heal-now vs supersede.

    ``resolver(condition_id)`` returns ``(market_end_ts_ms, status)``; unknown
    conditions (resolver None / returns None) stay live — discovery may lag
    and unknown must never read as dead. Ended = end_ts in the past or
    status closed/resolved. Pure (no I/O) for testability.
    """
    try:
        now = int(now_ms) if now_ms is not None else int(time.time() * 1000)
    except Exception:
        now = int(time.time() * 1000)
    plan = HealPlan()
    seen: set = set()
    for raw in token_ids or []:
        try:
            tok = str(raw).strip()
        except Exception:
            continue
        if not tok or tok in seen:
            continue
        seen.add(tok)
        cid: Optional[str] = None
        try:
            if condition_by_token:
                v = condition_by_token.get(tok)
                cid = str(v) if v else None
        except Exception:
            cid = None
        decision: Optional[str] = None
        if resolver is not None and cid:
            try:
                res = resolver(cid)
            except Exception:
                res = None
            if res is not None:
                try:
                    end_ms, status = res
                except Exception:
                    end_ms, status = None, None
                try:
                    if end_ms is not None and int(end_ms) < now:
                        decision = "ended_window"
                except Exception:
                    pass
                if decision is None and status is not None:
                    try:
                        if str(status).lower() in ENDED_STATUSES:
                            decision = "closed_status"
                    except Exception:
                        pass
        if decision is not None:
            plan.superseded.append(SupersededToken(token_id=tok, condition_id=cid, reason=decision))
        else:
            plan.live_token_ids.append(tok)
    return plan


@dataclass
class HealResult:
    books: Dict[str, Dict[str, list]] = field(default_factory=dict)
    missing: List[str] = field(default_factory=list)
    superseded: List[SupersededToken] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


async def heal_books_batched(
    token_ids: Sequence[str],
    http_post: HttpPostFn,
    *,
    url: str = BOOKS_BATCH_URL,
    batch_size: int = BOOKS_BATCH_SIZE,
    condition_by_token: Optional[Dict[str, Optional[str]]] = None,
    resolver: Optional[EndedMarketResolver] = None,
    now_ms: Optional[int] = None,
) -> HealResult:
    """Heal many tokens in one POST /books round-trip per batch.

    Ended windows are superseded up front (no request burned). A shared
    429 cooldown parks every caller behind one jittered backoff (no blind
    per-walker retries). Transport failures land in ``errors`` with the
    tokens in ``missing`` — the caller owns retry-after-cooldown; this
    helper never spins, never synthesizes books, and never raises on wire
    errors.
    """
    result = HealResult()
    plan = plan_heal(
        token_ids, condition_by_token=condition_by_token, resolver=resolver, now_ms=now_ms
    )
    result.superseded = plan.superseded
    if not plan.live_token_ids:
        return result
    for chunk in chunk_tokens(plan.live_token_ids, batch_size=batch_size):
        # Shared 429 discipline: while ANY walker tripped the limit, burn
        # zero requests — the chunk stays missing (caller keeps stale with
        # its episode) instead of N blind retries deepening the limit.
        if heal_rate_limited():
            result.errors.append("rate_limited:shared-backoff")
            result.missing.extend(chunk)
            continue
        payload = [{"token_id": t} for t in chunk]
        try:
            resp = await http_post(url, payload)
        except Exception as e:
            result.errors.append(f"post_failed:{str(e)[:160]}")
            result.missing.extend(chunk)
            continue
        try:
            status = int(getattr(resp, "status_code", 200) or 200)
        except Exception:
            status = 200
        if status == 429:
            # Shared (not per-token): every concurrent walker sees the same
            # cooldown, so one 429 costs 1 backoff, not N retry storms.
            note_heal_rate_limited(_retry_after_of(resp))
            result.errors.append("rate_limited:429")
            result.missing.extend(chunk)
            continue
        if status != 200:
            result.errors.append(f"bad_status:{status}")
            result.missing.extend(chunk)
            continue
        # Any 2xx proves the shared limit lifted — drop streak+cooldown so
        # old 429s never park future heals (parse faults below still land
        # the tokens in missing, honestly unhealed).
        note_heal_success()
        try:
            body = resp.json() if hasattr(resp, "json") else resp
            if callable(body):
                body = body()
                if hasattr(body, "__await__"):
                    body = await body
        except Exception as e:
            result.errors.append(f"bad_body:{str(e)[:160]}")
            result.missing.extend(chunk)
            continue
        books = parse_books_response(body)
        for t in chunk:
            if t in books:
                result.books[t] = books[t]
            else:
                result.missing.append(t)
    return result
