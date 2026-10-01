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

    Ended windows are superseded up front (no request burned). Transport
    failures land in ``errors`` with the tokens in ``missing`` — the caller
    owns backoff/retry; this helper never spins, never synthesizes books,
    and never raises on wire errors.
    """
    result = HealResult()
    plan = plan_heal(
        token_ids, condition_by_token=condition_by_token, resolver=resolver, now_ms=now_ms
    )
    result.superseded = plan.superseded
    if not plan.live_token_ids:
        return result
    for chunk in chunk_tokens(plan.live_token_ids, batch_size=batch_size):
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
            result.errors.append("rate_limited:429")
            result.missing.extend(chunk)
            continue
        if status != 200:
            result.errors.append(f"bad_status:{status}")
            result.missing.extend(chunk)
            continue
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
