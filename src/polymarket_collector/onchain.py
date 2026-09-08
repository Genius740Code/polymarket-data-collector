"""C2: on-chain maker/taker backfill from Polymarket CTF Exchange OrderFilled logs.

The Data-API both-legs attribution (export._backfill_trade_wallets) leaves
maker_wallet NULL (~78% of published rows) whenever the maker leg is not
indexed or is ambiguous. The chain itself names both sides on every fill via
``OrderFilled`` (maker/taker indexed topics) on the CTF Exchange contracts —
no API key needed, plain ``eth_getLogs`` on a public Polygon RPC.

Event ground truth (verified 2026-09-08):
- V1 (0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E): OrderFilled(bytes32,
  address,address,uint256,uint256,uint256,uint256,uint256) — signature from
  the official archived repo (ITrading.sol).
- V2 (0xE111180000d2663C0091e4f400237545B87B996B, current): OrderFilled(
  bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,
  bytes32) — signature from the official ctf-exchange-v2 repo (ITrading.sol).
  Topic0 verified live against 174 real logs in one recent block.
- Both versions index maker as topic2 and taker as topic3, so decoding is
  identical: only topic0 differs.

Honesty rule (same as the Data-API path): a tx naming several DISTINCT
makers (multi-fill) attributes NONE — NULL is kept, never guessed.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

# CTF Exchange contracts on Polygon mainnet (chain id 137) — Polymarket docs
# "Contract Addresses" page (single source of truth).
CTF_EXCHANGE_V1 = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
CTF_EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"

# topic0 = keccak256 of the canonical event signatures above (computed with
# standard keccak256; V2 topic verified live 2026-09-08).
ORDERFILLED_V1_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
ORDERFILLED_V2_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"

DEFAULT_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
# Max distinct txs per third-pass run (newest first; the rest ride the next
# 15-min cron). Bounds RPC time to a few minutes on the free tier.


def _topic_addr(topic: str) -> Optional[str]:
    """Last 20 bytes of a topic as a checksummed-lower address string."""
    try:
        t = topic.lower().removeprefix("0x")
        return ("0x" + t[-40:]).lower()
    except Exception:
        return None


def _u256_word(data: str, slot: int) -> Optional[int]:
    """Decode one 32-byte ABI word as int (data with 0x prefix)."""
    try:
        h = data.lower().removeprefix("0x")
        return int(h[slot * 64:(slot + 1) * 64], 16)
    except Exception:
        return None


def parse_order_filled_fills(logs: List[dict]) -> List[dict]:
    """Decode OrderFilled logs to per-FILL records:
    {tx_hash, token_id (decimal str) or None, maker_asset_id/taker_asset_id
    (V1 asset ids) or None, maker, taker, side or None}.

    V2 data layout: (Side side, tokenId, makerAmountFilled, takerAmountFilled,
    fee, builder, metadata). V1: (makerAssetId, takerAssetId,
    makerAmountFilled, takerAmountFilled, fee) — a fill matches a row when the
    row's token_id equals the V2 tokenId or either V1 asset id. Pure function.
    """
    fills: List[dict] = []
    for log in logs or []:
        try:
            topics = log.get("topics") or []
            if len(topics) < 4:
                continue
            topic0 = topics[0].lower()
            if topic0 not in (ORDERFILLED_V1_TOPIC, ORDERFILLED_V2_TOPIC):
                continue
            txh = str(log.get("transactionHash") or "").lower()
            if not txh:
                continue
            data = log.get("data") or ""
            rec = {"tx_hash": txh, "token_id": None, "maker_asset_id": None,
                   "taker_asset_id": None, "maker": _topic_addr(topics[2]),
                   "taker": _topic_addr(topics[3]), "side": None}
            if topic0 == ORDERFILLED_V2_TOPIC:
                token_id = _u256_word(data, 1)
                if token_id is None:
                    continue
                rec["token_id"] = str(token_id)
                rec["side"] = _u256_word(data, 0)
            else:
                rec["maker_asset_id"] = _u256_word(data, 0)
                rec["taker_asset_id"] = _u256_word(data, 1)
                if rec["maker_asset_id"] is None and rec["taker_asset_id"] is None:
                    continue
            fills.append(rec)
        except Exception:
            continue
    return fills


def backfill_wallets_from_fills(rows: List[dict], fills: List[dict]) -> Dict[str, int]:
    """Fill null maker_wallet/taker_wallet/wallet by (tx_hash, token_id) join.

    A row matches fills with the same tx AND the same token (V2 tokenId or
    either V1 asset id). Attribution only when ALL matching fills agree on
    the wallet — multi-maker same-token fills stay NULL. In place; returns
    {"filled_maker", "filled_taker", "filled_wallet"}.
    """
    # per (tx, token) unanimity — regroup with token granularity
    agree: Dict[tuple, Tuple[Optional[str], Optional[str]]] = {}
    groups: Dict[tuple, dict] = {}
    for f in fills:
        toks = set()
        if f.get("token_id"):
            toks.add(f["token_id"])
        for aid in (f.get("maker_asset_id"), f.get("taker_asset_id")):
            if aid is not None:
                toks.add(str(aid))
        for tok in toks:
            g = groups.setdefault((f["tx_hash"], tok), {"m": set(), "t": set()})
            if f.get("maker"):
                g["m"].add(f["maker"])
            if f.get("taker"):
                g["t"].add(f["taker"])
    for key, g in groups.items():
        agree[key] = (next(iter(g["m"])) if len(g["m"]) == 1 else None,
                      next(iter(g["t"])) if len(g["t"]) == 1 else None)
    stats = {"filled_maker": 0, "filled_taker": 0, "filled_wallet": 0}
    for r in rows:
        try:
            txh = str(r.get("transaction_hash") or "").lower()
            tok = str(r.get("token_id") or "")
            if not txh or not tok or (txh, tok) not in agree:
                continue
            maker, taker = agree[(txh, tok)]
            if r.get("maker_wallet") is None and maker:
                r["maker_wallet"] = maker
                stats["filled_maker"] += 1
            if r.get("taker_wallet") is None and taker:
                r["taker_wallet"] = taker
                stats["filled_taker"] += 1
            if r.get("wallet") is None and (r.get("taker_wallet") or r.get("maker_wallet")):
                r["wallet"] = r.get("taker_wallet") or r.get("maker_wallet")
                stats["filled_wallet"] += 1
        except Exception:
            continue
    return stats


def parse_order_filled_logs(logs: List[dict]) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """Decode OrderFilled logs → {tx_hash_lower: (maker_or_None, taker_or_None)}.

    Multi-fill txs naming DISTINCT makers (or takers) yield None on that side.
    Pure function — no network, fully unit-testable.
    """
    makers: Dict[str, set] = {}
    takers: Dict[str, set] = {}
    for log in logs or []:
        try:
            topics = log.get("topics") or []
            if len(topics) < 4:
                continue
            if topics[0].lower() not in (ORDERFILLED_V1_TOPIC, ORDERFILLED_V2_TOPIC):
                continue
            maker = _topic_addr(topics[2])
            taker = _topic_addr(topics[3])
            txh = str(log.get("transactionHash") or "").lower()
            if not txh:
                continue
            if maker:
                makers.setdefault(txh, set()).add(maker)
            if taker:
                takers.setdefault(txh, set()).add(taker)
        except Exception:
            continue
    out: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for txh in set(makers) | set(takers):
        m = makers.get(txh, set())
        t = takers.get(txh, set())
        out[txh] = (next(iter(m)) if len(m) == 1 else None,
                    next(iter(t)) if len(t) == 1 else None)
    return out


def fetch_receipt_fills(rpc_url: str, tx_hashes: List[str],
                        timeout: float = 20.0) -> List[dict]:
    """Per-fill records for specific transactions via eth_getTransactionReceipt.

    One small call per tx (vs scanning dense ranges: recent blocks carry ~50
    OrderFilled logs/block across ALL Polymarket markets). Logs not from the
    CTF Exchange contracts or without the OrderFilled topic0 are ignored.
    Unknown/failed receipts are skipped — the caller retries next run.
    """
    import httpx

    fills: List[dict] = []
    addrs = {CTF_EXCHANGE_V1.lower(), CTF_EXCHANGE_V2.lower()}
    with httpx.Client(timeout=timeout) as client:
        for txh in tx_hashes:
            try:
                payload = {"jsonrpc": "2.0", "id": 1,
                           "method": "eth_getTransactionReceipt",
                           "params": [txh]}
                resp = client.post(rpc_url, json=payload)
                resp.raise_for_status()
                receipt = (resp.json() or {}).get("result") or {}
                logs = [l for l in (receipt.get("logs") or [])
                        if str(l.get("address") or "").lower() in addrs]
                fills.extend(parse_order_filled_fills(logs))
            except Exception as e:
                print(f"[onchain] WARN receipt fetch failed for {txh[:14]}…: {e}")
                continue
    return fills


def latest_block(rpc_url: str, timeout: float = 20.0) -> int:
    """Current Polygon head block number."""
    import httpx

    resp = httpx.post(rpc_url, json={"jsonrpc": "2.0", "id": 1,
                                     "method": "eth_blockNumber", "params": []},
                      timeout=timeout)
    resp.raise_for_status()
    return int(resp.json()["result"], 16)


def tx_map_from_fills(fills: List[dict]) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """Tx-level {tx: (maker_or_None, taker_or_None)} unanimity map from fills —
    the same honesty rule as parse_order_filled_logs, reused as the fallback
    for rows without token_id. Pure function."""
    makers: Dict[str, set] = {}
    takers: Dict[str, set] = {}
    for f in fills:
        txh = f.get("tx_hash")
        if not txh:
            continue
        if f.get("maker"):
            makers.setdefault(txh, set()).add(f["maker"])
        if f.get("taker"):
            takers.setdefault(txh, set()).add(f["taker"])
    out: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for txh in set(makers) | set(takers):
        m = makers.get(txh, set())
        t = takers.get(txh, set())
        out[txh] = (next(iter(m)) if len(m) == 1 else None,
                    next(iter(t)) if len(t) == 1 else None)
    return out


def backfill_wallets_from_chain(rows: List[dict],
                                tx_map: Dict[str, Tuple[Optional[str], Optional[str]]]
                                ) -> Dict[str, int]:
    """Fill null maker_wallet/taker_wallet/wallet on trade-row dicts in place.

    Join key: lower(transaction_hash). Only fills NULL fields; unanimity already
    enforced by parse_order_filled_logs. Returns {"filled_maker", "filled_taker",
    "filled_wallet"} counts.
    """
    stats = {"filled_maker": 0, "filled_taker": 0, "filled_wallet": 0}
    for r in rows:
        try:
            txh = str(r.get("transaction_hash") or "").lower()
            if not txh or txh not in tx_map:
                continue
            maker, taker = tx_map[txh]
            if r.get("maker_wallet") is None and maker:
                r["maker_wallet"] = maker
                stats["filled_maker"] += 1
            if r.get("taker_wallet") is None and taker:
                r["taker_wallet"] = taker
                stats["filled_taker"] += 1
            if r.get("wallet") is None and (r.get("taker_wallet") or r.get("maker_wallet")):
                r["wallet"] = r.get("taker_wallet") or r.get("maker_wallet")
                stats["filled_wallet"] += 1
        except Exception:
            continue
    return stats
