// @arb/types — single source of truth for cross-venue schemas.
// Correctness > speed. No venue-specific logic here; connectors map TO these types.
// Prices: binary contract prices MUST be in [0,1]. Null book side = no liquidity (never 0-as-null).

export type VenueId = string;

export type TradingMode = 'PAPER' | 'DRY_RUN' | 'MANUAL_CONFIRMATION' | 'LIVE';

export type MarketStatus = 'open' | 'closed' | 'settled' | 'voided' | 'halted' | 'unknown';
export type BookState = 'live' | 'stale' | 'resyncing' | 'unknown';
export type OrderSide = 'buy' | 'sell';
export type OrderType = 'limit' | 'market';
export type TimeInForce = 'GTC' | 'IOC' | 'FOK';
export type OrderStatus = 'pending' | 'open' | 'filled' | 'partially_filled' | 'cancelled' | 'failed';
export type LegStatus =
  | 'UNFILLED'
  | 'PARTIALLY_FILLED'
  | 'FILLED'
  | 'CANCELLED'
  | 'FAILED'
  | 'HEDGED'
  | 'UNWOUND';
export type OpportunityStatus =
  | 'DETECTED'
  | 'VALIDATED'
  | 'EXPIRED'
  | 'EXECUTING'
  | 'COMPLETED'
  | 'FAILED'
  | 'REJECTED';

export type FeeSource = 'api' | 'manual';
export type CurrencyCode = string; // 'USD' | 'USDC' | 'GBP' | ...

/** Explicit settlement semantics — connector MUST populate; never assume equivalence by name. */
export interface SettlementRules {
  /** Verbatim rules text or URL from venue docs where available. */
  description: string;
  rulesUrl?: string;
  /** How the venue settles: e.g. 'binary_yes_pays_1', 'decimal_odds', 'american_odds'. */
  payoutModel: string;
  /** Guaranteed payout per 1 unit of the covered position (usually 1.0 for binary). */
  payoutPerUnit: number;
  includesOvertime?: boolean | null;
  voidRules?: string | null;
  cancellationRules?: string | null;
  /** Venue-declared complement semantics, e.g. whether NO == 1-YES. Null = unknown, do not assume. */
  complementSemantics?: string | null;
  compatibleWith?: string[] | null;
}

export interface Market {
  venue: VenueId;
  venueMarketId: string;
  sport: string;
  league: string;
  eventId: string;
  eventName: string;
  homeTeam: string | null;
  awayTeam: string | null;
  startTime: string; // ISO-8601 UTC
  marketType: string; // e.g. 'moneyline' | 'match_odds' | 'binary_yes_no'
  selection: string;
  outcome: string; // canonical side being priced, e.g. 'YES' | 'NO' | 'HOME' | 'AWAY'
  price: number; // normalized to [0,1] probability for binary; decimal odds elsewhere with priceModel noted
  priceModel: 'probability' | 'decimal_odds' | 'american_odds' | 'unknown';
  quantity?: number | null;
  currency: CurrencyCode;
  settlementRules: SettlementRules;
  status: MarketStatus;
  timestamp: string; // ISO-8601 UTC observation time
  raw?: Record<string, unknown> | null;
}

export interface OrderBookLevel {
  price: number; // [0,1] for probability books
  size: number; // >= 0 in contracts or stake units; null side represented by absence, not 0
}

export interface OrderBook {
  venue: VenueId;
  marketId: string;
  bids: OrderBookLevel[]; // best-first descending
  asks: OrderBookLevel[]; // best-first ascending
  spread?: number | null;
  timestamp: string; // venue timestamp ISO
  receivedAt: string; // local receipt ISO
  sequence?: number | string | null;
  bookState: BookState; // stale/resyncing books must never execute
}

export interface FeeTier {
  upToVolume?: number | null;
  makerFee: number; // decimal fraction, e.g. 0.001 = 0.1%
  takerFee: number;
}

export interface FeeSchedule {
  venue: VenueId;
  source: FeeSource; // 'manual' until verified via official fee endpoint/docs
  makerFee: number;
  takerFee: number;
  perContractFee?: number | null;
  settlementFee?: number | null;
  withdrawalFee?: number | null;
  depositFee?: number | null;
  networkFee?: number | null;
  currencyConversionFee?: number | null;
  tiers?: FeeTier[] | null;
  notes?: string | null;
  retrievedAt: string; // ISO
}

export interface Balance {
  venue: VenueId;
  currency: CurrencyCode;
  available: number;
  locked?: number | null;
  total?: number | null;
  retrievedAt: string;
}

export interface Order {
  clientOrderId: string;
  venue: VenueId;
  marketId: string;
  side: OrderSide;
  outcome?: string | null;
  price: number; // limit price in venue-native semantics mapped to [0,1] for binary
  size: number;
  type: OrderType;
  timeInForce?: TimeInForce;
  idempotencyKey?: string;
}

export interface OrderResult {
  venueOrderId: string;
  clientOrderId: string;
  status: OrderStatus;
  filledSize: number;
  avgFillPrice?: number | null;
  raw?: Record<string, unknown> | null;
}

export interface Fill {
  fillId: string;
  venueOrderId: string;
  venue: VenueId;
  marketId: string;
  price: number;
  size: number;
  feePaid?: number | null;
  feeCurrency?: CurrencyCode | null;
  timestamp: string;
}

export interface EventMatch {
  eventId: string; // canonical internal event id
  venueA: VenueId;
  venueAMarketId: string;
  venueB: VenueId;
  venueBMarketId: string;
  confidence: number; // 0..1
  reasons: string[];
  approved: boolean; // manual approval for uncertain matches
  createdAt: string;
}

export interface CostBreakdown {
  grossEdge: number;
  venueFees: number;
  networkFees: number;
  slippageCost: number;
  fxCost: number;
  totalCosts: number;
  guaranteedPayout: number;
  positionCost: number;
  netProfit: number;
  netRoi: number;
  capitalRequired: number;
}

export interface ArbitrageOpportunity {
  opportunityId: string;
  timestamp: string;
  eventId: string;
  eventName: string;
  sport: string;
  league: string;
  venueA: VenueId;
  venueB: VenueId;
  marketAId: string;
  marketBId: string;
  legA: { side: string; price: number; size: number };
  legB: { side: string; price: number; size: number };
  availableLiquidity: number;
  maxSize: number;
  minSize: number;
  recommendedSize: number;
  matchConfidence: number; // 0..1
  costs: CostBreakdown;
  executionRisk: string;
  status: OpportunityStatus;
  ageMs?: number | null;
}

export interface RiskLimits {
  maxCapitalPerTrade: number;
  maxCapitalPerVenue: number;
  maxDailyLoss: number;
  maxOpenPositions: number;
  maxExposure: number;
  maxEventExposure: number;
  maxOrderSize: number;
  maxSlippage: number; // fraction
  maxMarketAgeMs: number;
  minNetRoi: number;
  minNetProfit: number;
  killSwitch: boolean;
}

export interface LatencyStamps {
  marketTimestamp: string;
  receivedTimestamp: string;
  calculationTimestamp: string;
  orderSubmissionTimestamp?: string | null;
  fillTimestamp?: string | null;
}

export interface VenueHealth {
  venue: VenueId;
  status: 'ONLINE' | 'DEGRADED' | 'OFFLINE';
  latencyMs?: number | null;
  lastSuccessAt?: string | null;
  lastError?: string | null;
  wsConnected?: boolean | null;
}

// ---------- Pure validation helpers (no I/O, no venue logic) ----------

export function isValidProbability(p: unknown): p is number {
  return typeof p === 'number' && Number.isFinite(p) && p >= 0 && p <= 1;
}

export function assertValidProbability(p: number, what = 'price'): void {
  if (!isValidProbability(p)) throw new Error(`Invalid ${what}: ${String(p)} (must be 0..1)`);
}

export function isValidSize(s: unknown): s is number {
  return typeof s === 'number' && Number.isFinite(s) && s >= 0;
}

/** Confidence must be 0..1. */
export function assertValidConfidence(c: number): void {
  if (typeof c !== 'number' || !Number.isFinite(c) || c < 0 || c > 1) {
    throw new Error(`Invalid match confidence: ${String(c)} (must be 0..1)`);
  }
}

/**
 * Complement price helper. ONLY call when the connector's SettlementRules
 * explicitly declare complement semantics (e.g. NO == 1 - YES). Otherwise treat as unknown.
 */
export function complementPrice(yesPrice: number, declaresComplement: boolean): number | null {
  assertValidProbability(yesPrice, 'yesPrice');
  if (!declaresComplement) return null;
  return 1 - yesPrice;
}
