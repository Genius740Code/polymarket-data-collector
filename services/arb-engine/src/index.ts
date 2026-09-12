// @arb/arb-engine — Phase 1: pure binary-arbitrage evaluation.
// Distinguishes DISPLAYED vs EXECUTABLE: executable requires all gates in ArbGates.
// No I/O, no venue calls — caller supplies order-book-derived fill prices, fees, staleness.

import { assertValidProbability } from '@arb/types';

export interface ArbInput {
  priceA: number; // executable ask for outcome A leg (walked book, not just BBO)
  priceB: number; // executable ask for complementary leg
  size: number; // contracts/units evaluated
  payoutPerUnit?: number; // default 1.0 for binary YES/NO
  feeA?: number;
  feeB?: number;
  slippageCost?: number;
  networkCost?: number;
  fxCost?: number;
  bookAgeMsA?: number;
  bookAgeMsB?: number;
  matchConfidence?: number;
  liquidityAvailable?: number;
}

export interface ArbGates {
  minNetProfit: number;
  minNetRoi: number;
  maxOpportunityAgeMs: number;
  matchConfidenceThreshold: number;
  bookStateOk: boolean; // false if either book stale/resyncing/missing
  settlementCompatible: boolean;
  tradable: boolean; // both sides actually tradable (balances, limits verified by caller)
}

export interface ArbResult {
  rawCost: number;
  grossEdge: number;
  totalCosts: number;
  guaranteedPayout: number;
  positionCost: number;
  netProfit: number;
  netRoi: number;
  executable: boolean;
  reasons: string[];
}

/** positionCost = (priceA+priceB)*size; payout = payoutPerUnit*size. */
export function evaluateBinaryArb(input: ArbInput, gates: ArbGates): ArbResult {
  assertValidProbability(input.priceA, 'priceA');
  assertValidProbability(input.priceB, 'priceB');
  if (!(input.size > 0)) throw new Error('size must be > 0');
  const payoutPerUnit = input.payoutPerUnit ?? 1;
  if (!(payoutPerUnit > 0)) throw new Error('payoutPerUnit must be > 0');

  const feeA = input.feeA ?? 0;
  const feeB = input.feeB ?? 0;
  const slip = input.slippageCost ?? 0;
  const net = input.networkCost ?? 0;
  const fx = input.fxCost ?? 0;
  for (const [k, v] of Object.entries({ feeA, feeB, slip, net, fx })) {
    if (!(v >= 0)) throw new Error(`${k} must be >= 0`);
  }

  const rawCost = input.priceA + input.priceB;
  const grossEdge = payoutPerUnit - rawCost;
  const positionCost = rawCost * input.size;
  const guaranteedPayout = payoutPerUnit * input.size;
  const totalCosts = feeA + feeB + slip + net + fx;
  const netProfit = guaranteedPayout - positionCost - totalCosts;
  const capital = positionCost + totalCosts;
  const netRoi = capital > 0 ? netProfit / capital : 0;

  const reasons: string[] = [];
  if (!gates.bookStateOk) reasons.push('book stale/resyncing or missing');
  if (!gates.settlementCompatible) reasons.push('settlement rules incompatible/unverified');
  if (!gates.tradable) reasons.push('one or both legs not tradable');
  if ((input.bookAgeMsA ?? 0) > gates.maxOpportunityAgeMs) reasons.push('leg A book too old');
  if ((input.bookAgeMsB ?? 0) > gates.maxOpportunityAgeMs) reasons.push('leg B book too old');
  if ((input.matchConfidence ?? 0) < gates.matchConfidenceThreshold) reasons.push('match confidence below threshold');
  if ((input.liquidityAvailable ?? input.size) < input.size) reasons.push('insufficient liquidity');
  if (netProfit <= gates.minNetProfit) reasons.push('netProfit below minimum');
  if (netRoi <= gates.minNetRoi) reasons.push('netROI below minimum');

  return {
    rawCost,
    grossEdge,
    totalCosts,
    guaranteedPayout,
    positionCost,
    netProfit,
    netRoi,
    executable: reasons.length === 0,
    reasons,
  };
}
