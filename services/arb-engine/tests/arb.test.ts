import { describe, expect, it } from 'vitest';
import { evaluateBinaryArb } from '@arb/arb-engine';

const gates = {
  minNetProfit: 5,
  minNetRoi: 0.01,
  maxOpportunityAgeMs: 1000,
  matchConfidenceThreshold: 0.95,
  bookStateOk: true,
  settlementCompatible: true,
  tradable: true,
};

describe('arb engine (spec section 8 example)', () => {
  it('YES 0.46 + NO 0.51 gives gross edge 0.03', () => {
    const r = evaluateBinaryArb(
      { priceA: 0.46, priceB: 0.51, size: 1000, liquidityAvailable: 1000, matchConfidence: 0.99, bookAgeMsA: 100, bookAgeMsB: 100 },
      { ...gates, minNetProfit: -1e9, minNetRoi: -1e9 },
    );
    expect(r.rawCost).toBeCloseTo(0.97, 10);
    expect(r.grossEdge).toBeCloseTo(0.03, 10);
    expect(r.guaranteedPayout).toBe(1000);
    expect(r.positionCost).toBeCloseTo(970, 8);
  });
  it('fees can flip executable -> displayed-only', () => {
    const noFees = evaluateBinaryArb(
      { priceA: 0.46, priceB: 0.51, size: 1000, liquidityAvailable: 5000, matchConfidence: 0.99 },
      gates,
    );
    expect(noFees.executable).toBe(true);
    const withFees = evaluateBinaryArb(
      { priceA: 0.46, priceB: 0.51, size: 1000, feeA: 20, feeB: 20, liquidityAvailable: 5000, matchConfidence: 0.99 },
      gates,
    );
    expect(withFees.netProfit).toBeCloseTo(30 - 40, 8);
    expect(withFees.executable).toBe(false);
    expect(withFees.reasons.join(';')).toMatch(/netProfit|netROI/);
  });
  it('stale books / low confidence / illiquidity block execution', () => {
    const r = evaluateBinaryArb(
      { priceA: 0.4, priceB: 0.4, size: 100, liquidityAvailable: 10, matchConfidence: 0.5 },
      gates,
    );
    expect(r.executable).toBe(false);
    expect(r.reasons.length).toBeGreaterThan(0);
  });
});
