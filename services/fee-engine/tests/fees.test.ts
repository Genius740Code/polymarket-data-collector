import { describe, expect, it } from 'vitest';
import { quoteTakerFee } from '@arb/fee-engine';

describe('fee engine', () => {
  it('computes taker + per-contract + settlement', () => {
    const q = quoteTakerFee(
      { venue: 'x', source: 'manual', makerFee: 0, takerFee: 0.01, perContractFee: 0.001, settlementFee: 0.5, retrievedAt: new Date().toISOString() },
      1000,
      100,
    );
    expect(q.fee).toBeCloseTo(10 + 0.1 + 0.5, 8);
    expect(q.source).toBe('manual');
  });
  it('rejects negative notional', () => {
    expect(() =>
      quoteTakerFee({ venue: 'x', source: 'manual', makerFee: 0, takerFee: 0, retrievedAt: '' }, -1, 0),
    ).toThrow();
  });
});
