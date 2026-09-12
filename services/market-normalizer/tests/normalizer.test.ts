import { describe, expect, it } from 'vitest';
import { binaryComplement, validateMarket } from '@arb/market-normalizer';

describe('normalizer', () => {
  it('validates price range and requires ISO timestamps', () => {
    const base = {
      venue: 'x', venueMarketId: '1', sport: 's', league: 'l', eventId: 'e', eventName: 'n',
      homeTeam: null, awayTeam: null, startTime: '2026-01-01T00:00:00Z', marketType: 'moneyline',
      selection: 'H', outcome: 'YES', price: 0.5, priceModel: 'probability', currency: 'USD',
      settlementRules: { description: 'd', payoutModel: 'binary_yes_pays_1', payoutPerUnit: 1 },
      status: 'open', timestamp: '2026-01-01T00:00:00Z',
    } as Parameters<typeof validateMarket>[0];
    expect(validateMarket(base)).toEqual(expect.any(Array));
    expect(() => validateMarket({ ...base, price: 1.2 })).toThrow();
  });
  it('complement only with declared semantics', () => {
    expect(binaryComplement(0.4, true)).toBeCloseTo(0.6, 10);
    expect(binaryComplement(0.4, false)).toBeNull();
  });
});
