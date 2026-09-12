import { describe, expect, it } from 'vitest';
import { computeMatchConfidence, passesMatchGate } from '@arb/market-matcher';
import type { Market } from '@arb/types';

function m(over: Partial<Market>): Market {
  return {
    venue: 'a',
    venueMarketId: '1',
    sport: 'Basketball',
    league: 'NBA',
    eventId: 'e1',
    eventName: 'Lakers vs Celtics',
    homeTeam: 'Lakers',
    awayTeam: 'Celtics',
    startTime: '2026-01-01T00:00:00Z',
    marketType: 'moneyline',
    selection: 'Lakers',
    outcome: 'YES',
    price: 0.5,
    priceModel: 'probability',
    currency: 'USDC',
    settlementRules: { description: 'home win', payoutModel: 'binary_yes_pays_1', payoutPerUnit: 1 },
    status: 'open',
    timestamp: '2026-01-01T00:00:00Z',
    ...over,
  } as Market;
}

describe('market matcher', () => {
  it('identical events score high', () => {
    const r = computeMatchConfidence(m({}), m({ venue: 'b', venueMarketId: '2' }));
    expect(r.confidence).toBeGreaterThanOrEqual(0.95);
  });
  it('different teams score low and fail gate', () => {
    const r = computeMatchConfidence(m({}), m({ venue: 'b', venueMarketId: '2', homeTeam: 'Bulls', awayTeam: 'Heat' }));
    expect(r.confidence).toBeLessThan(0.95);
    expect(passesMatchGate(r, 0.95, true)).toBe(false);
  });
  it('unverified settlement never passes gate even with name match', () => {
    const r = computeMatchConfidence(m({}), m({ venue: 'b', venueMarketId: '2' }));
    expect(passesMatchGate(r, 0.5, false)).toBe(false);
  });
  it('never auto-approves uncertain matches', () => {
    const r = computeMatchConfidence(m({}), m({ venue: 'b', venueMarketId: '2', homeTeam: 'Bulls' }));
    expect(r.approved).toBe(false);
  });
});
