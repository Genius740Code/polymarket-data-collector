import { describe, expect, it } from 'vitest';
import { checkRiskLimits } from '@arb/risk-engine';

const limits = {
  maxCapitalPerTrade: 1000,
  maxCapitalPerVenue: 5000,
  maxDailyLoss: 500,
  maxOpenPositions: 10,
  maxExposure: 10000,
  maxEventExposure: 2000,
  maxOrderSize: 1000,
  maxSlippage: 0.005,
  maxMarketAgeMs: 5000,
  minNetRoi: 0.01,
  minNetProfit: 5,
  killSwitch: false,
};
const state = { dailyPnl: 0, openPositions: 0, venueExposure: {}, eventExposure: {}, totalExposure: 0 };

describe('risk engine', () => {
  it('allows sane order', () => {
    const r = checkRiskLimits(limits, state, { venue: 'kalshi', eventId: 'e', notional: 500, estSlippage: 0.001, marketAgeMs: 100 });
    expect(r.allowed).toBe(true);
  });
  it('blocks oversize + kill switch', () => {
    const r1 = checkRiskLimits(limits, state, { venue: 'kalshi', eventId: 'e', notional: 5000, estSlippage: 0, marketAgeMs: 0 });
    expect(r1.allowed).toBe(false);
    const r2 = checkRiskLimits({ ...limits, killSwitch: true }, state, { venue: 'k', eventId: 'e', notional: 1, estSlippage: 0, marketAgeMs: 0 });
    expect(r2.allowed).toBe(false);
    expect(r2.breaches.join(' ')).toMatch(/KILL_SWITCH/);
  });
});
