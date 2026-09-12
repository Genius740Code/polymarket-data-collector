import { describe, expect, it } from 'vitest';
import { BetfairConnector, ConnectorRegistry, KalshiConnector, PolymarketConnector } from '@arb/market-connectors';

describe('connector registry + stubs', () => {
  it('registers venues without touching core engine', () => {
    const reg = new ConnectorRegistry();
    reg.register(new PolymarketConnector());
    reg.register(new KalshiConnector());
    reg.register(new BetfairConnector());
    expect(reg.venues().sort()).toEqual(['betfair', 'kalshi', 'polymarket']);
  });
  it('stubs expose docs URLs and block live until verified', async () => {
    const c = new PolymarketConnector();
    expect(c.venueInfo().docsUrl).toContain('polymarket');
    expect(c.venueInfo().tradingAllowed).toBe(false);
    await expect(c.getMarkets()).rejects.toThrow(/official docs/);
    const fees = await c.getFees();
    expect(fees.source).toBe('manual');
  });
});
