// KalshiConnector — Phase 1 stub.
// Official docs: https://docs.kalshi.com (Trade API v2)
//   Base (to verify): https://api.elections.kalshi.com/trade-api/v2
// STATUS: NOT verified in this environment. Same gate as Polymarket before any live use.

import type { Balance, FeeSchedule, Market, Order, OrderBook, OrderResult, VenueHealth } from '@arb/types';
import type { Logger } from '@arb/logging';
import { ConnectorNotImplementedError, type MarketConnector, type VenueInfo } from './base.js';

export const KALSHI_DOCS = 'https://docs.kalshi.com';

export class KalshiConnector implements MarketConnector {
  readonly venue = 'kalshi';
  private logger?: Logger;
  constructor(opts: { logger?: Logger } = {}) {
    this.logger = opts.logger;
  }
  venueInfo(): VenueInfo {
    return {
      venue: this.venue,
      displayName: 'Kalshi',
      docsUrl: KALSHI_DOCS,
      apiBaseUrl: null,
      wsUrl: null,
      tradingAllowed: false,
      wsSupported: true,
      pollSupported: true,
    };
  }
  async getMarkets(): Promise<Market[]> {
    throw new ConnectorNotImplementedError(this.venue, 'getMarkets', KALSHI_DOCS);
  }
  async getMarket(id: string): Promise<Market> {
    void id;
    throw new ConnectorNotImplementedError(this.venue, 'getMarket', KALSHI_DOCS);
  }
  async getOrderBook(id: string): Promise<OrderBook> {
    void id;
    throw new ConnectorNotImplementedError(this.venue, 'getOrderBook', KALSHI_DOCS);
  }
  async getFees(): Promise<FeeSchedule> {
    // Kalshi publishes a fee schedule in docs; treat as manual until endpoint verified.
    return {
      venue: this.venue,
      source: 'manual',
      makerFee: 0,
      takerFee: 0,
      notes: 'Manually configured placeholder; verify current Kalshi fee schedule before live use.',
      retrievedAt: new Date().toISOString(),
    };
  }
  async getBalances(): Promise<Balance[]> {
    throw new ConnectorNotImplementedError(this.venue, 'getBalances', KALSHI_DOCS);
  }
  async placeOrder(_order: Order): Promise<OrderResult> {
    this.logger?.warn('Blocked live order: connector not verified', { venue: this.venue });
    throw new ConnectorNotImplementedError(this.venue, 'placeOrder', KALSHI_DOCS);
  }
  async cancelOrder(orderId: string): Promise<void> {
    void orderId;
    throw new ConnectorNotImplementedError(this.venue, 'cancelOrder', KALSHI_DOCS);
  }
  async subscribeToMarketData(markets: string[]): Promise<void> {
    void markets;
    throw new ConnectorNotImplementedError(this.venue, 'subscribeToMarketData', KALSHI_DOCS);
  }
  async health(): Promise<VenueHealth> {
    return { venue: this.venue, status: 'OFFLINE', lastError: 'Phase 1 stub: not verified', wsConnected: false };
  }
}
