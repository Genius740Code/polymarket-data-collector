// BetfairConnector — Phase 1 stub.
// Official docs: https://docs.developer.betfair.com (Betfair Exchange API-NG, JSON-RPC)
// STATUS: NOT verified; Betfair requires app key + certs + funded account. No live use in Phase 1.

import type { Balance, FeeSchedule, Market, Order, OrderBook, OrderResult, VenueHealth } from '@arb/types';
import type { Logger } from '@arb/logging';
import { ConnectorNotImplementedError, type MarketConnector, type VenueInfo } from './base.js';

export const BETFAIR_DOCS = 'https://docs.developer.betfair.com';

export class BetfairConnector implements MarketConnector {
  readonly venue = 'betfair';
  private logger?: Logger;
  constructor(opts: { logger?: Logger } = {}) {
    this.logger = opts.logger;
  }
  venueInfo(): VenueInfo {
    return {
      venue: this.venue,
      displayName: 'Betfair Exchange',
      docsUrl: BETFAIR_DOCS,
      apiBaseUrl: null,
      wsUrl: null,
      tradingAllowed: false,
      wsSupported: true, // Exchange Streaming API per docs; verify before use
      pollSupported: true,
    };
  }
  async getMarkets(): Promise<Market[]> {
    throw new ConnectorNotImplementedError(this.venue, 'getMarkets', BETFAIR_DOCS);
  }
  async getMarket(id: string): Promise<Market> {
    void id;
    throw new ConnectorNotImplementedError(this.venue, 'getMarket', BETFAIR_DOCS);
  }
  async getOrderBook(id: string): Promise<OrderBook> {
    void id;
    throw new ConnectorNotImplementedError(this.venue, 'getOrderBook', BETFAIR_DOCS);
  }
  async getFees(): Promise<FeeSchedule> {
    return {
      venue: this.venue,
      source: 'manual',
      makerFee: 0,
      takerFee: 0.05, // PLACEHOLDER commission; must verify market-specific commission before live use.
      notes: 'Manual placeholder commission. Betfair commission is market/account-specific; verify via official docs.',
      retrievedAt: new Date().toISOString(),
    };
  }
  async getBalances(): Promise<Balance[]> {
    throw new ConnectorNotImplementedError(this.venue, 'getBalances', BETFAIR_DOCS);
  }
  async placeOrder(_order: Order): Promise<OrderResult> {
    this.logger?.warn('Blocked live order: connector not verified', { venue: this.venue });
    throw new ConnectorNotImplementedError(this.venue, 'placeOrder', BETFAIR_DOCS);
  }
  async cancelOrder(orderId: string): Promise<void> {
    void orderId;
    throw new ConnectorNotImplementedError(this.venue, 'cancelOrder', BETFAIR_DOCS);
  }
  async subscribeToMarketData(markets: string[]): Promise<void> {
    void markets;
    throw new ConnectorNotImplementedError(this.venue, 'subscribeToMarketData', BETFAIR_DOCS);
  }
  async health(): Promise<VenueHealth> {
    return { venue: this.venue, status: 'OFFLINE', lastError: 'Phase 1 stub: not verified', wsConnected: false };
  }
}
