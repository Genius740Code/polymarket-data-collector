// PolymarketConnector — Phase 1 stub.
// Official docs: https://docs.polymarket.com
//   Gamma (markets/events): https://gamma-api.polymarket.com (public, documented)
//   CLOB (order book/trading): https://clob.polymarket.com (authenticated trading per docs)
//   WebSocket: wss://ws-subscriptions-clob.polymarket.com (per docs)
// STATUS: endpoints listed above are documented but NOT yet verified in this environment.
// Live trading requires: docs re-verification + ToS automated-trading review + credentials.
// Until then: market-data methods throw ConnectorNotImplementedError; getFees returns
// manually-configured schedule clearly marked source='manual'.

import type { Balance, FeeSchedule, Market, Order, OrderBook, OrderResult, VenueHealth } from '@arb/types';
import { assertValidProbability } from '@arb/types';
import type { Logger } from '@arb/logging';
import { ConnectorNotImplementedError, type MarketConnector, type VenueInfo } from './base.js';

export const POLYMARKET_DOCS = 'https://docs.polymarket.com';

export interface PolymarketConnectorOpts {
  logger?: Logger;
  manualTakerFee?: number; // clearly-marked manual config until fee endpoint verified
  manualMakerFee?: number;
}

export class PolymarketConnector implements MarketConnector {
  readonly venue = 'polymarket';
  private logger?: Logger;
  private takerFee: number;
  private makerFee: number;

  constructor(opts: PolymarketConnectorOpts = {}) {
    this.logger = opts.logger;
    this.takerFee = opts.manualTakerFee ?? 0;
    this.makerFee = opts.manualMakerFee ?? 0;
  }

  venueInfo(): VenueInfo {
    return {
      venue: this.venue,
      displayName: 'Polymarket',
      docsUrl: POLYMARKET_DOCS,
      apiBaseUrl: null, // set only after live docs verification (Phase 2 gate)
      wsUrl: null,
      tradingAllowed: false, // flip only after ToS review + explicit operator approval
      wsSupported: true,
      pollSupported: true,
    };
  }

  async getMarkets(): Promise<Market[]> {
    throw new ConnectorNotImplementedError(this.venue, 'getMarkets', POLYMARKET_DOCS);
  }
  async getMarket(id: string): Promise<Market> {
    void id;
    throw new ConnectorNotImplementedError(this.venue, 'getMarket', POLYMARKET_DOCS);
  }
  async getOrderBook(id: string): Promise<OrderBook> {
    void id;
    throw new ConnectorNotImplementedError(this.venue, 'getOrderBook', POLYMARKET_DOCS);
  }
  async getFees(): Promise<FeeSchedule> {
    return {
      venue: this.venue,
      source: 'manual',
      makerFee: this.makerFee,
      takerFee: this.takerFee,
      notes: 'Manually configured; no official fee endpoint verified yet. Verify https://docs.polymarket.com before live use.',
      retrievedAt: new Date().toISOString(),
    };
  }
  async getBalances(): Promise<Balance[]> {
    throw new ConnectorNotImplementedError(this.venue, 'getBalances', POLYMARKET_DOCS);
  }
  async placeOrder(order: Order): Promise<OrderResult> {
    assertValidProbability(order.price, 'order.price');
    this.logger?.warn('Blocked live order: connector not verified', { venue: this.venue, market: order.marketId });
    throw new ConnectorNotImplementedError(this.venue, 'placeOrder', POLYMARKET_DOCS);
  }
  async cancelOrder(orderId: string): Promise<void> {
    void orderId;
    throw new ConnectorNotImplementedError(this.venue, 'cancelOrder', POLYMARKET_DOCS);
  }
  async subscribeToMarketData(markets: string[]): Promise<void> {
    void markets;
    throw new ConnectorNotImplementedError(this.venue, 'subscribeToMarketData', POLYMARKET_DOCS);
  }
  async health(): Promise<VenueHealth> {
    return { venue: this.venue, status: 'OFFLINE', lastError: 'Phase 1 stub: not verified', wsConnected: false };
  }
}
