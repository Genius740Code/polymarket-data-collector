// MarketConnector — common interface every venue MUST implement.
// Venue-specific logic lives ONLY inside the connector. Core engines depend on this interface.
// Official-API only: no scraping, no private-API reverse engineering, no CAPTCHA/geo/KYC bypass.

import type {
  Balance,
  FeeSchedule,
  Market,
  Order,
  OrderBook,
  OrderResult,
  VenueHealth,
} from '@arb/types';

export interface VenueInfo {
  venue: string;
  displayName: string;
  /** Official public docs URL (never a scraped/guessed endpoint). */
  docsUrl: string;
  /** Verified API base URL, or null until verified against current official docs. */
  apiBaseUrl: string | null;
  wsUrl: string | null;
  /** True only after ToS + API-permission review explicitly allows automated trading. */
  tradingAllowed: boolean;
  wsSupported: boolean;
  pollSupported: boolean;
}

export interface MarketConnector {
  readonly venue: string;
  venueInfo(): VenueInfo;
  getMarkets(): Promise<Market[]>;
  getMarket(id: string): Promise<Market>;
  getOrderBook(id: string): Promise<OrderBook>;
  getFees(): Promise<FeeSchedule>;
  getBalances(): Promise<Balance[]>;
  placeOrder?(order: Order): Promise<OrderResult>;
  cancelOrder?(orderId: string): Promise<void>;
  subscribeToMarketData?(markets: string[]): Promise<void>;
  health(): Promise<VenueHealth>;
}

export class ConnectorNotImplementedError extends Error {
  constructor(
    public readonly venue: string,
    public readonly method: string,
    public readonly docsUrl: string,
  ) {
    super(
      `[${venue}] ${method} not yet verified/implemented. ` +
        `See official docs: ${docsUrl}. ` +
        `Live integration requires docs verification + ToS automated-trading review.`,
    );
    this.name = 'ConnectorNotImplementedError';
  }
}
