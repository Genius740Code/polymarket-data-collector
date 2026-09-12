// Connector registry — plugin system: adding a venue = register a new MarketConnector.
// Core arb engine iterates this registry; it never imports venue classes directly.

import type { MarketConnector } from './base.js';

export class ConnectorRegistry {
  private connectors = new Map<string, MarketConnector>();

  register(connector: MarketConnector): void {
    if (this.connectors.has(connector.venue)) {
      throw new Error(`Connector already registered for venue: ${connector.venue}`);
    }
    this.connectors.set(connector.venue, connector);
  }

  get(venue: string): MarketConnector {
    const c = this.connectors.get(venue);
    if (!c) throw new Error(`No connector registered for venue: ${venue}`);
    return c;
  }

  list(): MarketConnector[] {
    return [...this.connectors.values()];
  }

  venues(): string[] {
    return [...this.connectors.keys()];
  }
}

export const globalRegistry = new ConnectorRegistry();
