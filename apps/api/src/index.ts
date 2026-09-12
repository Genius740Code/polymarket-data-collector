// @arb/api — Phase 1 minimal backend (built-in node:http, no framework yet).
// Routes: GET /health, GET /api/venues, GET /api/opportunities (stub), GET /openapi.yaml
// Auth + full CRUD land in later phases. TRADING_MODE enforced: LIVE order routes do not exist in Phase 1.

import { createServer } from 'node:http';
import { loadConfig, publicConfig } from '@arb/config';
import { createLogger } from '@arb/logging';
import { BetfairConnector, KalshiConnector, PolymarketConnector } from '@arb/market-connectors';

const cfg = loadConfig(process.env);
const log = createLogger('api', { level: cfg.logLevel });

const connectors = [new PolymarketConnector(), new KalshiConnector(), new BetfairConnector()];

export function buildRouter() {
  return async (url: string, method: string) => {
    if (url === '/health') {
      const health = await Promise.all(connectors.map((c) => c.health()));
      return {
        status: 200,
        body: { ok: true, tradingMode: cfg.tradingMode, config: publicConfig(cfg), venues: health },
      };
    }
    if (url === '/api/venues') {
      return { status: 200, body: connectors.map((c) => c.venueInfo()) };
    }
    if (url === '/api/opportunities') {
      // Phase 1: no detection loop yet — empty list with schema pointer.
      return { status: 200, body: { data: [], note: 'detection engine lands in Phase 7; schema: arbitrage_opportunities' } };
    }
    return { status: 404, body: { error: 'not_found' } };
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const router = await buildRouter();
  const server = createServer(async (req, res) => {
    const r = await router(req.url ?? '/', req.method ?? 'GET');
    log.info('request', { service: 'api', result: String(r.status) });
    res.writeHead(r.status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(r.body));
  });
  server.listen(cfg.apiPort, () => log.info(`api listening on :${cfg.apiPort}`, { service: 'api' }));
}
