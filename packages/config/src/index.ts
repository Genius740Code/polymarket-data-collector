// @arb/config — env-driven configuration. PAPER mode by default.
// Secrets stay server-side; never import this package in frontend code.

import type { RiskLimits, TradingMode } from '@arb/types';

export interface VenueCredentials {
  polymarketApiKey?: string;
  polymarketPrivateKey?: string;
  polymarketFunderAddress?: string;
  kalshiApiKey?: string;
  kalshiPrivateKey?: string;
  betfairAppKey?: string;
  betfairUsername?: string;
  betfairPassword?: string;
  betfairCertPath?: string;
}

export interface AppConfig {
  tradingMode: TradingMode;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  databaseUrl: string;
  redisUrl: string;
  apiPort: number;
  minNetRoi: number;
  minNetProfit: number;
  maxCapitalPerTrade: number;
  maxSlippage: number;
  maxOpportunityAgeMs: number;
  matchConfidenceThreshold: number;
  risk: RiskLimits;
  credentials: VenueCredentials;
  alertWebhookUrl?: string;
}

function num(env: NodeJS.ProcessEnv, key: string, def: number): number {
  const raw = env[key];
  if (raw === undefined || raw === '') return def;
  const v = Number(raw);
  if (!Number.isFinite(v)) throw new Error(`Invalid numeric env ${key}=${raw}`);
  return v;
}

function str(env: NodeJS.ProcessEnv, key: string, def: string): string {
  const raw = env[key];
  return raw === undefined || raw === '' ? def : raw;
}

function opt(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const raw = env[key];
  return raw === undefined || raw === '' ? undefined : raw;
}

const TRADING_MODES: TradingMode[] = ['PAPER', 'DRY_RUN', 'MANUAL_CONFIRMATION', 'LIVE'];

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const modeRaw = str(env, 'TRADING_MODE', 'PAPER').toUpperCase();
  if (!(TRADING_MODES as string[]).includes(modeRaw)) {
    throw new Error(`Invalid TRADING_MODE=${modeRaw}; must be one of ${TRADING_MODES.join(',')}`);
  }
  const tradingMode = modeRaw as TradingMode;

  const logLevelRaw = str(env, 'LOG_LEVEL', 'info').toLowerCase();
  if (!['debug', 'info', 'warn', 'error'].includes(logLevelRaw)) {
    throw new Error(`Invalid LOG_LEVEL=${logLevelRaw}`);
  }

  const minNetRoi = num(env, 'MIN_NET_ROI', 0.01);
  const minNetProfit = num(env, 'MIN_NET_PROFIT', 5);
  const maxCapitalPerTrade = num(env, 'MAX_CAPITAL_PER_TRADE', 1000);
  const maxSlippage = num(env, 'MAX_SLIPPAGE', 0.005);
  const maxOpportunityAgeMs = num(env, 'MAX_OPPORTUNITY_AGE_MS', 1000);
  const matchConfidenceThreshold = num(env, 'MATCH_CONFIDENCE_THRESHOLD', 0.95);

  if (minNetRoi < 0 || minNetRoi > 1) throw new Error('MIN_NET_ROI must be 0..1');
  if (maxSlippage < 0 || maxSlippage > 1) throw new Error('MAX_SLIPPAGE must be 0..1');
  if (matchConfidenceThreshold < 0 || matchConfidenceThreshold > 1) {
    throw new Error('MATCH_CONFIDENCE_THRESHOLD must be 0..1');
  }
  if (maxCapitalPerTrade <= 0) throw new Error('MAX_CAPITAL_PER_TRADE must be > 0');

  const risk: RiskLimits = {
    maxCapitalPerTrade,
    maxCapitalPerVenue: num(env, 'MAX_CAPITAL_PER_VENUE', 5000),
    maxDailyLoss: num(env, 'MAX_DAILY_LOSS', 500),
    maxOpenPositions: Math.floor(num(env, 'MAX_OPEN_POSITIONS', 10)),
    maxExposure: num(env, 'MAX_EVENT_EXPOSURE', 2000) * 5,
    maxEventExposure: num(env, 'MAX_EVENT_EXPOSURE', 2000),
    maxOrderSize: num(env, 'MAX_ORDER_SIZE', 1000),
    maxSlippage,
    maxMarketAgeMs: num(env, 'MAX_MARKET_AGE_MS', 5000),
    minNetRoi,
    minNetProfit,
    killSwitch: str(env, 'KILL_SWITCH', 'false').toLowerCase() === 'true',
  };

  if (tradingMode === 'LIVE' && risk.killSwitch) {
    throw new Error('Refusing LIVE mode with KILL_SWITCH=true');
  }

  return {
    tradingMode,
    logLevel: logLevelRaw as AppConfig['logLevel'],
    databaseUrl: str(env, 'DATABASE_URL', 'postgres://arb:arb@localhost:5432/arb'),
    redisUrl: str(env, 'REDIS_URL', 'redis://localhost:6379'),
    apiPort: Math.floor(num(env, 'API_PORT', 3001)),
    minNetRoi,
    minNetProfit,
    maxCapitalPerTrade,
    maxSlippage,
    maxOpportunityAgeMs,
    matchConfidenceThreshold,
    risk,
    credentials: {
      polymarketApiKey: opt(env, 'POLYMARKET_API_KEY'),
      polymarketPrivateKey: opt(env, 'POLYMARKET_PRIVATE_KEY'),
      polymarketFunderAddress: opt(env, 'POLYMARKET_FUNDER_ADDRESS'),
      kalshiApiKey: opt(env, 'KALSHI_API_KEY'),
      kalshiPrivateKey: opt(env, 'KALSHI_PRIVATE_KEY'),
      betfairAppKey: opt(env, 'BETFAIR_APP_KEY'),
      betfairUsername: opt(env, 'BETFAIR_USERNAME'),
      betfairPassword: opt(env, 'BETFAIR_PASSWORD'),
      betfairCertPath: opt(env, 'BETFAIR_CERT_PATH'),
    },
    alertWebhookUrl: opt(env, 'ALERT_WEBHOOK_URL'),
  };
}

/** Safe subset for logs/health — never includes credentials. */
export function publicConfig(cfg: AppConfig): Record<string, unknown> {
  return {
    tradingMode: cfg.tradingMode,
    logLevel: cfg.logLevel,
    apiPort: cfg.apiPort,
    minNetRoi: cfg.minNetRoi,
    minNetProfit: cfg.minNetProfit,
    maxCapitalPerTrade: cfg.maxCapitalPerTrade,
    maxSlippage: cfg.maxSlippage,
    maxOpportunityAgeMs: cfg.maxOpportunityAgeMs,
    matchConfidenceThreshold: cfg.matchConfidenceThreshold,
    risk: cfg.risk,
    hasDatabase: cfg.databaseUrl.length > 0,
    hasRedis: cfg.redisUrl.length > 0,
  };
}
