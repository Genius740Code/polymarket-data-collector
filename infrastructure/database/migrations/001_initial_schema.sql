-- 001_initial_schema.sql — Phase 1: persistent storage for arb platform.
-- Postgres 15+. All timestamps TIMESTAMPTZ (UTC). Prices NUMERIC for exactness.
-- Run: psql "$DATABASE_URL" -f infrastructure/database/migrations/001_initial_schema.sql
-- Backtest/simulation reads from these tables; paper fills are marked mode='PAPER' explicitly.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Venues registry (one row per integrated venue; docs URL required, no fake endpoints).
CREATE TABLE IF NOT EXISTS venues (
  venue_id        TEXT PRIMARY KEY,
  display_name    TEXT NOT NULL,
  docs_url        TEXT NOT NULL,
  api_base_url    TEXT NULL,          -- NULL until verified against official docs
  ws_url          TEXT NULL,
  trading_allowed BOOLEAN NOT NULL DEFAULT FALSE, -- true only after ToS/API-permission review
  fee_source      TEXT NOT NULL DEFAULT 'manual' CHECK (fee_source IN ('api','manual')),
  status          TEXT NOT NULL DEFAULT 'OFFLINE' CHECK (status IN ('ONLINE','DEGRADED','OFFLINE')),
  last_success_at TIMESTAMPTZ NULL,
  last_error      TEXT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Canonical events (deduped across venues by matcher; never assume name-equality).
CREATE TABLE IF NOT EXISTS events (
  event_id    TEXT PRIMARY KEY,       -- internal canonical id (uuid)
  sport       TEXT NOT NULL,
  league      TEXT NOT NULL,
  event_name  TEXT NOT NULL,
  home_team   TEXT NULL,
  away_team   TEXT NULL,
  start_time  TIMESTAMPTZ NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_events_sport_league_time ON events (sport, league, start_time);

-- Normalized markets (one row per venue market; event_id FK set after matching).
CREATE TABLE IF NOT EXISTS markets (
  venue           TEXT NOT NULL REFERENCES venues(venue_id),
  venue_market_id TEXT NOT NULL,
  event_id        TEXT NULL REFERENCES events(event_id),
  sport           TEXT NOT NULL,
  league          TEXT NOT NULL,
  event_name      TEXT NOT NULL,
  home_team       TEXT NULL,
  away_team       TEXT NULL,
  start_time      TIMESTAMPTZ NOT NULL,
  market_type     TEXT NOT NULL,
  selection       TEXT NOT NULL,
  outcome         TEXT NOT NULL,
  price           NUMERIC(12,8) NOT NULL CHECK (price >= 0 AND price <= 1),
  price_model     TEXT NOT NULL DEFAULT 'probability',
  quantity        NUMERIC(20,8) NULL CHECK (quantity IS NULL OR quantity >= 0),
  currency        TEXT NOT NULL,
  settlement_rules JSONB NOT NULL DEFAULT '{}'::jsonb,
  status          TEXT NOT NULL DEFAULT 'open',
  observed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw             JSONB NULL,
  PRIMARY KEY (venue, venue_market_id)
);
CREATE INDEX IF NOT EXISTS idx_markets_event ON markets (event_id);
CREATE INDEX IF NOT EXISTS idx_markets_sport_time ON markets (sport, start_time);

-- Market matches with confidence; execution requires confidence >= threshold + approval if uncertain.
CREATE TABLE IF NOT EXISTS market_matches (
  match_id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  canonical_event_id TEXT NOT NULL REFERENCES events(event_id),
  venue_a           TEXT NOT NULL,
  venue_a_market_id TEXT NOT NULL,
  venue_b           TEXT NOT NULL,
  venue_b_market_id TEXT NOT NULL,
  confidence        NUMERIC(6,5) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  reasons           JSONB NOT NULL DEFAULT '[]'::jsonb,
  approved          BOOLEAN NOT NULL DEFAULT FALSE,
  approved_by       TEXT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (venue_a, venue_a_market_id, venue_b, venue_b_market_id)
);

-- Latest order book per market (Redis holds real-time; Postgres holds last-known + audit).
CREATE TABLE IF NOT EXISTS order_books (
  venue       TEXT NOT NULL,
  market_id   TEXT NOT NULL,
  bids        JSONB NOT NULL DEFAULT '[]'::jsonb,  -- [{price,size}] best-first
  asks        JSONB NOT NULL DEFAULT '[]'::jsonb,
  spread      NUMERIC(12,8) NULL,
  venue_ts    TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sequence    TEXT NULL,
  book_state  TEXT NOT NULL DEFAULT 'live' CHECK (book_state IN ('live','stale','resyncing','unknown')),
  PRIMARY KEY (venue, market_id)
);

-- Immutable book snapshots for backtesting (append-only).
CREATE TABLE IF NOT EXISTS order_book_snapshots (
  snapshot_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  venue       TEXT NOT NULL,
  market_id   TEXT NOT NULL,
  bids        JSONB NOT NULL,
  asks        JSONB NOT NULL,
  venue_ts    TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  book_state  TEXT NOT NULL DEFAULT 'live'
);
CREATE INDEX IF NOT EXISTS idx_book_snaps_market_time ON order_book_snapshots (venue, market_id, venue_ts DESC);

-- Fee schedules (source marks manual vs api; never hard-code in arb logic).
CREATE TABLE IF NOT EXISTS fees (
  venue                   TEXT PRIMARY KEY REFERENCES venues(venue_id),
  source                  TEXT NOT NULL CHECK (source IN ('api','manual')),
  maker_fee               NUMERIC(10,8) NOT NULL,
  taker_fee               NUMERIC(10,8) NOT NULL,
  per_contract_fee        NUMERIC(12,8) NULL,
  settlement_fee          NUMERIC(12,8) NULL,
  withdrawal_fee          NUMERIC(12,8) NULL,
  deposit_fee             NUMERIC(12,8) NULL,
  network_fee             NUMERIC(12,8) NULL,
  currency_conversion_fee NUMERIC(10,8) NULL,
  tiers                   JSONB NULL,
  notes                   TEXT NULL,
  retrieved_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Balance snapshots per venue/currency.
CREATE TABLE IF NOT EXISTS balances (
  venue       TEXT NOT NULL REFERENCES venues(venue_id),
  currency    TEXT NOT NULL,
  available   NUMERIC(20,8) NOT NULL,
  locked      NUMERIC(20,8) NULL,
  total       NUMERIC(20,8) NULL,
  retrieved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (venue, currency)
);

-- Every detected opportunity (displayed vs executable distinguished by status + net figures).
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
  opportunity_id      TEXT PRIMARY KEY,
  detected_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_id            TEXT NULL REFERENCES events(event_id),
  event_name          TEXT NOT NULL,
  sport               TEXT NOT NULL,
  league              TEXT NOT NULL,
  venue_a             TEXT NOT NULL,
  venue_b             TEXT NOT NULL,
  market_a_id         TEXT NOT NULL,
  market_b_id         TEXT NOT NULL,
  leg_a               JSONB NOT NULL,   -- {side, price, size}
  leg_b               JSONB NOT NULL,
  available_liquidity NUMERIC(20,8) NOT NULL,
  gross_edge          NUMERIC(12,8) NOT NULL,
  venue_fees          NUMERIC(20,8) NOT NULL,
  network_costs       NUMERIC(20,8) NOT NULL DEFAULT 0,
  slippage_cost       NUMERIC(20,8) NOT NULL DEFAULT 0,
  fx_cost             NUMERIC(20,8) NOT NULL DEFAULT 0,
  net_profit          NUMERIC(20,8) NOT NULL,
  net_roi             NUMERIC(12,8) NOT NULL,
  max_size            NUMERIC(20,8) NOT NULL,
  min_size            NUMERIC(20,8) NOT NULL DEFAULT 0,
  recommended_size    NUMERIC(20,8) NOT NULL,
  match_confidence    NUMERIC(6,5) NOT NULL,
  execution_risk      TEXT NOT NULL DEFAULT '',
  status              TEXT NOT NULL DEFAULT 'DETECTED'
                      CHECK (status IN ('DETECTED','VALIDATED','EXPIRED','EXECUTING','COMPLETED','FAILED','REJECTED')),
  mode                TEXT NOT NULL DEFAULT 'PAPER'
                      CHECK (mode IN ('PAPER','DRY_RUN','MANUAL_CONFIRMATION','LIVE')),
  age_ms              INTEGER NULL,
  details             JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_opps_status_time ON arbitrage_opportunities (status, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_opps_event ON arbitrage_opportunities (event_id);

-- Orders (both paper and live; mode column prevents confusion).
CREATE TABLE IF NOT EXISTS orders (
  client_order_id TEXT PRIMARY KEY,
  venue_order_id  TEXT NULL,
  venue           TEXT NOT NULL,
  market_id       TEXT NOT NULL,
  opportunity_id  TEXT NULL REFERENCES arbitrage_opportunities(opportunity_id),
  side            TEXT NOT NULL CHECK (side IN ('buy','sell')),
  outcome         TEXT NULL,
  price           NUMERIC(12,8) NOT NULL,
  size            NUMERIC(20,8) NOT NULL,
  order_type      TEXT NOT NULL DEFAULT 'limit',
  time_in_force   TEXT NULL,
  status          TEXT NOT NULL DEFAULT 'pending',
  mode            TEXT NOT NULL DEFAULT 'PAPER'
                  CHECK (mode IN ('PAPER','DRY_RUN','MANUAL_CONFIRMATION','LIVE')),
  idempotency_key TEXT NULL UNIQUE,
  submitted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_opp ON orders (opportunity_id);
CREATE INDEX IF NOT EXISTS idx_orders_venue_status ON orders (venue, status);

-- Fills (never assume fill from order ack; only rows here count as filled).
CREATE TABLE IF NOT EXISTS fills (
  fill_id        TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  venue_order_id TEXT NOT NULL,
  client_order_id TEXT NULL REFERENCES orders(client_order_id),
  venue          TEXT NOT NULL,
  market_id      TEXT NOT NULL,
  price          NUMERIC(12,8) NOT NULL,
  size           NUMERIC(20,8) NOT NULL,
  fee_paid       NUMERIC(20,8) NULL,
  fee_currency   TEXT NULL,
  filled_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (venue, venue_order_id, fill_id)
);

-- Positions (net exposure per venue/market/event).
CREATE TABLE IF NOT EXISTS positions (
  position_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  venue       TEXT NOT NULL,
  market_id   TEXT NOT NULL,
  event_id    TEXT NULL REFERENCES events(event_id),
  outcome     TEXT NOT NULL,
  size        NUMERIC(20,8) NOT NULL,
  avg_price   NUMERIC(12,8) NOT NULL,
  mode        TEXT NOT NULL DEFAULT 'PAPER',
  opened_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (venue, market_id, outcome, mode)
);

-- Trades = completed round-trips (for PnL audit).
CREATE TABLE IF NOT EXISTS trades (
  trade_id       TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  opportunity_id TEXT NULL REFERENCES arbitrage_opportunities(opportunity_id),
  event_id       TEXT NULL,
  venue_a        TEXT NOT NULL,
  venue_b        TEXT NOT NULL,
  stake          NUMERIC(20,8) NOT NULL,
  payout         NUMERIC(20,8) NOT NULL,
  fees           NUMERIC(20,8) NOT NULL DEFAULT 0,
  net_pnl        NUMERIC(20,8) NOT NULL,
  mode           TEXT NOT NULL DEFAULT 'PAPER',
  closed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Risk events (limit breaches, kill-switch, etc.).
CREATE TABLE IF NOT EXISTS risk_events (
  event_id    TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  event_type  TEXT NOT NULL,  -- e.g. 'LIMIT_BREACH','KILL_SWITCH','STALE_DATA','VENUE_OFFLINE'
  severity    TEXT NOT NULL DEFAULT 'warn',
  venue       TEXT NULL,
  detail      JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_risk_type_time ON risk_events (event_type, created_at DESC);

-- System events (connector health, discovery, errors).
CREATE TABLE IF NOT EXISTS system_events (
  event_id       TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  service        TEXT NOT NULL,
  venue          TEXT NULL,
  event_type     TEXT NOT NULL,
  correlation_id TEXT NULL,
  detail         JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sys_service_time ON system_events (service, created_at DESC);

-- Seed venue registry with docs URLs only; api_base_url NULL until verified (no fabricated endpoints).
INSERT INTO venues (venue_id, display_name, docs_url, api_base_url, trading_allowed, fee_source, status)
VALUES
  ('polymarket', 'Polymarket', 'https://docs.polymarket.com', NULL, FALSE, 'manual', 'OFFLINE'),
  ('kalshi',     'Kalshi',     'https://docs.kalshi.com',     NULL, FALSE, 'manual', 'OFFLINE'),
  ('betfair',    'Betfair Exchange', 'https://docs.developer.betfair.com', NULL, FALSE, 'manual', 'OFFLINE')
ON CONFLICT (venue_id) DO NOTHING;
