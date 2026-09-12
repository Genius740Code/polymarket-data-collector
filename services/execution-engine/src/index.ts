// @arb/execution-engine — Phase 1: mode gate + leg state machine types.
// Default PAPER. LIVE requires explicit operator action (enforced in apps/api, not here).
// Never assume fill from order ack: only Fill records count.

import type { LegStatus, Order, OrderResult, TradingMode } from '@arb/types';

export interface ExecutionRequest {
  mode: TradingMode;
  orders: [Order, Order]; // exactly two legs for binary arb
  opportunityId: string;
  idempotencyPrefix: string;
}

export interface LegState {
  order: Order;
  status: LegStatus;
  result?: OrderResult | null;
  filledSize: number;
  attempts: number;
}

export type UnwindPolicy = 'retry' | 'cancel' | 'hedge' | 'unwind' | 'alert';

export function defaultMode(): TradingMode {
  return 'PAPER';
}

/** Phase 1 paper-fill simulator hook: deterministic echo used ONLY in tests/simulation, never as market data. */
export function paperFill(order: Order): { filledSize: number; avgPrice: number } {
  // Simulates an immediate full fill at limit price; realistic partial/latency modeling lands in Phase 8.
  return { filledSize: order.size, avgPrice: order.price };
}

export function initialLegs(req: ExecutionRequest): [LegState, LegState] {
  const mk = (o: Order): LegState => ({ order: o, status: 'UNFILLED', filledSize: 0, attempts: 0 });
  return [mk(req.orders[0]), mk(req.orders[1])];
}
