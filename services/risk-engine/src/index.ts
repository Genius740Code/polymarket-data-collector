// @arb/risk-engine — Phase 1: hard limit checks + kill switch. Fail-safe: any breach => STOP new orders.

import type { RiskLimits } from '@arb/types';

export interface RiskState {
  dailyPnl: number;
  openPositions: number;
  venueExposure: Record<string, number>;
  eventExposure: Record<string, number>;
  totalExposure: number;
}

export interface RiskCheck {
  allowed: boolean;
  breaches: string[];
}

/** Pure check: returns breaches; caller must persist to risk_events and enforce kill switch. */
export function checkRiskLimits(
  limits: RiskLimits,
  state: RiskState,
  order: { venue: string; eventId: string; notional: number; estSlippage: number; marketAgeMs: number },
): RiskCheck {
  const breaches: string[] = [];
  if (limits.killSwitch) breaches.push('KILL_SWITCH engaged — all new orders blocked');
  if (order.notional > limits.maxCapitalPerTrade) breaches.push(`notional ${order.notional} > maxCapitalPerTrade ${limits.maxCapitalPerTrade}`);
  if (order.notional > limits.maxOrderSize) breaches.push(`notional ${order.notional} > maxOrderSize ${limits.maxOrderSize}`);
  if (order.estSlippage > limits.maxSlippage) breaches.push(`slippage ${order.estSlippage} > max ${limits.maxSlippage}`);
  if (order.marketAgeMs > limits.maxMarketAgeMs) breaches.push(`market age ${order.marketAgeMs}ms > max ${limits.maxMarketAgeMs}ms`);
  const venueExp = (state.venueExposure[order.venue] ?? 0) + order.notional;
  if (venueExp > limits.maxCapitalPerVenue) breaches.push(`venue exposure ${venueExp} > max ${limits.maxCapitalPerVenue}`);
  const evExp = (state.eventExposure[order.eventId] ?? 0) + order.notional;
  if (evExp > limits.maxEventExposure) breaches.push(`event exposure ${evExp} > max ${limits.maxEventExposure}`);
  if (state.totalExposure + order.notional > limits.maxExposure) breaches.push('total exposure breach');
  if (state.openPositions + 1 > limits.maxOpenPositions) breaches.push('max open positions breach');
  if (state.dailyPnl <= -Math.abs(limits.maxDailyLoss)) breaches.push('max daily loss breached');
  return { allowed: breaches.length === 0, breaches };
}
