// @arb/fee-engine — Phase 1: pure fee math. No hard-coded venue fees here;
// schedules come from connector.getFees() (source marks api vs manual) or DB.

import type { FeeSchedule } from '@arb/types';

export interface FeeQuote {
  venue: string;
  notional: number;
  fee: number;
  source: 'api' | 'manual';
  detail: string;
}

/** Taker fee on notional + optional per-contract fee. All rates are fractions. */
export function quoteTakerFee(schedule: FeeSchedule, notional: number, contracts: number): FeeQuote {
  if (!(notional >= 0)) throw new Error('notional must be >= 0');
  if (!(contracts >= 0)) throw new Error('contracts must be >= 0');
  const pct = notional * schedule.takerFee;
  const per = (schedule.perContractFee ?? 0) * contracts;
  const settlement = schedule.settlementFee ?? 0;
  return {
    venue: schedule.venue,
    notional,
    fee: pct + per + settlement,
    source: schedule.source,
    detail: `taker=${schedule.takerFee}*${notional}+perContract=${schedule.perContractFee ?? 0}*${contracts}+settlement=${settlement}`,
  };
}

export function quoteMakerFee(schedule: FeeSchedule, notional: number, contracts: number): FeeQuote {
  if (!(notional >= 0)) throw new Error('notional must be >= 0');
  const pct = notional * schedule.makerFee;
  const per = (schedule.perContractFee ?? 0) * contracts;
  return {
    venue: schedule.venue,
    notional,
    fee: pct + per,
    source: schedule.source,
    detail: `maker=${schedule.makerFee}*${notional}+perContract=${schedule.perContractFee ?? 0}*${contracts}`,
  };
}
