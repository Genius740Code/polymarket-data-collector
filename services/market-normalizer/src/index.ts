// @arb/market-normalizer — Phase 1: validation + explicit binary normalization.
// Rule: connector MUST declare settlement semantics; never assume NO == 1-YES blindly.

import { assertValidProbability, type Market } from '@arb/types';

export interface NormalizationResult {
  market: Market;
  warnings: string[];
}

/** Validate a normalized market; throws on bad price/quantity/status. */
export function validateMarket(m: Market): string[] {
  const warnings: string[] = [];
  assertValidProbability(m.price, `market.price[${m.venue}:${m.venueMarketId}]`);
  if (m.quantity !== undefined && m.quantity !== null && !(m.quantity >= 0)) {
    throw new Error('market.quantity must be >= 0 or null');
  }
  if (Number.isNaN(Date.parse(m.startTime))) throw new Error('market.startTime must be ISO-8601');
  if (Number.isNaN(Date.parse(m.timestamp))) throw new Error('market.timestamp must be ISO-8601');
  if (!m.settlementRules?.description) warnings.push('missing settlementRules.description');
  if (m.priceModel === 'unknown') warnings.push('priceModel unknown: cannot compare across venues');
  return warnings;
}

/**
 * Normalize a binary YES price to its complement ONLY when venue declares complement semantics.
 * Returns null when semantics unknown (caller must not assume).
 */
export function binaryComplement(yesPrice: number, declaresComplement: boolean): number | null {
  assertValidProbability(yesPrice, 'yesPrice');
  return declaresComplement ? 1 - yesPrice : null;
}
