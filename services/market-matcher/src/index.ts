// @arb/market-matcher — Phase 1: deterministic scoring, no auto-execution on guesses.
// Matches on sport/league/time/teams/market-type/settlement. Returns confidence 0..1 + reasons.
// Execution requires confidence >= threshold AND (approved || confidence == 1.0 on exact keys).

import { assertValidConfidence, type Market } from '@arb/types';

export interface MatchResult {
  confidence: number;
  reasons: string[];
  approved: boolean;
}

function norm(s: string | null | undefined): string {
  return (s ?? '').trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ');
}

export function computeMatchConfidence(a: Market, b: Market): MatchResult {
  const reasons: string[] = [];
  let score = 0;
  // Weights sum to 1.0. Settlement compatibility is a gate, not just points.
  if (norm(a.sport) && norm(a.sport) === norm(b.sport)) {
    score += 0.15;
    reasons.push('sport match');
  } else reasons.push('sport mismatch');
  if (norm(a.league) && norm(a.league) === norm(b.league)) {
    score += 0.15;
    reasons.push('league match');
  } else reasons.push('league mismatch');

  const ta = Date.parse(a.startTime);
  const tb = Date.parse(b.startTime);
  const dtMin = Math.abs(ta - tb) / 60000;
  if (Number.isFinite(dtMin) && dtMin <= 5) {
    score += 0.2;
    reasons.push('start-time match (<=5m)');
  } else if (Number.isFinite(dtMin) && dtMin <= 60) {
    score += 0.05;
    reasons.push('start-time close (<=60m)');
  } else reasons.push('start-time mismatch');

  const homeMatch = norm(a.homeTeam) !== '' && norm(a.homeTeam) === norm(b.homeTeam);
  const awayMatch = norm(a.awayTeam) !== '' && norm(a.awayTeam) === norm(b.awayTeam);
  const swapped =
    norm(a.homeTeam) !== '' &&
    (norm(a.homeTeam) === norm(b.awayTeam) || norm(a.awayTeam) === norm(b.homeTeam));
  if (homeMatch && awayMatch) {
    score += 0.25;
    reasons.push('teams match');
  } else if (swapped) {
    score += 0.05;
    reasons.push('teams possibly swapped — needs review');
  } else reasons.push('teams mismatch');

  if (norm(a.marketType) === norm(b.marketType)) {
    score += 0.1;
    reasons.push('market-type match');
  } else reasons.push('market-type mismatch');

  // Settlement compatibility gate: if either side unknown, cap confidence below execution threshold.
  const sa = a.settlementRules;
  const sb = b.settlementRules;
  const bothDeclared = Boolean(sa?.payoutModel && sb?.payoutModel);
  const payoutSame = bothDeclared && sa.payoutModel === sb.payoutModel;
  if (payoutSame) {
    score += 0.15;
    reasons.push('settlement payoutModel match');
  } else {
    reasons.push('settlement unverified — manual review required');
    score = Math.min(score, 0.6);
  }

  score = Math.max(0, Math.min(1, Math.round(score * 1000) / 1000));
  assertValidConfidence(score);
  return { confidence: score, reasons, approved: false };
}

/** Execution gate: confidence + approval + settlement declared. */
export function passesMatchGate(r: MatchResult, threshold: number, settlementVerified: boolean): boolean {
  if (!settlementVerified) return false;
  if (r.confidence < threshold) return false;
  if (r.confidence < 1 && !r.approved) return false;
  return true;
}
