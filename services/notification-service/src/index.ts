// @arb/notification-service — Phase 1: alert formatting only (no sends).
// Transports (Telegram/Discord/Email/Webhook) are wired in later phases with env secrets.

export interface ArbAlert {
  eventName: string;
  venues: [string, string];
  netRoi: number;
  netProfit: number;
  maxSize: number;
  matchConfidence: number;
  ageMs: number;
}

export function formatArbAlert(a: ArbAlert): string {
  const roi = (a.netRoi * 100).toFixed(2);
  const conf = (a.matchConfidence * 100).toFixed(1);
  return [
    'ARBITRAGE DETECTED (PAPER — verify before any live action)',
    '',
    a.eventName,
    '',
    `${a.venues[0]} + ${a.venues[1]}`,
    '',
    `Net ROI: ${roi}%`,
    `Expected profit: $${a.netProfit.toFixed(2)}`,
    `Maximum size: $${a.maxSize.toFixed(2)}`,
    `Match confidence: ${conf}%`,
    `Opportunity age: ${a.ageMs}ms`,
  ].join('\n');
}
