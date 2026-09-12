// Dashboard placeholder (Phase 1): required routes per spec.
// Full Next.js/React implementation lands in Phase 9. This file pins the route contract
// so backend OpenAPI stays aligned. No secrets here — dashboard calls backend API only.
export const DASHBOARD_ROUTES = [
  '/dashboard',
  '/markets',
  '/arbitrage',
  '/opportunities',
  '/orders',
  '/positions',
  '/balances',
  '/fees',
  '/risk',
  '/history',
  '/settings',
] as const;
export type DashboardRoute = (typeof DASHBOARD_ROUTES)[number];
