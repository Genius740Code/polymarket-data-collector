import { describe, expect, it } from 'vitest';
import { loadConfig, publicConfig } from '@arb/config';

describe('config', () => {
  it('defaults to PAPER mode', () => {
    const cfg = loadConfig({} as NodeJS.ProcessEnv);
    expect(cfg.tradingMode).toBe('PAPER');
    expect(cfg.minNetRoi).toBe(0.01);
    expect(cfg.matchConfidenceThreshold).toBe(0.95);
  });
  it('rejects invalid TRADING_MODE', () => {
    expect(() => loadConfig({ TRADING_MODE: 'YOLO' } as NodeJS.ProcessEnv)).toThrow();
  });
  it('rejects bad thresholds', () => {
    expect(() => loadConfig({ MIN_NET_ROI: '5' } as NodeJS.ProcessEnv)).toThrow();
    expect(() => loadConfig({ MATCH_CONFIDENCE_THRESHOLD: '1.5' } as NodeJS.ProcessEnv)).toThrow();
  });
  it('publicConfig never leaks credentials', () => {
    const cfg = loadConfig({ POLYMARKET_PRIVATE_KEY: 'shh', TRADING_MODE: 'PAPER' } as unknown as NodeJS.ProcessEnv);
    const pub = JSON.stringify(publicConfig(cfg));
    expect(pub).not.toContain('shh');
    expect(pub).toContain('PAPER');
  });
  it('LIVE + kill switch refuses to load', () => {
    expect(() => loadConfig({ TRADING_MODE: 'LIVE', KILL_SWITCH: 'true' } as NodeJS.ProcessEnv)).toThrow();
  });
});
