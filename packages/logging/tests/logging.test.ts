import { describe, expect, it } from 'vitest';
import { Logger, redact } from '@arb/logging';

describe('logging redaction', () => {
  it('redacts secret keys', () => {
    const out = redact({ apiKey: 'abc', nested: { password: 'x', ok: 1 } }) as Record<string, unknown>;
    expect(out['apiKey']).toBe('[REDACTED]');
    expect((out['nested'] as Record<string, unknown>)['password']).toBe('[REDACTED]');
    expect((out['nested'] as Record<string, unknown>)['ok']).toBe(1);
  });
  it('emits JSON with required fields and no secrets', () => {
    const lines: string[] = [];
    const log = new Logger('test-svc', { sink: (l) => lines.push(l) });
    log.info('hello', { venue: 'polymarket', apiKey: 'SHOULD_NOT_APPEAR' });
    expect(lines).toHaveLength(1);
    const rec = JSON.parse(lines[0] as string) as Record<string, unknown>;
    expect(rec['service']).toBe('test-svc');
    expect(rec['venue']).toBe('polymarket');
    expect(JSON.stringify(rec)).not.toContain('SHOULD_NOT_APPEAR');
    expect(rec['timestamp']).toBeTruthy();
  });
});
