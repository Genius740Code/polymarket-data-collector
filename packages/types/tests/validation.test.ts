import { describe, expect, it } from 'vitest';
import { assertValidConfidence, assertValidProbability, complementPrice, isValidProbability } from '@arb/types';

describe('probability validation', () => {
  it('accepts 0..1 inclusive', () => {
    expect(isValidProbability(0)).toBe(true);
    expect(isValidProbability(1)).toBe(true);
    expect(isValidProbability(0.46)).toBe(true);
  });
  it('rejects out-of-range / NaN', () => {
    expect(isValidProbability(-0.01)).toBe(false);
    expect(isValidProbability(1.01)).toBe(false);
    expect(isValidProbability(NaN)).toBe(false);
    expect(() => assertValidProbability(1.5)).toThrow();
    expect(() => assertValidConfidence(2)).toThrow();
  });
  it('complement requires explicit declaration (never blind NO=1-YES)', () => {
    expect(complementPrice(0.46, true)).toBeCloseTo(0.54, 10);
    expect(complementPrice(0.46, false)).toBeNull();
  });
});
