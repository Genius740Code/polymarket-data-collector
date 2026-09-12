import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: [
      'packages/*/tests/**/*.test.ts',
      'services/*/tests/**/*.test.ts',
      'tests/**/*.test.ts',
    ],
    testTimeout: 10000,
    reporters: ['default'],
  },
});
