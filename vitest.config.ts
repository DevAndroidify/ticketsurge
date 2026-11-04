import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

// Tests import workspace packages from their TypeScript sources, so no build is needed first.
const packagesDir = fileURLToPath(new URL('./packages', import.meta.url));
const alias = [{ find: /^@ticketsurge\/([^/]+)$/, replacement: `${packagesDir}/$1/src/index.ts` }];

export default defineConfig({
  test: {
    projects: [
      {
        resolve: { alias },
        test: {
          name: 'unit',
          environment: 'node',
          include: ['{apps,packages}/*/src/**/*.test.ts'],
        },
      },
      {
        resolve: { alias },
        test: {
          name: 'integration',
          environment: 'node',
          include: ['{apps,packages}/*/test/**/*.int.test.ts'],
          // Integration tests share real containers; run files one at a time.
          fileParallelism: false,
          testTimeout: 30_000,
          hookTimeout: 30_000,
        },
      },
    ],
  },
});
