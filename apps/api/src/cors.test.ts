import { createLogger, loadConfig } from '@ticketsurge/config';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { describe, expect, it } from 'vitest';
import { unitTestConfig } from '../test/support/config.js';
import { buildApp } from './app.js';
import { apiConfigSchema } from './config.js';

const logger = createLogger({ service: 'api-test', level: 'silent' });
const SITE = 'https://demo.example.com';

async function withApp(
  origins: string[],
  run: (app: Awaited<ReturnType<typeof buildApp>>) => Promise<void>,
) {
  const app = await buildApp({
    logger,
    config: unitTestConfig({ CORS_ORIGINS: origins }),
    db: {} as pg.Pool,
    redis: {} as Redis,
    healthChecks: { postgres: () => Promise.resolve() },
  });
  try {
    await run(app);
  } finally {
    await app.close();
  }
}

describe('CORS', () => {
  it('answers a preflight from a listed origin, including Chrome’s local-network check', async () => {
    await withApp([SITE], async (app) => {
      const response = await app.inject({
        method: 'OPTIONS',
        url: '/events/abc/holds',
        headers: {
          origin: SITE,
          'access-control-request-method': 'POST',
          'access-control-request-private-network': 'true',
        },
      });
      expect(response.statusCode).toBe(204);
      expect(response.headers['access-control-allow-origin']).toBe(SITE);
      expect(response.headers['access-control-allow-headers']).toContain('idempotency-key');
      expect(response.headers['access-control-allow-private-network']).toBe('true');
    });
  });

  it('adds the origin header to real responses and exposes retry-after', async () => {
    await withApp([SITE], async (app) => {
      const response = await app.inject({
        method: 'GET',
        url: '/health',
        headers: { origin: SITE },
      });
      expect(response.headers['access-control-allow-origin']).toBe(SITE);
      expect(response.headers['access-control-expose-headers']).toContain('retry-after');
    });
  });

  it('gives an unlisted origin no CORS headers', async () => {
    await withApp([SITE], async (app) => {
      const response = await app.inject({
        method: 'GET',
        url: '/health',
        headers: { origin: 'https://evil.example' },
      });
      expect(response.headers['access-control-allow-origin']).toBeUndefined();
    });
  });

  it('is off by default', async () => {
    await withApp([], async (app) => {
      const response = await app.inject({
        method: 'GET',
        url: '/health',
        headers: { origin: SITE },
      });
      expect(response.headers['access-control-allow-origin']).toBeUndefined();
    });
  });

  it('parses CORS_ORIGINS and rejects values with a path', () => {
    const base = {
      DATABASE_URL: 'postgres://u:p@127.0.0.1:1/db',
      REDIS_URL: 'redis://127.0.0.1:1/0',
      KAFKA_BROKERS: '127.0.0.1:1',
      JWT_SECRET: 'unit-test-jwt-secret-0123456789abcdef',
    };
    const config = loadConfig(apiConfigSchema, {
      ...base,
      CORS_ORIGINS: ' https://a.example , http://localhost:5173/ ',
    });
    expect(config.CORS_ORIGINS).toEqual(['https://a.example', 'http://localhost:5173']);
    expect(() =>
      loadConfig(apiConfigSchema, { ...base, CORS_ORIGINS: 'https://a.example/app' }),
    ).toThrow();
  });
});
