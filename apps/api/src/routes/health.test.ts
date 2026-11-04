import { createLogger } from '@ticketsurge/config';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { describe, expect, it } from 'vitest';
import { unitTestConfig } from '../../test/support/config.js';
import { buildApp } from '../app.js';
import type { HealthCheck } from '../health/run-checks.js';

const logger = createLogger({ service: 'api-test', level: 'silent' });
const up: HealthCheck = () => Promise.resolve();
const down: HealthCheck = () => Promise.reject(new Error('ECONNREFUSED'));

async function getHealth(checks: Record<string, HealthCheck>) {
  const app = await buildApp({
    logger,
    config: unitTestConfig({ HEALTH_CHECK_TIMEOUT_MS: 100 }),
    // Health checks are faked, so the routes never touch the database or Redis.
    db: {} as pg.Pool,
    redis: {} as Redis,
    healthChecks: checks,
  });
  try {
    return await app.inject({ method: 'GET', url: '/health' });
  } finally {
    await app.close();
  }
}

describe('GET /health', () => {
  it('returns 200 when PostgreSQL, Redis and Kafka are up', async () => {
    const response = await getHealth({ postgres: up, redis: up, kafka: up });
    expect(response.statusCode).toBe(200);
    const body = response.json<{ status: string; checks: Record<string, { status: string }> }>();
    expect(body.status).toBe('ok');
    expect(Object.keys(body.checks).sort()).toEqual(['kafka', 'postgres', 'redis']);
  });

  it.each(['postgres', 'redis', 'kafka'])('returns 503 when %s is down', async (failing) => {
    const checks = { postgres: up, redis: up, kafka: up, [failing]: down };
    const response = await getHealth(checks);
    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({
      status: 'unavailable',
      checks: { [failing]: { status: 'down', error: 'ECONNREFUSED' } },
    });
  });

  it('returns 503 when a dependency hangs', async () => {
    const hung: HealthCheck = () => new Promise(() => undefined);
    const response = await getHealth({ postgres: up, redis: up, kafka: hung });
    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({ checks: { kafka: { status: 'down' } } });
  });
});
