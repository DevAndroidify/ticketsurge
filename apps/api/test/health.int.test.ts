// Runs against the containers from infra/docker-compose.yml (npm run infra:up).
import { describe, expect, it } from 'vitest';
import type { ApiConfig } from '../src/config.js';
import { createTestApp } from './support/integration.js';

// Port 1 on localhost is never listening, so connections are refused.
const unreachable: [name: string, overrides: Partial<ApiConfig>][] = [
  ['postgres', { DATABASE_URL: 'postgres://nobody:nothing@127.0.0.1:1/none' }],
  ['redis', { REDIS_URL: 'redis://127.0.0.1:1/0' }],
  ['kafka', { KAFKA_BROKERS: ['127.0.0.1:1'] }],
];

async function healthWith(overrides: Partial<ApiConfig>) {
  const testApp = await createTestApp(overrides);
  try {
    const started = performance.now();
    const response = await testApp.app.inject({ method: 'GET', url: '/health' });
    return { response, elapsedMs: performance.now() - started, config: testApp.config };
  } finally {
    await testApp.close();
  }
}

describe('GET /health against real dependencies', () => {
  it('returns 200 after checking PostgreSQL, Redis and Kafka', async () => {
    const { response } = await healthWith({});
    expect(response.statusCode, response.body).toBe(200);
    expect(response.json()).toMatchObject({
      status: 'ok',
      checks: {
        postgres: { status: 'up' },
        redis: { status: 'up' },
        kafka: { status: 'up' },
      },
    });
  });

  it.each(unreachable)(
    'returns 503 within the timeout when %s is unreachable',
    async (name, overrides) => {
      const { response, elapsedMs, config } = await healthWith(overrides);
      expect(response.statusCode).toBe(503);
      const body = response.json<{ checks: Record<string, { status: string }> }>();
      expect(body.checks[name]?.status).toBe('down');
      // The other two dependencies are still reported as up.
      for (const other of ['postgres', 'redis', 'kafka'].filter((n) => n !== name)) {
        expect(body.checks[other]?.status).toBe('up');
      }
      expect(elapsedMs).toBeLessThan(config.HEALTH_CHECK_TIMEOUT_MS + 1000);
    },
  );
});
