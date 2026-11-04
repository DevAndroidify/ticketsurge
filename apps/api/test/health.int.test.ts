// Runs against the containers from infra/docker-compose.yml (npm run infra:up).
import { createLogger, loadConfig, loadEnvFile } from '@ticketsurge/config';
import { beforeAll, describe, expect, it } from 'vitest';
import { buildApp } from '../src/app.js';
import { createClients, type Clients } from '../src/clients.js';
import { apiConfigSchema, type ApiConfig } from '../src/config.js';
import { kafkaCheck, postgresCheck, redisCheck } from '../src/health/dependency-checks.js';

loadEnvFile();
const config = loadConfig(apiConfigSchema);
const logger = createLogger({ service: 'api-int-test', level: 'silent' });

// Port 1 on localhost is never listening, so connections are refused.
const unreachable: [name: string, overrides: Partial<ApiConfig>][] = [
  ['postgres', { DATABASE_URL: 'postgres://nobody:nothing@127.0.0.1:1/none' }],
  ['redis', { REDIS_URL: 'redis://127.0.0.1:1/0' }],
  ['kafka', { KAFKA_BROKERS: ['127.0.0.1:1'] }],
];

async function healthWith(overrides: Partial<ApiConfig>) {
  const merged = { ...config, ...overrides };
  const clients: Clients = createClients(merged, logger);
  const app = await buildApp({
    logger,
    healthCheckTimeoutMs: merged.HEALTH_CHECK_TIMEOUT_MS,
    healthChecks: {
      postgres: postgresCheck(clients.pg),
      redis: redisCheck(clients.redis, merged.HEALTH_CHECK_TIMEOUT_MS),
      kafka: kafkaCheck(clients.kafkaAdmin, merged.HEALTH_CHECK_TIMEOUT_MS),
    },
  });
  try {
    const started = performance.now();
    const response = await app.inject({ method: 'GET', url: '/health' });
    return { response, elapsedMs: performance.now() - started };
  } finally {
    await app.close();
    await clients.close();
  }
}

describe('GET /health against real dependencies', () => {
  let healthy: Awaited<ReturnType<typeof healthWith>>;

  beforeAll(async () => {
    healthy = await healthWith({});
  });

  it('returns 200 after checking PostgreSQL, Redis and Kafka', () => {
    expect(healthy.response.statusCode, healthy.response.body).toBe(200);
    expect(healthy.response.json()).toMatchObject({
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
      const { response, elapsedMs } = await healthWith(overrides);
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
