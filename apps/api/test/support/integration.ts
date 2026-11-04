// Integration harness: real PostgreSQL/Redis/Kafka from docker compose, test DB + Redis DB 1.
import { createLogger, loadConfig } from '@ticketsurge/config';
import type pg from 'pg';
import { useTestEnv } from '../../../../scripts/src/testing/test-env.js';
import { buildApp } from '../../src/app.js';
import { createClients, type Clients } from '../../src/clients.js';
import { apiConfigSchema, type ApiConfig } from '../../src/config.js';
import { kafkaCheck, postgresCheck, redisCheck } from '../../src/health/dependency-checks.js';

useTestEnv();

export const logger = createLogger({
  service: 'api-int-test',
  level: process.env.TEST_LOG_LEVEL === undefined ? 'silent' : 'debug',
});

export function integrationConfig(overrides: Partial<ApiConfig> = {}): ApiConfig {
  return { ...loadConfig(apiConfigSchema), ...overrides };
}

export interface TestApp {
  app: Awaited<ReturnType<typeof buildApp>>;
  config: ApiConfig;
  clients: Clients;
  close(): Promise<void>;
}

export async function createTestApp(overrides: Partial<ApiConfig> = {}): Promise<TestApp> {
  const config = integrationConfig(overrides);
  const clients = createClients(config, logger);
  const app = await buildApp({
    logger,
    config,
    db: clients.pg,
    healthChecks: {
      postgres: postgresCheck(clients.pg),
      redis: redisCheck(clients.redis, config.HEALTH_CHECK_TIMEOUT_MS),
      kafka: kafkaCheck(clients.kafkaAdmin, config.HEALTH_CHECK_TIMEOUT_MS),
    },
  });
  return {
    app,
    config,
    clients,
    async close() {
      await app.close();
      await clients.close();
    },
  };
}

/** Empties every booking table. Integration test files run one at a time, so this is safe. */
export async function resetDatabase(db: pg.Pool): Promise<void> {
  await db.query('TRUNCATE booking.events CASCADE');
}
