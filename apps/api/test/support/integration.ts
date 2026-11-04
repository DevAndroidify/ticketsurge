// Integration harness: real PostgreSQL/Redis/Kafka from docker compose, test DB + Redis DB 1.
import { randomUUID } from 'node:crypto';
import { createLogger, loadConfig } from '@ticketsurge/config';
import type pg from 'pg';
import { useTestEnv } from '../../../../scripts/src/testing/test-env.js';
import { buildApp, tokenServiceFrom } from '../../src/app.js';
import { createClients, type Clients } from '../../src/clients.js';
import { apiConfigSchema, type ApiConfig } from '../../src/config.js';
import { kafkaCheck, postgresCheck, redisCheck } from '../../src/health/dependency-checks.js';

useTestEnv();

export const logger = createLogger({
  service: 'api-int-test',
  level: process.env.TEST_LOG_LEVEL === undefined ? 'silent' : 'debug',
});

export const TEST_ADMIN_EMAIL = 'admin@test.local';

export function integrationConfig(overrides: Partial<ApiConfig> = {}): ApiConfig {
  return { ...loadConfig(apiConfigSchema), ADMIN_EMAILS: [TEST_ADMIN_EMAIL], ...overrides };
}

export interface TestUser {
  id: string;
  email: string;
  token: string;
}

/** Registers a real user through the API (so it exists in booking.users). */
export async function registerUser(t: TestApp, email = `user-${randomUUID()}@test.local`) {
  const response = await t.app.inject({
    method: 'POST',
    url: '/auth/register',
    payload: { email, password: 'correct horse battery' },
  });
  if (response.statusCode !== 201) throw new Error(`register failed: ${response.body}`);
  const body = response.json<{ user: { id: string; email: string }; accessToken: string }>();
  return { ...body.user, token: body.accessToken } satisfies TestUser;
}

/** A valid token for a user ID that need not exist in the database (Redis-only tests). */
export async function tokenFor(t: TestApp, id: string = randomUUID()): Promise<TestUser> {
  const email = `synthetic-${id}@test.local`;
  const { token } = await tokenServiceFrom(t.config).sign({ id, email });
  return { id, email, token };
}

export const bearer = (user: { token: string }) => ({ authorization: `Bearer ${user.token}` });

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
  await db.query('TRUNCATE booking.events, booking.users CASCADE');
}
