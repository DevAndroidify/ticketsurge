// Integration harness: real PostgreSQL/Redis/Kafka from docker compose, test DB + Redis DB 1.
import { randomUUID } from 'node:crypto';
import { createLogger, loadConfig } from '@ticketsurge/config';
import { keys } from '@ticketsurge/redis-scripts';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { TEST_REDIS_DB, useTestEnv } from '../../../../scripts/src/testing/test-env.js';
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
  return {
    ...loadConfig(apiConfigSchema),
    ADMIN_EMAILS: [TEST_ADMIN_EMAIL],
    // Tests fire many requests per user; the rate-limit test sets its own low limits.
    RATE_LIMIT_QUEUE_JOIN: 10_000,
    RATE_LIMIT_HOLDS: 10_000,
    RATE_LIMIT_BOOKINGS: 10_000,
    ...overrides,
  };
}

/** Marks a user as admitted for an event, as the admission worker would. */
export async function admit(t: TestApp, eventId: string, userId: string, ttlMs = 300_000) {
  await t.clients.redis.set(keys.admit(eventId, userId), '1', 'PX', ttlMs);
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
    redis: clients.redis,
    healthChecks: {
      postgres: postgresCheck(clients.pg),
      redis: redisCheck(clients.redis, config.HEALTH_CHECK_TIMEOUT_MS),
      kafka: kafkaCheck(clients.kafkaAdmin, config.HEALTH_CHECK_TIMEOUT_MS),
    },
  });
  // Tests send requests immediately, so wait for Redis to connect, unless this test points it
  // at an unreachable address on purpose.
  if (overrides.REDIS_URL === undefined) await redisReady(clients.redis);
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
  await db.query('TRUNCATE booking.events, booking.users, booking.outbox CASCADE');
}

/** Admits the user, holds the seats, and creates a PENDING_PAYMENT booking through the API. */
export async function bookSeats(
  t: TestApp,
  user: TestUser,
  eventId: string,
  seatIds: string[],
): Promise<{ bookingId: string; holdId: string }> {
  await admit(t, eventId, user.id);
  const hold = await t.app.inject({
    method: 'POST',
    url: `/events/${eventId}/holds`,
    headers: bearer(user),
    payload: { seatIds },
  });
  if (hold.statusCode !== 201) throw new Error(`hold failed: ${hold.body}`);
  const { holdId } = hold.json<{ holdId: string }>();
  const booking = await t.app.inject({
    method: 'POST',
    url: '/bookings',
    headers: { ...bearer(user), 'idempotency-key': randomUUID() },
    payload: { holdId },
  });
  if (booking.statusCode !== 202) throw new Error(`booking failed: ${booking.body}`);
  return { bookingId: booking.json<{ bookingId: string }>().bookingId, holdId };
}

/** Flushes the test Redis DB. Refuses to touch any other DB. */
export async function resetRedis(redis: Redis): Promise<void> {
  if (redis.options.db !== TEST_REDIS_DB) throw new Error('refusing to flush a non-test Redis DB');
  await redis.flushdb();
}

/** Waits until the client is connected; integration tests run right after client creation. */
export async function redisReady(redis: Redis): Promise<void> {
  if (redis.status === 'ready') return;
  await new Promise<void>((resolve) => {
    redis.once('ready', () => {
      resolve();
    });
  });
}
