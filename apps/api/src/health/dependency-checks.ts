import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { Redis } from 'ioredis';
import type { Pool } from 'pg';
import type { HealthCheck } from './run-checks.js';

type Admin = KafkaJS.Admin;

/** Runs a trivial query, which needs a pooled connection and a responsive server. */
export function postgresCheck(pool: Pool): HealthCheck {
  return async () => {
    await pool.query('SELECT 1');
  };
}

/**
 * The offline queue is disabled (see clients.ts), so commands fail instantly while the client is
 * still connecting. Wait for the connection first, bounded by the timeout, so a check that runs
 * right after startup or a reconnect does not report a healthy Redis as down.
 */
export function redisCheck(redis: Redis, timeoutMs: number): HealthCheck {
  return async () => {
    await waitForRedisReady(redis, timeoutMs);
    await redis.ping();
  };
}

function waitForRedisReady(redis: Redis, timeoutMs: number): Promise<void> {
  if (redis.status === 'ready') return Promise.resolve();
  return new Promise((resolve, reject) => {
    const onReady = () => {
      cleanup();
      resolve();
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`redis not connected (status: ${redis.status})`));
    }, timeoutMs);
    const cleanup = () => {
      clearTimeout(timer);
      redis.off('ready', onReady);
    };
    redis.on('ready', onReady);
  });
}

/**
 * Lists topics through the admin client, which requires a metadata round trip to a broker.
 * The admin connection is opened lazily and retried on the next check if it fails, so the API
 * can start while Kafka is still down and report healthy once it comes up.
 */
export function kafkaCheck(admin: Admin, timeoutMs: number): HealthCheck {
  let connecting: Promise<void> | undefined;

  return async () => {
    connecting ??= admin.connect().catch((error: unknown) => {
      connecting = undefined;
      throw error;
    });
    await connecting;
    await admin.listTopics({ timeout: timeoutMs });
  };
}
