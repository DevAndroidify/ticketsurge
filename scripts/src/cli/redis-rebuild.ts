// Restores sold seats and booked holds in Redis from PostgreSQL. Run after Redis lost its data.
import {
  durationMsSchema,
  loadConfig,
  loadEnvFile,
  postgresUrlSchema,
  redisUrlSchema,
} from '@ticketsurge/config';
import { Redis } from 'ioredis';
import { z } from 'zod';
import { createPool } from '../lib/db-config.js';
import { rebuildRedis } from '../lib/redis-rebuild.js';

loadEnvFile();
const config = loadConfig(
  z.object({
    DATABASE_URL: postgresUrlSchema,
    REDIS_URL: redisUrlSchema,
    HOLD_AFTER_DEADLINE_MS: durationMsSchema.default(60_000),
  }),
);
const pool = createPool(config.DATABASE_URL);
const redis = new Redis(config.REDIS_URL);
const started = performance.now();
try {
  const result = await rebuildRedis(pool, redis, {
    holdAfterDeadlineMs: config.HOLD_AFTER_DEADLINE_MS,
  });
  console.log(
    `redis rebuilt in ${Math.round(performance.now() - started)} ms: ` +
      `${result.soldSeats} sold seats (${result.soldBookings} bookings), ` +
      `${result.heldSeats} held seats (${result.heldBookings} pending bookings)`,
  );
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
} finally {
  await redis.quit();
  await pool.end();
}
