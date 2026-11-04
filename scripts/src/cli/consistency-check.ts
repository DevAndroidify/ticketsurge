// Checks the end-of-test invariants. Exit code 1 if any of them fails.
// Usage: npm run consistency:check [-- <eventId> ...]
import { loadConfig, loadEnvFile, postgresUrlSchema, redisUrlSchema } from '@ticketsurge/config';
import { Redis } from 'ioredis';
import { z } from 'zod';
import { checkConsistency, isConsistent } from '../lib/consistency.js';
import { createPool } from '../lib/db-config.js';

loadEnvFile();
const config = loadConfig(z.object({ DATABASE_URL: postgresUrlSchema, REDIS_URL: redisUrlSchema }));
const eventIds = process.argv.slice(2).filter((arg) => /^[0-9a-f-]{36}$/.test(arg));
const pool = createPool(config.DATABASE_URL);
const redis = new Redis(config.REDIS_URL);
try {
  const report = await checkConsistency(pool, redis, eventIds.length > 0 ? { eventIds } : {});
  console.log(JSON.stringify(report, null, 2));
  console.log(isConsistent(report) ? 'CONSISTENT' : 'INCONSISTENT');
  if (!isConsistent(report)) process.exitCode = 1;
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
} finally {
  await redis.quit();
  await pool.end();
}
