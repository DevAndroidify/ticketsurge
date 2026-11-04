import { loadEnvFile } from '@ticketsurge/config';

export const TEST_REDIS_DB = 1;

/**
 * Points integration tests at a separate database (`<db>_test`) and Redis DB 1, so tests can
 * truncate and flush freely without touching development data. Mutates process.env so every
 * config loader picks up the same values.
 */
export function useTestEnv(): { databaseUrl: string; redisUrl: string; adminDatabaseUrl: string } {
  loadEnvFile();
  const dev = process.env.DATABASE_URL;
  const redis = process.env.REDIS_URL;
  if (!dev || !redis) throw new Error('DATABASE_URL and REDIS_URL must be set (see .env.example)');

  const dbUrl = new URL(dev);
  const devName = dbUrl.pathname.replace(/^\//, '');
  const testName = devName.endsWith('_test') ? devName : `${devName}_test`;
  const adminUrl = new URL(dbUrl);
  adminUrl.pathname = '/postgres';
  dbUrl.pathname = `/${testName}`;

  const redisUrl = new URL(redis);
  redisUrl.pathname = `/${TEST_REDIS_DB}`;

  process.env.DATABASE_URL = dbUrl.toString();
  process.env.REDIS_URL = redisUrl.toString();
  process.env.NODE_ENV = 'test';
  process.env.LOG_LEVEL = process.env.TEST_LOG_LEVEL ?? 'silent';

  return {
    databaseUrl: dbUrl.toString(),
    redisUrl: redisUrl.toString(),
    adminDatabaseUrl: adminUrl.toString(),
  };
}
