import { loadConfig, loadEnvFile, postgresUrlSchema } from '@ticketsurge/config';
import pg from 'pg';
import { z } from 'zod';

const schema = z.object({ DATABASE_URL: postgresUrlSchema });

/** Validated DATABASE_URL from the environment or the nearest .env file. */
export function databaseUrlFromEnv(): string {
  loadEnvFile();
  return loadConfig(schema).DATABASE_URL;
}

export function createPool(connectionString: string): pg.Pool {
  return new pg.Pool({ connectionString, max: 4 });
}
