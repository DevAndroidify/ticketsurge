// Vitest global setup for the integration project: creates and migrates the test database once.
import pg from 'pg';
import { runMigrations } from '../lib/migrations.js';
import { deleteAllTestTopics } from './kafka.js';
import { useTestEnv } from './test-env.js';

export default async function setup(): Promise<void> {
  const { databaseUrl, adminDatabaseUrl } = useTestEnv();
  const dbName = new URL(databaseUrl).pathname.slice(1);

  const admin = new pg.Client({ connectionString: adminDatabaseUrl });
  await admin.connect();
  try {
    const { rowCount } = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [
      dbName,
    ]);
    if (!rowCount) {
      // Identifiers cannot be parameterised; the name comes from our own .env and is quoted.
      await admin.query(`CREATE DATABASE "${dbName.replaceAll('"', '""')}"`);
    }
  } finally {
    await admin.end();
  }

  const pool = new pg.Pool({ connectionString: databaseUrl, max: 1 });
  try {
    await runMigrations(pool);
  } finally {
    await pool.end();
  }

  // Each Kafka test file creates its own topics; remove any left by interrupted runs.
  await deleteAllTestTopics();
}
