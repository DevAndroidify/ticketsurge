import { createPool, databaseUrlFromEnv } from '../lib/db-config.js';
import { runMigrations } from '../lib/migrations.js';

const pool = createPool(databaseUrlFromEnv());
try {
  const applied = await runMigrations(pool, undefined, (message) => {
    console.log(message);
  });
  console.log(applied.length ? `${applied.length} migration(s) applied` : 'database is up to date');
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
