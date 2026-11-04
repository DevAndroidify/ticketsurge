import { createPool, databaseUrlFromEnv } from '../lib/db-config.js';
import { seedDemoEvent } from '../lib/seed.js';

const pool = createPool(databaseUrlFromEnv());
try {
  const { eventId, seats } = await seedDemoEvent(pool);
  console.log(`demo event ${eventId} has ${seats} seats`);
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
