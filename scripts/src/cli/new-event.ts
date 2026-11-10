// Opens a new 500-seat event on sale now, for when the demo event has sold out.
// The demo site sells from the newest open event with free seats, so it picks this one up.
// Usage: npm run demo:new-event
import { createPool, databaseUrlFromEnv } from '../lib/db-config.js';
import { createSaleEvent } from '../lib/seed.js';

const pool = createPool(databaseUrlFromEnv());
try {
  const time = new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  const { eventId, seats } = await createSaleEvent(pool, {
    name: `Flash sale ${time}`,
    venue: 'Demo Arena',
  });
  console.log(`event ${eventId} is on sale with ${seats} seats`);
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
