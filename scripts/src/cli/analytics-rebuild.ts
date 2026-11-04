// Rebuilds analytics.sales_by_event from Kafka. Stop analytics-service, run this, start it again.
//
// Order matters: the offset reset runs first and refuses while the service is still connected, so
// the tables are only emptied once nothing can write to them. Only messages still retained by
// Kafka (7 days by default) are replayed, so a rebuild reflects that window.
import { CONSUMER_GROUPS } from '@ticketsurge/events';
import { resetGroupToEarliest } from '@ticketsurge/kafka';
import { createPool, databaseUrlFromEnv } from '../lib/db-config.js';
import { withAdmin } from '../lib/kafka-admin.js';

try {
  await withAdmin((admin, prefix) =>
    resetGroupToEarliest(admin, `${prefix}${CONSUMER_GROUPS.analytics}`),
  );
  const pool = createPool(databaseUrlFromEnv());
  try {
    await pool.query('TRUNCATE analytics.sales_by_event, analytics.processed_events');
  } finally {
    await pool.end();
  }
  console.log('analytics cleared and offsets reset; start analytics-service to rebuild from Kafka');
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
}
