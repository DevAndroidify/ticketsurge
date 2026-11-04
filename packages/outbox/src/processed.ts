import type pg from 'pg';

/** A schema-qualified processed_events table, e.g. "payment.processed_events". */
export type ProcessedTable = `${string}.processed_events`;

/**
 * Records that `consumer` handled `messageId`. Returns false if it already had, in which case the
 * caller must roll back and only commit the Kafka offset.
 */
export async function claimMessage(
  client: pg.PoolClient,
  table: ProcessedTable,
  consumer: string,
  messageId: string,
): Promise<boolean> {
  if (!/^[a-z_]+\.processed_events$/.test(table)) throw new Error(`invalid table: ${table}`);
  const { rowCount } = await client.query(
    `INSERT INTO ${table} (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
    [consumer, messageId],
  );
  return rowCount === 1;
}

/**
 * The idempotent-consumer pattern from the brief:
 *
 *   BEGIN
 *     INSERT INTO processed_events ... ON CONFLICT DO NOTHING
 *     if 0 rows inserted: ROLLBACK and stop (the caller then commits the Kafka offset)
 *     apply the change, add any new events to the outbox     <- `apply`
 *   COMMIT
 *
 * The claim and the change commit together, so a redelivered or replayed message is never applied
 * twice. Returns false when the message was a duplicate.
 */
export async function processOnce(
  db: pg.Pool,
  table: ProcessedTable,
  consumer: string,
  messageId: string,
  apply: (client: pg.PoolClient) => Promise<void>,
): Promise<boolean> {
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    if (!(await claimMessage(client, table, consumer, messageId))) {
      await client.query('ROLLBACK');
      return false;
    }
    await apply(client);
    await client.query('COMMIT');
    return true;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}
