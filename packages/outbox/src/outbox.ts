import type { Logger } from '@ticketsurge/config';
import { messageKey, type AnyEnvelope } from '@ticketsurge/events';
import type { Producer } from '@ticketsurge/kafka';
import type pg from 'pg';

/** A schema-qualified outbox table, e.g. "booking.outbox". Validated because it is interpolated. */
export type OutboxTable = `${string}.outbox`;

function assertTable(table: string): asserts table is OutboxTable {
  if (!/^[a-z_]+\.outbox$/.test(table)) throw new Error(`invalid outbox table: ${table}`);
}

/**
 * Adds a message to the outbox. Call it with the client of the transaction that makes the data
 * change, so the change and the message commit (or roll back) together.
 */
export async function insertOutboxMessage(
  client: pg.PoolClient,
  table: OutboxTable,
  topic: string,
  envelope: AnyEnvelope,
): Promise<void> {
  assertTable(table);
  await client.query(
    `INSERT INTO ${table} (message_id, topic, message_key, payload) VALUES ($1, $2, $3, $4)`,
    [envelope.messageId, topic, messageKey(envelope), JSON.stringify(envelope)],
  );
}

/** Number of rows not yet published. The sweeper pauses when this grows too large. */
export async function outboxBacklog(db: pg.Pool, table: OutboxTable): Promise<number> {
  assertTable(table);
  const { rows } = await db.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM ${table} WHERE published_at IS NULL`,
  );
  return rows[0]?.n ?? 0;
}

export interface OutboxRelayOptions {
  db: pg.Pool;
  table: OutboxTable;
  producer: Producer;
  logger: Logger;
  batchSize?: number;
  pollIntervalMs?: number;
  /** Called with the number of rows each successful batch published (used for metrics). */
  onPublished?: (count: number) => void;
}

interface OutboxRow {
  id: string;
  topic: string;
  message_key: string;
  payload: AnyEnvelope;
}

/**
 * Publishes unpublished outbox rows in id order, then marks them published.
 *
 * The rows stay locked while they are sent, and published_at is set only after Kafka acknowledged
 * them (acks=all). A crash between the two re-publishes the batch on restart, which is why every
 * consumer deduplicates by messageId. An advisory lock keeps a second relay for the same table from
 * publishing concurrently and reordering messages.
 */
export class OutboxRelay {
  private readonly batchSize: number;
  private readonly pollIntervalMs: number;

  constructor(private readonly options: OutboxRelayOptions) {
    assertTable(options.table);
    this.batchSize = options.batchSize ?? 100;
    this.pollIntervalMs = options.pollIntervalMs ?? 200;
  }

  /** Publishes one batch. Returns how many rows were published. */
  async publishBatch(): Promise<number> {
    const { db, table, producer } = this.options;
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      const lock = await client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_xact_lock(hashtext($1)) AS locked',
        [table],
      );
      if (!lock.rows[0]?.locked) {
        await client.query('ROLLBACK');
        return 0;
      }
      const { rows } = await client.query<OutboxRow>(
        `SELECT id, topic, message_key, payload FROM ${table}
         WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE`,
        [this.batchSize],
      );
      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }

      // One message list per topic, each still in id order.
      const byTopic = new Map<string, OutboxRow[]>();
      for (const row of rows) byTopic.set(row.topic, [...(byTopic.get(row.topic) ?? []), row]);
      await producer.sendBatch({
        topicMessages: [...byTopic].map(([topic, topicRows]) => ({
          topic,
          messages: topicRows.map((row) => ({
            key: row.message_key,
            value: JSON.stringify(row.payload),
            headers: {
              messageId: row.payload.messageId,
              type: row.payload.type,
              traceId: row.payload.traceId,
            },
          })),
        })),
      });

      await client.query(`UPDATE ${table} SET published_at = now() WHERE id = ANY($1::bigint[])`, [
        rows.map((row) => row.id),
      ]);
      await client.query('COMMIT');
      this.options.onPublished?.(rows.length);
      return rows.length;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  /** Publishes until `signal` aborts. Full batches are followed immediately by the next one. */
  async run(signal: AbortSignal): Promise<void> {
    const log = this.options.logger.child({ component: 'outbox-relay', table: this.options.table });
    let failures = 0;
    while (!signal.aborted) {
      let wait = this.pollIntervalMs;
      try {
        const published = await this.publishBatch();
        failures = 0;
        if (published > 0) log.debug({ published }, 'published outbox batch');
        if (published === this.batchSize) wait = 0;
      } catch (error) {
        failures++;
        wait = Math.min(10_000, this.pollIntervalMs * 2 ** failures);
        log.warn({ err: error, retryInMs: wait }, 'outbox publish failed');
      }
      await sleep(wait, signal);
    }
  }
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
    function done() {
      clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    }
  });
}
