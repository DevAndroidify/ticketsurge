// Runs the asynchronous booking pipeline in-process against isolated test topics.
import { topicName, type AnyEnvelope, type EventType } from '@ticketsurge/events';
import { createProducer, type Producer } from '@ticketsurge/kafka';
import pg from 'pg';
import {
  startPaymentService,
  type RunningPaymentService,
} from '../../../payment-service/src/service.js';
import { createTestTopics, testKafka, testLogger } from '../../../../scripts/src/testing/kafka.js';
import { useTestEnv } from '../../../../scripts/src/testing/test-env.js';
import { startBookingWorker, type RunningBookingWorker, type Role } from '../../src/worker.js';

export interface PipelineOptions {
  roles?: readonly Role[];
  payment?: { delayMs?: number; failureRate?: number; random?: () => number } | false;
}

export interface Pipeline {
  prefix: string;
  db: pg.Pool;
  producer: Producer;
  worker: RunningBookingWorker;
  payment: RunningPaymentService | undefined;
  topic(type: EventType): string;
  /** Publishes an envelope directly, as if another service had sent it. */
  publish(envelope: AnyEnvelope): Promise<void>;
  stop(): Promise<void>;
}

/** pg's built-in parsing: BIGINT stays a string, as in a standalone service process. */
function standaloneTypes(): pg.CustomTypesConfig {
  const types = new pg.TypeOverrides();
  types.setTypeParser(pg.types.builtins.INT8, (value: string) => value);
  return types;
}

export async function startPipeline(options: PipelineOptions = {}): Promise<Pipeline> {
  const { databaseUrl } = useTestEnv();
  const topics = await createTestTopics();
  // The API code loaded in this test process installs a global BIGINT -> number parser. Real
  // workers run in their own processes without it, so give the services pg's default parsing.
  const db = new pg.Pool({
    connectionString: databaseUrl,
    max: 20,
    types: standaloneTypes(),
  });
  const kafka = testKafka('pipeline-test');
  const producer = await createProducer(kafka);
  const deps = { db, kafka, producer, logger: testLogger };

  const worker = await startBookingWorker(deps, {
    roles: options.roles ?? ['relay', 'saga'],
    topicPrefix: topics.prefix,
    outboxBatchSize: 100,
    outboxPollIntervalMs: 50,
    consumerConcurrency: 6,
  });
  const payment =
    options.payment === false
      ? undefined
      : await startPaymentService(deps, {
          topicPrefix: topics.prefix,
          delayMs: options.payment?.delayMs ?? 20,
          failureRate: options.payment?.failureRate ?? 0,
          ...(options.payment?.random ? { random: options.payment.random } : {}),
          concurrency: 6,
          outboxBatchSize: 100,
          outboxPollIntervalMs: 50,
        });

  // Consumers start at the earliest offset, but waiting for assignment keeps timings predictable.
  await Promise.all([
    ...Object.values(worker.consumers).map((c) => c.waitForAssignment()),
    payment?.consumer.waitForAssignment(),
  ]);

  return {
    prefix: topics.prefix,
    db,
    producer,
    worker,
    payment,
    topic: (type) => topicName(topics.prefix, type),
    async publish(envelope) {
      await producer.send({
        topic: topicName(topics.prefix, envelope.type),
        messages: [{ key: envelope.data.bookingId, value: JSON.stringify(envelope) }],
      });
    },
    async stop() {
      await payment?.stop();
      await worker.stop();
      await producer.disconnect();
      await db.end();
      await topics.cleanup();
    },
  };
}

/** Polls `check` until it returns a value that is not undefined, or fails after `timeoutMs`. */
export async function eventually<T>(
  check: () => Promise<T | undefined>,
  { timeoutMs = 30_000, intervalMs = 100, what = 'condition' } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

export async function bookingStatus(db: pg.Pool, bookingId: string): Promise<string | undefined> {
  const { rows } = await db.query<{ status: string }>(
    'SELECT status FROM booking.bookings WHERE id = $1',
    [bookingId],
  );
  return rows[0]?.status;
}

/** Waits until the booking reaches `status`. */
export function waitForStatus(db: pg.Pool, bookingId: string, status: string, timeoutMs = 30_000) {
  return eventually(
    async () => ((await bookingStatus(db, bookingId)) === status ? status : undefined),
    { timeoutMs, what: `booking ${bookingId} to become ${status}` },
  );
}

export async function countRows(db: pg.Pool, sql: string, params: unknown[] = []): Promise<number> {
  const { rows } = await db.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM (${sql}) q`,
    params,
  );
  return rows[0]?.n ?? 0;
}
