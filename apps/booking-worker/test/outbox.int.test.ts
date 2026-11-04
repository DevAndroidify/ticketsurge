import { TOPICS, topicName } from '@ticketsurge/events';
import { createProducer, type Producer } from '@ticketsurge/kafka';
import { OutboxRelay, outboxBacklog } from '@ticketsurge/outbox';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import {
  createTestTopics,
  testKafka,
  testLogger,
  waitForMessages,
} from '../../../scripts/src/testing/kafka.js';
import {
  bookSeats,
  createTestApp,
  registerUser,
  resetDatabase,
  resetRedis,
  type TestApp,
} from '../../api/test/support/integration.js';

let t: TestApp;
let topics: Awaited<ReturnType<typeof createTestTopics>>;
let producer: Producer;
let seatIds: string[];
let requestedTopic: string;

beforeAll(async () => {
  topics = await createTestTopics();
  requestedTopic = topicName(topics.prefix, TOPICS.bookingRequested);
  t = await createTestApp({ KAFKA_TOPIC_PREFIX: topics.prefix });
  producer = await createProducer(testKafka('outbox-test'));
});
afterAll(async () => {
  await producer.disconnect();
  await t.close();
  await topics.cleanup();
});
beforeEach(async () => {
  await resetDatabase(t.clients.pg);
  await resetRedis(t.clients.redis);
  await seedDemoEvent(t.clients.pg);
  const { rows } = await t.clients.pg.query<{ id: string }>(
    'SELECT id FROM booking.seats WHERE event_id = $1 ORDER BY section, row_label, seat_number',
    [DEMO_EVENT_ID],
  );
  seatIds = rows.map((row) => row.id);
});

function relay(overrides: Partial<ConstructorParameters<typeof OutboxRelay>[0]> = {}) {
  return new OutboxRelay({
    db: t.clients.pg,
    table: 'booking.outbox',
    producer,
    logger: testLogger,
    batchSize: 100,
    pollIntervalMs: 50,
    ...overrides,
  });
}

async function outboxRow(bookingId: string) {
  const { rows } = await t.clients.pg.query<{
    topic: string;
    message_key: string;
    published_at: Date | null;
    payload: { messageId: string };
  }>(
    `SELECT topic, message_key, published_at, payload FROM booking.outbox WHERE message_key = $1`,
    [bookingId],
  );
  return rows;
}

describe('booking outbox', () => {
  it('writes booking.requested in the booking transaction, and the relay publishes it', async () => {
    const user = await registerUser(t);
    const { bookingId, holdId } = await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 2));

    const [row] = await outboxRow(bookingId);
    expect(row).toMatchObject({
      topic: requestedTopic,
      message_key: bookingId,
      published_at: null,
    });

    // The relay is not running yet: the message waits safely in PostgreSQL.
    await new Promise((resolve) => setTimeout(resolve, 1000));
    expect((await outboxRow(bookingId))[0]?.published_at).toBeNull();
    expect(await outboxBacklog(t.clients.pg, 'booking.outbox')).toBe(1);

    // Starting the relay delivers it.
    const controller = new AbortController();
    const running = relay().run(controller.signal);
    try {
      const [message] = await waitForMessages(
        requestedTopic,
        (m) => m.envelope.type === 'booking.requested' && m.envelope.data.bookingId === bookingId,
      );
      expect(message?.key).toBe(bookingId);
      expect(message?.headers).toMatchObject({
        messageId: row?.payload.messageId,
        type: 'booking.requested',
      });
      expect(message?.envelope).toMatchObject({
        version: 1,
        data: { bookingId, eventId: DEMO_EVENT_ID, userId: user.id, holdId },
      });
      expect(
        message?.envelope.type === 'booking.requested' && message.envelope.data.seatIds,
      ).toEqual(expect.arrayContaining(seatIds.slice(0, 2)));
    } finally {
      controller.abort();
      await running;
    }
    expect((await outboxRow(bookingId))[0]?.published_at).toBeInstanceOf(Date);
    expect(await outboxBacklog(t.clients.pg, 'booking.outbox')).toBe(0);
  });

  it('writes no message when the booking is rejected', async () => {
    const first = await registerUser(t);
    await bookSeats(t, first, DEMO_EVENT_ID, seatIds.slice(0, 1));
    await t.clients.redis.flushdb();
    const second = await registerUser(t);
    await expect(bookSeats(t, second, DEMO_EVENT_ID, seatIds.slice(0, 1))).rejects.toThrow(
      /SEAT_TAKEN/,
    );
    expect(await outboxBacklog(t.clients.pg, 'booking.outbox')).toBe(1);
  });

  it('leaves rows unpublished when Kafka rejects the batch', async () => {
    const user = await registerUser(t);
    await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 1));
    const failing = {
      sendBatch: () => Promise.reject(new Error('broker unavailable')),
    } as unknown as Producer;
    await expect(relay({ producer: failing }).publishBatch()).rejects.toThrow('broker unavailable');
    expect(await outboxBacklog(t.clients.pg, 'booking.outbox')).toBe(1);
    // Once Kafka is back, the same row goes out.
    expect(await relay().publishBatch()).toBe(1);
  });

  it('publishes nothing while another relay holds the lock, then each row once in batches', async () => {
    const ids = new Set<string>();
    for (let i = 0; i < 5; i++) {
      ids.add(
        (await bookSeats(t, await registerUser(t), DEMO_EVENT_ID, [seatIds[i] ?? ''])).bookingId,
      );
    }
    // Simulate another relay mid-batch: hold the same transaction-level advisory lock.
    const other = await t.clients.pg.connect();
    try {
      await other.query('BEGIN');
      await other.query('SELECT pg_advisory_xact_lock(hashtext($1))', ['booking.outbox']);
      expect(await relay({ batchSize: 3 }).publishBatch()).toBe(0);
      expect(await outboxBacklog(t.clients.pg, 'booking.outbox')).toBe(5);
    } finally {
      await other.query('ROLLBACK');
      other.release();
    }

    // Two relays racing for the lock: however they interleave, each row is published exactly once.
    const [a, b] = await Promise.all([
      relay({ batchSize: 3 }).publishBatch(),
      relay({ batchSize: 3 }).publishBatch(),
    ]);
    const rest = await relay({ batchSize: 3 }).publishBatch();
    expect(a + b + rest).toBe(5);
    expect(Math.max(a, b, rest)).toBeLessThanOrEqual(3);
    expect(await outboxBacklog(t.clients.pg, 'booking.outbox')).toBe(0);

    const messages = await waitForMessages(
      requestedTopic,
      (m) => ids.has(m.envelope.data.bookingId),
      {
        count: 5,
      },
    );
    expect(new Set(messages.map((m) => m.envelope.data.bookingId))).toEqual(ids);
  });
});
