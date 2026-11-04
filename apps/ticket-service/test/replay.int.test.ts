import { randomUUID } from 'node:crypto';
import type { AnyEnvelope } from '@ticketsurge/events';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import {
  bookSeats,
  createTestApp,
  registerUser,
  resetDatabase,
  resetRedis,
  type TestApp,
} from '../../api/test/support/integration.js';
import {
  countRows,
  eventually,
  startPipeline,
  waitForStatus,
  type Pipeline,
} from '../../booking-worker/test/support/pipeline.js';

let nextOutcome: 'succeed' | 'fail' = 'succeed';
let t: TestApp;
let pipeline: Pipeline;
let seatIds: string[];

beforeAll(async () => {
  pipeline = await startPipeline({
    roles: ['relay', 'saga'],
    payment: { failureRate: 0.5, random: () => (nextOutcome === 'fail' ? 0 : 1) },
    downstream: true,
  });
  t = await createTestApp({ KAFKA_TOPIC_PREFIX: pipeline.prefix });
}, 90_000);
afterAll(async () => {
  await t.close();
  await pipeline.stop();
});
beforeEach(async () => {
  await resetDatabase(t.clients.pg);
  await t.clients.pg.query('TRUNCATE ticket.tickets, notification.notifications, payment.payments');
  await resetRedis(t.clients.redis);
  await seedDemoEvent(t.clients.pg);
  const { rows } = await t.clients.pg.query<{ id: string }>(
    'SELECT id FROM booking.seats WHERE event_id = $1 ORDER BY section, row_label, seat_number',
    [DEMO_EVENT_ID],
  );
  seatIds = rows.map((row) => row.id);
  pipeline.sent.length = 0;
  nextOutcome = 'succeed';
});

const ticketsFor = (bookingId: string) =>
  countRows(t.clients.pg, 'SELECT 1 FROM ticket.tickets WHERE booking_id = $1', [bookingId]);
const notificationsFor = (bookingId: string) =>
  countRows(t.clients.pg, 'SELECT 1 FROM notification.notifications WHERE booking_id = $1', [
    bookingId,
  ]);

async function outboxMessage(table: string, topicSuffix: string, bookingId: string) {
  const { rows } = await t.clients.pg.query<{ payload: AnyEnvelope }>(
    `SELECT payload FROM ${table} WHERE topic LIKE $1 AND message_key = $2`,
    [`%${topicSuffix}`, bookingId],
  );
  const payload = rows[0]?.payload;
  if (!payload) throw new Error(`no ${topicSuffix} message for ${bookingId}`);
  return payload;
}

/** Waits until `consumer` has recorded at least `n` processed messages in `table`. */
function processedAtLeast(table: string, n: number) {
  return eventually(
    async () => ((await countRows(t.clients.pg, `SELECT 1 FROM ${table}`)) >= n ? true : undefined),
    { what: `${table} to reach ${n}` },
  );
}

describe('tickets and notifications', () => {
  it('issues one ticket per seat and sends one notification after the tickets exist', async () => {
    const user = await registerUser(t);
    const { bookingId } = await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 3));
    await waitForStatus(t.clients.pg, bookingId, 'CONFIRMED');

    await eventually(async () => ((await notificationsFor(bookingId)) === 1 ? true : undefined), {
      what: 'tickets notification',
    });
    expect(await ticketsFor(bookingId)).toBe(3);
    const { rows } = await t.clients.pg.query<{ code: string; user_id: string }>(
      'SELECT code, user_id FROM ticket.tickets WHERE booking_id = $1',
      [bookingId],
    );
    expect(new Set(rows.map((r) => r.code)).size).toBe(3);
    expect(rows.every((r) => r.user_id === user.id)).toBe(true);

    expect(pipeline.sent).toEqual([
      expect.objectContaining({ bookingId, userId: user.id, type: 'TICKETS_ISSUED' }),
    ]);
    for (const { code } of rows) expect(pipeline.sent[0]?.text).toContain(code);
  }, 60_000);

  it('replaying a booking event creates no second ticket or notification', async () => {
    const user = await registerUser(t);
    const { bookingId } = await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 2));
    await eventually(async () => ((await notificationsFor(bookingId)) === 1 ? true : undefined), {
      what: 'first notification',
    });
    const ticketProcessed = await countRows(t.clients.pg, 'SELECT 1 FROM ticket.processed_events');
    const notificationProcessed = await countRows(
      t.clients.pg,
      'SELECT 1 FROM notification.processed_events',
    );

    // 1. Redelivery: the exact same messages again (same messageId).
    const confirmed = await outboxMessage('booking.outbox', 'booking.confirmed', bookingId);
    const issued = await outboxMessage('ticket.outbox', 'ticket.issued', bookingId);
    await pipeline.publish(confirmed);
    await pipeline.publish(issued);
    // 2. Replay with fresh message IDs, as a re-sent or re-created event would arrive.
    await pipeline.publish({ ...confirmed, messageId: randomUUID() });
    await pipeline.publish({ ...issued, messageId: randomUUID() });

    // The fresh IDs are claimed (proving the messages were consumed); the duplicates are not.
    await processedAtLeast('ticket.processed_events', ticketProcessed + 1);
    await processedAtLeast('notification.processed_events', notificationProcessed + 1);
    await new Promise((resolve) => setTimeout(resolve, 500));

    expect(await ticketsFor(bookingId)).toBe(2);
    expect(await notificationsFor(bookingId)).toBe(1);
    expect(pipeline.sent.filter((n) => n.bookingId === bookingId)).toHaveLength(1);
    // No second ticket.issued was published for the replays.
    expect(
      await countRows(t.clients.pg, 'SELECT 1 FROM ticket.outbox WHERE message_key = $1', [
        bookingId,
      ]),
    ).toBe(1);
  }, 60_000);

  it('notifies a cancelled booking once, even when booking.cancelled is replayed', async () => {
    nextOutcome = 'fail';
    const user = await registerUser(t);
    const { bookingId } = await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 1));
    await waitForStatus(t.clients.pg, bookingId, 'CANCELLED');
    await eventually(async () => ((await notificationsFor(bookingId)) === 1 ? true : undefined), {
      what: 'cancellation notification',
    });

    const cancelled = await outboxMessage('booking.outbox', 'booking.cancelled', bookingId);
    const before = await countRows(t.clients.pg, 'SELECT 1 FROM notification.processed_events');
    await pipeline.publish(cancelled);
    await pipeline.publish({ ...cancelled, messageId: randomUUID() });
    await processedAtLeast('notification.processed_events', before + 1);
    await new Promise((resolve) => setTimeout(resolve, 300));

    expect(await notificationsFor(bookingId)).toBe(1);
    expect(pipeline.sent.filter((n) => n.bookingId === bookingId)).toEqual([
      expect.objectContaining({ type: 'BOOKING_CANCELLED' }),
    ]);
    expect(await ticketsFor(bookingId)).toBe(0);
  }, 60_000);
});
