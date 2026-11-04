import { randomUUID } from 'node:crypto';
import { TOPICS, createEnvelope } from '@ticketsurge/events';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import { waitForMessages } from '../../../scripts/src/testing/kafka.js';
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
} from './support/pipeline.js';

// The simulated provider fails a charge when random() < failureRate (0.5). Tests pick the outcome.
let nextOutcome: 'succeed' | 'fail' = 'succeed';

let t: TestApp;
let pipeline: Pipeline;
let seatIds: string[];

beforeAll(async () => {
  pipeline = await startPipeline({
    payment: { failureRate: 0.5, random: () => (nextOutcome === 'fail' ? 0 : 1) },
  });
  t = await createTestApp({ KAFKA_TOPIC_PREFIX: pipeline.prefix });
}, 60_000);
afterAll(async () => {
  await t.close();
  await pipeline.stop();
});
beforeEach(async () => {
  await resetDatabase(t.clients.pg);
  await t.clients.pg.query(
    'TRUNCATE payment.payments, payment.outbox, payment.processed_events, booking.processed_events',
  );
  await resetRedis(t.clients.redis);
  await seedDemoEvent(t.clients.pg);
  const { rows } = await t.clients.pg.query<{ id: string }>(
    'SELECT id FROM booking.seats WHERE event_id = $1 ORDER BY section, row_label, seat_number',
    [DEMO_EVENT_ID],
  );
  seatIds = rows.map((row) => row.id);
  nextOutcome = 'succeed';
});

async function payment(bookingId: string) {
  const { rows } = await t.clients.pg.query<{ id: string; status: string; amount_minor: number }>(
    'SELECT id, status, amount_minor FROM payment.payments WHERE booking_id = $1',
    [bookingId],
  );
  return rows;
}

describe('payment and booking saga', () => {
  it('confirms the booking when the payment succeeds', async () => {
    const user = await registerUser(t);
    const { bookingId } = await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 2));

    await waitForStatus(t.clients.pg, bookingId, 'CONFIRMED');
    expect(await payment(bookingId)).toEqual([
      expect.objectContaining({ status: 'SUCCEEDED', amount_minor: 30_000 }),
    ]);
    expect(
      await countRows(
        t.clients.pg,
        'SELECT 1 FROM booking.booking_seats WHERE booking_id = $1 AND active',
        [bookingId],
      ),
    ).toBe(2);

    const [confirmed] = await waitForMessages(
      pipeline.topic(TOPICS.bookingConfirmed),
      (m) => m.envelope.data.bookingId === bookingId,
    );
    expect(confirmed?.envelope).toMatchObject({
      type: 'booking.confirmed',
      data: { bookingId, eventId: DEMO_EVENT_ID, userId: user.id, amountMinor: 30_000 },
    });
  });

  it('cancels the booking and deactivates its seats when the payment fails', async () => {
    nextOutcome = 'fail';
    const user = await registerUser(t);
    const { bookingId } = await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 2));

    await waitForStatus(t.clients.pg, bookingId, 'CANCELLED');
    expect(await payment(bookingId)).toEqual([expect.objectContaining({ status: 'FAILED' })]);
    expect(
      await countRows(
        t.clients.pg,
        'SELECT 1 FROM booking.booking_seats WHERE booking_id = $1 AND active',
        [bookingId],
      ),
    ).toBe(0);

    const [cancelled] = await waitForMessages(
      pipeline.topic(TOPICS.bookingCancelled),
      (m) => m.envelope.data.bookingId === bookingId,
    );
    expect(cancelled?.envelope).toMatchObject({ data: { reason: 'PAYMENT_FAILED' } });

    // The seats are free in PostgreSQL again: someone else can book them once the Redis hold is
    // released (seat-sync does that from booking.cancelled; done by hand here).
    await t.clients.redis.flushdb();
    const other = await registerUser(t);
    await expect(bookSeats(t, other, DEMO_EVENT_ID, seatIds.slice(0, 2))).resolves.toBeDefined();
  });

  it('applies a duplicated payment.completed message only once', async () => {
    const user = await registerUser(t);
    const { bookingId } = await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 1));
    await waitForStatus(t.clients.pg, bookingId, 'CONFIRMED');

    // Re-deliver the exact message the payment service sent.
    const { rows } = await t.clients.pg.query<{ payload: Parameters<Pipeline['publish']>[0] }>(
      'SELECT payload FROM payment.outbox WHERE message_key = $1',
      [bookingId],
    );
    const original = rows[0]?.payload;
    if (!original) throw new Error('payment.completed not found');
    await pipeline.publish(original);
    await pipeline.publish(original);

    // A later conflicting result (new message) cannot undo the confirmation either.
    if (original.type !== 'payment.completed') throw new Error('unexpected message');
    await pipeline.publish(
      createEnvelope(
        TOPICS.paymentCompleted,
        { ...original.data, status: 'FAILED' },
        { traceId: 'test' },
      ),
    );

    await eventually(
      async () =>
        (await countRows(
          t.clients.pg,
          "SELECT 1 FROM booking.processed_events WHERE consumer = 'booking-saga'",
        )) >= 2
          ? true
          : undefined,
      { what: 'saga to process the conflicting message' },
    );
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(await waitForStatus(t.clients.pg, bookingId, 'CONFIRMED')).toBe('CONFIRMED');
    expect(
      await countRows(
        t.clients.pg,
        "SELECT 1 FROM booking.outbox WHERE topic LIKE '%booking.confirmed' AND message_key = $1",
        [bookingId],
      ),
    ).toBe(1);
    expect(
      await countRows(
        t.clients.pg,
        "SELECT 1 FROM booking.outbox WHERE topic LIKE '%booking.cancelled'",
      ),
    ).toBe(0);
  });

  it('charges a booking once even if booking.requested is delivered twice', async () => {
    const user = await registerUser(t);
    const { bookingId } = await bookSeats(t, user, DEMO_EVENT_ID, seatIds.slice(0, 1));
    await waitForStatus(t.clients.pg, bookingId, 'CONFIRMED');

    const { rows } = await t.clients.pg.query<{ payload: Parameters<Pipeline['publish']>[0] }>(
      "SELECT payload FROM booking.outbox WHERE topic LIKE '%booking.requested' AND message_key = $1",
      [bookingId],
    );
    const requested = rows[0]?.payload;
    if (!requested) throw new Error('booking.requested not found');
    await pipeline.publish(requested);
    // Same booking, new messageId: the unique booking_id still prevents a second charge.
    await pipeline.publish({ ...requested, messageId: randomUUID() });

    await eventually(
      async () =>
        (await countRows(
          t.clients.pg,
          "SELECT 1 FROM payment.processed_events WHERE consumer = 'payment-service'",
        )) >= 2
          ? true
          : undefined,
      { what: 'payment service to process the duplicate' },
    );
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(await payment(bookingId)).toHaveLength(1);
    expect(await countRows(t.clients.pg, 'SELECT 1 FROM payment.outbox')).toBe(1);
  });
});
