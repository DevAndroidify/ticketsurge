import { randomUUID } from 'node:crypto';
import { TOPICS, createEnvelope } from '@ticketsurge/events';
import { afterEach, describe, expect, it } from 'vitest';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import { testLogger, waitForMessages } from '../../../scripts/src/testing/kafka.js';
import {
  bookSeats,
  createTestApp,
  registerUser,
  resetDatabase,
  resetRedis,
  type TestApp,
} from '../../api/test/support/integration.js';
import { createSagaHandler } from '../src/saga.js';
import { ExpirySweeper } from '../src/sweeper.js';
import {
  countRows,
  eventually,
  startPipeline,
  waitForStatus,
  type Pipeline,
} from './support/pipeline.js';

let t: TestApp | undefined;
let pipeline: Pipeline | undefined;
let seatIds: string[];

async function setup(options: Parameters<typeof startPipeline>[0], deadlineMs: number) {
  pipeline = await startPipeline(options);
  t = await createTestApp({
    KAFKA_TOPIC_PREFIX: pipeline.prefix,
    PAYMENT_DEADLINE_MS: deadlineMs,
  });
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
  return { t, pipeline };
}

afterEach(async () => {
  await t?.close();
  await pipeline?.stop();
  t = undefined;
  pipeline = undefined;
});

const activeSeatCount = (app: TestApp, bookingId: string) =>
  countRows(
    app.clients.pg,
    'SELECT 1 FROM booking.booking_seats WHERE booking_id = $1 AND active',
    [bookingId],
  );

async function paymentStatus(app: TestApp, bookingId: string) {
  const { rows } = await app.clients.pg.query<{ status: string }>(
    'SELECT status FROM payment.payments WHERE booking_id = $1',
    [bookingId],
  );
  return rows[0]?.status;
}

describe('expiry and refunds', () => {
  it('a delayed payment expires the booking and creates a refund request', async () => {
    // Payment deadline 1s, provider takes 3s: the sweeper expires the booking first.
    const { t: app, pipeline: p } = await setup(
      { roles: ['relay', 'saga', 'sweeper'], payment: { delayMs: 3000 } },
      1000,
    );
    const user = await registerUser(app);
    const { bookingId } = await bookSeats(app, user, DEMO_EVENT_ID, seatIds.slice(0, 2));

    await waitForStatus(app.clients.pg, bookingId, 'EXPIRED', 10_000);
    expect(await activeSeatCount(app, bookingId)).toBe(0);
    const [cancelled] = await waitForMessages(
      p.topic(TOPICS.bookingCancelled),
      (m) => m.envelope.data.bookingId === bookingId,
    );
    expect(cancelled?.envelope).toMatchObject({ data: { reason: 'EXPIRED' } });

    // The payment then succeeds, too late: refund requested, recorded, seats not restored.
    const [refund] = await waitForMessages(
      p.topic(TOPICS.refundRequested),
      (m) => m.envelope.data.bookingId === bookingId,
    );
    expect(refund?.envelope).toMatchObject({
      type: 'payment.refund-requested',
      data: { bookingId, amountMinor: 30_000, reason: 'PAID_AFTER_EXPIRY' },
    });
    await eventually(
      async () => ((await paymentStatus(app, bookingId)) === 'REFUNDED' ? true : undefined),
      { what: 'payment to be refunded' },
    );
    expect(await waitForStatus(app.clients.pg, bookingId, 'EXPIRED')).toBe('EXPIRED');
    expect(await activeSeatCount(app, bookingId)).toBe(0);
  }, 60_000);

  it('the saga expires and refunds a late payment even before the sweeper runs', async () => {
    const { t: app } = await setup({ roles: ['relay', 'saga'], payment: { delayMs: 1500 } }, 500);
    const user = await registerUser(app);
    const { bookingId } = await bookSeats(app, user, DEMO_EVENT_ID, seatIds.slice(0, 1));

    await waitForStatus(app.clients.pg, bookingId, 'EXPIRED', 15_000);
    expect(await activeSeatCount(app, bookingId)).toBe(0);
    await eventually(
      async () => ((await paymentStatus(app, bookingId)) === 'REFUNDED' ? true : undefined),
      { what: 'payment to be refunded' },
    );
    // Exactly one booking.cancelled and one refund request.
    expect(
      await countRows(
        app.clients.pg,
        "SELECT 1 FROM booking.outbox WHERE topic LIKE '%booking.cancelled'",
      ),
    ).toBe(1);
    expect(
      await countRows(
        app.clients.pg,
        "SELECT 1 FROM booking.outbox WHERE topic LIKE '%refund-requested'",
      ),
    ).toBe(1);
  }, 60_000);

  it('pauses while the outbox backlog is above the limit', async () => {
    // No relay: booking.requested rows pile up unpublished.
    const { t: app, pipeline: p } = await setup({ roles: ['saga'], payment: false }, 200);
    const ids = [];
    for (let i = 0; i < 3; i++) {
      ids.push(
        (await bookSeats(app, await registerUser(app), DEMO_EVENT_ID, [seatIds[i] ?? '']))
          .bookingId,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 300));

    const settings = {
      topicPrefix: p.prefix,
      intervalMs: 100,
      batchSize: 100,
      maxOutboxBacklog: 2,
    };
    const sweeper = new ExpirySweeper(p.db, settings, testLogger);
    expect(await sweeper.sweepOnce()).toEqual({ paused: true, backlog: 3 });
    for (const id of ids)
      expect(await waitForStatus(app.clients.pg, id, 'PENDING_PAYMENT')).toBeTruthy();

    // Once the relay catches up, the overdue bookings expire.
    await app.clients.pg.query('UPDATE booking.outbox SET published_at = now()');
    const result = await sweeper.sweepOnce();
    expect(result.paused).toBe(false);
    expect(!result.paused && [...result.expired].sort()).toEqual([...ids].sort());
  }, 60_000);

  it('lets only one of the sweeper and a failed payment close an overdue booking', async () => {
    const { t: app, pipeline: p } = await setup({ roles: ['saga'], payment: false }, 200);
    const sweeper = new ExpirySweeper(
      p.db,
      { topicPrefix: p.prefix, intervalMs: 100, batchSize: 100, maxOutboxBacklog: 1_000_000 },
      testLogger,
    );
    const saga = createSagaHandler(p.db, p.prefix, testLogger);
    const bookingIds = [];
    for (let i = 0; i < 10; i++) {
      bookingIds.push(
        (await bookSeats(app, await registerUser(app), DEMO_EVENT_ID, [seatIds[i] ?? '']))
          .bookingId,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 300));

    // Race 10 failed-payment messages against two concurrent sweeps.
    await Promise.all([
      sweeper.sweepOnce(),
      sweeper.sweepOnce(),
      ...bookingIds.map((bookingId) =>
        saga(
          createEnvelope(
            TOPICS.paymentCompleted,
            {
              bookingId,
              paymentId: randomUUID(),
              status: 'FAILED',
              amountMinor: 15_000,
              providerRef: null,
            },
            { traceId: 'race' },
          ),
        ),
      ),
    ]);

    for (const bookingId of bookingIds) {
      const status = await waitForStatus(app.clients.pg, bookingId, 'EXPIRED', 100).catch(() =>
        waitForStatus(app.clients.pg, bookingId, 'CANCELLED', 100),
      );
      expect(['EXPIRED', 'CANCELLED']).toContain(status);
      expect(await activeSeatCount(app, bookingId)).toBe(0);
    }
    // One booking.cancelled per booking, never two.
    expect(
      await countRows(
        app.clients.pg,
        "SELECT 1 FROM booking.outbox WHERE topic LIKE '%booking.cancelled'",
      ),
    ).toBe(10);
  }, 60_000);
});
