import {
  createProducer,
  resetGroupToEarliest,
  type Producer,
  type RunningConsumer,
} from '@ticketsurge/kafka';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import { testKafka, testLogger } from '../../../scripts/src/testing/kafka.js';
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
import { clearAnalytics } from '../src/analytics.js';
import { analyticsGroupId, startAnalyticsService } from '../src/service.js';

// Alternate payment outcomes so the totals include both confirmations and cancellations.
let charges = 0;
let t: TestApp;
let pipeline: Pipeline;
let producer: Producer;
let analytics: RunningConsumer | undefined;

interface Totals {
  event_id: string;
  confirmed_bookings: number;
  seats_sold: number;
  revenue_minor: string;
  cancelled_bookings: number;
  expired_bookings: number;
}

beforeAll(async () => {
  pipeline = await startPipeline({
    roles: ['relay', 'saga'],
    payment: { failureRate: 0.5, random: () => (charges++ % 3 === 2 ? 0 : 1) },
  });
  t = await createTestApp({ KAFKA_TOPIC_PREFIX: pipeline.prefix });
  producer = await createProducer(testKafka('analytics-test'));
  await resetDatabase(t.clients.pg);
  await resetRedis(t.clients.redis);
  await clearAnalytics(t.clients.pg);
  await seedDemoEvent(t.clients.pg);
}, 60_000);

afterAll(async () => {
  await analytics?.stop();
  await producer.disconnect();
  await t.close();
  await pipeline.stop();
});

function start() {
  return startAnalyticsService(
    { db: pipeline.db, kafka: testKafka('analytics-test'), producer, logger: testLogger },
    { topicPrefix: pipeline.prefix, concurrency: 6 },
  );
}

async function totals(): Promise<Totals[]> {
  const { rows } = await t.clients.pg.query<Totals>(
    `SELECT event_id, confirmed_bookings, seats_sold, revenue_minor::text AS revenue_minor,
            cancelled_bookings, expired_bookings
     FROM analytics.sales_by_event ORDER BY event_id`,
  );
  return rows;
}

/** Waits until analytics has applied `n` messages. */
function processed(n: number) {
  return eventually(
    async () =>
      (await countRows(t.clients.pg, 'SELECT 1 FROM analytics.processed_events')) === n
        ? true
        : undefined,
    { what: `analytics to apply ${n} messages`, timeoutMs: 45_000 },
  );
}

describe('analytics', () => {
  it('sales totals match before and after a rebuild from Kafka', async () => {
    analytics = await start();
    await analytics.waitForAssignment();

    const { rows: seats } = await t.clients.pg.query<{ id: string }>(
      'SELECT id FROM booking.seats WHERE event_id = $1 ORDER BY section, row_label, seat_number LIMIT 12',
      [DEMO_EVENT_ID],
    );
    const bookingIds: string[] = [];
    for (let i = 0; i < 6; i++) {
      const user = await registerUser(t);
      const ids = seats.slice(i * 2, i * 2 + 2).map((s) => s.id);
      bookingIds.push((await bookSeats(t, user, DEMO_EVENT_ID, ids)).bookingId);
    }
    for (const id of bookingIds) {
      await eventually(
        async () => {
          const { rows } = await t.clients.pg.query<{ status: string }>(
            'SELECT status FROM booking.bookings WHERE id = $1',
            [id],
          );
          return rows[0]?.status === 'PENDING_PAYMENT' ? undefined : rows[0]?.status;
        },
        { what: `booking ${id} to settle` },
      );
    }
    await processed(6);

    const before = await totals();
    // Ground truth from the booking service's own tables.
    const { rows: truth } = await t.clients.pg.query<{
      confirmed: number;
      cancelled: number;
      seats: number;
      revenue: string;
    }>(
      `SELECT count(*) FILTER (WHERE status = 'CONFIRMED')::int AS confirmed,
              count(*) FILTER (WHERE status = 'CANCELLED')::int AS cancelled,
              coalesce(sum((SELECT count(*) FROM booking.booking_seats s WHERE s.booking_id = b.id))
                FILTER (WHERE status = 'CONFIRMED'), 0)::int AS seats,
              coalesce(sum(amount_minor) FILTER (WHERE status = 'CONFIRMED'), 0)::text AS revenue
       FROM booking.bookings b`,
    );
    expect(truth[0]?.confirmed).toBe(4);
    expect(truth[0]?.cancelled).toBe(2);
    expect(before).toEqual([
      {
        event_id: DEMO_EVENT_ID,
        confirmed_bookings: 4,
        seats_sold: truth[0]?.seats,
        revenue_minor: truth[0]?.revenue,
        cancelled_bookings: 2,
        expired_bookings: 0,
      },
    ]);

    // A running service blocks the reset, so the tables can never be wiped under it.
    const admin = testKafka('analytics-admin').admin();
    await admin.connect();
    try {
      await expect(resetGroupToEarliest(admin, analyticsGroupId(pipeline.prefix))).rejects.toThrow(
        /active member/,
      );

      // Rebuild: stop, reset offsets to earliest, clear, start again.
      await analytics.stop();
      analytics = undefined;
      await eventually(
        async () => {
          const { groups } = await admin.describeGroups([analyticsGroupId(pipeline.prefix)]);
          return (groups[0]?.members.length ?? 0) === 0 ? true : undefined;
        },
        { what: 'analytics group to empty' },
      );
      expect(await resetGroupToEarliest(admin, analyticsGroupId(pipeline.prefix))).toBe('reset');
    } finally {
      await admin.disconnect();
    }
    await clearAnalytics(t.clients.pg);
    expect(await totals()).toEqual([]);

    analytics = await start();
    await processed(6);
    expect(await totals()).toEqual(before);

    // Every booking settled exactly as before; nothing was confirmed or cancelled twice.
    for (const id of bookingIds) {
      const status = await waitForStatus(t.clients.pg, id, 'CONFIRMED', 100).catch(
        () => 'CANCELLED',
      );
      expect(['CONFIRMED', 'CANCELLED']).toContain(status);
    }
  }, 120_000);
});
