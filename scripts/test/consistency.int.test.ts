import { randomUUID } from 'node:crypto';
import { keys } from '@ticketsurge/redis-scripts';
import { Redis } from 'ioredis';
import pg from 'pg';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { checkConsistency, isConsistent } from '../src/lib/consistency.js';
import { rebuildRedis } from '../src/lib/redis-rebuild.js';
import { DEMO_EVENT_ID, seedDemoEvent } from '../src/lib/seed.js';
import { TEST_REDIS_DB, useTestEnv } from '../src/testing/test-env.js';

const { databaseUrl, redisUrl } = useTestEnv();
const db = new pg.Pool({ connectionString: databaseUrl, max: 4 });
const redis = new Redis(redisUrl);
let seats: string[];

afterAll(async () => {
  await redis.quit();
  await db.end();
});

beforeEach(async () => {
  expect(redis.options.db).toBe(TEST_REDIS_DB);
  await redis.flushdb();
  await db.query('TRUNCATE booking.events, booking.users, booking.outbox CASCADE');
  await db.query('TRUNCATE payment.payments, ticket.tickets');
  await seedDemoEvent(db);
  const { rows } = await db.query<{ id: string }>(
    'SELECT id FROM booking.seats WHERE event_id = $1 ORDER BY section, row_label, seat_number LIMIT 10',
    [DEMO_EVENT_ID],
  );
  seats = rows.map((r) => r.id);
});

async function booking(
  status: string,
  seatIds: string[],
  expiresIn: string,
  extras: { paid?: boolean; ticketed?: boolean } = {},
) {
  const { rows: users } = await db.query<{ id: string }>(
    `INSERT INTO booking.users (email, password_hash) VALUES ($1, 'x') RETURNING id`,
    [`${randomUUID()}@test.local`],
  );
  const userId = users[0]?.id ?? '';
  const { rows } = await db.query<{ id: string; hold_id: string }>(
    `INSERT INTO booking.bookings (event_id, user_id, hold_id, status, amount_minor, idempotency_key, expires_at)
     VALUES ($1, $2, gen_random_uuid(), $3, 100, gen_random_uuid()::text, now() + $4::interval)
     RETURNING id, hold_id`,
    [DEMO_EVENT_ID, userId, status, expiresIn],
  );
  const b = rows[0] ?? { id: '', hold_id: '' };
  await db.query(
    `INSERT INTO booking.booking_seats (booking_id, event_id, seat_id) SELECT $1, $2, unnest($3::uuid[])`,
    [b.id, DEMO_EVENT_ID, seatIds],
  );
  if (extras.paid) {
    await db.query(
      `INSERT INTO payment.payments (booking_id, status, amount_minor) VALUES ($1, 'SUCCEEDED', 100)`,
      [b.id],
    );
  }
  if (extras.ticketed) {
    await db.query(
      `INSERT INTO ticket.tickets (booking_id, event_id, seat_id, user_id, code)
       SELECT $1, $2, s, $3, gen_random_uuid()::text FROM unnest($4::uuid[]) s`,
      [b.id, DEMO_EVENT_ID, userId, seatIds],
    );
  }
  return { ...b, userId };
}

describe('redis:rebuild', () => {
  it('restores sold seats from CONFIRMED and holds from PENDING_PAYMENT bookings', async () => {
    const sold = await booking('CONFIRMED', seats.slice(0, 2), '1 minute', {
      paid: true,
      ticketed: true,
    });
    const pending = await booking('PENDING_PAYMENT', seats.slice(2, 3), '2 minutes');
    await booking('CANCELLED', seats.slice(3, 4), '1 minute');
    // Simulates a hold someone grabbed right after a Redis restart on a seat PostgreSQL has booked.
    await redis.set(keys.seat(DEMO_EVENT_ID, seats[2] ?? ''), 'hold:stranger', 'PX', 600_000);

    const result = await rebuildRedis(db, redis, { holdAfterDeadlineMs: 60_000 });
    expect(result).toEqual({ soldBookings: 1, soldSeats: 2, heldBookings: 1, heldSeats: 1 });

    for (const seatId of seats.slice(0, 2)) {
      expect(await redis.get(keys.seat(DEMO_EVENT_ID, seatId))).toBe(`sold:${sold.id}`);
      expect(await redis.pttl(keys.seat(DEMO_EVENT_ID, seatId))).toBe(-1);
    }
    expect(await redis.get(keys.seat(DEMO_EVENT_ID, seats[2] ?? ''))).toBe(
      `hold:${pending.hold_id}`,
    );
    // TTL = deadline (2 min) + 1 min grace.
    const ttl = await redis.pttl(keys.seat(DEMO_EVENT_ID, seats[2] ?? ''));
    expect(ttl).toBeGreaterThan(170_000);
    expect(ttl).toBeLessThanOrEqual(180_000);
    expect(await redis.hget(keys.hold(DEMO_EVENT_ID, pending.hold_id), 'owner')).toBe(
      pending.userId,
    );
    expect(await redis.get(keys.holdRef(pending.hold_id))).toBe(DEMO_EVENT_ID);
    // Cancelled bookings leave their seat free.
    expect(await redis.exists(keys.seat(DEMO_EVENT_ID, seats[3] ?? ''))).toBe(0);

    // Safe to repeat.
    expect(await rebuildRedis(db, redis, { holdAfterDeadlineMs: 60_000 })).toEqual(result);
  });
});

describe('consistency check', () => {
  it('passes for a consistent state and reports each kind of violation', async () => {
    const good = await booking('CONFIRMED', seats.slice(0, 2), '1 minute', {
      paid: true,
      ticketed: true,
    });
    await rebuildRedis(db, redis, { holdAfterDeadlineMs: 60_000 });
    const clean = await checkConsistency(db, redis, { eventIds: [DEMO_EVENT_ID] });
    expect(isConsistent(clean)).toBe(true);
    expect(clean.totals).toMatchObject({ confirmedBookings: 1, soldSeats: 2, tickets: 2 });

    await booking('CONFIRMED', seats.slice(2, 3), '1 minute', { ticketed: true }); // unpaid
    await booking('CONFIRMED', seats.slice(3, 4), '1 minute', { paid: true }); // no ticket
    await booking('PENDING_PAYMENT', seats.slice(4, 5), '-1 minute'); // overdue
    await redis.set(keys.seat(DEMO_EVENT_ID, seats[0] ?? ''), `sold:${randomUUID()}`); // wrong booking
    await redis.set(keys.seat(DEMO_EVENT_ID, seats[9] ?? ''), `sold:${good.id}`); // sold only in Redis

    const report = await checkConsistency(db, redis, { eventIds: [DEMO_EVENT_ID] });
    expect(report).toMatchObject({
      seatsWithMultipleActiveBookings: 0,
      confirmedWithoutSuccessfulPayment: 1,
      confirmedSeatsWithoutTicket: 1,
      overduePendingBookings: 1,
    });
    // seat 0 wrong, seat 9 extra, plus the two new confirmed seats never marked sold.
    expect(report.redisSoldMismatches).toBe(4);
    expect(isConsistent(report)).toBe(false);
  });

  it('would catch two active bookings on one seat if the index were missing', async () => {
    await db.query('DROP INDEX booking.one_active_booking_per_seat');
    try {
      await booking('CONFIRMED', seats.slice(0, 1), '1 minute', { paid: true, ticketed: true });
      await booking('PENDING_PAYMENT', seats.slice(0, 1), '5 minutes');
      const report = await checkConsistency(db, redis, { eventIds: [DEMO_EVENT_ID] });
      expect(report.seatsWithMultipleActiveBookings).toBe(1);
    } finally {
      await db.query('DELETE FROM booking.booking_seats');
      await db.query(
        `CREATE UNIQUE INDEX one_active_booking_per_seat ON booking.booking_seats (event_id, seat_id) WHERE active = true`,
      );
    }
  });
});
