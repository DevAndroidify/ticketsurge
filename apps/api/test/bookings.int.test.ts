import { randomUUID } from 'node:crypto';
import { keys } from '@ticketsurge/redis-scripts';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { DEMO_EVENT_ID, DEMO_SECTIONS, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import {
  bearer,
  createTestApp,
  registerUser,
  resetDatabase,
  resetRedis,
  type TestApp,
  type TestUser,
} from './support/integration.js';

const PAYMENT_DEADLINE_MS = 300_000;
const HOLD_AFTER_DEADLINE_MS = 60_000;

let t: TestApp;
let seatIds: string[];
let user: TestUser;

beforeAll(async () => {
  t = await createTestApp({ ADMISSION_BYPASS: true, PAYMENT_DEADLINE_MS, HOLD_AFTER_DEADLINE_MS });
});
afterAll(async () => {
  await t.close();
});
beforeEach(async () => {
  await resetDatabase(t.clients.pg);
  await resetRedis(t.clients.redis);
  await seedDemoEvent(t.clients.pg);
  const { rows } = await t.clients.pg.query<{ id: string }>(
    `SELECT id FROM booking.seats WHERE event_id = $1 AND section = 'Floor'
     ORDER BY row_label, seat_number`,
    [DEMO_EVENT_ID],
  );
  seatIds = rows.map((row) => row.id);
  user = await registerUser(t);
});

async function createHold(owner: TestUser, ids: string[]): Promise<string> {
  const response = await t.app.inject({
    method: 'POST',
    url: `/events/${DEMO_EVENT_ID}/holds`,
    headers: bearer(owner),
    payload: { seatIds: ids },
  });
  expect(response.statusCode, response.body).toBe(201);
  return response.json<{ holdId: string }>().holdId;
}

/** `key: null` sends no Idempotency-Key header. */
function book(owner: TestUser, holdId: string, key: string | null = randomUUID()) {
  return t.app.inject({
    method: 'POST',
    url: '/bookings',
    headers: { ...bearer(owner), ...(key === null ? {} : { 'idempotency-key': key }) },
    payload: { holdId },
  });
}

async function countBookings(): Promise<number> {
  const { rows } = await t.clients.pg.query<{ n: number }>(
    'SELECT count(*)::int AS n FROM booking.bookings',
  );
  return rows[0]?.n ?? 0;
}

describe('POST /bookings', () => {
  it('creates a PENDING_PAYMENT booking with active seats and extends the hold', async () => {
    const holdId = await createHold(user, seatIds.slice(0, 2));
    const started = Date.now();
    const response = await book(user, holdId);

    expect(response.statusCode, response.body).toBe(202);
    const body = response.json<{
      bookingId: string;
      status: string;
      amountMinor: number;
      expiresAt: string;
    }>();
    const floorPrice = DEMO_SECTIONS[0]?.priceMinor ?? 0;
    expect(body).toMatchObject({ status: 'PENDING_PAYMENT', amountMinor: 2 * floorPrice });
    expect(Date.parse(body.expiresAt) - started).toBeGreaterThan(PAYMENT_DEADLINE_MS - 5_000);
    expect(Date.parse(body.expiresAt) - started).toBeLessThan(PAYMENT_DEADLINE_MS + 5_000);

    const seats = await t.clients.pg.query(
      'SELECT seat_id, active FROM booking.booking_seats WHERE booking_id = $1',
      [body.bookingId],
    );
    expect(seats.rows).toHaveLength(2);
    expect(seats.rows.every((row: { active: boolean }) => row.active)).toBe(true);

    // The hold now expires one minute after the payment deadline.
    const ttl = await t.clients.redis.pttl(keys.seat(DEMO_EVENT_ID, seatIds[0] ?? ''));
    expect(ttl).toBeGreaterThan(PAYMENT_DEADLINE_MS + HOLD_AFTER_DEADLINE_MS - 5_000);

    const fetched = await t.app.inject({
      url: `/bookings/${body.bookingId}`,
      headers: bearer(user),
    });
    expect(fetched.statusCode).toBe(200);
    expect(fetched.json()).toMatchObject({
      id: body.bookingId,
      eventId: DEMO_EVENT_ID,
      status: 'PENDING_PAYMENT',
      seats: [{ active: true }, { active: true }],
    });

    const map = (await t.app.inject(`/events/${DEMO_EVENT_ID}/seats`)).json<{
      counts: Record<string, number>;
    }>();
    expect(map.counts).toMatchObject({ held: 2 });
  });

  it('returns one booking when the same Idempotency-Key is repeated', async () => {
    const holdId = await createHold(user, seatIds.slice(0, 1));
    const key = randomUUID();
    const first = await book(user, holdId, key);
    const second = await book(user, holdId, key);
    const third = await book(user, holdId, key);

    expect([first.statusCode, second.statusCode, third.statusCode]).toEqual([202, 202, 202]);
    const ids = [first, second, third].map((r) => r.json<{ bookingId: string }>().bookingId);
    expect(new Set(ids).size).toBe(1);
    expect(await countBookings()).toBe(1);
  });

  it('returns one booking when the same key arrives 20 times concurrently', async () => {
    const holdId = await createHold(user, seatIds.slice(0, 1));
    const key = randomUUID();
    const responses = await Promise.all(Array.from({ length: 20 }, () => book(user, holdId, key)));

    expect(responses.map((r) => r.statusCode)).toEqual(Array(20).fill(202));
    const ids = new Set(responses.map((r) => r.json<{ bookingId: string }>().bookingId));
    expect(ids.size).toBe(1);
    expect(await countBookings()).toBe(1);
  });

  it('rejects a reused key for a different hold with 422', async () => {
    const key = randomUUID();
    await book(user, await createHold(user, seatIds.slice(0, 1)), key);
    const response = await book(user, await createHold(user, seatIds.slice(1, 2)), key);
    expect(response.statusCode).toBe(422);
    expect(await countBookings()).toBe(1);
  });

  it('rejects a second booking of the same hold with a new key', async () => {
    const holdId = await createHold(user, seatIds.slice(0, 1));
    expect((await book(user, holdId)).statusCode).toBe(202);
    const response = await book(user, holdId);
    expect(response.statusCode).toBe(409);
    expect(await countBookings()).toBe(1);
  });

  it('requires the Idempotency-Key header', async () => {
    const holdId = await createHold(user, seatIds.slice(0, 1));
    const response = await book(user, holdId, null);
    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ error: { code: 'IDEMPOTENCY_KEY_REQUIRED' } });
  });

  it('returns 409 when the hold is gone or belongs to someone else', async () => {
    const holdId = await createHold(user, seatIds.slice(0, 1));
    const other = await registerUser(t);
    expect((await book(other, holdId)).statusCode).toBe(409);

    await t.clients.redis.del(keys.hold(DEMO_EVENT_ID, holdId));
    const response = await book(user, holdId);
    expect(response.statusCode).toBe(409);
    expect(response.json()).toMatchObject({ error: { code: 'HOLD_EXPIRED' } });
    expect((await book(user, randomUUID())).statusCode).toBe(409);
    expect(await countBookings()).toBe(0);
  });

  it('returns 409 and releases the hold when PostgreSQL already has the seat', async () => {
    // First buyer books the seat.
    const first = await createHold(user, seatIds.slice(0, 1));
    expect((await book(user, first)).statusCode).toBe(202);

    // Simulate Redis losing its state (e.g. a restart): the seat looks free again.
    await t.clients.redis.flushdb();
    const second = await registerUser(t);
    const holdId = await createHold(second, seatIds.slice(0, 1));

    const response = await book(second, holdId);
    expect(response.statusCode).toBe(409);
    expect(response.json()).toMatchObject({ error: { code: 'SEAT_TAKEN' } });
    expect(await countBookings()).toBe(1);
    // The rejected hold was released.
    expect(await t.clients.redis.exists(keys.seat(DEMO_EVENT_ID, seatIds[0] ?? ''))).toBe(0);
  });

  it('DELETE /holds/:holdId refuses to release a booked hold', async () => {
    const holdId = await createHold(user, seatIds.slice(0, 1));
    await book(user, holdId);
    const response = await t.app.inject({
      method: 'DELETE',
      url: `/holds/${holdId}`,
      headers: bearer(user),
    });
    expect(response.statusCode).toBe(409);
  });
});

describe('GET /bookings/:id', () => {
  it('hides other users’ bookings', async () => {
    const holdId = await createHold(user, seatIds.slice(0, 1));
    const { bookingId } = (await book(user, holdId)).json<{ bookingId: string }>();
    const other = await registerUser(t);
    const response = await t.app.inject({ url: `/bookings/${bookingId}`, headers: bearer(other) });
    expect(response.statusCode).toBe(404);
  });
});

describe('one_active_booking_per_seat (direct SQL)', () => {
  async function insertBooking(seatId: string, active = true): Promise<string> {
    const { rows } = await t.clients.pg.query<{ id: string }>(
      `INSERT INTO booking.bookings
         (event_id, user_id, hold_id, status, amount_minor, idempotency_key, expires_at)
       VALUES ($1, $2, gen_random_uuid(), 'PENDING_PAYMENT', 100, gen_random_uuid()::text,
               now() + interval '5 minutes')
       RETURNING id`,
      [DEMO_EVENT_ID, user.id],
    );
    const id = rows[0]?.id ?? '';
    await t.clients.pg.query(
      'INSERT INTO booking.booking_seats (booking_id, event_id, seat_id, active) VALUES ($1, $2, $3, $4)',
      [id, DEMO_EVENT_ID, seatId, active],
    );
    return id;
  }

  it('rejects a second active booking for the same seat', async () => {
    const seatId = seatIds[0] ?? '';
    await insertBooking(seatId);
    await expect(insertBooking(seatId)).rejects.toMatchObject({
      code: '23505',
      constraint: 'one_active_booking_per_seat',
    });
  });

  it('allows a new active booking once the old one is inactive', async () => {
    const seatId = seatIds[0] ?? '';
    const first = await insertBooking(seatId);
    await t.clients.pg.query(
      'UPDATE booking.booking_seats SET active = false WHERE booking_id = $1',
      [first],
    );
    await expect(insertBooking(seatId)).resolves.toBeTypeOf('string');
    // Inactive history rows may repeat freely.
    await expect(insertBooking(seatId, false)).resolves.toBeTypeOf('string');
  });

  it('rejects a seat from another event', async () => {
    const { rows } = await t.clients.pg.query<{ id: string }>(
      `INSERT INTO booking.events (name, venue, starts_at, sale_starts_at)
       VALUES ('Other', 'Elsewhere', now() + interval '1 day', now()) RETURNING id`,
    );
    const otherEvent = rows[0]?.id ?? '';
    const seat = await t.clients.pg.query<{ id: string }>(
      `INSERT INTO booking.seats (event_id, section, row_label, seat_number, price_minor)
       VALUES ($1, 'X', 'A', 1, 100) RETURNING id`,
      [otherEvent],
    );
    await expect(insertBooking(seat.rows[0]?.id ?? '')).rejects.toMatchObject({ code: '23503' });
  });
});
