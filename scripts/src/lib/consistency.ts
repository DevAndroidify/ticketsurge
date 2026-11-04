import { keys, parseSeatValue } from '@ticketsurge/redis-scripts';
import type { Redis } from 'ioredis';
import type pg from 'pg';

export interface ConsistencyReport {
  /** Seats claimed by more than one active booking (must be 0; the partial unique index forbids it). */
  seatsWithMultipleActiveBookings: number;
  /** CONFIRMED bookings with no SUCCEEDED payment in the payment service. */
  confirmedWithoutSuccessfulPayment: number;
  /** Active seats of CONFIRMED bookings with no ticket for that booking. */
  confirmedSeatsWithoutTicket: number;
  /** PENDING_PAYMENT bookings past their deadline (the sweeper should have expired them). */
  overduePendingBookings: number;
  /** Seats whose Redis state (sold:<bookingId>) differs from PostgreSQL's confirmed seats. */
  redisSoldMismatches: number;
  /** Context, not failures. */
  totals: {
    confirmedBookings: number;
    soldSeats: number;
    cancelledBookings: number;
    expiredBookings: number;
    refundedPayments: number;
    tickets: number;
  };
  examples: Record<string, string[]>;
}

export const FAILURE_KEYS = [
  'seatsWithMultipleActiveBookings',
  'confirmedWithoutSuccessfulPayment',
  'confirmedSeatsWithoutTicket',
  'overduePendingBookings',
  'redisSoldMismatches',
] as const;

export function isConsistent(report: ConsistencyReport): boolean {
  return FAILURE_KEYS.every((key) => report[key] === 0);
}

/**
 * The end-of-test invariants from the brief. Each service's schema is queried separately and the
 * results are compared here, the same way the services themselves never join across schemas.
 */
export async function checkConsistency(
  db: pg.Pool,
  redis: Redis,
  { eventIds }: { eventIds?: string[] } = {},
): Promise<ConsistencyReport> {
  const eventFilter = eventIds ? 'AND b.event_id = ANY($1::uuid[])' : '';
  const params = eventIds ? [eventIds] : [];
  const examples: Record<string, string[]> = {};
  const sample = (name: string, items: string[]) => {
    if (items.length > 0) examples[name] = items.slice(0, 5);
    return items.length;
  };

  // 1. One active booking per seat.
  const multi = await db.query<{ seat: string }>(
    `SELECT bs.event_id || '/' || bs.seat_id AS seat
     FROM booking.booking_seats bs JOIN booking.bookings b ON b.id = bs.booking_id
     WHERE bs.active ${eventFilter}
     GROUP BY bs.event_id, bs.seat_id HAVING count(*) > 1`,
    params,
  );

  // Confirmed bookings and their active seats (booking schema).
  const confirmed = await db.query<{ id: string; event_id: string; seat_ids: string[] }>(
    `SELECT b.id, b.event_id, array_agg(bs.seat_id) FILTER (WHERE bs.active) AS seat_ids
     FROM booking.bookings b JOIN booking.booking_seats bs ON bs.booking_id = b.id
     WHERE b.status = 'CONFIRMED' ${eventFilter}
     GROUP BY b.id`,
    params,
  );
  const confirmedIds = confirmed.rows.map((r) => r.id);

  // 2. A SUCCEEDED payment for every confirmed booking (payment schema).
  const paid = await db.query<{ booking_id: string }>(
    `SELECT booking_id FROM payment.payments WHERE status = 'SUCCEEDED' AND booking_id = ANY($1::uuid[])`,
    [confirmedIds],
  );
  const paidSet = new Set(paid.rows.map((r) => r.booking_id));

  // 3. A ticket for every active confirmed seat (ticket schema).
  const tickets = await db.query<{ key: string }>(
    `SELECT booking_id || '/' || seat_id AS key FROM ticket.tickets WHERE booking_id = ANY($1::uuid[])`,
    [confirmedIds],
  );
  const ticketSet = new Set(tickets.rows.map((r) => r.key));
  const missingTickets: string[] = [];
  for (const booking of confirmed.rows) {
    for (const seatId of booking.seat_ids) {
      if (!ticketSet.has(`${booking.id}/${seatId}`)) missingTickets.push(`${booking.id}/${seatId}`);
    }
  }

  // 4. No pending booking past its deadline.
  const overdue = await db.query<{ id: string }>(
    `SELECT b.id FROM booking.bookings b
     WHERE b.status = 'PENDING_PAYMENT' AND b.expires_at < now() ${eventFilter}`,
    params,
  );

  // 5. Redis sold seats == PostgreSQL confirmed seats, per event.
  const expected = new Map<string, string>(); // "eventId/seatId" -> bookingId
  for (const booking of confirmed.rows) {
    for (const seatId of booking.seat_ids)
      expected.set(`${booking.event_id}/${seatId}`, booking.id);
  }
  const events =
    eventIds ??
    (await db.query<{ id: string }>('SELECT id FROM booking.events')).rows.map((r) => r.id);
  const actual = new Map<string, string>();
  for (const eventId of events) {
    const { rows: seats } = await db.query<{ id: string }>(
      'SELECT id FROM booking.seats WHERE event_id = $1',
      [eventId],
    );
    for (let i = 0; i < seats.length; i += 1000) {
      const chunk = seats.slice(i, i + 1000);
      const values = await redis.mget(...chunk.map((s) => keys.seat(eventId, s.id)));
      chunk.forEach((seat, index) => {
        const state = parseSeatValue(values[index] ?? null);
        if (state?.kind === 'sold') actual.set(`${eventId}/${seat.id}`, state.bookingId);
      });
    }
  }
  const mismatches: string[] = [];
  for (const [seat, bookingId] of expected) {
    if (actual.get(seat) !== bookingId)
      mismatches.push(`${seat}: pg=${bookingId} redis=${actual.get(seat) ?? 'none'}`);
  }
  for (const [seat, bookingId] of actual) {
    if (!expected.has(seat)) mismatches.push(`${seat}: pg=none redis=${bookingId}`);
  }

  const totals = await db.query<{
    cancelled: number;
    expired: number;
  }>(
    `SELECT count(*) FILTER (WHERE status = 'CANCELLED')::int AS cancelled,
            count(*) FILTER (WHERE status = 'EXPIRED')::int AS expired
     FROM booking.bookings b WHERE true ${eventFilter}`,
    params,
  );
  const refunded = await db.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM payment.payments WHERE status = 'REFUNDED'`,
  );

  return {
    seatsWithMultipleActiveBookings: sample(
      'seatsWithMultipleActiveBookings',
      multi.rows.map((r) => r.seat),
    ),
    confirmedWithoutSuccessfulPayment: sample(
      'confirmedWithoutSuccessfulPayment',
      confirmedIds.filter((id) => !paidSet.has(id)),
    ),
    confirmedSeatsWithoutTicket: sample('confirmedSeatsWithoutTicket', missingTickets),
    overduePendingBookings: sample(
      'overduePendingBookings',
      overdue.rows.map((r) => r.id),
    ),
    redisSoldMismatches: sample('redisSoldMismatches', mismatches),
    totals: {
      confirmedBookings: confirmed.rows.length,
      soldSeats: expected.size,
      cancelledBookings: totals.rows[0]?.cancelled ?? 0,
      expiredBookings: totals.rows[0]?.expired ?? 0,
      refundedPayments: refunded.rows[0]?.n ?? 0,
      tickets: ticketSet.size,
    },
    examples,
  };
}
