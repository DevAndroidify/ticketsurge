import { TOPICS, createEnvelope } from '@ticketsurge/events';
import { insertOutboxMessage } from '@ticketsurge/outbox';
import type pg from 'pg';
import { withTransaction } from '../../db.js';

export type BookingStatus = 'PENDING_PAYMENT' | 'CONFIRMED' | 'CANCELLED' | 'EXPIRED';

export interface BookingRow {
  id: string;
  eventId: string;
  userId: string;
  holdId: string;
  status: BookingStatus;
  amountMinor: number;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
}

const BOOKING_COLUMNS = `
  id, event_id AS "eventId", user_id AS "userId", hold_id AS "holdId", status,
  amount_minor AS "amountMinor", expires_at AS "expiresAt", created_at AS "createdAt",
  updated_at AS "updatedAt"`;

export async function findByIdempotencyKey(
  db: pg.Pool,
  userId: string,
  key: string,
): Promise<BookingRow | undefined> {
  const { rows } = await db.query<BookingRow>(
    `SELECT ${BOOKING_COLUMNS} FROM booking.bookings WHERE user_id = $1 AND idempotency_key = $2`,
    [userId, key],
  );
  return rows[0];
}

export async function holdHasBooking(db: pg.Pool, holdId: string): Promise<boolean> {
  const { rowCount } = await db.query('SELECT 1 FROM booking.bookings WHERE hold_id = $1', [
    holdId,
  ]);
  return Boolean(rowCount);
}

export interface CreateBookingInput {
  eventId: string;
  userId: string;
  holdId: string;
  seatIds: readonly string[];
  idempotencyKey: string;
  paymentDeadlineMs: number;
  /** Full Kafka topic name for booking.requested (includes any test prefix). */
  requestedTopic: string;
  traceId: string;
}

/**
 * Inserts the booking as PENDING_PAYMENT, its active seats, and its booking.requested outbox row in
 * one transaction. If another active booking already has one of the seats, the partial unique index
 * one_active_booking_per_seat rejects the insert and nothing is written, not even the message.
 */
export async function createPendingBooking(
  db: pg.Pool,
  input: CreateBookingInput,
): Promise<BookingRow> {
  return withTransaction(db, async (client) => {
    // Price from the database, never from the client.
    const priced = await client.query<{ seats: number; amount: number }>(
      `SELECT count(*)::int AS seats, coalesce(sum(price_minor), 0)::bigint AS amount
       FROM booking.seats WHERE event_id = $1 AND id = ANY($2::uuid[])`,
      [input.eventId, input.seatIds],
    );
    const { seats, amount } = priced.rows[0] ?? { seats: 0, amount: 0 };
    if (seats !== input.seatIds.length) throw new Error('hold references unknown seats');

    const inserted = await client.query<BookingRow>(
      `INSERT INTO booking.bookings
         (event_id, user_id, hold_id, status, amount_minor, idempotency_key, expires_at)
       VALUES ($1, $2, $3, 'PENDING_PAYMENT', $4, $5, now() + make_interval(secs => $6 / 1000.0))
       RETURNING ${BOOKING_COLUMNS}`,
      [
        input.eventId,
        input.userId,
        input.holdId,
        amount,
        input.idempotencyKey,
        input.paymentDeadlineMs,
      ],
    );
    const booking = inserted.rows[0];
    if (!booking) throw new Error('booking insert returned no row');

    await client.query(
      `INSERT INTO booking.booking_seats (booking_id, event_id, seat_id)
       SELECT $1, $2, unnest($3::uuid[])`,
      [booking.id, input.eventId, input.seatIds],
    );

    const envelope = createEnvelope(
      TOPICS.bookingRequested,
      {
        bookingId: booking.id,
        eventId: input.eventId,
        userId: input.userId,
        holdId: input.holdId,
        seatIds: [...input.seatIds],
        amountMinor: booking.amountMinor,
        expiresAt: booking.expiresAt.toISOString(),
      },
      { traceId: input.traceId },
    );
    await insertOutboxMessage(client, 'booking.outbox', input.requestedTopic, envelope);
    return booking;
  });
}

export interface BookingSeat {
  seatId: string;
  section: string;
  row: string;
  number: number;
  priceMinor: number;
  active: boolean;
}

export async function findBookingForUser(
  db: pg.Pool,
  bookingId: string,
  userId: string,
): Promise<(BookingRow & { seats: BookingSeat[] }) | undefined> {
  const { rows } = await db.query<BookingRow>(
    `SELECT ${BOOKING_COLUMNS} FROM booking.bookings WHERE id = $1 AND user_id = $2`,
    [bookingId, userId],
  );
  const booking = rows[0];
  if (!booking) return undefined;
  const seats = await db.query<BookingSeat>(
    `SELECT bs.seat_id AS "seatId", s.section, s.row_label AS row, s.seat_number AS number,
            s.price_minor AS "priceMinor", bs.active
     FROM booking.booking_seats bs
     JOIN booking.seats s ON s.id = bs.seat_id
     WHERE bs.booking_id = $1
     ORDER BY s.section, s.row_label, s.seat_number`,
    [bookingId],
  );
  return { ...booking, seats: seats.rows };
}

/** Seats claimed by active bookings: the source of truth for the seat map. */
export async function activeSeatClaims(
  db: pg.Pool,
  eventId: string,
): Promise<Map<string, BookingStatus>> {
  const { rows } = await db.query<{ seatId: string; status: BookingStatus }>(
    `SELECT bs.seat_id AS "seatId", b.status
     FROM booking.booking_seats bs
     JOIN booking.bookings b ON b.id = bs.booking_id
     WHERE bs.event_id = $1 AND bs.active`,
    [eventId],
  );
  return new Map(rows.map((row) => [row.seatId, row.status]));
}
