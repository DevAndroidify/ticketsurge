import { deleteHoldRef, markSold, restoreHold } from '@ticketsurge/redis-scripts';
import type { Redis } from 'ioredis';
import type pg from 'pg';

export interface RebuildResult {
  soldBookings: number;
  soldSeats: number;
  heldBookings: number;
  heldSeats: number;
}

interface BookingSeats {
  id: string;
  event_id: string;
  user_id: string;
  hold_id: string;
  seat_ids: string[];
  ttl_ms: number;
}

const CHUNK = 200;

async function inChunks<T>(items: T[], fn: (item: T) => Promise<unknown>) {
  for (let i = 0; i < items.length; i += CHUNK) {
    await Promise.all(items.slice(i, i + CHUNK).map(fn));
  }
}

/**
 * Restores Redis seat state from PostgreSQL, the source of truth, after Redis lost its data:
 * - CONFIRMED bookings: their active seats become sold:<bookingId> (markSold);
 * - PENDING_PAYMENT bookings: their hold is recreated with a TTL that ends `holdAfterDeadlineMs`
 *   after the payment deadline, as POST /bookings would have set it.
 *
 * Holds without a booking and queue places are not recoverable (PostgreSQL never knew about them):
 * those users select seats again or rejoin the queue. Safe to run repeatedly.
 */
export async function rebuildRedis(
  db: pg.Pool,
  redis: Redis,
  { holdAfterDeadlineMs }: { holdAfterDeadlineMs: number },
): Promise<RebuildResult> {
  const query = (status: string) =>
    db.query<BookingSeats>(
      `SELECT b.id, b.event_id, b.user_id, b.hold_id,
              array_agg(bs.seat_id ORDER BY bs.seat_id) AS seat_ids,
              GREATEST(1000, (extract(epoch FROM (b.expires_at - now())) * 1000)::bigint + $2)::float8
                AS ttl_ms
       FROM booking.bookings b
       JOIN booking.booking_seats bs ON bs.booking_id = b.id AND bs.active
       WHERE b.status = $1
       GROUP BY b.id`,
      [status, holdAfterDeadlineMs],
    );

  const confirmed = (await query('CONFIRMED')).rows;
  await inChunks(confirmed, async (b) => {
    await markSold(redis, {
      eventId: b.event_id,
      holdId: b.hold_id,
      bookingId: b.id,
      seatIds: b.seat_ids,
    });
    await deleteHoldRef(redis, b.hold_id);
  });

  const pending = (await query('PENDING_PAYMENT')).rows;
  await inChunks(pending, (b) =>
    restoreHold(redis, {
      eventId: b.event_id,
      holdId: b.hold_id,
      userId: b.user_id,
      seatIds: b.seat_ids,
      ttlMs: b.ttl_ms,
    }),
  );

  return {
    soldBookings: confirmed.length,
    soldSeats: confirmed.reduce((n, b) => n + b.seat_ids.length, 0),
    heldBookings: pending.length,
    heldSeats: pending.reduce((n, b) => n + b.seat_ids.length, 0),
  };
}
