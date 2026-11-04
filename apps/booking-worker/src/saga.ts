import type { Logger } from '@ticketsurge/config';
import { TOPICS, createEnvelope, topicName, type AnyEnvelope } from '@ticketsurge/events';
import { insertOutboxMessage, processOnce } from '@ticketsurge/outbox';
import type pg from 'pg';

export const SAGA_CONSUMER = 'booking-saga';

type PaymentCompleted = Extract<AnyEnvelope, { type: 'payment.completed' }>;

interface BookingRow {
  event_id: string;
  user_id: string;
  hold_id: string;
  amount_minor: number;
}

// pg returns BIGINT as a string. float8 is exact for minor-unit amounts (far below 2^53) and arrives
// as a JS number, independent of any process-wide type parser.
const BOOKING_RETURNING = 'event_id, user_id, hold_id, amount_minor::float8 AS amount_minor';

/**
 * Moves a PENDING_PAYMENT booking to CANCELLED or EXPIRED, deactivates its seats, and adds
 * booking.cancelled to the outbox, all inside the caller's transaction. The update is conditional on
 * `status = 'PENDING_PAYMENT'`, so whoever gets there second (saga or sweeper) changes nothing.
 * Returns false if the booking was no longer pending.
 */
export async function closePendingBooking(
  client: pg.PoolClient,
  bookingId: string,
  outcome:
    { status: 'CANCELLED'; reason: 'PAYMENT_FAILED' } | { status: 'EXPIRED'; reason: 'EXPIRED' },
  topicPrefix: string,
  traceId: string,
): Promise<boolean> {
  const { rows } = await client.query<BookingRow>(
    `UPDATE booking.bookings SET status = $2, updated_at = now()
     WHERE id = $1 AND status = 'PENDING_PAYMENT'
     RETURNING ${BOOKING_RETURNING}`,
    [bookingId, outcome.status],
  );
  const booking = rows[0];
  if (!booking) return false;
  const seatIds = await deactivateSeats(client, bookingId);
  await insertOutboxMessage(
    client,
    'booking.outbox',
    topicName(topicPrefix, TOPICS.bookingCancelled),
    createEnvelope(
      TOPICS.bookingCancelled,
      {
        bookingId,
        eventId: booking.event_id,
        userId: booking.user_id,
        holdId: booking.hold_id,
        seatIds,
        amountMinor: booking.amount_minor,
        reason: outcome.reason,
      },
      { traceId },
    ),
  );
  return true;
}

/**
 * Applies payment results to bookings:
 * - paid in time: CONFIRMED, publish booking.confirmed;
 * - payment failed: CANCELLED, seats deactivated, publish booking.cancelled;
 * - paid after expires_at: the booking is (or now becomes) EXPIRED and a refund is requested.
 *   The seats are never restored.
 */
export function createSagaHandler(db: pg.Pool, topicPrefix: string, logger: Logger) {
  async function confirm(client: pg.PoolClient, envelope: PaymentCompleted): Promise<boolean> {
    const { bookingId } = envelope.data;
    const { rows } = await client.query<BookingRow>(
      `UPDATE booking.bookings SET status = 'CONFIRMED', updated_at = now()
       WHERE id = $1 AND status = 'PENDING_PAYMENT' AND expires_at > now()
       RETURNING ${BOOKING_RETURNING}`,
      [bookingId],
    );
    const booking = rows[0];
    if (!booking) return false;
    await insertOutboxMessage(
      client,
      'booking.outbox',
      topicName(topicPrefix, TOPICS.bookingConfirmed),
      createEnvelope(
        TOPICS.bookingConfirmed,
        {
          bookingId,
          eventId: booking.event_id,
          userId: booking.user_id,
          holdId: booking.hold_id,
          seatIds: await activeSeats(client, bookingId),
          amountMinor: booking.amount_minor,
        },
        { traceId: envelope.traceId },
      ),
    );
    logger.info({ bookingId }, 'booking confirmed');
    return true;
  }

  /** A successful payment that could not confirm the booking. */
  async function latePayment(client: pg.PoolClient, envelope: PaymentCompleted): Promise<void> {
    const { bookingId, paymentId, amountMinor } = envelope.data;
    // Lock the row so the sweeper cannot change it between our read and our decision.
    const { rows } = await client.query<{ status: string; overdue: boolean }>(
      `SELECT status, expires_at <= now() AS overdue FROM booking.bookings WHERE id = $1 FOR UPDATE`,
      [bookingId],
    );
    const booking = rows[0];
    if (!booking) {
      logger.warn({ bookingId }, 'payment for an unknown booking');
      return;
    }
    let status = booking.status;
    if (status === 'PENDING_PAYMENT' && booking.overdue) {
      // Overdue but not swept yet: expire it now, exactly as the sweeper would.
      await closePendingBooking(
        client,
        bookingId,
        { status: 'EXPIRED', reason: 'EXPIRED' },
        topicPrefix,
        envelope.traceId,
      );
      status = 'EXPIRED';
    }
    if (status !== 'EXPIRED' && status !== 'CANCELLED') {
      logger.info({ bookingId, status }, 'payment result ignored');
      return;
    }
    await insertOutboxMessage(
      client,
      'booking.outbox',
      topicName(topicPrefix, TOPICS.refundRequested),
      createEnvelope(
        TOPICS.refundRequested,
        {
          bookingId,
          paymentId,
          amountMinor,
          reason: status === 'EXPIRED' ? 'PAID_AFTER_EXPIRY' : 'BOOKING_NOT_PENDING',
        },
        { traceId: envelope.traceId },
      ),
    );
    logger.warn({ bookingId, paymentId, status }, 'payment arrived too late, refund requested');
  }

  async function onPaymentCompleted(envelope: PaymentCompleted) {
    const { bookingId, status } = envelope.data;
    await processOnce(
      db,
      'booking.processed_events',
      SAGA_CONSUMER,
      envelope.messageId,
      async (client) => {
        if (status === 'SUCCEEDED') {
          if (!(await confirm(client, envelope))) await latePayment(client, envelope);
          return;
        }
        const cancelled = await closePendingBooking(
          client,
          bookingId,
          { status: 'CANCELLED', reason: 'PAYMENT_FAILED' },
          topicPrefix,
          envelope.traceId,
        );
        if (cancelled) logger.info({ bookingId }, 'booking cancelled: payment failed');
        else logger.info({ bookingId }, 'payment failed for a booking that is no longer pending');
      },
    );
  }

  return async (envelope: AnyEnvelope): Promise<void> => {
    if (envelope.type === 'payment.completed') await onPaymentCompleted(envelope);
  };
}

async function activeSeats(client: pg.PoolClient, bookingId: string): Promise<string[]> {
  const { rows } = await client.query<{ seat_id: string }>(
    'SELECT seat_id FROM booking.booking_seats WHERE booking_id = $1 AND active ORDER BY seat_id',
    [bookingId],
  );
  return rows.map((row) => row.seat_id);
}

/** Frees the booking's seats (the partial unique index stops covering them). Returns their IDs. */
async function deactivateSeats(client: pg.PoolClient, bookingId: string): Promise<string[]> {
  const { rows } = await client.query<{ seat_id: string }>(
    `UPDATE booking.booking_seats SET active = false
     WHERE booking_id = $1 AND active RETURNING seat_id`,
    [bookingId],
  );
  return rows.map((row) => row.seat_id).sort();
}
