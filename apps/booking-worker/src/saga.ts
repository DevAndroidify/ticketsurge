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
export const BOOKING_RETURNING = 'event_id, user_id, hold_id, amount_minor::float8 AS amount_minor';

/**
 * Applies payment results to bookings. Every status change is a conditional update on
 * `status = 'PENDING_PAYMENT'`, so a payment result and the expiry sweeper can never both win.
 */
export function createSagaHandler(db: pg.Pool, topicPrefix: string, logger: Logger) {
  async function onPaymentCompleted(envelope: PaymentCompleted) {
    const { bookingId, status } = envelope.data;

    await processOnce(
      db,
      'booking.processed_events',
      SAGA_CONSUMER,
      envelope.messageId,
      async (client) => {
        if (status === 'SUCCEEDED') {
          // Paid in time: confirm. A payment that arrives after expires_at does not confirm.
          const confirmed = await client.query<BookingRow>(
            `UPDATE booking.bookings SET status = 'CONFIRMED', updated_at = now()
           WHERE id = $1 AND status = 'PENDING_PAYMENT' AND expires_at > now()
           RETURNING ${BOOKING_RETURNING}`,
            [bookingId],
          );
          const booking = confirmed.rows[0];
          if (!booking) {
            logger.warn({ bookingId }, 'payment succeeded for a booking that is no longer pending');
            return;
          }
          const seatIds = await activeSeats(client, bookingId);
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
                seatIds,
                amountMinor: booking.amount_minor,
              },
              { traceId: envelope.traceId },
            ),
          );
          logger.info({ bookingId }, 'booking confirmed');
          return;
        }

        // Payment failed: cancel and free the seats in the same transaction.
        const cancelled = await client.query<BookingRow>(
          `UPDATE booking.bookings SET status = 'CANCELLED', updated_at = now()
         WHERE id = $1 AND status = 'PENDING_PAYMENT'
         RETURNING ${BOOKING_RETURNING}`,
          [bookingId],
        );
        const booking = cancelled.rows[0];
        if (!booking) {
          logger.info({ bookingId }, 'payment failed for a booking that is no longer pending');
          return;
        }
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
              reason: 'PAYMENT_FAILED',
            },
            { traceId: envelope.traceId },
          ),
        );
        logger.info({ bookingId }, 'booking cancelled: payment failed');
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
export async function deactivateSeats(client: pg.PoolClient, bookingId: string): Promise<string[]> {
  const { rows } = await client.query<{ seat_id: string }>(
    `UPDATE booking.booking_seats SET active = false
     WHERE booking_id = $1 AND active RETURNING seat_id`,
    [bookingId],
  );
  return rows.map((row) => row.seat_id).sort();
}
