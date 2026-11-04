import type { Logger } from '@ticketsurge/config';
import type { AnyEnvelope } from '@ticketsurge/events';
import { processOnce } from '@ticketsurge/outbox';
import type pg from 'pg';

export const CONSUMER_NAME = 'notification-service';

export type NotificationType = 'TICKETS_ISSUED' | 'BOOKING_CANCELLED' | 'BOOKING_EXPIRED';

/** Stand-in for an email/SMS provider. Real providers are out of scope. */
export type Sender = (notification: {
  userId: string;
  bookingId: string;
  type: NotificationType;
  text: string;
}) => void;

/**
 * Records and "sends" one notification per booking outcome:
 * - ticket.issued      -> TICKETS_ISSUED (sent after tickets exist, as the brief requires);
 * - booking.cancelled  -> BOOKING_CANCELLED or BOOKING_EXPIRED.
 *
 * processed_events stops redelivered messages, and the unique (booking_id, type) stops replays that
 * arrive with a new messageId, so a user is never notified twice about the same thing.
 *
 * The simulated send happens after the insert commits. With a real provider this would go through
 * an outbox as well; a crash between commit and send would then lose at most one message, never
 * duplicate it.
 */
export function createNotificationHandler(db: pg.Pool, logger: Logger, send: Sender) {
  async function record(
    messageId: string,
    userId: string,
    bookingId: string,
    type: NotificationType,
    text: string,
  ) {
    // Set inside the transaction callback, read after it commits.
    const outcome = { inserted: false };
    await processOnce(
      db,
      'notification.processed_events',
      CONSUMER_NAME,
      messageId,
      async (client) => {
        const { rowCount } = await client.query(
          `INSERT INTO notification.notifications (user_id, booking_id, type)
         VALUES ($1, $2, $3)
         ON CONFLICT ON CONSTRAINT notifications_booking_type_unique DO NOTHING`,
          [userId, bookingId, type],
        );
        outcome.inserted = rowCount === 1;
      },
    );
    if (outcome.inserted) send({ userId, bookingId, type, text });
    else logger.debug({ bookingId, type }, 'notification already sent');
  }

  return async (envelope: AnyEnvelope): Promise<void> => {
    if (envelope.type === 'ticket.issued') {
      const { userId, bookingId, tickets } = envelope.data;
      await record(
        envelope.messageId,
        userId,
        bookingId,
        'TICKETS_ISSUED',
        `Your ${tickets.length} ticket(s): ${tickets.map((t) => t.code).join(', ')}`,
      );
    } else if (envelope.type === 'booking.cancelled') {
      const { userId, bookingId, reason } = envelope.data;
      const expired = reason === 'EXPIRED';
      await record(
        envelope.messageId,
        userId,
        bookingId,
        expired ? 'BOOKING_EXPIRED' : 'BOOKING_CANCELLED',
        expired
          ? 'Your booking expired before payment completed. Your seats were released.'
          : 'Your payment failed. Your booking was cancelled and your seats were released.',
      );
    }
  };
}
