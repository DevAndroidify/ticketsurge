import type { Logger } from '@ticketsurge/config';
import type { AnyEnvelope } from '@ticketsurge/events';
import { processOnce } from '@ticketsurge/outbox';
import type pg from 'pg';

export const CONSUMER_NAME = 'analytics';

/**
 * Maintains analytics.sales_by_event from booking.confirmed and booking.cancelled.
 * Increments are only safe because processed_events guarantees each message is applied once; a
 * rebuild clears both tables together and replays the topics from the beginning.
 */
export function createAnalyticsHandler(db: pg.Pool, logger: Logger) {
  return async (envelope: AnyEnvelope): Promise<void> => {
    if (envelope.type === 'booking.confirmed') {
      const { eventId, seatIds, amountMinor } = envelope.data;
      await processOnce(
        db,
        'analytics.processed_events',
        CONSUMER_NAME,
        envelope.messageId,
        async (client) => {
          await client.query(
            `INSERT INTO analytics.sales_by_event (event_id, confirmed_bookings, seats_sold, revenue_minor)
           VALUES ($1, 1, $2, $3)
           ON CONFLICT (event_id) DO UPDATE SET
             confirmed_bookings = sales_by_event.confirmed_bookings + 1,
             seats_sold = sales_by_event.seats_sold + EXCLUDED.seats_sold,
             revenue_minor = sales_by_event.revenue_minor + EXCLUDED.revenue_minor,
             updated_at = now()`,
            [eventId, seatIds.length, amountMinor],
          );
        },
      );
      return;
    }
    if (envelope.type === 'booking.cancelled') {
      const { eventId, reason } = envelope.data;
      const expired = reason === 'EXPIRED' ? 1 : 0;
      await processOnce(
        db,
        'analytics.processed_events',
        CONSUMER_NAME,
        envelope.messageId,
        async (client) => {
          await client.query(
            `INSERT INTO analytics.sales_by_event (event_id, cancelled_bookings, expired_bookings)
           VALUES ($1, $2, $3)
           ON CONFLICT (event_id) DO UPDATE SET
             cancelled_bookings = sales_by_event.cancelled_bookings + EXCLUDED.cancelled_bookings,
             expired_bookings = sales_by_event.expired_bookings + EXCLUDED.expired_bookings,
             updated_at = now()`,
            [eventId, 1 - expired, expired],
          );
        },
      );
      return;
    }
    logger.debug({ type: envelope.type }, 'ignored');
  };
}

/** Empties the derived tables before a replay. */
export async function clearAnalytics(db: pg.Pool): Promise<void> {
  await db.query('TRUNCATE analytics.sales_by_event, analytics.processed_events');
}
