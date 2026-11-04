import { randomBytes } from 'node:crypto';
import type { Logger } from '@ticketsurge/config';
import { TOPICS, createEnvelope, topicName, type AnyEnvelope } from '@ticketsurge/events';
import { insertOutboxMessage, processOnce } from '@ticketsurge/outbox';
import type pg from 'pg';

export const CONSUMER_NAME = 'ticket-service';

// Crockford base32 without I, L, O, U: easy to read aloud and type.
const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

/** A random ticket code like TS-7K3M-Q9XA-2PRD (60 bits of randomness). */
export function ticketCode(): string {
  const bytes = randomBytes(12);
  const chars = [...bytes].map((byte) => ALPHABET[byte % 32] ?? '0');
  return `TS-${chars.slice(0, 4).join('')}-${chars.slice(4, 8).join('')}-${chars.slice(8, 12).join('')}`;
}

/**
 * Issues one ticket per seat of a confirmed booking and publishes ticket.issued.
 *
 * Duplicates are harmless twice over: processed_events stops a redelivered message, and the unique
 * (event_id, seat_id) constraint stops a replay that arrives with a new messageId. ticket.issued is
 * only published when this message actually created tickets.
 */
export function createTicketHandler(db: pg.Pool, topicPrefix: string, logger: Logger) {
  return async (envelope: AnyEnvelope): Promise<void> => {
    if (envelope.type !== 'booking.confirmed') return;
    const { bookingId, eventId, userId, seatIds } = envelope.data;

    await processOnce(
      db,
      'ticket.processed_events',
      CONSUMER_NAME,
      envelope.messageId,
      async (client) => {
        const inserted = await client.query<{ id: string }>(
          `INSERT INTO ticket.tickets (booking_id, event_id, seat_id, user_id, code)
         SELECT $1, $2, seat_id, $3, code
         FROM unnest($4::uuid[], $5::text[]) AS t(seat_id, code)
         ON CONFLICT ON CONSTRAINT tickets_event_seat_unique DO NOTHING
         RETURNING id`,
          [bookingId, eventId, userId, seatIds, seatIds.map(() => ticketCode())],
        );
        if (inserted.rowCount !== seatIds.length) {
          // Seats that already had a ticket must belong to this same booking (a replay). Anything else
          // would mean a seat was sold twice, which the booking schema is built to prevent.
          const { rows } = await client.query<{ seat_id: string }>(
            `SELECT seat_id FROM ticket.tickets
           WHERE event_id = $1 AND seat_id = ANY($2::uuid[]) AND booking_id <> $3`,
            [eventId, seatIds, bookingId],
          );
          if (rows.length > 0) {
            logger.error(
              { bookingId, seatIds: rows.map((r) => r.seat_id) },
              'seat already ticketed for another booking',
            );
          }
        }
        if (!inserted.rowCount) {
          logger.info({ bookingId }, 'tickets already issued');
          return;
        }

        const { rows: tickets } = await client.query<{
          ticketId: string;
          seatId: string;
          code: string;
        }>(
          `SELECT id AS "ticketId", seat_id AS "seatId", code FROM ticket.tickets
         WHERE booking_id = $1 ORDER BY seat_id`,
          [bookingId],
        );
        await insertOutboxMessage(
          client,
          'ticket.outbox',
          topicName(topicPrefix, TOPICS.ticketIssued),
          createEnvelope(
            TOPICS.ticketIssued,
            { bookingId, eventId, userId, tickets },
            { traceId: envelope.traceId },
          ),
        );
        logger.info({ bookingId, tickets: tickets.length }, 'tickets issued');
      },
    );
  };
}
