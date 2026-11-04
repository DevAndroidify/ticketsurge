import type { Logger } from '@ticketsurge/config';
import type { AnyEnvelope } from '@ticketsurge/events';
import {
  deleteHoldRef,
  markSold,
  publishSeatsUpdated,
  publishToUser,
  releaseHold,
} from '@ticketsurge/redis-scripts';
import type { Redis } from 'ioredis';

/**
 * Mirrors booking outcomes into Redis and tells connected clients:
 * - booking.confirmed: seats become sold:<bookingId> (no TTL);
 * - booking.cancelled: the booking's hold is released, so the seats can be held again.
 *
 * Redis only, so no processed_events table: markSold and releaseHold give the same result however
 * often they run, and the Pub/Sub messages are idempotent for clients (they carry the new state).
 * If Redis is down the handler throws and the consumer retries until it is back.
 */
export function createSeatSyncHandler(redis: Redis, logger: Logger) {
  return async (envelope: AnyEnvelope): Promise<void> => {
    if (envelope.type === 'booking.confirmed') {
      const { bookingId, eventId, holdId, userId, seatIds } = envelope.data;
      await markSold(redis, { eventId, holdId, bookingId, seatIds });
      await deleteHoldRef(redis, holdId);
      await publishSeatsUpdated(redis, eventId, seatIds, 'sold');
      await publishToUser(redis, userId, {
        type: 'booking.updated',
        bookingId,
        status: 'CONFIRMED',
      });
      logger.debug({ bookingId, seats: seatIds.length }, 'seats marked sold');
      return;
    }

    if (envelope.type === 'booking.cancelled') {
      const { bookingId, eventId, holdId, userId, seatIds, reason } = envelope.data;
      // No owner check: this is a system release. Only seats still showing this hold are freed.
      await releaseHold(redis, { eventId, holdId, seatIds });
      await deleteHoldRef(redis, holdId);
      await publishSeatsUpdated(redis, eventId, seatIds, 'available');
      await publishToUser(redis, userId, {
        type: 'booking.updated',
        bookingId,
        status: reason === 'EXPIRED' ? 'EXPIRED' : 'CANCELLED',
      });
      logger.debug({ bookingId, seats: seatIds.length, reason }, 'seats released');
    }
  };
}
