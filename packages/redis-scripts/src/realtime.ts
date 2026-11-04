import type { Redis } from 'ioredis';
import { keys } from './keys.js';
import { RedisScript } from './script.js';

const markSoldScript = RedisScript.fromFile('markSold', 'mark_sold.lua');

export interface MarkSoldInput {
  eventId: string;
  holdId: string;
  bookingId: string;
  seatIds: readonly string[];
}

/** Marks seats sold:<bookingId> without TTL and drops the hold. See lua/mark_sold.lua. */
export async function markSold(redis: Redis, input: MarkSoldInput): Promise<number> {
  const reply = await markSoldScript.run(
    redis,
    [
      keys.hold(input.eventId, input.holdId),
      ...input.seatIds.map((seatId) => keys.seat(input.eventId, seatId)),
    ],
    [input.bookingId],
  );
  return Number(reply);
}

// ---------- Pub/Sub messages (forwarded to WebSocket clients by realtime-gateway) ----------

export type SeatStatusUpdate = 'held' | 'available' | 'sold';

export interface SeatsUpdatedMessage {
  type: 'seats.updated';
  eventId: string;
  seatIds: string[];
  status: SeatStatusUpdate;
}

export interface BookingUpdatedMessage {
  type: 'booking.updated';
  bookingId: string;
  status: 'PENDING_PAYMENT' | 'CONFIRMED' | 'CANCELLED' | 'EXPIRED';
}

export interface QueueUpdatedMessage {
  type: 'queue.updated';
  eventId: string;
  position: number;
  admitted: boolean;
}

export type UserMessage = BookingUpdatedMessage | QueueUpdatedMessage;

/**
 * Pub/Sub is fire-and-forget: a message published while a client is disconnected is lost. Clients
 * must reload the seat map and their booking status after every reconnect.
 */
export async function publishSeatsUpdated(
  redis: Redis,
  eventId: string,
  seatIds: readonly string[],
  status: SeatStatusUpdate,
): Promise<void> {
  const message: SeatsUpdatedMessage = {
    type: 'seats.updated',
    eventId,
    seatIds: [...seatIds],
    status,
  };
  await redis.publish(keys.seatsChannel(eventId), JSON.stringify(message));
}

export async function publishToUser(redis: Redis, userId: string, message: UserMessage) {
  await redis.publish(keys.userChannel(userId), JSON.stringify(message));
}
