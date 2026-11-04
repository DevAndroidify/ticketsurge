import {
  deleteHoldRef,
  extendHold,
  extendHoldRef,
  getHold,
  getHoldRef,
  publishToUser,
  releaseHold,
} from '@ticketsurge/redis-scripts';
import { TOPICS, topicName } from '@ticketsurge/events';
import type { FastifyInstance, preHandlerAsyncHookHandler } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { z } from 'zod';
import type { ApiConfig } from '../../config.js';
import { PG_UNIQUE_VIOLATION, pgErrorInfo } from '../../db.js';
import { HttpError, badRequest, conflict, notFound } from '../../errors.js';
import { notify, withRedis } from '../../redis.js';
import { currentUser } from '../auth/guard.js';
import {
  createPendingBooking,
  findBookingForUser,
  findByIdempotencyKey,
  type BookingRow,
} from './repository.js';

export interface BookingRoutesOptions {
  db: pg.Pool;
  redis: Redis;
  config: Pick<ApiConfig, 'PAYMENT_DEADLINE_MS' | 'HOLD_AFTER_DEADLINE_MS' | 'KAFKA_TOPIC_PREFIX'>;
  authenticate: preHandlerAsyncHookHandler;
  bookingRateLimit: preHandlerAsyncHookHandler;
}

const bookingBody = z.object({ holdId: z.uuid() });
const bookingParams = z.object({ id: z.uuid() });
// Printable ASCII, so keys are safe to log and compare.
const idempotencyKey = z
  .string({ error: 'Idempotency-Key header is required' })
  .regex(/^[\x21-\x7e]{1,255}$/, 'Idempotency-Key must be 1-255 printable ASCII characters');

const holdGone = () => conflict('HOLD_EXPIRED', 'The hold has expired or does not exist');

function accepted(booking: BookingRow) {
  return {
    bookingId: booking.id,
    status: booking.status,
    amountMinor: booking.amountMinor,
    expiresAt: booking.expiresAt,
  };
}

export function bookingRoutes(app: FastifyInstance, options: BookingRoutesOptions): void {
  const { db, redis, config, authenticate, bookingRateLimit } = options;

  /** Same key + same hold returns the original booking; same key + other hold is an error. */
  const replay = (existing: BookingRow, holdId: string) => {
    if (existing.holdId !== holdId) {
      throw new HttpError(
        422,
        'IDEMPOTENCY_KEY_REUSED',
        'This Idempotency-Key was already used for a different hold',
      );
    }
    return accepted(existing);
  };

  app.post(
    '/bookings',
    { preHandler: [authenticate, bookingRateLimit] },
    async (request, reply) => {
      const user = currentUser(request);
      const parsedKey = idempotencyKey.safeParse(request.headers['idempotency-key']);
      if (!parsedKey.success) {
        throw badRequest(
          'IDEMPOTENCY_KEY_REQUIRED',
          parsedKey.error.issues[0]?.message ?? 'invalid',
        );
      }
      const key = parsedKey.data;
      const { holdId } = bookingBody.parse(request.body);

      const existing = await findByIdempotencyKey(db, user.id, key);
      if (existing) return reply.code(202).send(replay(existing, holdId));

      // a + b: the hold must exist, belong to this user, and be extended to cover the payment window.
      const holdTtlMs = config.PAYMENT_DEADLINE_MS + config.HOLD_AFTER_DEADLINE_MS;
      const { eventId, seatIds } = await withRedis(redis, async () => {
        const ref = await getHoldRef(redis, holdId);
        const hold = ref ? await getHold(redis, ref, holdId) : undefined;
        if (!ref || !hold || hold.owner !== user.id) throw holdGone();
        // Extend the lookup key first so it can never expire before the hold.
        await extendHoldRef(redis, holdId, holdTtlMs);
        const extended = await extendHold(redis, {
          eventId: ref,
          holdId,
          userId: user.id,
          ttlMs: holdTtlMs,
        });
        if (!extended) throw holdGone();
        return { eventId: ref, seatIds: hold.seatIds };
      });

      // c: booking, its seats, and the booking.requested outbox row in one transaction.
      // d: the unique index is the final protection.
      try {
        const booking = await createPendingBooking(db, {
          eventId,
          userId: user.id,
          holdId,
          seatIds,
          idempotencyKey: key,
          paymentDeadlineMs: config.PAYMENT_DEADLINE_MS,
          requestedTopic: topicName(config.KAFKA_TOPIC_PREFIX, TOPICS.bookingRequested),
          traceId: request.id,
        });
        await notify(request.log, () =>
          publishToUser(redis, user.id, {
            type: 'booking.updated',
            bookingId: booking.id,
            status: 'PENDING_PAYMENT',
          }),
        );
        return await reply.code(202).send(accepted(booking));
      } catch (error) {
        const info = pgErrorInfo(error);
        if (info.code !== PG_UNIQUE_VIOLATION) throw error;

        if (info.constraint === 'one_active_booking_per_seat') {
          // Redis let the hold through (e.g. after a Redis restart), but PostgreSQL says no.
          await withRedis(redis, async () => {
            await releaseHold(redis, { eventId, holdId, seatIds, owner: user.id });
            await deleteHoldRef(redis, holdId);
          }).catch((releaseError: unknown) => {
            request.log.warn({ err: releaseError, holdId }, 'could not release rejected hold');
          });
          throw conflict('SEAT_TAKEN', 'One or more seats were already booked');
        }

        // A concurrent request with the same key won the insert: return its booking.
        const winner = await findByIdempotencyKey(db, user.id, key);
        if (winner) return reply.code(202).send(replay(winner, holdId));
        if (info.constraint === 'bookings_hold_id_unique') {
          throw conflict('HOLD_ALREADY_BOOKED', 'This hold already has a booking');
        }
        throw error;
      }
    },
  );

  app.get('/bookings/:id', { preHandler: authenticate }, async (request) => {
    const user = currentUser(request);
    const { id } = bookingParams.parse(request.params);
    // Other users' bookings return 404, so booking IDs cannot be probed.
    const booking = await findBookingForUser(db, id, user.id);
    if (!booking) throw notFound('Booking not found');
    const { userId: _userId, holdId: _holdId, ...visible } = booking;
    return visible;
  });
}
