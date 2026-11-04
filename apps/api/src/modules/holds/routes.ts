import { randomUUID } from 'node:crypto';
import {
  deleteHoldRef,
  getHold,
  getHoldRef,
  holdSeats,
  releaseHold,
  setHoldRef,
} from '@ticketsurge/redis-scripts';
import type { FastifyInstance, preHandlerAsyncHookHandler } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { z } from 'zod';
import type { ApiConfig } from '../../config.js';
import { badRequest, conflict, forbidden, notFound } from '../../errors.js';
import { withRedis } from '../../redis.js';
import { currentUser } from '../auth/guard.js';

export interface HoldRoutesOptions {
  db: pg.Pool;
  redis: Redis;
  config: Pick<ApiConfig, 'HOLD_TTL_MS' | 'MAX_SEATS_PER_HOLD' | 'ADMISSION_BYPASS'>;
  authenticate: preHandlerAsyncHookHandler;
}

const eventParams = z.object({ id: z.uuid() });
const holdParams = z.object({ holdId: z.uuid() });

export function holdRoutes(app: FastifyInstance, options: HoldRoutesOptions): void {
  const { db, redis, config, authenticate } = options;

  const holdBody = z.object({
    seatIds: z
      .array(z.uuid())
      .min(1)
      .max(config.MAX_SEATS_PER_HOLD)
      .refine((ids) => new Set(ids).size === ids.length, 'seat IDs must be unique'),
  });

  app.post('/events/:id/holds', { preHandler: authenticate }, async (request, reply) => {
    const user = currentUser(request);
    const { id: eventId } = eventParams.parse(request.params);
    const { seatIds } = holdBody.parse(request.body);

    const { rows } = await db.query<{ status: string; on_sale: boolean }>(
      `SELECT status, sale_starts_at <= now() AS on_sale FROM booking.events
       WHERE id = $1 AND status <> 'DRAFT'`,
      [eventId],
    );
    const event = rows[0];
    if (!event) throw notFound('Event not found');
    if (event.status !== 'PUBLISHED' || !event.on_sale) {
      throw forbidden('SALE_NOT_OPEN', 'This event is not on sale');
    }

    // Redis keys are only created for real seats of this event.
    const known = await db.query<{ id: string }>(
      'SELECT id FROM booking.seats WHERE event_id = $1 AND id = ANY($2::uuid[])',
      [eventId, seatIds],
    );
    if (known.rowCount !== seatIds.length) {
      const found = new Set(known.rows.map((row) => row.id));
      throw badRequest('UNKNOWN_SEATS', 'Some seats do not belong to this event', {
        seatIds: seatIds.filter((seatId) => !found.has(seatId)),
      });
    }

    const holdId = randomUUID();
    const ttlMs = config.HOLD_TTL_MS;
    const result = await withRedis(redis, async () => {
      // Written first so it always outlives the hold; harmless if the hold then fails.
      await setHoldRef(redis, holdId, eventId, ttlMs);
      const outcome = await holdSeats(redis, {
        eventId,
        holdId,
        userId: user.id,
        seatIds,
        ttlMs,
        requireAdmission: !config.ADMISSION_BYPASS,
      });
      if (outcome.status !== 'held') await deleteHoldRef(redis, holdId);
      return outcome;
    });

    if (result.status === 'not_admitted') {
      throw forbidden('NOT_ADMITTED', 'Join the queue and wait to be admitted first');
    }
    if (result.status === 'conflict') {
      throw conflict('SEATS_UNAVAILABLE', 'Some seats are no longer available', {
        seatIds: result.seatIds,
      });
    }
    return reply.code(201).send({
      holdId,
      eventId,
      seatIds,
      expiresAt: new Date(Date.now() + ttlMs).toISOString(),
    });
  });

  app.delete('/holds/:holdId', { preHandler: authenticate }, async (request, reply) => {
    const user = currentUser(request);
    const { holdId } = holdParams.parse(request.params);

    const released = await withRedis(redis, async () => {
      const eventId = await getHoldRef(redis, holdId);
      if (!eventId) return false;
      const hold = await getHold(redis, eventId, holdId);
      // Another user's hold looks exactly like a missing one, so hold IDs cannot be probed.
      if (!hold || hold.owner !== user.id) return false;
      const outcome = await releaseHold(redis, {
        eventId,
        holdId,
        seatIds: hold.seatIds,
        owner: user.id,
      });
      if (outcome === 'not_owner') return false;
      await deleteHoldRef(redis, holdId);
      return true;
    });

    if (!released) throw notFound('Hold not found or already expired');
    return reply.code(204).send();
  });
}
