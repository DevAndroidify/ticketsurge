import { joinQueue, queueStatus, type QueueStatus } from '@ticketsurge/redis-scripts';
import type { FastifyInstance, preHandlerAsyncHookHandler } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { z } from 'zod';
import { forbidden, notFound } from '../../errors.js';
import { withRedis } from '../../redis.js';
import { currentUser } from '../auth/guard.js';

export interface QueueRoutesOptions {
  db: pg.Pool;
  redis: Redis;
  authenticate: preHandlerAsyncHookHandler;
  joinRateLimit: preHandlerAsyncHookHandler;
}

const eventParams = z.object({ id: z.uuid() });

function present(eventId: string, status: QueueStatus) {
  return status.admitted
    ? {
        eventId,
        admitted: true,
        position: 0,
        admissionExpiresAt: new Date(Date.now() + status.admissionTtlMs).toISOString(),
      }
    : { eventId, admitted: false, position: status.position };
}

export function queueRoutes(app: FastifyInstance, options: QueueRoutesOptions): void {
  const { db, redis, authenticate, joinRateLimit } = options;

  /** Joining is allowed before the sale opens, so the waiting room can fill up early. */
  app.post(
    '/events/:id/queue',
    { preHandler: [authenticate, joinRateLimit] },
    async (request, reply) => {
      const user = currentUser(request);
      const { id: eventId } = eventParams.parse(request.params);
      const { rows } = await db.query<{ status: string }>(
        `SELECT status FROM booking.events WHERE id = $1 AND status <> 'DRAFT'`,
        [eventId],
      );
      const event = rows[0];
      if (!event) throw notFound('Event not found');
      if (event.status !== 'PUBLISHED')
        throw forbidden('SALE_NOT_OPEN', 'This event is not on sale');

      const status = await withRedis(redis, () => joinQueue(redis, eventId, user.id));
      return reply.code(200).send(present(eventId, status));
    },
  );

  app.get('/events/:id/queue/me', { preHandler: authenticate }, async (request) => {
    const user = currentUser(request);
    const { id: eventId } = eventParams.parse(request.params);
    const status = await withRedis(redis, () => queueStatus(redis, eventId, user.id));
    if (!status) throw notFound('Not in the queue for this event');
    return present(eventId, status);
  });
}
