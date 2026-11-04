import type { FastifyInstance, preHandlerAsyncHookHandler } from 'fastify';
import type pg from 'pg';
import { createEventWithSeats } from '../events/repository.js';
import { createEventBody } from '../events/schemas.js';

export interface AdminRoutesOptions {
  db: pg.Pool;
  /** Rejects requests without a token (401) or from non-admin users (403). */
  requireAdmin: preHandlerAsyncHookHandler;
}

export function adminRoutes(app: FastifyInstance, { db, requireAdmin }: AdminRoutesOptions): void {
  app.post('/admin/events', { preHandler: requireAdmin }, async (request, reply) => {
    const body = createEventBody.parse(request.body);
    const event = await createEventWithSeats(db, body);
    return reply.code(201).send(event);
  });
}
