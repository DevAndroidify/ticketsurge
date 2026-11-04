import { createHash, timingSafeEqual } from 'node:crypto';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type pg from 'pg';
import { unauthorized } from '../../errors.js';
import { createEventWithSeats } from '../events/repository.js';
import { createEventBody } from '../events/schemas.js';

export interface AdminRoutesOptions {
  db: pg.Pool;
  adminApiKey: string;
}

export function adminRoutes(app: FastifyInstance, { db, adminApiKey }: AdminRoutesOptions): void {
  const expected = sha256(adminApiKey);

  // Constant-time comparison of fixed-length digests, so response timing reveals nothing.
  const requireAdmin = (
    request: FastifyRequest,
    _reply: FastifyReply,
    done: (error?: Error) => void,
  ) => {
    const header = request.headers['x-admin-key'];
    const provided = typeof header === 'string' ? header : '';
    if (!timingSafeEqual(sha256(provided), expected)) {
      done(unauthorized('Invalid admin key'));
      return;
    }
    done();
  };

  app.post('/admin/events', { preHandler: requireAdmin }, async (request, reply) => {
    const body = createEventBody.parse(request.body);
    const event = await createEventWithSeats(db, body);
    return reply.code(201).send(event);
  });
}

function sha256(value: string): Buffer {
  return createHash('sha256').update(value).digest();
}
