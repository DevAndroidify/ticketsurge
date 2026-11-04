import type { FastifyInstance } from 'fastify';
import type pg from 'pg';
import { notFound } from '../../errors.js';
import { listEvents, findEvent, listSeats } from './repository.js';
import { eventIdParams, listEventsQuery } from './schemas.js';

export type SeatStatus = 'available' | 'held' | 'sold';

export interface EventRoutesOptions {
  db: pg.Pool;
}

export function eventRoutes(app: FastifyInstance, { db }: EventRoutesOptions): void {
  app.get('/events', async (request) => {
    const query = listEventsQuery.parse(request.query);
    const events = await listEvents(db, query);
    return { events, limit: query.limit, offset: query.offset };
  });

  app.get('/events/:id', async (request) => {
    const { id } = eventIdParams.parse(request.params);
    const event = await findEvent(db, id);
    if (!event) throw notFound('Event not found');
    return event;
  });

  app.get('/events/:id/seats', async (request) => {
    const { id } = eventIdParams.parse(request.params);
    const event = await findEvent(db, id);
    if (!event) throw notFound('Event not found');

    const seats = await listSeats(db, id);
    const withStatus = seats.map((seat) => ({ ...seat, status: 'available' as const }));
    return {
      eventId: id,
      total: seats.length,
      counts: countByStatus(withStatus),
      seats: withStatus,
    };
  });
}

export function countByStatus(seats: { status: SeatStatus }[]): Record<SeatStatus, number> {
  const counts: Record<SeatStatus, number> = { available: 0, held: 0, sold: 0 };
  for (const seat of seats) counts[seat.status]++;
  return counts;
}
