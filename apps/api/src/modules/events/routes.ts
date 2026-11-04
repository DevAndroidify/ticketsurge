import { getSeatStates, type SeatState } from '@ticketsurge/redis-scripts';
import type { FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import { notFound } from '../../errors.js';
import { listEvents, findEvent, listSeats } from './repository.js';
import { eventIdParams, listEventsQuery } from './schemas.js';

export type SeatStatus = 'available' | 'held' | 'sold';

export interface EventRoutesOptions {
  db: pg.Pool;
  redis: Redis;
}

export function eventRoutes(app: FastifyInstance, { db, redis }: EventRoutesOptions): void {
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

  /**
   * Seat map with live status from Redis. If Redis is down the map is still served with
   * `degraded: true`, so clients can show the layout but must not trust "available".
   */
  app.get('/events/:id/seats', async (request) => {
    const { id } = eventIdParams.parse(request.params);
    const event = await findEvent(db, id);
    if (!event) throw notFound('Event not found');

    const seats = await listSeats(db, id);
    let states = new Map<string, SeatState>();
    let degraded = false;
    if (redis.status === 'ready') {
      try {
        states = await getSeatStates(
          redis,
          id,
          seats.map((seat) => seat.id),
        );
      } catch (error) {
        request.log.warn({ err: error }, 'seat map served without live Redis status');
        degraded = true;
      }
    } else {
      degraded = true;
    }

    const withStatus = seats.map((seat) => ({ ...seat, status: toStatus(states.get(seat.id)) }));
    return {
      eventId: id,
      total: seats.length,
      degraded,
      counts: countByStatus(withStatus),
      seats: withStatus,
    };
  });
}

function toStatus(state: SeatState | undefined): SeatStatus {
  if (!state) return 'available';
  return state.kind === 'sold' ? 'sold' : 'held';
}

export function countByStatus(seats: { status: SeatStatus }[]): Record<SeatStatus, number> {
  const counts: Record<SeatStatus, number> = { available: 0, held: 0, sold: 0 };
  for (const seat of seats) counts[seat.status]++;
  return counts;
}
