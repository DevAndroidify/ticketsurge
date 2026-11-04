import type pg from 'pg';
import { withTransaction } from '../../db.js';
import type { CreateEventInput } from './schemas.js';

export type EventStatus = 'DRAFT' | 'PUBLISHED' | 'CANCELLED';

export interface EventRecord {
  id: string;
  name: string;
  venue: string;
  startsAt: Date;
  saleStartsAt: Date;
  status: EventStatus;
  totalSeats: number;
}

export interface SeatRecord {
  id: string;
  section: string;
  row: string;
  number: number;
  priceMinor: number;
}

const EVENT_COLUMNS = `
  e.id, e.name, e.venue, e.starts_at AS "startsAt", e.sale_starts_at AS "saleStartsAt", e.status,
  (SELECT count(*) FROM booking.seats s WHERE s.event_id = e.id)::int AS "totalSeats"`;

/** Public listing: everything except drafts, soonest first. */
export async function listEvents(
  db: pg.Pool,
  { limit, offset }: { limit: number; offset: number },
): Promise<EventRecord[]> {
  const { rows } = await db.query<EventRecord>(
    `SELECT ${EVENT_COLUMNS}
     FROM booking.events e
     WHERE e.status <> 'DRAFT'
     ORDER BY e.starts_at, e.id
     LIMIT $1 OFFSET $2`,
    [limit, offset],
  );
  return rows;
}

export async function findEvent(db: pg.Pool, id: string): Promise<EventRecord | undefined> {
  const { rows } = await db.query<EventRecord>(
    `SELECT ${EVENT_COLUMNS} FROM booking.events e WHERE e.id = $1 AND e.status <> 'DRAFT'`,
    [id],
  );
  return rows[0];
}

export async function listSeats(db: pg.Pool, eventId: string): Promise<SeatRecord[]> {
  const { rows } = await db.query<SeatRecord>(
    `SELECT id, section, row_label AS row, seat_number AS number, price_minor AS "priceMinor"
     FROM booking.seats
     WHERE event_id = $1
     ORDER BY section, row_label, seat_number`,
    [eventId],
  );
  return rows;
}

/** Inserts the event and all of its seats atomically. */
export async function createEventWithSeats(
  db: pg.Pool,
  input: CreateEventInput,
): Promise<EventRecord> {
  return withTransaction(db, async (client) => {
    const { rows } = await client.query<Omit<EventRecord, 'totalSeats'>>(
      `INSERT INTO booking.events (name, venue, starts_at, sale_starts_at, status)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, name, venue, starts_at AS "startsAt", sale_starts_at AS "saleStartsAt", status`,
      [input.name, input.venue, input.startsAt, input.saleStartsAt, input.status],
    );
    const event = rows[0];
    if (!event) throw new Error('event insert returned no row');

    const sections: string[] = [];
    const rowLabels: string[] = [];
    const numbers: number[] = [];
    const prices: number[] = [];
    for (const section of input.sections) {
      for (const row of section.rows) {
        for (let number = 1; number <= section.seatsPerRow; number++) {
          sections.push(section.name);
          rowLabels.push(row);
          numbers.push(number);
          prices.push(section.priceMinor);
        }
      }
    }
    // One INSERT with parallel arrays instead of one round trip per seat.
    await client.query(
      `INSERT INTO booking.seats (event_id, section, row_label, seat_number, price_minor)
       SELECT $1, * FROM unnest($2::text[], $3::text[], $4::int[], $5::int[])`,
      [event.id, sections, rowLabels, numbers, prices],
    );
    return { ...event, totalSeats: sections.length };
  });
}
