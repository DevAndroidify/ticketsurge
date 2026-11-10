import type pg from 'pg';

/** Fixed ID so the seed is idempotent and docs/tests can reference the demo event. */
export const DEMO_EVENT_ID = '7c1f0b8e-5d2a-4c3e-9f4a-0d1e2b3c4a50';

export interface SeedSection {
  name: string;
  priceMinor: number;
}

/** 5 sections x 10 rows (A-J) x 10 seats = 500 seats. */
export const DEMO_SECTIONS: readonly SeedSection[] = [
  { name: 'Floor', priceMinor: 15000 },
  { name: 'Lower A', priceMinor: 9500 },
  { name: 'Lower B', priceMinor: 9500 },
  { name: 'Upper A', priceMinor: 5500 },
  { name: 'Upper B', priceMinor: 5500 },
];
export const DEMO_ROWS = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'] as const;
export const DEMO_SEATS_PER_ROW = 10;

/**
 * Creates a new event with the demo layout (500 seats), on sale from a minute ago. Used when the
 * demo event has sold out: a new event keeps every past booking, payment and ticket intact, where
 * freeing sold seats would mean rewriting history the other services have already acted on.
 */
export async function createSaleEvent(
  pool: pg.Pool,
  { name, venue }: { name: string; venue: string },
): Promise<{ eventId: string; seats: number }> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows: events } = await client.query<{ id: string }>(
      `INSERT INTO booking.events (name, venue, starts_at, sale_starts_at, status)
       VALUES ($1, $2, now() + interval '30 days', now() - interval '1 minute', 'PUBLISHED')
       RETURNING id`,
      [name, venue],
    );
    const eventId = events[0]?.id;
    if (!eventId) throw new Error('event insert returned no id');
    const { rowCount } = await client.query(
      `INSERT INTO booking.seats (event_id, section, row_label, seat_number, price_minor)
       SELECT $1, s.name, r.label, n, s.price
       FROM unnest($2::text[], $3::int[]) AS s(name, price)
       CROSS JOIN unnest($4::text[]) AS r(label)
       CROSS JOIN generate_series(1, $5::int) AS n`,
      [
        eventId,
        DEMO_SECTIONS.map((s) => s.name),
        DEMO_SECTIONS.map((s) => s.priceMinor),
        DEMO_ROWS,
        DEMO_SEATS_PER_ROW,
      ],
    );
    await client.query('COMMIT');
    return { eventId, seats: rowCount ?? 0 };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Creates the demo event and its 500 seats. Safe to run repeatedly: existing rows are kept.
 * Returns the number of seats the event has afterwards.
 */
export async function seedDemoEvent(pool: pg.Pool): Promise<{ eventId: string; seats: number }> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO booking.events (id, name, venue, starts_at, sale_starts_at, status)
       VALUES ($1, 'TicketSurge Live: Demo Night', 'Demo Arena',
               now() + interval '30 days', now() - interval '1 hour', 'PUBLISHED')
       ON CONFLICT (id) DO NOTHING`,
      [DEMO_EVENT_ID],
    );
    // Cross join sections x rows x seat numbers in one statement instead of 500 round trips.
    await client.query(
      `INSERT INTO booking.seats (event_id, section, row_label, seat_number, price_minor)
       SELECT $1, s.name, r.label, n, s.price
       FROM unnest($2::text[], $3::int[]) AS s(name, price)
       CROSS JOIN unnest($4::text[]) AS r(label)
       CROSS JOIN generate_series(1, $5::int) AS n
       ON CONFLICT ON CONSTRAINT seats_position_unique DO NOTHING`,
      [
        DEMO_EVENT_ID,
        DEMO_SECTIONS.map((s) => s.name),
        DEMO_SECTIONS.map((s) => s.priceMinor),
        DEMO_ROWS,
        DEMO_SEATS_PER_ROW,
      ],
    );
    const { rows } = await client.query<{ count: number }>(
      'SELECT count(*)::int AS count FROM booking.seats WHERE event_id = $1',
      [DEMO_EVENT_ID],
    );
    await client.query('COMMIT');
    return { eventId: DEMO_EVENT_ID, seats: rows[0]?.count ?? 0 };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
