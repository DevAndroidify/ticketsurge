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
