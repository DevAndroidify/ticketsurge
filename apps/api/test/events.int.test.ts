import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  DEMO_EVENT_ID,
  DEMO_ROWS,
  DEMO_SEATS_PER_ROW,
  DEMO_SECTIONS,
  seedDemoEvent,
} from '../../../scripts/src/lib/seed.js';
import {
  TEST_ADMIN_EMAIL,
  bearer,
  createTestApp,
  registerUser,
  resetDatabase,
  type TestApp,
  type TestUser,
} from './support/integration.js';

interface SeatMap {
  eventId: string;
  total: number;
  counts: { available: number; held: number; sold: number };
  seats: {
    id: string;
    section: string;
    row: string;
    number: number;
    priceMinor: number;
    status: string;
  }[];
}

let t: TestApp;

beforeAll(async () => {
  t = await createTestApp();
});
afterAll(async () => {
  await t.close();
});
beforeEach(async () => {
  await resetDatabase(t.clients.pg);
});

describe('seeded 500-seat event', () => {
  beforeEach(async () => {
    await seedDemoEvent(t.clients.pg);
  });

  it('GET /events/:id/seats returns the correct seat map', async () => {
    const response = await t.app.inject(`/events/${DEMO_EVENT_ID}/seats`);
    expect(response.statusCode).toBe(200);
    const map = response.json<SeatMap>();

    expect(map.eventId).toBe(DEMO_EVENT_ID);
    expect(map.total).toBe(500);
    expect(map.seats).toHaveLength(500);
    expect(map.counts).toEqual({ available: 500, held: 0, sold: 0 });

    // Every position appears exactly once, with its section's price and a unique seat ID.
    expect(new Set(map.seats.map((s) => s.id)).size).toBe(500);
    const positions = new Set(map.seats.map((s) => `${s.section}|${s.row}|${s.number}`));
    expect(positions.size).toBe(500);
    for (const section of DEMO_SECTIONS) {
      const seats = map.seats.filter((s) => s.section === section.name);
      expect(seats).toHaveLength(DEMO_ROWS.length * DEMO_SEATS_PER_ROW);
      expect(seats.every((s) => s.priceMinor === section.priceMinor)).toBe(true);
      for (const row of DEMO_ROWS) {
        const numbers = seats.filter((s) => s.row === row).map((s) => s.number);
        expect(numbers).toEqual(Array.from({ length: DEMO_SEATS_PER_ROW }, (_, i) => i + 1));
      }
    }
    expect(map.seats.every((s) => s.status === 'available')).toBe(true);
  });

  it('GET /events lists the event with its seat count', async () => {
    const response = await t.app.inject('/events');
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      events: [{ id: DEMO_EVENT_ID, totalSeats: 500, status: 'PUBLISHED' }],
    });
  });

  it('GET /events/:id returns the event', async () => {
    const response = await t.app.inject(`/events/${DEMO_EVENT_ID}`);
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      id: DEMO_EVENT_ID,
      name: 'TicketSurge Live: Demo Night',
      totalSeats: 500,
    });
  });
});

describe('event lookups', () => {
  it('returns 404 for an unknown event and 400 for a malformed ID', async () => {
    const missing = '00000000-0000-4000-8000-000000000000';
    expect((await t.app.inject(`/events/${missing}`)).statusCode).toBe(404);
    expect((await t.app.inject(`/events/${missing}/seats`)).statusCode).toBe(404);
    const malformed = await t.app.inject('/events/not-a-uuid');
    expect(malformed.statusCode).toBe(400);
    expect(malformed.json()).toMatchObject({ error: { code: 'VALIDATION_FAILED' } });
  });
});

describe('POST /admin/events', () => {
  const body = {
    name: 'Admin Created',
    venue: 'Hall 2',
    startsAt: '2030-01-01T20:00:00Z',
    saleStartsAt: '2029-12-01T10:00:00Z',
    sections: [
      { name: 'Stalls', priceMinor: 4500, rows: ['A', 'B', 'C'], seatsPerRow: 20 },
      { name: 'Balcony', priceMinor: 2500, rows: ['A', 'B'], seatsPerRow: 15 },
    ],
  };

  let admin: TestUser;
  beforeEach(async () => {
    admin = await registerUser(t, TEST_ADMIN_EMAIL);
  });

  it('creates the event and its seats for an admin', async () => {
    const response = await t.app.inject({
      method: 'POST',
      url: '/admin/events',
      headers: bearer(admin),
      payload: body,
    });
    expect(response.statusCode, response.body).toBe(201);
    const event = response.json<{ id: string; totalSeats: number }>();
    expect(event.totalSeats).toBe(90);

    const map = (await t.app.inject(`/events/${event.id}/seats`)).json<SeatMap>();
    expect(map.total).toBe(90);
    expect(map.seats.filter((s) => s.section === 'Balcony')).toHaveLength(30);
  });

  it('creates nothing when the caller is not an admin', async () => {
    const user = await registerUser(t);
    for (const headers of [{}, bearer(user)]) {
      const response = await t.app.inject({
        method: 'POST',
        url: '/admin/events',
        headers,
        payload: body,
      });
      expect([401, 403]).toContain(response.statusCode);
    }
    const { rows } = await t.clients.pg.query('SELECT count(*)::int AS n FROM booking.events');
    expect(rows[0]).toEqual({ n: 0 });
  });

  it('rejects an invalid layout with 400', async () => {
    const response = await t.app.inject({
      method: 'POST',
      url: '/admin/events',
      headers: bearer(admin),
      payload: { ...body, sections: [{ ...body.sections[0], rows: ['A', 'A'] }] },
    });
    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ error: { code: 'VALIDATION_FAILED' } });
  });
});
