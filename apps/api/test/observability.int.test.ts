import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import {
  admit,
  bearer,
  createTestApp,
  registerUser,
  resetDatabase,
  resetRedis,
  type TestApp,
} from './support/integration.js';

let t: TestApp;

beforeAll(async () => {
  t = await createTestApp();
  await resetDatabase(t.clients.pg);
  await resetRedis(t.clients.redis);
  await seedDemoEvent(t.clients.pg);
});
afterAll(async () => {
  await t.close();
});

describe('trace IDs', () => {
  it('echoes a valid X-Request-Id and generates one otherwise', async () => {
    const given = await t.app.inject({
      url: '/events',
      headers: { 'x-request-id': 'client-trace-0001' },
    });
    expect(given.headers['x-request-id']).toBe('client-trace-0001');

    for (const bad of ['short', 'has spaces in it', 'x'.repeat(200)]) {
      const response = await t.app.inject({ url: '/events', headers: { 'x-request-id': bad } });
      expect(response.headers['x-request-id']).toMatch(/^[0-9a-f-]{36}$/);
    }
  });

  it('carries the request trace ID into the booking.requested event', async () => {
    const user = await registerUser(t);
    await admit(t, DEMO_EVENT_ID, user.id);
    const { rows: seats } = await t.clients.pg.query<{ id: string }>(
      'SELECT id FROM booking.seats WHERE event_id = $1 LIMIT 1',
      [DEMO_EVENT_ID],
    );
    const hold = await t.app.inject({
      method: 'POST',
      url: `/events/${DEMO_EVENT_ID}/holds`,
      headers: bearer(user),
      payload: { seatIds: [seats[0]?.id] },
    });
    const booking = await t.app.inject({
      method: 'POST',
      url: '/bookings',
      headers: {
        ...bearer(user),
        'idempotency-key': 'trace-test',
        'x-request-id': 'checkout-trace-42',
      },
      payload: { holdId: hold.json<{ holdId: string }>().holdId },
    });
    expect(booking.statusCode).toBe(202);
    const { rows } = await t.clients.pg.query<{ trace: string }>(
      "SELECT payload->>'traceId' AS trace FROM booking.outbox WHERE message_key = $1",
      [booking.json<{ bookingId: string }>().bookingId],
    );
    expect(rows[0]?.trace).toBe('checkout-trace-42');
  });
});

describe('GET /metrics', () => {
  it('exposes request counts and latency per route template', async () => {
    await t.app.inject(`/events/${DEMO_EVENT_ID}`);
    await t.app.inject('/events/00000000-0000-4000-8000-000000000000');
    const response = await t.app.inject('/metrics');
    expect(response.statusCode).toBe(200);
    const text = response.body;
    // Route templates, not raw URLs: one series for every event ID.
    expect(text).toMatch(
      /ticketsurge_http_requests_total\{method="GET",route="\/events\/:id",status="200",service="api"\} \d+/,
    );
    expect(text).toMatch(
      /ticketsurge_http_requests_total\{method="GET",route="\/events\/:id",status="404",service="api"\} \d+/,
    );
    expect(text).toContain('ticketsurge_http_request_duration_seconds_bucket');
    expect(text).not.toContain(DEMO_EVENT_ID);
  });
});
