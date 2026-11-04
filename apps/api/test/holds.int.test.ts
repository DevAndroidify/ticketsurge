import { randomUUID } from 'node:crypto';
import { keys } from '@ticketsurge/redis-scripts';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import {
  bearer,
  createTestApp,
  resetDatabase,
  resetRedis,
  tokenFor,
  type TestApp,
} from './support/integration.js';

const HOLD_TTL_MS = 1500;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let t: TestApp;
let baseUrl: string;
let seatIds: string[];

beforeAll(async () => {
  t = await createTestApp({ HOLD_TTL_MS, ADMISSION_BYPASS: true });
  // A real listening server, so the 100 requests below are truly concurrent HTTP requests.
  baseUrl = await t.app.listen({ host: '127.0.0.1', port: 0 });
});
afterAll(async () => {
  await t.close();
});
beforeEach(async () => {
  await resetDatabase(t.clients.pg);
  await resetRedis(t.clients.redis);
  await seedDemoEvent(t.clients.pg);
  const { rows } = await t.clients.pg.query<{ id: string }>(
    'SELECT id FROM booking.seats WHERE event_id = $1 ORDER BY section, row_label, seat_number',
    [DEMO_EVENT_ID],
  );
  seatIds = rows.map((row) => row.id);
});

async function seatStatus(seatId: string): Promise<string | undefined> {
  const response = await t.app.inject(`/events/${DEMO_EVENT_ID}/seats`);
  const map = response.json<{ seats: { id: string; status: string }[] }>();
  return map.seats.find((seat) => seat.id === seatId)?.status;
}

function postHold(token: string, ids: string[], eventId = DEMO_EVENT_ID) {
  return fetch(`${baseUrl}/events/${eventId}/holds`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ seatIds: ids }),
  });
}

describe('POST /events/:id/holds under contention', () => {
  it('lets exactly one of 100 parallel requests hold a seat, and frees it after the TTL', async () => {
    const seatId = seatIds[0] ?? '';
    const users = await Promise.all(Array.from({ length: 100 }, () => tokenFor(t)));

    const responses = await Promise.all(users.map((user) => postHold(user.token, [seatId])));
    const statuses = responses.map((response) => response.status);
    expect(statuses.filter((status) => status === 201)).toHaveLength(1);
    expect(statuses.filter((status) => status === 409)).toHaveLength(99);

    const winner = await responses.find((response) => response.status === 201)?.json();
    expect(winner).toMatchObject({ eventId: DEMO_EVENT_ID, seatIds: [seatId] });
    const loser = await responses.find((response) => response.status === 409)?.json();
    expect(loser).toMatchObject({
      error: { code: 'SEATS_UNAVAILABLE', details: { seatIds: [seatId] } },
    });

    expect(await seatStatus(seatId)).toBe('held');

    await sleep(HOLD_TTL_MS + 300);
    expect(await t.clients.redis.exists(keys.seat(DEMO_EVENT_ID, seatId))).toBe(0);
    expect(await seatStatus(seatId)).toBe('available');
    const again = await postHold((await tokenFor(t)).token, [seatId]);
    expect(again.status).toBe(201);
  });
});

describe('POST /events/:id/holds validation', () => {
  it('returns 201 with holdId and expiresAt for a multi-seat hold', async () => {
    const user = await tokenFor(t);
    const started = Date.now();
    const response = await postHold(user.token, seatIds.slice(0, 3));
    expect(response.status).toBe(201);
    const body = (await response.json()) as { holdId: string; expiresAt: string };
    expect(body.holdId).toMatch(/^[0-9a-f-]{36}$/);
    const expires = Date.parse(body.expiresAt);
    expect(expires).toBeGreaterThanOrEqual(started + HOLD_TTL_MS - 50);
    expect(expires).toBeLessThanOrEqual(Date.now() + HOLD_TTL_MS + 50);
  });

  it.each([
    ['an unknown seat', () => [randomUUID()], 400],
    ['duplicate seat IDs', () => [seatIds[0], seatIds[0]], 400],
    ['too many seats', () => seatIds.slice(0, 9), 400],
    ['no seats', () => [], 400],
  ])('rejects %s', async (_label, pick, status) => {
    const user = await tokenFor(t);
    const response = await postHold(user.token, pick() as string[]);
    expect(response.status).toBe(status);
  });

  it('returns 404 for an unknown event', async () => {
    const response = await postHold((await tokenFor(t)).token, [seatIds[0] ?? ''], randomUUID());
    expect(response.status).toBe(404);
  });

  it('returns 403 when the sale has not started', async () => {
    await t.clients.pg.query(
      "UPDATE booking.events SET sale_starts_at = now() + interval '1 day' WHERE id = $1",
      [DEMO_EVENT_ID],
    );
    const response = await postHold((await tokenFor(t)).token, [seatIds[0] ?? '']);
    expect(response.status).toBe(403);
    expect(await response.json()).toMatchObject({ error: { code: 'SALE_NOT_OPEN' } });
  });

  it('a conflicting multi-seat request holds nothing', async () => {
    await postHold((await tokenFor(t)).token, [seatIds[1] ?? '']);
    const response = await postHold((await tokenFor(t)).token, seatIds.slice(0, 3));
    expect(response.status).toBe(409);
    expect(await seatStatus(seatIds[0] ?? '')).toBe('available');
    expect(await seatStatus(seatIds[2] ?? '')).toBe('available');
  });
});

describe('DELETE /holds/:holdId', () => {
  it('releases the owner’s hold, and returns 404 to anyone else or on repeat', async () => {
    const owner = await tokenFor(t);
    const created = await postHold(owner.token, seatIds.slice(0, 2));
    const { holdId } = (await created.json()) as { holdId: string };

    const stranger = await tokenFor(t);
    const asStranger = await t.app.inject({
      method: 'DELETE',
      url: `/holds/${holdId}`,
      headers: bearer(stranger),
    });
    expect(asStranger.statusCode).toBe(404);
    expect(await seatStatus(seatIds[0] ?? '')).toBe('held');

    const asOwner = await t.app.inject({
      method: 'DELETE',
      url: `/holds/${holdId}`,
      headers: bearer(owner),
    });
    expect(asOwner.statusCode).toBe(204);
    expect(await seatStatus(seatIds[0] ?? '')).toBe('available');
    expect(await seatStatus(seatIds[1] ?? '')).toBe('available');

    const repeat = await t.app.inject({
      method: 'DELETE',
      url: `/holds/${holdId}`,
      headers: bearer(owner),
    });
    expect(repeat.statusCode).toBe(404);
  });
});

describe('admission', () => {
  it('returns 403 NOT_ADMITTED when the bypass flag is off', async () => {
    const strict = await createTestApp({ ADMISSION_BYPASS: false });
    try {
      const user = await tokenFor(strict);
      const response = await strict.app.inject({
        method: 'POST',
        url: `/events/${DEMO_EVENT_ID}/holds`,
        headers: bearer(user),
        payload: { seatIds: [seatIds[0]] },
      });
      expect(response.statusCode).toBe(403);
      expect(response.json()).toMatchObject({ error: { code: 'NOT_ADMITTED' } });
    } finally {
      await strict.close();
    }
  });
});

describe('when Redis is unavailable', () => {
  it('returns 503 for holds and still serves a degraded seat map', async () => {
    const broken = await createTestApp({
      REDIS_URL: 'redis://127.0.0.1:1/1',
      ADMISSION_BYPASS: true,
    });
    try {
      const user = await tokenFor(broken);
      const hold = await broken.app.inject({
        method: 'POST',
        url: `/events/${DEMO_EVENT_ID}/holds`,
        headers: bearer(user),
        payload: { seatIds: [seatIds[0]] },
      });
      expect(hold.statusCode).toBe(503);
      expect(hold.json()).toMatchObject({ error: { code: 'REDIS_UNAVAILABLE' } });

      const release = await broken.app.inject({
        method: 'DELETE',
        url: `/holds/${randomUUID()}`,
        headers: bearer(user),
      });
      expect(release.statusCode).toBe(503);

      const map = await broken.app.inject(`/events/${DEMO_EVENT_ID}/seats`);
      expect(map.statusCode).toBe(200);
      expect(map.json()).toMatchObject({ degraded: true });
    } finally {
      await broken.close();
    }
  });
});
