import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { runAdmissionTick } from '../../admission-worker/src/admission.js';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import {
  bearer,
  createTestApp,
  resetDatabase,
  resetRedis,
  tokenFor,
  type TestApp,
  type TestUser,
} from './support/integration.js';

let t: TestApp;
let seatIds: string[];

const tick = (batchSize: number) =>
  runAdmissionTick(t.clients.redis, {
    batchSize,
    admissionTtlMs: 300_000,
    // Tiny interval so consecutive ticks in a test are not blocked by the per-event lock.
    intervalMs: 1,
    instanceId: 'test-worker',
  });

beforeAll(async () => {
  t = await createTestApp();
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

async function join(user: TestUser, eventId = DEMO_EVENT_ID) {
  return t.app.inject({ method: 'POST', url: `/events/${eventId}/queue`, headers: bearer(user) });
}

async function me(user: TestUser) {
  const response = await t.app.inject({
    url: `/events/${DEMO_EVENT_ID}/queue/me`,
    headers: bearer(user),
  });
  return { statusCode: response.statusCode, body: response.json<Record<string, unknown>>() };
}

async function hold(user: TestUser, seatId: string) {
  return t.app.inject({
    method: 'POST',
    url: `/events/${DEMO_EVENT_ID}/holds`,
    headers: bearer(user),
    payload: { seatIds: [seatId] },
  });
}

describe('virtual queue', () => {
  it('admits users in the order they joined', async () => {
    const users = await Promise.all(Array.from({ length: 8 }, () => tokenFor(t)));
    for (const [index, user] of users.entries()) {
      const response = await join(user);
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        eventId: DEMO_EVENT_ID,
        admitted: false,
        position: index + 1,
      });
    }

    // First round admits the first three, in order.
    const first = await tick(3);
    expect(first.get(DEMO_EVENT_ID)).toEqual(users.slice(0, 3).map((u) => u.id));
    for (const user of users.slice(0, 3)) {
      expect((await me(user)).body).toMatchObject({ admitted: true, position: 0 });
    }
    // Everyone behind moved up by three.
    for (const [index, user] of users.slice(3).entries()) {
      expect((await me(user)).body).toEqual({
        eventId: DEMO_EVENT_ID,
        admitted: false,
        position: index + 1,
      });
    }

    const second = await tick(3);
    expect(second.get(DEMO_EVENT_ID)).toEqual(users.slice(3, 6).map((u) => u.id));
    const third = await tick(3);
    expect(third.get(DEMO_EVENT_ID)).toEqual(users.slice(6).map((u) => u.id));
  });

  it('keeps a user’s place when they join again', async () => {
    const [a, b] = await Promise.all([tokenFor(t), tokenFor(t)]);
    await join(a);
    await join(b);
    expect((await join(a)).json()).toMatchObject({ position: 1 });
    expect((await join(b)).json()).toMatchObject({ position: 2 });
  });

  it('gives 300 concurrent joiners unique, gap-free positions', async () => {
    const users = await Promise.all(Array.from({ length: 300 }, () => tokenFor(t)));
    const responses = await Promise.all(users.map((user) => join(user)));
    const positions = responses
      .map((r) => r.json<{ position: number }>().position)
      .sort((x, y) => x - y);
    expect(positions).toEqual(Array.from({ length: 300 }, (_, i) => i + 1));
  });

  it('returns 404 from /queue/me for a user who never joined', async () => {
    expect((await me(await tokenFor(t))).statusCode).toBe(404);
  });

  it('returns 404 when joining the queue of an unknown event', async () => {
    expect((await join(await tokenFor(t), randomUUID())).statusCode).toBe(404);
  });
});

describe('holds require admission', () => {
  it('returns 403 before admission and 201 after', async () => {
    const user = await tokenFor(t);
    await join(user);

    const before = await hold(user, seatIds[0] ?? '');
    expect(before.statusCode).toBe(403);
    expect(before.json()).toMatchObject({ error: { code: 'NOT_ADMITTED' } });

    await tick(1);
    const after = await hold(user, seatIds[0] ?? '');
    expect(after.statusCode, after.body).toBe(201);
  });

  it('returns 403 for a user who never joined the queue', async () => {
    expect((await hold(await tokenFor(t), seatIds[0] ?? '')).statusCode).toBe(403);
  });
});

describe('admission worker', () => {
  it('admits only one batch per interval when two workers run at once', async () => {
    const users = await Promise.all(Array.from({ length: 10 }, () => tokenFor(t)));
    for (const user of users) await join(user);

    const settings = { batchSize: 2, admissionTtlMs: 300_000, intervalMs: 5_000 };
    const [a, b] = await Promise.all([
      runAdmissionTick(t.clients.redis, { ...settings, instanceId: 'worker-a' }),
      runAdmissionTick(t.clients.redis, { ...settings, instanceId: 'worker-b' }),
    ]);
    const admitted = [...(a.get(DEMO_EVENT_ID) ?? []), ...(b.get(DEMO_EVENT_ID) ?? [])];
    expect(admitted).toEqual(users.slice(0, 2).map((u) => u.id));
  });
});

describe('rate limits', () => {
  it('returns 429 with Retry-After after the per-user limit', async () => {
    const limited = await createTestApp({
      RATE_LIMIT_QUEUE_JOIN: 3,
      RATE_LIMIT_HOLDS: 2,
      RATE_LIMIT_WINDOW_MS: 60_000,
    });
    try {
      const user = await tokenFor(limited);
      const joins = [];
      for (let i = 0; i < 4; i++) {
        joins.push(
          await limited.app.inject({
            method: 'POST',
            url: `/events/${DEMO_EVENT_ID}/queue`,
            headers: bearer(user),
          }),
        );
      }
      expect(joins.map((r) => r.statusCode)).toEqual([200, 200, 200, 429]);
      const blocked = joins[3];
      expect(blocked?.json()).toMatchObject({ error: { code: 'RATE_LIMITED' } });
      expect(Number(blocked?.headers['retry-after'])).toBeGreaterThan(0);

      // Holds have their own budget.
      const holds = [];
      for (let i = 0; i < 3; i++) {
        holds.push(
          await limited.app.inject({
            method: 'POST',
            url: `/events/${DEMO_EVENT_ID}/holds`,
            headers: bearer(user),
            payload: { seatIds: [seatIds[i]] },
          }),
        );
      }
      // 403 = not admitted (the limiter let them through); the third is rate limited.
      expect(holds.map((r) => r.statusCode)).toEqual([403, 403, 429]);

      // Another user is not affected.
      const other = await tokenFor(limited);
      const otherJoin = await limited.app.inject({
        method: 'POST',
        url: `/events/${DEMO_EVENT_ID}/queue`,
        headers: bearer(other),
      });
      expect(otherJoin.statusCode).toBe(200);
    } finally {
      await limited.close();
    }
  });
});

describe('when Redis is unavailable', () => {
  it('returns 503 for queue endpoints', async () => {
    const broken = await createTestApp({ REDIS_URL: 'redis://127.0.0.1:1/1' });
    try {
      const user = await tokenFor(broken);
      const joinResponse = await broken.app.inject({
        method: 'POST',
        url: `/events/${DEMO_EVENT_ID}/queue`,
        headers: bearer(user),
      });
      expect(joinResponse.statusCode).toBe(503);
      const meResponse = await broken.app.inject({
        url: `/events/${DEMO_EVENT_ID}/queue/me`,
        headers: bearer(user),
      });
      expect(meResponse.statusCode).toBe(503);
    } finally {
      await broken.close();
    }
  });
});
