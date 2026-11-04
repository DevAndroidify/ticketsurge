import { keys } from '@ticketsurge/redis-scripts';
import { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import { DEMO_EVENT_ID, seedDemoEvent } from '../../../scripts/src/lib/seed.js';
import { testLogger } from '../../../scripts/src/testing/kafka.js';
import {
  admit,
  bearer,
  createTestApp,
  registerUser,
  resetDatabase,
  resetRedis,
  type TestApp,
  type TestUser,
} from '../../api/test/support/integration.js';
import {
  startPipeline,
  waitForStatus,
  type Pipeline,
} from '../../booking-worker/test/support/pipeline.js';
import { buildGateway } from '../src/gateway.js';

let nextOutcome: 'succeed' | 'fail' = 'succeed';
let t: TestApp;
let pipeline: Pipeline;
let subscriber: Redis;
let gateway: Awaited<ReturnType<typeof buildGateway>>;
let wsUrl: string;
let seatIds: string[];
const sockets: WebSocket[] = [];

beforeAll(async () => {
  pipeline = await startPipeline({
    roles: ['relay', 'saga', 'seat-sync'],
    payment: { failureRate: 0.5, random: () => (nextOutcome === 'fail' ? 0 : 1) },
  });
  t = await createTestApp({ KAFKA_TOPIC_PREFIX: pipeline.prefix });
  subscriber = new Redis(t.config.REDIS_URL);
  gateway = await buildGateway({
    logger: testLogger,
    subscriber,
    config: { ...t.config, WS_MAX_SUBSCRIPTIONS: 5, WS_PING_INTERVAL_MS: 30_000 },
  });
  const address = await gateway.app.listen({ host: '127.0.0.1', port: 0 });
  wsUrl = address.replace('http', 'ws');
}, 60_000);

afterAll(async () => {
  for (const socket of sockets) socket.terminate();
  await gateway.app.close();
  await subscriber.quit();
  await t.close();
  await pipeline.stop();
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
  nextOutcome = 'succeed';
});

interface LiveClient {
  messages: Record<string, unknown>[];
  next(
    match: (message: Record<string, unknown>) => boolean,
    timeoutMs?: number,
  ): Promise<Record<string, unknown>>;
  send(message: object): void;
}

/** Connects, subscribes to the demo event, and records every message. */
async function connect(user: TestUser): Promise<LiveClient> {
  const socket = new WebSocket(`${wsUrl}/ws?token=${user.token}`);
  sockets.push(socket);
  const messages: Record<string, unknown>[] = [];
  const waiters: {
    match: (m: Record<string, unknown>) => boolean;
    resolve: (m: Record<string, unknown>) => void;
  }[] = [];
  socket.on('message', (raw: Buffer) => {
    const message = JSON.parse(raw.toString()) as Record<string, unknown>;
    messages.push(message);
    for (const waiter of [...waiters]) {
      if (waiter.match(message)) {
        waiters.splice(waiters.indexOf(waiter), 1);
        waiter.resolve(message);
      }
    }
  });
  await new Promise<void>((resolve, reject) => {
    socket.once('open', () => {
      resolve();
    });
    socket.once('error', reject);
  });
  const client: LiveClient = {
    messages,
    send: (message) => {
      socket.send(JSON.stringify(message));
    },
    next: (match, timeoutMs = 20_000) => {
      const seen = messages.find(match);
      if (seen) {
        messages.splice(messages.indexOf(seen), 1);
        return Promise.resolve(seen);
      }
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(
            new Error(`no matching message within ${timeoutMs}ms; got ${JSON.stringify(messages)}`),
          );
        }, timeoutMs);
        waiters.push({
          match,
          resolve: (m) => {
            clearTimeout(timer);
            messages.splice(messages.indexOf(m), 1);
            resolve(m);
          },
        });
      });
    },
  };
  client.send({ type: 'subscribe.event', eventId: DEMO_EVENT_ID });
  await client.next((m) => m.type === 'subscribed');
  return client;
}

const seatUpdate = (seatId: string, status: string) => (m: Record<string, unknown>) =>
  m.type === 'seats.updated' &&
  m.status === status &&
  Array.isArray(m.seatIds) &&
  m.seatIds.includes(seatId);

async function hold(user: TestUser, ids: string[]) {
  await admit(t, DEMO_EVENT_ID, user.id);
  const response = await t.app.inject({
    method: 'POST',
    url: `/events/${DEMO_EVENT_ID}/holds`,
    headers: bearer(user),
    payload: { seatIds: ids },
  });
  expect(response.statusCode, response.body).toBe(201);
  return response.json<{ holdId: string }>().holdId;
}

async function book(user: TestUser, holdId: string) {
  const response = await t.app.inject({
    method: 'POST',
    url: '/bookings',
    headers: { ...bearer(user), 'idempotency-key': crypto.randomUUID() },
    payload: { holdId },
  });
  expect(response.statusCode, response.body).toBe(202);
  return response.json<{ bookingId: string }>().bookingId;
}

describe('live seat updates over WebSocket', () => {
  it('two connected clients see every seat change', async () => {
    const [alice, bob] = await Promise.all([registerUser(t), registerUser(t)]);
    const [a, b] = await Promise.all([connect(alice), connect(bob)]);
    const [first, second] = [seatIds[0] ?? '', seatIds[1] ?? ''];

    // Hold, then release.
    const holdId = await hold(alice, [first]);
    await Promise.all([a.next(seatUpdate(first, 'held')), b.next(seatUpdate(first, 'held'))]);
    const released = await t.app.inject({
      method: 'DELETE',
      url: `/holds/${holdId}`,
      headers: bearer(alice),
    });
    expect(released.statusCode).toBe(204);
    await Promise.all([
      a.next(seatUpdate(first, 'available')),
      b.next(seatUpdate(first, 'available')),
    ]);

    // Hold, book, pay: seat-sync marks the seat sold and both clients see it.
    const bookingId = await book(alice, await hold(alice, [second]));
    await Promise.all([a.next(seatUpdate(second, 'held')), b.next(seatUpdate(second, 'held'))]);
    await waitForStatus(t.clients.pg, bookingId, 'CONFIRMED');
    await Promise.all([a.next(seatUpdate(second, 'sold')), b.next(seatUpdate(second, 'sold'))]);

    // Booking status goes only to its owner.
    await a.next(
      (m) =>
        m.type === 'booking.updated' && m.bookingId === bookingId && m.status === 'PENDING_PAYMENT',
    );
    await a.next(
      (m) => m.type === 'booking.updated' && m.bookingId === bookingId && m.status === 'CONFIRMED',
    );
    expect(b.messages.some((m) => m.type === 'booking.updated')).toBe(false);

    // Redis now matches PostgreSQL: sold, no TTL, hold and lookup key gone.
    expect(await t.clients.redis.get(keys.seat(DEMO_EVENT_ID, second))).toBe(`sold:${bookingId}`);
    expect(await t.clients.redis.pttl(keys.seat(DEMO_EVENT_ID, second))).toBe(-1);
    const map = (await t.app.inject(`/events/${DEMO_EVENT_ID}/seats`)).json<{
      seats: { id: string; status: string }[];
    }>();
    expect(map.seats.find((s) => s.id === second)?.status).toBe('sold');
  }, 60_000);

  it('a failed payment releases the hold and both clients see the seat become available', async () => {
    nextOutcome = 'fail';
    const [alice, bob] = await Promise.all([registerUser(t), registerUser(t)]);
    const [a, b] = await Promise.all([connect(alice), connect(bob)]);
    const seat = seatIds[2] ?? '';

    const bookingId = await book(alice, await hold(alice, [seat]));
    await waitForStatus(t.clients.pg, bookingId, 'CANCELLED');
    await Promise.all([
      a.next(seatUpdate(seat, 'available')),
      b.next(seatUpdate(seat, 'available')),
    ]);
    await a.next((m) => m.type === 'booking.updated' && m.status === 'CANCELLED');
    expect(await t.clients.redis.exists(keys.seat(DEMO_EVENT_ID, seat))).toBe(0);

    // Bob can now take the seat.
    await expect(hold(bob, [seat])).resolves.toBeTypeOf('string');
  }, 60_000);

  it('only forwards seat updates for subscribed events', async () => {
    const carol = await registerUser(t);
    const c = await connect(carol);
    c.send({ type: 'unsubscribe.event', eventId: DEMO_EVENT_ID });
    await c.next((m) => m.type === 'unsubscribed');
    await hold(await registerUser(t), [seatIds[3] ?? '']);
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(c.messages.some((m) => m.type === 'seats.updated')).toBe(false);
  });

  it('rejects connections without a valid token', async () => {
    for (const query of ['', '?token=garbage']) {
      const socket = new WebSocket(`${wsUrl}/ws${query}`);
      // A refused upgrade makes ws emit "closed before the connection was established".
      socket.on('error', () => undefined);
      const status = await new Promise<number>((resolve) => {
        socket.once('unexpected-response', (_request, response) => {
          resolve(response.statusCode ?? 0);
        });
        socket.once('open', () => {
          resolve(101);
        });
      });
      socket.terminate();
      expect(status).toBe(401);
    }
  });

  it('rejects malformed messages and too many subscriptions', async () => {
    const dave = await connect(await registerUser(t));
    dave.send({ type: 'subscribe.event', eventId: 'not-a-uuid' });
    await dave.next((m) => m.type === 'error');
    for (let i = 0; i < 4; i++) {
      dave.send({ type: 'subscribe.event', eventId: crypto.randomUUID() });
      await dave.next((m) => m.type === 'subscribed');
    }
    dave.send({ type: 'subscribe.event', eventId: crypto.randomUUID() });
    expect(await dave.next((m) => m.type === 'error')).toMatchObject({
      message: 'too many subscriptions',
    });
  });
});
