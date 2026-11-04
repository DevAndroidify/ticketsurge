import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { TEST_REDIS_DB, useTestEnv } from '../../../scripts/src/testing/test-env.js';
import { admitNext, joinQueue, keys, queueStatus, rateLimit } from '../src/index.js';

const { redisUrl } = useTestEnv();
const redis = new Redis(redisUrl);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let eventId: string;

beforeEach(async () => {
  expect(redis.options.db).toBe(TEST_REDIS_DB);
  await redis.flushdb();
  eventId = randomUUID();
});
afterAll(async () => {
  await redis.quit();
});

describe('joinQueue', () => {
  it('gives increasing positions and keeps a user’s place on repeat joins', async () => {
    const [a, b, c] = [randomUUID(), randomUUID(), randomUUID()];
    expect(await joinQueue(redis, eventId, a)).toEqual({ admitted: false, position: 1 });
    expect(await joinQueue(redis, eventId, b)).toEqual({ admitted: false, position: 2 });
    expect(await joinQueue(redis, eventId, a)).toEqual({ admitted: false, position: 1 });
    expect(await joinQueue(redis, eventId, c)).toEqual({ admitted: false, position: 3 });
    expect(await redis.zcard(keys.queue(eventId))).toBe(3);
    expect(await redis.sismember(keys.activeQueues, eventId)).toBe(1);
  });

  it('assigns unique, contiguous positions to 200 concurrent joins', async () => {
    const users = Array.from({ length: 200 }, () => randomUUID());
    const results = await Promise.all(users.map((u) => joinQueue(redis, eventId, u)));
    const positions = results.map((r) => (r.admitted ? -1 : r.position)).sort((x, y) => x - y);
    expect(positions).toEqual(Array.from({ length: 200 }, (_, i) => i + 1));
  });

  it('reports an admitted user as admitted instead of re-queueing them', async () => {
    const user = randomUUID();
    await redis.set(keys.admit(eventId, user), '1', 'PX', 60_000);
    const status = await joinQueue(redis, eventId, user);
    expect(status).toMatchObject({ admitted: true });
    expect(await redis.zcard(keys.queue(eventId))).toBe(0);
  });
});

describe('admitNext', () => {
  it('admits users strictly in join order, N at a time, with a TTL', async () => {
    const users = Array.from({ length: 7 }, () => randomUUID());
    for (const user of users) await joinQueue(redis, eventId, user);

    expect(await admitNext(redis, eventId, 3, 5_000)).toEqual(users.slice(0, 3));
    expect(await queueStatus(redis, eventId, users[3] ?? '')).toEqual({
      admitted: false,
      position: 1,
    });
    expect(await admitNext(redis, eventId, 3, 5_000)).toEqual(users.slice(3, 6));
    expect(await admitNext(redis, eventId, 3, 5_000)).toEqual(users.slice(6));
    expect(await admitNext(redis, eventId, 3, 5_000)).toEqual([]);

    const ttl = await redis.pttl(keys.admit(eventId, users[0] ?? ''));
    expect(ttl).toBeGreaterThan(4_000);
    expect(await queueStatus(redis, eventId, users[0] ?? '')).toMatchObject({ admitted: true });
  });

  it('lets admission expire', async () => {
    const user = randomUUID();
    await joinQueue(redis, eventId, user);
    await admitNext(redis, eventId, 1, 100);
    await sleep(200);
    expect(await queueStatus(redis, eventId, user)).toBeUndefined();
  });
});

describe('rateLimit', () => {
  it('allows `limit` requests per window and then blocks until it resets', async () => {
    const user = randomUUID();
    const results = [];
    for (let i = 0; i < 4; i++) results.push(await rateLimit(redis, 'test', user, 3, 300));
    expect(results.map((r) => r.allowed)).toEqual([true, true, true, false]);
    expect(results[3]?.resetMs).toBeGreaterThan(0);
    expect(results[3]?.resetMs).toBeLessThanOrEqual(300);

    await sleep(350);
    expect((await rateLimit(redis, 'test', user, 3, 300)).allowed).toBe(true);
  });

  it('counts each user and route separately', async () => {
    const [a, b] = [randomUUID(), randomUUID()];
    await rateLimit(redis, 'holds', a, 1, 10_000);
    expect((await rateLimit(redis, 'holds', a, 1, 10_000)).allowed).toBe(false);
    expect((await rateLimit(redis, 'holds', b, 1, 10_000)).allowed).toBe(true);
    expect((await rateLimit(redis, 'queue', a, 1, 10_000)).allowed).toBe(true);
  });
});
