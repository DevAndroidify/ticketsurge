import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { TEST_REDIS_DB, useTestEnv } from '../../../scripts/src/testing/test-env.js';
import {
  extendHold,
  getHold,
  getSeatStates,
  holdSeats,
  keys,
  releaseHold,
  type HoldSeatsInput,
} from '../src/index.js';

const { redisUrl } = useTestEnv();
const redis = new Redis(redisUrl);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const ids = (n: number) => Array.from({ length: n }, () => randomUUID());

let eventId: string;
let seats: string[];

beforeEach(async () => {
  // Never flush anything but the dedicated test DB.
  expect(redis.options.db).toBe(TEST_REDIS_DB);
  await redis.flushdb();
  eventId = randomUUID();
  seats = ids(20);
});

afterAll(async () => {
  await redis.quit();
});

/** Holds seats as an admitted user (pass `admitted: false` to skip admission). */
async function hold(overrides: Partial<HoldSeatsInput> & { admitted?: boolean } = {}) {
  const { admitted = true, ...input } = overrides;
  const userId = input.userId ?? randomUUID();
  if (admitted) await redis.set(keys.admit(eventId, userId), '1', 'PX', 60_000);
  return holdSeats(redis, {
    eventId,
    holdId: randomUUID(),
    seatIds: seats.slice(0, 2),
    ttlMs: 10_000,
    ...input,
    userId,
  });
}

describe('holdSeats', () => {
  it('holds every seat with a TTL and records owner and seats', async () => {
    const holdId = randomUUID();
    const userId = randomUUID();
    expect(await hold({ holdId, userId, seatIds: seats.slice(0, 3) })).toEqual({ status: 'held' });

    for (const seatId of seats.slice(0, 3)) {
      expect(await redis.get(keys.seat(eventId, seatId))).toBe(`hold:${holdId}`);
      const ttl = await redis.pttl(keys.seat(eventId, seatId));
      expect(ttl).toBeGreaterThan(9_000);
      expect(ttl).toBeLessThanOrEqual(10_000);
    }
    expect(await getHold(redis, eventId, holdId)).toMatchObject({
      owner: userId,
      seatIds: seats.slice(0, 3),
    });
  });

  it('changes nothing and reports only the taken seats when any seat is taken', async () => {
    await hold({ seatIds: [seats[1] ?? ''] });
    const holdId = randomUUID();
    const result = await hold({ holdId, seatIds: seats.slice(0, 3) });

    expect(result).toEqual({ status: 'conflict', seatIds: [seats[1]] });
    expect(await redis.exists(keys.seat(eventId, seats[0] ?? ''))).toBe(0);
    expect(await redis.exists(keys.seat(eventId, seats[2] ?? ''))).toBe(0);
    expect(await redis.exists(keys.hold(eventId, holdId))).toBe(0);
  });

  it('requires admission and writes nothing without it', async () => {
    const userId = randomUUID();
    expect(await hold({ userId, admitted: false })).toEqual({ status: 'not_admitted' });
    expect(await getSeatStates(redis, eventId, seats)).toEqual(new Map());
    expect(await hold({ userId })).toEqual({ status: 'held' });
  });

  it('frees the seats when the TTL passes', async () => {
    await hold({ ttlMs: 200 });
    await sleep(300);
    expect(await getSeatStates(redis, eventId, seats)).toEqual(new Map());
    expect(await hold()).toEqual({ status: 'held' });
  });

  it('never double-holds a seat under heavy concurrent overlap', async () => {
    // 300 parallel attempts, each for 2 to 4 random seats out of 20.
    const attempts = Array.from({ length: 300 }, () => {
      const count = 2 + Math.floor(Math.random() * 3);
      const picked = [...seats].sort(() => Math.random() - 0.5).slice(0, count);
      return { holdId: randomUUID(), seatIds: picked };
    });
    const results = await Promise.all(attempts.map((a) => hold(a)));

    const winners = attempts.filter((_, i) => results[i]?.status === 'held');
    expect(winners.length).toBeGreaterThan(0);

    const states = await getSeatStates(redis, eventId, seats);
    const claimed = new Set<string>();
    for (const winner of winners) {
      for (const seatId of winner.seatIds) {
        // Each winner owns every one of its seats, and no seat has two winners.
        expect(claimed.has(seatId)).toBe(false);
        claimed.add(seatId);
        expect(states.get(seatId)).toEqual({ kind: 'hold', holdId: winner.holdId });
      }
    }
    expect(states.size).toBe(claimed.size);
  });
});

describe('extendHold', () => {
  it('resets the TTL only for the owner', async () => {
    const holdId = randomUUID();
    const userId = randomUUID();
    await hold({ holdId, userId, ttlMs: 2_000 });

    expect(await extendHold(redis, { eventId, holdId, userId: randomUUID(), ttlMs: 60_000 })).toBe(
      false,
    );
    expect(await extendHold(redis, { eventId, holdId, userId, ttlMs: 60_000 })).toBe(true);
    expect(await redis.pttl(keys.seat(eventId, seats[0] ?? ''))).toBeGreaterThan(59_000);
    expect(await redis.pttl(keys.hold(eventId, holdId))).toBeGreaterThan(59_000);
  });

  it('fails when a seat no longer belongs to the hold', async () => {
    const holdId = randomUUID();
    const userId = randomUUID();
    await hold({ holdId, userId });
    await redis.set(keys.seat(eventId, seats[1] ?? ''), 'sold:someone-else');
    expect(await extendHold(redis, { eventId, holdId, userId, ttlMs: 60_000 })).toBe(false);
    // The surviving seat keeps its original TTL.
    expect(await redis.pttl(keys.seat(eventId, seats[0] ?? ''))).toBeLessThanOrEqual(10_000);
  });

  it('fails after the hold expired', async () => {
    const holdId = randomUUID();
    const userId = randomUUID();
    await hold({ holdId, userId, ttlMs: 100 });
    await sleep(200);
    expect(await extendHold(redis, { eventId, holdId, userId, ttlMs: 60_000 })).toBe(false);
  });
});

describe('releaseHold', () => {
  it('frees only seats whose value still matches the hold, and is safe to repeat', async () => {
    const holdId = randomUUID();
    const userId = randomUUID();
    await hold({ holdId, userId, seatIds: seats.slice(0, 2) });
    // Simulate seat 1 having been sold through another path.
    await redis.set(keys.seat(eventId, seats[1] ?? ''), 'sold:other');

    const input = { eventId, holdId, seatIds: seats.slice(0, 2), owner: userId };
    expect(await releaseHold(redis, input)).toBe(1);
    expect(await redis.get(keys.seat(eventId, seats[0] ?? ''))).toBeNull();
    expect(await redis.get(keys.seat(eventId, seats[1] ?? ''))).toBe('sold:other');
    expect(await redis.exists(keys.hold(eventId, holdId))).toBe(0);
    expect(await releaseHold(redis, input)).toBe(0);
  });

  it('refuses to release another user’s hold', async () => {
    const holdId = randomUUID();
    await hold({ holdId });
    const result = await releaseHold(redis, {
      eventId,
      holdId,
      seatIds: seats.slice(0, 2),
      owner: randomUUID(),
    });
    expect(result).toBe('not_owner');
    expect(await redis.get(keys.seat(eventId, seats[0] ?? ''))).toBe(`hold:${holdId}`);
  });
});
