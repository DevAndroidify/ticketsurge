import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { TEST_REDIS_DB, useTestEnv } from '../../../scripts/src/testing/test-env.js';
import { holdSeats, keys, markSold, publishSeatsUpdated, releaseHold } from '../src/index.js';

const { redisUrl } = useTestEnv();
const redis = new Redis(redisUrl);

let eventId: string;
let seats: string[];

beforeEach(async () => {
  expect(redis.options.db).toBe(TEST_REDIS_DB);
  await redis.flushdb();
  eventId = randomUUID();
  seats = [randomUUID(), randomUUID()];
});
afterAll(async () => {
  await redis.quit();
});

async function holdAll(holdId: string) {
  const userId = randomUUID();
  await redis.set(keys.admit(eventId, userId), '1', 'PX', 60_000);
  await holdSeats(redis, { eventId, holdId, userId, seatIds: seats, ttlMs: 60_000 });
}

describe('markSold', () => {
  it('marks seats sold without a TTL, drops the hold, and is safe to repeat', async () => {
    const holdId = randomUUID();
    const bookingId = randomUUID();
    await holdAll(holdId);

    for (let i = 0; i < 3; i++) {
      expect(await markSold(redis, { eventId, holdId, bookingId, seatIds: seats })).toBe(2);
    }
    for (const seatId of seats) {
      expect(await redis.get(keys.seat(eventId, seatId))).toBe(`sold:${bookingId}`);
      expect(await redis.pttl(keys.seat(eventId, seatId))).toBe(-1);
    }
    expect(await redis.exists(keys.hold(eventId, holdId))).toBe(0);
  });

  it('wins over a stale hold, and a later release of that hold cannot free a sold seat', async () => {
    // After a Redis restart another user may hold a seat that PostgreSQL already confirmed.
    const staleHold = randomUUID();
    await holdAll(staleHold);
    const bookingId = randomUUID();
    await markSold(redis, { eventId, holdId: randomUUID(), bookingId, seatIds: seats });
    expect(await releaseHold(redis, { eventId, holdId: staleHold, seatIds: seats })).toBe(0);
    expect(await redis.get(keys.seat(eventId, seats[0] ?? ''))).toBe(`sold:${bookingId}`);
  });
});

describe('publishSeatsUpdated', () => {
  it('delivers a seats.updated message on the event channel', async () => {
    const subscriber = new Redis(redisUrl);
    try {
      await subscriber.subscribe(keys.seatsChannel(eventId));
      const received = new Promise<string>((resolve) => {
        subscriber.once('message', (_channel: string, message: string) => {
          resolve(message);
        });
      });
      await publishSeatsUpdated(redis, eventId, seats, 'sold');
      expect(JSON.parse(await received)).toEqual({
        type: 'seats.updated',
        eventId,
        seatIds: seats,
        status: 'sold',
      });
    } finally {
      await subscriber.quit();
    }
  });
});
