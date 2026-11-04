import type { Redis } from 'ioredis';
import { keys } from './keys.js';
import { RedisScript } from './script.js';

const joinQueueScript = RedisScript.fromFile('joinQueue', 'join_queue.lua');
const admitNextScript = RedisScript.fromFile('admitNext', 'admit_next.lua');
const rateLimitScript = RedisScript.fromFile('rateLimit', 'rate_limit.lua');

export type QueueStatus =
  { admitted: true; admissionTtlMs: number } | { admitted: false; position: number };

/** Joins the queue once (repeat calls keep the original place). See lua/join_queue.lua. */
export async function joinQueue(
  redis: Redis,
  eventId: string,
  userId: string,
): Promise<QueueStatus> {
  const reply = (await joinQueueScript.run(
    redis,
    [keys.queue(eventId), keys.queueSeq(eventId), keys.admit(eventId, userId)],
    [userId],
  )) as [number, number];
  // Registers the event for the admission worker. Not part of the script because this global
  // key lives outside the event's cluster slot.
  await redis.sadd(keys.activeQueues, eventId);
  return reply[0] === 1
    ? { admitted: true, admissionTtlMs: reply[1] }
    : { admitted: false, position: reply[1] };
}

/** Where the user stands, or undefined if they are neither waiting nor admitted. */
export async function queueStatus(
  redis: Redis,
  eventId: string,
  userId: string,
): Promise<QueueStatus | undefined> {
  const [admitTtl, rank] = await Promise.all([
    redis.pttl(keys.admit(eventId, userId)),
    redis.zrank(keys.queue(eventId), userId),
  ]);
  if (admitTtl > 0) return { admitted: true, admissionTtlMs: admitTtl };
  if (rank !== null) return { admitted: false, position: rank + 1 };
  return undefined;
}

/** Admits up to `count` users from the front of the queue. Returns them in queue order. */
export async function admitNext(
  redis: Redis,
  eventId: string,
  count: number,
  admissionTtlMs: number,
): Promise<string[]> {
  if (count < 1) return [];
  const reply = await admitNextScript.run(
    redis,
    [keys.queue(eventId)],
    [eventId, count, admissionTtlMs],
  );
  return reply as string[];
}

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  /** Milliseconds until the window resets. */
  resetMs: number;
}

export async function rateLimit(
  redis: Redis,
  route: string,
  userId: string,
  limit: number,
  windowMs: number,
): Promise<RateLimitResult> {
  const [count, ttl] = (await rateLimitScript.run(
    redis,
    [keys.rateLimit(route, userId)],
    [limit, windowMs],
  )) as [number, number];
  return { allowed: count <= limit, remaining: Math.max(0, limit - count), resetMs: ttl };
}
