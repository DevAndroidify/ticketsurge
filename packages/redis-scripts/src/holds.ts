import type { Redis } from 'ioredis';
import { keys, parseSeatValue, type SeatState } from './keys.js';
import { RedisScript } from './script.js';

const holdSeatsScript = RedisScript.fromFile('holdSeats', 'hold_seats.lua');
const extendHoldScript = RedisScript.fromFile('extendHold', 'extend_hold.lua');
const releaseHoldScript = RedisScript.fromFile('releaseHold', 'release_hold.lua');

export interface HoldSeatsInput {
  eventId: string;
  holdId: string;
  userId: string;
  seatIds: readonly string[];
  ttlMs: number;
}

export type HoldSeatsResult =
  { status: 'held' } | { status: 'not_admitted' } | { status: 'conflict'; seatIds: string[] };

/** Holds all seats or none. See lua/hold_seats.lua. */
export async function holdSeats(redis: Redis, input: HoldSeatsInput): Promise<HoldSeatsResult> {
  if (input.seatIds.length === 0) throw new Error('holdSeats needs at least one seat');
  if (input.seatIds.some((id) => id.includes(','))) throw new Error('seat IDs cannot contain ","');
  const reply = await holdSeatsScript.run(
    redis,
    [
      keys.admit(input.eventId, input.userId),
      keys.hold(input.eventId, input.holdId),
      ...input.seatIds.map((seatId) => keys.seat(input.eventId, seatId)),
    ],
    [input.holdId, input.userId, input.ttlMs, ...input.seatIds],
  );
  const [status, ...rest] = reply as string[];
  if (status === 'OK') return { status: 'held' };
  if (status === 'NOT_ADMITTED') return { status: 'not_admitted' };
  if (status === 'CONFLICT') return { status: 'conflict', seatIds: rest };
  throw new Error(`unexpected holdSeats reply: ${JSON.stringify(reply)}`);
}

export interface HoldInfo {
  owner: string;
  seatIds: string[];
  /** Remaining lifetime in milliseconds. */
  ttlMs: number;
}

export async function getHold(
  redis: Redis,
  eventId: string,
  holdId: string,
): Promise<HoldInfo | undefined> {
  const key = keys.hold(eventId, holdId);
  const [fields, ttl] = await Promise.all([redis.hmget(key, 'owner', 'seats'), redis.pttl(key)]);
  const [owner, seats] = fields;
  if (!owner || !seats || ttl < 0) return undefined;
  return { owner, seatIds: seats.split(','), ttlMs: ttl };
}

export interface ExtendHoldInput {
  eventId: string;
  holdId: string;
  userId: string;
  ttlMs: number;
}

/**
 * Resets the hold's TTL if the user still owns every seat. Returns false if the hold is gone.
 * The seat list is read first and re-verified inside the script, so a stale read cannot extend
 * the wrong seats.
 */
export async function extendHold(redis: Redis, input: ExtendHoldInput): Promise<boolean> {
  const seats = await redis.hget(keys.hold(input.eventId, input.holdId), 'seats');
  if (!seats) return false;
  const seatIds = seats.split(',');
  const reply = await extendHoldScript.run(
    redis,
    [
      keys.hold(input.eventId, input.holdId),
      ...seatIds.map((seatId) => keys.seat(input.eventId, seatId)),
    ],
    [input.holdId, input.userId, input.ttlMs, seats],
  );
  return reply === 1;
}

export interface ReleaseHoldInput {
  eventId: string;
  holdId: string;
  seatIds: readonly string[];
  /** Enforce ownership for user-initiated releases; omit for system releases. */
  owner?: string;
}

/** Frees seats still held by this hold. Returns the count released, or 'not_owner'. */
export async function releaseHold(
  redis: Redis,
  input: ReleaseHoldInput,
): Promise<number | 'not_owner'> {
  const reply = await releaseHoldScript.run(
    redis,
    [
      keys.hold(input.eventId, input.holdId),
      ...input.seatIds.map((seatId) => keys.seat(input.eventId, seatId)),
    ],
    [input.holdId, input.owner ?? ''],
  );
  if (reply === -1) return 'not_owner';
  return Number(reply);
}

/** Records which event a hold belongs to. Call before holdSeats with the same (or longer) TTL. */
export async function setHoldRef(redis: Redis, holdId: string, eventId: string, ttlMs: number) {
  await redis.set(keys.holdRef(holdId), eventId, 'PX', ttlMs);
}

export async function getHoldRef(redis: Redis, holdId: string): Promise<string | undefined> {
  return (await redis.get(keys.holdRef(holdId))) ?? undefined;
}

export async function extendHoldRef(redis: Redis, holdId: string, ttlMs: number) {
  await redis.pexpire(keys.holdRef(holdId), ttlMs);
}

export async function deleteHoldRef(redis: Redis, holdId: string) {
  await redis.del(keys.holdRef(holdId));
}

const MGET_CHUNK = 1000;

/** Current Redis state of each seat that is held or sold. Free seats are absent from the map. */
export async function getSeatStates(
  redis: Redis,
  eventId: string,
  seatIds: readonly string[],
): Promise<Map<string, SeatState>> {
  const states = new Map<string, SeatState>();
  for (let start = 0; start < seatIds.length; start += MGET_CHUNK) {
    const chunk = seatIds.slice(start, start + MGET_CHUNK);
    const values = await redis.mget(...chunk.map((seatId) => keys.seat(eventId, seatId)));
    chunk.forEach((seatId, index) => {
      const state = parseSeatValue(values[index] ?? null);
      if (state) states.set(seatId, state);
    });
  }
  return states;
}
