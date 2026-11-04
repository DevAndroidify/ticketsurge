/**
 * Redis key builders. Every per-event key wraps the event ID in a {hash tag}, so all keys of one
 * event live in the same Redis Cluster slot and a Lua script may touch any of them.
 */
export const keys = {
  /** Admission marker for one user, with a TTL. */
  admit: (eventId: string, userId: string) => `admit:{${eventId}}:${userId}`,
  /** Hash with the hold's owner and seat IDs; same TTL as its seats. */
  hold: (eventId: string, holdId: string) => `hold:{${eventId}}:${holdId}`,
  /** "hold:<holdId>" with a TTL, or "sold:<bookingId>" without one. */
  seat: (eventId: string, seatId: string) => `seat:{${eventId}}:${seatId}`,
  /**
   * holdId -> eventId lookup. DELETE /holds/:holdId and POST /bookings only receive a hold ID, but
   * hold keys are partitioned by event. Written before the hold and always outlives it.
   */
  holdRef: (holdId: string) => `holdref:${holdId}`,
  /** Waiting users: member = userId, score = join sequence. */
  queue: (eventId: string) => `queue:{${eventId}}`,
  /** Increasing join counter for the queue. */
  queueSeq: (eventId: string) => `queue:{${eventId}}:seq`,
  /** Set of event IDs that have a queue; the admission worker iterates it. */
  activeQueues: 'queues:active',
  /** Per-event lock so only one admission worker admits per interval. */
  admissionTick: (eventId: string) => `admission:{${eventId}}:tick`,
  /** Fixed-window rate-limit counter. */
  rateLimit: (route: string, userId: string) => `rl:${route}:${userId}`,
} as const;

export type SeatState = { kind: 'hold'; holdId: string } | { kind: 'sold'; bookingId: string };

/** Parses a seat key value. Returns undefined for a free seat or an unknown value. */
export function parseSeatValue(value: string | null): SeatState | undefined {
  if (value?.startsWith('hold:')) return { kind: 'hold', holdId: value.slice(5) };
  if (value?.startsWith('sold:')) return { kind: 'sold', bookingId: value.slice(5) };
  return undefined;
}
