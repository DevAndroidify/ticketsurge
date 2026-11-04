import type { FastifyBaseLogger } from 'fastify';
import type { Redis } from 'ioredis';
import { HttpError, serviceUnavailable } from './errors.js';

const CONNECTION_ERROR =
  /Connection is closed|Stream isn't writeable|Command timed out|ECONNREFUSED|ECONNRESET|ETIMEDOUT|EPIPE|EHOSTUNREACH/;

export const redisUnavailable = () =>
  serviceUnavailable('REDIS_UNAVAILABLE', 'Seat holds are temporarily unavailable, retry shortly');

/**
 * Runs Redis work for a request. Holds and queue places must never be granted without Redis,
 * so a disconnected client or a connection error becomes 503 instead of a silent fallback.
 */
/**
 * Publishes a live update. Best effort: the state change already happened, so a lost notification
 * must not fail the request. Clients reload state after reconnecting anyway.
 */
export async function notify(log: FastifyBaseLogger, publish: () => Promise<void>): Promise<void> {
  try {
    await publish();
  } catch (error) {
    log.warn({ err: error }, 'live update not published');
  }
}

// A function call, so the compiler does not assume the status stays the same across awaits.
const isReady = (redis: Redis) => redis.status === 'ready';

export async function withRedis<T>(redis: Redis, fn: () => Promise<T>): Promise<T> {
  if (!isReady(redis)) throw redisUnavailable();
  try {
    return await fn();
  } catch (error) {
    if (error instanceof HttpError) throw error;
    const name = error instanceof Error ? error.name : '';
    const message = error instanceof Error ? error.message : String(error);
    if (!isReady(redis) || name === 'MaxRetriesPerRequestError' || CONNECTION_ERROR.test(message)) {
      throw redisUnavailable();
    }
    throw error;
  }
}
