import { rateLimit } from '@ticketsurge/redis-scripts';
import type { preHandlerAsyncHookHandler } from 'fastify';
import type { Redis } from 'ioredis';
import { HttpError } from './errors.js';
import { currentUser } from './modules/auth/guard.js';
import { withRedis } from './redis.js';

export type RateLimitGuard = (route: string, limit: number) => preHandlerAsyncHookHandler;

/**
 * Per-user fixed-window rate limiting in Redis (key rl:<route>:<userId>).
 * Runs after `authenticate`. Redis being down gives 503, like every other contention endpoint.
 */
export function createRateLimiter(redis: Redis, windowMs: number): RateLimitGuard {
  return (route, limit) => async (request, reply) => {
    const user = currentUser(request);
    const result = await withRedis(redis, () => rateLimit(redis, route, user.id, limit, windowMs));
    void reply.header('x-ratelimit-limit', String(limit));
    void reply.header('x-ratelimit-remaining', String(result.remaining));
    if (!result.allowed) {
      throw new HttpError(429, 'RATE_LIMITED', 'Too many requests, slow down', undefined, {
        'retry-after': String(Math.max(1, Math.ceil(result.resetMs / 1000))),
      });
    }
  };
}
