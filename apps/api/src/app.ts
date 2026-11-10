import { randomUUID } from 'node:crypto';
import type { Logger } from '@ticketsurge/config';
import { createRegistry, httpMetrics } from '@ticketsurge/metrics';
import Fastify, { LogController, type FastifyBaseLogger } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import type { ApiConfig } from './config.js';
import { registerCors } from './cors.js';
import { registerErrorHandler } from './errors.js';
import type { HealthCheck } from './health/run-checks.js';
import { adminRoutes } from './modules/admin/routes.js';
import { registerAuth } from './modules/auth/guard.js';
import { authRoutes } from './modules/auth/routes.js';
import { TokenService } from './modules/auth/tokens.js';
import { bookingRoutes } from './modules/bookings/routes.js';
import { eventRoutes } from './modules/events/routes.js';
import { holdRoutes } from './modules/holds/routes.js';
import { queueRoutes } from './modules/queue/routes.js';
import { createRateLimiter } from './rate-limit.js';
import { healthRoutes } from './routes/health.js';

export interface AppDeps {
  logger: Logger;
  config: ApiConfig;
  db: pg.Pool;
  redis: Redis;
  healthChecks: Record<string, HealthCheck>;
}

/** Accepted incoming trace IDs: safe to log and to carry in event envelopes. */
const TRACE_ID = /^[A-Za-z0-9._:-]{8,128}$/;

export function tokenServiceFrom(config: ApiConfig): TokenService {
  return new TokenService({
    secret: config.JWT_SECRET,
    issuer: config.JWT_ISSUER,
    audience: config.JWT_AUDIENCE,
    ttlSeconds: config.JWT_TTL_SECONDS,
  });
}

/** Builds the Fastify app without listening, so tests can use app.inject(). */
export async function buildApp({ logger, config, db, redis, healthChecks }: AppDeps) {
  // Typed as Fastify's base logger so route modules can take a plain FastifyInstance.
  const loggerInstance: FastifyBaseLogger = logger;
  const app = Fastify({
    loggerInstance,
    bodyLimit: 1024 * 1024,
    // The request ID is the trace ID: logged as `traceId` on every request log line, returned in
    // X-Request-Id, and copied into every event this request produces, so one booking can be
    // followed from the HTTP call through every service's logs.
    requestIdHeader: false,
    logController: new LogController({ requestIdLogLabel: 'traceId' }),
    genReqId: (request) => {
      const incoming = request.headers['x-request-id'];
      return typeof incoming === 'string' && TRACE_ID.test(incoming) ? incoming : randomUUID();
    },
  });
  registerErrorHandler(app);
  registerCors(app, config.CORS_ORIGINS);

  const registry = createRegistry('api');
  const recordHttp = httpMetrics(registry);
  app.addHook('onSend', async (request, reply, payload) => {
    void reply.header('x-request-id', request.id);
    return payload;
  });
  app.addHook('onResponse', async (request, reply) => {
    // Route template ("/events/:id/holds"), never the raw URL, so label cardinality stays bounded.
    const route = request.routeOptions.url ?? 'unmatched';
    recordHttp(request.method, route, reply.statusCode, reply.elapsedTime / 1000);
  });
  app.get('/metrics', { logLevel: 'warn' }, async (_request, reply) => {
    return reply.type(registry.contentType).send(await registry.metrics());
  });

  const tokens = tokenServiceFrom(config);
  const { authenticate, requireAdmin } = registerAuth(app, tokens, config.ADMIN_EMAILS);

  await app.register(healthRoutes, {
    checks: healthChecks,
    timeoutMs: config.HEALTH_CHECK_TIMEOUT_MS,
  });
  authRoutes(app, { db, tokens });
  const limit = createRateLimiter(redis, config.RATE_LIMIT_WINDOW_MS);
  eventRoutes(app, { db, redis });
  queueRoutes(app, {
    db,
    redis,
    authenticate,
    joinRateLimit: limit('queue-join', config.RATE_LIMIT_QUEUE_JOIN),
  });
  holdRoutes(app, {
    db,
    redis,
    config,
    authenticate,
    holdRateLimit: limit('holds', config.RATE_LIMIT_HOLDS),
  });
  bookingRoutes(app, {
    db,
    redis,
    config,
    authenticate,
    bookingRateLimit: limit('bookings', config.RATE_LIMIT_BOOKINGS),
  });
  adminRoutes(app, { db, requireAdmin });

  return app;
}
