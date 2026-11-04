import type { Logger } from '@ticketsurge/config';
import Fastify, { type FastifyBaseLogger } from 'fastify';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import type { ApiConfig } from './config.js';
import { registerErrorHandler } from './errors.js';
import type { HealthCheck } from './health/run-checks.js';
import { adminRoutes } from './modules/admin/routes.js';
import { registerAuth } from './modules/auth/guard.js';
import { authRoutes } from './modules/auth/routes.js';
import { TokenService } from './modules/auth/tokens.js';
import { bookingRoutes } from './modules/bookings/routes.js';
import { eventRoutes } from './modules/events/routes.js';
import { holdRoutes } from './modules/holds/routes.js';
import { healthRoutes } from './routes/health.js';

export interface AppDeps {
  logger: Logger;
  config: ApiConfig;
  db: pg.Pool;
  redis: Redis;
  healthChecks: Record<string, HealthCheck>;
}

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
  const app = Fastify({ loggerInstance, bodyLimit: 1024 * 1024 });
  registerErrorHandler(app);

  const tokens = tokenServiceFrom(config);
  const { authenticate, requireAdmin } = registerAuth(app, tokens, config.ADMIN_EMAILS);

  await app.register(healthRoutes, {
    checks: healthChecks,
    timeoutMs: config.HEALTH_CHECK_TIMEOUT_MS,
  });
  authRoutes(app, { db, tokens });
  eventRoutes(app, { db, redis });
  holdRoutes(app, { db, redis, config, authenticate });
  bookingRoutes(app, { db, redis, config, authenticate });
  adminRoutes(app, { db, requireAdmin });

  return app;
}
