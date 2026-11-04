import type { Logger } from '@ticketsurge/config';
import Fastify, { type FastifyBaseLogger } from 'fastify';
import type pg from 'pg';
import type { ApiConfig } from './config.js';
import { registerErrorHandler } from './errors.js';
import type { HealthCheck } from './health/run-checks.js';
import { adminRoutes } from './modules/admin/routes.js';
import { eventRoutes } from './modules/events/routes.js';
import { healthRoutes } from './routes/health.js';

export interface AppDeps {
  logger: Logger;
  config: ApiConfig;
  db: pg.Pool;
  healthChecks: Record<string, HealthCheck>;
}

/** Builds the Fastify app without listening, so tests can use app.inject(). */
export async function buildApp({ logger, config, db, healthChecks }: AppDeps) {
  // Typed as Fastify's base logger so route modules can take a plain FastifyInstance.
  const loggerInstance: FastifyBaseLogger = logger;
  const app = Fastify({ loggerInstance, bodyLimit: 1024 * 1024 });
  registerErrorHandler(app);

  await app.register(healthRoutes, {
    checks: healthChecks,
    timeoutMs: config.HEALTH_CHECK_TIMEOUT_MS,
  });
  eventRoutes(app, { db });
  adminRoutes(app, { db, adminApiKey: config.ADMIN_API_KEY });

  return app;
}
