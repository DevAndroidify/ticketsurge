import type { Logger } from '@ticketsurge/config';
import Fastify from 'fastify';
import type { HealthCheck } from './health/run-checks.js';
import { healthRoutes } from './routes/health.js';

export interface AppOptions {
  logger: Logger;
  healthChecks: Record<string, HealthCheck>;
  healthCheckTimeoutMs: number;
}

/** Builds the Fastify app without listening, so tests can use app.inject(). */
export async function buildApp(options: AppOptions) {
  const app = Fastify({ loggerInstance: options.logger });

  await app.register(healthRoutes, {
    checks: options.healthChecks,
    timeoutMs: options.healthCheckTimeoutMs,
  });

  return app;
}
