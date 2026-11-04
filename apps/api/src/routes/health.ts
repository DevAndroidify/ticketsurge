import type { FastifyPluginCallback } from 'fastify';
import { runHealthChecks, type HealthCheck } from '../health/run-checks.js';

export interface HealthRouteOptions {
  checks: Record<string, HealthCheck>;
  timeoutMs: number;
}

const checkResultSchema = {
  type: 'object',
  required: ['status', 'latencyMs'],
  properties: {
    status: { type: 'string', enum: ['up', 'down'] },
    latencyMs: { type: 'number' },
    error: { type: 'string' },
  },
} as const;

const healthResponseSchema = {
  type: 'object',
  required: ['status', 'checks'],
  properties: {
    status: { type: 'string', enum: ['ok', 'unavailable'] },
    checks: { type: 'object', additionalProperties: checkResultSchema },
  },
} as const;

/** GET /health: 200 when every dependency responds, 503 with per-dependency details otherwise. */
export const healthRoutes: FastifyPluginCallback<HealthRouteOptions> = (app, options, done) => {
  app.get(
    '/health',
    {
      schema: { response: { 200: healthResponseSchema, 503: healthResponseSchema } },
      // Load balancers poll this often; keep it out of the request log.
      logLevel: 'warn',
    },
    async (_request, reply) => {
      const report = await runHealthChecks(options.checks, options.timeoutMs);
      if (report.status !== 'ok') {
        app.log.warn({ checks: report.checks }, 'health check failed');
      }
      return reply.code(report.status === 'ok' ? 200 : 503).send(report);
    },
  );
  done();
};
