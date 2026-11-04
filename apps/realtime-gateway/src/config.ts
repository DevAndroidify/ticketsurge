import {
  durationMsSchema,
  logLevelSchema,
  nodeEnvSchema,
  portSchema,
  redisUrlSchema,
} from '@ticketsurge/config';
import { z } from 'zod';

export const gatewayConfigSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  REALTIME_HOST: z.string().default('0.0.0.0'),
  REALTIME_PORT: portSchema.default(3001),
  REDIS_URL: redisUrlSchema,
  // Must match the API so the gateway accepts the same access tokens.
  JWT_SECRET: z.string().min(32, 'must be at least 32 characters'),
  JWT_ISSUER: z.string().min(1).default('ticketsurge-api'),
  JWT_AUDIENCE: z.string().min(1).default('ticketsurge'),
  WS_MAX_SUBSCRIPTIONS: z.coerce.number().int().min(1).max(1000).default(20),
  // Dead connections (no pong) are closed after one missed ping.
  WS_PING_INTERVAL_MS: durationMsSchema.default(30_000),
});

export type GatewayConfig = z.infer<typeof gatewayConfigSchema>;
