import { z } from 'zod';
import {
  brokerListSchema,
  durationMsSchema,
  logLevelSchema,
  nodeEnvSchema,
  portSchema,
  postgresUrlSchema,
  redisUrlSchema,
} from '@ticketsurge/config';

export const apiConfigSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  API_HOST: z.string().default('0.0.0.0'),
  API_PORT: portSchema.default(3000),
  DATABASE_URL: postgresUrlSchema,
  REDIS_URL: redisUrlSchema,
  KAFKA_BROKERS: brokerListSchema,
  KAFKA_CLIENT_ID: z.string().min(1).default('ticketsurge-api'),
  HEALTH_CHECK_TIMEOUT_MS: durationMsSchema.default(2000),
  // Shared secret for /admin/* routes, sent as the x-admin-key header.
  ADMIN_API_KEY: z.string().min(24, 'must be at least 24 characters'),
});

export type ApiConfig = z.infer<typeof apiConfigSchema>;
