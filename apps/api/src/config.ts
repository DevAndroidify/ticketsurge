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
  // HS256 signing key. 32+ characters so it cannot be brute-forced from a captured token.
  JWT_SECRET: z.string().min(32, 'must be at least 32 characters'),
  JWT_TTL_SECONDS: z.coerce.number().int().min(60).max(86_400).default(3600),
  JWT_ISSUER: z.string().min(1).default('ticketsurge-api'),
  JWT_AUDIENCE: z.string().min(1).default('ticketsurge'),
  // Users whose email is listed here may call /admin/* routes.
  ADMIN_EMAILS: z
    .string()
    .default('')
    .transform((value) =>
      value
        .split(',')
        .map((email) => email.trim().toLowerCase())
        .filter((email) => email.length > 0),
    )
    .pipe(z.array(z.email())),
});

export type ApiConfig = z.infer<typeof apiConfigSchema>;
