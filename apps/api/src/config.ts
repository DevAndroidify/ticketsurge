import { z } from 'zod';
import {
  brokerListSchema,
  durationMsSchema,
  kafkaTopicPrefixSchema,
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
  // Prepended to every topic name. Empty in normal use; tests use a unique prefix per run.
  KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema,
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

  // Redis commands that take longer than this fail, and the request gets 503.
  REDIS_COMMAND_TIMEOUT_MS: durationMsSchema.default(1000),
  HOLD_TTL_MS: durationMsSchema.default(600_000),
  MAX_SEATS_PER_HOLD: z.coerce.number().int().min(1).max(50).default(8),
  // A booking must be paid within this window after creation.
  PAYMENT_DEADLINE_MS: durationMsSchema.default(300_000),
  // POST /bookings resets the hold to expire this long after the payment deadline.
  HOLD_AFTER_DEADLINE_MS: durationMsSchema.default(60_000),

  // Per-user limits per RATE_LIMIT_WINDOW_MS.
  RATE_LIMIT_WINDOW_MS: durationMsSchema.default(10_000),
  RATE_LIMIT_QUEUE_JOIN: z.coerce.number().int().min(1).default(5),
  RATE_LIMIT_HOLDS: z.coerce.number().int().min(1).default(10),
  RATE_LIMIT_BOOKINGS: z.coerce.number().int().min(1).default(10),
});

export type ApiConfig = z.infer<typeof apiConfigSchema>;
