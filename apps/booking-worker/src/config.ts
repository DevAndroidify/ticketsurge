import {
  brokerListSchema,
  durationMsSchema,
  kafkaTopicPrefixSchema,
  logLevelSchema,
  nodeEnvSchema,
  postgresUrlSchema,
  redisUrlSchema,
} from '@ticketsurge/config';
import { z } from 'zod';
import { ROLES } from './worker.js';

export const bookingWorkerConfigSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  // Prometheus scrapes GET /metrics here; 0 disables it (e.g. a second instance on one host).
  METRICS_PORT: z.coerce.number().int().min(0).max(65535).default(9102),
  DATABASE_URL: postgresUrlSchema,
  REDIS_URL: redisUrlSchema,
  KAFKA_BROKERS: brokerListSchema,
  KAFKA_CLIENT_ID: z.string().min(1).default('booking-worker'),
  KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema,
  OUTBOX_BATCH_SIZE: z.coerce.number().int().min(1).max(5000).default(100),
  OUTBOX_POLL_INTERVAL_MS: durationMsSchema.default(200),
  CONSUMER_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(6),
  SWEEP_INTERVAL_MS: durationMsSchema.default(1000),
  SWEEP_BATCH_SIZE: z.coerce.number().int().min(1).max(5000).default(100),
  // The sweeper pauses while more booking outbox rows than this are unpublished.
  SWEEPER_MAX_OUTBOX_BACKLOG: z.coerce.number().int().min(0).default(1000),
  // Comma-separated parts to run in this process (default: all of them).
  BOOKING_WORKER_ROLES: z
    .string()
    .default(ROLES.join(','))
    .transform((value) =>
      value
        .split(',')
        .map((role) => role.trim())
        .filter(Boolean),
    )
    .pipe(z.array(z.enum(ROLES)).min(1)),
});

export type BookingWorkerConfig = z.infer<typeof bookingWorkerConfigSchema>;
