import {
  brokerListSchema,
  durationMsSchema,
  kafkaTopicPrefixSchema,
  logLevelSchema,
  nodeEnvSchema,
  postgresUrlSchema,
} from '@ticketsurge/config';
import { z } from 'zod';

export const bookingWorkerConfigSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  DATABASE_URL: postgresUrlSchema,
  KAFKA_BROKERS: brokerListSchema,
  KAFKA_CLIENT_ID: z.string().min(1).default('booking-worker'),
  KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema,
  OUTBOX_BATCH_SIZE: z.coerce.number().int().min(1).max(5000).default(100),
  OUTBOX_POLL_INTERVAL_MS: durationMsSchema.default(200),
});

export type BookingWorkerConfig = z.infer<typeof bookingWorkerConfigSchema>;
