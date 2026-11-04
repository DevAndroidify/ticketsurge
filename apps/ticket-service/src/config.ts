import {
  brokerListSchema,
  durationMsSchema,
  kafkaTopicPrefixSchema,
  logLevelSchema,
  nodeEnvSchema,
  postgresUrlSchema,
} from '@ticketsurge/config';
import { z } from 'zod';

export const ticketConfigSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  DATABASE_URL: postgresUrlSchema,
  KAFKA_BROKERS: brokerListSchema,
  KAFKA_CLIENT_ID: z.string().min(1).default('ticket-service'),
  KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema,
  CONSUMER_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(6),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().min(1).max(5000).default(100),
  OUTBOX_POLL_INTERVAL_MS: durationMsSchema.default(200),
});
