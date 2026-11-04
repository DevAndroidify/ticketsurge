import {
  brokerListSchema,
  kafkaTopicPrefixSchema,
  logLevelSchema,
  nodeEnvSchema,
  postgresUrlSchema,
} from '@ticketsurge/config';
import { z } from 'zod';

export const analyticsConfigSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  // Prometheus scrapes GET /metrics here; 0 disables it (e.g. a second instance on one host).
  METRICS_PORT: z.coerce.number().int().min(0).max(65535).default(9106),
  DATABASE_URL: postgresUrlSchema,
  KAFKA_BROKERS: brokerListSchema,
  KAFKA_CLIENT_ID: z.string().min(1).default('analytics-service'),
  KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema,
  CONSUMER_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(6),
});
