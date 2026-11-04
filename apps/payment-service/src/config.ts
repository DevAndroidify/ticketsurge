import {
  brokerListSchema,
  durationMsSchema,
  kafkaTopicPrefixSchema,
  logLevelSchema,
  nodeEnvSchema,
  postgresUrlSchema,
} from '@ticketsurge/config';
import { z } from 'zod';

export const paymentConfigSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  // Prometheus scrapes GET /metrics here; 0 disables it (e.g. a second instance on one host).
  METRICS_PORT: z.coerce.number().int().min(0).max(65535).default(9103),
  DATABASE_URL: postgresUrlSchema,
  KAFKA_BROKERS: brokerListSchema,
  KAFKA_CLIENT_ID: z.string().min(1).default('payment-service'),
  KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema,
  // Simulated provider: how long a charge takes, and the share of charges that fail (0 to 1).
  PAYMENT_DELAY_MS: z.coerce.number().int().min(0).default(1000),
  PAYMENT_FAILURE_RATE: z.coerce.number().min(0).max(1).default(0.1),
  CONSUMER_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(6),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().min(1).max(5000).default(100),
  OUTBOX_POLL_INTERVAL_MS: durationMsSchema.default(200),
});

export type PaymentConfig = z.infer<typeof paymentConfigSchema>;
