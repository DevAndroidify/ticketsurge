import {
  brokerListSchema,
  ConfigError,
  createLogger,
  kafkaTopicPrefixSchema,
  loadConfig,
  loadEnvFile,
  logLevelSchema,
  nodeEnvSchema,
  postgresUrlSchema,
} from '@ticketsurge/config';
import { createKafka, createProducer } from '@ticketsurge/kafka';
import { ServiceMetrics, startMetricsServer } from '@ticketsurge/metrics';
import pg from 'pg';
import { z } from 'zod';
import { startNotificationService } from './service.js';

const configSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  // Prometheus scrapes GET /metrics here; 0 disables it (e.g. a second instance on one host).
  METRICS_PORT: z.coerce.number().int().min(0).max(65535).default(9105),
  DATABASE_URL: postgresUrlSchema,
  KAFKA_BROKERS: brokerListSchema,
  KAFKA_CLIENT_ID: z.string().min(1).default('notification-service'),
  KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema,
  CONSUMER_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(6),
});

function readConfig() {
  loadEnvFile();
  try {
    return loadConfig(configSchema);
  } catch (error) {
    if (error instanceof ConfigError) {
      console.error(error.message);
      process.exit(1);
    }
    throw error;
  }
}

const config = readConfig();
const logger = createLogger({ service: 'notification-service', level: config.LOG_LEVEL });
const metrics = new ServiceMetrics('notification-service');
await startMetricsServer(metrics.registry, config.METRICS_PORT);
const db = new pg.Pool({
  connectionString: config.DATABASE_URL,
  max: config.CONSUMER_CONCURRENCY + 2,
});
db.on('error', (error) => {
  logger.warn({ err: error }, 'postgres idle client error');
});
const kafka = createKafka({
  brokers: config.KAFKA_BROKERS,
  clientId: config.KAFKA_CLIENT_ID,
  logger,
});

const producer = await createProducer(kafka);
const consumer = await startNotificationService(
  { db, kafka, producer, logger, metrics },
  { topicPrefix: config.KAFKA_TOPIC_PREFIX, concurrency: config.CONSUMER_CONCURRENCY },
);
logger.info('notification service started');

let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    if (stopping) return;
    stopping = true;
    logger.info({ signal }, 'shutting down');
    void (async () => {
      await consumer.stop().catch(() => undefined);
      await producer.disconnect().catch(() => undefined);
      await db.end().catch(() => undefined);
      process.exit(0);
    })();
  });
}
