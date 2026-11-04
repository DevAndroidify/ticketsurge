import { ConfigError, createLogger, loadConfig, loadEnvFile } from '@ticketsurge/config';
import { createKafka, createProducer } from '@ticketsurge/kafka';
import { Redis } from 'ioredis';
import { ServiceMetrics, startMetricsServer } from '@ticketsurge/metrics';
import pg from 'pg';
import { bookingWorkerConfigSchema } from './config.js';
import { startBookingWorker } from './worker.js';

function readConfig() {
  loadEnvFile();
  try {
    return loadConfig(bookingWorkerConfigSchema);
  } catch (error) {
    if (error instanceof ConfigError) {
      console.error(error.message);
      process.exit(1);
    }
    throw error;
  }
}

const config = readConfig();
const logger = createLogger({ service: 'booking-worker', level: config.LOG_LEVEL });
const metrics = new ServiceMetrics('booking-worker');
await startMetricsServer(metrics.registry, config.METRICS_PORT);

const db = new pg.Pool({
  connectionString: config.DATABASE_URL,
  max: config.CONSUMER_CONCURRENCY + 6,
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
const redis = new Redis(config.REDIS_URL, {
  // Fail fast while Redis is down; seat-sync's consumer retries the message with backoff.
  enableOfflineQueue: false,
  maxRetriesPerRequest: 1,
  retryStrategy: (attempt) => Math.min(attempt * 200, 2000),
});
redis.on('error', (error) => {
  logger.warn({ err: error }, 'redis connection error');
});

const worker = await startBookingWorker(
  { db, kafka, producer, logger, metrics, redis },
  {
    roles: config.BOOKING_WORKER_ROLES,
    topicPrefix: config.KAFKA_TOPIC_PREFIX,
    outboxBatchSize: config.OUTBOX_BATCH_SIZE,
    outboxPollIntervalMs: config.OUTBOX_POLL_INTERVAL_MS,
    consumerConcurrency: config.CONSUMER_CONCURRENCY,
    sweeper: {
      intervalMs: config.SWEEP_INTERVAL_MS,
      batchSize: config.SWEEP_BATCH_SIZE,
      maxOutboxBacklog: config.SWEEPER_MAX_OUTBOX_BACKLOG,
    },
  },
);

let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    if (stopping) return;
    stopping = true;
    logger.info({ signal }, 'shutting down');
    void (async () => {
      await worker.stop().catch((error: unknown) => {
        logger.error({ err: error }, 'error while stopping');
      });
      await producer.disconnect().catch(() => undefined);
      await redis.quit().catch(() => undefined);
      await db.end().catch(() => undefined);
      process.exit(0);
    })();
  });
}
