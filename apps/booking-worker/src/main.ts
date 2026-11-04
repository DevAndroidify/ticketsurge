import { ConfigError, createLogger, loadConfig, loadEnvFile } from '@ticketsurge/config';
import { createKafka, createProducer } from '@ticketsurge/kafka';
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

const worker = await startBookingWorker(
  { db, kafka, producer, logger },
  {
    roles: config.BOOKING_WORKER_ROLES,
    topicPrefix: config.KAFKA_TOPIC_PREFIX,
    outboxBatchSize: config.OUTBOX_BATCH_SIZE,
    outboxPollIntervalMs: config.OUTBOX_POLL_INTERVAL_MS,
    consumerConcurrency: config.CONSUMER_CONCURRENCY,
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
      await db.end().catch(() => undefined);
      process.exit(0);
    })();
  });
}
