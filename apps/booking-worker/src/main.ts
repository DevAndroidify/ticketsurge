import { ConfigError, createLogger, loadConfig, loadEnvFile } from '@ticketsurge/config';
import { createKafka, createProducer } from '@ticketsurge/kafka';
import { OutboxRelay } from '@ticketsurge/outbox';
import pg from 'pg';
import { bookingWorkerConfigSchema } from './config.js';

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

const db = new pg.Pool({ connectionString: config.DATABASE_URL, max: 10 });
db.on('error', (error) => {
  logger.warn({ err: error }, 'postgres idle client error');
});
const kafka = createKafka({
  brokers: config.KAFKA_BROKERS,
  clientId: config.KAFKA_CLIENT_ID,
  logger,
});
const producer = await createProducer(kafka);

const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    logger.info({ signal }, 'shutting down');
    controller.abort();
  });
}

const relay = new OutboxRelay({
  db,
  table: 'booking.outbox',
  producer,
  logger,
  batchSize: config.OUTBOX_BATCH_SIZE,
  pollIntervalMs: config.OUTBOX_POLL_INTERVAL_MS,
});

logger.info('booking worker started: outbox relay');
await relay.run(controller.signal);

await producer.disconnect().catch(() => undefined);
await db.end().catch(() => undefined);
