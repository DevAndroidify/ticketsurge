import { ConfigError, createLogger, loadConfig, loadEnvFile } from '@ticketsurge/config';
import { createKafka, createProducer } from '@ticketsurge/kafka';
import pg from 'pg';
import { analyticsConfigSchema } from './config.js';
import { startAnalyticsService } from './service.js';

function readConfig() {
  loadEnvFile();
  try {
    return loadConfig(analyticsConfigSchema);
  } catch (error) {
    if (error instanceof ConfigError) {
      console.error(error.message);
      process.exit(1);
    }
    throw error;
  }
}

const config = readConfig();
const logger = createLogger({ service: 'analytics-service', level: config.LOG_LEVEL });
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

const consumer = await startAnalyticsService(
  { db, kafka, producer, logger },
  { topicPrefix: config.KAFKA_TOPIC_PREFIX, concurrency: config.CONSUMER_CONCURRENCY },
);
logger.info('analytics service started');

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
