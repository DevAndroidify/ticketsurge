import confluent, { type KafkaJS } from '@confluentinc/kafka-javascript';
import type { Logger } from '@ticketsurge/config';
import { Redis } from 'ioredis';
import pg from 'pg';
import type { ApiConfig } from './config.js';

const { Kafka, logLevel } = confluent.KafkaJS;

export interface Clients {
  pg: pg.Pool;
  redis: Redis;
  kafkaAdmin: KafkaJS.Admin;
  close(): Promise<void>;
}

type ClientConfig = Pick<
  ApiConfig,
  'DATABASE_URL' | 'REDIS_URL' | 'KAFKA_BROKERS' | 'KAFKA_CLIENT_ID' | 'HEALTH_CHECK_TIMEOUT_MS'
>;

/**
 * Creates the PostgreSQL pool, Redis client and Kafka admin client.
 * None of them blocks startup: each reconnects in the background, and GET /health reports
 * which dependency is down.
 */
export function createClients(config: ClientConfig, logger: Logger): Clients {
  const pool = new pg.Pool({
    connectionString: config.DATABASE_URL,
    // Fail fast instead of waiting forever when PostgreSQL is unreachable.
    connectionTimeoutMillis: config.HEALTH_CHECK_TIMEOUT_MS,
  });
  // An idle client can error when the server restarts; without a listener this crashes the process.
  pool.on('error', (error) => {
    logger.warn({ err: error }, 'postgres idle client error');
  });

  const redis = new Redis(config.REDIS_URL, {
    // Fail commands immediately while disconnected instead of queueing them. Holds must never
    // be silently delayed, and callers turn the error into a 503.
    enableOfflineQueue: false,
    maxRetriesPerRequest: 1,
    connectTimeout: config.HEALTH_CHECK_TIMEOUT_MS,
    retryStrategy: (attempt) => Math.min(attempt * 200, 2000),
  });
  redis.on('error', (error) => {
    logger.warn({ err: error }, 'redis connection error');
  });

  const kafka = new Kafka({
    kafkaJS: {
      brokers: config.KAFKA_BROKERS,
      clientId: config.KAFKA_CLIENT_ID,
      connectionTimeout: config.HEALTH_CHECK_TIMEOUT_MS,
      requestTimeout: config.HEALTH_CHECK_TIMEOUT_MS,
      // librdkafka logs every reconnect attempt; the health endpoint already reports outages.
      logLevel: logLevel.ERROR,
    },
  });
  const kafkaAdmin = kafka.admin();

  return {
    pg: pool,
    redis,
    kafkaAdmin,
    async close() {
      await Promise.allSettled([pool.end(), redis.quit(), kafkaAdmin.disconnect()]);
    },
  };
}
