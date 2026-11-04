import {
  brokerListSchema,
  createLogger,
  kafkaTopicPrefixSchema,
  loadConfig,
  loadEnvFile,
} from '@ticketsurge/config';
import { createKafka, type Admin } from '@ticketsurge/kafka';
import { z } from 'zod';

/** Runs `fn` with a connected admin client configured from the environment. */
export async function withAdmin<T>(fn: (admin: Admin, prefix: string) => Promise<T>): Promise<T> {
  loadEnvFile();
  const config = loadConfig(
    z.object({ KAFKA_BROKERS: brokerListSchema, KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema }),
  );
  const logger = createLogger({ service: 'kafka-admin', level: 'warn' });
  const admin = createKafka({
    brokers: config.KAFKA_BROKERS,
    clientId: 'kafka-admin',
    logger,
  }).admin();
  await admin.connect();
  try {
    return await fn(admin, config.KAFKA_TOPIC_PREFIX);
  } finally {
    await admin.disconnect();
  }
}
