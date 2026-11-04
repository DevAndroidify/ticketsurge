// Creates every Kafka topic with the project's settings. Safe to run repeatedly.
import {
  brokerListSchema,
  createLogger,
  durationMsSchema,
  kafkaTopicPrefixSchema,
  loadConfig,
  loadEnvFile,
} from '@ticketsurge/config';
import { ALL_EVENT_TYPES, topicName } from '@ticketsurge/events';
import { createKafka, ensureTopics } from '@ticketsurge/kafka';
import { z } from 'zod';

loadEnvFile();
const config = loadConfig(
  z.object({
    KAFKA_BROKERS: brokerListSchema,
    KAFKA_TOPIC_PREFIX: kafkaTopicPrefixSchema,
    KAFKA_PARTITIONS: z.coerce.number().int().min(1).default(6),
    // 1 for the single local broker. Production-style: 3 (min.insync.replicas becomes 2).
    KAFKA_REPLICATION_FACTOR: z.coerce.number().int().min(1).default(1),
    KAFKA_RETENTION_MS: durationMsSchema.default(7 * 24 * 60 * 60 * 1000),
  }),
);

const logger = createLogger({ service: 'create-topics', level: 'info' });
const kafka = createKafka({ brokers: config.KAFKA_BROKERS, clientId: 'create-topics', logger });
const admin = kafka.admin();
await admin.connect();
try {
  const names = ALL_EVENT_TYPES.map((type) => topicName(config.KAFKA_TOPIC_PREFIX, type));
  const created = await ensureTopics(admin, names, {
    partitions: config.KAFKA_PARTITIONS,
    replicationFactor: config.KAFKA_REPLICATION_FACTOR,
    retentionMs: config.KAFKA_RETENTION_MS,
  });
  for (const name of names)
    console.log(`${created.includes(name) ? 'created' : 'exists '} ${name}`);
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
} finally {
  await admin.disconnect();
}
