import confluent, { type KafkaJS } from '@confluentinc/kafka-javascript';
import type { Logger } from '@ticketsurge/config';
import { kafkaLogger } from './logger.js';

const { Kafka, logLevel } = confluent.KafkaJS;

export type KafkaClient = KafkaJS.Kafka;
export type Producer = KafkaJS.Producer;
export type Admin = KafkaJS.Admin;

export interface KafkaSettings {
  brokers: string[];
  clientId: string;
  logger: Logger;
  /** Connection and request timeout; keeps calls from hanging while a broker is down. */
  timeoutMs?: number;
}

export function createKafka({ brokers, clientId, logger, timeoutMs = 5000 }: KafkaSettings) {
  return new Kafka({
    kafkaJS: {
      brokers,
      clientId,
      connectionTimeout: timeoutMs,
      requestTimeout: timeoutMs,
      logger: kafkaLogger(logger),
      logLevel: logLevel.WARN,
    },
  });
}

/**
 * Producer with acks=all and idempotence: a write is acknowledged only once every in-sync replica
 * has it, and broker-side retries cannot create duplicates or reorder messages within a partition.
 */
export async function createProducer(kafka: KafkaClient): Promise<Producer> {
  const producer = kafka.producer({ kafkaJS: { acks: -1, idempotent: true } });
  await producer.connect();
  return producer;
}

export interface TopicSettings {
  partitions: number;
  replicationFactor: number;
  retentionMs: number;
}

/** Creates the topics that do not exist yet. Existing topics are left untouched. */
export async function ensureTopics(
  admin: Admin,
  names: readonly string[],
  settings: TopicSettings,
): Promise<string[]> {
  const existing = new Set(await admin.listTopics());
  const missing = names.filter((name) => !existing.has(name));
  if (missing.length === 0) return [];
  await admin.createTopics({
    topics: missing.map((topic) => ({
      topic,
      numPartitions: settings.partitions,
      replicationFactor: settings.replicationFactor,
      configEntries: [
        { name: 'retention.ms', value: String(settings.retentionMs) },
        // With replication factor 3 this becomes 2: acks=all then survives one broker loss.
        {
          name: 'min.insync.replicas',
          value: String(Math.max(1, Math.min(2, settings.replicationFactor - 1))),
        },
      ],
    })),
  });
  return missing;
}

export async function deleteTopicsWithPrefix(admin: Admin, prefix: string): Promise<string[]> {
  if (!prefix) throw new Error('refusing to delete topics without a prefix');
  const topics = (await admin.listTopics()).filter((name) => name.startsWith(prefix));
  if (topics.length > 0) await admin.deleteTopics({ topics });
  return topics;
}
