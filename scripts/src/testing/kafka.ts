import { randomBytes } from 'node:crypto';
import { createLogger, loadEnvFile } from '@ticketsurge/config';
import { ALL_EVENT_TYPES, parseEnvelope, topicName, type AnyEnvelope } from '@ticketsurge/events';
import {
  createKafka,
  deleteTopicsWithPrefix,
  ensureTopics,
  type KafkaClient,
} from '@ticketsurge/kafka';

export const TEST_TOPIC_ROOT = 'test.';
export const testLogger = createLogger({
  service: 'kafka-test',
  level: process.env.TEST_LOG_LEVEL === undefined ? 'silent' : 'debug',
});

export function testKafka(clientId = 'kafka-test'): KafkaClient {
  loadEnvFile();
  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
  return createKafka({ brokers, clientId, logger: testLogger });
}

/**
 * Creates every topic under a fresh prefix (test.<random>.), so each test file gets empty topics
 * and never sees development traffic or messages from earlier runs. Returns the prefix and a
 * cleanup function that deletes the topics again.
 */
export async function createTestTopics(): Promise<{ prefix: string; cleanup(): Promise<void> }> {
  const prefix = `${TEST_TOPIC_ROOT}${randomBytes(4).toString('hex')}.`;
  const admin = testKafka('test-admin').admin();
  await admin.connect();
  try {
    await ensureTopics(
      admin,
      ALL_EVENT_TYPES.map((type) => topicName(prefix, type)),
      { partitions: 6, replicationFactor: 1, retentionMs: 3_600_000 },
    );
  } finally {
    await admin.disconnect();
  }
  return {
    prefix,
    async cleanup() {
      const cleanupAdmin = testKafka('test-admin').admin();
      await cleanupAdmin.connect();
      try {
        await deleteTopicsWithPrefix(cleanupAdmin, prefix);
      } finally {
        await cleanupAdmin.disconnect();
      }
    },
  };
}

/** Deletes topics left behind by interrupted test runs. */
export async function deleteAllTestTopics(): Promise<void> {
  const admin = testKafka('test-admin').admin();
  await admin.connect();
  try {
    await deleteTopicsWithPrefix(admin, TEST_TOPIC_ROOT);
  } finally {
    await admin.disconnect();
  }
}

export interface ReceivedMessage {
  topic: string;
  partition: number;
  key: string | null;
  headers: Record<string, string>;
  envelope: AnyEnvelope;
}

/**
 * Reads a topic from the beginning with a throwaway consumer group until `predicate` matches
 * `count` messages, or fails after `timeoutMs`.
 */
export async function waitForMessages(
  topic: string,
  predicate: (message: ReceivedMessage) => boolean,
  { count = 1, timeoutMs = 30_000 }: { count?: number; timeoutMs?: number } = {},
): Promise<ReceivedMessage[]> {
  const kafka = testKafka('test-reader');
  const consumer = kafka.consumer({
    kafkaJS: { groupId: `test-reader-${randomBytes(6).toString('hex')}`, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topics: [topic] });
  const matched: ReceivedMessage[] = [];
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`timed out waiting for ${count} message(s) on ${topic}`));
      }, timeoutMs);
      void consumer.run({
        eachMessage: async ({ topic: from, partition, message }) => {
          const headers: Record<string, string> = {};
          for (const [name, value] of Object.entries(message.headers ?? {})) {
            if (value !== undefined) headers[name] = value.toString();
          }
          const received: ReceivedMessage = {
            topic: from,
            partition,
            key: message.key?.toString() ?? null,
            headers,
            envelope: parseEnvelope(JSON.parse(message.value?.toString() ?? 'null')),
          };
          if (predicate(received)) matched.push(received);
          if (matched.length >= count) {
            clearTimeout(timer);
            resolve();
          }
          await Promise.resolve();
        },
      });
    });
  } finally {
    await consumer.disconnect();
  }
  return matched;
}
