import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { Logger } from '@ticketsurge/config';
import { parseEnvelope, type AnyEnvelope } from '@ticketsurge/events';
import type { KafkaClient } from './client.js';

export interface MessageMeta {
  topic: string;
  partition: number;
  offset: string;
}

export type EnvelopeHandler = (envelope: AnyEnvelope, meta: MessageMeta) => Promise<void>;

export interface ConsumerOptions {
  kafka: KafkaClient;
  groupId: string;
  topics: string[];
  logger: Logger;
  handle: EnvelopeHandler;
  /** Partitions processed in parallel; messages within one partition stay in order. */
  concurrency?: number;
  /** Retry delay bounds for failing handlers. */
  retry?: { initialMs: number; maxMs: number };
}

export interface RunningConsumer {
  consumer: KafkaJS.Consumer;
  /** Resolves once this member owns at least one partition (useful in tests). */
  waitForAssignment(timeoutMs?: number): Promise<void>;
  stop(): Promise<void>;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * At-least-once consumer:
 * - every message is validated against the shared envelope schema;
 * - the offset is committed manually, only after the handler (and its database transaction)
 *   succeeded, so a crash re-delivers instead of losing a message;
 * - handlers must therefore be idempotent (processed_events, conditional updates).
 *
 * A failing handler is retried in place with exponential backoff. A malformed message is logged
 * and skipped. (Phase 12 replaces both with bounded retries and a dead-letter topic.)
 */
export async function startConsumer(options: ConsumerOptions): Promise<RunningConsumer> {
  const { kafka, groupId, topics, logger, handle } = options;
  const retry = options.retry ?? { initialMs: 200, maxMs: 5000 };
  const log = logger.child({ consumerGroup: groupId });
  let stopping = false;

  const consumer = kafka.consumer({
    kafkaJS: {
      groupId,
      // A brand-new group starts at the earliest offset so nothing published before it is missed.
      fromBeginning: true,
      autoCommit: false,
    },
  });
  await consumer.connect();
  await consumer.subscribe({ topics });

  const commit = (topic: string, partition: number, offset: string) =>
    consumer.commitOffsets([{ topic, partition, offset: (BigInt(offset) + 1n).toString() }]);

  await consumer.run({
    partitionsConsumedConcurrently: options.concurrency ?? 1,
    eachMessage: async ({ topic, partition, message }) => {
      const meta = { topic, partition, offset: message.offset };

      let envelope: AnyEnvelope;
      try {
        envelope = parseEnvelope(JSON.parse(message.value?.toString('utf8') ?? 'null'));
      } catch (error) {
        log.error({ err: error, ...meta }, 'skipping malformed message');
        await commit(topic, partition, message.offset);
        return;
      }

      for (let attempt = 1; ; attempt++) {
        try {
          await handle(envelope, meta);
          break;
        } catch (error) {
          if (stopping) throw error;
          const delay = Math.min(retry.maxMs, retry.initialMs * 2 ** (attempt - 1));
          log.warn(
            { err: error, ...meta, messageId: envelope.messageId, attempt, retryInMs: delay },
            'handler failed, retrying',
          );
          await sleep(delay);
        }
      }
      // Committed only after the handler's own transaction committed.
      await commit(topic, partition, message.offset);
    },
  });

  return {
    consumer,
    async waitForAssignment(timeoutMs = 30_000) {
      const deadline = Date.now() + timeoutMs;
      while (consumer.assignment().length === 0) {
        if (Date.now() > deadline) throw new Error(`${groupId}: no partitions assigned`);
        await sleep(100);
      }
    },
    async stop() {
      stopping = true;
      await consumer.disconnect();
    },
  };
}
