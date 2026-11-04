import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { Logger } from '@ticketsurge/config';
import { parseEnvelope, type AnyEnvelope } from '@ticketsurge/events';
import { ZodError } from 'zod';
import type { KafkaClient, Producer } from './client.js';

export interface MessageMeta {
  topic: string;
  partition: number;
  offset: string;
}

export type EnvelopeHandler = (envelope: AnyEnvelope, meta: MessageMeta) => Promise<void>;

/**
 * Throw this from a handler when retrying cannot help (bad data, violated invariant). The message
 * goes straight to the dead-letter topic instead of being retried.
 */
export class PermanentError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'PermanentError';
  }
}

export interface RetryPolicy {
  /** Total attempts, including the first. */
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
}

export const DEFAULT_RETRY: RetryPolicy = { maxAttempts: 5, initialDelayMs: 200, maxDelayMs: 5000 };

export interface ConsumerOptions {
  kafka: KafkaClient;
  groupId: string;
  topics: string[];
  logger: Logger;
  handle: EnvelopeHandler;
  /** Dead-letter topic for this group (dlq.<consumer-group>) and the producer that writes to it. */
  deadLetter: { topic: string; producer: Producer };
  /** Partitions processed in parallel; messages within one partition stay in order. */
  concurrency?: number;
  retry?: RetryPolicy;
  /** Observes each message's outcome (used for metrics). */
  onOutcome?: (outcome: MessageOutcome) => void;
}

export interface MessageOutcome {
  topic: string;
  result: 'processed' | 'dead_lettered';
  attempts: number;
  durationMs: number;
}

export interface RunningConsumer {
  consumer: KafkaJS.Consumer;
  /** Resolves once this member owns at least one partition (useful in tests). */
  waitForAssignment(timeoutMs?: number): Promise<void>;
  stop(): Promise<void>;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Malformed input and explicit PermanentErrors are not worth retrying. */
export function isPermanent(error: unknown): boolean {
  return (
    error instanceof PermanentError || error instanceof ZodError || error instanceof SyntaxError
  );
}

function describe(error: unknown): string {
  if (error instanceof ZodError)
    return `invalid message: ${error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ')}`;
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  return String(error);
}

/**
 * At-least-once consumer:
 * - every message is validated against the shared envelope schema;
 * - a failing handler is retried with exponential backoff, up to `retry.maxAttempts`;
 * - a message that is malformed, fails permanently, or runs out of retries is sent to the
 *   dead-letter topic with its original key, value, headers, and the error details, and the consumer
 *   moves on, so one bad message cannot block its partition;
 * - the offset is committed manually, only after the handler (and its database transaction)
 *   succeeded or the message was safely dead-lettered. If the dead-letter write itself fails, the
 *   offset is not committed and the message is processed again.
 *
 * Handlers must be idempotent (processed_events, conditional updates), because redelivery happens.
 */
export async function startConsumer(options: ConsumerOptions): Promise<RunningConsumer> {
  const { kafka, groupId, topics, logger, handle, deadLetter } = options;
  const retry = options.retry ?? DEFAULT_RETRY;
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

  async function sendToDeadLetter(
    meta: MessageMeta,
    message: KafkaJS.KafkaMessage,
    error: unknown,
    attempts: number,
  ) {
    const headers: Record<string, string | Buffer> = {};
    for (const [name, value] of Object.entries(message.headers ?? {})) {
      if (typeof value === 'string' || Buffer.isBuffer(value)) headers[name] = value;
    }
    await deadLetter.producer.send({
      topic: deadLetter.topic,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...headers,
            'dlq-error': describe(error).slice(0, 2000),
            'dlq-original-topic': meta.topic,
            'dlq-original-partition': String(meta.partition),
            'dlq-original-offset': meta.offset,
            'dlq-consumer-group': groupId,
            'dlq-attempts': String(attempts),
            'dlq-failed-at': new Date().toISOString(),
          },
        },
      ],
    });
  }

  await consumer.run({
    partitionsConsumedConcurrently: options.concurrency ?? 1,
    eachMessage: async ({ topic, partition, message }) => {
      const meta = { topic, partition, offset: message.offset };
      const started = performance.now();
      let attempts = 0;
      let failure: unknown;

      try {
        const envelope = parseEnvelope(JSON.parse(message.value?.toString('utf8') ?? 'null'));
        for (;;) {
          attempts++;
          try {
            await handle(envelope, meta);
            failure = undefined;
            break;
          } catch (error) {
            failure = error;
            if (stopping) throw error;
            if (isPermanent(error) || attempts >= retry.maxAttempts) break;
            const delay = Math.min(retry.maxDelayMs, retry.initialDelayMs * 2 ** (attempts - 1));
            log.warn(
              {
                err: error,
                ...meta,
                messageId: envelope.messageId,
                attempt: attempts,
                retryInMs: delay,
              },
              'handler failed, retrying',
            );
            await sleep(delay);
          }
        }
      } catch (error) {
        if (stopping) throw error;
        // JSON or schema errors from parsing: the message can never be processed.
        failure = error;
      }

      if (failure !== undefined) {
        log.error(
          { err: failure, ...meta, attempts, deadLetterTopic: deadLetter.topic },
          'sending message to dead-letter topic',
        );
        await sendToDeadLetter(meta, message, failure, attempts);
      }
      // Committed only after success or a successful dead-letter write.
      await commit(topic, partition, message.offset);
      options.onOutcome?.({
        topic,
        result: failure === undefined ? 'processed' : 'dead_lettered',
        attempts,
        durationMs: performance.now() - started,
      });
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
