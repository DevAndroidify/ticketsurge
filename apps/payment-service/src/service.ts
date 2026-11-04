import type { Logger } from '@ticketsurge/config';
import { CONSUMER_GROUPS, TOPICS, dlqTopicName, topicName } from '@ticketsurge/events';
import {
  startConsumer,
  type KafkaClient,
  type Producer,
  type RunningConsumer,
} from '@ticketsurge/kafka';
import { OutboxRelay, outboxBacklog } from '@ticketsurge/outbox';
import type { ServiceMetrics } from '@ticketsurge/metrics';
import type pg from 'pg';
import { createPaymentHandler, type PaymentSettings } from './payments.js';

export interface PaymentServiceDeps {
  /** Prometheus metrics; omitted in tests that do not need them. */
  metrics?: ServiceMetrics;
  db: pg.Pool;
  kafka: KafkaClient;
  producer: Producer;
  logger: Logger;
}

export interface PaymentServiceSettings extends PaymentSettings {
  concurrency: number;
  outboxBatchSize: number;
  outboxPollIntervalMs: number;
}

export interface RunningPaymentService {
  consumer: RunningConsumer;
  stop(): Promise<void>;
}

/** Starts the consumer (booking.requested, payment.refund-requested) and the payment outbox relay. */
export async function startPaymentService(
  deps: PaymentServiceDeps,
  settings: PaymentServiceSettings,
): Promise<RunningPaymentService> {
  const { db, kafka, producer, logger } = deps;
  const prefix = settings.topicPrefix;

  const controller = new AbortController();
  const relay = new OutboxRelay({
    db,
    table: 'payment.outbox',
    onPublished: deps.metrics?.outbox('payment.outbox', () => outboxBacklog(db, 'payment.outbox')),
    producer,
    logger,
    batchSize: settings.outboxBatchSize,
    pollIntervalMs: settings.outboxPollIntervalMs,
  });
  const relayRun = relay.run(controller.signal);

  const consumer = await startConsumer({
    kafka,
    groupId: `${prefix}${CONSUMER_GROUPS.paymentService}`,
    onOutcome: deps.metrics?.consumer(`${prefix}${CONSUMER_GROUPS.paymentService}`),
    deadLetter: {
      topic: dlqTopicName(prefix, CONSUMER_GROUPS.paymentService),
      producer,
    },
    topics: [topicName(prefix, TOPICS.bookingRequested), topicName(prefix, TOPICS.refundRequested)],
    logger,
    concurrency: settings.concurrency,
    handle: createPaymentHandler(db, settings, logger),
  });

  return {
    consumer,
    async stop() {
      await consumer.stop();
      controller.abort();
      await relayRun;
    },
  };
}
