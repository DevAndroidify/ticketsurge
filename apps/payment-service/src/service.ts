import type { Logger } from '@ticketsurge/config';
import { CONSUMER_GROUPS, TOPICS, topicName } from '@ticketsurge/events';
import {
  startConsumer,
  type KafkaClient,
  type Producer,
  type RunningConsumer,
} from '@ticketsurge/kafka';
import { OutboxRelay } from '@ticketsurge/outbox';
import type pg from 'pg';
import { createPaymentHandler, type PaymentSettings } from './payments.js';

export interface PaymentServiceDeps {
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

/** Starts the booking.requested consumer and the payment outbox relay. */
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
    producer,
    logger,
    batchSize: settings.outboxBatchSize,
    pollIntervalMs: settings.outboxPollIntervalMs,
  });
  const relayRun = relay.run(controller.signal);

  const consumer = await startConsumer({
    kafka,
    groupId: `${prefix}${CONSUMER_GROUPS.paymentService}`,
    topics: [topicName(prefix, TOPICS.bookingRequested)],
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
