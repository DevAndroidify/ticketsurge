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
import { createSagaHandler } from './saga.js';

export const ROLES = ['relay', 'saga'] as const;
export type Role = (typeof ROLES)[number];

export interface BookingWorkerDeps {
  db: pg.Pool;
  kafka: KafkaClient;
  producer: Producer;
  logger: Logger;
}

export interface BookingWorkerSettings {
  /** Which parts run in this process. Splitting them lets each scale or fail independently. */
  roles: readonly Role[];
  topicPrefix: string;
  outboxBatchSize: number;
  outboxPollIntervalMs: number;
  consumerConcurrency: number;
}

export interface RunningBookingWorker {
  consumers: Partial<Record<Role, RunningConsumer>>;
  stop(): Promise<void>;
}

export async function startBookingWorker(
  deps: BookingWorkerDeps,
  settings: BookingWorkerSettings,
): Promise<RunningBookingWorker> {
  const { db, kafka, producer, logger } = deps;
  const prefix = settings.topicPrefix;
  const controller = new AbortController();
  const loops: Promise<void>[] = [];
  const consumers: Partial<Record<Role, RunningConsumer>> = {};

  if (settings.roles.includes('relay')) {
    const relay = new OutboxRelay({
      db,
      table: 'booking.outbox',
      producer,
      logger,
      batchSize: settings.outboxBatchSize,
      pollIntervalMs: settings.outboxPollIntervalMs,
    });
    loops.push(relay.run(controller.signal));
  }

  if (settings.roles.includes('saga')) {
    consumers.saga = await startConsumer({
      kafka,
      groupId: `${prefix}${CONSUMER_GROUPS.bookingSaga}`,
      topics: [topicName(prefix, TOPICS.paymentCompleted)],
      logger,
      concurrency: settings.consumerConcurrency,
      handle: createSagaHandler(db, prefix, logger),
    });
  }

  logger.info({ roles: settings.roles }, 'booking worker started');
  return {
    consumers,
    async stop() {
      await Promise.all(Object.values(consumers).map((consumer) => consumer.stop()));
      controller.abort();
      await Promise.all(loops);
    },
  };
}
