import type { Logger } from '@ticketsurge/config';
import { CONSUMER_GROUPS, TOPICS, dlqTopicName, topicName } from '@ticketsurge/events';
import {
  startConsumer,
  type KafkaClient,
  type Producer,
  type RunningConsumer,
} from '@ticketsurge/kafka';
import { OutboxRelay, outboxBacklog } from '@ticketsurge/outbox';
import type { Redis } from 'ioredis';
import type { ServiceMetrics } from '@ticketsurge/metrics';
import type pg from 'pg';
import { createSagaHandler } from './saga.js';
import { createSeatSyncHandler } from './seat-sync.js';
import { ExpirySweeper, type SweeperSettings } from './sweeper.js';

export const ROLES = ['relay', 'saga', 'sweeper', 'seat-sync'] as const;
export type Role = (typeof ROLES)[number];

export interface BookingWorkerDeps {
  /** Prometheus metrics; omitted in tests that do not need them. */
  metrics?: ServiceMetrics;
  db: pg.Pool;
  kafka: KafkaClient;
  producer: Producer;
  logger: Logger;
  /** Used by seat-sync. */
  redis: Redis;
}

export interface BookingWorkerSettings {
  /** Which parts run in this process. Splitting them lets each scale or fail independently. */
  roles: readonly Role[];
  topicPrefix: string;
  outboxBatchSize: number;
  outboxPollIntervalMs: number;
  consumerConcurrency: number;
  sweeper: Omit<SweeperSettings, 'topicPrefix'>;
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
      onPublished: deps.metrics?.outbox('booking.outbox', () =>
        outboxBacklog(db, 'booking.outbox'),
      ),
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
      onOutcome: deps.metrics?.consumer(`${prefix}${CONSUMER_GROUPS.bookingSaga}`),
      deadLetter: {
        topic: dlqTopicName(prefix, CONSUMER_GROUPS.bookingSaga),
        producer,
      },
      topics: [topicName(prefix, TOPICS.paymentCompleted)],
      logger,
      concurrency: settings.consumerConcurrency,
      handle: createSagaHandler(db, prefix, logger),
    });
  }

  if (settings.roles.includes('seat-sync')) {
    consumers['seat-sync'] = await startConsumer({
      kafka,
      groupId: `${prefix}${CONSUMER_GROUPS.seatSync}`,
      onOutcome: deps.metrics?.consumer(`${prefix}${CONSUMER_GROUPS.seatSync}`),
      deadLetter: {
        topic: dlqTopicName(prefix, CONSUMER_GROUPS.seatSync),
        producer,
      },
      topics: [
        topicName(prefix, TOPICS.bookingConfirmed),
        topicName(prefix, TOPICS.bookingCancelled),
      ],
      logger,
      concurrency: settings.consumerConcurrency,
      handle: createSeatSyncHandler(deps.redis, logger),
    });
  }

  if (settings.roles.includes('sweeper')) {
    const sweeper = new ExpirySweeper(db, { ...settings.sweeper, topicPrefix: prefix }, logger);
    loops.push(sweeper.run(controller.signal));
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
