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
import { createTicketHandler } from './tickets.js';

export interface TicketServiceDeps {
  /** Prometheus metrics; omitted in tests that do not need them. */
  metrics?: ServiceMetrics;
  db: pg.Pool;
  kafka: KafkaClient;
  producer: Producer;
  logger: Logger;
}

export interface TicketServiceSettings {
  topicPrefix: string;
  concurrency: number;
  outboxBatchSize: number;
  outboxPollIntervalMs: number;
}

export interface RunningTicketService {
  consumer: RunningConsumer;
  stop(): Promise<void>;
}

/** Starts the booking.confirmed consumer and the ticket outbox relay. */
export async function startTicketService(
  deps: TicketServiceDeps,
  settings: TicketServiceSettings,
): Promise<RunningTicketService> {
  const { db, kafka, producer, logger } = deps;
  const prefix = settings.topicPrefix;
  const controller = new AbortController();
  const relay = new OutboxRelay({
    db,
    table: 'ticket.outbox',
    onPublished: deps.metrics?.outbox('ticket.outbox', () => outboxBacklog(db, 'ticket.outbox')),
    producer,
    logger,
    batchSize: settings.outboxBatchSize,
    pollIntervalMs: settings.outboxPollIntervalMs,
  });
  const relayRun = relay.run(controller.signal);

  const consumer = await startConsumer({
    kafka,
    groupId: `${prefix}${CONSUMER_GROUPS.ticketService}`,
    onOutcome: deps.metrics?.consumer(`${prefix}${CONSUMER_GROUPS.ticketService}`),
    deadLetter: {
      topic: dlqTopicName(prefix, CONSUMER_GROUPS.ticketService),
      producer,
    },
    topics: [topicName(prefix, TOPICS.bookingConfirmed)],
    logger,
    concurrency: settings.concurrency,
    handle: createTicketHandler(db, prefix, logger),
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
