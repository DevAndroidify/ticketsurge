import type { Logger } from '@ticketsurge/config';
import { CONSUMER_GROUPS, TOPICS, dlqTopicName, topicName } from '@ticketsurge/events';
import {
  startConsumer,
  type KafkaClient,
  type Producer,
  type RunningConsumer,
} from '@ticketsurge/kafka';
import type pg from 'pg';
import { createNotificationHandler, type Sender } from './notifications.js';

export interface NotificationServiceDeps {
  db: pg.Pool;
  kafka: KafkaClient;
  /** Only used to write failed messages to the dead-letter topic. */
  producer: Producer;
  logger: Logger;
  /** Defaults to logging the message (simulated email/SMS). */
  send?: Sender;
}

export interface NotificationServiceSettings {
  topicPrefix: string;
  concurrency: number;
}

/**
 * Consumes ticket.issued and booking.cancelled. It publishes no events, so it has no outbox; its
 * producer only writes to the dead-letter topic.
 */
export async function startNotificationService(
  deps: NotificationServiceDeps,
  settings: NotificationServiceSettings,
): Promise<RunningConsumer> {
  const { db, kafka, producer, logger } = deps;
  const prefix = settings.topicPrefix;
  const send: Sender =
    deps.send ??
    ((notification) => {
      logger.info({ ...notification, channel: 'email (simulated)' }, 'notification sent');
    });
  return startConsumer({
    kafka,
    groupId: `${prefix}${CONSUMER_GROUPS.notificationService}`,
    deadLetter: {
      topic: dlqTopicName(prefix, CONSUMER_GROUPS.notificationService),
      producer,
    },
    topics: [topicName(prefix, TOPICS.ticketIssued), topicName(prefix, TOPICS.bookingCancelled)],
    logger,
    concurrency: settings.concurrency,
    handle: createNotificationHandler(db, logger, send),
  });
}
