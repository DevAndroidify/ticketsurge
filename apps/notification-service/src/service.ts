import type { Logger } from '@ticketsurge/config';
import { CONSUMER_GROUPS, TOPICS, topicName } from '@ticketsurge/events';
import { startConsumer, type KafkaClient, type RunningConsumer } from '@ticketsurge/kafka';
import type pg from 'pg';
import { createNotificationHandler, type Sender } from './notifications.js';

export interface NotificationServiceDeps {
  db: pg.Pool;
  kafka: KafkaClient;
  logger: Logger;
  /** Defaults to logging the message (simulated email/SMS). */
  send?: Sender;
}

export interface NotificationServiceSettings {
  topicPrefix: string;
  concurrency: number;
}

/** Consumes ticket.issued and booking.cancelled. Publishes nothing, so it has no outbox. */
export async function startNotificationService(
  deps: NotificationServiceDeps,
  settings: NotificationServiceSettings,
): Promise<RunningConsumer> {
  const { db, kafka, logger } = deps;
  const prefix = settings.topicPrefix;
  const send: Sender =
    deps.send ??
    ((notification) => {
      logger.info({ ...notification, channel: 'email (simulated)' }, 'notification sent');
    });
  return startConsumer({
    kafka,
    groupId: `${prefix}${CONSUMER_GROUPS.notificationService}`,
    topics: [topicName(prefix, TOPICS.ticketIssued), topicName(prefix, TOPICS.bookingCancelled)],
    logger,
    concurrency: settings.concurrency,
    handle: createNotificationHandler(db, logger, send),
  });
}
