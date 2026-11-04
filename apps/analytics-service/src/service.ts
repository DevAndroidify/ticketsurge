import type { Logger } from '@ticketsurge/config';
import { CONSUMER_GROUPS, TOPICS, dlqTopicName, topicName } from '@ticketsurge/events';
import {
  startConsumer,
  type KafkaClient,
  type Producer,
  type RunningConsumer,
} from '@ticketsurge/kafka';
import type pg from 'pg';
import { createAnalyticsHandler } from './analytics.js';

export interface AnalyticsServiceDeps {
  db: pg.Pool;
  kafka: KafkaClient;
  /** Only used to write failed messages to the dead-letter topic. */
  producer: Producer;
  logger: Logger;
}

export function analyticsGroupId(prefix: string): string {
  return `${prefix}${CONSUMER_GROUPS.analytics}`;
}

export async function startAnalyticsService(
  deps: AnalyticsServiceDeps,
  settings: { topicPrefix: string; concurrency: number },
): Promise<RunningConsumer> {
  const prefix = settings.topicPrefix;
  return startConsumer({
    kafka: deps.kafka,
    groupId: analyticsGroupId(prefix),
    deadLetter: { topic: dlqTopicName(prefix, CONSUMER_GROUPS.analytics), producer: deps.producer },
    topics: [
      topicName(prefix, TOPICS.bookingConfirmed),
      topicName(prefix, TOPICS.bookingCancelled),
    ],
    logger: deps.logger,
    concurrency: settings.concurrency,
    handle: createAnalyticsHandler(deps.db, deps.logger),
  });
}
