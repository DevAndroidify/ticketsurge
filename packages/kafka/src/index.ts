export {
  createKafka,
  createProducer,
  deleteTopicsWithPrefix,
  ensureTopics,
  resetGroupToEarliest,
  type Admin,
  type KafkaClient,
  type KafkaSettings,
  type Producer,
  type TopicSettings,
} from './client.js';
export {
  DEFAULT_RETRY,
  PermanentError,
  isPermanent,
  startConsumer,
  type ConsumerOptions,
  type EnvelopeHandler,
  type MessageMeta,
  type MessageOutcome,
  type RetryPolicy,
  type RunningConsumer,
} from './consumer.js';
export { kafkaLogger } from './logger.js';
