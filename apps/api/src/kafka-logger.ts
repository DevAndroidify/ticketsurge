import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { Logger } from '@ticketsurge/config';

/**
 * Routes Kafka client logs into pino at debug level. librdkafka logs every failed reconnect
 * (several per second while a broker is down), which would flood stderr; outages are reported
 * through GET /health instead. Set LOG_LEVEL=debug to see them.
 */
export function kafkaLogger(logger: Logger): KafkaJS.Logger {
  const child = logger.child({ component: 'kafka-client' });
  const forward = (message: string, extra?: object) => {
    child.debug(extra ?? {}, message);
  };
  const adapter: KafkaJS.Logger = {
    info: forward,
    warn: forward,
    error: forward,
    debug: forward,
    namespace: () => adapter,
    setLogLevel: () => undefined,
  };
  return adapter;
}
