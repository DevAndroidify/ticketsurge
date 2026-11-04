import { ConfigError, createLogger, loadConfig, loadEnvFile } from '@ticketsurge/config';
import { Redis } from 'ioredis';
import { gatewayConfigSchema } from './config.js';
import { buildGateway } from './gateway.js';

function readConfig() {
  loadEnvFile();
  try {
    return loadConfig(gatewayConfigSchema);
  } catch (error) {
    if (error instanceof ConfigError) {
      console.error(error.message);
      process.exit(1);
    }
    throw error;
  }
}

const config = readConfig();
const logger = createLogger({ service: 'realtime-gateway', level: config.LOG_LEVEL });

// Reconnects and re-subscribes automatically after a Redis restart. Messages published while it was
// disconnected are lost; clients reload state after reconnecting.
const subscriber = new Redis(config.REDIS_URL, {
  retryStrategy: (attempt) => Math.min(attempt * 200, 2000),
});
subscriber.on('error', (error) => {
  logger.warn({ err: error }, 'redis connection error');
});

const { app } = await buildGateway({ logger, subscriber, config });

let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    if (stopping) return;
    stopping = true;
    logger.info({ signal }, 'shutting down');
    void (async () => {
      await app.close().catch(() => undefined);
      await subscriber.quit().catch(() => undefined);
      process.exit(0);
    })();
  });
}

await app.listen({ host: config.REALTIME_HOST, port: config.REALTIME_PORT });
