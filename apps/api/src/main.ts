import { ConfigError, createLogger, loadConfig, loadEnvFile } from '@ticketsurge/config';
import { buildApp } from './app.js';
import { createClients } from './clients.js';
import { apiConfigSchema } from './config.js';
import { kafkaCheck, postgresCheck, redisCheck } from './health/dependency-checks.js';

function readConfig() {
  loadEnvFile();
  try {
    return loadConfig(apiConfigSchema);
  } catch (error) {
    if (error instanceof ConfigError) {
      // The logger depends on config, so report invalid config directly and stop.
      console.error(error.message);
      process.exit(1);
    }
    throw error;
  }
}

async function main(): Promise<void> {
  const config = readConfig();
  const logger = createLogger({ service: 'api', level: config.LOG_LEVEL });
  const clients = createClients(config, logger);

  const app = await buildApp({
    logger,
    healthCheckTimeoutMs: config.HEALTH_CHECK_TIMEOUT_MS,
    healthChecks: {
      postgres: postgresCheck(clients.pg),
      redis: redisCheck(clients.redis, config.HEALTH_CHECK_TIMEOUT_MS),
      kafka: kafkaCheck(clients.kafkaAdmin, config.HEALTH_CHECK_TIMEOUT_MS),
    },
  });
  app.addHook('onClose', async () => {
    await clients.close();
  });

  let shuttingDown = false;
  const shutdown = (signal: NodeJS.Signals) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');
    app.close().then(
      () => process.exit(0),
      (error: unknown) => {
        logger.error({ err: error }, 'error during shutdown');
        process.exit(1);
      },
    );
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  await app.listen({ host: config.API_HOST, port: config.API_PORT });
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
