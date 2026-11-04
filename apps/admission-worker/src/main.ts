import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { ConfigError, createLogger, loadConfig, loadEnvFile } from '@ticketsurge/config';
import { Redis } from 'ioredis';
import { runAdmissionLoop } from './admission.js';
import { admissionConfigSchema } from './config.js';

function readConfig() {
  loadEnvFile();
  try {
    return loadConfig(admissionConfigSchema);
  } catch (error) {
    if (error instanceof ConfigError) {
      console.error(error.message);
      process.exit(1);
    }
    throw error;
  }
}

const config = readConfig();
const logger = createLogger({ service: 'admission-worker', level: config.LOG_LEVEL });
const redis = new Redis(config.REDIS_URL, {
  // Fail fast while Redis is down; the loop retries on the next round.
  enableOfflineQueue: false,
  maxRetriesPerRequest: 1,
  retryStrategy: (attempt) => Math.min(attempt * 200, 2000),
});
redis.on('error', (error) => {
  logger.warn({ err: error }, 'redis connection error');
});

const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    logger.info({ signal }, 'shutting down');
    controller.abort();
  });
}

logger.info(
  {
    intervalMs: config.ADMISSION_INTERVAL_MS,
    batchSize: config.ADMISSION_BATCH_SIZE,
    admissionTtlMs: config.ADMISSION_TTL_MS,
  },
  'admission worker started',
);
await runAdmissionLoop(
  redis,
  {
    batchSize: config.ADMISSION_BATCH_SIZE,
    admissionTtlMs: config.ADMISSION_TTL_MS,
    intervalMs: config.ADMISSION_INTERVAL_MS,
    instanceId: `${hostname()}:${process.pid}:${randomUUID()}`,
  },
  logger,
  controller.signal,
);
await redis.quit().catch(() => undefined);
