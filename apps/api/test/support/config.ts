import { loadConfig } from '@ticketsurge/config';
import { apiConfigSchema, type ApiConfig } from '../../src/config.js';

/** Static, fully valid config for unit tests. No real services are contacted. */
export function unitTestConfig(overrides: Partial<ApiConfig> = {}): ApiConfig {
  const base = loadConfig(apiConfigSchema, {
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL: 'postgres://unit:unit@127.0.0.1:1/unit',
    REDIS_URL: 'redis://127.0.0.1:1/0',
    KAFKA_BROKERS: '127.0.0.1:1',
    ADMIN_API_KEY: 'unit-test-admin-key-0123456789',
  });
  return { ...base, ...overrides };
}
