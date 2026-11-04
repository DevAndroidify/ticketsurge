import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import {
  ConfigError,
  brokerListSchema,
  loadConfig,
  loadEnvFile,
  logLevelSchema,
  portSchema,
  postgresUrlSchema,
  redisUrlSchema,
} from './env.js';

const schema = z.object({
  PORT: portSchema,
  LOG_LEVEL: logLevelSchema,
  DATABASE_URL: postgresUrlSchema,
  REDIS_URL: redisUrlSchema,
  KAFKA_BROKERS: brokerListSchema,
});

const validEnv = {
  PORT: '3000',
  DATABASE_URL: 'postgres://u:p@localhost:5433/db',
  REDIS_URL: 'redis://:p@localhost:6380/0',
  KAFKA_BROKERS: 'localhost:9092, broker2:9093',
};

describe('loadConfig', () => {
  it('parses, coerces and applies defaults', () => {
    const config = loadConfig(schema, validEnv);
    expect(config).toEqual({
      PORT: 3000,
      LOG_LEVEL: 'info',
      DATABASE_URL: validEnv.DATABASE_URL,
      REDIS_URL: validEnv.REDIS_URL,
      KAFKA_BROKERS: ['localhost:9092', 'broker2:9093'],
    });
  });

  it('reports every invalid or missing variable in one error', () => {
    let error: unknown;
    try {
      loadConfig(schema, { PORT: '99999', REDIS_URL: 'http://nope', KAFKA_BROKERS: 'no-port' });
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(ConfigError);
    const issues = (error as ConfigError).issues.join('\n');
    expect(issues).toContain('PORT');
    expect(issues).toContain('DATABASE_URL');
    expect(issues).toContain('REDIS_URL');
    expect(issues).toContain('KAFKA_BROKERS');
  });

  it('treats empty strings as unset so defaults apply', () => {
    const config = loadConfig(schema, { ...validEnv, LOG_LEVEL: '' });
    expect(config.LOG_LEVEL).toBe('info');
  });

  it('rejects an unknown log level', () => {
    expect(() => loadConfig(schema, { ...validEnv, LOG_LEVEL: 'verbose' })).toThrow(ConfigError);
  });

  it('rejects an empty broker list', () => {
    expect(() => loadConfig(schema, { ...validEnv, KAFKA_BROKERS: ' , ' })).toThrow(ConfigError);
  });

  it('does not echo secret values in the error message', () => {
    try {
      loadConfig(schema, { ...validEnv, DATABASE_URL: 'mysql://user:supersecret@host/db' });
      expect.unreachable();
    } catch (error) {
      expect((error as Error).message).not.toContain('supersecret');
    }
  });
});

describe('loadEnvFile', () => {
  let dir: string | undefined;

  afterEach(() => {
    if (dir) rmSync(dir, { recursive: true, force: true });
    dir = undefined;
  });

  it('finds .env in a parent directory and does not override existing variables', () => {
    dir = mkdtempSync(join(tmpdir(), 'ticketsurge-env-'));
    const nested = join(dir, 'apps', 'api');
    mkdirSync(nested, { recursive: true });
    writeFileSync(join(dir, '.env'), 'FROM_FILE=file\nALREADY_SET=file\n');

    const env: Record<string, string | undefined> = { ALREADY_SET: 'real-env' };
    const loaded = loadEnvFile(nested, env);

    expect(loaded).toBe(join(dir, '.env'));
    expect(env.FROM_FILE).toBe('file');
    expect(env.ALREADY_SET).toBe('real-env');
  });
});
