import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { parseEnv } from 'node:util';
import { z } from 'zod';

/** Thrown when environment variables fail validation. Lists every problem at once. */
export class ConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid configuration:\n${issues.map((issue) => `  - ${issue}`).join('\n')}`);
    this.name = 'ConfigError';
    this.issues = issues;
  }
}

type EnvSource = Record<string, string | undefined>;

/**
 * Validates environment variables against a Zod schema.
 * Empty strings are treated as unset so `FOO=` in .env falls back to the schema default.
 */
export function loadConfig<TSchema extends z.ZodType>(
  schema: TSchema,
  env: EnvSource = process.env,
): z.infer<TSchema> {
  const cleaned: EnvSource = {};
  for (const [key, value] of Object.entries(env)) {
    if (value !== undefined && value !== '') cleaned[key] = value;
  }

  const result = schema.safeParse(cleaned);
  if (!result.success) {
    const issues = result.error.issues.map((issue) => {
      const path = issue.path.join('.') || '(root)';
      return `${path}: ${issue.message}`;
    });
    throw new ConfigError(issues);
  }
  return result.data;
}

/**
 * Finds the nearest .env file walking up from `startDir` and copies its values into `env`.
 * Variables already set in the real environment win, so deployments can override .env.
 * Returns the file path that was loaded, or undefined when no .env exists (env vars only).
 */
export function loadEnvFile(
  startDir: string = process.cwd(),
  env: EnvSource = process.env,
): string | undefined {
  let dir = resolve(startDir);
  for (;;) {
    const candidate = join(dir, '.env');
    if (existsSync(candidate)) {
      const parsed = parseEnv(readFileSync(candidate, 'utf8'));
      for (const [key, value] of Object.entries(parsed)) {
        if (env[key] === undefined) env[key] = value;
      }
      return candidate;
    }
    const parent = dirname(dir);
    if (parent === dir) return undefined;
    dir = parent;
  }
}

// ---------- Reusable field schemas ----------

export const logLevelSchema = z
  .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
  .default('info');

export const nodeEnvSchema = z.enum(['development', 'test', 'production']).default('development');

export const portSchema = z.coerce.number().int().min(1).max(65535);

export const durationMsSchema = z.coerce.number().int().positive();

export const postgresUrlSchema = z
  .url()
  .refine(
    (value) => /^postgres(ql)?:\/\//.test(value),
    'must start with postgres:// or postgresql://',
  );

export const redisUrlSchema = z
  .url()
  .refine((value) => /^rediss?:\/\//.test(value), 'must start with redis:// or rediss://');

/** Comma-separated host:port list, e.g. "localhost:9092,broker2:9092". */
export const brokerListSchema = z
  .string()
  .transform((value) =>
    value
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
  )
  .pipe(
    z
      .array(z.string().regex(/^[^\s:]+:\d{1,5}$/, 'each broker must be host:port'))
      .min(1, 'at least one broker is required'),
  );
