/** A dependency check resolves when the dependency is usable and rejects otherwise. */
export type HealthCheck = () => Promise<void>;

export type CheckResult =
  { status: 'up'; latencyMs: number } | { status: 'down'; latencyMs: number; error: string };

export interface HealthReport {
  status: 'ok' | 'unavailable';
  checks: Record<string, CheckResult>;
}

export class TimeoutError extends Error {
  constructor(ms: number) {
    super(`timed out after ${ms}ms`);
    this.name = 'TimeoutError';
  }
}

/** Rejects with TimeoutError if `promise` has not settled within `ms`. */
export async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new TimeoutError(ms));
    }, ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Runs all checks in parallel, each bounded by `timeoutMs`, so one hung dependency
 * cannot make the health endpoint itself hang.
 */
export async function runHealthChecks(
  checks: Record<string, HealthCheck>,
  timeoutMs: number,
): Promise<HealthReport> {
  const entries = await Promise.all(
    Object.entries(checks).map(async ([name, check]): Promise<[string, CheckResult]> => {
      const started = performance.now();
      try {
        await withTimeout(check(), timeoutMs);
        return [name, { status: 'up', latencyMs: elapsed(started) }];
      } catch (error) {
        return [name, { status: 'down', latencyMs: elapsed(started), error: describe(error) }];
      }
    }),
  );

  const results = Object.fromEntries(entries);
  const healthy = entries.every(([, result]) => result.status === 'up');
  return { status: healthy ? 'ok' : 'unavailable', checks: results };
}

function elapsed(started: number): number {
  return Math.round(performance.now() - started);
}

function describe(error: unknown): string {
  if (error instanceof Error && error.message) return error.message;
  if (error instanceof AggregateError) return 'multiple errors';
  return String(error);
}
