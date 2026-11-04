import { describe, expect, it } from 'vitest';
import { TimeoutError, runHealthChecks, withTimeout } from './run-checks.js';

const never = () => new Promise<void>(() => undefined);
const pass = () => Promise.resolve();
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

describe('withTimeout', () => {
  it('returns the value when the promise settles in time', async () => {
    await expect(withTimeout(Promise.resolve(42), 50)).resolves.toBe(42);
  });

  it('rejects with TimeoutError when the promise hangs', async () => {
    await expect(withTimeout(never(), 20)).rejects.toBeInstanceOf(TimeoutError);
  });
});

describe('runHealthChecks', () => {
  it('reports ok when every check passes', async () => {
    const report = await runHealthChecks({ a: pass, b: pass }, 100);
    expect(report.status).toBe('ok');
    expect(report.checks.a?.status).toBe('up');
    expect(report.checks.b?.status).toBe('up');
  });

  it('reports unavailable with the error of the failing check only', async () => {
    const report = await runHealthChecks(
      {
        ok: pass,
        broken: () => Promise.reject(new Error('connection refused')),
      },
      100,
    );
    expect(report.status).toBe('unavailable');
    expect(report.checks.ok).toMatchObject({ status: 'up' });
    expect(report.checks.broken).toMatchObject({ status: 'down', error: 'connection refused' });
  });

  it('marks a hung check as down instead of hanging', async () => {
    const started = performance.now();
    const report = await runHealthChecks({ hung: never }, 50);
    expect(performance.now() - started).toBeLessThan(500);
    expect(report.checks.hung).toMatchObject({ status: 'down', error: 'timed out after 50ms' });
  });

  it('runs checks in parallel', async () => {
    const started = performance.now();
    await runHealthChecks({ a: () => sleep(100), b: () => sleep(100), c: () => sleep(100) }, 1000);
    // Sequential execution would take at least 300ms.
    expect(performance.now() - started).toBeLessThan(250);
  });

  it('handles non-Error rejections', async () => {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
    const report = await runHealthChecks({ odd: () => Promise.reject('boom') }, 100);
    expect(report.checks.odd).toMatchObject({ status: 'down', error: 'boom' });
  });
});
