import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { PermanentError, isPermanent } from './consumer.js';

describe('isPermanent', () => {
  it('treats malformed input and PermanentError as not worth retrying', () => {
    expect(isPermanent(new PermanentError('bad'))).toBe(true);
    expect(isPermanent(z.object({ a: z.string() }).safeParse({}).error)).toBe(true);
    let syntax: unknown;
    try {
      JSON.parse('{');
    } catch (error) {
      syntax = error;
    }
    expect(isPermanent(syntax)).toBe(true);
  });

  it('retries everything else (timeouts, connection errors, deadlocks)', () => {
    expect(isPermanent(new Error('connection terminated'))).toBe(false);
    expect(isPermanent('string error')).toBe(false);
  });
});
