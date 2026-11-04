import { Writable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { createLogger } from './logger.js';
import { runWithTrace } from './trace.js';

function capture() {
  const lines: Record<string, unknown>[] = [];
  const destination = new Writable({
    write(chunk: Buffer, _encoding, callback) {
      lines.push(JSON.parse(chunk.toString()) as Record<string, unknown>);
      callback();
    },
  });
  return { lines, logger: createLogger({ service: 'test', level: 'info', destination }) };
}

describe('createLogger', () => {
  it('adds traceId and messageId inside runWithTrace, including across awaits', async () => {
    const { lines, logger } = capture();
    logger.info('outside');
    await runWithTrace({ traceId: 'trace-1', messageId: 'msg-1' }, async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      logger.child({ component: 'x' }).info('inside');
    });
    expect(lines[0]).not.toHaveProperty('traceId');
    expect(lines[1]).toMatchObject({
      service: 'test',
      traceId: 'trace-1',
      messageId: 'msg-1',
      msg: 'inside',
    });
  });

  it('redacts passwords and authorization headers', () => {
    const { lines, logger } = capture();
    logger.info({ password: 'secret', req: { headers: { authorization: 'Bearer x' } } }, 'r');
    expect(JSON.stringify(lines[0])).not.toContain('secret');
    expect(JSON.stringify(lines[0])).not.toContain('Bearer x');
  });
});
