import { pino, type DestinationStream, type Logger, type LevelWithSilent } from 'pino';
import { currentTrace } from './trace.js';

export type { Logger };

export interface LoggerOptions {
  /** Service name, added to every log line so mixed logs stay readable. */
  service: string;
  level: LevelWithSilent;
  /** Defaults to stdout. Tests pass a stream to capture lines. */
  destination?: DestinationStream;
}

/**
 * Structured JSON logger. Lines written inside runWithTrace() automatically include traceId and
 * messageId, so one booking can be followed through every service's logs with a single search.
 * Pipe through `pino-pretty` for local reading.
 */
export function createLogger({ service, level, destination }: LoggerOptions): Logger {
  return pino(
    {
      level,
      base: { service },
      timestamp: pino.stdTimeFunctions.isoTime,
      mixin: () => ({ ...currentTrace() }),
      // Never log credentials that may appear in config or request headers.
      redact: {
        paths: ['password', '*.password', 'req.headers.authorization', 'req.headers.cookie'],
        censor: '[redacted]',
      },
    },
    destination,
  );
}
