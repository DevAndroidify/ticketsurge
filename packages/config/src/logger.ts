import { pino, type Logger, type LevelWithSilent } from 'pino';

export type { Logger };

export interface LoggerOptions {
  /** Service name, added to every log line so mixed logs stay readable. */
  service: string;
  level: LevelWithSilent;
}

/** Structured JSON logger. Pipe through `pino-pretty` for local reading. */
export function createLogger({ service, level }: LoggerOptions): Logger {
  return pino({
    level,
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
    // Never log credentials that may appear in config or request headers.
    redact: {
      paths: ['password', '*.password', 'req.headers.authorization', 'req.headers.cookie'],
      censor: '[redacted]',
    },
  });
}
