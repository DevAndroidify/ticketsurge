import {
  durationMsSchema,
  logLevelSchema,
  nodeEnvSchema,
  redisUrlSchema,
} from '@ticketsurge/config';
import { z } from 'zod';

export const admissionConfigSchema = z.object({
  NODE_ENV: nodeEnvSchema,
  LOG_LEVEL: logLevelSchema,
  REDIS_URL: redisUrlSchema,
  // Every interval, up to ADMISSION_BATCH_SIZE users per event are admitted.
  // Throughput per event = batch size / interval (default 50 users per second).
  ADMISSION_INTERVAL_MS: durationMsSchema.default(1000),
  ADMISSION_BATCH_SIZE: z.coerce.number().int().min(1).max(10_000).default(50),
  // How long an admitted user may start holding seats.
  ADMISSION_TTL_MS: durationMsSchema.default(300_000),
});

export type AdmissionConfig = z.infer<typeof admissionConfigSchema>;
