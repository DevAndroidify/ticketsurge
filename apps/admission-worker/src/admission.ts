import type { Logger } from '@ticketsurge/config';
import { admitNext, keys, type QueueUpdatedMessage } from '@ticketsurge/redis-scripts';
import type { Redis } from 'ioredis';

export interface AdmissionSettings {
  batchSize: number;
  admissionTtlMs: number;
  intervalMs: number;
  /** Identifies this worker instance in the per-event lock. */
  instanceId: string;
}

/**
 * One admission round: for every event with a queue, admit the next batch of users.
 *
 * A per-event lock (SET NX PX) makes the configured rate hold even when several worker instances
 * run: only the instance that takes the lock admits for that event in this interval. The lock
 * expires slightly before the next round so timer drift cannot make a worker skip its turn.
 *
 * Returns the users admitted per event.
 */
export async function runAdmissionTick(
  redis: Redis,
  settings: AdmissionSettings,
): Promise<Map<string, string[]>> {
  const admittedByEvent = new Map<string, string[]>();
  const eventIds = await redis.smembers(keys.activeQueues);
  const lockMs = Math.max(1, Math.floor(settings.intervalMs * 0.9));

  for (const eventId of eventIds) {
    const locked = await redis.set(
      keys.admissionTick(eventId),
      settings.instanceId,
      'PX',
      lockMs,
      'NX',
    );
    if (locked !== 'OK') continue;
    const admitted = await admitNext(redis, eventId, settings.batchSize, settings.admissionTtlMs);
    if (admitted.length === 0) continue;
    admittedByEvent.set(eventId, admitted);
    // Tell each admitted user. Users still waiting poll GET /events/:id/queue/me for their
    // position; pushing a new position to everyone in a large queue every round would be costly.
    const pipeline = redis.pipeline();
    for (const userId of admitted) {
      const message: QueueUpdatedMessage = {
        type: 'queue.updated',
        eventId,
        position: 0,
        admitted: true,
      };
      pipeline.publish(keys.userChannel(userId), JSON.stringify(message));
    }
    await pipeline.exec();
  }
  return admittedByEvent;
}

/** Runs admission rounds until `signal` aborts. Rounds never overlap. */
export async function runAdmissionLoop(
  redis: Redis,
  settings: AdmissionSettings,
  logger: Logger,
  signal: AbortSignal,
  onAdmitted: (eventId: string, count: number) => void = () => undefined,
): Promise<void> {
  while (!signal.aborted) {
    const started = Date.now();
    try {
      // While Redis is (re)connecting there is nothing to do; the client reconnects on its own.
      if (!isReady(redis)) {
        logger.debug({ status: redis.status }, 'redis not ready, skipping admission round');
        await sleep(settings.intervalMs, signal);
        continue;
      }
      const admitted = await runAdmissionTick(redis, settings);
      for (const [eventId, users] of admitted) {
        logger.info({ eventId, admitted: users.length }, 'admitted users');
        onAdmitted(eventId, users.length);
      }
    } catch (error) {
      // Redis outages are expected during failure tests; keep trying on the next round.
      logger.warn({ err: error }, 'admission round failed');
    }
    const wait = Math.max(0, settings.intervalMs - (Date.now() - started));
    await sleep(wait, signal);
  }
}

const isReady = (redis: Redis) => redis.status === 'ready';

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
    function done() {
      clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    }
  });
}
