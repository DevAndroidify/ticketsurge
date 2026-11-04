import { randomUUID } from 'node:crypto';
import type { Logger } from '@ticketsurge/config';
import { outboxBacklog } from '@ticketsurge/outbox';
import type pg from 'pg';
import { closePendingBooking } from './saga.js';

export interface SweeperSettings {
  topicPrefix: string;
  intervalMs: number;
  batchSize: number;
  /**
   * Pause while more than this many booking outbox rows are unpublished. If the relay is behind,
   * booking.requested may not even have reached the payment service, so an overdue booking may be
   * our fault, not the user's. Expiring it would take their seat because of our own outage.
   */
  maxOutboxBacklog: number;
}

export type SweepResult = { paused: true; backlog: number } | { paused: false; expired: string[] };

/**
 * Expires PENDING_PAYMENT bookings whose payment deadline passed: EXPIRED, seats deactivated, and
 * booking.cancelled (reason EXPIRED), all in one transaction. It uses the same conditional update
 * as the saga, so a payment result and the sweeper can never both win.
 */
export class ExpirySweeper {
  constructor(
    private readonly db: pg.Pool,
    private readonly settings: SweeperSettings,
    private readonly logger: Logger,
  ) {}

  async sweepOnce(): Promise<SweepResult> {
    const backlog = await outboxBacklog(this.db, 'booking.outbox');
    if (backlog > this.settings.maxOutboxBacklog) {
      this.logger.warn(
        { backlog, limit: this.settings.maxOutboxBacklog },
        'expiry sweeper paused: outbox backlog too large',
      );
      return { paused: true, backlog };
    }

    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      // SKIP LOCKED: rows the saga is working on right now are left for the next round.
      const due = await client.query<{ id: string }>(
        `SELECT id FROM booking.bookings
         WHERE status = 'PENDING_PAYMENT' AND expires_at <= now()
         ORDER BY expires_at
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [this.settings.batchSize],
      );
      const traceId = `sweeper:${randomUUID()}`;
      const expired: string[] = [];
      for (const { id } of due.rows) {
        const closed = await closePendingBooking(
          client,
          id,
          { status: 'EXPIRED', reason: 'EXPIRED' },
          this.settings.topicPrefix,
          traceId,
        );
        if (closed) expired.push(id);
      }
      await client.query('COMMIT');
      if (expired.length > 0)
        this.logger.info({ expired: expired.length }, 'expired overdue bookings');
      return { paused: false, expired };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      let wait = this.settings.intervalMs;
      try {
        const result = await this.sweepOnce();
        // A full batch means more may be overdue: go again right away.
        if (!result.paused && result.expired.length === this.settings.batchSize) wait = 0;
      } catch (error) {
        this.logger.warn({ err: error }, 'expiry sweep failed');
      }
      await sleep(wait, signal);
    }
  }
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
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
