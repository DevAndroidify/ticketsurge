import { randomBytes } from 'node:crypto';
import type { Logger } from '@ticketsurge/config';
import { TOPICS, createEnvelope, topicName, type AnyEnvelope } from '@ticketsurge/events';
import { insertOutboxMessage, processOnce } from '@ticketsurge/outbox';
import type pg from 'pg';

export const CONSUMER_NAME = 'payment-service';

export interface PaymentSettings {
  topicPrefix: string;
  delayMs: number;
  failureRate: number;
  /** Injectable for deterministic tests. */
  random?: () => number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Handles messages for the payment service. booking.requested triggers a simulated charge:
 * wait `delayMs` (the provider call, outside any transaction), decide the outcome, then record the
 * payment and its payment.completed message in one transaction.
 *
 * Idempotency: processed_events stops a redelivered message, and payments.booking_id is unique, so
 * even a different message for the same booking cannot charge twice.
 */
export function createPaymentHandler(db: pg.Pool, settings: PaymentSettings, logger: Logger) {
  const random = settings.random ?? Math.random;

  async function charge(envelope: Extract<AnyEnvelope, { type: 'booking.requested' }>) {
    const { bookingId, amountMinor } = envelope.data;
    await sleep(settings.delayMs);
    const status = random() < settings.failureRate ? 'FAILED' : 'SUCCEEDED';
    const providerRef = status === 'SUCCEEDED' ? `sim_${randomBytes(8).toString('hex')}` : null;

    const applied = await processOnce(
      db,
      'payment.processed_events',
      CONSUMER_NAME,
      envelope.messageId,
      async (client) => {
        const { rows } = await client.query<{ id: string }>(
          `INSERT INTO payment.payments (booking_id, status, amount_minor, provider_ref)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (booking_id) DO NOTHING
           RETURNING id`,
          [bookingId, status, amountMinor, providerRef],
        );
        const payment = rows[0];
        if (!payment) {
          logger.info({ bookingId }, 'booking already has a payment, ignoring');
          return;
        }
        const completed = createEnvelope(
          TOPICS.paymentCompleted,
          { bookingId, paymentId: payment.id, status, amountMinor, providerRef },
          { traceId: envelope.traceId },
        );
        await insertOutboxMessage(
          client,
          'payment.outbox',
          topicName(settings.topicPrefix, TOPICS.paymentCompleted),
          completed,
        );
        logger.info({ bookingId, paymentId: payment.id, status }, 'payment processed');
      },
    );
    if (!applied) logger.debug({ messageId: envelope.messageId }, 'duplicate message ignored');
  }

  return async (envelope: AnyEnvelope): Promise<void> => {
    if (envelope.type === 'booking.requested') await charge(envelope);
  };
}
