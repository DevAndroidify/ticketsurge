import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { DATA_SCHEMAS, type EventData } from './schemas.js';
import type { EventType } from './topics.js';

export const ENVELOPE_VERSION = 1;

function envelopeSchema<T extends EventType>(type: T) {
  return z.object({
    // Deduplicates Kafka messages: consumers record it in processed_events.
    messageId: z.uuid(),
    type: z.literal(type),
    version: z.literal(ENVELOPE_VERSION),
    occurredAt: z.iso.datetime({ offset: true }),
    traceId: z.string().min(1).max(200),
    data: DATA_SCHEMAS[type],
  });
}

export interface Envelope<T extends EventType = EventType> {
  messageId: string;
  type: T;
  version: typeof ENVELOPE_VERSION;
  occurredAt: string;
  traceId: string;
  data: EventData<T>;
}

export type AnyEnvelope = { [T in EventType]: Envelope<T> }[EventType];

const anyEnvelopeSchema = z.discriminatedUnion('type', [
  envelopeSchema('booking.requested'),
  envelopeSchema('payment.completed'),
  envelopeSchema('booking.confirmed'),
  envelopeSchema('booking.cancelled'),
  envelopeSchema('payment.refund-requested'),
  envelopeSchema('ticket.issued'),
]);

/** Builds and validates an envelope. Throws if `data` does not match the type's schema. */
export function createEnvelope<T extends EventType>(
  type: T,
  data: EventData<T>,
  options: { traceId: string; messageId?: string; occurredAt?: Date },
): Envelope<T> {
  const envelope = {
    messageId: options.messageId ?? randomUUID(),
    type,
    version: ENVELOPE_VERSION,
    occurredAt: (options.occurredAt ?? new Date()).toISOString(),
    traceId: options.traceId,
    data,
  };
  return envelopeSchema(type).parse(envelope) as Envelope<T>;
}

/** Validates any incoming message. Throws a ZodError for malformed or unknown messages. */
export function parseEnvelope(value: unknown): AnyEnvelope {
  return anyEnvelopeSchema.parse(value);
}

/** Every booking and payment message is keyed by bookingId (see topics.ts). */
export function messageKey(envelope: AnyEnvelope): string {
  return envelope.data.bookingId;
}
