import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createEnvelope, messageKey, parseEnvelope } from './envelope.js';
import { TOPICS, dlqTopicName, topicName } from './topics.js';

const confirmed = {
  bookingId: randomUUID(),
  eventId: randomUUID(),
  userId: randomUUID(),
  holdId: randomUUID(),
  seatIds: [randomUUID()],
  amountMinor: 1500,
};

describe('envelopes', () => {
  it('creates a valid envelope and parses it back', () => {
    const envelope = createEnvelope(TOPICS.bookingConfirmed, confirmed, { traceId: 'trace-1' });
    expect(envelope).toMatchObject({ type: 'booking.confirmed', version: 1, traceId: 'trace-1' });
    expect(envelope.messageId).toMatch(/^[0-9a-f-]{36}$/);
    const parsed = parseEnvelope(JSON.parse(JSON.stringify(envelope)));
    expect(parsed).toEqual(envelope);
    expect(messageKey(parsed)).toBe(confirmed.bookingId);
  });

  it('rejects data that does not match the type', () => {
    expect(() =>
      createEnvelope(TOPICS.bookingConfirmed, { ...confirmed, seatIds: [] }, { traceId: 't' }),
    ).toThrow();
  });

  it.each([
    ['not an object', 'hello'],
    ['unknown type', { type: 'booking.exploded' }],
    ['wrong version', { type: 'booking.confirmed', version: 2 }],
    ['missing data', { type: 'booking.confirmed', version: 1, messageId: randomUUID() }],
  ])('parseEnvelope rejects %s', (_label, value) => {
    expect(() => parseEnvelope(value)).toThrow();
  });

  it('rejects a payment.completed with an unknown status', () => {
    const bad = {
      messageId: randomUUID(),
      type: 'payment.completed',
      version: 1,
      occurredAt: new Date().toISOString(),
      traceId: 't',
      data: {
        bookingId: randomUUID(),
        paymentId: randomUUID(),
        status: 'MAYBE',
        amountMinor: 1,
        providerRef: null,
      },
    };
    expect(() => parseEnvelope(bad)).toThrow();
  });
});

describe('topic names', () => {
  it('applies the prefix to topics and dead-letter topics', () => {
    expect(topicName('', TOPICS.bookingRequested)).toBe('booking.requested');
    expect(topicName('test.x.', TOPICS.bookingRequested)).toBe('test.x.booking.requested');
    expect(dlqTopicName('', 'payment-service')).toBe('dlq.payment-service');
  });
});
