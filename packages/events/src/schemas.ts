import { z } from 'zod';
import { TOPICS } from './topics.js';

const uuid = z.uuid();
const minor = z.number().int().nonnegative();

export const bookingRequestedData = z.object({
  bookingId: uuid,
  eventId: uuid,
  userId: uuid,
  holdId: uuid,
  seatIds: z.array(uuid).min(1),
  amountMinor: minor,
  expiresAt: z.iso.datetime({ offset: true }),
});

export const paymentCompletedData = z.object({
  bookingId: uuid,
  paymentId: uuid,
  status: z.enum(['SUCCEEDED', 'FAILED']),
  amountMinor: minor,
  providerRef: z.string().nullable(),
});

/** Shared by booking.confirmed and booking.cancelled so downstream services need no lookups. */
const bookingOutcomeBase = {
  bookingId: uuid,
  eventId: uuid,
  userId: uuid,
  holdId: uuid,
  seatIds: z.array(uuid).min(1),
  amountMinor: minor,
};

export const bookingConfirmedData = z.object(bookingOutcomeBase);

export const bookingCancelledData = z.object({
  ...bookingOutcomeBase,
  reason: z.enum(['PAYMENT_FAILED', 'EXPIRED']),
});

export const refundRequestedData = z.object({
  bookingId: uuid,
  paymentId: uuid,
  amountMinor: minor,
  reason: z.enum(['PAID_AFTER_EXPIRY', 'BOOKING_NOT_PENDING']),
});

export const ticketIssuedData = z.object({
  bookingId: uuid,
  eventId: uuid,
  userId: uuid,
  tickets: z.array(z.object({ ticketId: uuid, seatId: uuid, code: z.string().min(1) })).min(1),
});

export const DATA_SCHEMAS = {
  [TOPICS.bookingRequested]: bookingRequestedData,
  [TOPICS.paymentCompleted]: paymentCompletedData,
  [TOPICS.bookingConfirmed]: bookingConfirmedData,
  [TOPICS.bookingCancelled]: bookingCancelledData,
  [TOPICS.refundRequested]: refundRequestedData,
  [TOPICS.ticketIssued]: ticketIssuedData,
} as const;

export type DataSchemas = typeof DATA_SCHEMAS;
export type EventData<T extends keyof DataSchemas> = z.infer<DataSchemas[T]>;
