/**
 * Kafka topics. Every topic is keyed by bookingId, never by the show's eventId: a flash sale for one
 * show would otherwise send nearly every message to a single partition.
 */
export const TOPICS = {
  bookingRequested: 'booking.requested',
  paymentCompleted: 'payment.completed',
  bookingConfirmed: 'booking.confirmed',
  bookingCancelled: 'booking.cancelled',
  refundRequested: 'payment.refund-requested',
  ticketIssued: 'ticket.issued',
} as const;

export type EventType = (typeof TOPICS)[keyof typeof TOPICS];

export const ALL_EVENT_TYPES: readonly EventType[] = Object.values(TOPICS);

/**
 * Full topic name. The prefix is empty in normal use; tests set a unique prefix so they never mix
 * with development traffic on the shared local broker.
 */
export function topicName(prefix: string, type: EventType): string {
  return `${prefix}${type}`;
}

/** Dead-letter topic for a consumer group. */
export function dlqTopicName(prefix: string, consumerGroup: string): string {
  return `${prefix}dlq.${consumerGroup}`;
}

/** Consumer group names, prefixed like topics. */
export const CONSUMER_GROUPS = {
  paymentService: 'payment-service',
  bookingSaga: 'booking-saga',
  seatSync: 'seat-sync',
  ticketService: 'ticket-service',
  notificationService: 'notification-service',
  analytics: 'analytics',
} as const;
