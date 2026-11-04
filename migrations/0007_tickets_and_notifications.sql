-- Ticket and notification services, each with its own schema. Neither reads booking.*: everything
-- they need arrives in booking.confirmed, ticket.issued, and booking.cancelled messages.

CREATE SCHEMA IF NOT EXISTS ticket;

CREATE TABLE ticket.tickets (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  booking_id UUID NOT NULL,
  event_id   UUID NOT NULL,
  seat_id    UUID NOT NULL,
  user_id    UUID NOT NULL,
  -- Printed/scanned at the door; random, so it cannot be guessed from other tickets.
  code       TEXT NOT NULL UNIQUE,
  issued_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- A seat gets one ticket per event, however often booking.confirmed is delivered or replayed.
  CONSTRAINT tickets_event_seat_unique UNIQUE (event_id, seat_id)
);
CREATE INDEX tickets_booking_idx ON ticket.tickets (booking_id);

CREATE TABLE ticket.outbox (
  id           BIGSERIAL PRIMARY KEY,
  message_id   UUID NOT NULL UNIQUE,
  topic        TEXT NOT NULL,
  message_key  TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX ticket_outbox_unpublished_idx ON ticket.outbox (id) WHERE published_at IS NULL;

CREATE TABLE ticket.processed_events (
  consumer     TEXT NOT NULL,
  message_id   UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);

CREATE SCHEMA IF NOT EXISTS notification;

CREATE TABLE notification.notifications (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id    UUID NOT NULL,
  booking_id UUID NOT NULL,
  type       TEXT NOT NULL CHECK (type IN ('TICKETS_ISSUED', 'BOOKING_CANCELLED', 'BOOKING_EXPIRED')),
  sent_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- A user is told about each outcome of a booking exactly once.
  CONSTRAINT notifications_booking_type_unique UNIQUE (booking_id, type)
);

CREATE TABLE notification.processed_events (
  consumer     TEXT NOT NULL,
  message_id   UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);
