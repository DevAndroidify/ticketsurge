-- Booking saga deduplication, and the payment service's own schema.

-- Messages the booking saga has applied. Claimed in the same transaction as the status change,
-- so a redelivered payment.completed is never applied twice.
CREATE TABLE booking.processed_events (
  consumer     TEXT NOT NULL,
  message_id   UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);

-- The payment service owns this schema. It never joins to booking.*; it learns about bookings only
-- from booking.requested messages.
CREATE SCHEMA IF NOT EXISTS payment;

CREATE TABLE payment.payments (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- One payment attempt per booking, however often booking.requested is delivered.
  booking_id   UUID NOT NULL UNIQUE,
  status       TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
  amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
  provider_ref TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payment.outbox (
  id           BIGSERIAL PRIMARY KEY,
  message_id   UUID NOT NULL UNIQUE,
  topic        TEXT NOT NULL,
  message_key  TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX payment_outbox_unpublished_idx ON payment.outbox (id) WHERE published_at IS NULL;

CREATE TABLE payment.processed_events (
  consumer     TEXT NOT NULL,
  message_id   UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);
