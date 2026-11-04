-- Transactional outbox for the booking service. A booking and its booking.requested message are
-- written in the same transaction; the relay in booking-worker publishes rows to Kafka afterwards.

CREATE TABLE booking.outbox (
  id           BIGSERIAL PRIMARY KEY,
  message_id   UUID NOT NULL UNIQUE,
  topic        TEXT NOT NULL,
  message_key  TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

-- The relay only ever scans unpublished rows; keep that scan small as the table grows.
CREATE INDEX booking_outbox_unpublished_idx ON booking.outbox (id) WHERE published_at IS NULL;
