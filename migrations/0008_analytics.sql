-- Analytics service: sales totals per event, derived only from Kafka events. The table can be
-- emptied and rebuilt by replaying the topics (npm run analytics:rebuild).

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE analytics.sales_by_event (
  event_id           UUID PRIMARY KEY,
  confirmed_bookings INTEGER NOT NULL DEFAULT 0,
  seats_sold         INTEGER NOT NULL DEFAULT 0,
  revenue_minor      BIGINT NOT NULL DEFAULT 0,
  cancelled_bookings INTEGER NOT NULL DEFAULT 0,
  expired_bookings   INTEGER NOT NULL DEFAULT 0,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Totals are incremented, so each message must be applied exactly once.
CREATE TABLE analytics.processed_events (
  consumer     TEXT NOT NULL,
  message_id   UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);
