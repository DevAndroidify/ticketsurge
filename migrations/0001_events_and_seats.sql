-- Booking service schema: events and their seat maps.
-- api and booking-worker share this schema; other services never read it directly.

CREATE SCHEMA IF NOT EXISTS booking;

CREATE TABLE booking.events (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name           TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  venue          TEXT NOT NULL CHECK (length(venue) BETWEEN 1 AND 200),
  starts_at      TIMESTAMPTZ NOT NULL,
  sale_starts_at TIMESTAMPTZ NOT NULL,
  -- DRAFT: hidden. PUBLISHED: visible, sellable once sale_starts_at passes. CANCELLED: visible, not sellable.
  status         TEXT NOT NULL DEFAULT 'PUBLISHED'
                 CHECK (status IN ('DRAFT', 'PUBLISHED', 'CANCELLED')),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (sale_starts_at <= starts_at)
);

CREATE INDEX events_starts_at_idx ON booking.events (starts_at);

CREATE TABLE booking.seats (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id     UUID NOT NULL REFERENCES booking.events (id) ON DELETE CASCADE,
  section      TEXT NOT NULL CHECK (length(section) BETWEEN 1 AND 50),
  row_label    TEXT NOT NULL CHECK (length(row_label) BETWEEN 1 AND 10),
  seat_number  INTEGER NOT NULL CHECK (seat_number > 0),
  -- Money is stored in minor units (cents) to avoid floating point rounding.
  price_minor  INTEGER NOT NULL CHECK (price_minor >= 0),
  -- A physical seat position exists once per event.
  CONSTRAINT seats_position_unique UNIQUE (event_id, section, row_label, seat_number),
  -- Lets booking_seats reference (event_id, seat_id) so a booked seat always belongs to its event.
  CONSTRAINT seats_event_seat_unique UNIQUE (event_id, id)
);
