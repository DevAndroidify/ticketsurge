-- Bookings and the seats they claim. PostgreSQL makes the final decision on who gets a seat:
-- Redis holds only filter traffic, and the partial unique index below is the real guarantee.

CREATE TABLE booking.bookings (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id        UUID NOT NULL REFERENCES booking.events (id),
  user_id         UUID NOT NULL REFERENCES booking.users (id),
  hold_id         UUID NOT NULL,
  status          TEXT NOT NULL
                  CHECK (status IN ('PENDING_PAYMENT', 'CONFIRMED', 'CANCELLED', 'EXPIRED')),
  amount_minor    BIGINT NOT NULL CHECK (amount_minor >= 0),
  idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
  expires_at      TIMESTAMPTZ NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- A retried POST /bookings with the same key returns the same booking instead of a new one.
  CONSTRAINT bookings_user_idempotency_unique UNIQUE (user_id, idempotency_key),
  -- One hold can become at most one booking.
  CONSTRAINT bookings_hold_id_unique UNIQUE (hold_id),
  -- Target for booking_seats' composite foreign key.
  CONSTRAINT bookings_id_event_unique UNIQUE (id, event_id)
);

CREATE TABLE booking.booking_seats (
  booking_id UUID NOT NULL,
  event_id   UUID NOT NULL,
  seat_id    UUID NOT NULL,
  -- Set to false in the same transaction that cancels or expires the booking, which frees the seat.
  active     BOOLEAN NOT NULL DEFAULT true,
  PRIMARY KEY (booking_id, seat_id),
  -- The seat must belong to the same event as the booking.
  FOREIGN KEY (booking_id, event_id) REFERENCES booking.bookings (id, event_id),
  FOREIGN KEY (event_id, seat_id) REFERENCES booking.seats (event_id, id)
);

-- The core guarantee: a seat can be claimed by at most one active booking, no matter what Redis says.
CREATE UNIQUE INDEX one_active_booking_per_seat
  ON booking.booking_seats (event_id, seat_id)
  WHERE active = true;
