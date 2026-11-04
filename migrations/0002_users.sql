-- Booking service users. Emails are stored lowercased by the API, so a plain unique index is enough.

CREATE TABLE booking.users (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email         TEXT NOT NULL CHECK (email = lower(email) AND length(email) BETWEEN 3 AND 254),
  -- Self-describing scrypt hash: scrypt$N$r$p$salt$hash (see apps/api/src/modules/auth/passwords.ts).
  password_hash TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT users_email_unique UNIQUE (email)
);
