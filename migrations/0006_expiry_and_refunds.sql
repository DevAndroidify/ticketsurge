-- Expiry sweeper and refunds.

-- The sweeper repeatedly asks "which pending bookings are overdue?"; index exactly that.
CREATE INDEX bookings_pending_expiry_idx
  ON booking.bookings (expires_at)
  WHERE status = 'PENDING_PAYMENT';

-- A payment that succeeded after its booking expired is refunded (the seats are not restored).
ALTER TABLE payment.payments DROP CONSTRAINT payments_status_check;
ALTER TABLE payment.payments
  ADD CONSTRAINT payments_status_check CHECK (status IN ('SUCCEEDED', 'FAILED', 'REFUNDED')),
  ADD COLUMN refunded_at TIMESTAMPTZ,
  ADD CONSTRAINT payments_refund_consistent CHECK ((status = 'REFUNDED') = (refunded_at IS NOT NULL));
