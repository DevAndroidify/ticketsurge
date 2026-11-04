-- markSold: record a confirmed booking's seats as sold.
-- Each seat value becomes sold:<bookingId> with no TTL (a plain SET removes any expiry), and the
-- hold hash is deleted. Running it again gives the same result, so redelivered messages are harmless.
--
-- The seats are overwritten even if Redis shows a different hold: PostgreSQL already confirmed this
-- booking, and its partial unique index guarantees no other active booking has these seats.
--
-- KEYS[1]    hold:{eventId}:{holdId}
-- KEYS[2..n] seat:{eventId}:{seatId}
-- ARGV[1]    bookingId
--
-- Returns the number of seats marked.

local value = 'sold:' .. ARGV[1]
for i = 2, #KEYS do
  redis.call('SET', KEYS[i], value)
end
redis.call('DEL', KEYS[1])
return #KEYS - 1
