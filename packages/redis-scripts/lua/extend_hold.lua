-- extendHold: reset the TTL of a hold, but only if the user still owns every one of its seats.
--
-- KEYS[1]    hold:{eventId}:{holdId}
-- KEYS[2..n] seat:{eventId}:{seatId} for every seat in the hold
-- ARGV[1]    holdId
-- ARGV[2]    userId
-- ARGV[3]    new TTL in milliseconds (replaces the old one; it may be shorter)
-- ARGV[4]    comma-separated seat IDs the caller read from the hash
--
-- Returns 1 if extended, 0 if the hold is gone, belongs to someone else, or lost a seat.

local hold = redis.call('HMGET', KEYS[1], 'owner', 'seats')
if hold[1] ~= ARGV[2] then
  return 0
end
-- Guards against a caller passing a different seat list than the hold actually has.
if hold[2] ~= ARGV[4] then
  return 0
end

local value = 'hold:' .. ARGV[1]
for i = 2, #KEYS do
  if redis.call('GET', KEYS[i]) ~= value then
    return 0
  end
end

local ttl = tonumber(ARGV[3])
for i = 2, #KEYS do
  redis.call('PEXPIRE', KEYS[i], ttl)
end
redis.call('PEXPIRE', KEYS[1], ttl)
return 1
