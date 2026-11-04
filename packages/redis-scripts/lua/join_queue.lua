-- joinQueue: put a user in the event's waiting line once, and report where they stand.
-- Atomic, so a user who double-clicks gets one place, and the place is a strictly increasing
-- sequence number (first come, first served).
--
-- KEYS[1] queue:{eventId}           sorted set: member = userId, score = join sequence
-- KEYS[2] queue:{eventId}:seq       counter
-- KEYS[3] admit:{eventId}:{userId}  admission marker
-- ARGV[1] userId
--
-- Returns {1, ttlMs} if already admitted, otherwise {0, position} (1 = next in line).

if redis.call('EXISTS', KEYS[3]) == 1 then
  return { 1, redis.call('PTTL', KEYS[3]) }
end

local rank = redis.call('ZRANK', KEYS[1], ARGV[1])
if not rank then
  local seq = redis.call('INCR', KEYS[2])
  redis.call('ZADD', KEYS[1], 'NX', seq, ARGV[1])
  rank = redis.call('ZRANK', KEYS[1], ARGV[1])
end
return { 0, rank + 1 }
