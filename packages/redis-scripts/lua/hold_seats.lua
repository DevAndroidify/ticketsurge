-- holdSeats: hold every requested seat, or change nothing.
-- Runs atomically inside Redis, so two users can never both see a seat as free.
--
-- KEYS[1]    admit:{eventId}:{userId}   admission marker
-- KEYS[2]    hold:{eventId}:{holdId}    hold hash (owner, seat IDs)
-- KEYS[3..n] seat:{eventId}:{seatId}    one per requested seat, same order as ARGV[4..n]
-- ARGV[1]    holdId
-- ARGV[2]    userId
-- ARGV[3]    TTL in milliseconds
-- ARGV[4..n] seat IDs
--
-- Returns {'OK'} | {'NOT_ADMITTED'} | {'CONFLICT', seatId, ...}

-- Only users let out of the virtual queue may hold seats.
if redis.call('EXISTS', KEYS[1]) == 0 then
  return { 'NOT_ADMITTED' }
end

-- Check every seat before writing anything, so a partial hold is impossible.
local conflicts = { 'CONFLICT' }
for i = 3, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    conflicts[#conflicts + 1] = ARGV[i + 1]
  end
end
if #conflicts > 1 then
  return conflicts
end

local value = 'hold:' .. ARGV[1]
local ttl = tonumber(ARGV[3])
for i = 3, #KEYS do
  redis.call('SET', KEYS[i], value, 'PX', ttl)
end

-- The hash shares the seats' TTL, so it disappears together with them.
redis.call('HSET', KEYS[2], 'owner', ARGV[2], 'seats', table.concat(ARGV, ',', 4))
redis.call('PEXPIRE', KEYS[2], ttl)
return { 'OK' }
