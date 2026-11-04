-- releaseHold: free the seats of one hold. A seat key is deleted only while its value is still
-- hold:<holdId>, so this can never free a seat that was re-held by someone else or sold.
-- Safe to run repeatedly.
--
-- KEYS[1]    hold:{eventId}:{holdId}
-- KEYS[2..n] seat:{eventId}:{seatId}
-- ARGV[1]    holdId
-- ARGV[2]    owner userId to enforce, or '' to skip the check (system releases)
--
-- Returns the number of seats released, or -1 if the hold belongs to another user.

if ARGV[2] ~= '' then
  local owner = redis.call('HGET', KEYS[1], 'owner')
  if owner and owner ~= ARGV[2] then
    return -1
  end
end

local value = 'hold:' .. ARGV[1]
local released = 0
for i = 2, #KEYS do
  if redis.call('GET', KEYS[i]) == value then
    redis.call('DEL', KEYS[i])
    released = released + 1
  end
end
redis.call('DEL', KEYS[1])
return released
