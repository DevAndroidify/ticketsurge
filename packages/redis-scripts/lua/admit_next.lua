-- admitNext: move the next N users from the front of the queue to admitted.
-- Popping and admitting happen in one step, so a user is never lost between the two.
--
-- KEYS[1] queue:{eventId}
-- ARGV[1] eventId
-- ARGV[2] N, the maximum number of users to admit
-- ARGV[3] admission TTL in milliseconds
--
-- The admit:{eventId}:{userId} keys depend on who is popped, so they cannot be declared in KEYS.
-- They share the {eventId} hash tag with KEYS[1], so they live in the same cluster slot.
--
-- Returns the admitted user IDs in queue order.

local popped = redis.call('ZPOPMIN', KEYS[1], tonumber(ARGV[2]))
local admitted = {}
local ttl = tonumber(ARGV[3])
for i = 1, #popped, 2 do
  local userId = popped[i]
  redis.call('SET', 'admit:{' .. ARGV[1] .. '}:' .. userId, '1', 'PX', ttl)
  admitted[#admitted + 1] = userId
end
return admitted
