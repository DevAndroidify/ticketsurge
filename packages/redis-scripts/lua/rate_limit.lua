-- rateLimit: fixed-window counter. The window starts at the first request and lasts ARGV[2] ms.
--
-- KEYS[1] rl:<route>:<userId>
-- ARGV[1] limit (requests per window)
-- ARGV[2] window in milliseconds
--
-- Returns {count, ttlMs}. The request is allowed when count <= limit.

local count = redis.call('INCR', KEYS[1])
local ttl = redis.call('PTTL', KEYS[1])
-- First hit, or a key that somehow lost its expiry: (re)start the window.
if count == 1 or ttl < 0 then
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
  ttl = tonumber(ARGV[2])
end
return { count, ttl }
