package messaging

import "github.com/redis/go-redis/v9"

var submitScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if (inbox_type ~= 'none' and inbox_type ~= 'hash') or
   (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
local existing = redis.call('HGET', KEYS[1], 'task_id')
if existing then
  if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return -1 end
  return 0
end
redis.call('HSET', KEYS[1],
  'task_id', ARGV[1], 'digest', ARGV[2], 'payload', ARGV[3],
  'state', 'queued', 'attempt', ARGV[4], 'trace_id', ARGV[5], 'inbox_id', ARGV[6])
local stream_id = redis.call('XADD', KEYS[2], '*', 'payload', ARGV[3], 'inbox_id', ARGV[6])
redis.call('HSET', KEYS[1], 'stream_id', stream_id)
return 1
`)

var beginScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if inbox_type ~= 'hash' then return {-9, 0} end
if redis.call('HGET', KEYS[1], 'state') ~= 'queued' then return {-1, 0} end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return {-2, 0} end
if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return {-2, 0} end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return {-2, 0} end
if tonumber(redis.call('HGET', KEYS[1], 'attempt')) ~= tonumber(ARGV[5]) then return {-2, 0} end
local epoch = redis.call('HINCRBY', KEYS[1], 'lease_epoch', 1)
redis.call('HSET', KEYS[1], 'state', 'processing', 'owner', ARGV[3],
  'lease_until', ARGV[6], 'last_error', '')
return {1, epoch}
`)

var beginFailureScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if inbox_type ~= 'hash' or (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'queued' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return 0 end
local current_stream = redis.call('HGET', KEYS[1], 'stream_id')
if current_stream == ARGV[3] then
  local payload = redis.call('HGET', KEYS[1], 'payload')
  local inbox_id = redis.call('HGET', KEYS[1], 'inbox_id')
  if not payload or not inbox_id then return 0 end
  local new_stream = redis.call('XADD', KEYS[2], '*', 'payload', payload, 'inbox_id', inbox_id)
  redis.call('HSET', KEYS[1], 'stream_id', new_stream)
end
redis.call('XDEL', KEYS[2], ARGV[3])
redis.call('XACK', KEYS[2], ARGV[4], ARGV[3])
return 1
`)

var heartbeatScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if inbox_type ~= 'hash' or (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch')) ~= tonumber(ARGV[3]) then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
redis.call('HSET', KEYS[1], 'lease_until', ARGV[5])
redis.call('XCLAIM', KEYS[2], ARGV[6], ARGV[2], 0, ARGV[4], 'JUSTID')
return 1
`)

var retryScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local retry_type = redis.call('TYPE', KEYS[2])
local stream_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(retry_type) == 'table' then retry_type = retry_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if inbox_type ~= 'hash' or
   (retry_type ~= 'none' and retry_type ~= 'zset') or
   (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch')) ~= tonumber(ARGV[3]) then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
redis.call('HSET', KEYS[1], 'state', 'retry_wait', 'attempt', ARGV[5],
  'payload', ARGV[6], 'next_attempt_at', ARGV[7], 'last_error', ARGV[8])
redis.call('HDEL', KEYS[1], 'owner', 'lease_until', 'stream_id')
redis.call('ZADD', KEYS[2], ARGV[7], KEYS[1])
redis.call('XDEL', KEYS[3], ARGV[4])
redis.call('XACK', KEYS[3], ARGV[9], ARGV[4])
return 1
`)

var promoteScript = redis.NewScript(`
local retry_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(retry_type) == 'table' then retry_type = retry_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if (retry_type ~= 'none' and retry_type ~= 'zset') or
   (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, inbox in ipairs(due) do
  local inbox_type = redis.call('TYPE', inbox)
  if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
  if inbox_type ~= 'hash' then return -9 end
end
local promoted = 0
for _, inbox in ipairs(due) do
  local state = redis.call('HGET', inbox, 'state')
  local next_at = tonumber(redis.call('HGET', inbox, 'next_attempt_at') or '0')
  local payload = redis.call('HGET', inbox, 'payload')
  local inbox_id = redis.call('HGET', inbox, 'inbox_id')
  if state == 'retry_wait' and next_at <= tonumber(ARGV[1]) and payload and inbox_id then
    local stream_id = redis.call('XADD', KEYS[2], '*', 'payload', payload, 'inbox_id', inbox_id)
    redis.call('HSET', inbox, 'state', 'queued', 'stream_id', stream_id)
    redis.call('HDEL', inbox, 'next_attempt_at')
    promoted = promoted + 1
  end
  redis.call('ZREM', KEYS[1], inbox)
end
return promoted
`)

var completeScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local reply_type = redis.call('TYPE', KEYS[2])
local task_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(reply_type) == 'table' then reply_type = reply_type.ok end
if type(task_type) == 'table' then task_type = task_type.ok end
if inbox_type ~= 'hash' or
   (reply_type ~= 'none' and reply_type ~= 'stream') or
   (task_type ~= 'none' and task_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch')) ~= tonumber(ARGV[3]) then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
redis.call('HSET', KEYS[1], 'state', 'succeeded', 'result', ARGV[5], 'error_code', '')
redis.call('HDEL', KEYS[1], 'payload', 'owner', 'lease_until', 'next_attempt_at')
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[5])
redis.call('XDEL', KEYS[3], ARGV[4])
redis.call('XACK', KEYS[3], ARGV[7], ARGV[4])
return 1
`)

var failScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local reply_type = redis.call('TYPE', KEYS[2])
local task_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(reply_type) == 'table' then reply_type = reply_type.ok end
if type(task_type) == 'table' then task_type = task_type.ok end
if inbox_type ~= 'hash' or
   (reply_type ~= 'none' and reply_type ~= 'stream') or
   (task_type ~= 'none' and task_type ~= 'stream') then return -9 end
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'processing' then
  if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
  if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
  if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch')) ~= tonumber(ARGV[3]) then return 0 end
  if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
elseif state == 'queued' then
  if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
  if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
else
  return 0
end
redis.call('HSET', KEYS[1], 'state', 'failed_terminal', 'result', ARGV[5], 'error_code', ARGV[6])
redis.call('HDEL', KEYS[1], 'payload', 'owner', 'lease_until', 'next_attempt_at')
redis.call('EXPIRE', KEYS[1], ARGV[7])
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[5])
redis.call('XDEL', KEYS[3], ARGV[4])
redis.call('XACK', KEYS[3], ARGV[8], ARGV[4])
return 1
`)

var rejectScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local reply_type = redis.call('TYPE', KEYS[2])
local task_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(reply_type) == 'table' then reply_type = reply_type.ok end
if type(task_type) == 'table' then task_type = task_type.ok end
if inbox_type ~= 'hash' or
   (reply_type ~= 'none' and reply_type ~= 'stream') or
   (task_type ~= 'none' and task_type ~= 'stream') then return -9 end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'processing' and state ~= 'queued' then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'state', 'failed_terminal', 'result', ARGV[2], 'error_code', ARGV[3])
redis.call('HDEL', KEYS[1], 'payload', 'owner', 'lease_until', 'next_attempt_at')
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[2])
redis.call('XDEL', KEYS[3], ARGV[1])
redis.call('XACK', KEYS[3], ARGV[5], ARGV[1])
return 1
`)

var recoverScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local reply_type = redis.call('TYPE', KEYS[2])
local task_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(reply_type) == 'table' then reply_type = reply_type.ok end
if type(task_type) == 'table' then task_type = task_type.ok end
if inbox_type ~= 'hash' or
   (reply_type ~= 'none' and reply_type ~= 'stream') or
   (task_type ~= 'none' and task_type ~= 'stream') then return -9 end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'processing' and state ~= 'queued' then
  redis.call('XDEL', KEYS[3], ARGV[2])
  redis.call('XACK', KEYS[3], ARGV[8], ARGV[2])
  return 0
end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return -1 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[2] then return -1 end
if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[10] then return -1 end
if tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0') ~= tonumber(ARGV[11]) then return -1 end
if state == 'processing' and tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') > tonumber(ARGV[3]) then return -2 end
if tonumber(ARGV[4]) > tonumber(ARGV[5]) then
  redis.call('HSET', KEYS[1], 'state', 'failed_terminal', 'result', ARGV[7], 'error_code', 'worker_lost')
  redis.call('HDEL', KEYS[1], 'payload', 'owner', 'lease_until', 'next_attempt_at')
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  redis.call('XADD', KEYS[2], '*', 'payload', ARGV[7])
else
  local new_id = redis.call('XADD', KEYS[3], '*', 'payload', ARGV[9], 'inbox_id', ARGV[12])
  redis.call('HSET', KEYS[1], 'state', 'queued', 'attempt', ARGV[4], 'payload', ARGV[9], 'stream_id', new_id, 'last_error', 'worker_lost')
  redis.call('HDEL', KEYS[1], 'owner', 'lease_until', 'next_attempt_at')
end
redis.call('XDEL', KEYS[3], ARGV[2])
redis.call('XACK', KEYS[3], ARGV[8], ARGV[2])
return 1
`)

var ackReplyScript = redis.NewScript(`
local stream_type = redis.call('TYPE', KEYS[1])
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if stream_type ~= 'none' and stream_type ~= 'stream' then return -9 end
redis.call('XDEL', KEYS[1], ARGV[2])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
`)
