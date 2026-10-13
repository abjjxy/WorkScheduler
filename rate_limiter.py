"""
分布式限流器 —— 基于 Redis + Lua 脚本（原子无竞态）

两种算法：
  - SlidingWindowLimiter（滑动窗口）：HTTP API 用
  - TokenBucketLimiter（令牌桶）：TCP 连接用

所有逻辑用 Lua 脚本在 Redis 端原子执行，彻底解决并发竞态问题。
"""
import time
import uuid
from typing import Optional
from redis.asyncio import Redis
from config import settings


# ============================================================
# 滑动窗口限流器
# ============================================================

SLIDING_WINDOW_LUA = """
local key           = KEYS[1]
local window_start  = tonumber(ARGV[1])
local now_ms        = tonumber(ARGV[2])
local max_requests  = tonumber(ARGV[3])
local unique_id     = ARGV[4]
local ttl           = tonumber(ARGV[5])

redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

local current = redis.call('ZCARD', key)

if current < max_requests then
    redis.call('ZADD', key, now_ms, unique_id)
    redis.call('EXPIRE', key, ttl)
    return 1
else
    return 0
end
"""


class SlidingWindowLimiter:
    def __init__(self, redis: Redis, key_prefix: str = "rl:sw"):
        self.redis = redis
        self.key_prefix = key_prefix
        self._script_sha: Optional[str] = None

    async def _ensure_script(self) -> str:
        if self._script_sha is None:
            self._script_sha = await self.redis.script_load(SLIDING_WINDOW_LUA)
        return self._script_sha

    async def _eval(self, sha: str, key: str, *args) -> int:
        """evalsha 位置参数形式: evalsha(sha, numkeys, key1, arg1, arg2, ...)"""
        try:
            return await self.redis.evalsha(sha, 1, key, *args)
        except Exception:
            self._script_sha = await self.redis.script_load(SLIDING_WINDOW_LUA)
            return await self.redis.evalsha(self._script_sha, 1, key, *args)

    async def allow(self, resource: str,
                    window: int = None,
                    max_requests: int = None) -> bool:
        if not settings.RATE_LIMIT_ENABLED:
            return True

        window = window or settings.HTTP_RATE_WINDOW
        max_requests = max_requests or settings.HTTP_RATE_MAX_PER_WINDOW

        key = f"{self.key_prefix}:{resource}"
        now_ms = int(time.time() * 1000)
        window_start_ms = now_ms - window * 1000
        unique_id = f"{now_ms}:{uuid.uuid4().hex[:8]}"
        ttl = window + 1

        sha = await self._ensure_script()
        result = await self._eval(sha, key, window_start_ms, now_ms, max_requests, unique_id, ttl)
        return result == 1


# ============================================================
# 令牌桶限流器
# ============================================================

TOKEN_BUCKET_LUA = """
local key         = KEYS[1]
local capacity    = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now         = tonumber(ARGV[3])
local ttl         = tonumber(ARGV[4])

local data = redis.call('HGETALL', key)

local last_time
local tokens

if #data == 0 then
    last_time = now
    tokens = capacity
else
    local hash = {}
    for i = 1, #data, 2 do
        hash[data[i]] = data[i + 1]
    end
    last_time = tonumber(hash['last_time']) or now
    tokens = tonumber(hash['tokens']) or capacity

    local elapsed = now - last_time
    tokens = math.min(capacity, tokens + elapsed * refill_rate)
end

local allowed = 0
if tokens >= 1 then
    allowed = 1
    tokens = tokens - 1
else
    tokens = 0
end

redis.call('HSET', key, 'tokens', tokens, 'last_time', now)
redis.call('EXPIRE', key, ttl)

return allowed
"""


class TokenBucketLimiter:
    def __init__(self, redis: Redis, key_prefix: str = "rl:tb"):
        self.redis = redis
        self.key_prefix = key_prefix
        self._script_sha: Optional[str] = None

    async def _ensure_script(self) -> str:
        if self._script_sha is None:
            self._script_sha = await self.redis.script_load(TOKEN_BUCKET_LUA)
        return self._script_sha

    async def _eval(self, sha: str, key: str, *args) -> int:
        try:
            return await self.redis.evalsha(sha, 1, key, *args)
        except Exception:
            self._script_sha = await self.redis.script_load(TOKEN_BUCKET_LUA)
            return await self.redis.evalsha(self._script_sha, 1, key, *args)

    async def allow(self, resource: str,
                    capacity: int = None,
                    refill_rate: int = None) -> bool:
        if not settings.RATE_LIMIT_ENABLED:
            return True

        capacity = capacity or settings.TCP_RATE_BUCKET_CAPACITY
        refill_rate = refill_rate or settings.TCP_RATE_REFILL_RATE

        key = f"{self.key_prefix}:{resource}"
        now = time.time()
        ttl = max(10, int(capacity / refill_rate) + 10)

        sha = await self._ensure_script()
        result = await self._eval(sha, key, capacity, refill_rate, now, ttl)
        return result == 1