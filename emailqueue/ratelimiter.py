"""Cluster-wide sliding-window rate limiter built on a Redis sorted set.

Why not Celery's built-in task rate_limit: it is enforced per worker process, so
N workers would send N x 200/min. The limit must live in one shared place.
"""
import logging
from dataclasses import dataclass
from functools import lru_cache

import redis
from django.conf import settings

logger = logging.getLogger(__name__)

# Runs atomically inside Redis: no other command can interleave between the
# count and the add, so two workers can never both take "the 200th slot".
# Uses Redis server TIME so every worker shares one clock.
SLIDING_WINDOW_LUA = """
local key       = KEYS[1]
local limit     = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local member    = ARGV[3]

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- 1. Drop admissions that have left the window (score <= now - window).
redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - window_ms)

-- 2. Admit if there is room.
if redis.call('ZCARD', key) < limit then
  redis.call('ZADD', key, now_ms, member)
  redis.call('PEXPIRE', key, window_ms)   -- idle keys clean themselves up
  return {1, now_ms, 0}
end

-- 3. Full: tell the caller when the oldest admission expires.
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local retry_after_ms = tonumber(oldest[2]) + window_ms - now_ms
return {0, now_ms, retry_after_ms}
"""

# When Redis is unreachable we cannot know how many emails were sent, so we
# fail CLOSED (refuse) and ask the caller to try again after this delay.
FAIL_CLOSED_RETRY_MS = 5_000


@dataclass(frozen=True)
class Decision:
    allowed: bool
    admitted_at_ms: int | None  # Redis server time of the admission (when allowed)
    retry_after_ms: int         # how long to wait before trying again (when refused)


class SlidingWindowRateLimiter:
    def __init__(self, client: redis.Redis, key: str, limit: int, window_seconds: float):
        self.client = client
        self.key = key
        self.limit = limit
        self.window_ms = int(window_seconds * 1000)
        self._script = client.register_script(SLIDING_WINDOW_LUA)

    def acquire(self, member: str) -> Decision:
        """Try to take one slot. `member` must be unique per attempt (it is the
        sorted-set member; a repeated member would overwrite, not count)."""
        try:
            allowed, now_ms, retry_after_ms = self._script(
                keys=[self.key], args=[self.limit, self.window_ms, member]
            )
        except redis.RedisError:
            logger.exception("Rate limiter unavailable - failing closed")
            return Decision(False, None, FAIL_CLOSED_RETRY_MS)
        return Decision(bool(allowed), int(now_ms) if allowed else None, int(retry_after_ms))


@lru_cache(maxsize=8)
def _client(url: str) -> redis.Redis:
    # One connection pool per URL, reused across tasks in the worker process.
    return redis.Redis.from_url(url, socket_timeout=5, socket_connect_timeout=5)


def get_email_rate_limiter() -> SlidingWindowRateLimiter:
    """Built from settings on each call so tests can override the limit."""
    return SlidingWindowRateLimiter(
        client=_client(settings.REDIS_URL),
        key=settings.EMAIL_RATE_LIMIT_KEY,
        limit=settings.EMAIL_RATE_LIMIT,
        window_seconds=settings.EMAIL_RATE_WINDOW_SECONDS,
    )
