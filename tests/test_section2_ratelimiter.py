"""Unit tests for the Redis sliding-window limiter (no Celery involved)."""
import threading
import time
from unittest import mock

import redis

from emailqueue.ratelimiter import FAIL_CLOSED_RETRY_MS, SlidingWindowRateLimiter


def make(client, limit=5, window=1.0, key="rl:test"):
    return SlidingWindowRateLimiter(client, key=key, limit=limit, window_seconds=window)


def test_admits_exactly_limit_then_refuses(test_redis):
    limiter = make(test_redis, limit=5)
    decisions = [limiter.acquire(f"m{i}") for i in range(6)]
    assert [d.allowed for d in decisions] == [True] * 5 + [False]
    # The refusal says how long until the oldest admission leaves the window.
    assert 0 < decisions[-1].retry_after_ms <= 1000


def test_window_slides(test_redis):
    limiter = make(test_redis, limit=2, window=0.5)
    assert limiter.acquire("a").allowed and limiter.acquire("b").allowed
    assert not limiter.acquire("c").allowed
    time.sleep(0.55)  # sleeping in a TEST is fine; the limiter itself never sleeps
    assert limiter.acquire("d").allowed


def test_atomic_under_concurrency(test_redis):
    """20 threads x 10 attempts against a limit of 50: exactly 50 must win.
    A non-atomic count-then-add would let several threads take the same slot."""
    limiter = make(test_redis, limit=50, window=10)
    results = []
    lock = threading.Lock()

    def worker(t):
        for i in range(10):
            allowed = limiter.acquire(f"t{t}-{i}").allowed
            with lock:
                results.append(allowed)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert sum(results) == 50


def test_fails_closed_when_redis_is_down():
    # Port 1 has nothing listening: simulates Redis being unreachable.
    dead = redis.Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.2)
    # Capture the expected error log instead of letting it print a traceback.
    with mock.patch("emailqueue.ratelimiter.logger") as log:
        decision = SlidingWindowRateLimiter(dead, "rl:x", 5, 1).acquire("m")
    assert decision.allowed is False
    assert decision.retry_after_ms == FAIL_CLOSED_RETRY_MS
    log.exception.assert_called_once()  # the outage is still reported
