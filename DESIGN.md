# DESIGN — Section 2: Rate-Limited Async Job Queue

## Problem
Send transactional emails (order confirmations, OTPs, alerts) through a provider capped at **200 emails per 60 seconds**, while handling flash-sale bursts of **2,000 requests in under 10 seconds**. The system must never exceed 200 calls/min across workers, never block workers with `time.sleep()`, retry transient failures with exponential backoff, move permanent failures to a dead-letter store, and lose zero jobs if a worker is killed with `SIGKILL`.

## Options considered

| Option | Strengths | Weaknesses |
|---|---|---|
| **Celery + Redis** | Built-in late acks, worker-loss re-queueing, visibility timeouts, and non-blocking `self.retry(countdown=...)`. Shares Redis with our Lua rate limiter. | Extra worker process to run; built-in `rate_limit` is per-worker process rather than cluster-wide; redelivery requires idempotent tasks. |
| **Django Q** | Simple setup and native Django Admin integration out of the box. | ORM broker mode struggles under 200 writes/sec burst contention; weaker non-blocking deferral and crash-recovery primitives than Celery. |
| **Custom implementation (`asyncio` / threads + Redis `BLPOP`)** | Small footprint (~150 lines), no framework overhead. | Have to hand-roll process supervision, `SIGKILL` unacked message recovery, backoff timers, and dead-letter tooling. |

**Chosen: Celery + Redis** (with `EmailJob` rows in the database as the source of truth).

**Why:** Celery already solves the hard parts of worker crash recovery (`acks_late`, `reject_on_worker_lost`, `visibility_timeout`) and non-blocking delayed retries (`self.retry(countdown=...)`). Because Celery's built-in `rate_limit="200/m"` applies *per worker process* (4 workers would send 800/min), I paired Celery with a custom Redis sliding-window limiter (`emailqueue/ratelimiter.py`) shared across all workers.

## Rate limiter

### 1. Algorithm: Sliding Window (Option B: Redis sorted set + `ZREMRANGEBYSCORE`)
I picked **Option B (Sliding Window)** over Fixed Window (Option C) and Token Bucket (Option A):
- **Why not Fixed Window (`INCR` + `EXPIRE`)**: Fixed 60-second buckets allow boundary spikes—200 emails at `00:59` and 200 more at `01:01` pass two separate buckets but hit the provider with **400 emails in 2 seconds**, triggering `429` errors.
- **Why Sliding Window over Token Bucket (`DECR` + `TTL`)**: A rolling sorted set matches the provider's exact rule ("no more than 200 requests in any 60s window"). Better yet, when the window is full, reading the oldest entry's timestamp (`ZRANGE key 0 0 WITHSCORES`) tells us the **exact millisecond** (`retry_after_ms`) when the next slot frees up. That lets us pass an exact `countdown` to Celery instead of guessing. The key never holds more than 200 members, so memory stays tiny.

### 2. Atomicity: Redis Lua Script (`SLIDING_WINDOW_LUA`)
Checking `ZCARD` in Python and then calling `ZADD` is racy—under a burst, 8 workers can all read `ZCARD == 199` at the same time and all send, breaching the 200/min cap. Redis `MULTI/EXEC` can't branch on the result of `ZCARD` without `WATCH` retry loops that thrash under load.

Instead, `SlidingWindowRateLimiter` in `emailqueue/ratelimiter.py` runs one Lua script atomically on Redis's single execution thread:
1. Calls `redis.call('TIME')` so all workers share Redis's clock (avoiding host clock drift).
2. Removes entries older than 60s via `ZREMRANGEBYSCORE key -inf now_ms-window_ms`.
3. If `ZCARD key < limit`, adds the unique attempt (`ZADD key now_ms member`), sets `PEXPIRE key window_ms`, and returns `{1, now_ms, 0}`.
4. If full, fetches the oldest entry via `ZRANGE key 0 0 WITHSCORES` and returns `{0, now_ms, oldest_ms + window_ms - now_ms}`.

### 3. Non-blocking deferral (no `time.sleep()`)
When `acquire()` returns `allowed=False`, `send_email` increments `job.deferrals` and yields the worker immediately:
```python
countdown = decision.retry_after_ms / 1000 + random.uniform(0, settings.EMAIL_RATE_WINDOW_SECONDS)
raise self.retry(countdown=countdown)
```
Spreading retries with random jitter across the window stops 1,800 waiting tasks from hammering Redis at the exact same millisecond when the window rolls over.

### 4. Behaviour when Redis is down: Fail Closed
If Redis raises `redis.RedisError`, `acquire()` catches it, logs the error, and returns `Decision(allowed=False, None, retry_after_ms=5000)` (**fail closed**). Failing open during a 2,000-email flash sale would blast the provider with 2,000 unthrottled requests in seconds and get our API key blocked. Failing closed defers the task for 5s while the `EmailJob` row stays safely `queued` in the database.

## Retries and dead-letter handling
Rate-limit deferrals shouldn't burn a job's error retry budget—during a 2,000-job burst (10 minutes to drain at 200/min), a healthy job might get rate-limited 8+ times before it ever reaches the provider.

In `emailqueue/tasks.py`:
- `max_retries=None` disables Celery's built-in counter so rate-limit re-schedules don't count as failures.
- Rate-limit waits increment `EmailJob.deferrals`; actual provider calls increment `EmailJob.attempts` and log a `SendAttempt` row.
- **Transient errors (`TransientProviderError` — timeouts, 5xx, 429)**: Retried up to `EMAIL_MAX_SEND_ATTEMPTS = 5` using exponential backoff with jitter: `min(base * 2^(attempt - 1) + uniform(0, base), 300s)` (~2s, 4s, 8s, 16s).
- **Dead-letter queue (`EmailJob.Status.DEAD`)**: Permanent errors (`PermanentProviderError`, e.g. invalid recipient) or jobs that fail all 5 attempts call `dead_letter()`, which sets `status='dead'` and saves `last_error` on the `EmailJob` row. Keeping dead letters in the DB makes them durable across Redis restarts, visible in Django Admin, and easy to replay via `python manage.py redrive_dead_letters`.

## Crash safety
- **Commit before enqueue (`emailqueue/services.py`)**: `enqueue_emails()` bulk-creates `EmailJob` rows inside `transaction.atomic()` and publishes `send_email.delay()` inside `transaction.on_commit()`, so a worker never picks up a job before its DB row exists.
- **Worker settings (`config/settings.py`)**:
  - `CELERY_TASK_ACKS_LATE = True` — message stays unacked until the task returns.
  - `CELERY_TASK_REJECT_ON_WORKER_LOST = True` — re-queues the message if the worker child is `SIGKILL`'d.
  - `CELERY_WORKER_PREFETCH_MULTIPLIER = 1` — workers only hold 1 message at a time.
  - `CELERY_BROKER_TRANSPORT_OPTIONS = {"visibility_timeout": 3600}` — Redis redelivers unacked messages if a whole worker box vanishes.
- **Idempotency**: `send_email` exits early if `job.status != QUEUED` and marks completion with `EmailJob.objects.filter(pk=job_id, status=QUEUED).update(status=SENT)`.

## Known limitations
1. **Small duplicate-send window on `SIGKILL`**: If a worker is `SIGKILL`'d in the few milliseconds *after* `provider.send()` succeeds but *before* the DB `UPDATE status='sent'` commits, the redelivered task will send again. In production, we'd pass `Idempotency-Key: <job.id>` in the provider HTTP headers so the provider deduplicates retries.
2. **Single queue for OTPs and bulk emails**: During a 2,000-order burst, urgent OTP emails share the same queue and 200/min budget as order confirmations. In production, I'd route OTPs to a dedicated high-priority queue (`-Q otp,default`) and reserve ~20 slots/min for them.