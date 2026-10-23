# DESIGN — Section 2: Rate-Limited Async Job Queue

## Problem
<!-- 200 emails/min provider limit; bursts of 2,000 requests in <10 s; retries; no job loss on worker crash. -->

## Options considered
| Option | Strengths | Weaknesses |
|---|---|---|
| Celery + Redis | | |
| Django Q | | |
| Custom implementation | | |

**Chosen:** <!-- which and why -->

## Rate limiter
- Algorithm (token bucket / sliding window / fixed window) and why:
- Atomicity (Lua script / MULTI-EXEC / pipeline):
- Behaviour when Redis is down (fail open or closed) and why:

## Retries and dead-letter handling
<!-- backoff formula, max retries, where permanently failed jobs go -->

## Crash safety
<!-- acks_late, reject_on_worker_lost, prefetch, visibility_timeout, idempotency -->

## Known limitations
