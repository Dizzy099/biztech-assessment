# Biz-Tech Analytics — Full-Stack Developer Assessment

Python · Django · Celery · Redis

## Run it (under 5 minutes)

Prerequisites: Python 3.10+, Docker Desktop (for Redis), Git.

```bash
# 1. Start Redis
docker compose up -d

# 2. Virtual environment + dependencies
python -m venv .venv
# Windows:      .venv\Scripts\activate
# macOS/Linux:  source .venv/bin/activate
pip install -r requirements.txt

# 3. Database
python manage.py migrate

# 4. Tests (13 tests, ~15 s; Section 2 starts a real Celery worker in-process)
pytest
```

No `.env` file is needed; every setting has a safe local default (see `.env.example`).
Tests use Redis database 15 and flush it, so they never touch development data in db 0.

## Section 1 — Diagnose a broken system

```bash
python manage.py seed_orders            # heavy customer: 250 orders x 3 items
python manage.py profile_summary 1      # query count + time, regressed vs fixed
python manage.py runserver
```

| Endpoint | What |
|---|---|
| `/api/orders/summary/?customer_id=1` | Fixed: 2 queries regardless of order count |
| `/api/orders/summary/regressed/?customer_id=1` | Reproduces the incident: 1,251 queries for 250 orders |
| `/silk/` | django-silk profiler (query count per request) |

Profiler screenshots: `docs/`. Investigation log and explanation: `ANSWERS.md` §1.

## Section 2 — Rate-limited email queue

```bash
# Terminal 1 — worker
celery -A config worker -l info --pool=solo      # Windows (prefork is not supported)
celery -A config worker -l info                  # macOS/Linux

# Terminal 2 — burst of jobs, then watch them drain
python manage.py reset_demo                                          # optional: start from zero
python manage.py enqueue_emails --count 500 --fail-every 50 --dead 1
python manage.py queue_status
docker compose exec redis redis-cli ZCARD ratelimit:email-provider   # slots used in the current window
python manage.py rate_check                                          # busiest 60 s window across the run (<= 200)
python manage.py redrive_dead_letters                                # re-queue dead-lettered jobs
```

Production limit is 200 emails per 60 s (`EMAIL_RATE_LIMIT`, `EMAIL_RATE_WINDOW_SECONDS`).
Architecture and trade-offs: `DESIGN.md`. SIGKILL answer: `ANSWERS.md` §2.

## Layout

| Path | What |
|---|---|
| `orders/` | Section 1 — models, regressed vs fixed queryset (`OrderQuerySet`), API, seed/profile commands |
| `emailqueue/ratelimiter.py` | Redis sliding-window limiter (one Lua script, Redis server clock) |
| `emailqueue/tasks.py` | Celery task: rate limit, exponential backoff, dead-letter, idempotency |
| `emailqueue/models.py` | `EmailJob` (source of truth, status=dead is the dead-letter store), `SendAttempt` (audit) |
| `tests/` | `test_section1_orders.py`, `test_section2_ratelimiter.py`, `test_section2_queue.py` |
| `DESIGN.md` | Section 2 architecture decisions |
| `ANSWERS.md` | Written answers for Sections 1, 2 and 3 |

## Written answers and evidence

- Sections 1 and 3, plus the Section 2 SIGKILL answer: [ANSWERS.md](ANSWERS.md)
- Section 2 architecture decisions: [DESIGN.md](DESIGN.md)
- Profiler evidence (django-silk screenshots + CLI output): [docs/](docs/)

## Optional recording

Not recorded.

## Note on tooling

Implementation was built with AI coding assistance; design choices, written answers and verification are my own.
