"""End-to-end: a real Celery worker (in a thread) against real Redis.

Not eager mode on purpose: CELERY_TASK_ALWAYS_EAGER runs tasks inline and skips
the broker, acks and real retries - it would prove nothing about this queue.
The limit is scaled down (50 per 1 s instead of 200 per 60 s) so the test takes
seconds; the code path is identical.
"""
import time
import uuid

import pytest
from celery.contrib.testing.worker import start_worker

from config.celery import app as celery_app
from emailqueue.models import EmailJob, SendAttempt
from emailqueue.services import enqueue_emails

LIMIT = 50
WINDOW_S = 1.0

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture
def worker(settings, test_redis):
    settings.EMAIL_RATE_LIMIT = LIMIT
    settings.EMAIL_RATE_WINDOW_SECONDS = WINDOW_S
    settings.EMAIL_RATE_LIMIT_KEY = "ratelimit:test"
    settings.EMAIL_RETRY_BASE_SECONDS = 0.05
    settings.EMAIL_MAX_SEND_ATTEMPTS = 3
    # A private queue per run: a dev worker left running in another terminal
    # listens on the default "celery" queue and would otherwise steal these tasks.
    queue = f"test-emails-{uuid.uuid4().hex[:8]}"
    saved = {k: celery_app.conf[k] for k in ("broker_url", "result_backend", "task_default_queue")}
    celery_app.conf.update(broker_url=settings.REDIS_URL, result_backend=settings.REDIS_URL,
                           task_default_queue=queue)
    try:
        with start_worker(celery_app, pool="solo", perform_ping_check=False,
                          shutdown_timeout=10, loglevel="WARNING", queues=[queue]):
            yield
    finally:
        celery_app.conf.update(saved)


def wait_until_settled(expected, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if EmailJob.objects.exclude(status=EmailJob.Status.QUEUED).count() >= expected:
            return
        time.sleep(0.25)
    pytest.fail(f"Timed out: {EmailJob.objects.filter(status='queued').count()} still queued")


def max_in_any_window(timestamps_ms, window_ms):
    """Largest number of admissions inside any window of length window_ms
    (two-pointer sweep over sorted timestamps)."""
    ts = sorted(timestamps_ms)
    best, left = 0, 0
    for right, t in enumerate(ts):
        while t - ts[left] >= window_ms:
            left += 1
        best = max(best, right - left + 1)
    return best


def test_500_jobs_none_lost_limit_respected_failures_retried(worker):
    jobs = [
        EmailJob(kind=EmailJob.Kind.ORDER_CONFIRMATION, to_email=f"u{i}@example.com",
                 subject=f"Order {i}",
                 simulate_transient_failures=1 if i % 50 == 0 else 0)  # 10 flaky jobs
        for i in range(500)
    ]
    started = time.monotonic()
    enqueue_emails(jobs)
    wait_until_settled(500)
    elapsed = time.monotonic() - started

    # 1. No job lost: every one of the 500 ended SENT (flaky ones recovered).
    assert EmailJob.objects.count() == 500
    assert EmailJob.objects.filter(status=EmailJob.Status.SENT).count() == 500

    # 2. Rate limit never exceeded, checked on every provider call (including
    #    the failed ones, which also count against the provider's limit).
    admissions = list(SendAttempt.objects.values_list("admitted_at_ms", flat=True))
    assert len(admissions) == 500 + 10  # 500 successes + 10 failed first attempts
    assert max_in_any_window(admissions, int(WINDOW_S * 1000)) <= LIMIT
    # ...and the limiter really throttled: 510 calls at 50/s cannot finish in < ~9 s.
    assert elapsed >= (510 / LIMIT - 1) * WINDOW_S

    # 3. Intentional failures were retried correctly: failed once, then sent.
    flaky = EmailJob.objects.filter(simulate_transient_failures=1)
    assert flaky.count() == 10
    for job in flaky:
        outcomes = list(job.send_attempts.order_by("attempt_number").values_list("outcome", flat=True))
        assert outcomes == [SendAttempt.Outcome.TRANSIENT_ERROR, SendAttempt.Outcome.SENT]
        assert job.attempts == 2

    # 4. Rate-limit waits did not burn the failure budget.
    assert EmailJob.objects.filter(deferrals__gt=0).exists()
    assert not EmailJob.objects.filter(attempts__gt=2).exists()


def test_permanent_and_exhausted_failures_are_dead_lettered(worker):
    jobs = [
        EmailJob(kind=EmailJob.Kind.OTP, to_email="bad@example.com", subject="OTP",
                 simulate_permanent_failure=True),
        EmailJob(kind=EmailJob.Kind.ALERT, to_email="flaky@example.com", subject="Alert",
                 simulate_transient_failures=99),  # never recovers
        EmailJob(kind=EmailJob.Kind.ALERT, to_email="ok@example.com", subject="Alert"),
    ]
    enqueue_emails(jobs)
    wait_until_settled(3, timeout=30)

    permanent = EmailJob.objects.get(to_email="bad@example.com")
    assert permanent.status == EmailJob.Status.DEAD
    assert permanent.attempts == 1  # a permanent error is not retried

    exhausted = EmailJob.objects.get(to_email="flaky@example.com")
    assert exhausted.status == EmailJob.Status.DEAD
    assert exhausted.attempts == 3  # EMAIL_MAX_SEND_ATTEMPTS in this test
    assert "gave up after 3 attempts" in exhausted.last_error

    assert EmailJob.objects.get(to_email="ok@example.com").status == EmailJob.Status.SENT
