"""Email delivery task: rate-limited, retried with backoff, dead-lettered.

Crash safety comes from the Celery settings in config/settings.py
(acks_late + reject_on_worker_lost + prefetch 1): a message is acknowledged
only after this function returns, so a SIGKILL'd worker leaves it unacked and
Redis redelivers it after visibility_timeout. That is at-least-once delivery,
so this task is written to be safe to run twice (see the status checks).
"""
import logging
import random
import uuid

from celery import shared_task
from django.conf import settings
from django.db.models import F
from django.utils import timezone

from .models import EmailJob, SendAttempt
from .provider import PermanentProviderError, TransientProviderError, provider
from .ratelimiter import get_email_rate_limiter

logger = logging.getLogger(__name__)


def backoff_seconds(attempt_number: int) -> float:
    """Exponential backoff with jitter: base * 2^(n-1) + U(0, base), capped.
    With base=2s: ~2s, 4s, 8s, 16s ... The jitter stops many failed jobs from
    retrying in the same instant (thundering herd)."""
    base = settings.EMAIL_RETRY_BASE_SECONDS
    delay = base * (2 ** (attempt_number - 1)) + random.uniform(0, base)
    return min(delay, settings.EMAIL_RETRY_MAX_SECONDS)


def dead_letter(job_id, error: str) -> None:
    """Park a permanently failed job. status=dead in the database IS the
    dead-letter queue: durable, visible in the admin, and re-drivable with
    `manage.py redrive_dead_letters`."""
    EmailJob.objects.filter(pk=job_id, status=EmailJob.Status.QUEUED).update(
        status=EmailJob.Status.DEAD, last_error=error[:2000]
    )
    logger.error("Email job %s dead-lettered: %s", job_id, error)


@shared_task(bind=True, max_retries=None, ignore_result=True)
def send_email(self, job_id: str) -> None:
    # max_retries=None: Celery's retry counter would mix rate-limit waits with
    # real failures. We count real failures ourselves in job.attempts instead.
    job = EmailJob.objects.filter(pk=job_id).first()
    if job is None:
        logger.warning("Email job %s not found - skipping", job_id)
        return
    if job.status != EmailJob.Status.QUEUED:
        # Idempotency: a redelivered message (after a crash) for a job that was
        # already sent or dead-lettered must not send again.
        return

    # 1. Rate limit. A unique member per attempt so the sorted set counts it.
    decision = get_email_rate_limiter().acquire(member=f"{job_id}:{uuid.uuid4().hex}")
    if not decision.allowed:
        EmailJob.objects.filter(pk=job_id).update(deferrals=F("deferrals") + 1)
        # Wait until a slot frees, plus jitter across the window so the deferred
        # jobs come back spread out instead of all at the same millisecond.
        countdown = decision.retry_after_ms / 1000 + random.uniform(
            0, settings.EMAIL_RATE_WINDOW_SECONDS
        )
        raise self.retry(countdown=countdown)

    # 2. Call the provider. The slot is consumed whether the call succeeds or
    #    fails, because a failed call still counts against the provider's limit.
    attempt_number = job.attempts + 1
    EmailJob.objects.filter(pk=job_id).update(attempts=attempt_number)
    try:
        provider.send(job, attempt_number)
    except PermanentProviderError as exc:
        SendAttempt.objects.create(job_id=job_id, attempt_number=attempt_number,
                                   admitted_at_ms=decision.admitted_at_ms,
                                   outcome=SendAttempt.Outcome.PERMANENT_ERROR, error=str(exc))
        dead_letter(job_id, f"permanent: {exc}")
        return
    except TransientProviderError as exc:
        SendAttempt.objects.create(job_id=job_id, attempt_number=attempt_number,
                                   admitted_at_ms=decision.admitted_at_ms,
                                   outcome=SendAttempt.Outcome.TRANSIENT_ERROR, error=str(exc))
        EmailJob.objects.filter(pk=job_id).update(last_error=str(exc))
        if attempt_number >= settings.EMAIL_MAX_SEND_ATTEMPTS:
            dead_letter(job_id, f"gave up after {attempt_number} attempts: {exc}")
            return
        raise self.retry(countdown=backoff_seconds(attempt_number), exc=exc)

    # 3. Success. The conditional UPDATE means two concurrent copies of the same
    #    message cannot both mark the job sent.
    SendAttempt.objects.create(job_id=job_id, attempt_number=attempt_number,
                               admitted_at_ms=decision.admitted_at_ms,
                               outcome=SendAttempt.Outcome.SENT)
    EmailJob.objects.filter(pk=job_id, status=EmailJob.Status.QUEUED).update(
        status=EmailJob.Status.SENT, sent_at=timezone.now(), last_error=""
    )


@shared_task
def ping():
    """Setup check: proves a worker can pick up a task."""
    return "pong"
