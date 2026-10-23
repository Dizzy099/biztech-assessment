import uuid

from django.db import models


class EmailJob(models.Model):
    """One transactional email. The database row - not the broker message - is
    the source of truth: it makes sends idempotent, lets us prove no job was lost,
    and acts as the durable dead-letter store (status=dead)."""

    class Kind(models.TextChoices):
        ORDER_CONFIRMATION = "order_confirmation"
        OTP = "otp"
        ALERT = "alert"

    class Status(models.TextChoices):
        QUEUED = "queued"
        SENT = "sent"
        DEAD = "dead"  # permanently failed: the dead-letter state

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    kind = models.CharField(max_length=32, choices=Kind.choices)
    to_email = models.EmailField()
    subject = models.CharField(max_length=200)
    body = models.TextField(blank=True)

    status = models.CharField(max_length=10, choices=Status.choices, default=Status.QUEUED)
    attempts = models.PositiveIntegerField(default=0)   # real send attempts to the provider
    deferrals = models.PositiveIntegerField(default=0)  # times re-scheduled by the rate limiter
    last_error = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    sent_at = models.DateTimeField(null=True, blank=True)

    # Test/demo hooks for the fake provider (a real system would not have these).
    simulate_transient_failures = models.PositiveSmallIntegerField(default=0)
    simulate_permanent_failure = models.BooleanField(default=False)

    class Meta:
        indexes = [models.Index(fields=["status"], name="emailjob_status")]


class SendAttempt(models.Model):
    """Audit row per provider call. admitted_at_ms is the Redis server time at
    which the rate limiter granted the slot - the test checks the limit on these."""

    class Outcome(models.TextChoices):
        SENT = "sent"
        TRANSIENT_ERROR = "transient_error"
        PERMANENT_ERROR = "permanent_error"

    job = models.ForeignKey(EmailJob, on_delete=models.CASCADE, related_name="send_attempts")
    attempt_number = models.PositiveIntegerField()
    admitted_at_ms = models.BigIntegerField()
    outcome = models.CharField(max_length=20, choices=Outcome.choices)
    error = models.TextField(blank=True)
