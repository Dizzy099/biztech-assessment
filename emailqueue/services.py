from django.db import transaction

from .models import EmailJob
from .tasks import send_email


def enqueue_emails(jobs: list[EmailJob]) -> list[EmailJob]:
    """Persist jobs, then publish one task per job AFTER the transaction commits.
    Publishing inside the transaction could let a worker pick up a task whose row
    is not committed yet (it would see "job not found")."""
    with transaction.atomic():
        created = EmailJob.objects.bulk_create(jobs)
        transaction.on_commit(lambda: [send_email.delay(str(j.id)) for j in created])
    return created
